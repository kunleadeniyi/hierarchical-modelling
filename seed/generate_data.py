"""
generate_data.py — Synthetic data generator for the hierarchical issue tracker.

Produces fake-but-realistic rows that match the issues_raw schema and writes them
directly to Postgres. The downstream pipeline scripts (01–04) then run unchanged
against this synthetic data.

issues_raw columns produced:
    issue             TEXT  — issue message text
    error             TEXT  — raw error/log line
    tag               TEXT  — issue type category
    contact           TEXT  — owner / responsible user
    asset             TEXT  — asset filename (leaf)
    asset_path        TEXT  — normalised asset path (forward-slash, no drive letter)
    last_edit         TEXT  — date string "YYYY/MM/DD"
    build_machine_path TEXT — full UNC-style path (backslash-separated)
    refered_to_by     TEXT  — optional upstream reference (may be blank)
    cl                TEXT  — changelist number as a string

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    python seed/generate_data.py
    python seed/generate_data.py --cls 50 --issues 300 --seed 99
    python seed/generate_data.py --dry-run     # prints row count, no DB write
"""

from __future__ import annotations

import argparse
import hashlib
import os
import random
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================
# SECTION 1 — TOP-LEVEL CONFIG
# Adjust these constants to control the overall shape of the
# generated dataset without touching generator logic.
# ============================================================

# ------------------------------------
# 1.1  Project definitions
# Each entry becomes one row in the `project` table and one
# logical namespace for all generated data.
# ------------------------------------
@dataclass
class ProjectConfig:
    project_id: str        # short key used as FK throughout the schema, e.g. "proj-alpha"
    project_key: str       # human-readable slug, e.g. "alpha"
    project_name: str      # display name, e.g. "Project Alpha"
    server_prefix: str     # fake build-server hostname prefix, e.g. "bldsvr-01"
    share_name: str        # fake network share name, e.g. "builds$"
    base_dir: str          # top-level directory on the share, e.g. "DevOps/IssueTracker"
    sub_dir: str           # sub-directory under base_dir, e.g. "alphabuild/Alpha"

PROJECTS: list[ProjectConfig] = [
    ProjectConfig(
        project_id="proj-alpha",
        project_key="alpha",
        project_name="Project Alpha",
        server_prefix="bldsvr-01",
        share_name="builds$",
        base_dir="DevOps/IssueTracker",
        sub_dir="alphabuild/Alpha",
    ),
    ProjectConfig(
        project_id="proj-beta",
        project_key="beta",
        project_name="Project Beta",
        server_prefix="bldsvr-02",
        share_name="builds$",
        base_dir="DevOps/IssueTracker",
        sub_dir="betabuild/Beta",
    ),
]

# ------------------------------------
# 1.2  Volume controls
# ------------------------------------

# Number of changelists to generate per project.
# proj-alpha gets the full count; proj-beta gets BETA_CL_FRACTION of it.
DEFAULT_CLS_PER_PROJECT = 85
BETA_CL_FRACTION = 0.30          # proj-beta is a smaller project (~30% of CLs)

# Target number of unique issues (issue_instance rows) across all projects.
# Actual count may vary slightly due to de-duplication by signature.
DEFAULT_ISSUE_COUNT = 650

# How many observations to generate per CL on average.
# This controls row density in issues_raw; not every issue appears in every CL.
AVG_OBSERVATIONS_PER_CL = 18

# Maximum observations per CL (caps spike CLs).
MAX_OBSERVATIONS_PER_CL = 60

# ------------------------------------
# 1.3  Reproducibility
# ------------------------------------
DEFAULT_SEED = 42  # set via --seed to get a different but stable dataset

# ------------------------------------
# 1.4  CL numbering and timeline
# ------------------------------------

# First CL number (monotonically increasing integers from here).
CL_BASE_NUMBER = 10001

# Fake start date for the first CL.
TIMELINE_START_DATE = date(2024, 1, 8)

# Min/max days between consecutive CLs (uniform random in this range).
CL_GAP_DAYS_MIN = 1
CL_GAP_DAYS_MAX = 3

# ------------------------------------
# 1.5  Path hierarchy structure
# Defines the named segments used to build build_machine_path values.
# Levels: server/share/base_dir/sub_dir/Main_BuildMachine/<cl>/<issue_type>/<team>/<subdir>/<asset>
# ------------------------------------

TEAMS: list[str] = [
    "team-art",
    "team-audio",
    "team-code",
    "team-vfx",
    "team-ui",
    "team-env",
]

SUBDIRS_BY_TEAM: dict[str, list[str]] = {
    "team-art":   ["Characters", "Props", "Vehicles", "Weapons"],
    "team-audio": ["Ambient", "Music", "SFX", "Voice"],
    "team-code":  ["Blueprints", "Scripts", "Config", "Plugins"],
    "team-vfx":   ["Particles", "Materials", "Shaders", "PostProcess"],
    "team-ui":    ["Widgets", "HUD", "Menus", "Icons"],
    "team-env":   ["Levels", "Terrain", "Lighting", "Foliage"],
}

# ------------------------------------
# 1.6  Issue type definitions
# weight: relative frequency (will be normalised to a probability distribution)
# ------------------------------------
@dataclass
class IssueTypeConfig:
    tag: str                # value stored in the `tag` / issue_type column
    weight: float           # relative sampling probability
    asset_ext: str          # file extension for assets of this type
    asset_prefix: str       # prefix for fake asset names

ISSUE_TYPES: list[IssueTypeConfig] = [
    IssueTypeConfig(tag="assetLoad",     weight=0.45, asset_ext=".uasset", asset_prefix="DA_"),
    IssueTypeConfig(tag="audioMissing",  weight=0.25, asset_ext=".uasset", asset_prefix="AK_"),
    IssueTypeConfig(tag="shaderCompile", weight=0.20, asset_ext=".usf",    asset_prefix="M_"),
    IssueTypeConfig(tag="scriptError",   weight=0.10, asset_ext=".uasset", asset_prefix="BP_"),
]

# ------------------------------------
# 1.7  Issue presence profiles
# Controls how issues are distributed across CLs (see todo 6 for implementation).
# ratio: fraction of all issues assigned to this profile
# min_appearance_rate / max_appearance_rate: probability the issue appears in any given CL
# ------------------------------------
@dataclass
class PresenceProfile:
    name: str
    ratio: float             # fraction of issues assigned this profile
    min_appearance_rate: float
    max_appearance_rate: float

PRESENCE_PROFILES: list[PresenceProfile] = [
    PresenceProfile(name="persistent",    ratio=0.30, min_appearance_rate=0.75, max_appearance_rate=1.00),
    PresenceProfile(name="intermittent",  ratio=0.50, min_appearance_rate=0.20, max_appearance_rate=0.65),
    PresenceProfile(name="transient",     ratio=0.20, min_appearance_rate=0.01, max_appearance_rate=0.15),
]

# ------------------------------------
# 1.8  Contact / owner pool
# Fake coded user identifiers (no real names).
# ------------------------------------
CONTACTS: list[str] = [f"user-{i:03d}" for i in range(1, 21)]  # user-001 … user-020

# ============================================================
# SECTION 2 — DERIVED / COMPUTED CONFIG
# Do not edit these directly; they are calculated from the
# constants above and are here for convenience / documentation.
# ============================================================

def build_issue_type_weights() -> tuple[list[str], list[float]]:
    """Return (tags, normalised_weights) ready for random.choices()."""
    tags = [it.tag for it in ISSUE_TYPES]
    raw_weights = [it.weight for it in ISSUE_TYPES]
    total = sum(raw_weights)
    return tags, [w / total for w in raw_weights]


def cl_counts_per_project(n_cls: int) -> dict[str, int]:
    """Return CL count per project_id given the target per-project CL count."""
    counts: dict[str, int] = {}
    for proj in PROJECTS:
        if proj.project_id == PROJECTS[0].project_id:
            counts[proj.project_id] = n_cls
        else:
            counts[proj.project_id] = max(10, round(n_cls * BETA_CL_FRACTION))
    return counts


# ============================================================
# SECTION 3 — ISSUE TEMPLATE LIBRARY
#
# Provides two things per issue type:
#   1. issue_text templates  — what goes in issues_raw.issue
#   2. error_text templates  — what goes in issues_raw.error
#
# Templates use {token} placeholders filled at render time by the
# issue instance generator (todo 4).  Each type has multiple
# variants so repeated sampling doesn't produce identical strings.
#
# Column mapping to the downstream schema:
#   rendered issue_text  → issue_observation.issue_text
#                          issue_instance.issue_pattern (normalised)
#   rendered error_text  → issue_observation.error_text
#   asset_path token     → issue_instance.asset_path
#   missing_token token  → issue_instance.missing_token
# ============================================================

# ------------------------------------
# 3.1  Token pools
# Each pool is sampled when filling a template placeholder.
# Keeping them short (10–20 entries) ensures enough repetition
# across 650 issues to produce interesting presence-interval data.
# ------------------------------------

# Fake Unreal-style package/script tokens (used by assetLoad + scriptError)
_SCRIPT_TOKENS: list[str] = [
    "/Script/CoreUObject",
    "/Script/ModioUICore",
    "/Script/AnimGraphRuntime",
    "/Script/PhysicsCore",
    "/Script/NavigationSystem",
    "/Script/AIModule",
    "/Script/GameplayAbilities",
    "/Script/EnhancedInput",
    "/Script/Chaos",
    "/Script/NetCore",
]

# Fake audio-bank / wwise cue tokens (used by audioMissing)
_AUDIO_TOKENS: list[str] = [
    "AK_Amb_Forest_Loop",
    "AK_SFX_Explosion_Large",
    "AK_Music_Combat_01",
    "AK_SFX_Footstep_Gravel",
    "AK_Voice_NPC_Grunt",
    "AK_Amb_Wind_Desert",
    "AK_SFX_UI_Click",
    "AK_Music_Menu_Idle",
    "AK_SFX_Weapon_Reload",
    "AK_Amb_Rain_Heavy",
]

# Fake shader pass names (used by shaderCompile)
_SHADER_PASSES: list[str] = [
    "BasePass",
    "DepthPass",
    "ShadowDepth",
    "TranslucentPass",
    "VelocityPass",
    "CustomDepth",
    "PostProcess",
    "LightFunction",
    "DistortionAccumulate",
    "HitProxy",
]

# Fake Blueprint class name fragments (used by scriptError)
_BP_CLASS_FRAGMENTS: list[str] = [
    "PlayerController",
    "EnemyBase",
    "InventoryComponent",
    "QuestManager",
    "AbilitySystemComp",
    "DamageHandler",
    "SpawnVolume",
    "DialogueTrigger",
    "SaveGameSubsystem",
    "LevelTransitionGate",
]

# ------------------------------------
# 3.2  Template definitions
# Each IssueTemplate holds:
#   tag          — must match an IssueTypeConfig.tag in ISSUE_TYPES
#   issue_variants — list of f-string-style templates for issues_raw.issue
#   error_variants — list of f-string-style templates for issues_raw.error
#   token_pool   — the placeholder pool to sample {token} from
#
# Placeholders used:
#   {asset_path}  — full fake asset path  e.g. "/Game/team-art/Characters/DA_Sword"
#   {asset_name}  — filename without ext  e.g. "DA_Sword_01"
#   {token}       — type-specific token   e.g. "/Script/ModioUICore"
#   {pass_name}   — shader pass name      e.g. "BasePass"
#   {class_name}  — Blueprint class name  e.g. "BP_EnemyBase"
# ------------------------------------

@dataclass
class IssueTemplate:
    tag: str
    issue_variants: list[str]
    error_variants: list[str]
    token_pool: list[str]


ISSUE_TEMPLATES: dict[str, IssueTemplate] = {
    "assetLoad": IssueTemplate(
        tag="assetLoad",
        token_pool=_SCRIPT_TOKENS,
        issue_variants=[
            "LogLinker: Warning: [AssetLog] ({asset_path}): Failed to find object '{token}'",
            "LogUObjectGlobals: Warning: Failed to find object '{token}' for asset '{asset_name}'",
            "LogStreaming: Error: Couldn't find file for package '{asset_path}' requested by async loading code",
            "LogLinker: Warning: Unable to load '{token}': Can't find file.",
            "LogContentStreaming: Warning: Asset '{asset_name}' failed to resolve dependency '{token}'",
        ],
        error_variants=[
            "@blueprint:LogLinker: Warning: [AssetLog] ({asset_path}): Failed to find object '{token}'",
            "@blueprint:LogUObjectGlobals: Warning: Object not found: '{token}' in '{asset_path}'",
            "@blueprint:LogStreaming: Error: Async load failed for '{asset_path}' — missing dep '{token}'",
            "@blueprint:LogLinker: Warning: Skipping import of '{token}' — package unavailable",
            "@blueprint:LogContentStreaming: Warning: Dependency resolution failed: '{token}'",
        ],
    ),

    "audioMissing": IssueTemplate(
        tag="audioMissing",
        token_pool=_AUDIO_TOKENS,
        issue_variants=[
            "LogAudio: Warning: Sound cue '{token}' could not be found for asset '{asset_name}'",
            "LogWwise: Error: Event '{token}' not found in SoundBank — asset '{asset_path}'",
            "LogAudio: Warning: Missing audio reference '{token}' on '{asset_name}'",
            "LogWwise: Warning: Bank load failed for cue '{token}' referenced by '{asset_path}'",
            "LogAudioMixer: Error: AudioComponent on '{asset_name}' references undefined cue '{token}'",
        ],
        error_variants=[
            "Wwise: ERROR: Event '{token}' not found in any loaded SoundBank",
            "Wwise: WARNING: Could not post event '{token}' — bank not loaded",
            "AudioEngine: MissingCue '{token}' for '{asset_path}'",
            "Wwise: ERROR: SoundBank missing for cue '{token}' — check bank generation",
            "AudioEngine: Undefined reference '{token}' in asset '{asset_name}'",
        ],
    ),

    "shaderCompile": IssueTemplate(
        tag="shaderCompile",
        token_pool=_SHADER_PASSES,
        issue_variants=[
            "LogShaderCompilers: Warning: Failed to compile shader '{asset_name}.usf' for pass '{token}'",
            "LogMaterial: Error: Material '{asset_name}' failed to compile permutation for '{token}'",
            "LogShaderCompilers: Error: Compile error in '{asset_path}' ({token} pass): undeclared identifier",
            "LogMaterial: Warning: Shader '{asset_name}' has unsupported instruction count in '{token}'",
            "LogShaderCompilers: Warning: '{asset_name}.usf' — '{token}' pass exceeded resource limit",
        ],
        error_variants=[
            "D3D11: ShaderCompile FAILED — '{asset_name}.usf' pass '{token}': error X3000",
            "ShaderCompiler: ERROR — '{token}' pass in '{asset_path}' has 0 valid permutations",
            "ShaderCompiler: WARN — instruction count exceeded in '{asset_name}' ({token})",
            "D3D11: Compile error — '{asset_name}.usf' ({token}): undeclared identifier 'WorldPosition'",
            "ShaderCompiler: FAILED '{asset_name}' — '{token}' unsupported feature level",
        ],
    ),

    "scriptError": IssueTemplate(
        tag="scriptError",
        token_pool=_BP_CLASS_FRAGMENTS,
        issue_variants=[
            "LogBlueprint: Warning: [compiler] {class_name}: '{token}' is not defined in this scope",
            "LogBlueprint: Error: Blueprint '{asset_name}' failed to compile — variable '{token}' missing",
            "LogBlueprint: Warning: {class_name} references deprecated function '{token}'",
            "LogBlueprint: Error: Pin mismatch in '{asset_name}': expected '{token}' got 'None'",
            "LogBlueprint: Warning: Circular dependency detected in '{class_name}' via '{token}'",
        ],
        error_variants=[
            "Blueprint compile error in '{class_name}': '{token}' is not defined",
            "Blueprint ERROR: '{asset_name}' — variable '{token}' not found on target class",
            "Blueprint WARNING: deprecated call to '{token}' in '{class_name}'",
            "Blueprint ERROR: '{asset_name}' pin '{token}' type mismatch — recompile required",
            "Blueprint WARNING: circular ref in '{class_name}' — '{token}' may cause infinite loop",
        ],
    ),
}


# ------------------------------------
# 3.3  Template rendering helpers
# ------------------------------------

def _bp_class_name(asset_stem: str) -> str:
    """Return the asset stem as a Blueprint class name.

    The stem already carries its type prefix (e.g. 'BP_EnemyBase_03'),
    so no additional prefix is needed.
    """
    return asset_stem


def render_issue_text(
    tag: str,
    asset_path_str: str,
    asset_stem: str,       # filename without extension, prefix already included
) -> tuple[str, str, str]:
    """
    Render one (issue_text, error_text, token) triple for a given issue type.

    Picks a random variant from the template and fills all placeholders.
    Returns the tuple so the caller can store token as missing_token in
    issue_instance without re-parsing the rendered string.

    Args:
        tag:            issue type tag, must be a key in ISSUE_TEMPLATES
        asset_path_str: normalised asset path, e.g. "/Game/team-art/Characters/DA_Sword_01"
        asset_stem:     asset filename without extension, e.g. "DA_Sword_01"

    Returns:
        (issue_text, error_text, token)
    """
    tmpl = ISSUE_TEMPLATES[tag]
    token = random.choice(tmpl.token_pool)
    class_name = _bp_class_name(asset_stem)

    ctx = {
        "asset_path": asset_path_str,
        "asset_name": asset_stem,
        "token": token,
        "pass_name": token,    # shaderCompile reuses token pool as pass_name
        "class_name": class_name,
    }

    issue_text = random.choice(tmpl.issue_variants).format(**ctx)
    error_text = random.choice(tmpl.error_variants).format(**ctx)

    return issue_text, error_text, token


# ============================================================
# SECTION 5 — PATH HIERARCHY GENERATOR
#
# Produces the build_machine_path strings that drive every other
# table in the model.  The full path has this structure (12 segments):
#
#   server \ share \ base[0] \ base[1] \ sub[0] \ sub[1] \
#   Main_BuildMachine \ <cl> \ <issue_type> \ <team> \ <subdir> \ <contact> \ <asset>
#
# Segments relative to the CL root (what the BI view exposes):
#   level_1 = issue_type   e.g. "assetLoad"
#   level_2 = team         e.g. "team-art"
#   level_3 = subdir       e.g. "Characters"
#   level_4 = contact      e.g. "user-007"
#   level_5 = asset        e.g. "DA_Sword_01.uasset"   ← leaf, dropped by pipeline
#
# The pipeline's norm_path_build_machine() drops the leaf before
# writing to path_node, so observations are anchored at level_4
# (the contact folder), giving 4 navigable levels in the BI view.
# ============================================================

@dataclass
class PathSlot:
    """
    One valid location in the folder hierarchy below the CL root.

    issue_type, team, and subdir are fixed structural segments.
    contact is assigned per-observation from the CONTACTS pool.
    Together they uniquely identify a leaf-parent node in path_node.
    """
    issue_type: str    # level_1 below CL root
    team: str          # level_2
    subdir: str        # level_3
    # contact (level_4) is NOT stored here — it varies per observation
    # so that the same (issue_type, team, subdir) slot can be shared
    # across multiple owners without creating a path_node per owner.


def generate_path_slots() -> list[PathSlot]:
    """
    Return every valid (issue_type, team, subdir) combination.

    Produces 4 * 6 * 4 = 96 unique slots across the two projects.
    The assembler samples from this list when placing an issue
    observation in the hierarchy.
    """
    slots: list[PathSlot] = []
    for issue_type_cfg in ISSUE_TYPES:
        for team in TEAMS:
            for subdir in SUBDIRS_BY_TEAM[team]:
                slots.append(PathSlot(
                    issue_type=issue_type_cfg.tag,
                    team=team,
                    subdir=subdir,
                ))
    return slots


# Pre-built once at import time; other functions reference this list.
ALL_PATH_SLOTS: list[PathSlot] = generate_path_slots()


def make_build_machine_path(
    proj: ProjectConfig,
    cl_number: int,
    slot: PathSlot,
    contact: str,
    asset_filename: str,
) -> str:
    """
    Construct the full UNC-style build_machine_path (backslash-separated).

    This is the raw value written to issues_raw.build_machine_path.
    The pipeline will:
      - convert backslashes → forward slashes
      - strip the leaf segment (asset_filename) → container_key
      - find Main_BuildMachine position → cl_root_key

    Example output:
      bldsvr-01\\builds$\\DevOps\\IssueTracker\\alphabuild\\Alpha\\
      Main_BuildMachine\\10001\\assetLoad\\team-art\\Characters\\user-007\\
      DA_Sword_01.uasset
    """
    parts: list[str] = [
        proj.server_prefix,
        proj.share_name,
        *proj.base_dir.split("/"),    # "DevOps/IssueTracker" → ["DevOps", "IssueTracker"]
        *proj.sub_dir.split("/"),     # "alphabuild/Alpha"    → ["alphabuild", "Alpha"]
        "Main_BuildMachine",
        str(cl_number),
        slot.issue_type,
        slot.team,
        slot.subdir,
        contact,
        asset_filename,
    ]
    return "\\".join(parts)


def container_key(
    proj: ProjectConfig,
    cl_number: int,
    slot: PathSlot,
    contact: str,
) -> str:
    """
    Forward-slash path up to (not including) the asset leaf.

    This is what norm_path_build_machine() produces from a full
    build_machine_path, and what path_node.full_path_key is set to
    for the observation's anchor node.

    Example:
      bldsvr-01/builds$/DevOps/IssueTracker/alphabuild/Alpha/
      Main_BuildMachine/10001/assetLoad/team-art/Characters/user-007
    """
    parts: list[str] = [
        proj.server_prefix,
        proj.share_name,
        *proj.base_dir.split("/"),
        *proj.sub_dir.split("/"),
        "Main_BuildMachine",
        str(cl_number),
        slot.issue_type,
        slot.team,
        slot.subdir,
        contact,
    ]
    return "/".join(parts)


def cl_root_key(proj: ProjectConfig, cl_number: int) -> str:
    """
    Forward-slash path to the CL root node (Main_BuildMachine/<cl>).

    Matches what cl_node_full_path_key() in the changelist pipeline
    script produces, and what changelist.changelist_node_id references
    in path_node.

    Example:
      bldsvr-01/builds$/DevOps/IssueTracker/alphabuild/Alpha/
      Main_BuildMachine/10001
    """
    parts: list[str] = [
        proj.server_prefix,
        proj.share_name,
        *proj.base_dir.split("/"),
        *proj.sub_dir.split("/"),
        "Main_BuildMachine",
        str(cl_number),
    ]
    return "/".join(parts)


def all_ancestor_keys(full_key: str) -> list[str]:
    """
    Return every ancestor path key for a given full_path_key, including
    itself, ordered from root to leaf.

    Used to enumerate all path_node rows that need to exist before
    the pipeline's path_closure build step can run.

    Example:
      "a/b/c/d"  →  ["a", "a/b", "a/b/c", "a/b/c/d"]
    """
    parts = full_key.split("/")
    return ["/".join(parts[:i]) for i in range(1, len(parts) + 1)]


def depth_of_key(full_key: str) -> int:
    """Return 0-based depth of a path key (root node = 0)."""
    return full_key.count("/")


# ============================================================
# SECTION 6 — ISSUE INSTANCE GENERATOR
#
# Produces a fixed pool of unique IssueInstance objects per project.
# These are the rows that will later become issue_instance table entries.
#
# Uniqueness guarantee: every instance gets a distinct asset number,
# producing a distinct (norm_issue_text | norm_asset_path) pair, which
# means a distinct SHA-256 signature — matching the pipeline's own
# signature logic exactly.
#
# Issues are distributed across types by ISSUE_TYPES weights, and
# each instance is pinned to a specific PathSlot and contact so that
# the same issue always appears under the same folder path across CLs.
# ============================================================

def _norm_text(s: str) -> str:
    """Normalise issue text identically to the pipeline's norm_issue()."""
    return re.sub(r"\s+", " ", s.strip().lower())


def _norm_asset_path(p: str) -> str:
    """Normalise asset path identically to the pipeline's norm_asset_path()."""
    p = p.replace("\\", "/")
    p = re.sub(r"/{2,}", "/", p)
    p = p.lower()
    p = re.sub(r"^[a-z]:", "", p)   # strip Windows drive letter
    return p.lstrip("/")


def _signature(issue_norm: str, asset_norm: str) -> str:
    """SHA-256 of 'issue_norm|asset_norm' — same formula as the pipeline."""
    raw = f"{issue_norm}|{asset_norm}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


@dataclass
class IssueInstance:
    """One unique issue — becomes one row in the issue_instance table."""
    issue_signature: str
    project_id: str
    issue_type: str          # tag, e.g. "assetLoad"
    issue_pattern: str       # rendered issue_text (stable across CLs)
    error_pattern: str       # rendered error_text (stable across CLs)
    missing_token: str       # extracted token, stored in issue_instance.missing_token
    asset_path: str          # normalised path, e.g. "game/team-art/characters/da_0042"
    asset_name: str          # stem with prefix, e.g. "DA_0042"
    asset_filename: str      # with extension, e.g. "DA_0042.uasset"
    slot: PathSlot           # hierarchy location (issue_type/team/subdir)
    contact: str             # owner folder — level_4 in the hierarchy


def generate_issue_instances(
    project_id: str,
    n_issues: int,
    number_offset: int = 0,
) -> list[IssueInstance]:
    """
    Generate n_issues unique IssueInstance objects for one project.

    Args:
        project_id:     which project these issues belong to
        n_issues:       how many unique instances to create
        number_offset:  added to the asset sequence number so that different
                        projects produce distinct asset names and signatures
                        (avoids accidental signature collisions between projects)

    Returns:
        List of IssueInstance, each with a unique issue_signature.
    """
    _, type_weights = build_issue_type_weights()
    type_tags = [it.tag for it in ISSUE_TYPES]
    type_cfg_map: dict[str, IssueTypeConfig] = {it.tag: it for it in ISSUE_TYPES}

    # PathSlots pre-grouped by issue type for fast lookup
    slots_by_type: dict[str, list[PathSlot]] = {}
    for slot in ALL_PATH_SLOTS:
        slots_by_type.setdefault(slot.issue_type, []).append(slot)

    # Determine how many issues of each type to create, respecting weights
    sampled_tags: list[str] = random.choices(type_tags, weights=type_weights, k=n_issues)
    type_counts: dict[str, int] = {}
    for tag in sampled_tags:
        type_counts[tag] = type_counts.get(tag, 0) + 1

    instances: list[IssueInstance] = []

    for tag, count in type_counts.items():
        cfg = type_cfg_map[tag]
        available_slots = slots_by_type[tag]

        for i in range(1, count + 1):
            seq = i + number_offset
            asset_stem     = f"{cfg.asset_prefix}{seq:04d}"
            asset_filename = f"{asset_stem}{cfg.asset_ext}"
            slot           = random.choice(available_slots)
            contact        = random.choice(CONTACTS)

            # Normalised asset path mirrors what the pipeline produces from
            # a raw Windows-style path like /Game/team-art/Characters/DA_0042
            asset_path_raw  = f"/Game/{slot.team}/{slot.subdir}/{asset_stem}"
            asset_path_norm = _norm_asset_path(asset_path_raw)

            issue_text, error_text, token = render_issue_text(
                tag, asset_path_raw, asset_stem
            )

            sig = _signature(_norm_text(issue_text), asset_path_norm)

            instances.append(IssueInstance(
                issue_signature=sig,
                project_id=project_id,
                issue_type=tag,
                issue_pattern=issue_text,
                error_pattern=error_text,
                missing_token=token,
                asset_path=asset_path_norm,
                asset_name=asset_stem,
                asset_filename=asset_filename,
                slot=slot,
                contact=contact,
            ))

    return instances


def issues_per_project(total_issues: int, cl_counts: dict[str, int]) -> dict[str, int]:
    """
    Split total_issues across projects proportionally to each project's CL count.

    More CLs → more opportunity for issues to appear → more unique issues needed
    to produce interesting presence-interval data.  Each project gets at least 20.
    """
    total_cls = sum(cl_counts.values())
    result: dict[str, int] = {}
    allocated = 0
    projects = list(cl_counts.keys())

    for proj_id in projects[:-1]:
        share = max(20, round(total_issues * cl_counts[proj_id] / total_cls))
        result[proj_id] = share
        allocated += share

    # Last project gets the remainder so the total is exact
    result[projects[-1]] = max(20, total_issues - allocated)
    return result


# ============================================================
# SECTION 7 — CL TIMELINE GENERATOR
#
# Produces an ordered list of CLEntry objects per project.
# CL numbers are sequential integers; dates advance by a random
# 1–3 day gap to simulate a realistic build cadence.
#
# proj-beta uses a separate CL number space (offset by 10 000) so
# that the two projects' cl_numbers are visually distinguishable
# in queries, matching how separate VCS branches work in practice.
# ============================================================

CL_NUMBER_OFFSET_PER_PROJECT: dict[str, int] = {
    proj.project_id: idx * 10_000
    for idx, proj in enumerate(PROJECTS)
}


@dataclass
class CLEntry:
    """One changelist — becomes one row in the changelist table."""
    project_id: str
    cl_number: int
    cl_date: date
    cl_date_str: str     # "YYYY/MM/DD" — value written to issues_raw.last_edit


def generate_cl_timeline(project_id: str, n_cls: int) -> list[CLEntry]:
    """
    Generate n_cls CLEntry objects for a project.

    CL numbers start at CL_BASE_NUMBER + project offset and increment by 1.
    Dates start at TIMELINE_START_DATE and advance CL_GAP_DAYS_MIN–MAX days
    between each CL.
    """
    offset     = CL_NUMBER_OFFSET_PER_PROJECT.get(project_id, 0)
    start_num  = CL_BASE_NUMBER + offset
    current_dt = TIMELINE_START_DATE

    entries: list[CLEntry] = []
    for i in range(n_cls):
        entries.append(CLEntry(
            project_id=project_id,
            cl_number=start_num + i,
            cl_date=current_dt,
            cl_date_str=current_dt.strftime("%Y/%m/%d"),
        ))
        current_dt += timedelta(days=random.randint(CL_GAP_DAYS_MIN, CL_GAP_DAYS_MAX))

    return entries


# ============================================================
# SECTION 8 — PRESENCE PATTERN GENERATOR
#
# Assigns each IssueInstance a personal appearance rate and uses it
# to decide which CLs the issue appears in.
#
# Three presence profiles drive different shapes in the downstream
# issue_presence_interval table:
#   persistent   → one long open interval across almost all CLs
#   intermittent → multiple open/close cycles (most interesting for queries)
#   transient    → one short closed interval, appears then vanishes
#
# Every issue is guaranteed ≥1 appearance so no orphan instances exist.
# ============================================================


@dataclass
class IssuePresence:
    """
    Pairs an IssueInstance with the CL indices (0-based positions into
    the project's CLEntry list) where it appears.
    """
    issue: IssueInstance
    appearing_cl_indices: list[int]   # sorted, 0-based into the timeline
    profile_name: str                  # which presence profile was assigned


def assign_presence_profiles(
    issues: list[IssueInstance],
    n_cls: int,
) -> list[IssuePresence]:
    """
    Assign a presence profile and appearance pattern to every IssueInstance.

    For each issue:
      1. Pick a profile by PRESENCE_PROFILES ratios.
      2. Draw a personal appearance rate uniformly from the profile's range.
         (Personal rate varies per issue so no two issues are identical.)
      3. For each CL index, include it with probability = personal rate.
      4. Guarantee at least one appearance.

    Args:
        issues:  list of IssueInstance for one project
        n_cls:   number of CLs in the project's timeline

    Returns:
        List of IssuePresence, one per issue, in the same order as issues.
    """
    profile_names   = [p.name for p in PRESENCE_PROFILES]
    profile_weights = [p.ratio for p in PRESENCE_PROFILES]
    profile_map     = {p.name: p for p in PRESENCE_PROFILES}

    result: list[IssuePresence] = []

    for issue in issues:
        profile_name = random.choices(profile_names, weights=profile_weights, k=1)[0]
        profile      = profile_map[profile_name]

        # Personal rate — varies within profile bounds so every issue is unique
        rate = random.uniform(profile.min_appearance_rate, profile.max_appearance_rate)

        # Independent Bernoulli draw per CL
        appearing = [i for i in range(n_cls) if random.random() < rate]

        # Guarantee at least one appearance (prevents orphan issue_instance rows)
        if not appearing:
            appearing = [random.randint(0, n_cls - 1)]

        result.append(IssuePresence(
            issue=issue,
            appearing_cl_indices=sorted(appearing),
            profile_name=profile_name,
        ))

    return result


def presence_summary(presences: list[IssuePresence], n_cls: int) -> dict[str, Any]:
    """
    Return aggregate stats about a presence assignment for smoke-testing.

    Keys: total_issues, profile_counts, avg_cl_appearances,
          pct_open_at_last_cl, interval_count_estimate
    """
    profile_counts: dict[str, int] = {}
    total_appearances = 0
    open_at_last = 0
    interval_est = 0

    for p in presences:
        profile_counts[p.profile_name] = profile_counts.get(p.profile_name, 0) + 1
        total_appearances += len(p.appearing_cl_indices)
        if (n_cls - 1) in p.appearing_cl_indices:
            open_at_last += 1
        # Rough interval count: each gap in appearances = one close + one open
        indices = p.appearing_cl_indices
        gaps = sum(1 for a, b in zip(indices, indices[1:]) if b - a > 1)
        interval_est += gaps + 1  # at least 1 interval per issue

    return {
        "total_issues":         len(presences),
        "profile_counts":       profile_counts,
        "avg_cl_appearances":   round(total_appearances / max(len(presences), 1), 1),
        "pct_open_at_last_cl":  round(100 * open_at_last / max(len(presences), 1), 1),
        "interval_count_est":   interval_est,
    }


# ============================================================
# SECTION 9 — ROW ASSEMBLER
#
# Flattens the generator output into plain dicts that match the
# issues_raw schema column-for-column.  This is the only place
# that knows about the column names — all other sections deal in
# typed dataclasses.
#
# Column order matches the original notebook's df.dtypes output:
#   issue, error, tag, contact, asset, asset_path,
#   last_edit, build_machine_path, refered_to_by, cl
#
# Signature-consistency note
# ──────────────────────────
# The pipeline's norm_asset_path() strips drive letters and leading
# slashes but does NOT strip extensions.  To keep the pipeline's
# computed issue_signature identical to the one stored in
# IssueInstance.issue_signature we write asset_path WITHOUT the
# file extension.  Example:
#   written to issues_raw : /Game/team-art/Characters/DA_0042
#   pipeline normalises to: game/team-art/characters/da_0042   ✓
#   generator computed    : game/team-art/characters/da_0042   ✓
# ============================================================

# Column order expected by the pipeline scripts (must not change)
_ISSUES_RAW_COLUMNS: list[str] = [
    "project_id",          # added so pipeline scripts can filter per-project
    "issue",
    "error",
    "tag",
    "contact",
    "asset",
    "asset_path",
    "last_edit",
    "build_machine_path",
    "refered_to_by",
    "cl",
]


def assemble_issues_raw_rows(
    proj: ProjectConfig,
    presences: list[IssuePresence],
    timeline: list[CLEntry],
) -> list[dict[str, str]]:
    """
    Produce one dict per (issue, CL appearance) pair.

    Each dict has exactly the columns in _ISSUES_RAW_COLUMNS so it can
    be passed directly to pd.DataFrame() and then to_sql().

    Args:
        proj:      the ProjectConfig the rows belong to
        presences: list of IssuePresence for this project
        timeline:  ordered list of CLEntry for this project

    Returns:
        List of dicts ready to be written to issues_raw.
    """
    rows: list[dict[str, str]] = []

    for presence in presences:
        inst = presence.issue
        # Raw asset_path (no extension) keeps signature consistent with pipeline
        asset_path_raw = f"/Game/{inst.slot.team}/{inst.slot.subdir}/{inst.asset_name}"

        for cl_idx in presence.appearing_cl_indices:
            cl_entry = timeline[cl_idx]
            bmp = make_build_machine_path(
                proj,
                cl_entry.cl_number,
                inst.slot,
                inst.contact,
                inst.asset_filename,
            )
            rows.append({
                "project_id":         inst.project_id,
                "issue":              inst.issue_pattern,
                "error":              inst.error_pattern,
                "tag":                inst.issue_type,
                "contact":            inst.contact,
                "asset":              inst.asset_filename,
                "asset_path":         asset_path_raw,
                "last_edit":          cl_entry.cl_date_str,
                "build_machine_path": bmp,
                "refered_to_by":      "",
                "cl":                 str(cl_entry.cl_number),
            })

    return rows


# ============================================================
# SECTION 10 — POSTGRES WRITER
#
# Reads PG_DSN from the environment, assembles a single DataFrame
# from all projects, and writes it to issue_tracker.issues_raw
# using pandas to_sql (same mechanism as the original notebook).
#
# if_exists='replace' drops and recreates issues_raw on every run,
# matching the original notebook and making reruns idempotent.
#
# The downstream pipeline scripts read from issues_raw and are
# completely unaffected by how it was populated.
# ============================================================

def _get_engine():
    """Build a SQLAlchemy engine from the PG_DSN environment variable."""
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError(
            "PG_DSN environment variable is not set.\n"
            "Example:\n"
            "  export PG_DSN=postgresql://user:pass@localhost:5432/devops\n"
            "Then re-run the generator."
        )
    return create_engine(dsn)


def write_to_postgres(
    all_presences: dict[str, list[IssuePresence]],
    all_timelines: dict[str, list[CLEntry]],
) -> int:
    """
    Assemble all rows and write them to issue_tracker.issues_raw.

    Creates the issue_tracker schema if it does not already exist so
    the generator can run against a freshly initialised Postgres instance
    (before the DDL scripts have been applied).

    Args:
        all_presences: {project_id: [IssuePresence, ...]}
        all_timelines: {project_id: [CLEntry, ...]}

    Returns:
        Total number of rows written.
    """
    engine = _get_engine()
    proj_map: dict[str, ProjectConfig] = {p.project_id: p for p in PROJECTS}

    all_rows: list[dict[str, str]] = []
    for pid, presences in all_presences.items():
        proj     = proj_map[pid]
        timeline = all_timelines[pid]
        all_rows.extend(assemble_issues_raw_rows(proj, presences, timeline))

    df = pd.DataFrame(all_rows, columns=_ISSUES_RAW_COLUMNS)

    # Ensure schema exists and project rows are present before pipeline runs
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS issue_tracker"))
        for proj in PROJECTS:
            conn.execute(text("""
                INSERT INTO issue_tracker.project (project_id, project_key, project_name)
                VALUES (:pid, :pkey, :pname)
                ON CONFLICT (project_id) DO UPDATE
                  SET project_key  = EXCLUDED.project_key,
                      project_name = EXCLUDED.project_name
            """), {"pid": proj.project_id, "pkey": proj.project_key, "pname": proj.project_name})
        conn.commit()

    df.to_sql(
        "issues_raw",
        engine,
        schema="issue_tracker",
        if_exists="replace",
        index=False,
    )

    return len(df)


# ============================================================
# SECTION 11 — ENTRY POINT
# Wires all generators together and handles CLI parsing.
# ============================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic issues_raw data and write to Postgres."
    )
    parser.add_argument(
        "--cls",
        type=int,
        default=DEFAULT_CLS_PER_PROJECT,
        metavar="N",
        help=f"Changelists to generate for the primary project (default: {DEFAULT_CLS_PER_PROJECT})",
    )
    parser.add_argument(
        "--issues",
        type=int,
        default=DEFAULT_ISSUE_COUNT,
        metavar="N",
        help=f"Target unique issue count across all projects (default: {DEFAULT_ISSUE_COUNT})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for reproducibility (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print expected row counts without writing to the database.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    cl_counts = cl_counts_per_project(args.cls)

    print("Generator config:")
    print(f"  seed          : {args.seed}")
    print(f"  dry_run       : {args.dry_run}")
    for proj in PROJECTS:
        print(f"  {proj.project_id:<14}: {cl_counts[proj.project_id]} CLs")
    print(f"  target issues : {args.issues}")

    # Path hierarchy smoke-check
    sample_proj = PROJECTS[0]
    sample_slot = ALL_PATH_SLOTS[0]
    sample_contact = CONTACTS[6]
    sample_asset = "DA_Sword_01.uasset"
    sample_cl = CL_BASE_NUMBER

    print("\nPath hierarchy sample:")
    print(f"  total slots   : {len(ALL_PATH_SLOTS)}")
    print(f"  build_machine_path :")
    print(f"    {make_build_machine_path(sample_proj, sample_cl, sample_slot, sample_contact, sample_asset)}")
    print(f"  container_key :")
    print(f"    {container_key(sample_proj, sample_cl, sample_slot, sample_contact)}")
    print(f"  cl_root_key   :")
    print(f"    {cl_root_key(sample_proj, sample_cl)}")
    sample_ck = container_key(sample_proj, sample_cl, sample_slot, sample_contact)
    print(f"  depth         : {depth_of_key(sample_ck)}")
    print(f"  ancestors ({len(all_ancestor_keys(sample_ck))} nodes):")
    for k in all_ancestor_keys(sample_ck):
        print(f"    [{depth_of_key(k)}] {k}")

    # ---- Issue instances
    issue_counts = issues_per_project(args.issues, cl_counts)
    all_issues: dict[str, list[IssueInstance]] = {}
    offset = 0
    for proj in PROJECTS:
        pid = proj.project_id
        n   = issue_counts[pid]
        all_issues[pid] = generate_issue_instances(pid, n, number_offset=offset)
        offset += n   # keep asset numbers distinct across projects

    print("\nIssue instances:")
    for proj in PROJECTS:
        pid      = proj.project_id
        by_type  = {}
        for inst in all_issues[pid]:
            by_type[inst.issue_type] = by_type.get(inst.issue_type, 0) + 1
        print(f"  {pid}: {len(all_issues[pid])} instances  {by_type}")
    # Sample rendered text
    sample_inst = all_issues[PROJECTS[0].project_id][0]
    print(f"\n  sample issue  : {sample_inst.issue_pattern}")
    print(f"  sample error  : {sample_inst.error_pattern}")
    print(f"  sample sig    : {sample_inst.issue_signature[:16]}...")
    print(f"  sample path   : {sample_inst.asset_path}")

    # ---- CL timelines
    all_timelines: dict[str, list[CLEntry]] = {}
    for proj in PROJECTS:
        pid = proj.project_id
        all_timelines[pid] = generate_cl_timeline(pid, cl_counts[pid])

    print("\nCL timelines:")
    for proj in PROJECTS:
        pid      = proj.project_id
        timeline = all_timelines[pid]
        print(
            f"  {pid}: {len(timeline)} CLs  "
            f"[{timeline[0].cl_number} {timeline[0].cl_date_str}] → "
            f"[{timeline[-1].cl_number} {timeline[-1].cl_date_str}]"
        )

    # ---- Presence patterns
    all_presences: dict[str, list[IssuePresence]] = {}
    for proj in PROJECTS:
        pid = proj.project_id
        all_presences[pid] = assign_presence_profiles(
            all_issues[pid], cl_counts[pid]
        )

    print("\nPresence patterns:")
    for proj in PROJECTS:
        pid   = proj.project_id
        stats = presence_summary(all_presences[pid], cl_counts[pid])
        print(f"  {pid}:")
        print(f"    profiles          : {stats['profile_counts']}")
        print(f"    avg CL appearances: {stats['avg_cl_appearances']}")
        print(f"    open at last CL   : {stats['pct_open_at_last_cl']}%")
        print(f"    est. intervals    : {stats['interval_count_est']}")

    # Total issues_raw rows = sum of appearances across all issues and projects
    total_rows = sum(
        len(p.appearing_cl_indices)
        for plist in all_presences.values()
        for p in plist
    )
    print(f"\n  issues_raw rows to write: {total_rows}")

    if args.dry_run:
        print("  dry-run: no database writes performed.")
    else:
        print("\nWriting to Postgres...")
        written = write_to_postgres(all_presences, all_timelines)
        print(f"  done — {written} rows written to issue_tracker.issues_raw")


if __name__ == "__main__":
    main()
