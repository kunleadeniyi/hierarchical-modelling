# Synthetic Data Generator

`generate_data.py` produces fake-but-realistic issue records that match the `issues_raw` schema and writes them directly to Postgres. The downstream pipeline scripts (`01`–`04`) then run unchanged against this synthetic data.

No real project data is used or required. This generator exists so the repository can be made public without exposing proprietary build-system output.

---

## How it fits in the pipeline

```
generate_data.py
      │
      ▼
issues_raw  (staging table)
      │
      ├── 00_populate_path_nodes.py  →  path_node, path_closure
      ├── 01_ingest_changelists.py   →  changelist
      ├── 02_ingest_issue_instances.py → issue_instance
      ├── 03_ingest_observations.py  →  snapshot, issue_observation
      └── 04_build_presence_intervals.py → issue_presence_interval
```

`issues_raw` acts as a staging area: the generator fills it, and each pipeline script reads from it to populate the normalised core tables. Nothing in the pipeline scripts is aware that the data is synthetic.

---

## Prerequisites

- Python 3.10+
- `pip install pandas sqlalchemy psycopg2-binary` (or `pip install -r requirements.txt` from the project root)
- Postgres running with the schema already applied (`postgres/sql/01_schema.sql`)
- `PG_DSN` environment variable set (see below)

---

## Usage

```bash
export PG_DSN=postgresql://user:pass@localhost:5432/devops

# Run with defaults  (85 CLs for proj-alpha, ~650 unique issues, seed 42)
python3 seed/generate_data.py

# Smaller dataset for quick iteration
python3 seed/generate_data.py --cls 20 --issues 200 --seed 7

# Preview expected row counts without writing anything to Postgres
python3 seed/generate_data.py --dry-run
```

Expected output (default settings):

```
[proj-alpha] 85 changelists, 488 issue instances
[proj-beta]  26 changelists, 162 issue instances
issues_raw: 22745 rows written
```

---

## CLI Parameters

| Flag | Default | Description |
|---|---|---|
| `--cls N` | `85` | Changelists to generate for the primary project (`proj-alpha`). The secondary project receives 30% of this count (minimum 10). |
| `--issues N` | `650` | Target number of unique issue instances across all projects. Actual count may differ slightly due to signature de-duplication. |
| `--seed N` | `42` | Random seed. Any fixed seed produces an identical, reproducible dataset. |
| `--dry-run` | off | Prints expected row counts and exits without writing to Postgres. |

---

## Output: `issues_raw` columns

Each row written to `issue_tracker.issues_raw` maps to the following columns:

| Column | Type | Example | Notes |
|---|---|---|---|
| `project_id` | TEXT | `proj-alpha` | FK to `project.project_id` |
| `issue` | TEXT | `LogLinker: Warning: Failed to find object '/Script/ModioUICore'` | Rendered from a type-specific template |
| `error` | TEXT | `@blueprint:LogLinker: Warning: ...` | Rendered error log line |
| `tag` | TEXT | `assetLoad` | Issue type category |
| `contact` | TEXT | `user-007` | Coded owner identifier, no real names |
| `asset` | TEXT | `DA_Sword_01` | Asset filename (leaf, no extension) |
| `asset_path` | TEXT | `game/team-art/characters/da_sword_01` | Normalised path (lowercase, forward-slash, no drive letter) |
| `last_edit` | TEXT | `2024/03/14` | Date string in `YYYY/MM/DD` format |
| `build_machine_path` | TEXT | `\\bldsvr-01\builds$\DevOps\...\Main_BuildMachine\10024\assetLoad\team-art\Characters\DA_Sword_01.uasset` | Full UNC-style path (backslash-separated); the pipeline extracts the path hierarchy from this |
| `refered_to_by` | TEXT | _(blank)_ | Optional upstream reference; left empty by the generator |
| `cl` | TEXT | `10024` | Changelist number as a string |

The `build_machine_path` column is the key structural input. Pipeline script `00_populate_path_nodes.py` splits it on `\` to populate the `path_node` and `path_closure` hierarchy tables.

---

## Config Block Reference

The top of `generate_data.py` is divided into numbered config sections. All dataset tuning happens here — the generator logic below reads from these constants.

| Section | Key constants | Purpose |
|---|---|---|
| `1.1 ProjectConfig` | `PROJECTS` | Two project definitions (`proj-alpha`, `proj-beta`), each with a fake build-server hostname, share name, and base directory used to construct realistic UNC paths. |
| `1.2 Volume controls` | `DEFAULT_CLS_PER_PROJECT`, `BETA_CL_FRACTION`, `DEFAULT_ISSUE_COUNT`, `AVG_OBSERVATIONS_PER_CL`, `MAX_OBSERVATIONS_PER_CL` | Controls overall dataset size. `proj-beta` is intentionally smaller (30% of CLs) to simulate a secondary project. Average 18 observations per CL; capped at 60. |
| `1.3 Reproducibility` | `DEFAULT_SEED` | Single seed passed to `random.seed()` before any generation begins. Change it to get a different-but-stable dataset. |
| `1.4 Timeline` | `CL_BASE_NUMBER`, `TIMELINE_START_DATE`, `CL_GAP_DAYS_MIN/MAX` | CL numbers start at `10001` and increment by 1. Dates start at `2024-01-08` with a 1–3 day gap between CLs, producing a realistic build cadence. |
| `1.5 Path structure` | `TEAMS`, `SUBDIRS_BY_TEAM` | Six team folders (`team-art`, `team-audio`, `team-code`, `team-vfx`, `team-ui`, `team-env`), each with four sub-directories, forming the deep path segments inside `build_machine_path`. |
| `1.6 IssueTypeConfig` | `ISSUE_TYPES` | Four issue types with relative frequency weights (see table below). Each type has an associated asset extension and asset name prefix. |
| `1.7 PresenceProfile` | `PRESENCE_PROFILES` | Three profiles that control how often an issue appears across CLs (see table below). |
| `1.8 Contacts` | `CONTACTS` | Twenty coded user identifiers (`user-001` … `user-020`) used as the `contact` / owner column. No real names. |

---

## Path Hierarchy Structure

Each `build_machine_path` value is a UNC-style path with a fixed structure:

```
\\<server>\<share>\<base_dir>\<sub_dir>\Main_BuildMachine\<cl_number>\<issue_type>\<team>\<subdir>\<asset_file>
```

Example:

```
\\bldsvr-01\builds$\DevOps\IssueTracker\alphabuild\Alpha\Main_BuildMachine\10024\assetLoad\team-art\Characters\DA_Sword_01.uasset
```

| Segment | Example | Notes |
|---|---|---|
| Server | `bldsvr-01` | Fake build-server hostname |
| Share | `builds$` | Fake network share |
| Base dir | `DevOps\IssueTracker` | Fixed per project |
| Sub dir | `alphabuild\Alpha` | Fixed per project |
| `Main_BuildMachine` | — | Structural anchor; used by `cl_root_key()` to locate the CL segment |
| CL number | `10024` | Links this observation to a specific changelist |
| Issue type | `assetLoad` | Matches `tag` column |
| Team | `team-art` | One of six teams |
| Sub-directory | `Characters` | Four options per team |
| Asset file | `DA_Sword_01.uasset` | Leaf node; contains asset prefix, serial number, and type extension |

The pipeline's `norm_container()` function strips the asset leaf and uses the remainder as the `path_node` lookup key.

---

## Issue Type Distribution

| Tag | Weight | Approx. frequency | Asset ext | Asset prefix |
|---|---|---|---|---|
| `assetLoad` | 0.45 | ~45% | `.uasset` | `DA_` |
| `audioMissing` | 0.25 | ~25% | `.uasset` | `AK_` |
| `shaderCompile` | 0.20 | ~20% | `.usf` | `M_` |
| `scriptError` | 0.10 | ~10% | `.uasset` | `BP_` |

Weights are relative and normalised internally — they do not need to sum to 1. Change any weight to shift the distribution without touching the others.

---

## Issue Presence Profiles

Each generated issue is assigned one of three profiles at creation time. The profile determines the per-CL probability that the issue appears in `issues_raw`, which drives the shape of `issue_presence_interval` rows downstream.

| Profile | Share of issues | Appearance rate per CL | Downstream effect |
|---|---|---|---|
| `persistent` | 30% | 75–100% | Appears in almost every CL; produces one long open interval that typically remains open at the latest CL |
| `intermittent` | 50% | 20–65% | Appears and disappears across CLs; produces multiple open/close intervals per issue |
| `transient` | 20% | 1–15% | Appears in very few CLs then vanishes; produces short closed intervals |

The mix of profiles is what makes the presence interval data interesting: a reviewer can query "which issues have the most open/close cycles?" and get a meaningful answer.

---

## Issue Template System

Each issue type has a set of `issue_text` and `error_text` templates with `{placeholder}` slots. At render time, `render_issue_text()` fills the slots from type-specific token pools:

| Issue type | Token pool | Placeholder used |
|---|---|---|
| `assetLoad` | Unreal script package paths (e.g. `/Script/ModioUICore`) | `{token}` |
| `audioMissing` | Wwise audio cue names (e.g. `AK_SFX_Explosion_Large`) | `{token}` |
| `shaderCompile` | Render pass names (e.g. `BasePass`, `ShadowDepth`) | `{token}`, `{pass_name}` |
| `scriptError` | Blueprint class fragments (e.g. `EnemyBase`, `QuestManager`) | `{token}`, `{class_name}` |

Each type has 5 `issue_text` variants and 5 `error_text` variants, so repeated sampling across 650 issues produces varied but recognisably typed messages.

---

## Helper Functions

### `build_issue_type_weights() -> tuple[list[str], list[float]]`

Returns two parallel lists — `(tags, normalised_weights)` — ready to pass directly to `random.choices()`.

Reads `ISSUE_TYPES`, extracts the `tag` and `weight` from each entry, and divides each weight by the total so they sum to exactly 1.0. This means you can change any raw `weight` value in config without manually rebalancing the others.

```python
tags, weights = build_issue_type_weights()
chosen_tag = random.choices(tags, weights=weights, k=1)[0]
```

### `cl_counts_per_project(n_cls: int) -> dict[str, int]`

Takes the primary project's target CL count and returns a `{project_id: count}` mapping for every project in `PROJECTS`.

The first project receives `n_cls` exactly. Every subsequent project receives `max(10, round(n_cls * BETA_CL_FRACTION))`, ensuring secondary projects are proportionally smaller but always have at least 10 CLs (enough to produce meaningful presence interval data).

```python
cl_counts = cl_counts_per_project(85)
# {"proj-alpha": 85, "proj-beta": 26}
```

This is what allows `--cls` to scale both projects in proportion with a single flag.
