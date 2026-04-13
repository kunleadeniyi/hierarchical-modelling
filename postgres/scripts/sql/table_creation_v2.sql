-- setting default schema
show search_path;
set search_path = issue_tracker;

-- ============================================================
-- Issue Tracker DevOps - Postgres schema (recommended tables)
-- ============================================================

-- ----------------------------
-- 0) Projects (recommended)
-- ----------------------------
DROP TABLE IF EXISTS project cascade;
CREATE table if not EXISTS project (
  project_id        VARCHAR PRIMARY KEY,
  project_key       TEXT NOT NULL UNIQUE,         -- e.g. 'alpha'
  project_name      TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------
-- 1) Path hierarchy
-- ----------------------------

-- Optional: classify common node kinds (purely for convenience)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'path_node_type') THEN
    CREATE TYPE path_node_type AS ENUM (
      'root',
      'build_machine',
      'changelist',
      'folder',
      'file',
      'unknown'
    );
  END IF;
END$$;

drop table if exists  path_node cascade ;
CREATE TABLE IF NOT EXISTS path_node (
  node_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id        VARCHAR NOT NULL REFERENCES project(project_id) ON DELETE CASCADE,
  parent_node_id    BIGINT NULL REFERENCES path_node(node_id) ON DELETE CASCADE,
  node_name         TEXT NOT NULL,                -- e.g. 'Main_BuildMachine', '10024', 'assetLoad', 'team-art', 'user-007'
  node_type         path_node_type NOT NULL DEFAULT 'unknown',
  depth             INTEGER NOT NULL CHECK (depth >= 0),
  -- Helpful for de-dupe / debugging / direct prefix searches; not required for rollups.
  full_path_key     TEXT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Prevent duplicate siblings with same name under the same parent in a project.
  CONSTRAINT uq_path_node_sibling UNIQUE (project_id, parent_node_id, node_name),
  constraint uq_full_path_key unique (full_path_key)
);

CREATE INDEX IF NOT EXISTS ix_path_node_project_parent ON path_node(project_id, parent_node_id);
CREATE INDEX IF NOT EXISTS ix_path_node_project_name   ON path_node(project_id, node_name);
CREATE INDEX IF NOT EXISTS ix_path_node_full_path_key  ON path_node(full_path_key);

-- Closure table: fast ancestor/descendant rollups at any depth
CREATE TABLE IF NOT EXISTS path_closure (
  ancestor_node_id   BIGINT NOT NULL REFERENCES path_node(node_id) ON DELETE CASCADE,
  descendant_node_id BIGINT NOT NULL REFERENCES path_node(node_id) ON DELETE CASCADE,
  distance           INTEGER NOT NULL CHECK (distance >= 0),
  PRIMARY KEY (ancestor_node_id, descendant_node_id)
);

CREATE INDEX IF NOT EXISTS ix_path_closure_descendant ON path_closure(descendant_node_id);
CREATE INDEX IF NOT EXISTS ix_path_closure_ancestor_distance ON path_closure(ancestor_node_id, distance);

-- ----------------------------
-- 2) Changelists + snapshots
-- ----------------------------
drop table if exists changelist cascade;
CREATE TABLE IF NOT EXISTS changelist (
  changelist_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id         VARCHAR NOT NULL REFERENCES project(project_id) ON DELETE CASCADE,
  cl_number          INTEGER NOT NULL CHECK (cl_number > 0),   -- e.g. 10024 (monotonic per project)
  created_at         TIMESTAMPTZ NULL,                         -- when CL was created (added later)
  first_seen_at      TIMESTAMPTZ NOT NULL DEFAULT now(),       -- when your pipeline first observed it
  -- Node corresponding to "...Main_BuildMachine/<cl_number>"
  changelist_node_id BIGINT NULL REFERENCES path_node(node_id) ON DELETE SET NULL,

  CONSTRAINT uq_changelist_per_project UNIQUE (project_id, cl_number)
);

CREATE INDEX IF NOT EXISTS ix_changelist_project_clnum ON changelist(project_id, cl_number);

-- Snapshot = one ingestion/run for a changelist (useful if you ingest same CL multiple times)
drop table if exists "snapshot" cascade ;
CREATE TABLE IF NOT EXISTS snapshot (
  snapshot_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id     VARCHAR NOT NULL REFERENCES project(project_id) ON DELETE CASCADE,
  changelist_id  BIGINT NOT NULL REFERENCES changelist(changelist_id) ON DELETE CASCADE,

  observed_at    TIMESTAMPTZ NOT NULL,            -- when this snapshot represents (ingest time or derived)
  ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Optional: track pipeline/source info
  source_name    TEXT NULL,
  source_ref     TEXT NULL,

  -- Prevent accidental duplicates for the same CL at the exact same time
  CONSTRAINT uq_snapshot_unique UNIQUE (changelist_id, observed_at)
);

CREATE INDEX IF NOT EXISTS ix_snapshot_project_time ON snapshot(project_id, observed_at);
CREATE INDEX IF NOT EXISTS ix_snapshot_changelist   ON snapshot(changelist_id);

-- ----------------------------
-- 3) Issue identity: family + instance
-- ----------------------------

-- Optional grouping level: "root cause" signature (e.g. missing dependency token)
CREATE TABLE IF NOT EXISTS issue_family (
  issue_family_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id         VARCHAR NOT NULL REFERENCES project(project_id) ON DELETE CASCADE,

  family_signature   TEXT NOT NULL,               -- stable fingerprint you choose
  issue_type         TEXT NULL,                   -- e.g. 'assetLoad'
  title              TEXT NULL,                   -- optional human-friendly label

  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_issue_family UNIQUE (project_id, family_signature)
);

CREATE INDEX IF NOT EXISTS ix_issue_family_project_type ON issue_family(project_id, issue_type);

-- Primary identity level used for "new/resolved" diffs across changelist
drop table if exists issue_instance cascade ;
CREATE TABLE IF NOT EXISTS issue_instance (
  issue_instance_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id         VARCHAR NOT NULL REFERENCES project(project_id) ON DELETE CASCADE,

  -- Your stable identity key (hash or canonical concatenated string)
  issue_signature    TEXT NOT NULL,

  -- Common dimensions (store whatever is stable enough for reporting)
  issue_type         TEXT NOT NULL,               -- e.g. 'assetLoad'
  issue_pattern      TEXT NULL,                   -- e.g. normalized "Failed to load 'X': Can't find file."
  missing_token      TEXT NULL,                   -- e.g. "/Script/ModioUICore" or "/Game/.../AK_..."
  asset_path         TEXT NULL,                   -- asset this issue is tied to (if part of signature)
  asset_name         TEXT NULL,

  issue_family_id    BIGINT NULL REFERENCES issue_family(issue_family_id) ON DELETE SET NULL,

  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT uq_issue_instance UNIQUE (project_id, issue_signature)
);

CREATE INDEX IF NOT EXISTS ix_issue_instance_project_type ON issue_instance(project_id, issue_type);
CREATE INDEX IF NOT EXISTS ix_issue_instance_family       ON issue_instance(issue_family_id);

-- ----------------------------
-- 4) Observations (facts)
-- ----------------------------

-- One row per (snapshot, issue_instance, hierarchy location) observation.
drop table if exists issue_observation cascade ;
CREATE TABLE IF NOT EXISTS issue_observation (
  observation_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

  snapshot_id        BIGINT NOT NULL REFERENCES snapshot(snapshot_id) ON DELETE CASCADE,
  issue_instance_id  BIGINT NOT NULL REFERENCES issue_instance(issue_instance_id) ON DELETE CASCADE,

  -- Where in the hierarchy it was observed (leaf node from build_machine_path, or a chosen node)
  path_node_id       BIGINT NOT NULL REFERENCES path_node(node_id) ON DELETE CASCADE,

  -- Raw fields captured "as seen" for history (keep these flexible)
  owner              TEXT NULL,                   -- or "contact" in your CSV
  issue_text         TEXT NULL,                   -- raw issue field
  error_text         TEXT NULL,                   -- raw error field
  last_modified      TIMESTAMPTZ NULL,            -- as parsed; can be NULL if unknown
  asset              TEXT NULL,
  asset_path_raw     TEXT NULL,
  build_machine_path_raw TEXT NULL,

  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Avoid duplicate rows when the same fact is ingested multiple times
  CONSTRAINT uq_issue_observation UNIQUE (snapshot_id, issue_instance_id, path_node_id)
);

CREATE INDEX IF NOT EXISTS ix_issue_obs_snapshot        ON issue_observation(snapshot_id);
CREATE INDEX IF NOT EXISTS ix_issue_obs_instance        ON issue_observation(issue_instance_id);
CREATE INDEX IF NOT EXISTS ix_issue_obs_path_node       ON issue_observation(path_node_id);

-- Helpful composite index for "count at node X at snapshot S" rollups via closure
CREATE INDEX IF NOT EXISTS ix_issue_obs_snapshot_path   ON issue_observation(snapshot_id, path_node_id);

-- ----------------------------
-- 5) Presence intervals (derived, but recommended to store)
-- ----------------------------

-- One row per continuous run of presence across changelists for an issue_instance.
-- end_changelist_id is NULL when the issue is still present/open at latest observed CL.
drop table if exists  issue_presence_interval cascade ;
CREATE TABLE IF NOT EXISTS issue_presence_interval (
  interval_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  project_id          VARCHAR NOT NULL REFERENCES project(project_id) ON DELETE CASCADE,

  issue_instance_id   BIGINT NOT NULL REFERENCES issue_instance(issue_instance_id) ON DELETE CASCADE,

  start_changelist_id BIGINT NOT NULL REFERENCES changelist(changelist_id) ON DELETE CASCADE,
  end_changelist_id   BIGINT NULL REFERENCES changelist(changelist_id) ON DELETE SET NULL,

  -- Optional: capture your chosen resolution rule ("closed after K misses", etc.)
  close_reason        TEXT NULL,

  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- NULL end = still open; non-NULL end must be >= start (allows single-CL closed intervals)
  CONSTRAINT ck_interval_order CHECK (end_changelist_id IS NULL OR end_changelist_id >= start_changelist_id)
);

-- At most one open interval per issue_instance
CREATE UNIQUE INDEX IF NOT EXISTS uq_issue_interval_open
  ON issue_presence_interval(issue_instance_id)
  WHERE end_changelist_id IS NULL;

CREATE INDEX IF NOT EXISTS ix_issue_interval_start ON issue_presence_interval(start_changelist_id);
CREATE INDEX IF NOT EXISTS ix_issue_interval_end   ON issue_presence_interval(end_changelist_id);

-- ----------------------------
-- 6) Changelist metrics (pre-aggregated reporting)
-- ----------------------------

CREATE TABLE IF NOT EXISTS changelist_metrics (
  project_id       BIGINT NOT NULL REFERENCES project(project_id) ON DELETE CASCADE,
  changelist_id    BIGINT NOT NULL REFERENCES changelist(changelist_id) ON DELETE CASCADE,

  total_issues     INTEGER NOT NULL CHECK (total_issues >= 0),
  new_issues       INTEGER NOT NULL CHECK (new_issues >= 0),
  resolved_issues  INTEGER NOT NULL CHECK (resolved_issues >= 0),

  computed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (project_id, changelist_id)
);

CREATE INDEX IF NOT EXISTS ix_changelist_metrics_cl ON changelist_metrics(changelist_id);

-- ============================================================
-- Notes:
-- - path_closure is maintained by your ingest logic (insert self row + ancestor links).
-- - issue_presence_interval and changelist_metrics are typically derived from observations.
-- - If you later want breakdown trends, add tables like:
--     changelist_metrics_by_issue_type(project_id, changelist_id, issue_type, total, new, resolved)
--     changelist_metrics_by_node(project_id, changelist_id, ancestor_node_id, total, new, resolved)
-- ============================================================
