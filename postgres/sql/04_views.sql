CREATE OR REPLACE VIEW issue_tracker.v_treemap_cl_levels8_latest AS
WITH latest_snapshot AS (
  SELECT DISTINCT ON (s.project_id, s.changelist_id)
    s.snapshot_id,
    s.project_id,
    s.changelist_id,
    s.observed_at
  FROM issue_tracker.snapshot s
  ORDER BY s.project_id, s.changelist_id, s.observed_at DESC
),
base AS (
  SELECT
    ls.project_id,
    c.cl_number,
    ls.snapshot_id,
    ls.observed_at,
    io.issue_instance_id,
    leaf.full_path_key  AS leaf_path,
    clroot.full_path_key AS cl_root_path
  FROM latest_snapshot ls
  JOIN issue_tracker.changelist c
    ON c.changelist_id = ls.changelist_id
   AND c.project_id    = ls.project_id
  JOIN issue_tracker.issue_observation io
    ON io.snapshot_id = ls.snapshot_id
  JOIN issue_tracker.path_node leaf
    ON leaf.node_id = io.path_node_id
  JOIN issue_tracker.path_node clroot
    ON clroot.node_id = c.changelist_node_id
  WHERE c.changelist_node_id IS NOT NULL
),
rel AS (
  SELECT
    project_id,
    cl_number,
    snapshot_id,
    observed_at,
    issue_instance_id,
    CASE
      WHEN leaf_path = cl_root_path THEN ''                      -- issue attached at CL root
      WHEN leaf_path LIKE cl_root_path || '/%' THEN substr(leaf_path, length(cl_root_path) + 2)
      ELSE NULL                                                  -- safety: leaf not under CL root
    END AS rel_path
  FROM base
)
SELECT
  project_id,
  cl_number,
  observed_at,
  -- 8 exposed hierarchy levels under the CL root
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 1), '') AS level_1,
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 2), '') AS level_2,
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 3), '') AS level_3,
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 4), '') AS level_4,
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 5), '') AS level_5,
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 6), '') AS level_6,
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 7), '') AS level_7,
  NULLIF(split_part(COALESCE(rel_path, ''), '/', 8), '') AS level_8,
  COUNT(DISTINCT issue_instance_id)::int AS issue_count
FROM rel
WHERE rel_path IS NOT NULL
GROUP BY
  project_id, cl_number, observed_at,
  level_1, level_2, level_3, level_4, level_5, level_6, level_7, level_8;
