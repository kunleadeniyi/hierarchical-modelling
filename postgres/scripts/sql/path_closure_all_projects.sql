SET search_path = issue_tracker;

TRUNCATE TABLE path_closure;

WITH RECURSIVE up AS (
  SELECT
    n.project_id,
    n.node_id AS descendant_node_id,
    n.node_id AS ancestor_node_id,
    0          AS distance
  FROM path_node n

  UNION ALL

  SELECT
    up.project_id,
    up.descendant_node_id,
    p.parent_node_id AS ancestor_node_id,
    up.distance + 1  AS distance
  FROM up
  JOIN path_node p
    ON p.project_id = up.project_id
   AND p.node_id    = up.ancestor_node_id
  WHERE p.parent_node_id IS NOT NULL
)
INSERT INTO path_closure (ancestor_node_id, descendant_node_id, distance)
SELECT ancestor_node_id, descendant_node_id, distance
FROM up;

-- =================================== incremental maintenance
-- Inputs:
--   :new_id    = newly inserted path_node.node_id
--   :parent_id = its parent path_node.node_id (nullable)

INSERT INTO path_closure (ancestor_node_id, descendant_node_id, distance)
VALUES (:new_id, :new_id, 0)
ON CONFLICT DO NOTHING;

-- If parent exists, inherit its ancestors
INSERT INTO path_closure (ancestor_node_id, descendant_node_id, distance)
SELECT
  pc.ancestor_node_id,
  :new_id AS descendant_node_id,
  pc.distance + 1 AS distance
FROM path_closure pc
WHERE pc.descendant_node_id = :parent_id
ON CONFLICT (ancestor_node_id, descendant_node_id) DO UPDATE
SET distance = EXCLUDED.distance;
