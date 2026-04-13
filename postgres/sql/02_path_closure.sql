SET search_path = issue_tracker;

-- (Optional) Safety: ensure closure table is empty for this project
-- Replace 'proj-alpha' with the target project_id before running.
DELETE FROM path_closure pc
USING path_node n
WHERE pc.descendant_node_id = n.node_id
  AND n.project_id = 'proj-alpha';

-- Build closure for project 'proj-alpha'
WITH RECURSIVE up AS (
  -- base: each node is its own ancestor at distance 0
  SELECT
    n.project_id,
    n.node_id AS descendant_node_id,
    n.node_id AS ancestor_node_id,
    0          AS distance
  FROM path_node n
  WHERE n.project_id = 'proj-alpha'

  UNION ALL

  -- step: move ancestor upward via parent pointer
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
FROM up
ON CONFLICT (ancestor_node_id, descendant_node_id) DO UPDATE
SET distance = EXCLUDED.distance;
