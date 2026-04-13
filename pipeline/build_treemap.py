# treemap_export.py
import os
import json
import psycopg2

SCHEMA = "issue_tracker"

SQL_SNAPSHOT_ID = """
SELECT s.snapshot_id
FROM snapshot s
JOIN changelist c ON c.changelist_id = s.changelist_id
WHERE s.project_id = %s
  AND c.project_id = %s
  AND c.cl_number  = %s
ORDER BY s.observed_at DESC
LIMIT 1;
"""

SQL_ROOT_NODE_ID = """
SELECT c.changelist_node_id
FROM changelist c
WHERE c.project_id = %s
  AND c.cl_number  = %s;
"""

SQL_TREE_WITH_VALUES_DISTINCT = """
WITH subtree AS (
  SELECT descendant_node_id AS node_id
  FROM path_closure
  WHERE ancestor_node_id = %s
),
rollup AS (
  SELECT
    pc.ancestor_node_id AS node_id,
    COUNT(DISTINCT io.issue_instance_id) AS value
  FROM issue_observation io
  JOIN path_closure pc
    ON pc.descendant_node_id = io.path_node_id
  WHERE io.snapshot_id = %s
    AND pc.ancestor_node_id IN (SELECT node_id FROM subtree)
  GROUP BY pc.ancestor_node_id
)
SELECT
  n.node_id,
  n.parent_node_id,
  n.node_name,
  COALESCE(r.value, 0) AS value
FROM path_node n
LEFT JOIN rollup r ON r.node_id = n.node_id
WHERE n.node_id IN (SELECT node_id FROM subtree);
"""

def build_treemap_json(project_id: str, cl_number: int) -> dict:
    dsn = os.environ["PG_DSN"]
    connection = psycopg2.connect(dsn)

    with connection as conn, conn.cursor() as cur:
        cur.execute(f"SET search_path = {SCHEMA};")

        # 1) snapshot_id (latest for that CL)
        cur.execute(SQL_SNAPSHOT_ID, (project_id, project_id, cl_number))
        row = cur.fetchone()
        if not row:
            raise RuntimeError(f"No snapshot found for project_id={project_id}, cl_number={cl_number}")
        snapshot_id = row[0]

        # 2) root node (changelist node)
        cur.execute(SQL_ROOT_NODE_ID, (project_id, cl_number))
        row = cur.fetchone()
        if not row or row[0] is None:
            raise RuntimeError(f"No changelist_node_id found for project_id={project_id}, cl_number={cl_number}")
        root_node_id = row[0]

        # 3) fetch nodes + values within subtree
        cur.execute(SQL_TREE_WITH_VALUES_DISTINCT, (root_node_id, snapshot_id))
        rows = cur.fetchall()

    # rows: (node_id, parent_node_id, node_name, value)
    nodes = {}
    children_by_parent = {}

    for node_id, parent_id, name, value in rows:
        nodes[node_id] = {"name": name, "value": int(value), "children": []}
        children_by_parent.setdefault(parent_id, []).append(node_id)

    # attach children
    for parent_id, child_ids in children_by_parent.items():
        if parent_id is None:
            continue
        if parent_id in nodes:
            nodes[parent_id]["children"] = [nodes[cid] for cid in child_ids if cid in nodes]

    # return root object
    if root_node_id not in nodes:
        raise RuntimeError("Root node missing from subtree query results (path_closure / path_node mismatch).")

    return nodes[root_node_id]

if __name__ == "__main__":
    # Usage: PG_DSN=postgresql://user:pass@localhost:5432/dbname python build_treemap.py
    data = build_treemap_json(project_id="proj-alpha", cl_number=10024)
    print(json.dumps(data, indent=2))
