"""
00_populate_path_nodes.py

Reads build_machine_path values from issues_raw, decomposes each path into its
ancestor nodes, and upserts every unique node into path_node.
Then builds path_closure for every project via a recursive Python traversal
(equivalent to the SQL recursive CTE in path_closure_per_project.sql).

Must run before any other pipeline script because changelist ingest depends on
path_node.full_path_key lookups.

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    python3 pipeline/00_populate_path_nodes.py
"""
from __future__ import annotations

import os
import re

import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import pandas as pd

SCHEMA = "issue_tracker"


def _get_dsn() -> str:
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is not set.")
    return dsn


def norm_container(p: str) -> str:
    """Normalise build_machine_path → container key (drops leaf, forward slashes)."""
    p = str(p).strip().replace("\\", "/")
    p = re.sub(r"/{2,}", "/", p).lstrip("/")
    parts = [s for s in p.split("/") if s]
    return "/".join(parts[:-1]) if len(parts) > 1 else "/".join(parts)


def all_ancestor_keys(full_key: str) -> list[str]:
    """Return all ancestor keys from root to full_key, inclusive."""
    parts = full_key.split("/")
    return ["/".join(parts[:i]) for i in range(1, len(parts) + 1)]


def detect_node_type(parts: list[str]) -> str:
    """Assign a path_node_type enum value based on position in the path."""
    if len(parts) == 1:
        return "root"
    try:
        mi = parts.index("Main_BuildMachine")
        depth_from_main = len(parts) - 1 - mi
        if depth_from_main == 0:
            return "build_machine"
        if depth_from_main == 1:
            return "changelist"
        return "folder"
    except ValueError:
        return "folder"


def main() -> None:
    dsn = _get_dsn()
    engine = create_engine(dsn)

    df = pd.read_sql_table("issues_raw", engine, schema=SCHEMA,
                           columns=["project_id", "build_machine_path"])
    df = df.drop_duplicates(subset=["project_id", "build_machine_path"])

    conn = psycopg2.connect(dsn)
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path = {SCHEMA}")

            for project_id, grp in df.groupby("project_id"):
                # Collect every unique ancestor key for this project
                all_keys: set[str] = set()
                for bmp in grp["build_machine_path"]:
                    container = norm_container(bmp)
                    for k in all_ancestor_keys(container):
                        all_keys.add(k)

                # Insert root-to-leaf so each parent exists before its children
                sorted_keys = sorted(all_keys, key=lambda k: k.count("/"))
                node_id_map: dict[str, int] = {}

                for key in sorted_keys:
                    parts = key.split("/")
                    node_name = parts[-1]
                    depth     = len(parts) - 1
                    ntype     = detect_node_type(parts)
                    parent_key = "/".join(parts[:-1]) if depth > 0 else None
                    parent_id  = node_id_map.get(parent_key) if parent_key else None

                    cur.execute("""
                        INSERT INTO path_node
                          (project_id, parent_node_id, node_name, node_type, depth, full_path_key)
                        VALUES (%s, %s, %s, %s::path_node_type, %s, %s)
                        ON CONFLICT (full_path_key) DO UPDATE
                          SET node_type = EXCLUDED.node_type
                        RETURNING node_id
                    """, (project_id, parent_id, node_name, ntype, depth, key))

                    row = cur.fetchone()
                    if row:
                        node_id_map[key] = row[0]
                    else:
                        cur.execute("SELECT node_id FROM path_node WHERE full_path_key = %s", (key,))
                        node_id_map[key] = cur.fetchone()[0]

                # Build path_closure: delete stale rows then re-insert
                cur.execute("""
                    DELETE FROM path_closure pc
                    USING path_node n
                    WHERE pc.descendant_node_id = n.node_id
                      AND n.project_id = %s
                """, (project_id,))

                closure_rows: list[tuple[int, int, int]] = []
                for key in sorted_keys:
                    desc_id = node_id_map.get(key)
                    if desc_id is None:
                        continue
                    anc_parts = key.split("/")
                    for i in range(len(anc_parts), 0, -1):
                        anc_key = "/".join(anc_parts[:i])
                        anc_id  = node_id_map.get(anc_key)
                        if anc_id is not None:
                            closure_rows.append((anc_id, desc_id, len(anc_parts) - i))

                execute_values(cur, """
                    INSERT INTO path_closure (ancestor_node_id, descendant_node_id, distance)
                    VALUES %s
                    ON CONFLICT (ancestor_node_id, descendant_node_id) DO UPDATE
                      SET distance = EXCLUDED.distance
                """, closure_rows, page_size=5000)

                print(f"  [{project_id}] {len(sorted_keys)} path nodes, "
                      f"{len(closure_rows)} closure rows")

    print("Done.")


if __name__ == "__main__":
    main()
