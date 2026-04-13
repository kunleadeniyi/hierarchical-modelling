"""
01_ingest_changelists.py

For each project in issues_raw, upserts one row per unique CL number into the
changelist table, linking each CL to its path_node entry via full_path_key lookup.

Depends on: issues_raw, path_node (run 00_populate_path_nodes.py first)

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    python3 pipeline/01_ingest_changelists.py
"""
from __future__ import annotations

import os
import re

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

SCHEMA = "issue_tracker"


def _get_dsn() -> str:
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is not set.")
    return dsn


def norm_container(p: str) -> str:
    p = str(p).strip().replace("\\", "/")
    p = re.sub(r"/{2,}", "/", p).lstrip("/")
    parts = [s for s in p.split("/") if s]
    return "/".join(parts[:-1]) if len(parts) > 1 else "/".join(parts)


def cl_root_key(container: str, cl_number: int) -> str | None:
    parts = container.split("/")
    try:
        i = parts.index("Main_BuildMachine")
    except ValueError:
        return None
    if i + 1 >= len(parts):
        return None
    return "/".join(parts[: i + 2])


def main() -> None:
    dsn = _get_dsn()
    engine = create_engine(dsn)

    df = pd.read_sql_table("issues_raw", engine, schema=SCHEMA,
                           columns=["project_id", "cl", "build_machine_path", "last_edit"])

    df["container"]   = df["build_machine_path"].map(norm_container)
    df["last_edit_dt"] = pd.to_datetime(df["last_edit"], format="%Y/%m/%d", errors="coerce", utc=True)

    conn = psycopg2.connect(dsn)
    total = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path = {SCHEMA}")

            for project_id, grp in df.groupby("project_id"):
                cl_rows: list[tuple] = []
                for cl, cg in grp.groupby("cl"):
                    cl_int       = int(cl)
                    sample_cont  = cg["container"].iloc[0]
                    node_key     = cl_root_key(sample_cont, cl_int)
                    first_seen   = cg["last_edit_dt"].max()
                    cl_rows.append((project_id, cl_int, first_seen, node_key))

                # Resolve node_keys → node_ids
                node_keys = sorted({r[3] for r in cl_rows if r[3]})
                node_map: dict[str, int] = {}
                if node_keys:
                    cur.execute("SELECT full_path_key, node_id FROM path_node "
                                "WHERE full_path_key = ANY(%s)", (node_keys,))
                    node_map = dict(cur.fetchall())

                insert_rows = [
                    (pid, cl_num, first_seen, node_map.get(nk))
                    for pid, cl_num, first_seen, nk in cl_rows
                ]

                execute_values(cur, """
                    INSERT INTO changelist (project_id, cl_number, first_seen_at, changelist_node_id)
                    VALUES %s
                    ON CONFLICT (project_id, cl_number) DO UPDATE
                      SET first_seen_at      = COALESCE(EXCLUDED.first_seen_at, changelist.first_seen_at),
                          changelist_node_id = COALESCE(EXCLUDED.changelist_node_id, changelist.changelist_node_id)
                """, insert_rows, page_size=5000)

                print(f"  [{project_id}] {len(insert_rows)} changelists upserted "
                      f"({sum(1 for r in insert_rows if r[3])} with node_id)")
                total += len(insert_rows)

    print(f"Done — {total} total changelists.")


if __name__ == "__main__":
    main()
