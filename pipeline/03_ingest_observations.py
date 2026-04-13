"""
03_ingest_observations.py

For each unique (project, CL), creates one snapshot row, then creates one
issue_observation row per (snapshot, issue_instance, path_node) triple.

Depends on: issues_raw, changelist, issue_instance, path_node
            (run 00, 01, 02 first)

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    python3 pipeline/03_ingest_observations.py
"""
from __future__ import annotations

import hashlib
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


def norm_issue(s: str) -> str:
    s = "" if s is None else str(s)
    return re.sub(r"\s+", " ", s.strip().lower())


def norm_asset_path(p: str) -> str:
    p = "" if p is None else str(p).strip()
    p = p.replace("\\", "/")
    p = re.sub(r"/{2,}", "/", p).lower()
    p = re.sub(r"^[a-z]:", "", p)
    return p.lstrip("/")


def issue_signature(issue_norm: str, asset_norm: str) -> str:
    return hashlib.sha256(f"{issue_norm}|{asset_norm}".encode("utf-8")).hexdigest()


def norm_container(p: str) -> str:
    p = str(p).strip().replace("\\", "/")
    p = re.sub(r"/{2,}", "/", p).lstrip("/")
    parts = [s for s in p.split("/") if s]
    return "/".join(parts[:-1]) if len(parts) > 1 else "/".join(parts)


def main() -> None:
    dsn = _get_dsn()
    engine = create_engine(dsn)

    df = pd.read_sql_table("issues_raw", engine, schema=SCHEMA)

    df["issue_norm"]   = df["issue"].map(norm_issue)
    df["asset_norm"]   = df["asset_path"].map(norm_asset_path)
    df["sig"]          = [issue_signature(i, a)
                          for i, a in zip(df["issue_norm"], df["asset_norm"])]
    df["container_key"]= df["build_machine_path"].map(norm_container)
    df["last_edit_dt"] = pd.to_datetime(df["last_edit"], format="%Y/%m/%d", errors="coerce", utc=True)

    conn = psycopg2.connect(dsn)
    total_obs = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path = {SCHEMA}")

            for project_id, grp in df.groupby("project_id"):
                # ---- maps: cl_number → changelist_id
                cur.execute("SELECT changelist_id, cl_number FROM changelist WHERE project_id = %s",
                            (project_id,))
                cl_map = {int(cl): int(cid) for cid, cl in cur.fetchall()}

                # ---- upsert snapshots (one per CL, observed_at = max last_edit for that CL)
                snap_rows = []
                for cl, cg in grp.groupby("cl"):
                    cl_int      = int(cl)
                    cid         = cl_map.get(cl_int)
                    if cid is None:
                        continue
                    observed_at = cg["last_edit_dt"].max()
                    if pd.isna(observed_at):
                        observed_at = pd.Timestamp.now("UTC").floor("D")
                    snap_rows.append((project_id, cid,
                                      observed_at.to_pydatetime(), "issues_raw", None))

                execute_values(cur, """
                    INSERT INTO snapshot (project_id, changelist_id, observed_at, source_name, source_ref)
                    VALUES %s
                    ON CONFLICT (changelist_id, observed_at) DO NOTHING
                """, snap_rows, page_size=5000)

                # ---- map (cl_number → snapshot_id)
                cur.execute("""
                    SELECT c.cl_number, s.snapshot_id
                    FROM snapshot s
                    JOIN changelist c ON c.changelist_id = s.changelist_id
                    WHERE s.project_id = %s
                """, (project_id,))
                snap_map = {int(cl): int(sid) for cl, sid in cur.fetchall()}

                # ---- map (issue_signature → issue_instance_id)
                cur.execute("SELECT issue_instance_id, issue_signature FROM issue_instance "
                            "WHERE project_id = %s", (project_id,))
                issue_map = {sig: int(iid) for iid, sig in cur.fetchall()}

                # ---- map (full_path_key → node_id)
                container_keys = grp["container_key"].dropna().unique().tolist()
                cur.execute("SELECT full_path_key, node_id FROM path_node "
                            "WHERE full_path_key = ANY(%s)", (container_keys,))
                node_map = dict(cur.fetchall())

                # ---- build observation rows
                obs_rows = []
                dropped  = 0
                for r in grp.itertuples(index=False):
                    cl_int   = int(r.cl)
                    snap_id  = snap_map.get(cl_int)
                    issue_id = issue_map.get(r.sig)
                    node_id  = node_map.get(r.container_key)

                    if snap_id is None or issue_id is None:
                        continue
                    if node_id is None:
                        dropped += 1
                        continue

                    last_mod = pd.to_datetime(r.last_edit, format="%Y/%m/%d",
                                              errors="coerce", utc=True)
                    last_mod_py = None if pd.isna(last_mod) else last_mod.to_pydatetime()

                    obs_rows.append((
                        snap_id, issue_id, int(node_id),
                        r.contact, r.issue, r.error,
                        last_mod_py, r.asset, r.asset_path, r.build_machine_path,
                    ))

                # De-duplicate before insert (respects uq_issue_observation constraint)
                obs_df = pd.DataFrame(obs_rows, columns=[
                    "snapshot_id", "issue_instance_id", "path_node_id",
                    "owner", "issue_text", "error_text", "last_modified",
                    "asset", "asset_path_raw", "build_machine_path_raw",
                ]).drop_duplicates(subset=["snapshot_id", "issue_instance_id", "path_node_id"])

                execute_values(cur, """
                    INSERT INTO issue_observation
                      (snapshot_id, issue_instance_id, path_node_id, owner,
                       issue_text, error_text, last_modified,
                       asset, asset_path_raw, build_machine_path_raw)
                    VALUES %s
                    ON CONFLICT (snapshot_id, issue_instance_id, path_node_id) DO NOTHING
                """, list(obs_df.replace({pd.NaT: None}).itertuples(index=False, name=None)),
                page_size=5000)

                print(f"  [{project_id}] {len(obs_df)} observations "
                      f"({dropped} dropped — no path_node match)")
                total_obs += len(obs_df)

    print(f"Done — {total_obs} total observations.")


if __name__ == "__main__":
    main()
