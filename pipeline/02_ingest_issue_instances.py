"""
02_ingest_issue_instances.py

Derives unique issue_instance rows from issues_raw using a content-hash signature
of (normalised issue text, normalised asset path).  Idempotent — reruns update
existing rows without creating duplicates.

Depends on: issues_raw

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    python3 pipeline/02_ingest_issue_instances.py
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


def main() -> None:
    dsn = _get_dsn()
    engine = create_engine(dsn)

    df = pd.read_sql_table("issues_raw", engine, schema=SCHEMA,
                           columns=["project_id", "issue", "asset_path", "tag", "asset"])

    df["issue_norm"] = df["issue"].map(norm_issue)
    df["asset_norm"] = df["asset_path"].map(norm_asset_path)
    df["sig"]        = [issue_signature(i, a)
                        for i, a in zip(df["issue_norm"], df["asset_norm"])]

    conn = psycopg2.connect(dsn)
    total = 0
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path = {SCHEMA}")

            for project_id, grp in df.groupby("project_id"):
                # Stable representative values per signature
                agg = grp.groupby("sig", as_index=False).agg(
                    issue_type   = ("tag",        "first"),
                    issue_pattern= ("issue",      "first"),
                    asset_path   = ("asset_norm", "first"),
                    asset_name   = ("asset",      "first"),
                )

                rows = [
                    (project_id, r.sig, r.issue_type, r.issue_pattern,
                     None, r.asset_path, r.asset_name)
                    for r in agg.itertuples()
                ]

                execute_values(cur, """
                    INSERT INTO issue_instance
                      (project_id, issue_signature, issue_type, issue_pattern,
                       missing_token, asset_path, asset_name)
                    VALUES %s
                    ON CONFLICT (project_id, issue_signature) DO UPDATE
                      SET issue_type    = EXCLUDED.issue_type,
                          issue_pattern = EXCLUDED.issue_pattern,
                          asset_path    = EXCLUDED.asset_path,
                          asset_name    = EXCLUDED.asset_name
                """, rows, page_size=5000)

                print(f"  [{project_id}] {len(rows)} issue_instances upserted")
                total += len(rows)

    print(f"Done — {total} total issue_instances.")


if __name__ == "__main__":
    main()
