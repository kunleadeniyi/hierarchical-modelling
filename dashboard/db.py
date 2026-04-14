"""
db.py — shared database connection and cached query helpers.

All queries return pandas DataFrames.
@st.cache_data ensures each query is only executed once per unique set of
arguments until the cache is cleared or the TTL expires.
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

SCHEMA = "issue_tracker"


def _engine():
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        st.error(
            "PG_DSN environment variable is not set.\n\n"
            "Run: `export PG_DSN=postgresql://user:pass@localhost:5432/devops`"
        )
        st.stop()
    return create_engine(dsn)


@st.cache_data(ttl=300)
def projects() -> list[str]:
    with _engine().connect() as c:
        rows = c.execute(text(f"SELECT project_id FROM {SCHEMA}.project ORDER BY project_id"))
        return [r[0] for r in rows]


@st.cache_data(ttl=300)
def kpis(project_id: str) -> dict:
    sql = text(f"""
        SELECT
          (SELECT COUNT(*)   FROM {SCHEMA}.issue_instance       WHERE project_id = :p) AS total_issues,
          (SELECT COUNT(*)   FROM {SCHEMA}.changelist           WHERE project_id = :p) AS total_cls,
          (SELECT COUNT(*)   FROM {SCHEMA}.issue_presence_interval WHERE project_id = :p
                                                                AND end_changelist_id IS NULL) AS open_issues,
          (SELECT COUNT(*)   FROM {SCHEMA}.issue_presence_interval WHERE project_id = :p
                                                                AND end_changelist_id IS NOT NULL) AS closed_issues
    """)
    with _engine().connect() as c:
        row = c.execute(sql, {"p": project_id}).fetchone()
        return dict(row._mapping)


@st.cache_data(ttl=300)
def cl_trend(project_id: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT
          c.cl_number,
          cm.total_issues,
          cm.new_issues,
          cm.resolved_issues,
          SUM(cm.new_issues - cm.resolved_issues)
            OVER (ORDER BY c.cl_number
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_open
        FROM {SCHEMA}.changelist_metrics cm
        JOIN {SCHEMA}.changelist c ON c.changelist_id = cm.changelist_id
        WHERE cm.project_id = :p
        ORDER BY c.cl_number
    """)
    with _engine().connect() as c:
        return pd.read_sql(sql, c, params={"p": project_id})


@st.cache_data(ttl=300)
def issue_type_dist(project_id: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT issue_type, COUNT(*) AS issue_count
        FROM {SCHEMA}.issue_instance
        WHERE project_id = :p
        GROUP BY issue_type
        ORDER BY issue_count DESC
    """)
    with _engine().connect() as c:
        return pd.read_sql(sql, c, params={"p": project_id})


@st.cache_data(ttl=300)
def treemap_data(project_id: str) -> pd.DataFrame:
    """All CL snapshots for the animated treemap / icicle."""
    sql = text(f"""
        SELECT
          cl_number,
          COALESCE(level_1, '(none)') AS level_1,
          COALESCE(level_2, '(none)') AS level_2,
          COALESCE(level_3, '(none)') AS level_3,
          issue_count
        FROM {SCHEMA}.v_treemap_cl_levels8_latest
        WHERE project_id = :p
          AND level_1 IS NOT NULL
        ORDER BY cl_number, level_1, level_2, level_3
    """)
    with _engine().connect() as c:
        return pd.read_sql(sql, c, params={"p": project_id})


@st.cache_data(ttl=300)
def team_heatmap_data(project_id: str) -> pd.DataFrame:
    """Issue count per team per CL — for the temporal heatmap."""
    sql = text(f"""
        SELECT
          c.cl_number,
          team.node_name                           AS team,
          COUNT(DISTINCT io.issue_instance_id)::int AS issue_count
        FROM {SCHEMA}.issue_observation io
        JOIN {SCHEMA}.snapshot       s    ON s.snapshot_id         = io.snapshot_id
        JOIN {SCHEMA}.changelist     c    ON c.changelist_id        = s.changelist_id
        JOIN {SCHEMA}.path_closure   pc   ON pc.descendant_node_id  = io.path_node_id
        JOIN {SCHEMA}.path_node      team ON team.node_id            = pc.ancestor_node_id
        WHERE c.project_id   = :p
          AND team.node_name LIKE 'team-%'
        GROUP BY c.cl_number, team.node_name
        ORDER BY c.cl_number, team.node_name
    """)
    with _engine().connect() as c:
        return pd.read_sql(sql, c, params={"p": project_id})


@st.cache_data(ttl=300)
def presence_intervals(project_id: str) -> pd.DataFrame:
    """Intervals with start/end CL numbers for the lifecycle Gantt."""
    sql = text(f"""
        SELECT
          ipi.issue_instance_id,
          ii.issue_type,
          LEFT(ii.issue_pattern, 70)                      AS label,
          c_s.cl_number                                    AS start_cl,
          COALESCE(c_e.cl_number, c_max.max_cl)            AS end_cl,
          CASE WHEN ipi.end_changelist_id IS NULL
               THEN 'open' ELSE 'closed' END               AS status,
          COALESCE(ipi.close_reason, 'open')               AS close_reason
        FROM {SCHEMA}.issue_presence_interval ipi
        JOIN {SCHEMA}.issue_instance ii      ON ii.issue_instance_id  = ipi.issue_instance_id
        JOIN {SCHEMA}.changelist     c_s     ON c_s.changelist_id     = ipi.start_changelist_id
        LEFT JOIN {SCHEMA}.changelist c_e    ON c_e.changelist_id     = ipi.end_changelist_id
        CROSS JOIN LATERAL (
          SELECT MAX(cl_number) AS max_cl
          FROM {SCHEMA}.changelist WHERE project_id = ipi.project_id
        ) c_max
        WHERE ipi.project_id = :p
        ORDER BY start_cl, ii.issue_type
    """)
    with _engine().connect() as c:
        return pd.read_sql(sql, c, params={"p": project_id})


@st.cache_data(ttl=300)
def recurring_issues(project_id: str) -> pd.DataFrame:
    """Issues with more than one presence interval (reappearing issues)."""
    sql = text(f"""
        SELECT
          ii.issue_type,
          LEFT(ii.issue_pattern, 90)  AS issue_pattern,
          COUNT(ipi.interval_id)      AS interval_count,
          MIN(c_s.cl_number)          AS first_seen_cl,
          MAX(COALESCE(c_e.cl_number, 0)) AS last_closed_cl
        FROM {SCHEMA}.issue_presence_interval ipi
        JOIN {SCHEMA}.issue_instance ii  ON ii.issue_instance_id = ipi.issue_instance_id
        JOIN {SCHEMA}.changelist     c_s ON c_s.changelist_id    = ipi.start_changelist_id
        LEFT JOIN {SCHEMA}.changelist c_e ON c_e.changelist_id   = ipi.end_changelist_id
        WHERE ipi.project_id = :p
        GROUP BY ipi.issue_instance_id, ii.issue_type, ii.issue_pattern
        HAVING COUNT(ipi.interval_id) > 1
        ORDER BY interval_count DESC
        LIMIT 30
    """)
    with _engine().connect() as c:
        return pd.read_sql(sql, c, params={"p": project_id})
