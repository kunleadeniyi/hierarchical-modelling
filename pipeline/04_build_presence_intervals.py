"""
04_build_presence_intervals.py

Derives issue_presence_interval rows from issue_observation.

For each issue_instance in a project, finds the ordered sequence of CLs it was
observed in and identifies contiguous runs.  A gap of ≥ 1 CL between observations
closes the current interval and opens a new one.  An interval whose last CL is the
final CL in the project remains open (end_changelist_id = NULL).

Algorithm (same logic as the original issue_presence_interval.ipynb):
  1. Build ordered CL list for the project.
  2. For each issue, collect the CL positions it appeared in.
  3. Walk the position list; when consecutive positions differ by > 1, close interval.
  4. Truncate and re-insert (idempotent).

Depends on: issue_observation, snapshot, changelist

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    python3 pipeline/04_build_presence_intervals.py
"""
from __future__ import annotations

import os

import psycopg2
from psycopg2.extras import execute_values

SCHEMA = "issue_tracker"


def _get_dsn() -> str:
    dsn = os.environ.get("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is not set.")
    return dsn


def build_intervals(
    issue_positions: dict[int, list[int]],   # issue_id → sorted list of CL positions
    cl_ids: list[int],                        # ordered changelist_ids for the project
) -> list[tuple]:
    """
    Convert per-issue CL-position sets into (issue_id, start_cl_id, end_cl_id, reason) tuples.

    A contiguous run of positions produces one interval.
    Runs separated by a gap produce separate intervals.
    The last run is open (end_cl_id = NULL) if it ends at the final CL, closed otherwise.
    """
    last_pos = len(cl_ids) - 1
    rows: list[tuple] = []

    for issue_id, positions in issue_positions.items():
        if not positions:
            continue

        start = positions[0]
        prev  = positions[0]

        for p in positions[1:]:
            if p == prev + 1:
                prev = p
                continue
            # Gap — close current run.
            # end_cl = cl_ids[prev] for both single-CL and multi-CL closed runs.
            # (Schema allows end_cl >= start_cl; single-CL → end_cl == start_cl.)
            rows.append((issue_id, cl_ids[start], cl_ids[prev], "absent_next_changelist"))
            start = p
            prev  = p

        # Final run: open if it reaches the last CL, closed otherwise.
        if prev == last_pos:
            rows.append((issue_id, cl_ids[start], None, None))
        else:
            rows.append((issue_id, cl_ids[start], cl_ids[prev], "absent_next_changelist"))

    return rows


def main() -> None:
    dsn = _get_dsn()
    conn = psycopg2.connect(dsn)
    total = 0

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path = {SCHEMA}")

            # Get all project_ids that have changelist data
            cur.execute("SELECT DISTINCT project_id FROM changelist ORDER BY project_id")
            project_ids = [r[0] for r in cur.fetchall()]

            for project_id in project_ids:
                # Ordered CL list for this project
                cur.execute("""
                    SELECT changelist_id, cl_number
                    FROM changelist
                    WHERE project_id = %s
                    ORDER BY cl_number
                """, (project_id,))
                cls      = cur.fetchall()
                cl_ids   = [int(cid) for cid, _ in cls]
                pos_map  = {cid: i for i, cid in enumerate(cl_ids)}

                if not cl_ids:
                    continue

                # For each issue: which CL positions did it appear in?
                cur.execute("""
                    SELECT io.issue_instance_id, s.changelist_id
                    FROM issue_observation io
                    JOIN snapshot s ON s.snapshot_id = io.snapshot_id
                    WHERE s.project_id = %s
                    GROUP BY io.issue_instance_id, s.changelist_id
                """, (project_id,))

                issue_positions: dict[int, list[int]] = {}
                for issue_id, cl_id in cur.fetchall():
                    cl_id = int(cl_id)
                    if cl_id in pos_map:
                        issue_positions.setdefault(int(issue_id), []).append(pos_map[cl_id])

                for issue_id in issue_positions:
                    issue_positions[issue_id] = sorted(issue_positions[issue_id])

                interval_rows = build_intervals(issue_positions, cl_ids)

                # Prepend project_id to each row
                full_rows = [(project_id, iid, s, e, r)
                             for iid, s, e, r in interval_rows]

                # Rebuild deterministically
                cur.execute("""
                    DELETE FROM issue_presence_interval
                    WHERE project_id = %s
                """, (project_id,))

                execute_values(cur, """
                    INSERT INTO issue_presence_interval
                      (project_id, issue_instance_id,
                       start_changelist_id, end_changelist_id, close_reason)
                    VALUES %s
                """, full_rows, page_size=5000)

                open_count   = sum(1 for _, _, e, _ in interval_rows if e is None)
                closed_count = len(interval_rows) - open_count
                print(f"  [{project_id}] {len(interval_rows)} intervals "
                      f"({open_count} open, {closed_count} closed)")
                total += len(interval_rows)

    print(f"Done — {total} total presence intervals.")


if __name__ == "__main__":
    main()
