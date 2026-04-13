"""
run_all.py

Orchestrator: runs pipeline steps 00–04 in order.

Each step streams its output live. If any step exits with a non-zero status
the run is aborted immediately and the process exits with code 1.
A summary table is printed at the end showing the row count reported by each step.

Usage:
    export PG_DSN=postgresql://user:pass@localhost:5432/devops
    python3 pipeline/run_all.py

    # Skip the seed generator (useful when issues_raw is already populated):
    python3 pipeline/run_all.py --skip-seed
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Step definitions
# ---------------------------------------------------------------------------

ROOT = Path(__file__).parent.parent

STEPS = [
    {
        "name": "00 — populate path nodes",
        "script": ROOT / "pipeline" / "00_populate_path_nodes.py",
        "summary_pattern": r"\[(\w[\w-]*)\]\s+(\d+) path nodes",   # per-project line
        "total_pattern":   None,                                     # "Done." has no count
    },
    {
        "name": "01 — ingest changelists",
        "script": ROOT / "pipeline" / "01_ingest_changelists.py",
        "summary_pattern": None,
        "total_pattern":   r"Done — (\d+) total changelist",
    },
    {
        "name": "02 — ingest issue instances",
        "script": ROOT / "pipeline" / "02_ingest_issue_instances.py",
        "summary_pattern": None,
        "total_pattern":   r"Done — (\d+) total issue_instance",
    },
    {
        "name": "03 — ingest observations",
        "script": ROOT / "pipeline" / "03_ingest_observations.py",
        "summary_pattern": None,
        "total_pattern":   r"Done — (\d+) total observation",
    },
    {
        "name": "04 — build presence intervals",
        "script": ROOT / "pipeline" / "04_build_presence_intervals.py",
        "summary_pattern": None,
        "total_pattern":   r"Done — (\d+) total presence interval",
    },
]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_step(step: dict) -> tuple[int, str]:
    """
    Run one pipeline step, streaming stdout/stderr live.

    Returns (exit_code, captured_output).
    """
    script = str(step["script"])
    print(f"\n{'─' * 60}")
    print(f"  {step['name']}")
    print(f"{'─' * 60}")

    start = time.monotonic()
    lines: list[str] = []

    proc = subprocess.Popen(
        [sys.executable, script],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="", flush=True)
        lines.append(line)

    proc.wait()
    elapsed = time.monotonic() - start
    status = "OK" if proc.returncode == 0 else "FAILED"
    print(f"  [{status}] {elapsed:.1f}s")

    return proc.returncode, "".join(lines)


def extract_count(output: str, pattern: str | None) -> str:
    """Extract a row count from captured output using a regex pattern."""
    if pattern is None:
        return "—"
    m = re.search(pattern, output)
    return f"{int(m.group(1)):,}" if m else "—"


def print_summary(results: list[dict]) -> None:
    col_w = [40, 12, 8]
    header = f"{'Step':<{col_w[0]}}  {'Rows':>{col_w[1]}}  {'Status':>{col_w[2]}}"
    divider = "─" * (sum(col_w) + 4)

    print(f"\n{'═' * (sum(col_w) + 4)}")
    print("  PIPELINE SUMMARY")
    print(f"{'═' * (sum(col_w) + 4)}")
    print(f"  {header}")
    print(f"  {divider}")
    for r in results:
        row = f"{r['name']:<{col_w[0]}}  {r['count']:>{col_w[1]}}  {r['status']:>{col_w[2]}}"
        print(f"  {row}")
    print(f"{'═' * (sum(col_w) + 4)}\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full ingest pipeline (steps 00–04).")
    parser.add_argument(
        "--skip-seed", action="store_true",
        help="Skip seeding check; assume issues_raw is already populated.",
    )
    return parser.parse_args()


def main() -> None:
    parse_args()   # parse early so --help works without needing PG_DSN

    import os
    if not os.environ.get("PG_DSN"):
        print("ERROR: PG_DSN environment variable is not set.", file=sys.stderr)
        print("  export PG_DSN=postgresql://user:pass@localhost:5432/dbname", file=sys.stderr)
        sys.exit(1)

    results: list[dict] = []

    for step in STEPS:
        exit_code, output = run_step(step)

        count = extract_count(output, step["total_pattern"])
        status = "OK" if exit_code == 0 else "FAILED"
        results.append({"name": step["name"], "count": count, "status": status})

        if exit_code != 0:
            print(f"\nAborting: step '{step['name']}' failed (exit {exit_code}).",
                  file=sys.stderr)
            print_summary(results)
            sys.exit(1)

    print_summary(results)


if __name__ == "__main__":
    main()
