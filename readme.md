# Hierarchical Issue Tracker — Data Modelling Showcase

![CI](https://github.com/kunleadeniyi/hierarchical-modelling/actions/workflows/ci.yml/badge.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-required-blue?logo=docker&logoColor=white)

Build systems generate thousands of issues per changelist. Tracking which issues are new, which resolved, and rolling up counts through a deep folder hierarchy at query time is expensive. This project models the problem properly: a closure table for instant hierarchy rollups, a snapshot model for point-in-time queries, and pre-computed presence intervals so new/resolved state is a single index scan.

All data is synthetic. A generator produces realistic fake records; the pipeline ingests them into a normalised Postgres schema. No proprietary data is used.

---

## Architecture

```
seed/generate_data.py
        │
        ▼
  issues_raw  (staging)
        │
        ├── 00_populate_path_nodes.py  ──▶  path_node, path_closure
        ├── 01_ingest_changelists.py   ──▶  changelist
        ├── 02_ingest_issue_instances.py ─▶ issue_instance
        ├── 03_ingest_observations.py  ──▶  snapshot, issue_observation
        └── 04_build_presence_intervals.py ▶ issue_presence_interval
```

See [docs/architecture.md](docs/architecture.md) for the full ERD and data flow diagram.

---

## Data Model

| Table | Purpose |
|---|---|
| `project` | One row per project; all other tables FK here |
| `path_node` | One row per unique path segment (build server, CL folder, team, asset) |
| `path_closure` | Closure table: every ancestor–descendant pair with distance; enables O(1) hierarchy rollups |
| `changelist` | One row per CL per project; links to the CL's path_node |
| `snapshot` | One ingestion run per CL; separates observation time from CL time |
| `issue_instance` | Deduplicated issue identity, keyed by SHA-256 of normalised text + asset path |
| `issue_observation` | Fact table: one row per (snapshot, issue, path location) |
| `issue_presence_interval` | Derived: contiguous runs of CL presence per issue; NULL end = still open |
| `changelist_metrics` | Pre-aggregated total/new/resolved counts per CL |

Key design decisions are documented in [docs/data_model.md](docs/data_model.md).

---

## Quickstart

**Prerequisites:** Docker, Python 3.10+

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd hierarchical-modelling
pip install -r requirements.txt
```

### 2. Start Postgres

```bash
cd postgres
cp ../.env.example .env     # edit credentials if desired

docker compose up -d                          # DB only
# docker compose --profile pgadmin up -d     # + pgAdmin  (if you have no DB IDE)
# docker compose --profile dashboard up -d   # + Metabase (see Dashboard section below)
cd ..
```

### 3. Apply the schema

```bash
export PG_DSN=postgresql://devops_user:changeme@localhost:5432/devops
psql $PG_DSN -f postgres/sql/01_schema.sql
```

### 4. Generate synthetic data

```bash
python3 seed/generate_data.py
# [proj-alpha] 85 changelists, ~488 issue instances
# [proj-beta]  26 changelists, ~162 issue instances
# issues_raw: 22745 rows written
```

### 5. Run the pipeline

```bash
python3 pipeline/run_all.py
```

Or run steps individually if you need to re-run a single step:

```bash
python3 pipeline/00_populate_path_nodes.py      # ~32k path nodes, ~1.4M closure rows
python3 pipeline/01_ingest_changelists.py        # 111 changelists
python3 pipeline/02_ingest_issue_instances.py    # ~650 issue instances
python3 pipeline/03_ingest_observations.py       # ~22k observations
python3 pipeline/04_build_presence_intervals.py  # ~7.6k intervals
```

### 6. Apply derived SQL

```bash
psql $PG_DSN -f postgres/sql/03_metrics.sql   # populate changelist_metrics
psql $PG_DSN -f postgres/sql/04_views.sql     # create BI view
```

---

## Key Design Decisions

- **Closure table** (`path_closure`): hierarchy rollups are a single join — no recursion at read time. Scales to 8+ levels with millions of rows. [Read more](docs/data_model.md#closure-table-for-path-hierarchy)

- **Snapshot model** (`snapshot`): decouples ingestion time from changelist time. The same CL can be re-ingested without duplicating observations. Enables point-in-time queries. [Read more](docs/data_model.md#snapshot-model)

- **Presence intervals** (`issue_presence_interval`): pre-computing contiguous runs of CL presence makes new/resolved queries a simple range filter instead of a full observation scan. [Read more](docs/data_model.md#presence-intervals-as-a-derived-table)

---

## Dashboard (Streamlit)

```bash
export PG_DSN=postgresql://devops_user:changeme@localhost:5432/devops
streamlit run dashboard/app.py
```

Opens at [http://localhost:8501](http://localhost:8501). Three pages:

| Page | Content |
|---|---|
| **Overview** | KPI cards, new/resolved/total trend per CL, running open count, issue type breakdown |
| **Hierarchy** | Animated treemap/icicle/sunburst that steps through CL numbers; team × CL heatmap; static snapshot at a chosen CL |
| **Lifecycle** | Presence interval Gantt (one bar per interval, gaps = reappearances); recurring issues chart |

---

## Dashboard (Metabase)

After running the pipeline, you can explore the data visually in Metabase.

```bash
cd postgres
docker compose --profile dashboard up -d
```

Open [http://localhost:3000](http://localhost:3000) and complete the setup wizard:

1. Create an admin account.
2. On the **Add your data** step, choose **PostgreSQL** and enter:
   - **Host:** `postgres_db`
   - **Port:** `5432`
   - **Database:** `devops` (or your `POSTGRES_DB` value)
   - **Username / Password:** your `POSTGRES_USER` / `POSTGRES_PASSWORD` values
   - **Schema:** `issue_tracker`
3. Click **Connect** — Metabase will scan the schema and all tables become available for querying.

Suggested starting points once connected:
- **`changelist_metrics`** — new/resolved/total trend per CL (line chart by `cl_number`)
- **`v_treemap_cl_levels8_latest`** — hierarchical issue breakdown (pivot or treemap by `level_1` → `level_4`)
- **`issue_presence_interval`** — filter `end_changelist_id IS NULL` to see all currently open issues

---

## Query Showcase

Eight queries demonstrating window functions, closure table rollups, point-in-time filtering, and interval analysis: [docs/query_showcase.md](docs/query_showcase.md)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Database | PostgreSQL 16 |
| Containerisation | Docker Compose (profiles: `pgadmin`, `dashboard`) |
| Pipeline | Python 3.10+, pandas, SQLAlchemy, psycopg2 |
| Dashboard | Streamlit, Plotly |
| Synthetic data | Python (dataclasses, random, hashlib) |
| Schema | Pure SQL DDL — no ORM migrations |
