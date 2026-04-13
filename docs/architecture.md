# Architecture

## Data Flow

```mermaid
flowchart TD
    A[seed/generate_data.py] -->|writes rows| B[(issues_raw\nstaging table)]

    B --> C[00_populate_path_nodes.py]
    B --> D[01_ingest_changelists.py]
    B --> E[02_ingest_issue_instances.py]
    B --> F[03_ingest_observations.py]

    C -->|inserts| G[(path_node)]
    C -->|inserts| H[(path_closure)]
    D -->|upserts| I[(changelist)]
    E -->|upserts| J[(issue_instance)]
    F -->|upserts| K[(snapshot)]
    F -->|upserts| L[(issue_observation)]

    L --> M[04_build_presence_intervals.py]
    I --> M
    M -->|rebuilds| N[(issue_presence_interval)]

    I --> O[03_metrics.sql]
    L --> O
    N --> O
    O -->|upserts| P[(changelist_metrics)]

    G --> Q[04_views.sql]
    H --> Q
    I --> Q
    K --> Q
    L --> Q
    Q -->|creates| R[v_treemap_cl_levels8_latest]
```

---

## Entity Relationship Diagram

```mermaid
erDiagram
    project {
        varchar project_id PK
        text project_key
        text project_name
        timestamptz created_at
    }

    path_node {
        bigint node_id PK
        varchar project_id FK
        bigint parent_node_id FK
        text node_name
        path_node_type node_type
        int depth
        text full_path_key
    }

    path_closure {
        bigint ancestor_node_id FK
        bigint descendant_node_id FK
        int distance
    }

    changelist {
        bigint changelist_id PK
        varchar project_id FK
        int cl_number
        timestamptz created_at
        timestamptz first_seen_at
        bigint changelist_node_id FK
    }

    snapshot {
        bigint snapshot_id PK
        varchar project_id FK
        bigint changelist_id FK
        timestamptz observed_at
        timestamptz ingested_at
        text source_name
    }

    issue_family {
        bigint issue_family_id PK
        varchar project_id FK
        text family_signature
        text issue_type
        text title
    }

    issue_instance {
        bigint issue_instance_id PK
        varchar project_id FK
        text issue_signature
        text issue_type
        text issue_pattern
        text missing_token
        text asset_path
        text asset_name
        bigint issue_family_id FK
    }

    issue_observation {
        bigint observation_id PK
        bigint snapshot_id FK
        bigint issue_instance_id FK
        bigint path_node_id FK
        text owner
        text issue_text
        text error_text
        timestamptz last_modified
    }

    issue_presence_interval {
        bigint interval_id PK
        varchar project_id FK
        bigint issue_instance_id FK
        bigint start_changelist_id FK
        bigint end_changelist_id FK
        text close_reason
    }

    changelist_metrics {
        varchar project_id FK
        bigint changelist_id FK
        int total_issues
        int new_issues
        int resolved_issues
        timestamptz computed_at
    }

    project ||--o{ path_node : "has"
    project ||--o{ changelist : "has"
    project ||--o{ snapshot : "has"
    project ||--o{ issue_instance : "has"
    project ||--o{ issue_presence_interval : "has"
    project ||--o{ issue_family : "has"

    path_node ||--o{ path_node : "parent of"
    path_node ||--o{ path_closure : "ancestor in"
    path_node ||--o{ path_closure : "descendant in"
    path_node ||--o{ issue_observation : "observed at"
    path_node ||--o| changelist : "anchors"

    changelist ||--o{ snapshot : "has"
    changelist ||--o{ issue_presence_interval : "starts"
    changelist ||--o{ issue_presence_interval : "ends"
    changelist ||--o| changelist_metrics : "has metrics"

    snapshot ||--o{ issue_observation : "contains"

    issue_instance ||--o{ issue_observation : "observed as"
    issue_instance ||--o{ issue_presence_interval : "tracked by"
    issue_instance }o--|| issue_family : "belongs to"
```

---

## Table Roles

| Table | Role | Written by |
|---|---|---|
| `issues_raw` | Staging: raw ingest rows, one per observation in source data | `seed/generate_data.py` |
| `project` | Root dimension; all FK chains start here | `seed/generate_data.py` |
| `path_node` | Adjacency list for the build-machine path hierarchy | `pipeline/00_populate_path_nodes.py` |
| `path_closure` | Materialised ancestor–descendant pairs (closure table) | `pipeline/00_populate_path_nodes.py` |
| `changelist` | One CL per project; links to its path_node in the hierarchy | `pipeline/01_ingest_changelists.py` |
| `snapshot` | One ingestion event per CL; separates observation time from CL time | `pipeline/03_ingest_observations.py` |
| `issue_instance` | Deduplicated issue identity (SHA-256 signature) | `pipeline/02_ingest_issue_instances.py` |
| `issue_observation` | Fact: issue seen at a path location in a snapshot | `pipeline/03_ingest_observations.py` |
| `issue_presence_interval` | Derived: contiguous CL runs per issue; open if `end_cl IS NULL` | `pipeline/04_build_presence_intervals.py` |
| `changelist_metrics` | Pre-aggregated total/new/resolved per CL | `postgres/sql/03_metrics.sql` |

---

## Key Constraints

| Constraint | Table | What it enforces |
|---|---|---|
| `uq_issue_instance` | `issue_instance` | One row per `(project_id, issue_signature)` — the hash is the identity |
| `uq_issue_observation` | `issue_observation` | No duplicate `(snapshot, issue, path_node)` triples |
| `uq_issue_interval_open` | `issue_presence_interval` | At most one open interval (NULL end) per issue at any time |
| `ck_interval_order` | `issue_presence_interval` | `end_cl >= start_cl` when not NULL — allows single-CL closed intervals |
| `uq_snapshot_unique` | `snapshot` | One snapshot per `(changelist_id, observed_at)` |
| `uq_changelist_per_project` | `changelist` | One CL number per project |
