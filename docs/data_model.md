# Data Model Design Decisions

Five non-obvious choices made in the schema, with the reasoning behind each.

---

## Closure Table for Path Hierarchy

**The problem:** Build machine paths are 8–12 levels deep (server → share → project → `Main_BuildMachine` → CL → issue type → team → sub-directory → asset). A common requirement is "how many issues are under this team folder across all CLs?" — a rollup that spans many levels.

**Naive alternative:** Recursive CTE at query time.
```sql
-- This works but is O(depth × rows) per query
WITH RECURSIVE subtree AS (
  SELECT node_id FROM path_node WHERE node_id = :root
  UNION ALL
  SELECT n.node_id FROM path_node n
  JOIN subtree s ON s.node_id = n.parent_node_id
)
SELECT COUNT(*) FROM issue_observation WHERE path_node_id IN (SELECT node_id FROM subtree);
```

**Why closure table instead:** Pre-materialise every (ancestor, descendant, distance) pair once at ingest time. The rollup query becomes:
```sql
SELECT COUNT(DISTINCT io.issue_instance_id)
FROM issue_observation io
JOIN path_closure pc ON pc.descendant_node_id = io.path_node_id
WHERE pc.ancestor_node_id = :team_folder_node_id;
```
One join, one index scan. No recursion at read time.

**Trade-off accepted:** Write amplification. Inserting a new path node at depth _d_ requires _d_ rows in `path_closure` (one per ancestor). For this dataset (~32k path nodes) that produces ~1.4M closure rows — acceptable since path nodes are written once during ingest and never updated.

**See also:** `pipeline/00_populate_path_nodes.py` builds the closure table in Python during ingest. `postgres/sql/02_path_closure.sql` is the equivalent SQL for manual/incremental use.

---

## Snapshot Model

**The problem:** A build pipeline may ingest the same CL multiple times (reruns, backfills, corrections). Storing only "current state" would silently overwrite prior observations.

**Naive alternative:** One row per (CL, issue) with `ON CONFLICT DO UPDATE`. This loses the history of what was seen on each ingest run.

**Why snapshot model:** A `snapshot` row represents one ingestion event for a CL. `issue_observation` rows reference the snapshot, not the CL directly. This means:
- Re-ingesting a CL creates a new snapshot and new observations — history is preserved.
- Point-in-time queries filter by `observed_at`: "what did we see at midnight on 2024-03-01?"
- The `UNIQUE (changelist_id, observed_at)` constraint prevents true duplicates while allowing multiple snapshots per CL at different times.

**Trade-off accepted:** Slightly more complex joins — queries must go `issue_observation → snapshot → changelist` rather than directly to `changelist`. The `ix_snapshot_changelist` and `ix_snapshot_project_time` indexes keep this fast.

---

## Presence Intervals as a Derived Table

**The problem:** Computing "is this issue new in this CL?" from raw observations requires checking whether the issue appeared in any prior CL interval. With 85 CLs and 650 issues, that is a 55,250-cell matrix scan on every query.

**Naive alternative:** Compute new/resolved at query time using window functions over `issue_observation`. This works for small datasets but degrades as observation history grows.

**Why pre-compute intervals:** `issue_presence_interval` stores contiguous runs of CL presence as (start_cl, end_cl) pairs. The algorithm in `pipeline/04_build_presence_intervals.py`:
1. For each issue, collects the sorted list of CL positions it appeared in.
2. Walks the list; when two consecutive positions differ by more than 1, close the current interval and open a new one.
3. An interval whose last CL is the project's final CL gets `end_changelist_id = NULL` (still open).

With this table, "issues new in CL 10045" is:
```sql
SELECT * FROM issue_presence_interval
WHERE start_changelist_id = (SELECT changelist_id FROM changelist WHERE cl_number = 10045);
```
One index scan on `ix_issue_interval_start`.

**Trade-off accepted:** The table must be rebuilt when observations are backfilled. The pipeline script truncates per-project and re-inserts, making it idempotent. A partial unique index (`uq_issue_interval_open`) enforces at most one open interval per issue at a time.

---

## Issue Signature as a Content Hash

**The problem:** Issues arrive as raw text strings. The same underlying issue (same error, same asset) may appear across dozens of CLs. We need a stable identity that:
- Survives pipeline reruns without creating duplicates.
- Doesn't require a round-trip lookup before insert.
- Is consistent across projects if the same issue type appears in both.

**Naive alternative:** Auto-increment surrogate key assigned on first insert. This works but requires a lookup on every observation row to find the existing `issue_instance_id`.

**Why content hash:** `issue_signature = SHA-256(normalise(issue_text) | normalise(asset_path))`. The normalisation strips case, collapses whitespace, and removes drive letters so that minor formatting differences in the source data don't produce separate identities. The hash is computed in Python before the DB round-trip, enabling `ON CONFLICT (project_id, issue_signature) DO UPDATE` upserts with no prior lookup.

**Trade-off accepted:** The identity is sensitive to the normalisation rules. Changing `norm_issue()` or `norm_asset_path()` in the pipeline would produce different hashes for the same logical issue, fragmenting history. The normalisation functions are documented in `pipeline/02_ingest_issue_instances.py` and must be kept stable.

---

## Upsert-Everywhere

**The problem:** The pipeline is designed to be re-runnable. Running it twice on the same data should produce identical DB state, not duplicate rows or errors.

**Approach:** Every insert uses `ON CONFLICT … DO UPDATE` or `DO NOTHING`:

| Table | Strategy | Reason |
|---|---|---|
| `project` | `DO UPDATE SET project_name` | Allow name changes without breaking FK chain |
| `changelist` | `DO UPDATE SET first_seen_at, changelist_node_id` | Fill in node link if it was NULL on first run |
| `issue_instance` | `DO UPDATE SET issue_type, issue_pattern, asset_path, asset_name` | Allow enrichment on rerun |
| `snapshot` | `DO NOTHING` | Snapshot is immutable once created |
| `issue_observation` | `DO NOTHING` | Observations are facts; duplicates are silently dropped |
| `issue_presence_interval` | `DELETE + INSERT` | Rebuild deterministically per project |

**Trade-off accepted:** `DO UPDATE` is slightly more expensive than `INSERT` due to the conflict check. For the observation table (the largest insert by volume) `DO NOTHING` is used instead, which avoids the write penalty. The `page_size=5000` in `execute_values` batches inserts to balance round-trip cost against memory.
