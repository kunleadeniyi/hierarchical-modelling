# Query Showcase

Eight queries against the hierarchical issue tracker schema. Each answers a real business question and demonstrates a technique worth explaining.

All queries assume `SET search_path = issue_tracker;` and use the synthetic dataset (`proj-alpha`, `proj-beta`).

---

## 1. Issue count rolled up to team folder — closure table join

**Question:** How many distinct issues has each team folder accumulated across all CLs?

```sql
SELECT
  team.node_name                           AS team,
  team.project_id,
  COUNT(DISTINCT io.issue_instance_id)     AS total_issues
FROM issue_observation  io
JOIN path_closure       pc   ON pc.descendant_node_id = io.path_node_id
JOIN path_node          team ON team.node_id           = pc.ancestor_node_id
WHERE team.node_name LIKE 'team-%'
GROUP BY team.node_name, team.project_id
ORDER BY total_issues DESC;
```

**Why it works:** `path_closure` holds every (ancestor, descendant) pair regardless of depth. Joining observations to their ancestors via `path_closure` and then filtering on `node_name LIKE 'team-%'` collapses all descendant observations up to the team level in a single join — no recursion at query time.

---

## 2. New / resolved / total trend per CL with running open count — window function

**Question:** For each CL in `proj-alpha`, what were the total, new, and resolved issue counts, and what was the cumulative open issue count?

```sql
SELECT
  c.cl_number,
  cm.total_issues,
  cm.new_issues,
  cm.resolved_issues,
  SUM(cm.new_issues - cm.resolved_issues)
    OVER (ORDER BY c.cl_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    AS running_open
FROM changelist_metrics cm
JOIN changelist c ON c.changelist_id = cm.changelist_id
WHERE cm.project_id = 'proj-alpha'
ORDER BY c.cl_number;
```

**Why it works:** `changelist_metrics` is a pre-aggregated table — no scan of `issue_observation` at query time. The `SUM(...) OVER (ORDER BY cl_number ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` is a cumulative window that computes the running open balance by adding new issues and subtracting resolved ones at each CL.

---

## 3. Top 10 longest-lived issues — presence interval span

**Question:** Which 10 issues have been present across the most CLs (by CL number span)?

```sql
SELECT
  ii.issue_type,
  LEFT(ii.issue_pattern, 80)                            AS issue_pattern,
  c_start.cl_number                                     AS first_cl,
  COALESCE(c_end.cl_number, c_latest.max_cl)            AS last_cl,
  COALESCE(c_end.cl_number, c_latest.max_cl)
    - c_start.cl_number + 1                             AS cl_span,
  CASE WHEN ipi.end_changelist_id IS NULL
    THEN 'open' ELSE 'closed' END                       AS status
FROM issue_presence_interval ipi
JOIN issue_instance  ii       ON ii.issue_instance_id  = ipi.issue_instance_id
JOIN changelist      c_start  ON c_start.changelist_id = ipi.start_changelist_id
LEFT JOIN changelist c_end    ON c_end.changelist_id   = ipi.end_changelist_id
CROSS JOIN LATERAL (
  SELECT MAX(cl_number) AS max_cl
  FROM changelist
  WHERE project_id = ipi.project_id
) c_latest
ORDER BY cl_span DESC
LIMIT 10;
```

**Why it works:** `CROSS JOIN LATERAL` evaluates a subquery once per outer row — here it fetches the project's latest CL number so open intervals (where `end_changelist_id IS NULL`) can still be measured by substituting the latest CL. Without this, open intervals would have no span.

---

## 4. Folder subtrees with highest issue churn — closure + interval count

**Question:** Which team sub-directories have the most issue open/close cycles (high churn)?

```sql
SELECT
  ancestor.node_name                                          AS subtree,
  ancestor.project_id,
  COUNT(ipi.interval_id)                                      AS total_intervals,
  COUNT(DISTINCT ipi.issue_instance_id)                       AS distinct_issues,
  ROUND(
    COUNT(ipi.interval_id)::numeric
    / NULLIF(COUNT(DISTINCT ipi.issue_instance_id), 0), 2
  )                                                           AS intervals_per_issue
FROM issue_presence_interval ipi
JOIN issue_observation io
  ON io.issue_instance_id = ipi.issue_instance_id
JOIN path_closure pc
  ON pc.descendant_node_id = io.path_node_id
JOIN path_node ancestor
  ON ancestor.node_id = pc.ancestor_node_id
WHERE ipi.end_changelist_id IS NOT NULL      -- only completed (closed) intervals
  AND ancestor.node_name LIKE 'team-%'
GROUP BY ancestor.node_name, ancestor.project_id
ORDER BY intervals_per_issue DESC
LIMIT 10;
```

**Why it works:** Churn = intervals per distinct issue. An issue that opens and closes multiple times contributes multiple rows to `issue_presence_interval`. The `NULLIF` guard prevents division-by-zero on subtrees with no issues. Filtering `end_changelist_id IS NOT NULL` excludes currently-open intervals, counting only completed cycles.

---

## 5. Issues that resolved and reappeared — HAVING on interval count

**Question:** Which issues have appeared, disappeared, and come back at least once (multiple presence intervals)?

```sql
SELECT
  ii.issue_type,
  LEFT(ii.issue_pattern, 80)      AS issue_pattern,
  COUNT(ipi.interval_id)          AS interval_count,
  MIN(c_s.cl_number)              AS first_seen_cl,
  MAX(COALESCE(c_e.cl_number, 0)) AS last_closed_cl
FROM issue_presence_interval ipi
JOIN issue_instance  ii  ON ii.issue_instance_id = ipi.issue_instance_id
JOIN changelist      c_s ON c_s.changelist_id     = ipi.start_changelist_id
LEFT JOIN changelist c_e ON c_e.changelist_id     = ipi.end_changelist_id
GROUP BY ipi.issue_instance_id, ii.issue_type, ii.issue_pattern
HAVING COUNT(ipi.interval_id) > 1
ORDER BY interval_count DESC
LIMIT 20;
```

**Why it works:** Grouping on `issue_instance_id` and counting `interval_id` rows is the cleanest way to find recurrent issues — no self-join or subquery needed. `HAVING COUNT > 1` filters to issues with at least two separate presence runs. `MIN(c_s.cl_number)` gives the first time the issue appeared; `MAX(c_e.cl_number)` gives the last time it was seen closed.

---

## 6. Point-in-time snapshot — issues open at a specific CL

**Question:** Which issues were open at CL 10045 in `proj-alpha`?

```sql
WITH target AS (
  SELECT changelist_id, cl_number
  FROM changelist
  WHERE project_id = 'proj-alpha'
    AND cl_number  = 10045
)
SELECT
  ii.issue_type,
  LEFT(ii.issue_pattern, 80)   AS issue_pattern,
  c_start.cl_number            AS open_since_cl
FROM issue_presence_interval ipi
JOIN target                  t       ON t.changelist_id       >= ipi.start_changelist_id
JOIN issue_instance          ii      ON ii.issue_instance_id  =  ipi.issue_instance_id
JOIN changelist              c_start ON c_start.changelist_id =  ipi.start_changelist_id
WHERE ipi.project_id = 'proj-alpha'
  AND (
    ipi.end_changelist_id IS NULL
    OR ipi.end_changelist_id >= (SELECT changelist_id FROM target)
  )
ORDER BY c_start.cl_number;
```

**Why it works:** An issue was open at CL _X_ if its interval started at or before _X_ AND either it hasn't closed yet (`end IS NULL`) or it closed at or after _X_. The CTE isolates the target CL's surrogate key so the range filter hits the `ix_issue_interval_start` and `ix_issue_interval_end` indexes cleanly.

---

## 7. Issue type driving the most new issues per CL — aggregation with ratio

**Question:** On average, which issue type introduces the most new issues per CL?

```sql
SELECT
  ii.issue_type,
  COUNT(ipi.interval_id)                              AS total_new_intervals,
  COUNT(DISTINCT ipi.start_changelist_id)             AS cls_with_new_of_type,
  ROUND(
    COUNT(ipi.interval_id)::numeric
    / NULLIF(COUNT(DISTINCT ipi.start_changelist_id), 0), 2
  )                                                   AS avg_new_per_cl
FROM issue_presence_interval ipi
JOIN issue_instance ii ON ii.issue_instance_id = ipi.issue_instance_id
GROUP BY ii.issue_type
ORDER BY avg_new_per_cl DESC;
```

**Why it works:** Each row in `issue_presence_interval` represents one "new appearance" of an issue (a new interval). Counting intervals grouped by issue type and dividing by the number of distinct CLs in which they started gives the average new-issue rate per CL for that type. This query answers in one pass without touching `issue_observation` at all — the presence interval table is sufficient.

---

## 8. Ancestor rollup from a single leaf node — closure traversal upward

**Question:** For a specific observation node (e.g. `team-art/Characters`), how many distinct issues have been seen at every ancestor level?

```sql
WITH leaf AS (
  SELECT node_id, full_path_key
  FROM path_node
  WHERE full_path_key LIKE '%/Main_BuildMachine/%/assetLoad/team-art/Characters'
  LIMIT 1
)
SELECT
  ancestor.full_path_key            AS ancestor_path,
  ancestor.depth,
  ancestor.node_name,
  COUNT(DISTINCT io.issue_instance_id) AS issue_count
FROM leaf
JOIN path_closure     pc       ON pc.descendant_node_id = leaf.node_id
JOIN path_node        ancestor ON ancestor.node_id       = pc.ancestor_node_id
JOIN issue_observation io      ON io.path_node_id        = leaf.node_id
GROUP BY ancestor.full_path_key, ancestor.depth, ancestor.node_name
ORDER BY ancestor.depth;
```

**Why it works:** Starting from one leaf node, `path_closure` gives all ancestors in a single scan — depth 0 (self) up to the root. Every ancestor gets the same `issue_count` because the observations are at the leaf; the closure join just tags each ancestor with those counts. This is the query pattern that powers the `v_treemap_cl_levels8_latest` view, generalised to operate from any single node rather than across all CLs.
