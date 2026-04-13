-- populate_changelist_metrics.sql
SET search_path = issue_tracker;

-- Rebuild (idempotent) metrics per changelist for all projects.
-- Replace 'proj-alpha' with a specific project_id to scope to one project.
WITH ordered AS (
  SELECT
    changelist_id,
    project_id,
    cl_number,
    LAG(changelist_id) OVER (PARTITION BY project_id ORDER BY cl_number) AS prev_changelist_id
  FROM changelist
  WHERE project_id = 'proj-alpha'
),
totals AS (
  SELECT
    s.changelist_id,
    COUNT(DISTINCT io.issue_instance_id)::int AS total_issues
  FROM snapshot s
  JOIN issue_observation io ON io.snapshot_id = s.snapshot_id
  WHERE s.project_id = 'proj-alpha'
  GROUP BY s.changelist_id
),
news AS (
  SELECT
    start_changelist_id AS changelist_id,
    COUNT(*)::int AS new_issues
  FROM issue_presence_interval
  WHERE project_id = 'proj-alpha'
  GROUP BY start_changelist_id
),
resolved AS (
  SELECT
    o.changelist_id,
    COUNT(i.interval_id)::int AS resolved_issues
  FROM ordered o
  LEFT JOIN issue_presence_interval i
    ON i.project_id = o.project_id
   AND i.end_changelist_id = o.prev_changelist_id
  GROUP BY o.changelist_id
)
INSERT INTO changelist_metrics (project_id, changelist_id, total_issues, new_issues, resolved_issues)
SELECT
  o.project_id,
  o.changelist_id,
  COALESCE(t.total_issues, 0),
  COALESCE(n.new_issues, 0),
  COALESCE(r.resolved_issues, 0)
FROM ordered o
LEFT JOIN totals t ON t.changelist_id = o.changelist_id
LEFT JOIN news   n ON n.changelist_id = o.changelist_id
LEFT JOIN resolved r ON r.changelist_id = o.changelist_id
ON CONFLICT (project_id, changelist_id) DO UPDATE
SET total_issues = EXCLUDED.total_issues,
    new_issues = EXCLUDED.new_issues,
    resolved_issues = EXCLUDED.resolved_issues,
    computed_at = now();
