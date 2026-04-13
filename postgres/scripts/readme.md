Run order
- changelist.py
- issue_instance -> issue.ipynb
- issue_observation.ipynb 
- path_closure related queries
  - path_closure_all_projects.sql or
  - path_closure_per_project.sql or
  - incremental load for specific paths
  
Rollups
- issue_presence_interval.ipynb
- changelist_metrics.sql

BI
- BI_tool_ready_view.sql : this view allows hierarchical visualisation up to 8 levels of hierarchy from the CL

JSON extract
- build_treemap.py