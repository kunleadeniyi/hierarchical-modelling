## Note:
- Files are either notebooks, python scripts or sql scripts
- SQL scripts should be run on the database
- Python scripts can be run anywhere 
- Notebook will typically require a Jupyter notebook like environment. Most IDE's support it. Notebooks are file that end with the .ipynb extension


## Tables 

issues_raw - starting point to derive all other data. Not really part of the model but necessary for the PoC

issue_observation - the observation of the issue as seen in the asset file with some enrichment and reference to other table

issue_instance - table containing one row per issue instance, using a concat of (issue, asset) as the issue_signature 

issue_family - not used, but table present in ddl

path_node - table containing adjacent parent/child relationships only for node (in this case build_machine_path, that is the folder as seen in the shared drive)

path_closure - table contain relation of each node in a path to the parent.

changelist - should contain one row per CL

changelist_metrics - Rollup/aggregate table showing issue stats per CL

issue_presence_interval - Rollup/aggregate table showing issue presence per CL

project - should contain one row project, can be used to support projects on a branch level by use the project_id as <project>_<branch>. e.g d5_main

snapshot - should contain one row per build run, will contain multiple rows if a CL is built multiple times.

## Views
v_treemap_cl_levels8_latest - view for hierarchical reporting, built to support BI Tools like Tableau and Looker


## How to use
1. Ensure all packages are installed -> psycopg2, pandas, sqlalchemy
2. Run extract_data.ipynb to dump raw data from shared drive to issues folder in the project root directory
2. Start Postgres server using docker-compose or your existing docker 
3. Run postgres/scripts/sql/table_creation_v2.sql in postgres database
4. run explore_and_save_data_to_postgres.ipynb to explore data and save issues_raw and path_node tables to postgres.
5. Continue with instructions on run order of other scripts in postgres/scripts/readme.md 