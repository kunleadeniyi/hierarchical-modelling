-- Create the issue_tracker schema and grant access to the application user.
-- Replace 'your_db_user' with the Postgres role used by your application.
CREATE SCHEMA IF NOT EXISTS issue_tracker
    AUTHORIZATION your_db_user;

GRANT ALL ON SCHEMA issue_tracker TO your_db_user WITH GRANT OPTION;

-- make default schema
show search_path;
set search_path = issue_tracker;
show search_path;
