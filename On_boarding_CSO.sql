CREATE DATABASE APPOMNI_D_45954;
USE DATABASE APPOMNI_D_45954;
CREATE SCHEMA APPOMNI;
USE SCHEMA APPOMNI;
CREATE role DV0_ROLE_APPOMNI_ADMIN;

CREATE WAREHOUSE IF NOT EXISTS DV0_DW_APPOMNI_WH_45954
WITH WAREHOUSE_SIZE=XSMALL AUTO_SUSPEND=60 AUTO_RESUME=TRUE
MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD'
INITIALLY_SUSPENDED=TRUE COMMENT= 'For XSMALL -- DWS Warehouse';


GRANT USAGE         ON DATABASE APPOMNI_D_45954 TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT USAGE ON SCHEMA APPOMNI_D_45954.APPOMNI  	TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT CREATE SCHEMA ON DATABASE APPOMNI_D_45954 TO ROLE DV0_ROLE_APPOMNI_ADMIN;

GRANT USAGE ON WAREHOUSE DV0_DW_APPOMNI_WH_45954 		TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT OPERATE ON WAREHOUSE DV0_DW_APPOMNI_WH_45954 TO ROLE DV0_ROLE_APPOMNI_ADMIN ;

GRANT SELECT ON ALL TABLES IN SCHEMA  APPOMNI_D_45954.APPOMNI TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT SELECT ON ALL VIEWS IN SCHEMA   APPOMNI_D_45954.APPOMNI TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT OPERATE ON ALL TASKS IN SCHEMA  APPOMNI_D_45954.APPOMNI TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA APPOMNI_D_45954.APPOMNI TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA APPOMNI_D_45954.APPOMNI TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT OPERATE ON FUTURE TASKS IN SCHEMA APPOMNI_D_45954.APPOMNI TO ROLE DV0_ROLE_APPOMNI_ADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DV0_ROLE_APPOMNI_ADMIN;

create or replace view query_history as select * from snowflake.account_usage.query_history;
create or replace view login_history as select * from snowflake.account_usage.login_history;

--  account parameters data
show parameters in account;
create table if not exists stored_account_parms as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_account_parms;
--  users data
show users;
create table if not exists stored_users as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_users;
--  roles data
show roles;
create table if not exists stored_roles as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_roles;
--  integrations data
show integrations;
create table if not exists stored_integrations as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_integrations;

--  network policies data
show network policies;
create table if not exists stored_network_policies as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_network_policies;
--  shares data
show shares;
create table if not exists stored_shares as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_shares;
--  delegated auth data
show delegated authorizations;
create table if not exists stored_delegated_authorizations as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_delegated_authorizations;
--  external functions data
show external functions;
create table if not exists stored_external_functions as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_external_functions;

-- grants on integrations
show integrations;
create table if not exists stored_grants_on_integrations as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_grants_on_integrations;
-- grants to roles
show roles;
show grants to role ACCOUNTADMIN;
create table if not exists stored_grants_to_roles as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_grants_to_roles;

-- grants to users
show grants;
create table if not exists stored_grants_to_users as select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()));
select * from stored_grants_to_users;
-- ONE TIME SETUP PER TABLE FINISH

-- TBD: stored grants vs AU perf
create or replace view grants_to_users as select * from snowflake.account_usage.grants_to_users;
create or replace view grants_to_roles as select * from snowflake.account_usage.grants_to_roles;
create or replace view roles as select * from snowflake.account_usage.roles;
create or replace view users as select * from snowflake.account_usage.users;


-- TASKS SECTION
--use database identifier($dbname);
--use schema identifier($schemaname);
-- MAIN TASK PREPS BY TRUNCATING TABLES, AND THEN IS FOLLOWED BY LOADING TABLES
create or replace task task_store_all_data
  schedule = '11520 minute'
  user_task_managed_initial_warehouse_size = 'x-small'
as
  begin
  truncate table stored_account_parms;
  truncate table stored_users;
  truncate table stored_roles;
  truncate table stored_integrations;
  truncate table stored_network_policies;
  truncate table stored_shares;
  truncate table stored_delegated_authorizations;
  truncate table stored_external_functions;
end;

-- NEXT TASK DOES ALL BASIC SINGLE LINE STEPS
create or replace task task_store_single_query_data
  user_task_managed_initial_warehouse_size = 'x-small'
  after task_store_all_data
as
  begin
  show parameters in account;
  insert into stored_account_parms select *, current_timestamp() from table(result_scan(last_query_id()));
  show users;
  insert into stored_users select *, current_timestamp() from table(result_scan(last_query_id()));
  show roles;
  insert into stored_roles select *, current_timestamp() from table(result_scan(last_query_id()));
  show integrations;
  insert into stored_integrations select *, current_timestamp() from table(result_scan(last_query_id()));
  show network policies;
  insert into stored_network_policies select *, current_timestamp() from table(result_scan(last_query_id()));
  show shares;
  insert into stored_shares select *, current_timestamp() from table(result_scan(last_query_id()));
  show delegated authorizations;
  insert into stored_delegated_authorizations select *, current_timestamp() from table(result_scan(last_query_id()));
  show external functions;
  insert into stored_external_functions select *, current_timestamp() from table(result_scan(last_query_id()));
end;


-- NEXT TASK FOR GRANTS ON INTEGRATIONS, THAT IS MULTI-STEP
create or replace task task_store_grants_on_integrations
  user_task_managed_initial_warehouse_size = 'x-small'
  after task_store_single_query_data
as
declare
  show_statement varchar;
  insert_statement varchar;
  c1 cursor for select "name" as int_name from stored_integrations;
begin
  truncate table stored_grants_on_integrations;
  for record in c1 do
    show_statement := 'show grants on integration "' || record.int_name || '"';
    execute immediate :show_statement;
    insert_statement := 'insert into stored_grants_on_integrations select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()))';
    execute immediate :insert_statement;
  end for;
end;


-- NEXT TASK FOR GRANTS ON ROLES, WHICH IS MULTI-STEP
--  TBD: stored grants vs AU
create or replace task task_store_grants_to_roles
  user_task_managed_initial_warehouse_size = 'x-small'
  after task_store_single_query_data
as
declare
  show_statement varchar;
  insert_statement varchar;
  c1 cursor for select "name" as role_name from stored_roles;
begin
  truncate table stored_grants_to_roles;
  for record in c1 do
    show_statement := 'show grants to role "' || record.role_name || '"';
    execute immediate :show_statement;
    insert_statement := 'insert into stored_grants_to_roles select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()))';
    execute immediate :insert_statement;
  end for;
end;

--  TBD: stored grants vs AU
create or replace task task_store_grants_to_users
  user_task_managed_initial_warehouse_size = 'x-small'
  after task_store_single_query_data
as
declare
  show_statement varchar;
  insert_statement varchar;
  c1 cursor for select "name" as user_name from stored_users;
begin
  truncate table stored_grants_to_users;
  for record in c1 do
    show_statement := 'show grants to user "' || record.user_name || '"';
    execute immediate :show_statement;
    insert_statement := 'insert into stored_grants_to_users select *, current_timestamp() as last_refresh from table(result_scan(last_query_id()))';
    execute immediate :insert_statement;
  end for;
end;

-- SEE THAT ALL TASKS STARTED SUSPENDED
show tasks;
-- RESUME ALL FROM ROOT THROUGH CHILDREN, AS CHILD TASKS NEED TO BE ENABLED TO RUN UNLESS EXPLICITLY CALLED
select system$task_dependents_enable('task_store_all_data');
-- VERIFY
show tasks;
-- SUSPEND ONLY ROOT TASK SO IT RUNS ONLY ON COMMAND
alter task task_store_all_data suspend;
-- VERIFY
show tasks;

CREATE or replace SECURITY INTEGRATION DV0_ROLE_APPOMNI_ADMIN
TYPE = OAUTH
ENABLED = TRUE
OAUTH_CLIENT = CUSTOM
OAUTH_CLIENT_TYPE = 'CONFIDENTIAL'
OAUTH_REDIRECT_URI = 'https://id.appomni.com/r/'
OAUTH_ALLOW_NON_TLS_REDIRECT_URI = TRUE
OAUTH_ISSUE_REFRESH_TOKENS = TRUE
OAUTH_REFRESH_TOKEN_VALIDITY = 7776000
;
