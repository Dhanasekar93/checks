/*=========================================================================================================*/
/*                           !!! Connect to ETL_CMDB database as ADMIN in new DEV !!!                      */
/*=========================================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

-- ==== MOVING KITCHENS

-- calling procedure for moving individual kitchens
/*
+----------+--------------+
|schemaname|pg_size_pretty|
+----------+--------------+
|crm       |0 bytes       |
|data_out  |8192 bytes    |
|data_in   |838 MB        |
+----------+--------------+
*/


call admin.reimport_foreign_schema('crm','crm');
-- [2021-11-25 22:46:29] completed in 529 ms
-- [2021-12-05 21:54:02] completed in 1 s 3 ms
-- [2022-01-28 02:45:54] completed in 484 ms
call admin.reimport_foreign_schema('data_in','data_in');
-- [2021-11-25 22:46:29] completed in 1 m 55 s 928 ms
-- [2021-12-05 21:55:43] completed in 1 m 22 s 639 ms
-- [2022-01-28 02:45:22] completed in 1 m 15 s 209 ms
call admin.reimport_foreign_schema('data_out','data_out');
-- [2021-11-25 22:49:27] completed in 471 ms
-- [2021-12-05 21:59:57] completed in 1 s 52 ms
-- [2022-01-28 02:45:54] completed in 484 ms

call admin.reimport_foreign_schema('apj_crm','crm');
--
call admin.reimport_foreign_schema('apj_data_in','data_in');
--
call admin.reimport_foreign_schema('apj_data_out','data_out');
--

SELECT schemaname, pg_size_pretty(sum(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint)
FROM pg_tables
WHERE schemaname IN ('crm','data_in','data_out','apj_crm','apj_data_in','apj_data_out')
GROUP BY schemaname
ORDER BY sum(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint;

/*
+----------+--------------+
|schemaname|pg_size_pretty|
+----------+--------------+
|crm       |0 bytes       |
|data_out  |8192 bytes    |
|data_in   |838 MB        |
+----------+--------------+
*/

/* ============================================================= */
-- ==== STEP 2) grant privileges
/* ============================================================= */
SET ROLE emea_su;

-- ==== FOR ROLE dev_schema_users which is used by developers

-- grant SELECT to ALL tables (developers can SELECT from all tables)
GRANT SELECT ON ALL TABLES IN SCHEMA data_in TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA data_out TO dev_schema_users;

GRANT SELECT ON ALL TABLES IN SCHEMA apj_data_in TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_crm TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_data_out TO dev_schema_users;

-- grant ALL to job scheduler tables only (developers can SEL, INS, UPD, DEL from this table only)
-- tables
GRANT ALL PRIVILEGES ON data_in._etl_job_schedules TO dev_schema_users;
GRANT ALL PRIVILEGES ON crm._job_schedules TO dev_schema_users;
GRANT ALL PRIVILEGES ON data_out._etl_job_schedules TO dev_schema_users;

GRANT ALL PRIVILEGES ON apj_data_in._etl_job_schedules TO dev_schema_users;
GRANT ALL PRIVILEGES ON apj_crm._job_schedules TO dev_schema_users;
GRANT ALL PRIVILEGES ON apj_data_out._etl_job_schedules TO dev_schema_users;

-- sequences
GRANT ALL PRIVILEGES ON data_in._etl_job_schedules_job_id_seq TO dev_schema_users;
GRANT ALL PRIVILEGES ON crm._job_schedules_job_id_seq TO dev_schema_users;
GRANT ALL PRIVILEGES ON data_out._etl_job_schedules_job_id_seq TO dev_schema_users;

GRANT ALL PRIVILEGES ON apj_data_in._etl_job_schedules_job_id_seq TO dev_schema_users;
GRANT ALL PRIVILEGES ON apj_crm._job_schedules_job_id_seq TO dev_schema_users;
GRANT ALL PRIVILEGES ON apj_data_out._etl_job_schedules_job_id_seq TO dev_schema_users;

-- grant ALL to ALL routines
GRANT ALL PRIVILEGES ON SCHEMA data_in TO dev_schema_users;
GRANT ALL PRIVILEGES ON SCHEMA crm TO dev_schema_users;
GRANT ALL PRIVILEGES ON SCHEMA data_out TO dev_schema_users;

GRANT ALL PRIVILEGES ON SCHEMA apj_data_in TO dev_schema_users;
GRANT ALL PRIVILEGES ON SCHEMA apj_crm TO dev_schema_users;
GRANT ALL PRIVILEGES ON SCHEMA apj_data_out TO dev_schema_users;

-- ==== FOR ROLE etl_app_user which is used by python - give all privs on all
-- grant ALL to ALL
-- tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_in TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crm TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_out TO etl_app_user;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA apj_data_in TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA apj_crm TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA apj_data_out TO etl_app_user;

-- sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_in TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA crm TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_out TO etl_app_user;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA apj_data_in TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA apj_crm TO etl_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA apj_data_out TO etl_app_user;

-- routines
GRANT ALL PRIVILEGES ON SCHEMA data_in TO etl_app_user;
GRANT ALL PRIVILEGES ON SCHEMA crm TO etl_app_user;
GRANT ALL PRIVILEGES ON SCHEMA data_out TO etl_app_user;
GRANT ALL PRIVILEGES ON SCHEMA apj_data_in TO etl_app_user;
GRANT ALL PRIVILEGES ON SCHEMA apj_crm TO etl_app_user;
GRANT ALL PRIVILEGES ON SCHEMA apj_data_out TO etl_app_user;
/* ============================================================= */
-- ==== STEP 3) default privileges for future objects
/* ============================================================= */
-- NOTE : default privileges must be executed logged in as each user, this means we are
-- giving privileges to role dev_schema_users for all objects created in the future by logged in user.
-- Doing it logged in as dev_schema_users (for all users at once) DOES NOT WORK

-- And it has to be done for each schema where new objects will be created



SET ROLE emea_su; -- (when DBAs are doing something)
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;


SET ROLE emea_cust_360_admin; -- (when ADMIN is doing something)
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TABLES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON SEQUENCES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON FUNCTIONS TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON ROUTINES TO etl_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TYPES TO etl_app_user;


SET ROLE etl_app_user; -- if etl_app_user (through scheduled python jobs) creates any new objects in any schema
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT SELECT ON TABLES TO dev_schema_users;