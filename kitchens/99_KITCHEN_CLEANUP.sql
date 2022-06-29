/*=========================================================================================================*/
/*                                 !!! Connect as EMEA_SU to new DEV !!!                                   */
/*=========================================================================================================*/

-- !!! IMPORTANT - the purpose of this script is to remove everything related to kitchens in new DEV
-- !!! IMPORTANT - after this, DEV will be initialized to a state before running any kitchen scripts
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

-- select * from pg_user_mappings;
DROP USER MAPPING IF EXISTS FOR emea_cust_360_admin SERVER fgnsrv_sub_2_pub;
DROP USER MAPPING IF EXISTS FOR emea_cust_360_admin SERVER fgnsrv_newsub_2_oldsub;

drop foreign table if exists admin.tbl_kitchen_mat_view;
drop foreign table if exists admin.tbl_kitchen_routine;
drop foreign table if exists admin.tbl_kitchen_sequence;
drop foreign table if exists admin.tbl_kitchen_table;
drop foreign table if exists admin.tbl_kitchen_view;
drop procedure if exists admin.reimport_foreign_schema(p_schema_name varchar, p_owner varchar);

-- select * from pg_foreign_server;
DROP SERVER IF EXISTS fgnsrv_sub_2_pub;
DROP SERVER IF EXISTS fgnsrv_newsub_2_oldsub;

-- select * from pg_extension;
DROP EXTENSION IF EXISTS postgres_fdw;

DROP SCHEMA IF EXISTS patrick CASCADE;
DROP SCHEMA IF EXISTS pavi CASCADE;
DROP SCHEMA IF EXISTS renato CASCADE;
DROP SCHEMA IF EXISTS sergi CASCADE;
DROP SCHEMA IF EXISTS carlos CASCADE;
DROP SCHEMA IF EXISTS imane CASCADE;
DROP SCHEMA IF EXISTS santiago CASCADE;
DROP SCHEMA IF EXISTS zhivko CASCADE;
DROP SCHEMA IF EXISTS abhishek CASCADE;
DROP SCHEMA IF EXISTS joanne CASCADE;
DROP SCHEMA IF EXISTS sonali CASCADE;
DROP SCHEMA IF EXISTS nagamalliswara CASCADE;
DROP SCHEMA IF EXISTS ashwini CASCADE;
DROP SCHEMA IF EXISTS manuele CASCADE;


DROP SCHEMA IF EXISTS dev_data_in CASCADE;
DROP SCHEMA IF EXISTS dev_crm CASCADE;
DROP SCHEMA IF EXISTS dev_data_out CASCADE;
DROP SCHEMA IF EXISTS dev_cmdb_catalog CASCADE;

DROP SCHEMA IF EXISTS dev_apj_data_in CASCADE;
DROP SCHEMA IF EXISTS dev_apj_crm CASCADE;
DROP SCHEMA IF EXISTS dev_apj_data_out CASCADE;

DROP DATABASE IF EXISTS etl_cmdb;
DROP DATABASE IF EXISTS uat_cmdb;

DROP USER IF EXISTS patrick;
DROP USER IF EXISTS pavi;
DROP USER IF EXISTS renato;
DROP USER IF EXISTS sergi;
DROP USER IF EXISTS carlos;
DROP USER IF EXISTS imane;
DROP USER IF EXISTS santiago;
DROP USER IF EXISTS zhivko;
DROP USER IF EXISTS abhishek;
DROP USER IF EXISTS joanne;
DROP USER IF EXISTS sonali;
DROP USER IF EXISTS nagamalliswara;
DROP USER IF EXISTS ashwini;
DROP USER IF EXISTS manuele;


DROP USER IF EXISTS dev_app_user;
DROP USER IF EXISTS etl_app_user;
DROP USER IF EXISTS uat_app_user;



REVOKE CONNECT ON DATABASE emea_cust_360 FROM dev_schema_users;

-- REVOKE usage FROM schemas
REVOKE USAGE ON SCHEMA alpaca FROM dev_schema_users;
REVOKE USAGE ON SCHEMA bi FROM dev_schema_users;
REVOKE USAGE ON SCHEMA data_in FROM dev_schema_users;
REVOKE USAGE ON SCHEMA crm FROM dev_schema_users;
REVOKE USAGE ON SCHEMA data_out FROM dev_schema_users;
REVOKE USAGE ON SCHEMA cmdb_catalog FROM dev_schema_users;



-- REVOKE SELECT only FROM replicated schemas
REVOKE SELECT ON ALL TABLES IN SCHEMA alpaca FROM dev_schema_users;
REVOKE SELECT ON ALL TABLES IN SCHEMA bi FROM dev_schema_users;
REVOKE SELECT ON ALL TABLES IN SCHEMA data_in FROM dev_schema_users;
REVOKE SELECT ON ALL TABLES IN SCHEMA crm FROM dev_schema_users;
REVOKE SELECT ON ALL TABLES IN SCHEMA data_out FROM dev_schema_users;
REVOKE SELECT ON ALL TABLES IN SCHEMA cmdb_catalog FROM dev_schema_users;





-- ==== 2) REVOKE default privileges for all future tables FROM role dev_schema_users
-- NOTE : default privileges must be executed logged in as each user, this means we are
-- giving privileges FROM role dev_schema_users for all objects created in the future by logged in user.
-- Doing it logged in as dev_schema_users (for all users at once) DOES NOT WORK

-- And it has FROM be done for each schema where new objects will be created




SET ROLE emea_su; -- if EMEA_SU creates any new objects in any schema (usually manually by DBA)
-- in replicated schemas
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;



SET ROLE emea_cust_360_admin; -- if ADMIN creates any new objects in any schema
-- in replicated schemas (through DDL Sync)
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE ALL PRIVILEGES ON TABLES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE ALL PRIVILEGES ON SEQUENCES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE EXECUTE ON FUNCTIONS FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE EXECUTE ON ROUTINES FROM dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog REVOKE ALL PRIVILEGES ON TYPES FROM dev_schema_users;

SET ROLE emea_su;
DROP ROLE IF EXISTS dev_schema_users;

