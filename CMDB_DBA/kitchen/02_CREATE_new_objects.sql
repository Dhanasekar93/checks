/*============================================================================================*/
/*            !!! Connect to EMEA_CUST_360 DB as EMEA_SU in NEW DEV !!!                       */
/*============================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

SELECT FORMAT('SELECT pg_cancel_backend(%s);',pid) cancel_pid, a.*
FROM pg_stat_activity a
WHERE state !='idle'
ORDER BY usename, application_name, backend_start DESC;

/*======================================================================================*/
/*================== 1. DB, SCHEMAS and USERS IN NEW DEV  ==============================*/
/*======================================================================================*/

-- 1.1) ==== create the foreign data wrapper extension
-- see the list of extensions
select * from pg_extension;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- 1.2) ==== create the foreign server pointing to MASTER, make it readonly (updatable=false)
-- see all the foreign servers defined
select * from pg_foreign_server;
drop server if exists fgnsrv_sub_2_pub;
CREATE SERVER fgnsrv_sub_2_pub
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
--            host 'emea-cust-360-v1.cluster-cct3lkcjopzt.us-west-2.rds.amazonaws.com'
            host 'emea-cust-360-dev1pd-cluster.cluster-cct3lkcjopzt.us-west-2.rds.amazonaws.com'
            , port '5432'
            , dbname 'emea_cust_360'
            , updatable 'true');

-- here we increase the rows fetch size, the default is 100, increasing to 50000. Could reduce networking
ALTER SERVER fgnsrv_sub_2_pub
    OPTIONS (fetch_size '50000');

-- here we force foreign server to use remote query analysis and estimate and hopefully speeding up the complex query execution
ALTER SERVER fgnsrv_sub_2_pub
  OPTIONS (ADD use_remote_estimate 'true');


-- 1.3) ==== create the user mapping for emea_cust_360_admin, pointing to existing user on MASTER DB

-- see all the user mappings defined
select * from pg_user_mappings;
drop user mapping if exists FOR emea_cust_360_admin server fgnsrv_sub_2_pub;
CREATE USER MAPPING FOR emea_cust_360_admin
        SERVER fgnsrv_sub_2_pub
        OPTIONS (user 'emea_cust_360_fgn', password 'ef!THa4nQcA>;Pw2');

CREATE USER MAPPING FOR emea_su
        SERVER fgnsrv_sub_2_pub
        OPTIONS (user 'emea_cust_360_fgn', password 'ef!THa4nQcA>;Pw2');


-- 1.4) ==== Create UAT and ETL databases
/* ==================== DATABASES ==================== */
create database uat_cmdb;
create database etl_cmdb;

-- 1.5) ==== Create foreign table pointing to PROD admin.tbl_kitchen table
-- we need this table for kitchen migration automation and create new kitchen automation (there's a list of all kitchens + dev_ schemas)
-- this table will also be updated from the new DEV

-- first we drop the table that came with copying instance
DROP TABLE IF EXISTS admin.tbl_kitchen;
DROP FOREIGN TABLE IF EXISTS admin.tbl_kitchen;

-- and then we create foreign table pointing to PROD
IMPORT FOREIGN SCHEMA admin LIMIT TO (tbl_kitchen)
    FROM SERVER fgnsrv_sub_2_pub INTO admin;


-- 1.6) ==== Create "dev_" schemas and kitchens

-- first we create the SP that will create all schemas, users and passwords
-- set schema owners and set search path to users (all of this using admin.tbl_kitchen foreign table)
create or replace procedure admin.sp_create_schema_and_users()
	language plpgsql
as $$
DECLARE
    rec_statement RECORD;
    v_statement TEXT;

    err_context  text;
    v_err_context text;
BEGIN
    RAISE NOTICE '[%] Creating schemas',"current_user"();
    FOR rec_statement IN
        (
            SELECT
                format('CREATE SCHEMA %s;',schema_name) ddl_create_schema
            FROM admin.tbl_kitchen
            WHERE active = 1
            ORDER BY migration_order
        )
    LOOP
        v_statement := rec_statement.ddl_create_schema;
        EXECUTE(v_statement);
    END LOOP;

    RAISE NOTICE '[%] Creating users with passwords and setting search path',"current_user"();
    FOR rec_statement IN
        (
            WITH
                users AS
                    (
                        SELECT distinct schema_owner, schema_password, search_path
                        FROM admin.tbl_kitchen
                        WHERE active = 1
                    )
            SELECT
                format('CREATE USER %s PASSWORD ''%s'';',u.schema_owner,u.schema_password) ddl_create_user
                , format('ALTER ROLE %s SET search_path TO %s;',u.schema_owner,u.search_path) ddl_set_search_path
            FROM users u
        )
    LOOP
        v_statement := rec_statement.ddl_create_user;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_set_search_path;
        EXECUTE(v_statement);
    END LOOP;

    RAISE NOTICE '[%] Setting schema owners',"current_user"();
    FOR rec_statement IN
        (
            SELECT
                format('ALTER SCHEMA %s owner to %s;',schema_name,schema_owner) ddl_set_schema_owner
            FROM admin.tbl_kitchen
            WHERE active = 1
            ORDER BY migration_order
        )
    LOOP
        v_statement := rec_statement.ddl_set_schema_owner;
        EXECUTE(v_statement);
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;

-- then we call it
call admin.sp_create_schema_and_users();

/*
SELECT string_agg (substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$&()*+,-./;<=>?', ceil (random() * 79)::integer, 1), '')
FROM generate_series(1, 16);
*/

-- 1.7) ==== Create ROLE for developers
CREATE ROLE dev_schema_users;

-- 1.8) ==== Create etl_app_user for running the etl_cmdb jobs
CREATE USER etl_app_user PASSWORD 'x8NJS<@0V88;H2hd';

-- 1.9) ==== Create uat_app_user for running the uat_cmdb jobs
CREATE USER uat_app_user PASSWORD '5x*159vNjK!Q?ose';

