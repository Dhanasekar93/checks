/*=============================================================================================*/
/*         !!! Connect to EMEA_CUST_360 DB as EMEA_CUST_360_ADMIN in new DEV !!!               */
/*=============================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

-- STEP 1) ==== PREPARE NEW DEV (create foreign server pointing to OLD DEV)

select * from pg_foreign_server;
-- we should already have one foreign server (fgnsrv_sub_2_pub)
-- CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- !!! IMPORTANT - Pay attention - foreign server must point to OLD DEV
-- create a new foreign server pointing to OLD kitchens
-- parameter host must point to instance from which we want to move kitchens from
CREATE SERVER fgnsrv_newsub_2_oldsub
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
            host 'emea-cust-360-dev1-cluster.cluster-cct3lkcjopzt.us-west-2.rds.amazonaws.com'
            , port '5432'
            , dbname 'emea_cust_360'
            , updatable 'false');

-- here we increase the rows fetch size, the default is 100, increasing to 50000. Could reduce networking
ALTER SERVER fgnsrv_newsub_2_oldsub
    OPTIONS (fetch_size '50000');

-- here we force foreign server to use remote qury analysis and estimate and hopefully speeding up the complex query execution
ALTER SERVER fgnsrv_newsub_2_oldsub
  OPTIONS (ADD use_remote_estimate 'true');

-- create the user mapping for emea_cust_360_admin, pointing to existing user on remote DB
select * from pg_user_mappings;

CREATE USER MAPPING FOR emea_cust_360_admin
        SERVER fgnsrv_newsub_2_oldsub
        OPTIONS (user 'emea_cust_360_fgn', password 'ef!THa4nQcA>;Pw2');

CREATE USER MAPPING FOR emea_su
        SERVER fgnsrv_newsub_2_oldsub
        OPTIONS (user 'emea_cust_360_fgn', password 'ef!THa4nQcA>;Pw2');

-- importing the definition for sequences, tables, routines and views from OLD kitchen to NEW kitchen as foreign tables

drop foreign table if exists admin.tbl_kitchen_mat_view;
drop foreign table if exists admin.tbl_kitchen_routine;
drop foreign table if exists admin.tbl_kitchen_sequence;
drop foreign table if exists admin.tbl_kitchen_table;
drop foreign table if exists admin.tbl_kitchen_view;

IMPORT FOREIGN SCHEMA admin LIMIT TO (tbl_kitchen_routine, tbl_kitchen_sequence, tbl_kitchen_table, tbl_kitchen_view, tbl_kitchen_mat_view)
    FROM SERVER fgnsrv_newsub_2_oldsub INTO admin;



-- STEP 2) ==== CREATE procedure for moving kitchens
-- this is the procedure which will recreate all table structures from the old DEV to the new DEV
create or replace procedure admin.reimport_foreign_schema(p_schema_name VARCHAR, p_owner VARCHAR)
	language plpgsql
as $$
DECLARE
    v_statement TEXT;
    rec_statement RECORD;

    -- counts
    v_seq_cnt BIGINT;
    v_tbl_cnt BIGINT;
    v_prt_cnt  BIGINT;

    err_context  text;
    v_err_context text;
BEGIN
    v_seq_cnt := 0;
    v_tbl_cnt := 0;
    v_prt_cnt := 0;

    RAISE NOTICE '[%] OBJECTS MIGRATION :',upper(p_schema_name);

    -- move structure from old kitchen to new kitchen
    -- a) sequences

    RAISE NOTICE '[%] Creating sequences',upper(p_schema_name);
    FOR rec_statement IN
        (
            SELECT
                s.*
                , FORMAT('ALTER SEQUENCE %s."%s" OWNER TO %s;',s.schema_name,s.sequence_name,k.schema_owner) q3
            FROM admin.tbl_kitchen_sequence s
            JOIN admin.tbl_kitchen k
                ON k.schema_name = s.schema_name
            -- !!! here name the kitchen from which to recreate the sequences
            WHERE s.schema_name = p_schema_name
        )
    LOOP
        v_statement := rec_statement.sequence_ddl;
        EXECUTE(v_statement);
        v_statement := rec_statement.q3;
        EXECUTE(v_statement);
        v_seq_cnt := v_seq_cnt + 1;
    END LOOP;


    -- b) tables
    RAISE NOTICE '[%] Creating tables',upper(p_schema_name);
    FOR rec_statement IN
        (
            SELECT
                t.*
                , FORMAT('ALTER TABLE %s."%s" OWNER TO %s;',t.schema_name,t.table_name,k.schema_owner) q3
            FROM admin.tbl_kitchen_table t
            JOIN admin.tbl_kitchen k
                ON k.schema_name = t.schema_name
            WHERE
                -- !!! here name the kitchen from which to recreate the tables
                t.schema_name = p_schema_name
                -- only tables
                AND t.is_partition = false
        )
    LOOP
        v_statement := rec_statement.table_ddl;
        EXECUTE(v_statement);
        v_statement := rec_statement.q3;
        EXECUTE(v_statement);
        v_tbl_cnt := v_tbl_cnt + 1;
    END LOOP;

    -- c) partitions
    RAISE NOTICE '[%] Creating partitions',upper(p_schema_name);
    FOR rec_statement IN
        (
            SELECT
                t.*
                , FORMAT('ALTER TABLE %s."%s" OWNER TO %s;',t.schema_name,t.table_name,k.schema_owner) q3
            FROM admin.tbl_kitchen_table t
            JOIN admin.tbl_kitchen k
                ON k.schema_name = t.schema_name
            WHERE
                -- !!! here name the kitchen from which to recreate the tables
                t.schema_name = p_schema_name
                -- only partitions
                AND t.is_partition = true
        )
    LOOP
        v_statement := rec_statement.table_ddl;
        EXECUTE(v_statement);
        v_statement := rec_statement.q3;
        EXECUTE(v_statement);
        v_prt_cnt := v_prt_cnt + 1;
    END LOOP;

    RAISE NOTICE '[%] MIGRATION SUMMARY :',upper(p_schema_name);
    RAISE NOTICE '[%] === OBJECTS ====',upper(p_schema_name);
    RAISE NOTICE '[%] sequences  [%]',upper(p_schema_name), LPAD(v_seq_cnt::text, 3, '0');
    RAISE NOTICE '[%] tables     [%]',upper(p_schema_name), LPAD(v_tbl_cnt::text, 3, '0');
    RAISE NOTICE '[%] partitions [%]',upper(p_schema_name), LPAD(v_prt_cnt::text, 3, '0');
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;

-- STEP 3) ==== CREATE procedure for moving kitchen views and materialized views
-- this is the procedure which will recreate all views and materialized views from the old DEV to the new DEV
drop procedure if exists admin.reimport_foreign_schema_views(p_schema_name VARCHAR, p_owner VARCHAR);
create or replace procedure admin.reimport_foreign_schema_views()
	language plpgsql
as $$
DECLARE
    v_statement TEXT;
    rec_statement RECORD;

    -- counts
    v_vew_cnt BIGINT;
    v_mvw_cnt BIGINT;
    v_rtn_cnt BIGINT;

    err_context  text;
    v_err_context text;
BEGIN
    v_vew_cnt := 0;
    v_mvw_cnt := 0;
    v_rtn_cnt := 0;

    RAISE NOTICE '[%] OBJECTS MIGRATION :','ALL SCHEMAS';

    -- move structure from old kitchen to new kitchen

    -- d) materialized views
        RAISE NOTICE '[%] Creating materialized views','ALL SCHEMAS';
        FOR rec_statement IN
            (
                SELECT
                    v.*
                    , FORMAT('ALTER MATERIALIZED VIEW %s."%s" OWNER TO %s;',v.schema_name,v.view_name,k.schema_owner) q3
                FROM admin.tbl_kitchen_mat_view v
                JOIN admin.tbl_kitchen k
                    ON k.schema_name = v.schema_name
                -- !!! here name the kitchen from which to recreate the views
                ORDER BY v.view_name
            )
        LOOP
            v_statement := rec_statement.view_ddl;
            EXECUTE(v_statement);
            v_statement := rec_statement.q3;
            EXECUTE(v_statement);
            v_mvw_cnt := v_mvw_cnt + 1;
        END LOOP;

    -- e) views

        RAISE NOTICE '[%] Creating views','ALL SCHEMAS';
        FOR rec_statement IN
            (
                SELECT
                    v.*
                    , FORMAT('ALTER VIEW %s."%s" OWNER TO %s;',v.schema_name,v.view_name,k.schema_owner) q3
                FROM admin.tbl_kitchen_view v
                JOIN admin.tbl_kitchen k
                    ON k.schema_name = v.schema_name
                ORDER BY v.id
            )
        LOOP
            v_statement := rec_statement.view_ddl;
            EXECUTE(v_statement);
            v_statement := rec_statement.q3;
            EXECUTE(v_statement);
            v_vew_cnt := v_vew_cnt + 1;
        END LOOP;

    -- f) routines
    RAISE NOTICE '[%] Creating routines','ALL SCHEMAS';
    FOR rec_statement IN
        (
            SELECT
                r.*
                ,
                    FORMAT
                        (
                            'ALTER FUNCTION %s.%s(%s) OWNER TO %s;'
                            , r.schema_name
                            , r.routine_name
                            , COALESCE(r.routine_arguments,'')
                            , k.schema_owner
                        ) q3
            FROM admin.tbl_kitchen_routine r
            JOIN admin.tbl_kitchen k
                ON k.schema_name = r.schema_name

        )
    LOOP
        v_statement := rec_statement.routine_ddl;
        EXECUTE(v_statement);
        v_statement := rec_statement.q3;
        EXECUTE(v_statement);
        v_rtn_cnt := v_rtn_cnt + 1;
    END LOOP;

    RAISE NOTICE '[%] MIGRATION SUMMARY :',upper('ALL SCHEMAS');
    RAISE NOTICE '[%] === OBJECTS ====',upper('ALL SCHEMAS');
    RAISE NOTICE '[%] mat. views [%]',upper('ALL SCHEMAS'), LPAD(v_mvw_cnt::text, 3, '0');
    RAISE NOTICE '[%] views      [%]',upper('ALL SCHEMAS'), LPAD(v_vew_cnt::text, 3, '0');
    RAISE NOTICE '[%] routines   [%]',upper('ALL SCHEMAS'), LPAD(v_rtn_cnt::text, 3, '0');
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;

-- STEP 4) ==== CREATE procedure for moving kitchen data
-- this is the procedure which will migrate data from the old DEV to the new DEV
create or replace procedure admin.reimport_foreign_schema_data(p_schema_name VARCHAR, p_owner VARCHAR)
	language plpgsql
as $$
DECLARE
    v_fgn_schema_name TEXT;
    v_statement TEXT;
    rec_statement RECORD;

    v_max_rpad INTEGER;
    -- counts
    v_seq_cnt BIGINT;
    v_tbl_cnt BIGINT;
    v_vew_cnt BIGINT;
    v_mvw_cnt BIGINT;
    v_rtn_cnt BIGINT;

    v_curr_table BIGINT;

    v_rows_source BIGINT;
    v_rows_target BIGINT;
    v_total_rows_source BIGINT;
    v_total_rows_target BIGINT;

    v_tbl_ok BIGINT;
    v_tbl_failed BIGINT;

    err_context  text;
    v_err_context text;
BEGIN
    v_seq_cnt := 0;
    v_tbl_cnt := 0;
    v_vew_cnt := 0;
    v_mvw_cnt := 0;
    v_rtn_cnt := 0;

    SELECT MAX(length(table_name)) INTO v_max_rpad FROM admin.tbl_kitchen_table WHERE schema_name = p_schema_name;

    v_fgn_schema_name := FORMAT('fgn_%s',p_schema_name);
    RAISE NOTICE '[%] Creating foreign schema %',upper(p_schema_name), v_fgn_schema_name;

    -- create temporary schema with foreign tables
    v_statement := FORMAT('CREATE SCHEMA %s;',v_fgn_schema_name);
    EXECUTE(v_statement);

    -- import all tables from the source as foreign tables
    v_statement := FORMAT('IMPORT FOREIGN SCHEMA %s FROM SERVER fgnsrv_newsub_2_oldsub INTO %s;',p_schema_name,v_fgn_schema_name);
    EXECUTE(v_statement);

    RAISE NOTICE '[%] DATA MIGRATION :',upper(p_schema_name);

    v_curr_table        := 0;
    v_total_rows_source := 0;
    v_total_rows_target := 0;
    v_tbl_ok            := 0;
    v_tbl_failed        := 0;

    SELECT COUNT(*) INTO v_tbl_cnt FROM admin.tbl_kitchen_table WHERE schema_name = p_schema_name AND is_partition = false;

    FOR rec_statement IN
        (
            SELECT
                t.schema_name
                , t.table_name
                , t.estimate_row_count
                ,
                FORMAT
                    (
                        'INSERT INTO %s."%s" SELECT * FROM %s."%s";'
                        , t.schema_name
                        , t.table_name
                        , v_fgn_schema_name
                        , t.table_name
                    ) q1
                ,
                FORMAT ('SELECT COUNT(*) FROM %s."%s"',v_fgn_schema_name,t.table_name) q2
            FROM admin.tbl_kitchen_table t
            -- !!! here name the foreign schema from which to recreate the tables
            WHERE
                t.schema_name = p_schema_name
                -- we do not insert directly into partitions because
                -- foreign data wrapper does not see partitions, instead
                -- we insert into parent table only
                and t.is_partition = false
        )
    LOOP
        v_rows_source := 0;
        v_rows_target := 0;

        v_statement := rec_statement.q2;
        EXECUTE(v_statement) INTO v_rows_source;
        v_total_rows_source := v_total_rows_source + v_rows_source;

        v_statement := rec_statement.q1;
        BEGIN
            EXECUTE(v_statement);
            GET DIAGNOSTICS v_rows_target = ROW_COUNT;
            v_total_rows_target := v_total_rows_target + v_rows_target;

            v_curr_table := v_curr_table + 1;
            v_tbl_ok := v_tbl_ok + 1;

            RAISE NOTICE '[%] %/% - OK - TABLE % - ROWS src|tgt : [%|%]'
                , upper(p_schema_name)
                , LPAD(v_curr_table::text, 3, '0')
                , LPAD(v_tbl_cnt::text, 3, '0')
                , RPAD(rec_statement.table_name,v_max_rpad)
                , v_rows_source
                , v_rows_target;
        EXCEPTION
            WHEN OTHERS THEN
                v_curr_table := v_curr_table + 1;
                v_tbl_failed := v_tbl_failed + 1;

                RAISE NOTICE '[%] %/% - FAILED - TABLE %'
                    , upper(p_schema_name)
                    , LPAD(v_curr_table::text, 3, '0')
                    , LPAD(v_tbl_cnt::text, 3, '0')
                    , rec_statement.table_name;

        END;
    END LOOP;

    -- remove temp. foreign schema
    RAISE NOTICE '[%] Dropping foreign schema %',upper(p_schema_name), v_fgn_schema_name;
    v_statement := 'DROP SCHEMA ' || v_fgn_schema_name || ' CASCADE';
    EXECUTE(v_statement);

    RAISE NOTICE '[%] MIGRATION SUMMARY :',upper(p_schema_name);
    RAISE NOTICE '[%] ===== DATA =====',upper(p_schema_name);
    RAISE NOTICE '[%] TBL.OK     [%]',upper(p_schema_name), LPAD(v_tbl_ok::text, 3, '0');
    RAISE NOTICE '[%] TBL.FAILED [%]',upper(p_schema_name), LPAD(v_tbl_failed::text, 3, '0');
    RAISE NOTICE '[%] TOTAL.SRC  [%]',upper(p_schema_name), v_total_rows_source;
    RAISE NOTICE '[%] TOTAL.TGT  [%]',upper(p_schema_name), v_total_rows_target;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;

-- STEP 5) ==== CREATE procedure for creating new kitchen
-- this is the procedure which will create new kitchen in new DEV, and assign all required privileges
create or replace function admin.fn_create_new_kitchen(p_kitchen_name VARCHAR) returns text
	language plpgsql
as $$
DECLARE
    v_statement TEXT;
    v_schema_password TEXT;

    c_default_privileges CONSTANT TEXT :=
        '
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON TABLES TO %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON SEQUENCES TO %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON ROUTINES TO %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON TYPES TO %s;
        ';

    v_next_id INTEGER;
    v_next_migration_order INTEGER;

    err_context  text;
    v_err_context text;
BEGIN
    v_statement := FORMAT('SET ROLE %s','emea_su');
    EXECUTE(v_statement);

    RAISE NOTICE '[%] Creating new schema',upper('emea_su');
    -- create temporary schema with foreign tables
    v_statement := FORMAT('CREATE SCHEMA %s;',p_kitchen_name);
    EXECUTE(v_statement);

    SELECT string_agg (substr('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!#$&()*+,-./;<=>?', ceil (random() * 79)::integer, 1), '')
    INTO v_schema_password
    FROM generate_series(1, 16);


    RAISE NOTICE '[%] Creating new user and password',upper('emea_su');

    v_statement := FORMAT('CREATE USER %s PASSWORD ''%s'';',p_kitchen_name,v_schema_password);
    EXECUTE(v_statement);
    v_statement := FORMAT('ALTER SCHEMA %s owner to %s;',p_kitchen_name,p_kitchen_name);
    EXECUTE(v_statement);
    v_statement := FORMAT('ALTER ROLE %s SET search_path TO %s;',p_kitchen_name,p_kitchen_name);
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Assigning privileges for new kitchen to role dev_schema_users',upper('emea_su');
    v_statement := FORMAT('GRANT USAGE ON SCHEMA %s TO dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);
    v_statement := FORMAT('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);
    v_statement := FORMAT('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);
    v_statement := FORMAT('GRANT ALL PRIVILEGES ON SCHEMA %s TO dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Creating default privileges in DEV_ schemas for role dev_shema_users',upper(p_kitchen_name);
    v_statement := FORMAT('SET ROLE %s',p_kitchen_name);
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Creating default privileges in new kitchen for role dev_shema_users',upper('emea_su');
    v_statement := FORMAT('SET ROLE %s','emea_su');
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Creating default privileges in new kitchen for role dev_shema_users',upper('emea_cust_360_admin');
    v_statement := FORMAT('SET ROLE %s','emea_cust_360_admin');
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Creating default privileges in new kitchen for role dev_shema_users',upper('dev_app_user');
    v_statement := FORMAT('SET ROLE %s','dev_app_user');
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Granting role dev_schema_user to new user',upper('emea_su');
    v_statement := FORMAT('SET ROLE %s','emea_su');
    EXECUTE(v_statement);
    v_statement := FORMAT('GRANT dev_schema_users TO %s',p_kitchen_name);
    EXECUTE(v_statement);

    /* =========== AT THE END, WE NEED TO UPDATE admin.tbl_kitchen IN PRODUCTION ===== */
    -- we do it using foreign table in DEV that is connected to the table in PROD
    RAISE NOTICE '[%] Updating the admin.tbl_kitchen in PROD',upper('emea_cust_360_admin');
    v_statement := FORMAT('SET ROLE %s','emea_cust_360_admin');
    EXECUTE(v_statement);

    SELECT max(id) + 1, max(migration_order) + 10
    INTO v_next_id, v_next_migration_order
    FROM admin.tbl_kitchen;

    INSERT INTO admin.tbl_kitchen
    (id, database_name, schema_name, schema_password, migration_order, is_developer, active)
    VALUES
        (
            v_next_id
            , 'emea_cust_360'
            , p_kitchen_name
            , v_schema_password
            , v_next_migration_order
            , 1
            , 1
        );

    v_statement := FORMAT('SET ROLE %s','emea_su');
    EXECUTE(v_statement);

    RETURN v_schema_password;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;


-- STEP 5) ==== CREATE procedure for removing kitchen
-- this is the procedure which will drop schema, privileges and user for given kitchen in DEV
create or replace procedure admin.sp_remove_kitchen(p_kitchen_name VARCHAR)
	language plpgsql
as $$
DECLARE
    v_statement TEXT;
    v_schema_password TEXT;

    c_default_privileges CONSTANT TEXT :=
        '
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL PRIVILEGES ON TABLES FROM %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL PRIVILEGES ON SEQUENCES FROM %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE EXECUTE ON FUNCTIONS FROM %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE EXECUTE ON ROUTINES FROM %s;
            ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL PRIVILEGES ON TYPES FROM %s;
        ';

    err_context  text;
    v_err_context text;
BEGIN
    v_statement := FORMAT('SET ROLE %s','emea_su');
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Revoking privileges for new kitchen from role dev_schema_users',upper('emea_su');
    v_statement := FORMAT('REVOKE USAGE ON SCHEMA %s FROM dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);
    v_statement := FORMAT('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s FROM dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);
    v_statement := FORMAT('REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s FROM dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);
    v_statement := FORMAT('REVOKE ALL PRIVILEGES ON SCHEMA %s FROM dev_schema_users;',p_kitchen_name);
    EXECUTE(v_statement);



    RAISE NOTICE '[%] Revoke default privileges in new kitchen from role dev_schema_users',upper('emea_su');
    v_statement := FORMAT('SET ROLE %s','emea_su');
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Revoke default privileges in new kitchen from role dev_schema_users',upper('emea_cust_360_admin');
    v_statement := FORMAT('SET ROLE %s','emea_cust_360_admin');
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Revoke default privileges in new kitchen for role dev_schema_users',upper('dev_app_user');
    v_statement := FORMAT('SET ROLE %s','dev_app_user');
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);


    RAISE NOTICE '[%] Revoking default privileges in DEV_ schemas for role dev_schema_users',upper(p_kitchen_name);
    v_statement := FORMAT('SET ROLE %s',p_kitchen_name);
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
            , 'dev_data_in', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
            , 'dev_data_out', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
            , 'dev_crm', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
            , 'dev_cmdb_catalog', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
            , 'dev_apj_data_in', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
            , 'dev_apj_data_out', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
            , 'dev_apj_crm', 'dev_schema_users'
        );
    EXECUTE(v_statement);

    v_statement := format
        (
            c_default_privileges
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
            , p_kitchen_name, 'dev_schema_users'
        );
    EXECUTE(v_statement);



    /* =========== AT THE END, WE NEED TO UPDATE admin.tbl_kitchen IN PRODUCTION ===== */
    -- we do it using foreign table in DEV that is connected to the table in PROD
    RAISE NOTICE '[%] Updating the admin.tbl_kitchen in PROD',upper('emea_cust_360_admin');
    v_statement := FORMAT('SET ROLE %s','emea_cust_360_admin');
    EXECUTE(v_statement);

    UPDATE admin.tbl_kitchen
    SET active = 0
    WHERE schema_name = p_kitchen_name;

    RAISE NOTICE '[%] Dropping kitchen and user',upper('emea_su');

    v_statement := FORMAT('SET ROLE %s','emea_su');
    EXECUTE(v_statement);

    v_statement := FORMAT('DROP SCHEMA %s CASCADE;',p_kitchen_name);
    EXECUTE(v_statement);

    v_statement := FORMAT('DROP USER %s;',p_kitchen_name);
    EXECUTE(v_statement);

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;

