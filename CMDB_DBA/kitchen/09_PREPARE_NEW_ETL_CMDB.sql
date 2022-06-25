/*=========================================================================================================*/
/*                           !!! Connect to ETL_CMDB database as ADMIN in new DEV !!!                      */
/*=========================================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

-- STEP 1) ==== PREPARE NEW DEV (create foreign server pointing to OLD DEV)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

select * from pg_foreign_server;


-- !!! IMPORTANT - Pay attention - foreign server must point to OLD DEV
-- create a new foreign server pointing to OLD kitchens
-- parameter host must point to instance from which we want to move kitchens from
CREATE SERVER fgnsrv_newsub_2_oldsub
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
            host 'emea-cust-360-dev1-cluster.cluster-cct3lkcjopzt.us-west-2.rds.amazonaws.com'
            , port '5432'
            , dbname 'etl_cmdb'
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

-- importing the definition for sequences, tables, routines and views from OLD kitchen to NEW kitchen as foreign tables
/*
drop foreign table admin.tbl_kitchen_mat_view;
drop foreign table admin.tbl_kitchen_routine;
drop foreign table admin.tbl_kitchen_sequence;
drop foreign table admin.tbl_kitchen_table;
drop foreign table admin.tbl_kitchen_view;
*/
IMPORT FOREIGN SCHEMA admin LIMIT TO (tbl_kitchen_routine, tbl_kitchen_sequence, tbl_kitchen_table, tbl_kitchen_view, tbl_kitchen_mat_view)
    FROM SERVER fgnsrv_newsub_2_oldsub INTO admin;



-- STEP 2) ==== CREATE procedure for moving kitchens
-- this is the procedure which will recreate all table structures from the old DEV to the new DEV, and move data
create or replace procedure admin.reimport_foreign_schema(p_schema_name VARCHAR, p_owner VARCHAR)
	language plpgsql
as $$
DECLARE
    v_fgn_schema_name TEXT;
    v_statement TEXT;
    rec_statement RECORD;

    v_max_rpad INTEGER;
    -- countstbl_kitchen_table
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

    RAISE NOTICE '[%] OBJECTS MIGRATION :',upper(p_schema_name);

    -- move structure from old kitchen to new kitchen
    -- a) sequences

        RAISE NOTICE '[%] Creating sequences',upper(p_schema_name);
        FOR rec_statement IN
            (
                SELECT
                    s.*
                    , FORMAT('ALTER SEQUENCE %s."%s" OWNER TO %s;',s.schema_name,s.sequence_name,s.sequence_owner) q3
                FROM admin.tbl_kitchen_sequence s
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
                , FORMAT('ALTER TABLE %s."%s" OWNER TO %s;',t.schema_name,t.table_name,t.table_owner) q3
            FROM admin.tbl_kitchen_table t
            -- !!! here name the kitchen from which to recreate the tables
            WHERE t.schema_name = p_schema_name
        )
    LOOP
        v_statement := rec_statement.table_ddl;
        EXECUTE(v_statement);
        v_statement := rec_statement.q3;
        EXECUTE(v_statement);
        v_tbl_cnt := v_tbl_cnt + 1;
    END LOOP;

    -- c) views

        RAISE NOTICE '[%] Creating views',upper(p_schema_name);
        FOR rec_statement IN
            (
                SELECT
                    v.*
                    , FORMAT('ALTER VIEW %s."%s" OWNER TO %s;',v.schema_name,v.view_name,v.view_owner) q3
                FROM admin.tbl_kitchen_view v
                -- !!! here name the kitchen from which to recreate the views
                WHERE v.schema_name = p_schema_name
                ORDER BY v.view_name
            )
        LOOP
            v_statement := rec_statement.view_ddl;
            EXECUTE(v_statement);
            v_statement := rec_statement.q3;
            EXECUTE(v_statement);
            v_vew_cnt := v_vew_cnt + 1;
        END LOOP;


    -- d) materialized views
        RAISE NOTICE '[%] Creating materialized views',upper(p_schema_name);
        FOR rec_statement IN
            (
                SELECT
                    v.*
                    , FORMAT('ALTER MATERIALIZED VIEW %s."%s" OWNER TO %s;',v.schema_name,v.view_name,v.view_owner) q3
                FROM admin.tbl_kitchen_mat_view v
                -- !!! here name the kitchen from which to recreate the views
                WHERE v.schema_name = p_schema_name
                ORDER BY v.view_name
            )
        LOOP
            v_statement := rec_statement.view_ddl;
            EXECUTE(v_statement);
            v_statement := rec_statement.q3;
            EXECUTE(v_statement);
            v_mvw_cnt := v_mvw_cnt + 1;
        END LOOP;


    -- e) routines
    RAISE NOTICE '[%] Creating routines',upper(p_schema_name);
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
                            , r.routine_owner
                        ) q3
            FROM admin.tbl_kitchen_routine r
            -- !!! here name the kitchen from which to recreate routines
            WHERE r.schema_name = p_schema_name
        )
    LOOP
        v_statement := rec_statement.routine_ddl;
        EXECUTE(v_statement);
        v_statement := rec_statement.q3;
        EXECUTE(v_statement);
        v_rtn_cnt := v_rtn_cnt + 1;
    END LOOP;


    -- move data only
    RAISE NOTICE '[%] DATA MIGRATION :',upper(p_schema_name);

    v_curr_table        := 0;
    v_total_rows_source := 0;
    v_total_rows_target := 0;
    v_tbl_ok            := 0;
    v_tbl_failed        := 0;

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
            WHERE t.schema_name = p_schema_name
        )
    LOOP
        v_rows_source := 0;
        v_rows_target := 0;

        EXECUTE(rec_statement.q2) INTO v_rows_source;
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
    RAISE NOTICE '[%] === OBJECTS ====',upper(p_schema_name);
    RAISE NOTICE '[%] sequences  [%]',upper(p_schema_name), LPAD(v_seq_cnt::text, 3, '0');
    RAISE NOTICE '[%] tables     [%]',upper(p_schema_name), LPAD(v_tbl_cnt::text, 3, '0');
    RAISE NOTICE '[%] views      [%]',upper(p_schema_name), LPAD(v_vew_cnt::text, 3, '0');
    RAISE NOTICE '[%] mat. views [%]',upper(p_schema_name), LPAD(v_mvw_cnt::text, 3, '0');
    RAISE NOTICE '[%] routines   [%]',upper(p_schema_name), LPAD(v_rtn_cnt::text, 3, '0');
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