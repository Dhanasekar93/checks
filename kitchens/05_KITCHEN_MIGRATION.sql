/*=========================================================================================================*/
/*          !!! Connect to EMEA_CUST_360 DB as EMEA_CUST_360_ADMIN in NEW DEV !!!                          */
/*=========================================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

-- 1) ==== MOVING KITCHENS
-- create a SP for aut. migration of all active kitchens
create or replace procedure admin.sp_migrate_all_active_foreign_schemas()
	language plpgsql
as $$
DECLARE
    rec_statement RECORD;
    v_statement TEXT;

    err_context  text;
    v_err_context text;
BEGIN
    RAISE NOTICE '[%] Migrating all active schemas from old DEV to new DEV',"current_user"();
    FOR rec_statement IN
        (
            SELECT
                schema_name
                , format('call admin.reimport_foreign_schema(''%s'',''%s'');',schema_name,schema_owner) ddl_migrate_schema
            FROM admin.tbl_kitchen
            WHERE active = 1
            ORDER BY migration_order
        )
    LOOP
        v_statement := rec_statement.ddl_migrate_schema;
        EXECUTE(v_statement);
    END LOOP;

    RAISE NOTICE '[%] Migrating all views, materialized views and routines from old DEV to new DEV',"current_user"();
    call admin.reimport_foreign_schema_views();

    RAISE NOTICE '[%] Migrating all data from old DEV to new DEV',"current_user"();
    FOR rec_statement IN
        (
            SELECT
                schema_name
                , format('call admin.reimport_foreign_schema_data(''%s'',''%s'');',schema_name,schema_owner) ddl_migrate_schema
            FROM admin.tbl_kitchen
            WHERE active = 1
            ORDER BY migration_order
        )
    LOOP
        v_statement := rec_statement.ddl_migrate_schema;
        EXECUTE(v_statement);
    END LOOP;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;



-- calling procedure for aut. migration of all active schemas from old DEV to new DEV
call admin.sp_migrate_all_active_foreign_schemas();



-- 2) ==== Check the new sizes
SELECT t.schemaname, pg_size_pretty(sum(pg_relation_size(quote_ident(t.schemaname) || '.' || quote_ident(t.tablename)))::bigint)
FROM pg_tables t
JOIN admin.tbl_kitchen k
    ON k.schema_name = t.schemaname
    AND k.active = 1
GROUP BY t.schemaname
ORDER BY sum(pg_relation_size(quote_ident(t.schemaname) || '.' || quote_ident(t.tablename)))::bigint;

/*
+------------+--------------+
|schemaname  |pg_size_pretty|
+------------+--------------+
|renato      |16 kB         |
|dev_data_out|1936 kB       |
|patrick     |3182 MB       |
|sergi       |3215 MB       |
|pavi        |3845 MB       |
|dev_crm     |15 GB         |
|dev_data_in |33 GB         |
|carlos      |74 GB         |
+------------+--------------+
*/


-- 3) ==== I don't know what this is.. didn't write it
/*
select table_schema,
       table_name,
       (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
from (
  select table_name, table_schema,
         query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
  from information_schema.tables
  where table_schema = 'dev_data_out' --<< change here for the schema you want
) t
*/