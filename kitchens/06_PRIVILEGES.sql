/*============================================================================================*/
/*            !!! Connect to EMEA_CUST_360 DB as EMEA_SU in NEW DEV !!!                       */
/*============================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

-- ==== 1) Grant all privileges on all existing tables to role dev_schema_users

-- grant connect to database
GRANT CONNECT ON DATABASE emea_cust_360 TO dev_schema_users;

-- grant usage to schemas
GRANT USAGE ON SCHEMA alpaca TO dev_schema_users;
GRANT USAGE ON SCHEMA bi TO dev_schema_users;
GRANT USAGE ON SCHEMA data_in TO dev_schema_users;
GRANT USAGE ON SCHEMA crm TO dev_schema_users;
GRANT USAGE ON SCHEMA data_out TO dev_schema_users;
GRANT USAGE ON SCHEMA cmdb_catalog TO dev_schema_users;

GRANT USAGE ON SCHEMA apj_bi TO dev_schema_users;
GRANT USAGE ON SCHEMA apj_data_in TO dev_schema_users;
GRANT USAGE ON SCHEMA apj_crm TO dev_schema_users;
GRANT USAGE ON SCHEMA apj_data_out TO dev_schema_users;


-- grant SELECT only to replicated schemas
GRANT SELECT ON ALL TABLES IN SCHEMA alpaca TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA bi TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA data_in TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA data_out TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA cmdb_catalog TO dev_schema_users;

GRANT SELECT ON ALL TABLES IN SCHEMA apj_bi TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_data_in TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_crm TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_data_out TO dev_schema_users;

-- DEFAULT PRIVILEGES in replicated schemas
-- when EMEA_SU creates in replicated schemas (by DBA)
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

-- when ADMIN creates in replicated schemas (through DDL Sync)
SET ROLE emea_cust_360_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA cmdb_catalog GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_bi GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_in GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_crm GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;

ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT EXECUTE ON ROUTINES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA apj_data_out GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;


create or replace procedure admin.sp_migrate_privileges()
	language plpgsql
as $$
DECLARE
    rec_statement RECORD;
    rec_statement_2 RECORD;
    v_statement TEXT;

    err_context  text;
    v_err_context text;
BEGIN
    v_statement := 'SET ROLE emea_su;';
    EXECUTE(v_statement);

    RAISE NOTICE '[%] Granting required privileges on all developers schemas to dev_schema_users',"current_user"();

    -- privileges for EXISTING objects and NEW objects (created by emea_su, admin and dev_app_user)
    FOR rec_statement IN
        (
            SELECT
                schema_name
                -- grant usage to schemas
                , format('GRANT USAGE ON SCHEMA %s TO dev_schema_users;',schema_name) ddl_gnt_1
                -- grant ALL to ALL tables in "kitchen" schemas
                , format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %s TO dev_schema_users;',schema_name) ddl_gnt_2
                -- grant ALL to ALL sequences in "kitchen" schemas
                , format('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %s TO dev_schema_users;',schema_name) ddl_gnt_3
                -- grant ALL to ALL routines in "kitchen" schemas
                , format('GRANT ALL PRIVILEGES ON SCHEMA %s TO dev_schema_users;',schema_name) ddl_gnt_4

                -- DEFAULT privileges for all new TABLES (created by emea_su)
                , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;',schema_name) ddl_gnt_5
                -- DEFAULT privileges for all new SEQUENCES (created by emea_su)
                , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;',schema_name) ddl_gnt_6
                -- DEFAULT privileges for all new FUNCTIONS (created by emea_su)
                , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;',schema_name) ddl_gnt_7
                -- DEFAULT privileges for all new PROCEDURES (created by emea_su)
                , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON ROUTINES TO dev_schema_users;',schema_name) ddl_gnt_8
                -- DEFAULT privileges for all new TYPES (created by emea_su)
                , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;',schema_name) ddl_gnt_9
            FROM admin.tbl_kitchen
            WHERE
                active = 1
            ORDER BY migration_order
        )
    LOOP
        v_statement := 'SET ROLE emea_su;';
        EXECUTE(v_statement);
        RAISE NOTICE '[%] Granting required privileges on existing objects in schema % to dev_schema_users',"current_user"(),rec_statement.schema_name;
        v_statement := rec_statement.ddl_gnt_1;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_2;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_3;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_4;
        EXECUTE(v_statement);

        -- when EMEA_SU creates in dev_ schemas and kitchens
        RAISE NOTICE '[%] Granting default privileges for new objects in schema % to dev_schema_users',"current_user"(),rec_statement.schema_name;
        v_statement := rec_statement.ddl_gnt_5;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_6;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_7;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_8;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_9;
        EXECUTE(v_statement);

        -- when ADMIN creates in dev_ schemas and kitchens
        v_statement := 'SET ROLE emea_cust_360_admin;';
        EXECUTE(v_statement);
        RAISE NOTICE '[%] Granting default privileges for new objects in schema % to dev_schema_users',"current_user"(),rec_statement.schema_name;
        v_statement := rec_statement.ddl_gnt_5;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_6;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_7;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_8;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_9;
        EXECUTE(v_statement);

        -- when dev_app_user creates in dev_ schemas and kitchens (through scheduled python jobs)
        v_statement := 'SET ROLE dev_app_user;';
        EXECUTE(v_statement);
        RAISE NOTICE '[%] Granting default privileges for new objects in schema % to dev_schema_users',"current_user"(),rec_statement.schema_name;
        v_statement := rec_statement.ddl_gnt_5;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_6;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_7;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_8;
        EXECUTE(v_statement);
        v_statement := rec_statement.ddl_gnt_9;
        EXECUTE(v_statement);
    END LOOP;

    v_statement := 'SET ROLE emea_su;';
    EXECUTE(v_statement);

    -- DEFAULT privileges for new objects created in the future by other developers
    FOR rec_statement IN
        (
            SELECT
                schema_name
            FROM admin.tbl_kitchen
            WHERE
                active = 1
                AND is_developer = 1
            ORDER BY migration_order
        )
    LOOP
        -- loop through all developers
        v_statement := 'SET ROLE emea_su;';
        EXECUTE(v_statement);

        FOR rec_statement_2 IN
            (
                SELECT
                    schema_name
                    , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON TABLES TO dev_schema_users;',schema_name) ddl_gnt_1
                    , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON SEQUENCES TO dev_schema_users;',schema_name) ddl_gnt_2
                    , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO dev_schema_users;',schema_name) ddl_gnt_3
                    , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON ROUTINES TO dev_schema_users;',schema_name) ddl_gnt_4
                    , format('ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT ALL PRIVILEGES ON TYPES TO dev_schema_users;',schema_name) ddl_gnt_5
                FROM admin.tbl_kitchen
                WHERE
                    active = 1
                    AND
                        (
                            is_developer = 0
                            OR
                            schema_name = rec_statement.schema_name
                        )

                ORDER BY migration_order
            )
        LOOP
            -- and execute default privileges logged in as each developer
            v_statement := format('SET ROLE %s;',rec_statement.schema_name);
            EXECUTE(v_statement);

            RAISE NOTICE '[%] Granting default privileges for new objects in schema % to dev_schema_users',"current_user"(),rec_statement_2.schema_name;
            v_statement := rec_statement_2.ddl_gnt_1;
            EXECUTE(v_statement);
            v_statement := rec_statement_2.ddl_gnt_2;
            EXECUTE(v_statement);
            v_statement := rec_statement_2.ddl_gnt_3;
            EXECUTE(v_statement);
            v_statement := rec_statement_2.ddl_gnt_4;
            EXECUTE(v_statement);
            v_statement := rec_statement_2.ddl_gnt_5;
            EXECUTE(v_statement);
        END LOOP;
    END LOOP;

    v_statement := 'SET ROLE emea_su;';
    EXECUTE(v_statement);
    -- Finally we grant ROLE dev_schema_users to developers
    FOR rec_statement IN
        (
            SELECT
                schema_name
                , format('GRANT dev_schema_users TO %s;',schema_name) ddl_gnt_1
            FROM admin.tbl_kitchen
            WHERE
                active = 1
                AND is_developer = 1
            ORDER BY migration_order
        )
    LOOP
        RAISE NOTICE '[%] Granting role dev_schema_users to %',"current_user"(),rec_statement.schema_name;
        v_statement := rec_statement.ddl_gnt_1;
        EXECUTE(v_statement);
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS err_context = PG_EXCEPTION_CONTEXT;

        v_err_context := FORMAT('Error Context:%s | Error Message:%s | Error State:%s',err_context, sqlerrm, sqlstate);
        RAISE EXCEPTION '%###%',v_err_context,v_statement;
END
$$;

call admin.sp_migrate_privileges();


SET ROLE emea_su;
GRANT dev_schema_users TO dev_app_user;
GRANT dev_schema_users TO emea_cust_360_fgn;


