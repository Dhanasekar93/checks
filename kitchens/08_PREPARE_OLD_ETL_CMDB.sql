/*=========================================================================================================*/
/*                          !!! Connect to ETL_CMDB as ADMIN in OLD DEV !!!                                */
/*=========================================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;

SELECT *
FROM pg_stat_activity
WHERE state !='idle'
ORDER BY usename, application_name;

/* SEQUENCE DEFINITION */
DROP TABLE IF EXISTS admin.tbl_kitchen_sequence;
DROP FOREIGN TABLE IF EXISTS admin.tbl_kitchen_sequence;
CREATE TABLE admin.tbl_kitchen_sequence
    (id SERIAL PRIMARY KEY, schema_name text, sequence_name text, sequence_owner text, sequence_ddl text);
INSERT INTO admin.tbl_kitchen_sequence
(schema_name, sequence_name, sequence_owner, sequence_ddl)
SELECT
    c.relnamespace::regnamespace::text schema_name
    , c.relname::text sequence_name
    , pg_get_userbyid(c.relowner) sequence_owner
    , 'CREATE SEQUENCE '
    || FORMAT('"%s"."%s"',c.relnamespace::regnamespace::text, c.relname::text)
    || ' START '
    ||
        (
            SELECT
                SETVAL
                    (
                        FORMAT('"%s"."%s"',c.relnamespace::regnamespace::text,c.relname::text)
                        , NEXTVAL
                            (
                                FORMAT('"%s"."%s"',c.relnamespace::regnamespace::text,c.relname::text)
                            ) --- s.seqincrement
                    )
        ) AS sequence_ddl
FROM pg_class c
JOIN pg_sequence s
    ON s.seqrelid = c.oid
WHERE
    c.relkind = 'S'
    AND c.relnamespace::regnamespace::text IN ('crm','data_out','data_in','apj_crm','apj_data_out','apj_data_in')
ORDER BY
    c.relnamespace::regnamespace::text
    , c.relname::text;

/* TABLES AND CONSTRAINTS DEFINITION */
DROP TABLE IF EXISTS admin.tbl_kitchen_table;
DROP FOREIGN TABLE IF EXISTS admin.tbl_kitchen_table;
CREATE TABLE admin.tbl_kitchen_table
    (id SERIAL PRIMARY KEY, schema_name text, table_name text, table_owner text, estimate_row_count bigint, table_ddl text);
INSERT INTO admin.tbl_kitchen_table
(schema_name, table_name, table_owner, estimate_row_count, table_ddl)
SELECT
    t.table_schema schema_name
    , t.table_name
    , pgt.tableowner table_owner
    ,
        (
            CASE
                WHEN c.reltuples < 0 THEN NULL       -- never vacuumed
                WHEN c.relpages = 0 THEN float8 '0'  -- empty table
                ELSE c.reltuples / c.relpages
            END
            * (pg_relation_size(c.oid) / pg_catalog.current_setting('block_size')::int)
        )::bigint estimate_row_count
    , admin.get_ddl_t(t.table_schema,t.table_name) table_ddl
FROM information_schema.tables t
JOIN pg_tables pgt
    ON pgt.schemaname = t.table_schema
    AND pgt.tablename = t.table_name
LEFT JOIN pg_class c
    ON c.oid = FORMAT('"%s"."%s"',t.table_schema,t.table_name)::regclass
WHERE
    t.table_schema IN ('crm','data_out','data_in','apj_crm','apj_data_out','apj_data_in')
    AND t.table_type = 'BASE TABLE'
ORDER BY
    t.table_schema
    , t.table_name;

/* ROUTINES DEFINITION */
DROP TABLE IF EXISTS admin.tbl_kitchen_routine;
DROP FOREIGN TABLE IF EXISTS admin.tbl_kitchen_routine;
CREATE TABLE admin.tbl_kitchen_routine
    (id SERIAL PRIMARY KEY, schema_name text, routine_name text, routine_owner text, routine_ddl text, routine_arguments text);
INSERT INTO admin.tbl_kitchen_routine
(schema_name, routine_name, routine_owner, routine_ddl, routine_arguments)
SELECT
    n.nspname schema_name
    , p.proname routine_name
    , p.proowner::regrole routine_owner
    ,
        CASE
            WHEN l.lanname = 'internal' THEN p.prosrc
            ELSE pg_get_functiondef(p.oid)
        END routine_ddl
    , pg_get_function_identity_arguments(p.oid) routine_arguments
FROM pg_proc p
LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
LEFT JOIN pg_language l ON p.prolang = l.oid
LEFT JOIN pg_type t ON t.oid = p.prorettype
WHERE
    n.nspname IN ('crm','data_out','data_in','apj_crm','apj_data_out','apj_data_in')
ORDER BY
    n.nspname
    , p.proname;

/* VIEWS DEFINITION */
-- 1st level of view dependency
CREATE OR REPLACE VIEW admin.v_view_dep_1 as
SELECT DISTINCT
    nm.nspname dep_schema_name
    , v.relname dep_view_name
    , refobjnm.nspname ref_schema_name
    , refobj.relname ref_object_name
FROM pg_depend AS d      -- objects that depend on the table
JOIN pg_rewrite AS r  -- rules depending on the table
    ON r.oid = d.objid
JOIN pg_class AS v    -- views for the rules
    ON v.oid = r.ev_class
JOIN pg_namespace AS nm    -- namespace of the views
    ON nm.oid = v.relnamespace
JOIN pg_class refobj
    ON refobj.oid = d.refobjid
JOIN pg_namespace refobjnm
    ON refobjnm.oid = refobj.relnamespace
JOIN
    (
        SELECT
            v.table_schema schema_name
            , v.table_name view_name
        FROM information_schema.views v
        JOIN pg_views pgv
            ON pgv.schemaname = v.table_schema
            AND pgv.viewname = v.table_name
        WHERE v.table_schema IN ('crm','data_out','data_in','apj_crm','apj_data_out','apj_data_in')
        ) kv
    ON (kv.schema_name||'.'||kv.view_name)::regclass = d.refobjid
WHERE v.relkind = 'v'    -- only interested in views
  -- dependency must be a rule depending on a relation
  AND d.classid = 'pg_rewrite'::regclass
  AND d.refclassid = 'pg_class'::regclass
  AND d.deptype = 'n'    -- normal dependency
  AND v.oid != d.refobjid;

-- 2nd level of view dependency
CREATE OR REPLACE VIEW admin.v_view_dep_2 as
SELECT DISTINCT
    nm.nspname dep_schema_name
    , v.relname dep_view_name
    , refobjnm.nspname ref_schema_name
    , refobj.relname ref_object_name
FROM pg_depend AS d      -- objects that depend on the table
JOIN pg_rewrite AS r  -- rules depending on the table
    ON r.oid = d.objid
JOIN pg_class AS v    -- views for the rules
    ON v.oid = r.ev_class
JOIN pg_namespace AS nm    -- namespace of the views
    ON nm.oid = v.relnamespace
JOIN pg_class refobj
    ON refobj.oid = d.refobjid
JOIN pg_namespace refobjnm
    ON refobjnm.oid = refobj.relnamespace
JOIN
    (
        SELECT DISTINCT dep_schema_name, dep_view_name
        FROM admin.v_view_dep_1
    ) vd1
    ON (vd1.dep_schema_name||'.'||vd1.dep_view_name)::regclass = d.refobjid
WHERE v.relkind = 'v'    -- only interested in views
  -- dependency must be a rule depending on a relation
  AND d.classid = 'pg_rewrite'::regclass
  AND d.refclassid = 'pg_class'::regclass
  AND d.deptype = 'n'    -- normal dependency
  AND v.oid != d.refobjid;

-- 3rd level of view dependency
CREATE OR REPLACE VIEW admin.v_view_dep_3 as
SELECT DISTINCT
    nm.nspname dep_schema_name
    , v.relname dep_view_name
    , refobjnm.nspname ref_schema_name
    , refobj.relname ref_object_name
FROM pg_depend AS d      -- objects that depend on the table
JOIN pg_rewrite AS r  -- rules depending on the table
    ON r.oid = d.objid
JOIN pg_class AS v    -- views for the rules
    ON v.oid = r.ev_class
JOIN pg_namespace AS nm    -- namespace of the views
    ON nm.oid = v.relnamespace
JOIN pg_class refobj
    ON refobj.oid = d.refobjid
JOIN pg_namespace refobjnm
    ON refobjnm.oid = refobj.relnamespace
JOIN
    (
        SELECT DISTINCT dep_schema_name, dep_view_name
        FROM admin.v_view_dep_2
    ) vd2
    ON (vd2.dep_schema_name||'.'||vd2.dep_view_name)::regclass = d.refobjid
WHERE v.relkind = 'v'    -- only interested in views
  -- dependency must be a rule depending on a relation
  AND d.classid = 'pg_rewrite'::regclass
  AND d.refclassid = 'pg_class'::regclass
  AND d.deptype = 'n'    -- normal dependency
  AND v.oid != d.refobjid;

DROP TABLE IF EXISTS admin.tbl_kitchen_view;
DROP FOREIGN TABLE IF EXISTS admin.tbl_kitchen_view;
CREATE TABLE admin.tbl_kitchen_view
    (id SERIAL PRIMARY KEY, schema_name text, view_name text, view_owner text, view_ddl text);
INSERT INTO admin.tbl_kitchen_view
(schema_name, view_name, view_owner, view_ddl)
SELECT v.*
FROM
    (
        SELECT
            v.table_schema schema_name
            , v.table_name view_name
            , pgv.viewowner view_owner
            , FORMAT
                (
                    'CREATE VIEW "%s"."%s" AS %s'
                    , v.table_schema
                    , v.table_name
                    , pg_get_viewdef(FORMAT('"%s"."%s"',v.table_schema,v.table_name), true)
                ) view_ddl
        FROM information_schema.views v
        JOIN pg_views pgv
            ON pgv.schemaname = v.table_schema
            AND pgv.viewname = v.table_name
        WHERE v.table_schema IN ('crm','data_out','data_in','apj_crm','apj_data_out','apj_data_in')
    ) v
LEFT JOIN admin.v_view_dep_1 vd1
    ON vd1.dep_schema_name = v.schema_name
    AND vd1.dep_view_name = v.view_name
LEFT JOIN admin.v_view_dep_2 vd2
    ON vd2.dep_schema_name = v.schema_name
    AND vd2.dep_view_name = v.view_name
LEFT JOIN admin.v_view_dep_3 vd3
    ON vd3.dep_schema_name = v.schema_name
    AND vd3.dep_view_name = v.view_name
ORDER BY
    vd3.ref_schema_name NULLS FIRST, vd3.ref_object_name NULLS FIRST
    , vd2.ref_schema_name NULLS FIRST, vd2.ref_object_name NULLS FIRST
    , vd1.ref_schema_name NULLS FIRST, vd1.ref_object_name NULLS FIRST;



/* MATERIALIZED VIEWS DEFINITION */
DROP TABLE IF EXISTS admin.tbl_kitchen_mat_view;
DROP FOREIGN TABLE IF EXISTS admin.tbl_kitchen_mat_view;
CREATE TABLE admin.tbl_kitchen_mat_view
    (id SERIAL PRIMARY KEY, schema_name text, view_name text, view_owner text, view_ddl text);
INSERT INTO admin.tbl_kitchen_mat_view
(schema_name, view_name, view_owner, view_ddl)
SELECT
    schemaname schema_name
    , matviewname view_name
    , matviewowner view_owner
    , FORMAT
        (
            'CREATE MATERIALIZED VIEW "%s"."%s" AS %s'
            , schemaname
            , matviewname
            , pg_get_viewdef(FORMAT('"%s"."%s"',schemaname,matviewname), true)
        ) view_ddl
FROM pg_matviews
WHERE schemaname IN ('crm','data_out','data_in','apj_crm','apj_data_out','apj_data_in')
ORDER BY
    schemaname
    , matviewname;


-- now grant access to all this to fgn user, so it can be accessed from NEW DEV
GRANT USAGE ON SCHEMA admin TO emea_cust_360_fgn;
GRANT SELECT ON ALL TABLES IN SCHEMA admin TO emea_cust_360_fgn;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA admin TO emea_cust_360_fgn;
GRANT USAGE ON SCHEMA data_in TO emea_cust_360_fgn;
GRANT SELECT ON ALL TABLES IN SCHEMA data_in TO emea_cust_360_fgn;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA data_in TO emea_cust_360_fgn;
GRANT USAGE ON SCHEMA data_out TO emea_cust_360_fgn;
GRANT SELECT ON ALL TABLES IN SCHEMA data_out TO emea_cust_360_fgn;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA data_out TO emea_cust_360_fgn;
GRANT USAGE ON SCHEMA crm TO emea_cust_360_fgn;
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO emea_cust_360_fgn;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA crm TO emea_cust_360_fgn;
GRANT USAGE ON SCHEMA apj_data_in TO emea_cust_360_fgn;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_data_in TO emea_cust_360_fgn;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA apj_data_in TO emea_cust_360_fgn;
GRANT USAGE ON SCHEMA apj_data_out TO emea_cust_360_fgn;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_data_out TO emea_cust_360_fgn;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA apj_data_out TO emea_cust_360_fgn;
GRANT USAGE ON SCHEMA apj_crm TO emea_cust_360_fgn;
GRANT SELECT ON ALL TABLES IN SCHEMA apj_crm TO emea_cust_360_fgn;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA apj_crm TO emea_cust_360_fgn;




-- get the schema sizes we are about to move
SELECT schemaname, pg_size_pretty(sum(pg_relation_size(quote_ident(schemaname) || '.' || quote_ident(tablename)))::bigint)
FROM pg_tables
WHERE schemaname IN ('crm','data_out','data_in','apj_crm','apj_data_out','apj_data_in')
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
