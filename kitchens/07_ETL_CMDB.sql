/*=========================================================================================================*/
/*                    !!! Connect to ETL_CMDB database as EMEA_SU in new DEV !!!                           */
/*=========================================================================================================*/
SELECT
    current_database()::text
    , "current_schema"()::text
    , "current_user"()::text
    , current_timestamp
    , date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;


/* ============================================================= */
-- ==== STEP 1) create objects
/* ============================================================= */
CREATE SCHEMA admin;
CREATE SCHEMA data_in;
CREATE SCHEMA crm;
CREATE SCHEMA data_out;
CREATE SCHEMA apj_data_in;
CREATE SCHEMA apj_crm;
CREATE SCHEMA apj_data_out;

ALTER SCHEMA admin owner to emea_cust_360_admin;
ALTER SCHEMA data_in owner to etl_app_user;
ALTER SCHEMA crm owner to etl_app_user;
ALTER SCHEMA data_out owner to etl_app_user;
ALTER SCHEMA apj_data_in owner to etl_app_user;
ALTER SCHEMA apj_crm owner to etl_app_user;
ALTER SCHEMA apj_data_out owner to etl_app_user;

ALTER ROLE etl_app_user SET search_path TO crm;

create function admin.get_ddl_oid(_sn text DEFAULT 'public'::text, _tn text DEFAULT ''::text, _opt json DEFAULT '{}'::json) returns text
	language plpgsql
as $$
declare
 _sntn TEXT;
 _oid text;
begin
  --********* QUERY **********
/*
  SELECT c.oid INTO _oid
  FROM pg_catalog.pg_class c
       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relname = _tn
    AND n.nspname = _sn
    AND pg_catalog.pg_table_is_visible(c.oid)
  ;
*/
  _sntn := FORMAT('"%s"."%s"',_sn,_tn);
  SELECT _sntn::regclass::oid INTO _oid;

  return _oid;
end;
$$;

alter function admin.get_ddl_oid(text, text, json) owner to emea_cust_360_admin;

create function admin.get_ddl_idx_tbl(_sn text DEFAULT 'public'::text, _tn text DEFAULT ''::text, _opt json DEFAULT '{}'::json) returns text
	language plpgsql
as $$
declare
    _oid bigint;
    _rtn text;
    _seq text;
    _t   text;
    _r   record;
begin
    select admin.get_ddl_oid(_sn, _tn, _opt)::bigint into _oid;
    for _r in (
        --********* QUERY **********
        SELECT c2.relname,
               i.indisprimary,
               i.indisunique,
               i.indisclustered,
               i.indisvalid,
               pg_catalog.pg_get_indexdef(i.indexrelid, 0, true),
               pg_catalog.pg_get_constraintdef(con.oid, true),
               contype,
               condeferrable,
               condeferred,
               c2.reltablespace
                ,
               conname
        FROM pg_catalog.pg_class c,
             pg_catalog.pg_class c2,
             pg_catalog.pg_index i
                 LEFT JOIN pg_catalog.pg_constraint con
                           ON (conrelid = i.indrelid AND conindid = i.indexrelid AND contype IN ('p', 'u', 'x'))
        WHERE c.oid = _oid
          AND c.oid = i.indrelid
          AND i.indexrelid = c2.oid
          --AND contype is null
        ORDER BY i.indisprimary DESC, i.indisunique DESC, c2.relname
    )
        loop
            if _r.contype is null then
                _rtn := concat(_rtn, _r.pg_get_indexdef, ';', chr(10));
            else
                _rtn := concat(_rtn, format('ALTER TABLE ONLY %I.%I ADD CONSTRAINT %I ', _sn, _tn, _r.conname),
                               _r.pg_get_constraintdef, ';', chr(10));
            end if;
        end loop;

    return _rtn;
end;
$$;

alter function admin.get_ddl_idx_tbl(text, text, json) owner to emea_cust_360_admin;

create function admin.get_ddl_seq_tbl(_sn text DEFAULT 'public'::text, _tn text DEFAULT ''::text, _opt json DEFAULT '{}'::json) returns text
	language plpgsql
as $$
declare
 _rtn text;
 _t text;
 _r record;
 _oid text;
begin
  select admin.get_ddl_oid(_sn,_tn,_opt) into _oid;
  /*
  https://www.postgresql.org/docs/current/static/functions-info.html
  pg_get_serial_sequence(table_name, column_name) is the only pg_get_ f() using name instead of oid
  need additional format for "weird.relation.names"
  */
  for _r in (
/*
--    SELECT pg_get_serial_sequence(format('%I',_tn),a.attname) _seq
    SELECT pg_get_serial_sequence(format('"%s"."%s"',_sn,_tn),a.attname) _seq
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid::text = _oid AND a.attnum > 0 AND NOT a.attisdropped
    and length(pg_get_serial_sequence(format('"%s"."%s"',_sn,_tn),a.attname)) > 1
*/

    select pg_get_serial_sequence(format('"%s"."%s"',_sn,_tn),column_name) _seq
    from information_schema.columns
    where table_name = format('"%s"',_tn)
    and table_schema = format('"%s"',_sn)
    and length(pg_get_serial_sequence(format('"%s"."%s"',_sn,_tn),column_name)) > 1

  ) loop
    begin
      EXECUTE FORMAT($f$
        SELECT concat(
          'CREATE SEQUENCE %s'
          , chr(10),chr(9), 'START WITH ', start_value
          , chr(10),chr(9), 'INCREMENT BY ', increment_by
          , chr(10),chr(9), 'MINVALUE ', min_value
          , chr(10),chr(9), 'MAXVALUE ', max_value
          , chr(10),chr(9), 'CACHE ', cache_value
          , chr(10),');',chr(10)
          )
        FROM %s$f$,_r._seq,_r._seq)
      into _t;
    exception
      when others
      then
        if _opt->>'handle exceptions' then
          --raise info '%',SQLERRM;
          _rtn := concat(chr(10),_rtn,'--',SQLSTATE,': ',SQLERRM);
        else
          raise exception '%',concat(SQLSTATE,': ',SQLERRM);
        end if;
    end;
    _rtn := concat(chr(10),_rtn,_t);
  end loop;
  return _rtn;
end;
$$;

alter function admin.get_ddl_seq_tbl(text, text, json) owner to emea_cust_360_admin;

create function admin.get_ddl_t(_sn text DEFAULT 'public'::text, _tn text DEFAULT ''::text, _opt json DEFAULT '{}'::json) returns text
	language plpgsql
as $$
declare
 _c bigint;
 _n int := 0;
 _columns text;
 _comments text;
 _table_comments text;
 _indices_ddl text;
 _rtn text := '';
 _oid text;
 _seq text;
begin
  select admin.get_ddl_oid(_sn,_tn,_opt) into _oid;

--  select get_ddl_seq_tbl(_sn,_tn,_opt) into _seq;

  select pg_catalog.obj_description(_oid::bigint) into _table_comments;
  select admin.get_ddl_idx_tbl(_sn,_tn,_opt) into _indices_ddl;
  -- 1. Get list of columns
  SELECT concat(
      chr(10)
    , string_agg(
      concat(
        chr(9)
        , format('%I',a.attname)
        , ' '
        , pg_catalog.format_type(a.atttypid, a.atttypmod)
        , ' '
        , (
          SELECT concat('DEFAULT ',substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128))
          FROM pg_catalog.pg_attrdef d
          WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef
        )

        , case when attnotnull then ' NOT NULL' end
      )
      , concat(',',chr(10))
      ) over (order by attnum)
    )
    , string_agg('COMMENT ON COLUMN '||_sn||'.'||_tn||'.'||a.attname||$c$ IS '$c$||col_description(a.attrelid, a.attnum)||$c$';$c$,chr(10)) over (order by attnum)
  into _columns,_comments
  FROM pg_catalog.pg_attribute a
  join pg_class c on c.oid = a.attrelid
  join pg_namespace s on s.oid = c.relnamespace
  WHERE a.attnum > 0 AND NOT a.attisdropped
    AND nspname = _sn
    and relname = _tn
    order by 1 desc limit 1;

--  _rtn := '--Sequences DDL:'||_seq; --we want to skip this line when no comments => no concat
  _rtn := concat(_rtn,chr(10),chr(10),'--Table DDL:',chr(10),format('DROP TABLE IF EXISTS %I.%I; CREATE TABLE %I.%I (',_sn,_tn,_sn,_tn),_columns,chr(10), ');');
  _rtn := concat(_rtn,(chr(10)||chr(10)||'--Columns Comments:'||chr(10)||_comments));
  _rtn := concat(_rtn,(chr(10)||chr(10)||'--Table Comments:'||chr(10)||case when _table_comments is not null then format($f$COMMENT ON TABLE %s.%I is '%s';$f$,_sn,_tn,_table_comments) end));
  _rtn := concat(_rtn,(chr(10)||chr(10)||'--Indexes DDL:'||chr(10)||_indices_ddl));

  return _rtn;
end;
$$;

alter function admin.get_ddl_t(text, text, json) owner to emea_cust_360_admin;




/* ============================================================= */
-- ==== STEP 2) grant privileges
/* ============================================================= */

-- ==== FOR ROLE dev_schema_users which is used by developers

-- grant connect to database
GRANT CONNECT ON DATABASE etl_cmdb TO emea_cust_360_admin;
GRANT CONNECT ON DATABASE etl_cmdb TO dev_schema_users;
GRANT USAGE ON SCHEMA data_in TO dev_schema_users;
GRANT USAGE ON SCHEMA crm TO dev_schema_users;
GRANT USAGE ON SCHEMA data_out TO dev_schema_users;
GRANT USAGE ON SCHEMA apj_data_in TO dev_schema_users;
GRANT USAGE ON SCHEMA apj_crm TO dev_schema_users;
GRANT USAGE ON SCHEMA apj_data_out TO dev_schema_users;



-- ==== FOR ROLE etl_app_user which is used by python - give all privs on all

-- grant connect to database
GRANT CONNECT ON DATABASE etl_cmdb TO etl_app_user;
GRANT USAGE ON SCHEMA data_in TO etl_app_user;
GRANT USAGE ON SCHEMA crm TO etl_app_user;
GRANT USAGE ON SCHEMA data_out TO etl_app_user;
GRANT USAGE ON SCHEMA apj_data_in TO etl_app_user;
GRANT USAGE ON SCHEMA apj_crm TO etl_app_user;
GRANT USAGE ON SCHEMA apj_data_out TO etl_app_user;