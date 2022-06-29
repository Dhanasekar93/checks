/*=========================================================================================================*/
/*                    !!! Connect to UAT_CMDB database as EMEA_SU on new DEV !!!                           */
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

CREATE SCHEMA alpaca;
CREATE SCHEMA bi;
CREATE SCHEMA data_in;
CREATE SCHEMA crm;
CREATE SCHEMA data_out;

ALTER SCHEMA alpaca owner to uat_app_user;
ALTER SCHEMA bi owner to uat_app_user;
ALTER SCHEMA data_in owner to uat_app_user;
ALTER SCHEMA crm owner to uat_app_user;
ALTER SCHEMA data_out owner to uat_app_user;
ALTER ROLE uat_app_user SET search_path TO crm;

create table crm._job_history
(
	insert_ts timestamp not null,
	job_id bigint not null,
	details jsonb not null,
	id serial not null
		constraint _job_history_pkey
			primary key
);

create table crm._job_schedules
(
	job_id smallserial not null
		constraint _job_schedules_pkey
			primary key,
	is_active boolean not null,
	run_query text not null,
	run_days jsonb not null,
	run_time time not null,
	comments varchar(500),
	notification_recipients varchar(400),
	status varchar(50),
	chaining json,
	last_run timestamp,
	run_week jsonb
);

alter table crm._job_schedules owner to uat_app_user;
alter table crm._job_history owner to uat_app_user;

create function crm.get_jobs() returns jsonb
	language plpgsql
as $$
begin
		Return(
			SELECT array_to_json(array_agg(t)) FROM (
			select * from crm."_job_schedules"
			where is_active = true
			and run_days?to_char(NOW(), 'Dy')
			and date_part('hour',now()::time-run_time)::int=0
			and date_part('minute',now()::time-run_time)::int = 0
			and run_time<=now()::time
			AND (CASE
	            WHEN run_week IS NULL THEN TRUE
	            ELSE run_week @> (date_part('week', now())::int)::text::jsonb END)
			order by job_id asc
			)t
		);-- return QUERY
    END;
$$;

alter function crm.get_jobs() owner to uat_app_user;

create table data_in._etl_job_history
(
	insert_ts timestamp not null,
	job_id bigint not null,
	details jsonb not null
);

alter table data_in._etl_job_history owner to uat_app_user;

create table data_in._etl_job_schedules
(
	job_id smallserial not null
		constraint _etl_job_schedules_pkey
			primary key,
	is_active boolean not null,
	pull_from varchar(50) not null,
	pull_type varchar(10) not null,
	pull_query text not null,
	push_to varchar(50),
	push_type varchar(10) not null,
	push_to_table varchar(100) not null,
	do_append boolean not null,
	run_days jsonb not null,
	run_time time not null,
	comments varchar(500),
	notification_recipients varchar(400),
	data_protection jsonb,
	chaining json,
	status text,
	last_run timestamp,
	run_week jsonb
);

comment on column data_in._etl_job_schedules.do_append is 'false: Table is dropped and recreated from scratch at every run of the job
true: Table is created first time if does not exist and data is then appended at every job run';

alter table data_in._etl_job_schedules owner to uat_app_user;

create function data_in.get_jobs() returns jsonb
	language plpgsql
as $$
BEGIN
    RETURN (
        SELECT array_to_json(array_agg(t))
        FROM (
                 SELECT *
                 FROM data_in."_etl_job_schedules"
                 WHERE is_active = TRUE
                   AND run_days ? to_char(NOW(), 'Dy')
                   AND date_part('hour', now()::time - run_time)::int = 0
                   AND date_part('minute', now()::time - run_time)::int = 0
                   AND run_time <= now()::time
                   AND (CASE
                            WHEN run_week IS NULL THEN TRUE
                            ELSE run_week @> (date_part('week', now())::int)::text::jsonb END)
                 ORDER BY job_id ASC
             ) t
    );-- return QUERY
END;
$$;

alter function data_in.get_jobs() owner to uat_app_user;


create table data_out._etl_job_history
(
	insert_ts timestamp not null,
	job_id bigint not null,
	details jsonb not null,
	id serial not null
		constraint _etl_job_history_pkey
			primary key
);

alter table data_out._etl_job_history owner to uat_app_user;

create table data_out._etl_job_schedules
(
	job_id smallserial not null
		constraint _etl_job_schedules_pkey
			primary key,
	is_active boolean not null,
	pull_from varchar(50) default 'c360_r'::character varying not null,
	pull_type varchar(10) not null,
	pull_query text not null,
	push_to varchar(50),
	push_type varchar(10) not null,
	push_to_table text not null,
	do_append boolean not null,
	run_days jsonb not null,
	run_time time not null,
	comments varchar(500),
	notification_recipients varchar(400),
	data_protection jsonb,
	status text,
	last_run timestamp,
	run_week jsonb
);

alter table data_out._etl_job_schedules owner to uat_app_user;
create function data_out.get_jobs() returns jsonb
	language plpgsql
as $$
BEGIN
    RETURN (
        SELECT array_to_json(array_agg(t))
        FROM (
                 SELECT *
                 FROM data_out."_etl_job_schedules"
                 WHERE is_active = TRUE
                   AND run_days ? to_char(NOW(), 'Dy')
                   AND date_part('hour', now()::time - run_time)::int = 0
                   AND date_part('minute', now()::time - run_time)::int = 0
                   AND run_time <= now()::time
                   AND (CASE
                            WHEN run_week IS NULL THEN TRUE
                            ELSE run_week @> (date_part('week', now())::int)::text::jsonb END)
                 ORDER BY job_id ASC
             ) t
    );-- return QUERY
END;
$$;

alter function data_out.get_jobs() owner to uat_app_user;

/* ============================================================= */
-- ==== STEP 2) grant privileges
/* ============================================================= */

-- ==== FOR ROLE dev_schema_users which is used by developers

-- grant connect to database
GRANT CONNECT ON DATABASE uat_cmdb TO dev_schema_users;
GRANT USAGE ON SCHEMA alpaca TO dev_schema_users;
GRANT USAGE ON SCHEMA bi TO dev_schema_users;
GRANT USAGE ON SCHEMA data_in TO dev_schema_users;
GRANT USAGE ON SCHEMA crm TO dev_schema_users;
GRANT USAGE ON SCHEMA data_out TO dev_schema_users;

-- grant SELECT to ALL tables (developers can SELECT from all tables)
GRANT SELECT ON ALL TABLES IN SCHEMA alpaca TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA bi TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA data_in TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA crm TO dev_schema_users;
GRANT SELECT ON ALL TABLES IN SCHEMA data_out TO dev_schema_users;

-- grant ALL to job scheduler tables only (developers can SEL, INS, UPD, DEL from this table only)
-- tables
GRANT ALL PRIVILEGES ON data_in._etl_job_schedules TO dev_schema_users;
GRANT ALL PRIVILEGES ON crm._job_schedules TO dev_schema_users;
GRANT ALL PRIVILEGES ON data_out._etl_job_schedules TO dev_schema_users;
-- sequnces
GRANT ALL PRIVILEGES ON data_in._etl_job_schedules_job_id_seq TO dev_schema_users;
GRANT ALL PRIVILEGES ON crm._job_schedules_job_id_seq TO dev_schema_users;
GRANT ALL PRIVILEGES ON data_out._etl_job_schedules_job_id_seq TO dev_schema_users;

-- grant ALL to ALL routines
GRANT ALL PRIVILEGES ON SCHEMA data_in TO dev_schema_users;
GRANT ALL PRIVILEGES ON SCHEMA crm TO dev_schema_users;
GRANT ALL PRIVILEGES ON SCHEMA data_out TO dev_schema_users;


-- ==== FOR ROLE uat_app_user which is used by python - give all privs on all

-- grant connect to database
GRANT CONNECT ON DATABASE uat_cmdb TO uat_app_user;
GRANT USAGE ON SCHEMA alpaca TO uat_app_user;
GRANT USAGE ON SCHEMA bi TO uat_app_user;
GRANT USAGE ON SCHEMA data_in TO uat_app_user;
GRANT USAGE ON SCHEMA crm TO uat_app_user;
GRANT USAGE ON SCHEMA data_out TO uat_app_user;

-- grant ALL to ALL
-- tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA alpaca TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bi TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_in TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crm TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA data_out TO uat_app_user;
-- sequences
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA alpaca TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bi TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_in TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA crm TO uat_app_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA data_out TO uat_app_user;
-- routines
GRANT ALL PRIVILEGES ON SCHEMA alpaca TO uat_app_user;
GRANT ALL PRIVILEGES ON SCHEMA bi TO uat_app_user;
GRANT ALL PRIVILEGES ON SCHEMA data_in TO uat_app_user;
GRANT ALL PRIVILEGES ON SCHEMA crm TO uat_app_user;
GRANT ALL PRIVILEGES ON SCHEMA data_out TO uat_app_user;



/* ============================================================= */
-- ==== STEP 3) default privileges for future objects
/* ============================================================= */
-- NOTE : default privileges must be executed logged in as each user, this means we are
-- giving privileges to role dev_schema_users for all objects created in the future by logged in user.
-- Doing it logged in as dev_schema_users (for all users at once) DOES NOT WORK

-- And it has to be done for each schema where new objects will be created


SET ROLE emea_su; -- (when DBAs are doing something)
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

SET ROLE emea_cust_360_admin; -- (when ADMIN is doing something)
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TABLES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON SEQUENCES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON FUNCTIONS TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT EXECUTE ON ROUTINES TO uat_app_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT ALL PRIVILEGES ON TYPES TO uat_app_user;

SET ROLE uat_app_user; -- if uat_app_user (through scheduled python jobs) creates any new objects in any schema
ALTER DEFAULT PRIVILEGES IN SCHEMA alpaca GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA bi GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_in GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA crm GRANT SELECT ON TABLES TO dev_schema_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA data_out GRANT SELECT ON TABLES TO dev_schema_users;


/* ============================================================= */
-- ==== LAST STEP manually copy data
/* ============================================================= */
-- from odl DEV do a manual export of table data (use "as SQL Inserts" option) for tables :
    -- uat_cmdb.crm._job_schedules
    -- uat_cmdb.data_in._etl_job_schedules
    -- uat_cmdb.data_out._etl_job_schedules
-- end execute INSERTS into same tables in new DEV

