/*============================================================================================*/
/*            !!! Connect to dummy DB as dhanasekarravindran in NEW DEV !!!                   */
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

select pg_sleep(2);
-- 1.1) ==== create the foreign data wrapper extension
-- see the list of extensions
select * from pg_extension;

