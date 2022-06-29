/*============================================================================================*/
/*            !!! Connect to dummy DB as dhanasekarravindran in MASTER !!!                   */
/*============================================================================================*/

CREATE OR REPLACE PROCEDURE raise_warning() AS $$
DECLARE
warn INT := 1;
BEGIN
RAISE NOTICE 'value of warn : % at %: ', warn, now();
RAISE NOTICE 'Sleeping for: % at %: ', warn, now();
PERFORM pg_sleep(warn);
warn := warn + 2;
RAISE WARNING 'value of warn : % at %: ', warn, now();
RAISE NOTICE 'Sleeping for: % at %: ', warn, now();
PERFORM pg_sleep(warn);
warn := warn + 3;
RAISE INFO 'value of warn : % at %: ', warn, now();
RAISE NOTICE 'Sleeping for: % at %: ', warn, now();
PERFORM pg_sleep(warn);
END;
$$
LANGUAGE plpgsql;

call raise_warning();

