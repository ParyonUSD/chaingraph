/* Array columns are built after initial sync. The old global-lock maintenance
 * remains disabled even after backfill: only the reviewed incremental agent
 * path may maintain these columns. TRUNCATE guards remain enabled. */
DO $defer$
DECLARE
  legacy_trigger record;
  disabled_count integer := 0;
BEGIN
  FOR legacy_trigger IN
    SELECT c.relname AS table_name, t.tgname AS trigger_name
      FROM pg_trigger t
      JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND NOT t.tgisinternal
        AND t.tgname IN (
          'trigger_output_membership_lock',
          'trigger_zz_output_membership_insert',
          'trigger_zz_output_membership_delete',
          'trigger_zz_output_membership_update'
        )
  LOOP
    EXECUTE format('ALTER TABLE public.%I DISABLE TRIGGER %I',
                   legacy_trigger.table_name, legacy_trigger.trigger_name);
    disabled_count := disabled_count + 1;
  END LOOP;
  IF disabled_count <> 22 THEN
    RAISE EXCEPTION 'expected 22 legacy membership triggers, found %', disabled_count;
  END IF;
  UPDATE output_membership.state
    SET ready = false, updated_at = clock_timestamp()
    WHERE id;
END
$defer$;

/* Fail closed if an operator accidentally calls a legacy entry point. */
CREATE OR REPLACE FUNCTION output_membership.lock_writer() RETURNS trigger
  LANGUAGE plpgsql
AS $legacy$
BEGIN
  RAISE EXCEPTION 'legacy array maintenance is retired; use deferred build and incremental publication';
END
$legacy$;

CREATE OR REPLACE PROCEDURE output_membership.backfill(batch_heap_blocks bigint DEFAULT 20000)
  LANGUAGE plpgsql
AS $legacy$
BEGIN
  RAISE EXCEPTION 'legacy backfill is retired; use the fenced lock-free backfill runner';
END
$legacy$;
