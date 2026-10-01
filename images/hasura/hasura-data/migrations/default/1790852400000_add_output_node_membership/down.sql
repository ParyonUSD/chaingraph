DROP FUNCTION IF EXISTS public.unspent_output(text);
DROP FUNCTION IF EXISTS public.accepted_output(text);

DO $triggers$
DECLARE
  source_table text;
BEGIN
  FOREACH source_table IN ARRAY ARRAY[
    'transaction',
    'block',
    'output',
    'input',
    'block_transaction',
    'node_block',
    'node_transaction'
  ] LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trigger_output_membership_lock ON public.%I',
      source_table
    );
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trigger_output_membership_reject_truncate ON public.%I',
      source_table
    );
  END LOOP;

  FOREACH source_table IN ARRAY ARRAY[
    'output',
    'input',
    'block_transaction',
    'node_block',
    'node_transaction'
  ] LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trigger_zz_output_membership_insert ON public.%I',
      source_table
    );
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trigger_zz_output_membership_delete ON public.%I',
      source_table
    );
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trigger_zz_output_membership_update ON public.%I',
      source_table
    );
  END LOOP;
END
$triggers$;

DROP INDEX IF EXISTS public.output_accepted_node_ids_gin;
DROP INDEX IF EXISTS public.output_unspent_node_ids_gin;
DROP INDEX IF EXISTS public.output_unspent_fungible_category;
DROP INDEX IF EXISTS public.output_unspent_locking_prefix;

ALTER TABLE public.output
  DROP CONSTRAINT IF EXISTS output_node_membership_valid,
  DROP COLUMN accepted_node_ids,
  DROP COLUMN unspent_node_ids;

DROP SCHEMA output_membership CASCADE;
