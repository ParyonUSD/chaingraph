-- Run after applying the packaged migrations to an owned, fresh PostgreSQL DB.
DO $proof$
BEGIN
  IF to_regnamespace('output_membership') IS NOT NULL
     OR EXISTS (SELECT 1 FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
       WHERE a.attrelid = 'public.output'::regclass AND a.attnum > 0
         AND NOT a.attisdropped AND t.typcategory = 'A')
     OR EXISTS (SELECT 1 FROM pg_trigger WHERE tgname LIKE '%output_membership%')
     OR EXISTS (SELECT 1 FROM pg_class WHERE relkind = 'i' AND relname IN
       ('output_acceptance_index', 'unspent_output_index',
        'unspent_output_category_index', 'unspent_output_search_index'))
     OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = 'public' AND p.proname IN
         ('accepted_output', 'unspent_output')) THEN
    RAISE EXCEPTION 'No-array baseline contains membership objects';
  END IF;
  IF to_regclass('public.node_block_block_node_index') IS NULL
     OR position('WITH spent_outpoints AS MATERIALIZED' IN
       pg_get_functiondef('public.trigger_node_block_insert()'::regprocedure)) = 0 THEN
    RAISE EXCEPTION 'Required normalized index/bounded confirmation fix is missing';
  END IF;
END
$proof$;
SELECT jsonb_build_object(
  'postgres_version', version(),
  'membership_schema_absent', to_regnamespace('output_membership') IS NULL,
  'output_columns', (SELECT jsonb_agg(jsonb_build_object('name', attname,
    'type', format_type(atttypid, atttypmod)) ORDER BY attnum)
    FROM pg_attribute WHERE attrelid = 'public.output'::regclass
      AND attnum > 0 AND NOT attisdropped),
  'normalized_node_block_index', pg_get_indexdef('public.node_block_block_node_index'::regclass),
  'bounded_confirmation_fix', position('WITH spent_outpoints AS MATERIALIZED' IN
    pg_get_functiondef('public.trigger_node_block_insert()'::regprocedure)) > 0);
