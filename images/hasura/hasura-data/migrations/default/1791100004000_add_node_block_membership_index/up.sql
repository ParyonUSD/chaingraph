/*
 * This index bounds output-membership lookups from a block to the nodes that
 * accept it. On an existing large database, build this exact index with
 * CREATE INDEX CONCURRENTLY before applying this migration. The catalog guard
 * makes application idempotent only when the existing index is valid and has
 * the expected, unqualified definition.
 */
DO $migration$
BEGIN
  IF to_regclass('public.node_block_block_node_index') IS NULL THEN
    CREATE INDEX node_block_block_node_index
      ON public.node_block USING btree (block_internal_id, node_internal_id);
  ELSIF NOT EXISTS (
    SELECT 1
      FROM pg_catalog.pg_index
      INNER JOIN pg_catalog.pg_class index_relation
        ON index_relation.oid = pg_index.indexrelid
      INNER JOIN pg_catalog.pg_class table_relation
        ON table_relation.oid = pg_index.indrelid
      INNER JOIN pg_catalog.pg_namespace index_namespace
        ON index_namespace.oid = index_relation.relnamespace
      WHERE index_namespace.nspname = 'public'
        AND index_relation.relname = 'node_block_block_node_index'
        AND table_relation.oid = 'public.node_block'::regclass
        AND pg_index.indisvalid
        AND pg_index.indisready
        AND NOT pg_index.indisunique
        AND pg_index.indpred IS NULL
        AND pg_index.indexprs IS NULL
        AND pg_get_indexdef(pg_index.indexrelid) =
          'CREATE INDEX node_block_block_node_index ON public.node_block USING btree (block_internal_id, node_internal_id)'
  ) THEN
    RAISE EXCEPTION
      'public.node_block_block_node_index exists but is invalid or has an unexpected definition';
  END IF;
END
$migration$;
