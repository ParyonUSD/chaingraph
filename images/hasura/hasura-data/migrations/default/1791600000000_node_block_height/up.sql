-- node_block.block_height: a copy of block.height on each node_block row, plus
-- an index on (node_internal_id, block_height DESC), so "the highest block
-- accepted by node X" is a single index probe:
--
--   node(where: {name: {_eq: $node}}) {
--     accepted_blocks(order_by: {block_height: desc}, limit: 1) {
--       block { hash height previous_block_hash }
--     }
--   }
--
-- Without it, the usual tip query
--   block(where: {accepted_by: {node: {name: {_eq: $node}}}},
--         order_by: {height: desc}, limit: 1)
-- walks block_height_index downwards and probes node_block for every block,
-- so on a database shared by several networks it visits every block of the
-- other networks above the node's tip (~620k blocks for chipnet next to
-- mainnet). No index can fix that shape: the node filter and the height live
-- in different tables.
--
-- Block heights never change (a block row is never updated), so the copy
-- cannot go stale. The agent writes block_height on every insert; the
-- BEFORE INSERT trigger fills it from block for any other writer that omits
-- it (it does not fire for the agent's inserts).

-- Locking: adding and backfilling the column needs ACCESS EXCLUSIVE on
-- node_block, held until the migration transaction commits (Hasura's migrate
-- apply runs migrations in a transaction, so batching and CREATE INDEX
-- CONCURRENTLY are not available here). That lock blocks every node_block
-- reader (including the tip queries) and the agent's block inserts, so the
-- backfill is built to hold it as briefly as possible:
--
-- - lock_timeout bounds the wait for the lock. While the ALTER waits, every new
--   node_block reader queues behind it, so a long-running reader (e.g. the
--   agent's startup incomplete-block scan) would stall all node_block queries.
--   If the lock is not granted within 3 s the migration fails, the transaction
--   rolls back, the Hasura container exits, and its restart retries.
-- - The backfill rewrites the table (ALTER COLUMN TYPE ... USING) instead of
--   updating every row: an UPDATE is non-HOT here (every row grows), takes
--   about twice as long and doubles both the heap and node_block_pkey.
-- - The USING expression looks the height up in an array literal indexed by
--   block internal_id, built from block with one set-based scan once the lock
--   is held (no node_block row can appear after that). A per-row SQL function
--   lookup instead costs a full plan per row under plan_cache_mode =
--   force_custom_plan (production setting): 33.6 s of lock on 1.3M rows on a
--   production clone. If block internal_ids are too sparse for an array, or
--   outside the int4 range of array subscripts, the function is used with a
--   generic plan.
-- - If block_height already exists and is NOT NULL, it is complete and correct
--   (backfilled, then written by the agent and the trigger below), so it is
--   not rebuilt; re-applying the migration is a no-op.
--
-- Measured on 1,299,941 synthetic node_block rows (laptop, PG 18): about 2.0 s
-- of ACCESS EXCLUSIVE (vs 6.0 s for UPDATE ... FROM block and 6.4 s for the
-- per-row function under force_custom_plan); expect 2-3x that on a cloud vCPU.
--
-- Optional afterwards, only on a database where block_height was filled by an
-- in-place UPDATE (bloated heap and node_block_pkey), outside a transaction
-- and without blocking readers or the agent:
--   VACUUM (ANALYZE) node_block;
--   REINDEX INDEX CONCURRENTLY node_block_pkey;
SET LOCAL lock_timeout = '3s';

DO $migration$
DECLARE
  min_block_internal_id bigint;
  max_block_internal_id bigint;
  heights_by_internal_id text;
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_attribute
      WHERE attrelid = 'node_block'::regclass AND attname = 'block_height'
        AND attnotnull AND NOT attisdropped
  ) THEN
    RETURN;
  END IF;

  -- Takes ACCESS EXCLUSIVE on node_block (subject to lock_timeout).
  ALTER TABLE node_block ADD COLUMN IF NOT EXISTS block_height bigint;
  -- The trigger depends on the column type; recreated below.
  DROP TRIGGER IF EXISTS trigger_public_node_block_fill_height ON node_block;

  SELECT min(internal_id), max(internal_id)
    INTO min_block_internal_id, max_block_internal_id FROM block;

  -- Array subscripts are int4, so the array path also needs every id in range.
  IF max_block_internal_id IS NULL
    OR (max_block_internal_id - min_block_internal_id < 5000000
        AND min_block_internal_id >= -2147483648
        AND max_block_internal_id <= 2147483647) THEN
    -- '[min:max]={h_min,...,h_max}': subscript = block internal_id. Gaps are
    -- -1, never NULL (a NULL makes every subscript scan the null bitmap);
    -- node_block's foreign key guarantees every referenced block exists.
    SELECT format('[%s:%s]=', min_block_internal_id, max_block_internal_id)
           || array_agg(coalesce(block.height, -1) ORDER BY ids.internal_id)::text
      INTO heights_by_internal_id
      FROM generate_series(min_block_internal_id, max_block_internal_id) AS ids (internal_id)
      LEFT JOIN block ON block.internal_id = ids.internal_id;
    EXECUTE format(
      'ALTER TABLE node_block
         ALTER COLUMN block_height TYPE bigint USING (%L::bigint[])[block_internal_id],
         ALTER COLUMN block_height SET NOT NULL',
      coalesce(heights_by_internal_id, '{}'));
  ELSE
    SET LOCAL plan_cache_mode = 'force_generic_plan';
    CREATE FUNCTION migration_node_block_height_of(target_block_internal_id bigint) RETURNS bigint
      LANGUAGE sql STABLE
    AS 'SELECT height FROM block WHERE internal_id = target_block_internal_id';
    ALTER TABLE node_block
      ALTER COLUMN block_height TYPE bigint USING migration_node_block_height_of(block_internal_id),
      ALTER COLUMN block_height SET NOT NULL;
    DROP FUNCTION migration_node_block_height_of(bigint);
  END IF;
END
$migration$;

COMMENT ON COLUMN node_block.block_height IS 'The height of the referenced block (a copy of block.height, for ordering a node''s accepted blocks by height without joining block).';

CREATE INDEX IF NOT EXISTS node_block_node_height_index ON node_block USING btree (node_internal_id, block_height DESC);

CREATE OR REPLACE FUNCTION trigger_node_block_fill_height() RETURNS trigger
  LANGUAGE plpgsql
AS $$
BEGIN
  SELECT height INTO NEW.block_height FROM block WHERE internal_id = NEW.block_internal_id;
  RETURN NEW;
END;
$$;
CREATE OR REPLACE TRIGGER trigger_public_node_block_fill_height
  BEFORE INSERT ON node_block
  FOR EACH ROW WHEN (NEW.block_height IS NULL)
  EXECUTE FUNCTION trigger_node_block_fill_height();
COMMENT ON TRIGGER trigger_public_node_block_fill_height ON node_block IS 'Fills node_block.block_height from block.height when an insert omits it.';
