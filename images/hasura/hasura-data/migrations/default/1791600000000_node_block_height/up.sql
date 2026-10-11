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

-- Locking: every step below needs ACCESS EXCLUSIVE on node_block, held until
-- the migration transaction commits (Hasura's migrate apply runs migrations in
-- a transaction, so batching and CREATE INDEX CONCURRENTLY are not available
-- here). While the ALTER waits for the lock, every new node_block reader queues
-- behind it, so a long-running reader (e.g. the agent's startup scan) would
-- stall all queries that touch node_block. lock_timeout bounds that wait: if
-- the lock is not granted within 3 s, the migration fails, the transaction
-- rolls back, the Hasura container exits, and its restart retries the
-- migration. Once the lock is held, the rewrite below takes a few seconds per
-- million node_block rows.
SET LOCAL lock_timeout = '3s';

ALTER TABLE node_block ADD COLUMN IF NOT EXISTS block_height bigint;

-- Backfill by rewriting the table rather than with an UPDATE: a USING
-- expression forces ALTER COLUMN TYPE to rewrite the heap and rebuild every
-- index, so the backfill leaves no dead tuples and no index bloat (an UPDATE
-- of every row is non-HOT here and doubles both the heap and
-- node_block_pkey), and it is about twice as fast. On a database where the
-- column already exists, this re-copies it and compacts the table (the
-- trigger is dropped first because it depends on the column; it is recreated
-- below).
DROP TRIGGER IF EXISTS trigger_public_node_block_fill_height ON node_block;
CREATE FUNCTION migration_node_block_height_of(target_block_internal_id bigint) RETURNS bigint
  LANGUAGE sql STABLE
AS 'SELECT height FROM block WHERE internal_id = target_block_internal_id';
ALTER TABLE node_block
  ALTER COLUMN block_height TYPE bigint USING migration_node_block_height_of(block_internal_id),
  ALTER COLUMN block_height SET NOT NULL;
DROP FUNCTION migration_node_block_height_of(bigint);

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
CREATE TRIGGER trigger_public_node_block_fill_height
  BEFORE INSERT ON node_block
  FOR EACH ROW WHEN (NEW.block_height IS NULL)
  EXECUTE FUNCTION trigger_node_block_fill_height();
COMMENT ON TRIGGER trigger_public_node_block_fill_height ON node_block IS 'Fills node_block.block_height from block.height when an insert omits it.';
