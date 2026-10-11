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

ALTER TABLE node_block ADD COLUMN block_height bigint;

UPDATE node_block SET block_height = block.height
  FROM block WHERE block.internal_id = node_block.block_internal_id;

ALTER TABLE node_block ALTER COLUMN block_height SET NOT NULL;

COMMENT ON COLUMN node_block.block_height IS 'The height of the referenced block (a copy of block.height, for ordering a node''s accepted blocks by height without joining block).';

CREATE INDEX node_block_node_height_index ON node_block USING btree (node_internal_id, block_height DESC);

CREATE FUNCTION trigger_node_block_fill_height() RETURNS trigger
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
