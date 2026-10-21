DROP TRIGGER IF EXISTS trigger_public_node_block_fill_height ON node_block;
DROP FUNCTION IF EXISTS trigger_node_block_fill_height();
DROP INDEX IF EXISTS node_block_node_height_index;
ALTER TABLE node_block DROP COLUMN IF EXISTS block_height;
