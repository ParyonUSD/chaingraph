DROP FUNCTION IF EXISTS unspent_output_deferred_array(text);
DROP FUNCTION IF EXISTS unspent_deferred_array_build_root();
DROP FUNCTION IF EXISTS unspent_deferred_recompute_array();
ALTER TABLE output DROP COLUMN IF EXISTS unspent_node_ids;
-- unspent_deferred_run_batch / _initialize / _backfill keep their array-aware bodies (1791400002000 down drops them).
