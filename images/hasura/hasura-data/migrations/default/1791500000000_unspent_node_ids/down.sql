DROP FUNCTION IF EXISTS unspent_output_stored (text);
DROP FUNCTION IF EXISTS unspent_node_ids_build_root ();
DROP FUNCTION IF EXISTS unspent_node_ids_gained_creators (bigint);
DROP FUNCTION IF EXISTS unspent_node_ids_released_spends (bigint);
DROP FUNCTION IF EXISTS unspent_node_ids_backlog_transactions ();
DROP FUNCTION IF EXISTS unspent_node_ids_dirty_creators ();
DROP FUNCTION IF EXISTS unspent_node_ids_recent_spends ();
DROP FUNCTION IF EXISTS unspent_node_ids_dirty_transactions ();
DROP FUNCTION IF EXISTS unspent_node_ids_stored_path (text);
DROP FUNCTION IF EXISTS unspent_node_ids_rebuild_pending ();
DROP FUNCTION IF EXISTS unspent_node_ids_backfill_pending ();
DROP FUNCTION IF EXISTS unspent_node_ids_read_tier ();
DROP FUNCTION IF EXISTS unspent_node_ids_delete_consumed_events ();
DROP FUNCTION IF EXISTS unspent_node_ids_stop ();
DROP FUNCTION IF EXISTS unspent_node_ids_watch_pass (bigint);
DROP FUNCTION IF EXISTS unspent_node_ids_repartition (integer);
DROP FUNCTION IF EXISTS unspent_node_ids_initialize (integer, bigint, bigint, bigint);
DROP FUNCTION IF EXISTS unspent_node_ids_initialize (integer, bigint, bigint);
DROP FUNCTION IF EXISTS unspent_node_ids_backfill_scope (bytea, bytea);
DROP FUNCTION IF EXISTS unspent_node_ids_backfill_batch (integer, integer, integer);
DROP FUNCTION IF EXISTS unspent_node_ids_backfill_reset ();
DROP FUNCTION IF EXISTS unspent_node_ids_backfill_prepare (bigint, bigint, bigint);
DROP FUNCTION IF EXISTS unspent_node_ids_backfill_node_blocks (bigint, bigint, bigint);
DROP FUNCTION IF EXISTS unspent_node_ids_run_batch (integer, integer, bigint, bigint, bigint, integer, integer, integer, bigint);
-- signatures of earlier drafts of this migration (lab databases)
DROP FUNCTION IF EXISTS unspent_node_ids_run_batch (integer, integer, bigint, bigint, integer, integer, integer, bigint, boolean);
DROP FUNCTION IF EXISTS unspent_node_ids_temp_tables ();
DROP FUNCTION IF EXISTS unspent_node_ids_recompute ();
DROP FUNCTION IF EXISTS unspent_node_ids_recompute (boolean);
DROP FUNCTION IF EXISTS unspent_node_ids_accepting (bigint);
DROP TRIGGER IF EXISTS trigger_unspent_tracking_node_block_delete ON node_block;
DROP TRIGGER IF EXISTS trigger_unspent_tracking_node_transaction_delete ON node_transaction;
DROP FUNCTION IF EXISTS trigger_unspent_tracking_node_block_delete ();
DROP FUNCTION IF EXISTS trigger_unspent_tracking_node_transaction_delete ();
DROP TABLE IF EXISTS unspent_tracking_backfill_rows;
DROP TABLE IF EXISTS unspent_tracking_backfill_spends;
DROP TABLE IF EXISTS unspent_tracking_backfill_transactions;
DROP TABLE IF EXISTS unspent_tracking_backfill_nodes;
DROP TABLE IF EXISTS unspent_tracking_backfill;
DROP TABLE IF EXISTS unspent_tracking_skipped;
DROP TABLE IF EXISTS unspent_tracking_nodes;
DROP TABLE IF EXISTS unspent_tracking_watch;
DROP TABLE IF EXISTS unspent_tracking_settings;
DROP TABLE IF EXISTS unspent_tracking_progress;
DROP TABLE IF EXISTS unspent_tracking_events;
-- per-node partial indexes created by the agent
DO $$
DECLARE
  index_name text;
BEGIN
  FOR index_name IN SELECT indexname FROM pg_indexes WHERE schemaname = 'public' AND indexname LIKE 'output_unspent_node_%' LOOP
    EXECUTE format('DROP INDEX IF EXISTS %I', index_name);
  END LOOP;
END;
$$;
ALTER TABLE output DROP COLUMN IF EXISTS unspent_node_ids;
