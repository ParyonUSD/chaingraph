-- Stored per-node unspent set: output.unspent_node_ids, maintained off the
-- ingestion path by a job inside the agent (CHAINGRAPH_UNSPENT_TRACKING=true).
--
-- output.unspent_node_ids:
--   NULL   = not processed by the job yet;
--   '{}'   = processed, in no node's UTXO set;
--   {1,3}  = unspent for nodes 1 and 3 (sorted node internal ids): created by
--            a transaction the node accepts (in a block it accepts or in its
--            mempool) and not spent by any transaction the node accepts.
-- Every fact is per node: the value is the per-node unspent test (the
-- semantics of the unspent_output(node_name) query root), precomputed for
-- every node at once.
-- Rows that exist when the column is added read '{}' (a constant default is
-- kept in the catalog: no table rewrite, no WAL per row) and rows inserted
-- later read NULL. A pre-tracking value (an output of a transaction at or below
-- the tracking start) is trusted only once the backfill is complete; until
-- then the query root answers with the live per-node test. The backfill
-- (unspent_tracking_backfill_prepare / _batch) writes only the rows whose
-- value is not '{}' or may differ from what is stored: most outputs are spent
-- by a transaction every node accepts and keep the '{}' they already read.
--
-- Ingestion stays append-only. Outputs are inserted with NULL; nothing is
-- added to `input`. The only write-path triggers are two statement-level
-- AFTER DELETE triggers on node_block / node_transaction appending "released"
-- events (created disabled; the agent enables them when tracking is on). The
-- agent appends an "accepted" event only on its re-acceptance paths (a
-- node_block row for a block that already existed; a node_transaction row for
-- a transaction that already existed).
--
-- The job (unspent_tracking_run_batch) runs in N partitions (outpoint hash
-- ranges: ((get_byte(hash, 0) * N) >> 8) = partition), each in its own
-- REPEATABLE READ transaction on its own connection, with its own watermarks
-- in unspent_tracking_progress. A batch of partition k:
--   - consumes events above its consumed id (events are deleted once every
--     partition has consumed them);
--   (once per pass, before the batches, unspent_tracking_watch_pass re-checks
--   the watch set of recently released transactions and appends an
--   "accepted" event for each re-acceptance);
--   - walks the inputs above its input watermark (input_pkey order) whose
--     outpoint is in partition k, stopping before a transaction with an input
--     whose output row is missing (child-before-parent: retried next pass);
--   - takes the outputs created in the same transaction range (hash in k);
--   - takes blocks above its block watermark (newly accepted blocks whose
--     transactions were saved earlier, e.g. from a mempool);
--   - recomputes every collected output from scratch (idempotent and
--     order-independent); outputs seen for the first time and touched by
--     nothing else take a probe-free fast path.
-- Partitions never write the same row, so they never conflict with each
-- other. The query root uses the minimum watermark of all partitions: every
-- row created at or below it has been processed, and every spend above it is
-- re-checked live.
--
-- Reads: unspent_output(node_name) (generated per node by
-- unspent_tracking_build_root(); tracked in Hasura). The stored set, plus a
-- live correction for whatever the job has not processed, so the result
-- is always exact; above a backlog threshold (unspent_tracking_settings) it
-- falls back to the live per-node test.

-- Existing rows read the default '{}' (PostgreSQL >= 11 stores a constant
-- default once, in pg_attribute.attmissingval); dropping the default at once
-- makes every later insert NULL (the agent's inserts name their columns).
ALTER TABLE output ADD COLUMN unspent_node_ids integer[] DEFAULT '{}';
ALTER TABLE output ALTER COLUMN unspent_node_ids DROP DEFAULT;
COMMENT ON COLUMN output.unspent_node_ids IS 'Per-node unspent set (CHAINGRAPH_UNSPENT_TRACKING): NULL = not processed yet; otherwise the sorted internal ids of the nodes for which the output is unspent (created by a transaction the node accepts, no spender the node accepts). Rows that existed when the column was added read ''{}'' until the backfill has processed them: values of outputs created at or below the tracking start are used only once the backfill is complete. Maintained by the agent''s tracking job; read through unspent_output(node_name).';

CREATE TABLE unspent_tracking_events (
  id bigserial PRIMARY KEY,
  event_kind text NOT NULL CHECK (event_kind IN ('released', 'accepted')),
  node_internal_id bigint NOT NULL,
  block_internal_id bigint,
  transaction_internal_id bigint,
  created_at timestamp NOT NULL DEFAULT now()
);
COMMENT ON TABLE unspent_tracking_events IS 'Acceptance changes not yet consumed by every partition of the unspent tracking job: released (a node_block / node_transaction row deleted) or accepted (the agent''s re-acceptance paths). Not tracked by Hasura.';

CREATE TABLE unspent_tracking_progress (
  partition_index integer NOT NULL,
  partition_count integer NOT NULL,
  input_transaction_internal_id bigint NOT NULL,
  block_internal_id bigint NOT NULL,
  consumed_event_id bigint NOT NULL DEFAULT 0,
  backfill_transaction_internal_id bigint NOT NULL DEFAULT 0,
  backfill_prefix integer NOT NULL DEFAULT 0,
  updated_at timestamp NOT NULL DEFAULT now(),
  CONSTRAINT unspent_tracking_progress_pkey PRIMARY KEY (partition_index)
);
COMMENT ON TABLE unspent_tracking_progress IS 'Unspent tracking job watermarks, one row per partition (outpoint hash range): inputs of transactions up to input_transaction_internal_id, blocks up to block_internal_id and events up to consumed_event_id are processed for the partition''s outputs. Backfill: outputs whose transaction hash starts below the two-byte prefix backfill_prefix (an integer, 0-65536) are backfilled; backfill_transaction_internal_id is set to the tracking start once the partition''s whole range is (0 before). Not tracked by Hasura.';

CREATE TABLE unspent_tracking_settings (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  tracking_start_transaction_internal_id bigint,
  tracking_start_block_internal_id bigint,
  backfill_complete boolean NOT NULL DEFAULT false,
  rebuild_required boolean NOT NULL DEFAULT false,
  rebuild_through_transaction_internal_id bigint,
  read_hashed_max_transactions bigint NOT NULL DEFAULT 20000,
  read_probed_max_transactions bigint NOT NULL DEFAULT 200000,
  read_stored_max_events bigint NOT NULL DEFAULT 10000,
  watch_consumed_event_id bigint NOT NULL DEFAULT 0
);
INSERT INTO unspent_tracking_settings DEFAULT VALUES;
COMMENT ON TABLE unspent_tracking_settings IS 'Unspent tracking state and read thresholds (written by the agent from CHAINGRAPH_UNSPENT_TRACKING_* settings). tracking_start_*: outputs of earlier transactions are pre-tracking (their values are used once backfilled); backfill_complete: every partition''s backfill covered its whole range, so every pre-tracking value is exact (status only: reads derive it from the cursors, never from this flag); rebuild_required: tracking was stopped (stored values may be stale; the backfill takes every value that is not ''{}''), and reads use the live per-node test until that backfill is complete and the job has passed rebuild_through_transaction_internal_id (the highest transaction id when tracking restarted). Reads correct a backlog of up to read_hashed_max_transactions unprocessed transactions with one hashed set, up to read_probed_max_transactions with per-row probes, and use the live per-node test above it (or above read_stored_max_events unconsumed events). Not tracked by Hasura.';

CREATE TABLE unspent_tracking_watch (
  transaction_internal_id bigint NOT NULL,
  released_at_height bigint NOT NULL,
  accepted_node_ids integer[] NOT NULL,
  CONSTRAINT unspent_tracking_watch_pkey PRIMARY KEY (transaction_internal_id)
);
COMMENT ON TABLE unspent_tracking_watch IS 'Transactions some node stopped accepting recently (re-org, mempool drop or replacement; not a mempool row removed on confirmation), with the nodes accepting them when last checked; re-checked once per pass (unspent_tracking_watch_pass) for 100 blocks after the release, so a re-acceptance through a path that writes no event still becomes an "accepted" event. Not tracked by Hasura.';

CREATE TABLE unspent_tracking_nodes (
  node_internal_id integer NOT NULL,
  ready_after_event_id bigint NOT NULL,
  indexed_at timestamp NOT NULL DEFAULT now(),
  CONSTRAINT unspent_tracking_nodes_pkey PRIMARY KEY (node_internal_id)
);
COMMENT ON TABLE unspent_tracking_nodes IS 'Nodes the query root may serve from the stored set: a row is written by the agent once the node''s three per-node partial indexes are valid (and removed while they are rebuilt); the node is served from the stored set once every partition has consumed the events up to ready_after_event_id (the acceptance changes recorded before its indexes existed). Other nodes are answered by the live per-node test. Not tracked by Hasura.';

CREATE TABLE unspent_tracking_skipped (
  outpoint_transaction_hash bytea NOT NULL,
  outpoint_index bigint NOT NULL,
  transaction_internal_id bigint NOT NULL,
  CONSTRAINT unspent_tracking_skipped_pkey PRIMARY KEY (outpoint_transaction_hash, outpoint_index, transaction_internal_id)
);
COMMENT ON TABLE unspent_tracking_skipped IS 'Inputs the tracking job passed while their output row did not exist (a stall over the limit, or child-before-parent across the start of tracking): the output is fully recomputed (never fast-pathed) when it arrives. Not tracked by Hasura.';

-- Backfill state, written once by unspent_tracking_backfill_prepare (one
-- REPEATABLE READ snapshot, "the preparation") and emptied when the backfill
-- is complete (or tracking stops).
CREATE TABLE unspent_tracking_backfill (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  node_ids integer[] NOT NULL,
  full_node_ids integer[] NOT NULL,
  transaction_floor bigint NOT NULL,
  block_floor bigint NOT NULL,
  event_floor bigint NOT NULL,
  prepared_at timestamp NOT NULL DEFAULT now(),
  stats jsonb
);
COMMENT ON TABLE unspent_tracking_backfill IS 'The prepared backfill (one row while a backfill is prepared and not complete): node_ids = the node table at the preparation; full_node_ids = the "full set", the most common set of nodes accepting a block; transaction / block / event floors = settled sequence values read before the preparation (later transactions, blocks and events are re-checked by every batch). Not tracked by Hasura.';
CREATE TABLE unspent_tracking_backfill_nodes (
  node_internal_id integer NOT NULL,
  floor_blocks bigint NOT NULL,
  CONSTRAINT unspent_tracking_backfill_nodes_pkey PRIMARY KEY (node_internal_id)
);
COMMENT ON TABLE unspent_tracking_backfill_nodes IS 'Per node, its node_block rows at or below the block floor as of the event floor (unspent_tracking_backfill_node_blocks at the preparation): a batch stops ("stale") if the number changed, i.e. a node gained or lost such a block without an event. Not tracked by Hasura.';
CREATE TABLE unspent_tracking_backfill_transactions (
  internal_id bigint NOT NULL,
  hash bytea NOT NULL,
  CONSTRAINT unspent_tracking_backfill_transactions_pkey PRIMARY KEY (internal_id)
);
CREATE INDEX unspent_tracking_backfill_transactions_hash ON unspent_tracking_backfill_transactions (hash);
COMMENT ON TABLE unspent_tracking_backfill_transactions IS 'Transactions (at or below the transaction floor) not certainly accepted by exactly the full set at the preparation: in no block accepted by exactly the full set, or in a block accepted by a node outside it. Their outputs, and the outputs they spend, are recomputed exactly. Not tracked by Hasura.';
CREATE TABLE unspent_tracking_backfill_spends (
  outpoint_transaction_hash bytea NOT NULL,
  outpoint_index bigint NOT NULL,
  CONSTRAINT unspent_tracking_backfill_spends_pkey PRIMARY KEY (outpoint_transaction_hash, outpoint_index)
);
COMMENT ON TABLE unspent_tracking_backfill_spends IS 'Outpoints spent by unspent_tracking_backfill_transactions (recomputed exactly). Not tracked by Hasura.';
CREATE TABLE unspent_tracking_backfill_rows (
  transaction_hash bytea NOT NULL,
  output_index bigint NOT NULL,
  CONSTRAINT unspent_tracking_backfill_rows_pkey PRIMARY KEY (transaction_hash, output_index)
);
COMMENT ON TABLE unspent_tracking_backfill_rows IS 'Outputs whose stored value was not ''{}'' at the preparation (NULL: saved after the column was added; or a value left by an earlier tracking run): written whatever their new value. Not tracked by Hasura.';

-- Release events: statement-level, one INSERT ... SELECT per DELETE, whichever
-- code path deletes the rows (re-org, mempool drop, replacement, cascade,
-- mempool cleaning on block acceptance).
CREATE FUNCTION trigger_node_block_delete_unspent_tracking() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO unspent_tracking_events (event_kind, node_internal_id, block_internal_id)
    SELECT 'released', node_internal_id, block_internal_id FROM old_rows;
  RETURN NULL;
END;
$$;
CREATE FUNCTION trigger_node_transaction_delete_unspent_tracking() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO unspent_tracking_events (event_kind, node_internal_id, transaction_internal_id)
    SELECT 'released', node_internal_id, transaction_internal_id FROM old_rows;
  RETURN NULL;
END;
$$;
CREATE TRIGGER trigger_public_node_block_delete_unspent_tracking AFTER DELETE ON node_block
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_node_block_delete_unspent_tracking();
CREATE TRIGGER trigger_public_node_transaction_delete_unspent_tracking AFTER DELETE ON node_transaction
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_node_transaction_delete_unspent_tracking();
ALTER TABLE node_block DISABLE TRIGGER trigger_public_node_block_delete_unspent_tracking;
ALTER TABLE node_transaction DISABLE TRIGGER trigger_public_node_transaction_delete_unspent_tracking;

-- The nodes accepting a transaction now (sorted), in a block or in a mempool.
CREATE FUNCTION unspent_tracking_accepting (transaction_internal_id bigint)
  RETURNS integer[] LANGUAGE sql STABLE AS $$
  SELECT ARRAY(SELECT c.node FROM (
      SELECT n.internal_id::integer AS node
        FROM block_transaction bt
        CROSS JOIN node n
        JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
        WHERE bt.transaction_internal_id = $1
      UNION
      SELECT nt.node_internal_id::integer FROM node_transaction nt WHERE nt.transaction_internal_id = $1
    ) c ORDER BY c.node)
$$;

-- Recompute every output collected in pg_temp.unspent_tracking_affected from
-- scratch: (nodes accepting the creator) EXCEPT (nodes accepting any spender),
-- sorted. Only rows whose value changes are written, in key order; every
-- collected output gets its value, '{}' included (NULL only ever means "not
-- processed yet": the query root never reads a NULL as spent). Returns the
-- number of rows changed.
CREATE FUNCTION unspent_tracking_recompute ()
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  changed bigint;
BEGIN
  UPDATE output o SET unspent_node_ids = v.ids
    FROM (SELECT a.h, a.i,
                 ARRAY(SELECT c.node FROM (
                         SELECT n.internal_id::integer AS node
                           FROM block_transaction bt
                           CROSS JOIN node n
                           JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                           WHERE bt.transaction_internal_id = ct.internal_id
                         UNION
                         SELECT nt.node_internal_id::integer FROM node_transaction nt WHERE nt.transaction_internal_id = ct.internal_id
                         EXCEPT
                         SELECT n.internal_id::integer
                           FROM (SELECT transaction_internal_id FROM input
                                   WHERE input.outpoint_transaction_hash = a.h AND input.outpoint_index = a.i OFFSET 0) x
                           JOIN block_transaction bt ON bt.transaction_internal_id = x.transaction_internal_id
                           CROSS JOIN node n
                           JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                         EXCEPT
                         SELECT nt.node_internal_id::integer
                           FROM (SELECT transaction_internal_id FROM input
                                   WHERE input.outpoint_transaction_hash = a.h AND input.outpoint_index = a.i OFFSET 0) x
                           JOIN node_transaction nt ON nt.transaction_internal_id = x.transaction_internal_id
                       ) c ORDER BY c.node)::integer[] AS ids
            FROM (SELECT DISTINCT h, i FROM pg_temp.unspent_tracking_affected ORDER BY h, i) a
            CROSS JOIN LATERAL (
              SELECT internal_id FROM transaction WHERE transaction.hash = a.h OFFSET 0) ct
          OFFSET 0) v
    WHERE o.transaction_hash = v.h AND o.output_index = v.i
      AND o.unspent_node_ids IS DISTINCT FROM v.ids;
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed;
END;
$$;

CREATE FUNCTION unspent_tracking_temp_tables ()
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_affected (h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE INDEX IF NOT EXISTS unspent_tracking_affected_key ON pg_temp.unspent_tracking_affected (h, i);
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_fresh (h bytea NOT NULL, i bigint NOT NULL, creator bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_txs (id bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_inputs (tx bigint NOT NULL, h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_consumed (id bigint NOT NULL, event_kind text NOT NULL, node bigint NOT NULL,
    block_id bigint, tx_id bigint) ON COMMIT DELETE ROWS;
END;
$$;

-- One job batch for partition `part` of `part_count`. Every lookup is a keyed
-- probe (hash/merge joins and sequential scans disabled; the job's own small
-- tables grow between plans, so a plan cached while they were empty must not
-- scan them per row). Call inside a REPEATABLE READ transaction (one snapshot
-- for every step) and commit; `tx_limit` / `block_limit` / `event_limit` must
-- be settled sequence values (every transaction that allocated an id at or
-- below them has finished). Events are consumed up to `event_limit` only: an
-- event id is allocated when its row is inserted, so a later id can commit
-- first, and consuming past an uncommitted id would skip that event forever.
-- `max_inputs` bounds the inputs of all partitions walked by one batch. `skip_stall_through`: inputs of transactions up to this id whose
-- output row is missing are passed instead of stopping the batch (0 = never skip).
CREATE FUNCTION unspent_tracking_run_batch (part integer, part_count integer, tx_limit bigint, block_limit bigint,
  event_limit bigint, max_inputs integer, max_blocks integer, max_events integer, skip_stall_through bigint)
  RETURNS jsonb LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  zero_hash constant bytea := '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea;
  w bigint;
  wb bigint;
  consumed bigint;
  stored_count integer;
  upper_tx bigint;
  upper_block bigint;
  stall_tx bigint;
  skipped_inputs bigint := 0;
  n_inputs bigint := 0;
  n_events bigint := 0;
  n_blocks bigint := 0;
  n_block_txs bigint := 0;
  n_event_txs bigint := 0;
  n_affected bigint := 0;
  n_changed bigint := 0;
  n_fresh bigint := 0;
  max_event bigint;
BEGIN
  -- one worker per partition (also across agents): a second caller returns at once
  IF NOT pg_try_advisory_xact_lock(1970172785, part) THEN
    RETURN jsonb_build_object('busy', true);
  END IF;
  SELECT p.input_transaction_internal_id, p.block_internal_id, p.consumed_event_id, p.partition_count
    INTO w, wb, consumed, stored_count
    FROM unspent_tracking_progress p WHERE p.partition_index = part
    FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('uninitialized', true);
  END IF;
  IF stored_count <> part_count THEN
    RETURN jsonb_build_object('repartition', true);
  END IF;
  PERFORM unspent_tracking_temp_tables();

  -- 1. events above this partition's consumed id, up to the settled limit, in id order
  INSERT INTO unspent_tracking_consumed
    SELECT e.id, e.event_kind, e.node_internal_id, e.block_internal_id, e.transaction_internal_id
      FROM unspent_tracking_events e WHERE e.id > consumed AND e.id <= event_limit ORDER BY e.id LIMIT max_events;
  GET DIAGNOSTICS n_events = ROW_COUNT;
  IF n_events > 0 THEN
    SELECT max(id) INTO max_event FROM unspent_tracking_consumed;
    INSERT INTO unspent_tracking_txs (id)
      SELECT c.tx_id FROM unspent_tracking_consumed c WHERE c.tx_id IS NOT NULL
      UNION ALL
      SELECT bt.transaction_internal_id
        FROM (SELECT DISTINCT c.block_id FROM unspent_tracking_consumed c WHERE c.block_id IS NOT NULL) b
        CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                              WHERE block_transaction.block_internal_id = b.block_id OFFSET 0) bt;
    GET DIAGNOSTICS n_event_txs = ROW_COUNT;
  END IF;
  -- (2. the watch set is re-checked once per pass by unspent_tracking_watch_pass,
  -- which turns a re-acceptance into "accepted" events consumed here)

  -- 3. input / transaction range above the watermark (bounded by the inputs of
  -- all partitions); only this partition's outpoints
  upper_tx := w;
  IF tx_limit > w THEN
    SELECT i.transaction_internal_id INTO upper_tx
      FROM input i
      WHERE i.transaction_internal_id > w AND i.transaction_internal_id <= tx_limit
      ORDER BY i.transaction_internal_id, i.input_index
      OFFSET GREATEST(max_inputs, 1) - 1 LIMIT 1;
    IF upper_tx IS NULL THEN
      upper_tx := tx_limit;
    END IF;
    INSERT INTO unspent_tracking_inputs (tx, h, i)
      SELECT i.transaction_internal_id, i.outpoint_transaction_hash, i.outpoint_index
        FROM input i
        WHERE i.transaction_internal_id > w AND i.transaction_internal_id <= upper_tx
          AND i.outpoint_transaction_hash <> zero_hash
          AND ((get_byte(i.outpoint_transaction_hash, 0) * part_count) >> 8) = part;
    -- child-before-parent: stop before the first transaction with an input
    -- whose output row does not exist yet (retried next pass)
    SELECT min(c.tx) INTO stall_tx
      FROM unspent_tracking_inputs c
      WHERE c.tx > skip_stall_through
        AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = c.h AND o.output_index = c.i);
    IF stall_tx IS NOT NULL THEN
      upper_tx := stall_tx - 1;
      DELETE FROM unspent_tracking_inputs WHERE tx > upper_tx;
    END IF;
    IF skip_stall_through > w THEN
      INSERT INTO unspent_tracking_skipped (outpoint_transaction_hash, outpoint_index, transaction_internal_id)
        SELECT c.h, c.i, c.tx
          FROM unspent_tracking_inputs c
          WHERE c.tx <= skip_stall_through
            AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = c.h AND o.output_index = c.i)
        ON CONFLICT ON CONSTRAINT unspent_tracking_skipped_pkey DO NOTHING;
      GET DIAGNOSTICS skipped_inputs = ROW_COUNT;
    END IF;
    SELECT count(*) INTO n_inputs FROM unspent_tracking_inputs;
    INSERT INTO unspent_tracking_affected (h, i) SELECT h, i FROM unspent_tracking_inputs;
    IF upper_tx > w THEN
      -- outputs created by the range (this partition): those the job has not
      -- seen (NULL) take the fast path in step 6 unless something else touches them
      INSERT INTO unspent_tracking_fresh (h, i, creator)
        SELECT o.transaction_hash, o.output_index, t.internal_id
          FROM transaction t
          CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_ids AS stored FROM output
                                WHERE output.transaction_hash = t.hash OFFSET 0) o
          WHERE t.internal_id > w AND t.internal_id <= upper_tx
            AND ((get_byte(t.hash, 0) * part_count) >> 8) = part AND o.stored IS NULL;
      INSERT INTO unspent_tracking_affected (h, i)
        SELECT o.transaction_hash, o.output_index
          FROM transaction t
          CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_ids AS stored FROM output
                                WHERE output.transaction_hash = t.hash OFFSET 0) o
          WHERE t.internal_id > w AND t.internal_id <= upper_tx
            AND ((get_byte(t.hash, 0) * part_count) >> 8) = part AND o.stored IS NOT NULL;
      -- fresh outputs a passed (skipped) input spends: full recompute
      INSERT INTO unspent_tracking_affected (h, i)
        SELECT f.h, f.i FROM unspent_tracking_fresh f
          WHERE EXISTS (SELECT 1 FROM unspent_tracking_skipped s
                          WHERE s.outpoint_transaction_hash = f.h AND s.outpoint_index = f.i);
    END IF;
  END IF;

  -- 4. blocks above the block watermark: their transactions saved before this
  -- batch's range (the range step sees the acceptance of the others)
  upper_block := wb;
  IF block_limit > wb THEN
    SELECT max(b.internal_id), count(*) INTO upper_block, n_blocks
      FROM (SELECT internal_id FROM block WHERE internal_id > wb AND internal_id <= block_limit
              ORDER BY internal_id LIMIT GREATEST(max_blocks, 1)) b;
    IF upper_block IS NULL THEN
      upper_block := wb;
    ELSE
      INSERT INTO unspent_tracking_txs (id)
        SELECT bt.transaction_internal_id
          FROM (SELECT internal_id FROM block WHERE internal_id > wb AND internal_id <= upper_block) b
          CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                WHERE block_transaction.block_internal_id = b.internal_id
                                  AND block_transaction.transaction_internal_id <= w OFFSET 0) bt;
      GET DIAGNOSTICS n_block_txs = ROW_COUNT;
    END IF;
  END IF;

  -- 5. collected transactions -> the outputs they spend and create (this partition)
  INSERT INTO unspent_tracking_affected (h, i)
    SELECT i.outpoint_transaction_hash, i.outpoint_index
      FROM (SELECT DISTINCT id FROM unspent_tracking_txs) x
      CROSS JOIN LATERAL (SELECT outpoint_transaction_hash, outpoint_index FROM input
                            WHERE input.transaction_internal_id = x.id OFFSET 0) i
      WHERE i.outpoint_transaction_hash <> zero_hash
        AND ((get_byte(i.outpoint_transaction_hash, 0) * part_count) >> 8) = part;
  INSERT INTO unspent_tracking_affected (h, i)
    SELECT o.transaction_hash, o.output_index
      FROM (SELECT DISTINCT id FROM unspent_tracking_txs) x
      CROSS JOIN LATERAL (SELECT hash FROM transaction WHERE transaction.internal_id = x.id OFFSET 0) t
      CROSS JOIN LATERAL (SELECT transaction_hash, output_index FROM output
                            WHERE output.transaction_hash = t.hash OFFSET 0) o
      WHERE ((get_byte(t.hash, 0) * part_count) >> 8) = part;

  -- 6. recompute every collected output from scratch; then the fast path for
  -- outputs the job sees for the first time and nothing else touched (no
  -- processed input can spend them: the job stops before an input whose output
  -- is missing, and records the ones it passes in unspent_tracking_skipped):
  -- the creator's accepting nodes, without probing spent_by_index
  SELECT count(*) INTO n_affected FROM unspent_tracking_affected;
  IF n_affected > 0 THEN
    n_changed := unspent_tracking_recompute();
  END IF;
  UPDATE output o SET unspent_node_ids = v.ids
    FROM (SELECT f.h, f.i, unspent_tracking_accepting(f.creator) AS ids
            FROM unspent_tracking_fresh f
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.unspent_tracking_affected a WHERE a.h = f.h AND a.i = f.i)
            ORDER BY f.h, f.i OFFSET 0) v
    WHERE o.transaction_hash = v.h AND o.output_index = v.i
      AND o.unspent_node_ids IS DISTINCT FROM v.ids;
  GET DIAGNOSTICS n_fresh = ROW_COUNT;
  n_affected := n_affected + n_fresh;
  n_changed := n_changed + n_fresh;
  -- passed inputs whose output this batch processed are dropped
  DELETE FROM unspent_tracking_skipped s USING unspent_tracking_fresh f
    WHERE s.outpoint_transaction_hash = f.h AND s.outpoint_index = f.i;

  -- 7. watermarks (same transaction as the updates)
  UPDATE unspent_tracking_progress p
    SET input_transaction_internal_id = upper_tx,
        block_internal_id = upper_block,
        consumed_event_id = GREATEST(p.consumed_event_id, COALESCE(max_event, 0)),
        updated_at = now()
    WHERE p.partition_index = part;

  RETURN jsonb_build_object(
    'inputWatermark', upper_tx, 'previousInputWatermark', w,
    'blockWatermark', upper_block, 'previousBlockWatermark', wb,
    'inputs', n_inputs, 'stalledAt', stall_tx, 'skippedInputs', skipped_inputs,
    'events', n_events, 'eventTransactions', n_event_txs,
    'blocks', n_blocks, 'blockTransactions', n_block_txs,
    'affected', n_affected, 'changed', n_changed, 'fresh', n_fresh);
END;
$$;
COMMENT ON FUNCTION unspent_tracking_run_batch (integer, integer, bigint, bigint, bigint, integer, integer, integer, bigint) IS 'One batch of the unspent tracking job for one partition (outpoint hash range). Call in a REPEATABLE READ transaction with settled limits; see migration 1791500000000_unspent_node_ids.';

-- Backfill of pre-tracking outputs (created by transactions at or below the
-- tracking start). Values of these outputs are used only once every
-- partition's backfill is complete (until then the query root answers with
-- the live per-node test), so the backfill may leave a row as it is only if
-- its stored value is exact.
--
-- Most outputs need no write: they read '{}' (the column default for rows
-- that existed when the column was added) and they are spent by a transaction
-- every node accepts. The backfill writes only the others:
--   (1) unspent_tracking_backfill_prepare, once, in one REPEATABLE READ
--       snapshot (the preparation), with settled floors (transaction, block,
--       event sequence values read before it):
--       - the full set F: the most common set of nodes accepting a block
--         (on a database with one chain: every node);
--       - M (unspent_tracking_backfill_transactions): transactions up to the
--         transaction floor that are not certainly accepted by exactly F: in
--         no block accepted by exactly F, or in a block accepted by a node
--         outside F. Found from the blocks: one pass over node_block gives
--         each block's node set; the "other" blocks (not accepted by exactly
--         F: stale blocks, another network's) are few on a single-chain
--         database. Their transactions are read with keyed probes driven
--         from the other blocks, plus one anti-join of transaction against
--         block_transaction for the transactions in no block at all;
--       - the outpoints M spends (unspent_tracking_backfill_spends), with
--         keyed probes driven from M;
-- Statements whose cost must follow a small driving set (the other blocks,
-- M) take their probes through LATERAL subqueries with OFFSET 0: the planner
-- cannot reorder or flatten them, so the plan is a nested loop driven by the
-- small side whatever the statistics say (on mainnet, with another network's
-- transactions in M, the planner's estimates walked whole indexes instead).
--       - the rows whose stored value is not '{}' (NULL: saved after the
--         column was added; or values left by an earlier run, after a stop)
--         (unspent_tracking_backfill_rows; one sequential scan of output);
--       - per node, its node_block rows at or below the block floor.
--   (2) unspent_tracking_backfill_batch, per partition, a range of two-byte
--       transaction hash prefixes per batch, in a REPEATABLE READ snapshot S
--       that holds the partition's progress row (so the batch is serialised
--       with the partition's job batches). With X = M plus the transactions
--       whose acceptance may have changed since the preparation (those of
--       events above the event floor, of blocks above the block floor, with a
--       node_transaction row now, and above the transaction floor):
--       - exact recompute (unspent_tracking_recompute) of the outputs in the
--         range created by X or spent by X;
--       - outputs in the range with no spending input at all (a merge
--         anti-join of the output_pkey and spent_by_index ranges) whose
--         creator is not in X: written F;
--       - rows of unspent_tracking_backfill_rows in the range that are in
--         neither group: written '{}';
--       - every other output in the range keeps its value ('{}').
-- Why every other output is exact at S: it has a spending input, its creator
-- C is not in X and no spender is in X. C not in M means C was, at the
-- preparation, in a block accepted by exactly F, in no block accepted by a
-- node outside F, and (C not in X) since then no event touched C or its
-- blocks, no later block contains it and it has no node_transaction row: the
-- nodes accepting C at S are exactly F. A spender s not in X is likewise
-- accepted at S by every node of F. So no node accepts C without accepting a
-- spender: the value is '{}'. Its stored value is '{}' too: it was '{}' at the
-- preparation (else it is in unspent_tracking_backfill_rows), and a job batch
-- that wrote it since recomputed it from a state where C and s had the same
-- acceptance (any change is an event, a block or a mempool row, all in X),
-- i.e. '{}' again. Likewise an output with no spender whose creator is not in
-- X is unspent for exactly F. Losses of acceptance always write an event (the
-- release triggers), and the agent writes an "accepted" event whenever it adds
-- a node_block row to an existing block or a node_transaction row to an
-- existing transaction; a batch stops ("stale") if the node table changed or a
-- node's node_block rows at or below the block floor changed in a way its
-- events do not account for (then unspent_tracking_backfill_reset).
-- Events above the event floor are kept while the backfill is prepared.
-- Whatever the batch writes is computed from S and commits with the
-- partition's progress row, so later job batches of the partition see it, and
-- every acceptance change committed after S is still ahead of them.
-- A node's node_block rows at or below the block floor, as of the event floor:
-- the rows now, minus those added by "accepted" events above the floor, plus
-- those removed by "released" events above it. Constant while every change
-- writes its event (the release triggers; the agent's re-acceptance paths).
CREATE FUNCTION unspent_tracking_backfill_node_blocks (node_id bigint, block_floor bigint, event_floor bigint)
  RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT (SELECT count(*) FROM node_block nb WHERE nb.node_internal_id = node_id AND nb.block_internal_id <= block_floor)
       - (SELECT count(*) FROM unspent_tracking_events e
            WHERE e.id > event_floor AND e.node_internal_id = node_id AND e.event_kind = 'accepted'
              AND e.block_internal_id <= block_floor)
       + (SELECT count(*) FROM unspent_tracking_events e
            WHERE e.id > event_floor AND e.node_internal_id = node_id AND e.event_kind = 'released'
              AND e.block_internal_id <= block_floor)
$$;

CREATE FUNCTION unspent_tracking_backfill_prepare (transaction_floor bigint, block_floor bigint, event_floor bigint)
  RETURNS jsonb LANGUAGE plpgsql
  SET jit = off SET plan_cache_mode = force_custom_plan AS $$
DECLARE
  zero_hash constant bytea := '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea;
  start_tx bigint;
  full_ids integer[];
  current_node_ids integer[];
  n_full_blocks bigint := 0;
  n_unblocked bigint := 0;
  n_other bigint := 0;
  n_spends bigint := 0;
  n_rows bigint := 0;
  n_other_blocks bigint := 0;
  started timestamp := clock_timestamp();
  clock timestamp := clock_timestamp();
  steps jsonb := '{}';
  stats jsonb;
BEGIN
  IF NOT pg_try_advisory_xact_lock(1970172789) THEN
    RETURN jsonb_build_object('busy', true);
  END IF;
  SELECT tracking_start_transaction_internal_id INTO start_tx FROM unspent_tracking_settings;
  IF start_tx IS NULL OR NOT EXISTS (SELECT 1 FROM unspent_tracking_progress) THEN
    RETURN jsonb_build_object('uninitialized', true);
  END IF;
  IF EXISTS (SELECT 1 FROM unspent_tracking_backfill) THEN
    RETURN jsonb_build_object('prepared', true);
  END IF;
  IF NOT unspent_tracking_backfill_pending() THEN
    RETURN jsonb_build_object('done', true);
  END IF;
  SELECT COALESCE(array_agg(n.internal_id::integer ORDER BY n.internal_id), '{}') INTO current_node_ids FROM node n;
  -- each block's node set (one pass over node_block)
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_block_nodes (id bigint PRIMARY KEY, ids integer[] NOT NULL) ON COMMIT DROP;
  INSERT INTO unspent_tracking_block_nodes (id, ids)
    SELECT nb.block_internal_id, array_agg(nb.node_internal_id::integer ORDER BY nb.node_internal_id)
      FROM node_block nb GROUP BY nb.block_internal_id;
  ANALYZE pg_temp.unspent_tracking_block_nodes;
  SELECT bn.ids INTO full_ids FROM pg_temp.unspent_tracking_block_nodes bn
    GROUP BY bn.ids ORDER BY count(*) DESC, bn.ids LIMIT 1;
  full_ids := COALESCE(full_ids, '{}');
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_full_blocks (id bigint PRIMARY KEY) ON COMMIT DROP;
  INSERT INTO unspent_tracking_full_blocks (id)
    SELECT bn.id FROM pg_temp.unspent_tracking_block_nodes bn WHERE bn.ids = full_ids;
  GET DIAGNOSTICS n_full_blocks = ROW_COUNT;
  ANALYZE pg_temp.unspent_tracking_full_blocks;
  -- the other blocks: not accepted by exactly F (accepted by no node, by a
  -- part of F, or by a node outside F: `outside`)
  CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_other_blocks (id bigint PRIMARY KEY, outside boolean NOT NULL) ON COMMIT DROP;
  INSERT INTO unspent_tracking_other_blocks (id, outside)
    SELECT b.internal_id, COALESCE(NOT (bn.ids <@ full_ids), false)
      FROM block b LEFT JOIN pg_temp.unspent_tracking_block_nodes bn ON bn.id = b.internal_id
      WHERE bn.ids IS DISTINCT FROM full_ids;
  GET DIAGNOSTICS n_other_blocks = ROW_COUNT;
  ANALYZE pg_temp.unspent_tracking_other_blocks;
  steps := steps || jsonb_build_object('blocks', round(extract(epoch FROM clock_timestamp() - clock) * 1000));
  clock := clock_timestamp();
  -- M (a): in no block at all (dropped or mempool-only transactions)
  INSERT INTO unspent_tracking_backfill_transactions (internal_id, hash)
    SELECT t.internal_id, t.hash FROM transaction t
      WHERE t.internal_id <= transaction_floor
        AND NOT EXISTS (SELECT 1 FROM block_transaction bt WHERE bt.transaction_internal_id = t.internal_id);
  GET DIAGNOSTICS n_unblocked = ROW_COUNT;
  steps := steps || jsonb_build_object('unblocked', round(extract(epoch FROM clock_timestamp() - clock) * 1000));
  clock := clock_timestamp();
  -- M (b): in an other block, and in a block accepted by a node outside F or
  -- in no block accepted by exactly F. Driven from the other blocks: per
  -- block, its block_transaction range (primary key), then per transaction
  -- one probe of block_inclusions_index (only for blocks with no node
  -- outside F) and one of transaction_pkey for the hash.
  INSERT INTO unspent_tracking_backfill_transactions (internal_id, hash)
    SELECT x.id, t.hash
      FROM pg_temp.unspent_tracking_other_blocks ob
      CROSS JOIN LATERAL (SELECT bt.transaction_internal_id AS id FROM block_transaction bt
                            WHERE bt.block_internal_id = ob.id AND bt.transaction_internal_id <= transaction_floor
                            OFFSET 0) x
      CROSS JOIN LATERAL (SELECT tx.hash FROM transaction tx WHERE tx.internal_id = x.id OFFSET 0) t
      WHERE ob.outside
         OR NOT EXISTS (SELECT 1 FROM (SELECT bt2.block_internal_id FROM block_transaction bt2
                                         WHERE bt2.transaction_internal_id = x.id OFFSET 0) i
                          CROSS JOIN LATERAL (SELECT 1 FROM pg_temp.unspent_tracking_full_blocks f
                                                WHERE f.id = i.block_internal_id OFFSET 0) full_block)
    ON CONFLICT ON CONSTRAINT unspent_tracking_backfill_transactions_pkey DO NOTHING;
  GET DIAGNOSTICS n_other = ROW_COUNT;
  steps := steps || jsonb_build_object('otherBlocks', round(extract(epoch FROM clock_timestamp() - clock) * 1000));
  clock := clock_timestamp();
  -- the outpoints M spends: per transaction of M, its input_pkey range
  -- (statistics for M first: the batches join it too)
  ANALYZE unspent_tracking_backfill_transactions;
  INSERT INTO unspent_tracking_backfill_spends (outpoint_transaction_hash, outpoint_index)
    SELECT i.outpoint_transaction_hash, i.outpoint_index
      FROM unspent_tracking_backfill_transactions m
      CROSS JOIN LATERAL (SELECT input.outpoint_transaction_hash, input.outpoint_index FROM input
                            WHERE input.transaction_internal_id = m.internal_id
                              AND input.outpoint_transaction_hash <> zero_hash
                            OFFSET 0) i
    ON CONFLICT ON CONSTRAINT unspent_tracking_backfill_spends_pkey DO NOTHING;
  GET DIAGNOSTICS n_spends = ROW_COUNT;
  steps := steps || jsonb_build_object('spends', round(extract(epoch FROM clock_timestamp() - clock) * 1000));
  clock := clock_timestamp();
  INSERT INTO unspent_tracking_backfill_rows (transaction_hash, output_index)
    SELECT o.transaction_hash, o.output_index FROM output o
      WHERE o.unspent_node_ids IS DISTINCT FROM '{}'::integer[];
  GET DIAGNOSTICS n_rows = ROW_COUNT;
  steps := steps || jsonb_build_object('rows', round(extract(epoch FROM clock_timestamp() - clock) * 1000));
  INSERT INTO unspent_tracking_backfill_nodes (node_internal_id, floor_blocks)
    SELECT n.internal_id, unspent_tracking_backfill_node_blocks(n.internal_id, block_floor, event_floor)
      FROM node n;
  stats := jsonb_build_object('fullNodeIds', full_ids, 'fullBlocks', n_full_blocks, 'otherBlocks', n_other_blocks,
    'transactions', n_unblocked + n_other, 'unblockedTransactions', n_unblocked, 'otherBlockTransactions', n_other,
    'spends', n_spends, 'rows', n_rows,
    'ms', round(extract(epoch FROM clock_timestamp() - started) * 1000), 'msSteps', steps);
  INSERT INTO unspent_tracking_backfill (node_ids, full_node_ids, transaction_floor, block_floor, event_floor, stats)
    VALUES (current_node_ids, full_ids, transaction_floor, block_floor, event_floor, stats);
  ANALYZE unspent_tracking_backfill_spends;
  ANALYZE unspent_tracking_backfill_rows;
  RETURN stats;
END;
$$;
COMMENT ON FUNCTION unspent_tracking_backfill_prepare (bigint, bigint, bigint) IS 'Prepare the backfill of pre-tracking outputs (once; call in a REPEATABLE READ transaction with settled sequence values); see migration 1791500000000_unspent_node_ids.';

-- Forget the prepared backfill (a batch found it stale): the partitions whose
-- backfill is not complete restart from the beginning of their range after a
-- new preparation (done partitions stay done: their values were exact at
-- their snapshots, and every later change is the job's).
CREATE FUNCTION unspent_tracking_backfill_reset ()
  RETURNS boolean LANGUAGE plpgsql SET lock_timeout = '2s' AS $$
DECLARE
  start_tx bigint;
BEGIN
  LOCK TABLE unspent_tracking_progress IN EXCLUSIVE MODE;
  SELECT tracking_start_transaction_internal_id INTO start_tx FROM unspent_tracking_settings;
  DELETE FROM unspent_tracking_backfill;
  TRUNCATE unspent_tracking_backfill_nodes, unspent_tracking_backfill_transactions,
    unspent_tracking_backfill_spends, unspent_tracking_backfill_rows;
  UPDATE unspent_tracking_progress SET backfill_prefix = 0, updated_at = now()
    WHERE backfill_transaction_internal_id < COALESCE(start_tx, 0);
  RETURN true;
END;
$$;

-- One backfill batch of partition `part`: the next `max_prefixes` two-byte
-- transaction hash prefixes of the partition's range (see above). Call in a
-- REPEATABLE READ transaction after unspent_tracking_backfill_prepare.
CREATE FUNCTION unspent_tracking_backfill_batch (part integer, part_count integer, max_prefixes integer)
  RETURNS jsonb LANGUAGE plpgsql
  SET jit = off SET plan_cache_mode = force_custom_plan AS $$
DECLARE
  cursor_tx bigint;
  start_tx bigint;
  stored_count integer;
  prefix integer;
  bf unspent_tracking_backfill;
  range_start integer;
  range_end integer;
  p1 integer;
  p2 integer;
  lo bytea;
  hi bytea;
  n_dynamic bigint := 0;
  n_exact bigint := 0;
  n_no_spender bigint := 0;
  n_full bigint := 0;
  n_reset bigint := 0;
  n_changed bigint := 0;
  clock timestamp := clock_timestamp();
  ms_select numeric := 0;
  ms_exact numeric := 0;
  ms_write numeric := 0;
BEGIN
  IF NOT pg_try_advisory_xact_lock(1970172786, part) THEN
    RETURN jsonb_build_object('busy', true);
  END IF;
  SELECT tracking_start_transaction_internal_id INTO start_tx FROM unspent_tracking_settings;
  SELECT p.backfill_transaction_internal_id, p.partition_count, p.backfill_prefix INTO cursor_tx, stored_count, prefix
    FROM unspent_tracking_progress p WHERE p.partition_index = part FOR UPDATE;
  IF NOT FOUND OR start_tx IS NULL THEN
    RETURN jsonb_build_object('uninitialized', true);
  END IF;
  IF stored_count <> part_count THEN
    RETURN jsonb_build_object('repartition', true);
  END IF;
  IF cursor_tx >= start_tx THEN
    RETURN jsonb_build_object('done', true, 'backfillWatermark', cursor_tx);
  END IF;
  SELECT * INTO bf FROM unspent_tracking_backfill;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('unprepared', true);
  END IF;
  IF bf.node_ids IS DISTINCT FROM (SELECT COALESCE(array_agg(n.internal_id::integer ORDER BY n.internal_id), '{}') FROM node n)
     OR EXISTS (
       SELECT 1 FROM unspent_tracking_backfill_nodes bn
         WHERE unspent_tracking_backfill_node_blocks(bn.node_internal_id, bf.block_floor, bf.event_floor)
               <> bn.floor_blocks) THEN
    RETURN jsonb_build_object('stale', true);
  END IF;
  -- the partition's first hash bytes: ((byte * part_count) >> 8) = part
  range_start := ((part * 256 + part_count - 1) / part_count) * 256;
  range_end := (((part + 1) * 256 + part_count - 1) / part_count) * 256;
  p1 := GREATEST(prefix, range_start);
  p2 := LEAST(p1 + GREATEST(max_prefixes, 1), range_end);
  IF p1 < range_end THEN
    lo := decode(lpad(to_hex(p1), 4, '0'), 'hex');
    hi := CASE WHEN p2 < 65536 THEN decode(lpad(to_hex(p2), 4, '0'), 'hex') ELSE decode(repeat('ff', 33), 'hex') END;
    PERFORM unspent_tracking_temp_tables();
    CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_dynamic (id bigint NOT NULL, hash bytea NOT NULL) ON COMMIT DELETE ROWS;
    CREATE TEMP TABLE IF NOT EXISTS unspent_tracking_no_spender (h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
    -- X beyond M: acceptance may have changed since the preparation
    INSERT INTO unspent_tracking_dynamic (id, hash)
      SELECT x.id, t.hash
        FROM (SELECT e.transaction_internal_id AS id FROM unspent_tracking_events e
                WHERE e.id > bf.event_floor AND e.transaction_internal_id IS NOT NULL
              UNION
              SELECT bt.transaction_internal_id FROM unspent_tracking_events e
                JOIN block_transaction bt ON bt.block_internal_id = e.block_internal_id
                WHERE e.id > bf.event_floor
              UNION
              SELECT bt.transaction_internal_id FROM block b
                JOIN block_transaction bt ON bt.block_internal_id = b.internal_id
                WHERE b.internal_id > bf.block_floor
              UNION
              SELECT nt.transaction_internal_id FROM node_transaction nt
              UNION
              SELECT t2.internal_id FROM transaction t2 WHERE t2.internal_id > bf.transaction_floor) x
        JOIN transaction t ON t.internal_id = x.id;
    GET DIAGNOSTICS n_dynamic = ROW_COUNT;
    ANALYZE pg_temp.unspent_tracking_dynamic;
    -- exact: created by X or spent by X
    INSERT INTO unspent_tracking_affected (h, i)
      SELECT o.transaction_hash, o.output_index FROM unspent_tracking_backfill_transactions m
        JOIN output o ON o.transaction_hash = m.hash
        WHERE m.hash >= lo AND m.hash < hi
      UNION ALL
      SELECT o.transaction_hash, o.output_index FROM pg_temp.unspent_tracking_dynamic d
        JOIN output o ON o.transaction_hash = d.hash
        WHERE d.hash >= lo AND d.hash < hi
      UNION ALL
      SELECT s.outpoint_transaction_hash, s.outpoint_index FROM unspent_tracking_backfill_spends s
        WHERE s.outpoint_transaction_hash >= lo AND s.outpoint_transaction_hash < hi
      UNION ALL
      -- per dynamic transaction, its input_pkey range (not the prefix range
      -- of spent_by_index: millions of inputs per batch on mainnet)
      SELECT i.outpoint_transaction_hash, i.outpoint_index FROM pg_temp.unspent_tracking_dynamic d
        CROSS JOIN LATERAL (SELECT input.outpoint_transaction_hash, input.outpoint_index FROM input
                              WHERE input.transaction_internal_id = d.id
                                AND input.outpoint_transaction_hash >= lo AND input.outpoint_transaction_hash < hi
                              OFFSET 0) i;
    GET DIAGNOSTICS n_exact = ROW_COUNT;
    -- no spending input at all (both index ranges, merged)
    INSERT INTO unspent_tracking_no_spender (h, i)
      SELECT o.transaction_hash, o.output_index FROM output o
        WHERE o.transaction_hash >= lo AND o.transaction_hash < hi
          AND NOT EXISTS (SELECT 1 FROM input i
                            WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
                              AND i.outpoint_transaction_hash >= lo AND i.outpoint_transaction_hash < hi);
    GET DIAGNOSTICS n_no_spender = ROW_COUNT;
    ms_select := round(extract(epoch FROM clock_timestamp() - clock) * 1000);
    clock := clock_timestamp();
    IF n_exact > 0 THEN
      n_changed := unspent_tracking_recompute();
    END IF;
    ms_exact := round(extract(epoch FROM clock_timestamp() - clock) * 1000);
    clock := clock_timestamp();
    -- no spender, creator not in X: unspent for exactly F
    UPDATE output o SET unspent_node_ids = bf.full_node_ids
      FROM (SELECT n.h, n.i FROM pg_temp.unspent_tracking_no_spender n
              WHERE NOT EXISTS (SELECT 1 FROM unspent_tracking_backfill_transactions m WHERE m.hash = n.h)
                AND NOT EXISTS (SELECT 1 FROM pg_temp.unspent_tracking_dynamic d WHERE d.hash = n.h)
              ORDER BY n.h, n.i OFFSET 0) v
      WHERE o.transaction_hash = v.h AND o.output_index = v.i
        AND o.unspent_node_ids IS DISTINCT FROM bf.full_node_ids;
    GET DIAGNOSTICS n_full = ROW_COUNT;
    -- stored with another value at the preparation, spent and outside X: '{}'
    UPDATE output o SET unspent_node_ids = '{}'
      FROM (SELECT r.transaction_hash AS h, r.output_index AS i FROM unspent_tracking_backfill_rows r
              WHERE r.transaction_hash >= lo AND r.transaction_hash < hi
                AND NOT EXISTS (SELECT 1 FROM pg_temp.unspent_tracking_affected a
                                  WHERE a.h = r.transaction_hash AND a.i = r.output_index)
                AND NOT EXISTS (SELECT 1 FROM pg_temp.unspent_tracking_no_spender n
                                  WHERE n.h = r.transaction_hash AND n.i = r.output_index)
              ORDER BY r.transaction_hash, r.output_index OFFSET 0) v
      WHERE o.transaction_hash = v.h AND o.output_index = v.i
        AND o.unspent_node_ids IS DISTINCT FROM '{}'::integer[];
    GET DIAGNOSTICS n_reset = ROW_COUNT;
    ms_write := round(extract(epoch FROM clock_timestamp() - clock) * 1000);
  ELSE
    p2 := range_end;
  END IF;
  UPDATE unspent_tracking_progress
    SET backfill_prefix = p2,
        backfill_transaction_internal_id = CASE WHEN p2 >= range_end THEN start_tx ELSE backfill_transaction_internal_id END,
        updated_at = now()
    WHERE partition_index = part;
  RETURN jsonb_build_object('previousPrefix', p1, 'prefix', p2, 'rangeEnd', range_end,
    'dynamic', n_dynamic, 'exact', n_exact, 'noSpender', n_no_spender, 'full', n_full, 'reset', n_reset,
    'changed', n_changed + n_full + n_reset, 'done', p2 >= range_end,
    'msSelect', ms_select, 'msExact', ms_exact, 'msWrite', ms_write);
END;
$$;
COMMENT ON FUNCTION unspent_tracking_backfill_batch (integer, integer, integer) IS 'One backfill batch of one partition (a range of two-byte transaction hash prefixes). Call in a REPEATABLE READ transaction after unspent_tracking_backfill_prepare; see migration 1791500000000_unspent_node_ids.';

-- Recompute every output of one scope (a token category and/or a 25-byte
-- locking bytecode prefix) from scratch. For targeted fills (tests, measurements); a
-- full backfill uses unspent_tracking_backfill_prepare / _batch. It does not
-- advance the backfill cursors: the query root uses pre-tracking values only
-- once the full backfill is complete.
CREATE FUNCTION unspent_tracking_backfill_scope (category bytea, bytecode_prefix bytea)
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET jit = off AS $$
DECLARE
  changed bigint;
BEGIN
  PERFORM unspent_tracking_temp_tables();
  DELETE FROM unspent_tracking_affected;
  INSERT INTO unspent_tracking_affected (h, i)
    SELECT o.transaction_hash, o.output_index FROM output o
      WHERE (category IS NULL OR o.token_category = category)
        AND (bytecode_prefix IS NULL OR substring(o.locking_bytecode, 0, 26) = bytecode_prefix);
  changed := unspent_tracking_recompute();
  RETURN changed;
END;
$$;

-- Start tracking (no-op if already started) with `part_count` partitions; the
-- watermarks start at the given settled sequence values. Outputs of earlier
-- transactions (pre-tracking) keep whatever they store ('{}' or NULL) and are
-- used only once the backfill is complete; from genesis (0, 0) there is
-- nothing to backfill. The partitions do not consume the events up
-- to `event_watermark` (settled): they predate tracking, and every output they
-- concern is either pre-tracking (answered live, then backfilled from the
-- current state) or processed later from the current state. The watch
-- pass does read them: a transaction released before tracking started (from
-- when the agent enabled the release-event triggers) is watched like any
-- other, so a later re-acceptance through a path that writes no event still
-- reaches outputs processed after the start (e.g. the output of a parent saved
-- after tracking started, spent by a released child saved before).
-- After a stop (rebuild_required), values written before the stop may be
-- stale: rebuild_through is set to the highest transaction id at the restart,
-- and reads use the live per-node test until the job has passed it and the
-- backfill is complete (the backfill takes every row whose value is not '{}',
-- so stale values too).
CREATE FUNCTION unspent_tracking_initialize (part_count integer, input_watermark bigint, block_watermark bigint,
  event_watermark bigint DEFAULT 0)
  RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  started bigint;
BEGIN
  SELECT tracking_start_transaction_internal_id INTO started FROM unspent_tracking_settings FOR UPDATE;
  IF started IS NOT NULL THEN
    RETURN false;
  END IF;
  UPDATE unspent_tracking_settings
    SET tracking_start_transaction_internal_id = input_watermark,
        tracking_start_block_internal_id = block_watermark,
        backfill_complete = (input_watermark = 0),
        rebuild_through_transaction_internal_id = CASE WHEN rebuild_required
          THEN GREATEST(input_watermark, (SELECT COALESCE(max(internal_id), 0) FROM transaction)) END,
        watch_consumed_event_id = 0;
  DELETE FROM unspent_tracking_backfill;
  TRUNCATE unspent_tracking_backfill_nodes, unspent_tracking_backfill_transactions,
    unspent_tracking_backfill_spends, unspent_tracking_backfill_rows;
  INSERT INTO unspent_tracking_progress (partition_index, partition_count, input_transaction_internal_id,
      block_internal_id, consumed_event_id, backfill_transaction_internal_id)
    SELECT p, part_count, input_watermark, block_watermark, event_watermark, 0 FROM generate_series(0, part_count - 1) p;
  -- child-before-parent across the start of tracking: recent inputs whose
  -- output is not saved yet (bounded: the last 100,000 transactions)
  INSERT INTO unspent_tracking_skipped (outpoint_transaction_hash, outpoint_index, transaction_internal_id)
    SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
      FROM input i
      WHERE i.transaction_internal_id > input_watermark - 100000 AND i.transaction_internal_id <= input_watermark
        AND i.outpoint_transaction_hash <> '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
        AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index)
    ON CONFLICT ON CONSTRAINT unspent_tracking_skipped_pkey DO NOTHING;
  RETURN true;
END;
$$;

-- Change the number of partitions, preserving progress: a partition is a
-- range of first hash bytes (((byte * count) >> 8) = partition), so each new
-- partition starts at the minimum watermarks (input, block, consumed event,
-- backfill cursor) of the old partitions whose byte ranges overlap its own:
-- every row in its range was processed by one of them up to at least that
-- point (re-processing is idempotent). A new partition with no byte of its
-- own (more than 256 partitions), or old rows that are not one consistent
-- partitioning, start at the global minimum. The global minimum is
-- unchanged, so the query root and event deletion are unaffected.
-- The backfill's prefix cursor (backfill_prefix) is carried over the same
-- way: a new partition starts at the lowest effective cursor of the
-- overlapping old partitions whose backfill is not complete (an old
-- partition's prefixes from its range start up to its cursor are backfilled
-- under the current preparation, which is global, so they stay backfilled
-- whichever partition owns them; complete old partitions cover their whole
-- range). Every prefix of the new range below that cursor is in an old
-- partition that is either complete or past it (the cursor is clamped to the
-- new range). If every overlapping old partition is complete the cursor is
-- the range end (and the partition is complete); without a consistent old
-- partitioning it is 0.
-- Called before every pass: when the count is unchanged it returns without
-- taking the table lock. The EXCLUSIVE lock conflicts with the row lock every
-- batch holds on its progress row, so a queued request would wait for the
-- batches in flight (another agent's job, or a long catch-up batch) and make
-- every later batch queue behind it. A real re-partition waits at most
-- lock_timeout (55P03); the caller retries on its next pass.
CREATE FUNCTION unspent_tracking_repartition (part_count integer)
  RETURNS boolean LANGUAGE plpgsql SET lock_timeout = '2s' AS $$
DECLARE
  current_count integer;
  consistent boolean;
  w bigint;
  wb bigint;
  consumed bigint;
  backfilled bigint;
  start_tx bigint;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM unspent_tracking_progress)
     OR ((SELECT count(*) FROM unspent_tracking_progress) = part_count
         AND NOT EXISTS (SELECT 1 FROM unspent_tracking_progress p WHERE p.partition_count <> part_count)) THEN
    RETURN false;
  END IF;
  LOCK TABLE unspent_tracking_progress IN EXCLUSIVE MODE;
  SELECT tracking_start_transaction_internal_id INTO start_tx FROM unspent_tracking_settings;
  SELECT min(partition_count), min(input_transaction_internal_id), min(block_internal_id), min(consumed_event_id),
         min(backfill_transaction_internal_id),
         min(partition_count) = max(partition_count) AND count(*) = min(partition_count)
           AND count(DISTINCT partition_index) = count(*)
           AND min(partition_index) = 0 AND max(partition_index) = min(partition_count) - 1
    INTO current_count, w, wb, consumed, backfilled, consistent FROM unspent_tracking_progress;
  IF current_count IS NULL OR (current_count = part_count AND
      (SELECT count(*) FROM unspent_tracking_progress) = part_count) THEN
    RETURN false;
  END IF;
  WITH old AS (
    DELETE FROM unspent_tracking_progress
      RETURNING partition_index, input_transaction_internal_id, block_internal_id, consumed_event_id,
                backfill_transaction_internal_id, backfill_prefix
  )
  INSERT INTO unspent_tracking_progress (partition_index, partition_count, input_transaction_internal_id,
      block_internal_id, consumed_event_id, backfill_transaction_internal_id, backfill_prefix)
    SELECT q, part_count, COALESCE(o.min_w, w), COALESCE(o.min_wb, wb), COALESCE(o.min_consumed, consumed),
           COALESCE(o.min_backfilled, backfilled),
           -- (GREATEST / LEAST ignore NULLs: test min_prefix itself)
           CASE WHEN o.min_prefix IS NOT NULL
                  THEN LEAST(GREATEST(o.min_prefix, ((q * 256 + part_count - 1) / part_count) * 256),
                             (((q + 1) * 256 + part_count - 1) / part_count) * 256)
                WHEN o.min_backfilled >= start_tx
                  THEN (((q + 1) * 256 + part_count - 1) / part_count) * 256
                ELSE 0 END
      FROM generate_series(0, part_count - 1) q
      LEFT JOIN LATERAL (
        SELECT min(old.input_transaction_internal_id) AS min_w, min(old.block_internal_id) AS min_wb,
               min(old.consumed_event_id) AS min_consumed, min(old.backfill_transaction_internal_id) AS min_backfilled,
               -- effective cursor of an incomplete old partition: its stored
               -- prefix within its own range (the batch's p1)
               min(LEAST(GREATEST(old.backfill_prefix,
                                  ((old.partition_index * 256 + current_count - 1) / current_count) * 256),
                         (((old.partition_index + 1) * 256 + current_count - 1) / current_count) * 256))
                 FILTER (WHERE old.backfill_transaction_internal_id < start_tx) AS min_prefix
          FROM generate_series(0, 255) b
          JOIN old ON old.partition_index = ((b * current_count) >> 8)
          WHERE consistent AND ((b * part_count) >> 8) = q
      ) o ON true;
  RETURN true;
END;
$$;

-- The watch set, once per pass (one caller at a time, also across agents),
-- before the partitions' batches; call in its own transaction with the
-- settled event limit. Cost is proportional to acceptance changes, not to the
-- chain: (1) release events above the watch's consumed id add the
-- transactions their node no longer accepts (a mempool row removed because a
-- block the node accepts confirmed the transaction is not a release: the node
-- still accepts it, and it is not watched); (2) every watched transaction is
-- re-checked, and a node that accepts it again (through a path that writes no
-- event) gets an "accepted" event, which the partitions consume like any
-- other; (3) entries older than 100 blocks expire.
CREATE FUNCTION unspent_tracking_watch_pass (event_limit bigint)
  RETURNS jsonb LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  consumed bigint;
  tip bigint;
  n_added bigint := 0;
  n_watched bigint := 0;
  n_reaccepted bigint := 0;
  n_expired bigint := 0;
BEGIN
  IF NOT pg_try_advisory_xact_lock(1970172788) THEN
    RETURN jsonb_build_object('busy', true);
  END IF;
  SELECT s.watch_consumed_event_id INTO consumed FROM unspent_tracking_settings s
    WHERE s.tracking_start_transaction_internal_id IS NOT NULL FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('uninitialized', true);
  END IF;
  SELECT max(height) INTO tip FROM block;
  -- 1. released (node, transaction) pairs the node no longer accepts
  INSERT INTO unspent_tracking_watch (transaction_internal_id, released_at_height, accepted_node_ids)
    SELECT r.tx, COALESCE(tip, 0), unspent_tracking_accepting(r.tx)
      FROM (SELECT DISTINCT x.tx FROM (
              SELECT e.node_internal_id AS node, e.transaction_internal_id AS tx FROM unspent_tracking_events e
                WHERE e.id > consumed AND e.id <= event_limit AND e.event_kind = 'released' AND e.transaction_internal_id IS NOT NULL
              UNION
              SELECT e.node_internal_id, bt.transaction_internal_id FROM unspent_tracking_events e
                CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                      WHERE block_transaction.block_internal_id = e.block_internal_id OFFSET 0) bt
                WHERE e.id > consumed AND e.id <= event_limit AND e.event_kind = 'released' AND e.block_internal_id IS NOT NULL) x
            WHERE NOT EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = x.tx AND nt.node_internal_id = x.node)
              AND NOT EXISTS (SELECT 1 FROM block_transaction bt
                                JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id AND nb.node_internal_id = x.node
                                WHERE bt.transaction_internal_id = x.tx)) r
    ON CONFLICT ON CONSTRAINT unspent_tracking_watch_pkey DO UPDATE
      SET released_at_height = EXCLUDED.released_at_height, accepted_node_ids = EXCLUDED.accepted_node_ids;
  GET DIAGNOSTICS n_added = ROW_COUNT;
  -- 2. re-acceptances: one "accepted" event per node that accepts a watched transaction again
  SELECT count(*) INTO n_watched FROM unspent_tracking_watch;
  IF n_watched > 0 THEN
    WITH current_acceptance AS (
      SELECT w.transaction_internal_id AS tx, w.accepted_node_ids AS previous_ids,
             unspent_tracking_accepting(w.transaction_internal_id) AS ids
        FROM unspent_tracking_watch w
    ), changed AS (
      UPDATE unspent_tracking_watch w SET accepted_node_ids = c.ids
        FROM current_acceptance c
        WHERE w.transaction_internal_id = c.tx AND c.ids <> c.previous_ids
        RETURNING c.tx, c.previous_ids, c.ids
    )
    INSERT INTO unspent_tracking_events (event_kind, node_internal_id, transaction_internal_id)
      SELECT 'accepted', g.node, c.tx FROM changed c CROSS JOIN LATERAL unnest(c.ids) g(node)
        WHERE NOT g.node = ANY (c.previous_ids);
    GET DIAGNOSTICS n_reaccepted = ROW_COUNT;
    -- 3. expiry
    DELETE FROM unspent_tracking_watch WHERE released_at_height < COALESCE(tip, 0) - 100;
    GET DIAGNOSTICS n_expired = ROW_COUNT;
  END IF;
  UPDATE unspent_tracking_settings SET watch_consumed_event_id = GREATEST(consumed, event_limit);
  RETURN jsonb_build_object('added', n_added, 'watched', n_watched, 'reaccepted', n_reaccepted, 'expired', n_expired);
END;
$$;

-- Stop tracking (the agent runs with CHAINGRAPH_UNSPENT_TRACKING=false on a
-- database where tracking had started; the release-event triggers are
-- disabled first, so acceptance changes are not recorded): drop the
-- watermarks, events, watch set and passed inputs, so the query root answers
-- with the live per-node test (read mode "live" without watermarks), and
-- mark the stored values as possibly stale (rebuild_required) for the next
-- start. Returns whether tracking was running.
CREATE FUNCTION unspent_tracking_stop ()
  RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  LOCK TABLE unspent_tracking_progress IN EXCLUSIVE MODE;
  PERFORM 1 FROM unspent_tracking_settings FOR UPDATE;
  IF NOT EXISTS (SELECT 1 FROM unspent_tracking_progress)
     AND (SELECT tracking_start_transaction_internal_id IS NULL FROM unspent_tracking_settings) THEN
    RETURN false;
  END IF;
  DELETE FROM unspent_tracking_progress;
  DELETE FROM unspent_tracking_events;
  DELETE FROM unspent_tracking_watch;
  DELETE FROM unspent_tracking_skipped;
  DELETE FROM unspent_tracking_backfill;
  TRUNCATE unspent_tracking_backfill_nodes, unspent_tracking_backfill_transactions,
    unspent_tracking_backfill_spends, unspent_tracking_backfill_rows;
  UPDATE unspent_tracking_settings
    SET tracking_start_transaction_internal_id = NULL,
        tracking_start_block_internal_id = NULL,
        backfill_complete = false,
        rebuild_required = true,
        rebuild_through_transaction_internal_id = NULL,
        watch_consumed_event_id = 0;
  RETURN true;
END;
$$;

-- Events consumed by every partition and by the watch pass (READ COMMITTED,
-- after a round), except those above a prepared backfill's event floor (its
-- batches re-check their transactions). Once the backfill is complete, its
-- state is emptied.
CREATE FUNCTION unspent_tracking_delete_consumed_events ()
  RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  deleted bigint;
BEGIN
  DELETE FROM unspent_tracking_events
    WHERE id <= LEAST((SELECT COALESCE(min(consumed_event_id), 0) FROM unspent_tracking_progress),
                      (SELECT watch_consumed_event_id FROM unspent_tracking_settings),
                      COALESCE((SELECT b.event_floor FROM unspent_tracking_backfill b), 9223372036854775807));
  GET DIAGNOSTICS deleted = ROW_COUNT;
  IF NOT (SELECT backfill_complete FROM unspent_tracking_settings)
     AND NOT unspent_tracking_backfill_pending() THEN
    UPDATE unspent_tracking_settings SET backfill_complete = true;
  END IF;
  IF EXISTS (SELECT 1 FROM unspent_tracking_backfill) AND NOT unspent_tracking_backfill_pending() THEN
    DELETE FROM unspent_tracking_backfill;
    TRUNCATE unspent_tracking_backfill_nodes, unspent_tracking_backfill_transactions,
      unspent_tracking_backfill_spends, unspent_tracking_backfill_rows;
  END IF;
  IF (SELECT rebuild_required FROM unspent_tracking_settings) AND NOT unspent_tracking_rebuild_pending() THEN
    UPDATE unspent_tracking_settings SET rebuild_required = false, rebuild_through_transaction_internal_id = NULL;
  END IF;
  RETURN deleted;
END;
$$;

-- Read side. All helpers read the watermarks in the caller's snapshot and use
-- the minimum over the partitions (every row created at or below it is
-- processed). The mode is chosen once per query:
--   'hash'     backlog <= read_hashed_max_transactions: the outpoints spent by
--              unprocessed / dirty transactions are hashed once;
--   'probe'    backlog <= read_probed_max_transactions: one spent_by_index probe per
--              stored row;
--   'live'     larger backlog, too many events, tracking not started (or
--              stopped), the backfill of pre-tracking outputs not complete,
--              or a rebuild after a stop in progress: the live
--              per-node test.
-- `SET chaingraph.unspent_read_mode = hash|probe|live` forces a mode
-- (tests and measurements).
CREATE FUNCTION unspent_tracking_read_mode ()
  RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
  forced text := current_setting('chaingraph.unspent_read_mode', true);
  s unspent_tracking_settings;
  w bigint;
  max_tx bigint;
  events bigint;
BEGIN
  SELECT * INTO s FROM unspent_tracking_settings;
  SELECT min(input_transaction_internal_id) INTO w FROM unspent_tracking_progress;
  IF w IS NULL OR s.tracking_start_transaction_internal_id IS NULL THEN
    RETURN 'live';
  END IF;
  -- after a stop, stored values may be stale until the rebuild is complete
  IF s.rebuild_required AND unspent_tracking_rebuild_pending() THEN
    RETURN 'live';
  END IF;
  -- pre-tracking values are used only once the backfill is complete (a '{}'
  -- read from the column default may be an output nobody has processed yet)
  IF unspent_tracking_backfill_pending() THEN
    RETURN 'live';
  END IF;
  IF forced IN ('hash', 'probe', 'live') THEN
    RETURN forced;
  END IF;
  SELECT count(*) INTO events FROM (SELECT 1 FROM unspent_tracking_events LIMIT s.read_stored_max_events + 1) e;
  IF events > s.read_stored_max_events THEN
    RETURN 'live';
  END IF;
  SELECT max(internal_id) INTO max_tx FROM transaction;
  IF COALESCE(max_tx, 0) - w > s.read_probed_max_transactions THEN
    RETURN 'live';
  END IF;
  IF COALESCE(max_tx, 0) - w > s.read_hashed_max_transactions THEN
    RETURN 'probe';
  END IF;
  RETURN 'hash';
END;
$$;

-- Whether pre-tracking values may still be unprocessed (the column default
-- '{}', NULL, or stale): some partition's backfill has not covered its whole
-- range (or tracking has not started). Derived from the cursors, which only
-- the backfill advances after writing every output behind them, never from a
-- settable flag: while it is true the read mode is "live" (the live per-node
-- test), so an unprocessed value is never read.
CREATE FUNCTION unspent_tracking_backfill_pending ()
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT NOT COALESCE((SELECT min(p.backfill_transaction_internal_id) FROM unspent_tracking_progress p)
                      >= (SELECT s.tracking_start_transaction_internal_id FROM unspent_tracking_settings s), false)
$$;

-- Whether a rebuild after a stop is still in progress: the backfill (which
-- then recomputes every pre-tracking output) is incomplete, or the job has
-- not passed rebuild_through_transaction_internal_id.
CREATE FUNCTION unspent_tracking_rebuild_pending ()
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT s.rebuild_required
     AND (unspent_tracking_backfill_pending()
          OR NOT COALESCE((SELECT min(p.input_transaction_internal_id) FROM unspent_tracking_progress p)
                            >= s.rebuild_through_transaction_internal_id, false))
    FROM unspent_tracking_settings s
$$;

-- Whether the query root serves `node_name` from the stored set: the read mode
-- is not "live", the node's per-node indexes are valid
-- (unspent_tracking_nodes) and every partition has consumed the events
-- recorded before they were. Otherwise (a node registered after tracking
-- started, before its indexes exist; an unknown name) the root answers with
-- the live per-node test only.
CREATE FUNCTION unspent_tracking_serves_node (node_name text)
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT unspent_tracking_read_mode() <> 'live'
     AND EXISTS (SELECT 1 FROM node n JOIN unspent_tracking_nodes r ON r.node_internal_id = n.internal_id
                   WHERE n.name = node_name
                     AND r.ready_after_event_id
                         <= (SELECT COALESCE(min(p.consumed_event_id), -1) FROM unspent_tracking_progress p))
$$;

-- Transactions whose acceptance may have changed after a partition's snapshot:
-- those of events not yet deleted and of blocks above the minimum block
-- watermark. (These helpers force custom plans: a generic plan for
-- `> watermark` estimates a third of the table and scans it.)
CREATE FUNCTION unspent_tracking_dirty_transactions ()
  RETURNS TABLE (id bigint) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off SET plan_cache_mode = force_custom_plan AS $$
DECLARE
  wb bigint;
BEGIN
  SELECT min(block_internal_id) INTO wb FROM unspent_tracking_progress;
  IF wb IS NULL THEN
    RETURN;
  END IF;
  RETURN QUERY
    SELECT e.transaction_internal_id FROM unspent_tracking_events e WHERE e.transaction_internal_id IS NOT NULL
    UNION
    SELECT bt.transaction_internal_id
      FROM (SELECT e.block_internal_id AS block_id FROM unspent_tracking_events e WHERE e.block_internal_id IS NOT NULL
            UNION
            SELECT b.internal_id FROM block b WHERE b.internal_id > wb) blocks
      CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                            WHERE block_transaction.block_internal_id = blocks.block_id OFFSET 0) bt;
END;
$$;

-- Outpoints spent by inputs above the minimum input watermark or by dirty
-- transactions (the stored value of these outputs may be stale).
CREATE FUNCTION unspent_tracking_recent_spends ()
  RETURNS TABLE (outpoint_transaction_hash bytea, outpoint_index bigint) LANGUAGE plpgsql STABLE ROWS 1000
  SET enable_hashjoin = off SET enable_mergejoin = off SET plan_cache_mode = force_custom_plan AS $$
DECLARE
  w bigint;
BEGIN
  SELECT min(input_transaction_internal_id) INTO w FROM unspent_tracking_progress;
  IF w IS NULL THEN
    RETURN;
  END IF;
  RETURN QUERY
    SELECT i.outpoint_transaction_hash, i.outpoint_index FROM input i WHERE i.transaction_internal_id > w
    UNION ALL
    SELECT i.outpoint_transaction_hash, i.outpoint_index
      FROM unspent_tracking_dirty_transactions() d
      CROSS JOIN LATERAL (SELECT input.outpoint_transaction_hash, input.outpoint_index FROM input
                            WHERE input.transaction_internal_id = d.id OFFSET 0) i
      WHERE d.id <= w;
END;
$$;

-- Hashes of dirty transactions (their outputs' creator acceptance may have
-- changed; stored rows they created are re-checked).
CREATE FUNCTION unspent_tracking_dirty_creators ()
  RETURNS TABLE (hash bytea) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off SET plan_cache_mode = force_custom_plan AS $$
BEGIN
  RETURN QUERY
    SELECT DISTINCT t.hash FROM unspent_tracking_dirty_transactions() d
      CROSS JOIN LATERAL (SELECT transaction.hash FROM transaction WHERE transaction.internal_id = d.id OFFSET 0) t;
END;
$$;

-- Hashes of transactions created above the minimum input watermark (their
-- outputs may be unprocessed, i.e. NULL).
CREATE FUNCTION unspent_tracking_backlog_transactions ()
  RETURNS TABLE (hash bytea) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off SET plan_cache_mode = force_custom_plan AS $$
DECLARE
  w bigint;
BEGIN
  SELECT min(input_transaction_internal_id) INTO w FROM unspent_tracking_progress;
  IF w IS NULL THEN
    RETURN;
  END IF;
  RETURN QUERY SELECT t.hash FROM transaction t WHERE t.internal_id > w;
END;
$$;

-- Outpoints spent by transactions of unconsumed release events that the node
-- no longer accepts (stored as spent for the node, possibly unspent now).
CREATE FUNCTION unspent_tracking_released_spends (node_id bigint)
  RETURNS TABLE (outpoint_transaction_hash bytea, outpoint_index bigint) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off SET plan_cache_mode = force_custom_plan AS $$
BEGIN
  RETURN QUERY
    SELECT DISTINCT i.outpoint_transaction_hash, i.outpoint_index
      FROM (SELECT e.transaction_internal_id AS id FROM unspent_tracking_events e
              WHERE e.event_kind = 'released' AND e.transaction_internal_id IS NOT NULL AND e.node_internal_id = node_id
            UNION
            SELECT bt.transaction_internal_id
              FROM (SELECT DISTINCT e.block_internal_id FROM unspent_tracking_events e
                      WHERE e.event_kind = 'released' AND e.block_internal_id IS NOT NULL AND e.node_internal_id = node_id) b
              CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                    WHERE block_transaction.block_internal_id = b.block_internal_id OFFSET 0) bt) r
      CROSS JOIN LATERAL (SELECT input.outpoint_transaction_hash, input.outpoint_index FROM input
                            WHERE input.transaction_internal_id = r.id OFFSET 0) i
      WHERE NOT (EXISTS (SELECT 1 FROM block_transaction bt
                           JOIN node_block nb ON nb.node_internal_id = node_id AND nb.block_internal_id = bt.block_internal_id
                           WHERE bt.transaction_internal_id = r.id)
                 OR EXISTS (SELECT 1 FROM node_transaction nt
                              WHERE nt.transaction_internal_id = r.id AND nt.node_internal_id = node_id));
END;
$$;

-- Hashes of transactions the node may have started accepting after their
-- outputs were processed (stored without the node, possibly unspent for it
-- now): those of the node's unconsumed "accepted" events, and those of blocks
-- above the minimum block watermark the node accepts, limited to transactions
-- some partition may have processed (at or below the highest input
-- watermark; outputs of later transactions are unprocessed and handled by the
-- backlog arm) and leaving out transactions the node already accepted in its
-- mempool (an unconsumed node_transaction release event for the node: the
-- mempool row was removed when the block confirmed it). Only transactions the
-- node accepts now.
CREATE FUNCTION unspent_tracking_gained_creators (node_id bigint)
  RETURNS TABLE (hash bytea) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off SET plan_cache_mode = force_custom_plan AS $$
DECLARE
  wb bigint;
  w_max bigint;
BEGIN
  SELECT min(block_internal_id), max(input_transaction_internal_id) INTO wb, w_max FROM unspent_tracking_progress;
  IF wb IS NULL THEN
    RETURN;
  END IF;
  RETURN QUERY
    SELECT DISTINCT t.hash
      FROM (SELECT e.transaction_internal_id AS id FROM unspent_tracking_events e
              WHERE e.event_kind = 'accepted' AND e.node_internal_id = node_id AND e.transaction_internal_id IS NOT NULL
            UNION
            SELECT bt.transaction_internal_id
              FROM (SELECT DISTINCT e.block_internal_id FROM unspent_tracking_events e
                      WHERE e.event_kind = 'accepted' AND e.node_internal_id = node_id AND e.block_internal_id IS NOT NULL) b
              CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                    WHERE block_transaction.block_internal_id = b.block_internal_id OFFSET 0) bt
            UNION
            SELECT bt.transaction_internal_id
              FROM (SELECT b.internal_id FROM block b WHERE b.internal_id > wb) nbk
              JOIN node_block nb ON nb.block_internal_id = nbk.internal_id AND nb.node_internal_id = node_id
              CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                    WHERE block_transaction.block_internal_id = nbk.internal_id
                                      AND block_transaction.transaction_internal_id <= w_max OFFSET 0) bt
              WHERE NOT EXISTS (SELECT 1 FROM unspent_tracking_events e
                                  WHERE e.event_kind = 'released' AND e.node_internal_id = node_id
                                    AND e.transaction_internal_id = bt.transaction_internal_id)) x
      CROSS JOIN LATERAL (SELECT transaction.hash FROM transaction WHERE transaction.internal_id = x.id OFFSET 0) t
      WHERE EXISTS (SELECT 1 FROM block_transaction bt
                      JOIN node_block nb ON nb.node_internal_id = node_id AND nb.block_internal_id = bt.block_internal_id
                      WHERE bt.transaction_internal_id = x.id)
         OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = x.id AND nt.node_internal_id = node_id);
END;
$$;

-- (Re)create unspent_output(node_name) for the registered nodes. The
-- agent calls it whenever tracking is configured (once all nodes are
-- registered) and when it creates the per-node indexes. Serialised by a
-- transaction-scoped advisory lock (idempotent: the last build wins and every
-- build reads the current node table). Per node (the node id a literal, so `<id> = ANY (unspent_node_ids)`
-- matches that node's partial indexes; the other nodes' arms are removed at
-- plan time by `node_name = '<name>'`):
--   (a1) stored token outputs with a locking bytecode up to 1,000 bytes:
--        index-only through the covering per-node category index;
--   (a2) every other stored output (no token, or a longer locking bytecode):
--        the per-node "other" index, keyed on the category too, so a category
--        filter is an index condition there (no token: NULL keys, deduplicated);
--   both also reachable through the per-node 25-byte prefix index; each row
--   re-checked live when spent above the watermark or by a dirty
--   transaction, or created by a dirty transaction. The two arm predicates
--   are complementary and spelled exactly as the index predicates, so the
--   planner matches them.
-- Shared arms (node resolved once):
--   (b1) unprocessed outputs of transactions above the minimum input watermark;
--   (d1) outputs spent by released transactions the node no longer accepts;
--   (d2) outputs of transactions the node started accepting;
--   (f)  the live per-node test, instead of every other arm, when the read
--        mode is "live" or the node is not served from the stored set yet
--        (unspent_tracking_serves_node: its per-node indexes are not valid,
--        or the events recorded before they were are not consumed, or the
--        name is unknown). Every arm is gated by that one per-query value,
--        so the arms never overlap.
CREATE FUNCTION unspent_tracking_build_root ()
  RETURNS void LANGUAGE plpgsql AS $build$
DECLARE
  arms text := '';
  node record;
  live_test_literal text;
  live_test constant text := $f$CASE
      WHEN EXISTS (
        SELECT 1 FROM input i
        WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
          AND (EXISTS (SELECT 1 FROM block_transaction bt
                       JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = i.transaction_internal_id
                         AND nb.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))
            OR EXISTS (SELECT 1 FROM node_transaction nt
                       WHERE nt.transaction_internal_id = i.transaction_internal_id
                         AND nt.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))))
      THEN false
      ELSE EXISTS (
        SELECT 1 FROM transaction t
        WHERE t.hash = o.transaction_hash
          AND (EXISTS (SELECT 1 FROM block_transaction bt
                       JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = t.internal_id
                         AND nb.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))
            OR EXISTS (SELECT 1 FROM node_transaction nt
                       WHERE nt.transaction_internal_id = t.internal_id
                         AND nt.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))))
    END$f$;
  stored_arm constant text := $t$  SELECT o.* FROM output o
  WHERE node_name = %1$L AND %2$s = ANY (o.unspent_node_ids) AND %3$s
    AND (SELECT unspent_tracking_serves_node(node_name))
    AND CASE
      WHEN CASE WHEN (SELECT unspent_tracking_read_mode()) = 'hash'
             THEN (o.transaction_hash, o.output_index) IN (
                    SELECT r.outpoint_transaction_hash, r.outpoint_index FROM unspent_tracking_recent_spends() r)
             ELSE EXISTS (
                    SELECT 1 FROM input i
                    WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
                      AND (i.transaction_internal_id > (SELECT min(p.input_transaction_internal_id) FROM unspent_tracking_progress p)
                        OR i.transaction_internal_id IN (SELECT d.id FROM unspent_tracking_dirty_transactions() d)))
           END
        OR o.transaction_hash IN (SELECT c.hash FROM unspent_tracking_dirty_creators() c)
      THEN %4$s
      ELSE true
    END
  UNION ALL
$t$;
BEGIN
  -- one build at a time (two agents, or one agent configuring per node, or the
  -- migration): concurrent CREATE OR REPLACE of the same function fails with
  -- "tuple concurrently updated"; the lock is held until the caller commits
  PERFORM pg_advisory_xact_lock(1970172787);
  -- stored arms only for nodes whose per-node indexes are valid (the others
  -- are answered by the live per-node test; unspent_tracking_serves_node gates every arm at run time)
  FOR node IN SELECT n.internal_id, n.name FROM node n
                JOIN unspent_tracking_nodes r ON r.node_internal_id = n.internal_id
                ORDER BY n.internal_id LOOP
    live_test_literal := replace(live_test, '(SELECT n.internal_id FROM node n WHERE n.name = node_name)', node.internal_id::text);
    arms := arms
      || format(stored_arm, node.name, node.internal_id,
           'o.token_category IS NOT NULL AND octet_length(o.locking_bytecode) <= 1000', live_test_literal)
      || format(stored_arm, node.name, node.internal_id,
           '(o.token_category IS NULL OR octet_length(o.locking_bytecode) > 1000)', live_test_literal);
  END LOOP;
  EXECUTE 'CREATE OR REPLACE FUNCTION unspent_output (node_name text) RETURNS SETOF output LANGUAGE sql STABLE AS $root$'
    || chr(10) || arms
    || format($shared$  SELECT o.* FROM unspent_tracking_backlog_transactions() b
  CROSS JOIN LATERAL (
    SELECT * FROM output WHERE output.transaction_hash = b.hash OFFSET 0) o
  WHERE o.unspent_node_ids IS NULL
    AND (SELECT unspent_tracking_serves_node(node_name))
    AND %1$s
  UNION ALL
  SELECT o.* FROM unspent_tracking_released_spends((SELECT n.internal_id FROM node n WHERE n.name = node_name)) r
  CROSS JOIN LATERAL (
    SELECT * FROM output
      WHERE output.transaction_hash = r.outpoint_transaction_hash AND output.output_index = r.outpoint_index OFFSET 0) o
  WHERE NOT ((SELECT n.internal_id FROM node n WHERE n.name = node_name)::integer = ANY (o.unspent_node_ids))
    AND (SELECT unspent_tracking_serves_node(node_name))
    AND %1$s
  UNION ALL
  SELECT o.* FROM unspent_tracking_gained_creators((SELECT n.internal_id FROM node n WHERE n.name = node_name)) c
  CROSS JOIN LATERAL (
    SELECT * FROM output WHERE output.transaction_hash = c.hash OFFSET 0) o
  WHERE NOT ((SELECT n.internal_id FROM node n WHERE n.name = node_name)::integer = ANY (o.unspent_node_ids))
    AND NOT ((o.transaction_hash, o.output_index) IN (
      SELECT r.outpoint_transaction_hash, r.outpoint_index
        FROM unspent_tracking_released_spends((SELECT n.internal_id FROM node n WHERE n.name = node_name)) r))
    AND (SELECT unspent_tracking_serves_node(node_name))
    AND %1$s
  UNION ALL
  SELECT o.* FROM output o
  WHERE NOT (SELECT unspent_tracking_serves_node(node_name))
    AND %1$s
$shared$, live_test) || '$root$';
  EXECUTE $c$COMMENT ON FUNCTION unspent_output (text) IS 'The outputs unspent according to the named node: created by a transaction the node accepts (in a block it accepts or in its mempool) and not spent by any transaction the node accepts. Served from a per-node set (output.unspent_node_ids) with a live correction for anything the tracking job has not processed yet. Filter by token_category (and nonfungible_token_capability) or use a 25-byte locking bytecode prefix; a call filtered only by locking_bytecode scans every output.'$c$;
END;
$build$;
SELECT unspent_tracking_build_root();
