-- Stored per-node unspent set: output.unspent_node_ids, maintained off the
-- ingestion path by a job inside the agent (CHAINGRAPH_UNSPENT_NODE_IDS=true).
--
-- output.unspent_node_ids:
--   NULL   = not processed by the job yet;
--   '{}'   = processed, in no node's UTXO set;
--   {1,3}  = unspent for nodes 1 and 3 (sorted node internal ids): created by
--            a transaction the node accepts (in a block it accepts or in its
--            mempool) and not spent by any transaction the node accepts.
-- Every fact is per node: the value is the same F1g test that
-- unspent_output(node) evaluates live, stored for every node at once.
--
-- Ingestion stays append-only. Outputs are inserted with NULL; nothing is
-- added to `input`. The only write-path triggers are two statement-level
-- AFTER DELETE triggers on node_block / node_transaction appending "released"
-- events (created disabled; the agent enables them when tracking is on). The
-- agent appends an "accepted" event only on its re-acceptance paths (a
-- node_block row for a block that already existed; a node_transaction row for
-- a transaction that already existed).
--
-- The job (unspent_node_ids_run_batch) runs in N partitions (outpoint hash
-- ranges: ((get_byte(hash, 0) * N) >> 8) = partition), each in its own
-- REPEATABLE READ transaction on its own connection, with its own watermarks
-- in unspent_tracking_progress. A batch of partition k:
--   - consumes events above its consumed id (events are deleted once every
--     partition has consumed them);
--   (once per pass, before the batches, unspent_node_ids_watch_pass re-checks
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
-- Reads: unspent_output_stored(node_name) (generated per node by
-- unspent_node_ids_build_root(); tracked in Hasura). The stored set, plus a
-- live F1g correction for whatever the job has not processed, so the result
-- is always exact; above a backlog threshold (unspent_tracking_settings) it
-- falls back to F1g.

ALTER TABLE output ADD COLUMN unspent_node_ids integer[];
COMMENT ON COLUMN output.unspent_node_ids IS 'Per-node unspent set (CHAINGRAPH_UNSPENT_NODE_IDS): NULL = not processed yet; otherwise the sorted internal ids of the nodes for which the output is unspent (created by a transaction the node accepts, no spender the node accepts). Maintained by the agent''s tracking job; read through unspent_output_stored(node_name).';

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
  updated_at timestamp NOT NULL DEFAULT now(),
  CONSTRAINT unspent_tracking_progress_pkey PRIMARY KEY (partition_index)
);
COMMENT ON TABLE unspent_tracking_progress IS 'Unspent tracking job watermarks, one row per partition (outpoint hash range): inputs of transactions up to input_transaction_internal_id, blocks up to block_internal_id and events up to consumed_event_id are processed for the partition''s outputs; pre-tracking outputs of transactions up to backfill_transaction_internal_id are backfilled. Not tracked by Hasura.';

CREATE TABLE unspent_tracking_settings (
  id boolean PRIMARY KEY DEFAULT true CHECK (id),
  tracking_start_transaction_internal_id bigint,
  tracking_start_block_internal_id bigint,
  backfill_complete boolean NOT NULL DEFAULT false,
  rebuild_required boolean NOT NULL DEFAULT false,
  rebuild_through_transaction_internal_id bigint,
  hash_max_transactions bigint NOT NULL DEFAULT 20000,
  fallback_transactions bigint NOT NULL DEFAULT 200000,
  fallback_events bigint NOT NULL DEFAULT 10000,
  watch_consumed_event_id bigint NOT NULL DEFAULT 0
);
INSERT INTO unspent_tracking_settings DEFAULT VALUES;
COMMENT ON TABLE unspent_tracking_settings IS 'Unspent tracking state and read thresholds (written by the agent from CHAINGRAPH_UNSPENT_NODE_IDS_* settings). tracking_start_*: outputs of earlier transactions are pre-tracking (NULL until backfilled); backfill_complete: every partition''s backfill cursor passed the tracking start, so no pre-tracking output is NULL (status only: reads derive it from the cursors, never from this flag); rebuild_required: tracking was stopped (stored values may be stale), so pre-tracking outputs are recomputed whatever their value, and reads use the live F1g predicate until that backfill is complete and the job has passed rebuild_through_transaction_internal_id (the highest transaction id when tracking restarted). Reads correct a backlog of up to hash_max_transactions unprocessed transactions with one hashed set, up to fallback_transactions with per-row probes, and fall back to the live F1g predicate above it (or above fallback_events unconsumed events). Not tracked by Hasura.';

CREATE TABLE unspent_tracking_watch (
  transaction_internal_id bigint NOT NULL,
  released_at_height bigint NOT NULL,
  accepted_node_ids integer[] NOT NULL,
  CONSTRAINT unspent_tracking_watch_pkey PRIMARY KEY (transaction_internal_id)
);
COMMENT ON TABLE unspent_tracking_watch IS 'Transactions some node stopped accepting recently (re-org, mempool drop or replacement; not a mempool row removed on confirmation), with the nodes accepting them when last checked; re-checked once per pass (unspent_node_ids_watch_pass) for 100 blocks after the release, so a re-acceptance through a path that writes no event still becomes an "accepted" event. Not tracked by Hasura.';

CREATE TABLE unspent_tracking_nodes (
  node_internal_id integer NOT NULL,
  ready_after_event_id bigint NOT NULL,
  indexed_at timestamp NOT NULL DEFAULT now(),
  CONSTRAINT unspent_tracking_nodes_pkey PRIMARY KEY (node_internal_id)
);
COMMENT ON TABLE unspent_tracking_nodes IS 'Nodes the query root may serve from the stored set: a row is written by the agent once the node''s three per-node partial indexes are valid (and removed while they are rebuilt); the node is served from the stored set once every partition has consumed the events up to ready_after_event_id (the acceptance changes recorded before its indexes existed). Other nodes are answered by the live F1g predicate. Not tracked by Hasura.';

CREATE TABLE unspent_tracking_skipped (
  outpoint_transaction_hash bytea NOT NULL,
  outpoint_index bigint NOT NULL,
  transaction_internal_id bigint NOT NULL,
  CONSTRAINT unspent_tracking_skipped_pkey PRIMARY KEY (outpoint_transaction_hash, outpoint_index, transaction_internal_id)
);
COMMENT ON TABLE unspent_tracking_skipped IS 'Inputs the tracking job passed while their output row did not exist (a stall over the limit, or child-before-parent across the start of tracking): the output is fully recomputed (never fast-pathed) when it arrives. Not tracked by Hasura.';

-- Release events: statement-level, one INSERT ... SELECT per DELETE, whichever
-- code path deletes the rows (re-org, mempool drop, replacement, cascade,
-- mempool cleaning on block acceptance).
CREATE FUNCTION trigger_unspent_tracking_node_block_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO unspent_tracking_events (event_kind, node_internal_id, block_internal_id)
    SELECT 'released', node_internal_id, block_internal_id FROM old_rows;
  RETURN NULL;
END;
$$;
CREATE FUNCTION trigger_unspent_tracking_node_transaction_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO unspent_tracking_events (event_kind, node_internal_id, transaction_internal_id)
    SELECT 'released', node_internal_id, transaction_internal_id FROM old_rows;
  RETURN NULL;
END;
$$;
CREATE TRIGGER trigger_unspent_tracking_node_block_delete AFTER DELETE ON node_block
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_tracking_node_block_delete();
CREATE TRIGGER trigger_unspent_tracking_node_transaction_delete AFTER DELETE ON node_transaction
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_tracking_node_transaction_delete();
ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_tracking_node_block_delete;
ALTER TABLE node_transaction DISABLE TRIGGER trigger_unspent_tracking_node_transaction_delete;

-- The nodes accepting a transaction now (sorted), in a block or in a mempool.
CREATE FUNCTION unspent_node_ids_accepting (transaction_internal_id bigint)
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

-- Recompute every output collected in pg_temp.unspent_node_ids_affected from
-- scratch: (nodes accepting the creator) EXCEPT (nodes accepting any spender),
-- sorted. Only rows whose value changes are written, in key order; every
-- collected output gets its value, '{}' included (NULL only ever means "not
-- processed yet": the query root never reads a NULL as spent). Returns the
-- number of rows changed.
CREATE FUNCTION unspent_node_ids_recompute ()
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
            FROM (SELECT DISTINCT h, i FROM pg_temp.unspent_node_ids_affected ORDER BY h, i) a
            CROSS JOIN LATERAL (
              SELECT internal_id FROM transaction WHERE transaction.hash = a.h OFFSET 0) ct
          OFFSET 0) v
    WHERE o.transaction_hash = v.h AND o.output_index = v.i
      AND o.unspent_node_ids IS DISTINCT FROM v.ids;
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed;
END;
$$;

CREATE FUNCTION unspent_node_ids_temp_tables ()
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS unspent_node_ids_affected (h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE INDEX IF NOT EXISTS unspent_node_ids_affected_key ON pg_temp.unspent_node_ids_affected (h, i);
  CREATE TEMP TABLE IF NOT EXISTS unspent_node_ids_fresh (h bytea NOT NULL, i bigint NOT NULL, creator bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_node_ids_txs (id bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_node_ids_inputs (tx bigint NOT NULL, h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_node_ids_consumed (id bigint NOT NULL, event_kind text NOT NULL, node bigint NOT NULL,
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
-- output row is missing no longer stop the batch (0 = never skip).
CREATE FUNCTION unspent_node_ids_run_batch (part integer, part_count integer, tx_limit bigint, block_limit bigint,
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
  PERFORM unspent_node_ids_temp_tables();

  -- 1. events above this partition's consumed id, up to the settled limit, in id order
  INSERT INTO unspent_node_ids_consumed
    SELECT e.id, e.event_kind, e.node_internal_id, e.block_internal_id, e.transaction_internal_id
      FROM unspent_tracking_events e WHERE e.id > consumed AND e.id <= event_limit ORDER BY e.id LIMIT max_events;
  GET DIAGNOSTICS n_events = ROW_COUNT;
  IF n_events > 0 THEN
    SELECT max(id) INTO max_event FROM unspent_node_ids_consumed;
    INSERT INTO unspent_node_ids_txs (id)
      SELECT c.tx_id FROM unspent_node_ids_consumed c WHERE c.tx_id IS NOT NULL
      UNION ALL
      SELECT bt.transaction_internal_id
        FROM (SELECT DISTINCT c.block_id FROM unspent_node_ids_consumed c WHERE c.block_id IS NOT NULL) b
        CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                              WHERE block_transaction.block_internal_id = b.block_id OFFSET 0) bt;
    GET DIAGNOSTICS n_event_txs = ROW_COUNT;
  END IF;
  -- (2. the watch set is re-checked once per pass by unspent_node_ids_watch_pass,
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
    INSERT INTO unspent_node_ids_inputs (tx, h, i)
      SELECT i.transaction_internal_id, i.outpoint_transaction_hash, i.outpoint_index
        FROM input i
        WHERE i.transaction_internal_id > w AND i.transaction_internal_id <= upper_tx
          AND i.outpoint_transaction_hash <> zero_hash
          AND ((get_byte(i.outpoint_transaction_hash, 0) * part_count) >> 8) = part;
    -- child-before-parent: stop before the first transaction with an input
    -- whose output row does not exist yet (retried next pass)
    SELECT min(c.tx) INTO stall_tx
      FROM unspent_node_ids_inputs c
      WHERE c.tx > skip_stall_through
        AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = c.h AND o.output_index = c.i);
    IF stall_tx IS NOT NULL THEN
      upper_tx := stall_tx - 1;
      DELETE FROM unspent_node_ids_inputs WHERE tx > upper_tx;
    END IF;
    IF skip_stall_through > w THEN
      INSERT INTO unspent_tracking_skipped (outpoint_transaction_hash, outpoint_index, transaction_internal_id)
        SELECT c.h, c.i, c.tx
          FROM unspent_node_ids_inputs c
          WHERE c.tx <= skip_stall_through
            AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = c.h AND o.output_index = c.i)
        ON CONFLICT ON CONSTRAINT unspent_tracking_skipped_pkey DO NOTHING;
      GET DIAGNOSTICS skipped_inputs = ROW_COUNT;
    END IF;
    SELECT count(*) INTO n_inputs FROM unspent_node_ids_inputs;
    INSERT INTO unspent_node_ids_affected (h, i) SELECT h, i FROM unspent_node_ids_inputs;
    IF upper_tx > w THEN
      -- outputs created by the range (this partition): those the job has not
      -- seen (NULL) take the fast path in step 6 unless something else touches them
      INSERT INTO unspent_node_ids_fresh (h, i, creator)
        SELECT o.transaction_hash, o.output_index, t.internal_id
          FROM transaction t
          CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_ids AS stored FROM output
                                WHERE output.transaction_hash = t.hash OFFSET 0) o
          WHERE t.internal_id > w AND t.internal_id <= upper_tx
            AND ((get_byte(t.hash, 0) * part_count) >> 8) = part AND o.stored IS NULL;
      INSERT INTO unspent_node_ids_affected (h, i)
        SELECT o.transaction_hash, o.output_index
          FROM transaction t
          CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_ids AS stored FROM output
                                WHERE output.transaction_hash = t.hash OFFSET 0) o
          WHERE t.internal_id > w AND t.internal_id <= upper_tx
            AND ((get_byte(t.hash, 0) * part_count) >> 8) = part AND o.stored IS NOT NULL;
      -- fresh outputs a passed (skipped) input spends: full recompute
      INSERT INTO unspent_node_ids_affected (h, i)
        SELECT f.h, f.i FROM unspent_node_ids_fresh f
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
      INSERT INTO unspent_node_ids_txs (id)
        SELECT bt.transaction_internal_id
          FROM (SELECT internal_id FROM block WHERE internal_id > wb AND internal_id <= upper_block) b
          CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                WHERE block_transaction.block_internal_id = b.internal_id
                                  AND block_transaction.transaction_internal_id <= w OFFSET 0) bt;
      GET DIAGNOSTICS n_block_txs = ROW_COUNT;
    END IF;
  END IF;

  -- 5. collected transactions -> the outputs they spend and create (this partition)
  INSERT INTO unspent_node_ids_affected (h, i)
    SELECT i.outpoint_transaction_hash, i.outpoint_index
      FROM (SELECT DISTINCT id FROM unspent_node_ids_txs) x
      CROSS JOIN LATERAL (SELECT outpoint_transaction_hash, outpoint_index FROM input
                            WHERE input.transaction_internal_id = x.id OFFSET 0) i
      WHERE i.outpoint_transaction_hash <> zero_hash
        AND ((get_byte(i.outpoint_transaction_hash, 0) * part_count) >> 8) = part;
  INSERT INTO unspent_node_ids_affected (h, i)
    SELECT o.transaction_hash, o.output_index
      FROM (SELECT DISTINCT id FROM unspent_node_ids_txs) x
      CROSS JOIN LATERAL (SELECT hash FROM transaction WHERE transaction.internal_id = x.id OFFSET 0) t
      CROSS JOIN LATERAL (SELECT transaction_hash, output_index FROM output
                            WHERE output.transaction_hash = t.hash OFFSET 0) o
      WHERE ((get_byte(t.hash, 0) * part_count) >> 8) = part;

  -- 6. recompute every collected output from scratch; then the fast path for
  -- outputs the job sees for the first time and nothing else touched (no
  -- processed input can spend them: the job stops before an input whose output
  -- is missing, and records the ones it passes in unspent_tracking_skipped):
  -- the creator's accepting nodes, without probing spent_by_index
  SELECT count(*) INTO n_affected FROM unspent_node_ids_affected;
  IF n_affected > 0 THEN
    n_changed := unspent_node_ids_recompute();
  END IF;
  UPDATE output o SET unspent_node_ids = v.ids
    FROM (SELECT f.h, f.i, unspent_node_ids_accepting(f.creator) AS ids
            FROM unspent_node_ids_fresh f
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.unspent_node_ids_affected a WHERE a.h = f.h AND a.i = f.i)
            ORDER BY f.h, f.i OFFSET 0) v
    WHERE o.transaction_hash = v.h AND o.output_index = v.i
      AND o.unspent_node_ids IS DISTINCT FROM v.ids;
  GET DIAGNOSTICS n_fresh = ROW_COUNT;
  n_affected := n_affected + n_fresh;
  n_changed := n_changed + n_fresh;
  -- passed inputs whose output has now been processed are no longer needed
  DELETE FROM unspent_tracking_skipped s USING unspent_node_ids_fresh f
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
COMMENT ON FUNCTION unspent_node_ids_run_batch (integer, integer, bigint, bigint, bigint, integer, integer, integer, bigint) IS 'One batch of the unspent tracking job for one partition (outpoint hash range). Call in a REPEATABLE READ transaction with settled limits; see migration 1791500000000_unspent_node_ids.';

-- Backfill of pre-tracking outputs (created by transactions at or below the
-- tracking start) for one partition: the next `max_transactions` transactions
-- above the partition's backfill cursor; every NULL output of this partition
-- among them (every output, after a stop: rebuild_required) is fully
-- recomputed and written, '{}' included, so no pre-tracking output is left
-- NULL behind the cursor (an output whose spenders exist but are accepted by
-- no node is unspent for the nodes accepting its creator). Call in a
-- REPEATABLE READ transaction. The backfill is complete once every
-- partition's cursor has passed the tracking start
-- (unspent_node_ids_backfill_pending).
CREATE FUNCTION unspent_node_ids_backfill_batch (part integer, part_count integer, max_transactions integer)
  RETURNS jsonb LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  cursor_tx bigint;
  upper_tx bigint;
  start_tx bigint;
  rebuild boolean;
  stored_count integer;
  n_affected bigint := 0;
  n_changed bigint := 0;
BEGIN
  IF NOT pg_try_advisory_xact_lock(1970172786, part) THEN
    RETURN jsonb_build_object('busy', true);
  END IF;
  SELECT tracking_start_transaction_internal_id, rebuild_required INTO start_tx, rebuild FROM unspent_tracking_settings;
  SELECT p.backfill_transaction_internal_id, p.partition_count INTO cursor_tx, stored_count
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
  upper_tx := LEAST(cursor_tx + GREATEST(max_transactions, 1), start_tx);
  PERFORM unspent_node_ids_temp_tables();
  INSERT INTO unspent_node_ids_affected (h, i)
    SELECT o.transaction_hash, o.output_index
      FROM transaction t
      CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_ids AS stored FROM output
                            WHERE output.transaction_hash = t.hash OFFSET 0) o
      WHERE t.internal_id > cursor_tx AND t.internal_id <= upper_tx
        AND ((get_byte(t.hash, 0) * part_count) >> 8) = part AND (o.stored IS NULL OR rebuild);
  GET DIAGNOSTICS n_affected = ROW_COUNT;
  IF n_affected > 0 THEN
    n_changed := unspent_node_ids_recompute();
  END IF;
  UPDATE unspent_tracking_progress SET backfill_transaction_internal_id = upper_tx, updated_at = now()
    WHERE partition_index = part;
  RETURN jsonb_build_object('previousBackfillWatermark', cursor_tx, 'backfillWatermark', upper_tx,
    'affected', n_affected, 'changed', n_changed, 'done', upper_tx >= start_tx);
END;
$$;

-- Backfill of one scope (a token category and/or a 25-byte locking bytecode
-- prefix): every NULL output of the scope, recomputed and written ('{}'
-- included). For targeted backfills (tests, lab); a full backfill uses
-- unspent_node_ids_backfill_batch. It does not advance the backfill cursors:
-- the query root keeps answering the remaining NULL outputs live (it never
-- reads a NULL as spent), so a scoped backfill cannot hide an unspent output.
CREATE FUNCTION unspent_node_ids_backfill_scope (category bytea, bytecode_prefix bytea)
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET jit = off AS $$
BEGIN
  PERFORM unspent_node_ids_temp_tables();
  DELETE FROM unspent_node_ids_affected;
  INSERT INTO unspent_node_ids_affected (h, i)
    SELECT o.transaction_hash, o.output_index FROM output o
      WHERE o.unspent_node_ids IS NULL
        AND (category IS NULL OR o.token_category = category)
        AND (bytecode_prefix IS NULL OR substring(o.locking_bytecode, 0, 26) = bytecode_prefix);
  RETURN unspent_node_ids_recompute();
END;
$$;

-- Start tracking (no-op if already started) with `part_count` partitions; the
-- watermarks start at the given settled sequence values. Outputs of earlier
-- transactions stay NULL (pre-tracking) until backfilled; from genesis (0, 0)
-- there is nothing to backfill. The partitions do not consume the events up
-- to `event_watermark` (settled): they predate tracking, and every output they
-- concern is either unprocessed (NULL: answered live, then backfilled with
-- the current state) or processed later from the current state. The watch
-- pass does read them: a transaction released before tracking started (from
-- when the agent enabled the release-event triggers) is watched like any
-- other, so a later re-acceptance through a path that writes no event still
-- reaches outputs processed after the start (e.g. the output of a parent saved
-- after tracking started, spent by a released child saved before).
-- there is nothing to backfill. After a stop (rebuild_required), values
-- written before the stop may be stale: rebuild_through is set to the highest
-- transaction id now, and reads use F1g until the job has passed it and the
-- backfill (which then recomputes every pre-tracking output) is complete.
CREATE FUNCTION unspent_node_ids_initialize (part_count integer, input_watermark bigint, block_watermark bigint,
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

-- Change the number of partitions: every new partition restarts from the
-- minimum of the old watermarks (re-processing is idempotent). Events above
-- the minimum consumed id are kept (deletion uses the minimum).
CREATE FUNCTION unspent_node_ids_repartition (part_count integer)
  RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  current_count integer;
  w bigint;
  wb bigint;
  consumed bigint;
  backfilled bigint;
BEGIN
  LOCK TABLE unspent_tracking_progress IN EXCLUSIVE MODE;
  SELECT min(partition_count), min(input_transaction_internal_id), min(block_internal_id), min(consumed_event_id),
         min(backfill_transaction_internal_id)
    INTO current_count, w, wb, consumed, backfilled FROM unspent_tracking_progress;
  IF current_count IS NULL OR (current_count = part_count AND
      (SELECT count(*) FROM unspent_tracking_progress) = part_count) THEN
    RETURN false;
  END IF;
  DELETE FROM unspent_tracking_progress;
  INSERT INTO unspent_tracking_progress (partition_index, partition_count, input_transaction_internal_id,
      block_internal_id, consumed_event_id, backfill_transaction_internal_id)
    SELECT p, part_count, w, wb, consumed, backfilled FROM generate_series(0, part_count - 1) p;
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
CREATE FUNCTION unspent_node_ids_watch_pass (event_limit bigint)
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
    SELECT r.tx, COALESCE(tip, 0), unspent_node_ids_accepting(r.tx)
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
             unspent_node_ids_accepting(w.transaction_internal_id) AS ids
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

-- Stop tracking (the agent runs with CHAINGRAPH_UNSPENT_NODE_IDS=false on a
-- database where tracking had started; the release-event triggers are
-- disabled first, so acceptance changes are no longer recorded): drop the
-- watermarks, events, watch set and passed inputs, so the query root answers
-- with the live F1g predicate (read tier "fallback" without watermarks), and
-- mark the stored values as possibly stale (rebuild_required) for the next
-- start. Returns whether tracking was running.
CREATE FUNCTION unspent_node_ids_stop ()
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
-- after a round).
CREATE FUNCTION unspent_node_ids_delete_consumed_events ()
  RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE
  deleted bigint;
BEGIN
  DELETE FROM unspent_tracking_events
    WHERE id <= LEAST((SELECT COALESCE(min(consumed_event_id), 0) FROM unspent_tracking_progress),
                      (SELECT watch_consumed_event_id FROM unspent_tracking_settings));
  GET DIAGNOSTICS deleted = ROW_COUNT;
  IF NOT (SELECT backfill_complete FROM unspent_tracking_settings)
     AND NOT unspent_node_ids_backfill_pending() THEN
    UPDATE unspent_tracking_settings SET backfill_complete = true;
  END IF;
  IF (SELECT rebuild_required FROM unspent_tracking_settings) AND NOT unspent_node_ids_rebuild_pending() THEN
    UPDATE unspent_tracking_settings SET rebuild_required = false, rebuild_through_transaction_internal_id = NULL;
  END IF;
  RETURN deleted;
END;
$$;

-- Read side. All helpers read the watermarks in the caller's snapshot and use
-- the minimum over the partitions (every row created at or below it is
-- processed). The tier is chosen once per query:
--   'hash'     backlog <= hash_max_transactions: the outpoints spent by
--              unprocessed / dirty transactions are hashed once;
--   'probe'    backlog <= fallback_transactions: one spent_by_index probe per
--              stored row;
--   'fallback' larger backlog, too many events, tracking not started (or
--              stopped), or a rebuild after a stop in progress: the live F1g
--              predicate.
-- `SET chaingraph.unspent_read_tier = hash|probe|fallback` forces a tier
-- (tests and measurements).
CREATE FUNCTION unspent_node_ids_read_tier ()
  RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
  forced text := current_setting('chaingraph.unspent_read_tier', true);
  s unspent_tracking_settings;
  w bigint;
  max_tx bigint;
  events bigint;
BEGIN
  SELECT * INTO s FROM unspent_tracking_settings;
  SELECT min(input_transaction_internal_id) INTO w FROM unspent_tracking_progress;
  IF w IS NULL OR s.tracking_start_transaction_internal_id IS NULL THEN
    RETURN 'fallback';
  END IF;
  -- after a stop, stored values may be stale until the rebuild is complete
  IF s.rebuild_required AND unspent_node_ids_rebuild_pending() THEN
    RETURN 'fallback';
  END IF;
  IF forced IN ('hash', 'probe', 'fallback') THEN
    RETURN forced;
  END IF;
  SELECT count(*) INTO events FROM (SELECT 1 FROM unspent_tracking_events LIMIT s.fallback_events + 1) e;
  IF events > s.fallback_events THEN
    RETURN 'fallback';
  END IF;
  SELECT max(internal_id) INTO max_tx FROM transaction;
  IF COALESCE(max_tx, 0) - w > s.fallback_transactions THEN
    RETURN 'fallback';
  END IF;
  IF COALESCE(max_tx, 0) - w > s.hash_max_transactions THEN
    RETURN 'probe';
  END IF;
  RETURN 'hash';
END;
$$;

-- Whether pre-tracking outputs may still be NULL: some partition's backfill
-- cursor has not passed the tracking start (or tracking has not started).
-- Derived from the cursors, which only the backfill advances after writing
-- every output behind them, never from a settable flag: while it is true the
-- query root answers every NULL output live, so a NULL is never read as spent.
CREATE FUNCTION unspent_node_ids_backfill_pending ()
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT NOT COALESCE((SELECT min(p.backfill_transaction_internal_id) FROM unspent_tracking_progress p)
                      >= (SELECT s.tracking_start_transaction_internal_id FROM unspent_tracking_settings s), false)
$$;

-- Whether a rebuild after a stop is still in progress: the backfill (which
-- then recomputes every pre-tracking output) is incomplete, or the job has
-- not passed rebuild_through_transaction_internal_id.
CREATE FUNCTION unspent_node_ids_rebuild_pending ()
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT s.rebuild_required
     AND (unspent_node_ids_backfill_pending()
          OR NOT COALESCE((SELECT min(p.input_transaction_internal_id) FROM unspent_tracking_progress p)
                            >= s.rebuild_through_transaction_internal_id, false))
    FROM unspent_tracking_settings s
$$;

-- Whether the query root serves `node_name` from the stored set: the read tier
-- is not "fallback", the node's per-node indexes are valid
-- (unspent_tracking_nodes) and every partition has consumed the events
-- recorded before they were. Otherwise (a node registered after tracking
-- started, before its indexes exist; an unknown name) the root answers with
-- the live F1g predicate only.
CREATE FUNCTION unspent_node_ids_stored_path (node_name text)
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT unspent_node_ids_read_tier() <> 'fallback'
     AND EXISTS (SELECT 1 FROM node n JOIN unspent_tracking_nodes r ON r.node_internal_id = n.internal_id
                   WHERE n.name = node_name
                     AND r.ready_after_event_id
                         <= (SELECT COALESCE(min(p.consumed_event_id), -1) FROM unspent_tracking_progress p))
$$;

-- Transactions whose acceptance may have changed after a partition's snapshot:
-- those of events not yet deleted and of blocks above the minimum block
-- watermark. (These helpers force custom plans: a generic plan for
-- `> watermark` estimates a third of the table and scans it.)
CREATE FUNCTION unspent_node_ids_dirty_transactions ()
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
CREATE FUNCTION unspent_node_ids_recent_spends ()
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
      FROM unspent_node_ids_dirty_transactions() d
      CROSS JOIN LATERAL (SELECT input.outpoint_transaction_hash, input.outpoint_index FROM input
                            WHERE input.transaction_internal_id = d.id OFFSET 0) i
      WHERE d.id <= w;
END;
$$;

-- Hashes of dirty transactions (their outputs' creator acceptance may have
-- changed; stored rows they created are re-checked).
CREATE FUNCTION unspent_node_ids_dirty_creators ()
  RETURNS TABLE (hash bytea) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off SET plan_cache_mode = force_custom_plan AS $$
BEGIN
  RETURN QUERY
    SELECT DISTINCT t.hash FROM unspent_node_ids_dirty_transactions() d
      CROSS JOIN LATERAL (SELECT transaction.hash FROM transaction WHERE transaction.internal_id = d.id OFFSET 0) t;
END;
$$;

-- Hashes of transactions created above the minimum input watermark (their
-- outputs may be unprocessed, i.e. NULL).
CREATE FUNCTION unspent_node_ids_backlog_transactions ()
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
CREATE FUNCTION unspent_node_ids_released_spends (node_id bigint)
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
CREATE FUNCTION unspent_node_ids_gained_creators (node_id bigint)
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

-- (Re)create unspent_output_stored(node_name) for the registered nodes. The
-- agent calls it whenever tracking is configured (once all nodes are
-- registered) and when it creates the per-node indexes. Serialised by a
-- transaction-scoped advisory lock (idempotent: the last build wins and every
-- build reads the current node table). Per node (the node id a literal, so `<id> = ANY (unspent_node_ids)`
-- matches that node's partial indexes; the other nodes' arms are removed at
-- plan time by `node_name = '<name>'`):
--   (a1) stored token outputs with a locking bytecode up to 1,000 bytes:
--        index-only through the covering per-node category index;
--   (a2) every other stored output (no token, or a longer locking bytecode):
--        the per-node "rest" index, keyed on the category too, so a category
--        filter is an index condition there (no token: NULL keys, deduplicated);
--   both also reachable through the per-node 25-byte prefix index; each row
--   re-checked live (F1g) when spent above the watermark or by a dirty
--   transaction, or created by a dirty transaction. The two arm predicates
--   are complementary and spelled exactly as the index predicates, so the
--   planner matches them.
-- Shared arms (node resolved once):
--   (b1) unprocessed outputs of transactions above the minimum input watermark;
--   (b2) pre-tracking NULL outputs while the backfill is incomplete (the
--        caller's own index: token_category_index, output_search_index);
--   (d1) outputs spent by released transactions the node no longer accepts;
--   (d2) outputs of transactions the node started accepting;
--   (f)  F1g, instead of every other arm, when the read tier is "fallback" or
--        the node is not served from the stored set yet
--        (unspent_node_ids_stored_path: its per-node indexes are not valid,
--        or the events recorded before they were are not consumed, or the
--        name is unknown). Every arm is gated by that one per-query value,
--        so the arms never overlap.
CREATE FUNCTION unspent_node_ids_build_root ()
  RETURNS void LANGUAGE plpgsql AS $build$
DECLARE
  arms text := '';
  node record;
  f1g_literal text;
  f1g_shared constant text := $f$CASE
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
    AND (SELECT unspent_node_ids_stored_path(node_name))
    AND CASE
      WHEN CASE WHEN (SELECT unspent_node_ids_read_tier()) = 'hash'
             THEN (o.transaction_hash, o.output_index) IN (
                    SELECT r.outpoint_transaction_hash, r.outpoint_index FROM unspent_node_ids_recent_spends() r)
             ELSE EXISTS (
                    SELECT 1 FROM input i
                    WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
                      AND (i.transaction_internal_id > (SELECT min(p.input_transaction_internal_id) FROM unspent_tracking_progress p)
                        OR i.transaction_internal_id IN (SELECT d.id FROM unspent_node_ids_dirty_transactions() d)))
           END
        OR o.transaction_hash IN (SELECT c.hash FROM unspent_node_ids_dirty_creators() c)
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
  -- are answered by F1g; unspent_node_ids_stored_path gates every arm at run time)
  FOR node IN SELECT n.internal_id, n.name FROM node n
                JOIN unspent_tracking_nodes r ON r.node_internal_id = n.internal_id
                ORDER BY n.internal_id LOOP
    f1g_literal := replace(f1g_shared, '(SELECT n.internal_id FROM node n WHERE n.name = node_name)', node.internal_id::text);
    arms := arms
      || format(stored_arm, node.name, node.internal_id,
           'o.token_category IS NOT NULL AND octet_length(o.locking_bytecode) <= 1000', f1g_literal)
      || format(stored_arm, node.name, node.internal_id,
           '(o.token_category IS NULL OR octet_length(o.locking_bytecode) > 1000)', f1g_literal);
  END LOOP;
  EXECUTE 'CREATE OR REPLACE FUNCTION unspent_output_stored (node_name text) RETURNS SETOF output LANGUAGE sql STABLE AS $root$'
    || chr(10) || arms
    || format($shared$  SELECT o.* FROM unspent_node_ids_backlog_transactions() b
  CROSS JOIN LATERAL (
    SELECT * FROM output WHERE output.transaction_hash = b.hash OFFSET 0) o
  WHERE o.unspent_node_ids IS NULL
    AND (SELECT unspent_node_ids_stored_path(node_name))
    AND %1$s
  UNION ALL
  SELECT o.* FROM output o
  WHERE o.unspent_node_ids IS NULL
    AND (SELECT unspent_node_ids_backfill_pending())
    AND (SELECT unspent_node_ids_stored_path(node_name))
    AND NOT (o.transaction_hash IN (SELECT b.hash FROM unspent_node_ids_backlog_transactions() b))
    AND %1$s
  UNION ALL
  SELECT o.* FROM unspent_node_ids_released_spends((SELECT n.internal_id FROM node n WHERE n.name = node_name)) r
  CROSS JOIN LATERAL (
    SELECT * FROM output
      WHERE output.transaction_hash = r.outpoint_transaction_hash AND output.output_index = r.outpoint_index OFFSET 0) o
  WHERE NOT ((SELECT n.internal_id FROM node n WHERE n.name = node_name)::integer = ANY (o.unspent_node_ids))
    AND (SELECT unspent_node_ids_stored_path(node_name))
    AND %1$s
  UNION ALL
  SELECT o.* FROM unspent_node_ids_gained_creators((SELECT n.internal_id FROM node n WHERE n.name = node_name)) c
  CROSS JOIN LATERAL (
    SELECT * FROM output WHERE output.transaction_hash = c.hash OFFSET 0) o
  WHERE NOT ((SELECT n.internal_id FROM node n WHERE n.name = node_name)::integer = ANY (o.unspent_node_ids))
    AND NOT ((o.transaction_hash, o.output_index) IN (
      SELECT r.outpoint_transaction_hash, r.outpoint_index
        FROM unspent_node_ids_released_spends((SELECT n.internal_id FROM node n WHERE n.name = node_name)) r))
    AND (SELECT unspent_node_ids_stored_path(node_name))
    AND %1$s
  UNION ALL
  SELECT o.* FROM output o
  WHERE NOT (SELECT unspent_node_ids_stored_path(node_name))
    AND %1$s
$shared$, f1g_shared) || '$root$';
  EXECUTE $c$COMMENT ON FUNCTION unspent_output_stored (text) IS 'The outputs unspent according to the named node: created by a transaction the node accepts (in a block it accepts or in its mempool) and not spent by any transaction the node accepts. Same rows as unspent_output(node_name); served from the stored per-node set (output.unspent_node_ids) with a live correction for anything the tracking job has not processed yet. Filter by token_category (and nonfungible_token_capability) or use a 25-byte locking bytecode prefix; a call filtered only by locking_bytecode scans every output.'$c$;
END;
$build$;
SELECT unspent_node_ids_build_root();
