-- Experiment (E17b): deferred per-node integer[] (CHAINGRAPH_UNSPENT_TRACKING=deferred-array), on top of
-- 1791400002000_unspent_deferred. output.unspent_node_ids: NULL = unprocessed; '{}' = audited, unspent for no node;
-- otherwise the sorted node internal ids for which the output is unspent (creator accepted, no accepted spender).
-- Maintained only by the tracking job (same batch, events, watch and query-root corrections as the bitmask); no
-- trigger beyond the release-event pair, no advisory lock beyond the job's own, no GIN index. Per-node partial
-- B-tree indexes (`<n> = ANY (unspent_node_ids)`) are created by the agent; the query root's stored-set arm is
-- generated per node (unspent_deferred_array_build_root) so each node's arm can use its partial indexes.
ALTER TABLE output ADD COLUMN unspent_node_ids integer[];
COMMENT ON COLUMN output.unspent_node_ids IS 'Experiment (CHAINGRAPH_UNSPENT_TRACKING=deferred-array): NULL = unprocessed; sorted node internal ids for which the output is unspent (created by a transaction that node accepts, no spender that node accepts).';

CREATE FUNCTION unspent_deferred_recompute_array ()
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  changed bigint;
BEGIN
  UPDATE output o SET unspent_node_ids = v.ids
    FROM (SELECT a.h, a.i,
                 ARRAY(SELECT c.node FROM (
                         SELECT n.internal_id AS node
                           FROM block_transaction bt
                           CROSS JOIN node n
                           JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                           WHERE bt.transaction_internal_id = ct.internal_id
                         UNION
                         SELECT nt.node_internal_id FROM node_transaction nt WHERE nt.transaction_internal_id = ct.internal_id
                         EXCEPT
                         SELECT n.internal_id
                           FROM (SELECT transaction_internal_id FROM input
                                   WHERE input.outpoint_transaction_hash = a.h AND input.outpoint_index = a.i OFFSET 0) x
                           CROSS JOIN node n
                           WHERE EXISTS (SELECT 1 FROM block_transaction bt
                                           JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                                           WHERE bt.transaction_internal_id = x.transaction_internal_id)
                              OR EXISTS (SELECT 1 FROM node_transaction nt
                                           WHERE nt.transaction_internal_id = x.transaction_internal_id AND nt.node_internal_id = n.internal_id)
                       ) c ORDER BY c.node)::integer[] AS ids
            FROM (SELECT DISTINCT h, i FROM pg_temp.unspent_deferred_affected ORDER BY h, i) a
            CROSS JOIN LATERAL (
              SELECT internal_id FROM transaction WHERE transaction.hash = a.h OFFSET 0) ct
          OFFSET 0) v
    WHERE o.transaction_hash = v.h AND o.output_index = v.i
      AND o.unspent_node_ids IS DISTINCT FROM v.ids;
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed;
END;
$$;
CREATE OR REPLACE FUNCTION unspent_deferred_run_batch (kind text, tx_limit bigint, block_limit bigint,
  max_inputs integer, max_blocks integer, max_events integer, sweep_rows integer, skip_stall_through bigint,
  check_watch boolean DEFAULT true)
  RETURNS jsonb LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  zero_hash constant bytea := '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea;
  w bigint;
  wb bigint;
  upper_tx bigint;
  upper_block bigint;
  stall_tx bigint;
  skipped_inputs bigint := 0;
  n_inputs bigint := 0;
  n_events bigint := 0;
  n_blocks bigint := 0;
  n_block_txs bigint := 0;
  n_watch_changed bigint := 0;
  n_watch_added bigint := 0;
  n_watch_expired bigint := 0;
  n_sweep bigint := 0;
  n_event_txs bigint := 0;
  n_affected bigint := 0;
  n_changed bigint := 0;
  n_fresh bigint := 0;
  max_event bigint;
  tip bigint;
BEGIN
  IF kind NOT IN ('marker', 'bitmask', 'array') THEN
    RAISE EXCEPTION 'unknown tracking kind %', kind;
  END IF;
  -- single instance (also across agents): the second caller returns at once
  IF NOT pg_try_advisory_xact_lock(1970172784, hashtext(kind)) THEN
    RETURN jsonb_build_object('busy', true);
  END IF;
  SELECT p.input_transaction_internal_id, p.node_block_block_internal_id INTO w, wb
    FROM unspent_tracking_progress p
    WHERE p.tracking_kind = kind AND p.node_internal_id = 0
    FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('uninitialized', true);
  END IF;
  CREATE TEMP TABLE IF NOT EXISTS unspent_deferred_affected (h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE INDEX IF NOT EXISTS unspent_deferred_affected_key ON pg_temp.unspent_deferred_affected (h, i);
  CREATE TEMP TABLE IF NOT EXISTS unspent_deferred_fresh (h bytea NOT NULL, i bigint NOT NULL, creator bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_deferred_txs (id bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_deferred_inputs (tx bigint NOT NULL, h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_deferred_consumed (id bigint NOT NULL, event_kind text NOT NULL, node bigint NOT NULL,
    block_id bigint, tx_id bigint) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE IF NOT EXISTS unspent_deferred_released (tx bigint NOT NULL, node bigint NOT NULL) ON COMMIT DELETE ROWS;
  SELECT max(height) INTO tip FROM block;

  -- 1. events, in id order (deleted below, in the same transaction)
  INSERT INTO unspent_deferred_consumed
    SELECT e.id, e.event_kind, e.node_internal_id, e.block_internal_id, e.transaction_internal_id
      FROM unspent_tracking_events e ORDER BY e.id LIMIT max_events;
  GET DIAGNOSTICS n_events = ROW_COUNT;
  IF n_events > 0 THEN
    SELECT max(id) INTO max_event FROM unspent_deferred_consumed;
    INSERT INTO unspent_deferred_released (tx, node)
      SELECT c.tx_id, c.node FROM unspent_deferred_consumed c WHERE c.event_kind = 'released' AND c.tx_id IS NOT NULL
      UNION
      SELECT bt.transaction_internal_id, b.node
        FROM (SELECT DISTINCT c.block_id, c.node FROM unspent_deferred_consumed c
                WHERE c.event_kind = 'released' AND c.block_id IS NOT NULL) b
        CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                              WHERE block_transaction.block_internal_id = b.block_id OFFSET 0) bt;
    INSERT INTO unspent_deferred_txs (id)
      SELECT c.tx_id FROM unspent_deferred_consumed c WHERE c.tx_id IS NOT NULL
      UNION ALL
      SELECT bt.transaction_internal_id
        FROM (SELECT DISTINCT c.block_id FROM unspent_deferred_consumed c WHERE c.block_id IS NOT NULL) b
        CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                              WHERE block_transaction.block_internal_id = b.block_id OFFSET 0) bt;
    GET DIAGNOSTICS n_event_txs = ROW_COUNT;
    -- watch set: released transactions the releasing node (bitmask) / every
    -- node (marker) no longer accepts
    IF kind = 'marker' THEN
      INSERT INTO unspent_tracking_watch (tracking_kind, transaction_internal_id, released_at_height, accepted_node_bits)
        SELECT kind, r.tx, COALESCE(tip, 0), 0
          FROM (SELECT DISTINCT tx FROM unspent_deferred_released) r
          WHERE CASE WHEN EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                                JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                                WHERE bt.transaction_internal_id = r.tx)
                  OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = r.tx)
             THEN 1 ELSE 0 END::bigint = 0
        ON CONFLICT ON CONSTRAINT unspent_tracking_watch_pkey DO UPDATE
          SET released_at_height = EXCLUDED.released_at_height, accepted_node_bits = EXCLUDED.accepted_node_bits;
    ELSE
      INSERT INTO unspent_tracking_watch (tracking_kind, transaction_internal_id, released_at_height, accepted_node_bits)
        SELECT kind, r.tx, COALESCE(tip, 0), r.bits
          FROM (SELECT x.tx, (SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0)
                  FROM block_transaction bt
                  CROSS JOIN node n
                  JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                  WHERE bt.transaction_internal_id = x.tx AND n.internal_id < 64)
             | (SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0)
                  FROM node_transaction nt
                  WHERE nt.transaction_internal_id = x.tx AND nt.node_internal_id < 64) AS bits, x.released_bits
                  FROM (SELECT tx, bit_or(CASE WHEN node < 64 THEN 1::bigint << node::integer ELSE 0 END) AS released_bits
                          FROM unspent_deferred_released GROUP BY tx) x) r
          WHERE (r.bits & r.released_bits) <> r.released_bits
        ON CONFLICT ON CONSTRAINT unspent_tracking_watch_pkey DO UPDATE
          SET released_at_height = EXCLUDED.released_at_height, accepted_node_bits = EXCLUDED.accepted_node_bits;
    END IF;
    GET DIAGNOSTICS n_watch_added = ROW_COUNT;
    DELETE FROM unspent_tracking_events e USING unspent_deferred_consumed c WHERE e.id = c.id;
  END IF;

  -- 2. watch set: acceptance changed since the last check -> recompute
  -- (probes written inline: a SQL function with sub-queries is never inlined)
  IF check_watch THEN
    IF kind = 'marker' THEN
      WITH changed AS (
        UPDATE unspent_tracking_watch w0 SET accepted_node_bits = s.bits
          FROM (SELECT wt.transaction_internal_id, CASE WHEN EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                                JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                                WHERE bt.transaction_internal_id = wt.transaction_internal_id)
                  OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = wt.transaction_internal_id)
             THEN 1 ELSE 0 END::bigint AS bits
                  FROM unspent_tracking_watch wt WHERE wt.tracking_kind = kind) s
          WHERE w0.tracking_kind = kind AND w0.transaction_internal_id = s.transaction_internal_id
            AND w0.accepted_node_bits <> s.bits
          RETURNING w0.transaction_internal_id
      )
      INSERT INTO unspent_deferred_txs (id) SELECT transaction_internal_id FROM changed;
    ELSE
      WITH changed AS (
        UPDATE unspent_tracking_watch w0 SET accepted_node_bits = s.bits
          FROM (SELECT wt.transaction_internal_id, (SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0)
                  FROM block_transaction bt
                  CROSS JOIN node n
                  JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                  WHERE bt.transaction_internal_id = wt.transaction_internal_id AND n.internal_id < 64)
             | (SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0)
                  FROM node_transaction nt
                  WHERE nt.transaction_internal_id = wt.transaction_internal_id AND nt.node_internal_id < 64) AS bits
                  FROM unspent_tracking_watch wt WHERE wt.tracking_kind = kind) s
          WHERE w0.tracking_kind = kind AND w0.transaction_internal_id = s.transaction_internal_id
            AND w0.accepted_node_bits <> s.bits
          RETURNING w0.transaction_internal_id
      )
      INSERT INTO unspent_deferred_txs (id) SELECT transaction_internal_id FROM changed;
    END IF;
    GET DIAGNOSTICS n_watch_changed = ROW_COUNT;
    DELETE FROM unspent_tracking_watch WHERE tracking_kind = kind AND released_at_height < COALESCE(tip, 0) - 100;
    GET DIAGNOSTICS n_watch_expired = ROW_COUNT;
  END IF;

  -- 3. input / transaction range above the watermark (bounded)
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
    INSERT INTO unspent_deferred_inputs (tx, h, i)
      SELECT i.transaction_internal_id, i.outpoint_transaction_hash, i.outpoint_index
        FROM input i
        WHERE i.transaction_internal_id > w AND i.transaction_internal_id <= upper_tx
          AND i.outpoint_transaction_hash <> zero_hash;
    -- child-before-parent: stop before the first transaction with an input
    -- whose output row does not exist yet (retried next pass)
    SELECT min(c.tx) INTO stall_tx
      FROM unspent_deferred_inputs c
      WHERE c.tx > skip_stall_through
        AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = c.h AND o.output_index = c.i);
    IF stall_tx IS NOT NULL THEN
      upper_tx := stall_tx - 1;
      DELETE FROM unspent_deferred_inputs WHERE tx > upper_tx;
    END IF;
    IF skip_stall_through > w THEN
      INSERT INTO unspent_tracking_skipped (outpoint_transaction_hash, outpoint_index, transaction_internal_id)
        SELECT c.h, c.i, c.tx
          FROM unspent_deferred_inputs c
          WHERE c.tx <= skip_stall_through
            AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = c.h AND o.output_index = c.i)
        ON CONFLICT ON CONSTRAINT unspent_tracking_skipped_pkey DO NOTHING;
      GET DIAGNOSTICS skipped_inputs = ROW_COUNT;
    END IF;
    SELECT count(*) INTO n_inputs FROM unspent_deferred_inputs;
    INSERT INTO unspent_deferred_affected (h, i) SELECT h, i FROM unspent_deferred_inputs;
    IF upper_tx > w THEN
      -- outputs created by the range; those the job has not seen (NULL) take
      -- the fast path in step 7 unless something else touches them
      IF kind = 'marker' THEN
        INSERT INTO unspent_deferred_fresh (h, i, creator)
          SELECT o.transaction_hash, o.output_index, t.internal_id
            FROM transaction t
            CROSS JOIN LATERAL (SELECT transaction_hash, output_index, spent_by_transaction_internal_id AS stored FROM output
                                  WHERE output.transaction_hash = t.hash OFFSET 0) o
            WHERE t.internal_id > w AND t.internal_id <= upper_tx AND o.stored IS NULL;
        INSERT INTO unspent_deferred_affected (h, i)
          SELECT o.transaction_hash, o.output_index
            FROM transaction t
            CROSS JOIN LATERAL (SELECT transaction_hash, output_index, spent_by_transaction_internal_id AS stored FROM output
                                  WHERE output.transaction_hash = t.hash OFFSET 0) o
            WHERE t.internal_id > w AND t.internal_id <= upper_tx AND o.stored IS NOT NULL;
      ELSIF kind = 'bitmask' THEN
        INSERT INTO unspent_deferred_fresh (h, i, creator)
          SELECT o.transaction_hash, o.output_index, t.internal_id
            FROM transaction t
            CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_bits AS stored FROM output
                                  WHERE output.transaction_hash = t.hash OFFSET 0) o
            WHERE t.internal_id > w AND t.internal_id <= upper_tx AND o.stored IS NULL;
        INSERT INTO unspent_deferred_affected (h, i)
          SELECT o.transaction_hash, o.output_index
            FROM transaction t
            CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_bits AS stored FROM output
                                  WHERE output.transaction_hash = t.hash OFFSET 0) o
            WHERE t.internal_id > w AND t.internal_id <= upper_tx AND o.stored IS NOT NULL;
      ELSE
        INSERT INTO unspent_deferred_fresh (h, i, creator)
          SELECT o.transaction_hash, o.output_index, t.internal_id
            FROM transaction t
            CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_ids AS stored FROM output
                                  WHERE output.transaction_hash = t.hash OFFSET 0) o
            WHERE t.internal_id > w AND t.internal_id <= upper_tx AND o.stored IS NULL;
        INSERT INTO unspent_deferred_affected (h, i)
          SELECT o.transaction_hash, o.output_index
            FROM transaction t
            CROSS JOIN LATERAL (SELECT transaction_hash, output_index, unspent_node_ids AS stored FROM output
                                  WHERE output.transaction_hash = t.hash OFFSET 0) o
            WHERE t.internal_id > w AND t.internal_id <= upper_tx AND o.stored IS NOT NULL;
      END IF;
      -- fresh outputs a passed (skipped) input spends: full recompute
      INSERT INTO unspent_deferred_affected (h, i)
        SELECT f.h, f.i FROM unspent_deferred_fresh f
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
      INSERT INTO unspent_deferred_txs (id)
        SELECT bt.transaction_internal_id
          FROM (SELECT internal_id FROM block WHERE internal_id > wb AND internal_id <= upper_block) b
          CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                WHERE block_transaction.block_internal_id = b.internal_id
                                  AND block_transaction.transaction_internal_id <= w OFFSET 0) bt;
      GET DIAGNOSTICS n_block_txs = ROW_COUNT;
    END IF;
  END IF;

  -- 5. collected transactions -> the outputs they spend (and, bitmask, create)
  INSERT INTO unspent_deferred_affected (h, i)
    SELECT i.outpoint_transaction_hash, i.outpoint_index
      FROM (SELECT DISTINCT id FROM unspent_deferred_txs) x
      CROSS JOIN LATERAL (SELECT outpoint_transaction_hash, outpoint_index FROM input
                            WHERE input.transaction_internal_id = x.id OFFSET 0) i
      WHERE i.outpoint_transaction_hash <> zero_hash;
  IF kind IN ('bitmask', 'array') THEN
    INSERT INTO unspent_deferred_affected (h, i)
      SELECT o.transaction_hash, o.output_index
        FROM (SELECT DISTINCT id FROM unspent_deferred_txs) x
        CROSS JOIN LATERAL (SELECT hash FROM transaction WHERE transaction.internal_id = x.id OFFSET 0) t
        CROSS JOIN LATERAL (SELECT transaction_hash, output_index FROM output
                              WHERE output.transaction_hash = t.hash OFFSET 0) o;
  END IF;

  -- 6. sweep: NULL token outputs of transactions below the watermark (backfill)
  IF sweep_rows > 0 THEN
    IF kind = 'marker' THEN
      INSERT INTO unspent_deferred_affected (h, i)
        SELECT o.transaction_hash, o.output_index
          FROM (SELECT transaction_hash, output_index FROM output
                  WHERE spent_by_transaction_internal_id IS NULL AND token_category IS NOT NULL
                  ORDER BY token_category) o
          CROSS JOIN LATERAL (SELECT internal_id FROM transaction WHERE transaction.hash = o.transaction_hash OFFSET 0) t
          WHERE t.internal_id <= upper_tx
          LIMIT sweep_rows;
    ELSIF kind = 'bitmask' THEN
      INSERT INTO unspent_deferred_affected (h, i)
        SELECT o.transaction_hash, o.output_index
          FROM (SELECT transaction_hash, output_index FROM output
                  WHERE unspent_node_bits IS NULL AND token_category IS NOT NULL
                  ORDER BY token_category) o
          CROSS JOIN LATERAL (SELECT internal_id FROM transaction WHERE transaction.hash = o.transaction_hash OFFSET 0) t
          WHERE t.internal_id <= upper_tx
          LIMIT sweep_rows;
    ELSE
      INSERT INTO unspent_deferred_affected (h, i)
        SELECT o.transaction_hash, o.output_index
          FROM (SELECT transaction_hash, output_index FROM output
                  WHERE unspent_node_ids IS NULL AND token_category IS NOT NULL
                  ORDER BY token_category) o
          CROSS JOIN LATERAL (SELECT internal_id FROM transaction WHERE transaction.hash = o.transaction_hash OFFSET 0) t
          WHERE t.internal_id <= upper_tx
          LIMIT sweep_rows;
    END IF;
    GET DIAGNOSTICS n_sweep = ROW_COUNT;
  END IF;

  -- 7. recompute: every collected output from scratch; then the fast path
  -- for outputs the job sees for the first time and nothing else touched (no
  -- processed input can spend them: the job stops before an input whose
  -- output is missing, and records the ones it passes in
  -- unspent_tracking_skipped): marker 0 / the creator's acceptance bits,
  -- without probing spent_by_index
  SELECT count(*) INTO n_affected FROM unspent_deferred_affected;
  IF n_affected > 0 THEN
    IF kind = 'marker' THEN
      n_changed := unspent_deferred_recompute_marker();
    ELSIF kind = 'bitmask' THEN
      n_changed := unspent_deferred_recompute_bits();
    ELSE
      n_changed := unspent_deferred_recompute_array();
    END IF;
  END IF;
  IF kind = 'marker' THEN
    UPDATE output o SET spent_by_transaction_internal_id = 0
      FROM (SELECT f.h, f.i FROM unspent_deferred_fresh f
              WHERE NOT EXISTS (SELECT 1 FROM pg_temp.unspent_deferred_affected a WHERE a.h = f.h AND a.i = f.i)
              ORDER BY f.h, f.i OFFSET 0) v
      WHERE o.transaction_hash = v.h AND o.output_index = v.i
        AND o.spent_by_transaction_internal_id IS DISTINCT FROM 0;
  ELSIF kind = 'bitmask' THEN
    UPDATE output o SET unspent_node_bits = v.bits
      FROM (SELECT f.h, f.i,
                   (SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0)
                      FROM block_transaction bt
                      CROSS JOIN node n
                      JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                      WHERE bt.transaction_internal_id = f.creator AND n.internal_id < 64)
                 | (SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0)
                      FROM node_transaction nt
                      WHERE nt.transaction_internal_id = f.creator AND nt.node_internal_id < 64) AS bits
              FROM unspent_deferred_fresh f
              WHERE NOT EXISTS (SELECT 1 FROM pg_temp.unspent_deferred_affected a WHERE a.h = f.h AND a.i = f.i)
              ORDER BY f.h, f.i OFFSET 0) v
      WHERE o.transaction_hash = v.h AND o.output_index = v.i
        AND o.unspent_node_bits IS DISTINCT FROM v.bits;
  ELSE
    UPDATE output o SET unspent_node_ids = v.ids
      FROM (SELECT f.h, f.i,
                   ARRAY(SELECT c.node FROM (
                           SELECT n.internal_id AS node
                             FROM block_transaction bt
                             CROSS JOIN node n
                             JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                             WHERE bt.transaction_internal_id = f.creator
                           UNION
                           SELECT nt.node_internal_id FROM node_transaction nt WHERE nt.transaction_internal_id = f.creator
                         ) c ORDER BY c.node)::integer[] AS ids
              FROM unspent_deferred_fresh f
              WHERE NOT EXISTS (SELECT 1 FROM pg_temp.unspent_deferred_affected a WHERE a.h = f.h AND a.i = f.i)
              ORDER BY f.h, f.i OFFSET 0) v
      WHERE o.transaction_hash = v.h AND o.output_index = v.i
        AND o.unspent_node_ids IS DISTINCT FROM v.ids;
  END IF;
  GET DIAGNOSTICS n_fresh = ROW_COUNT;
  n_affected := n_affected + n_fresh;
  n_changed := n_changed + n_fresh;
  -- passed inputs whose output has now been processed are no longer needed
  DELETE FROM unspent_tracking_skipped s USING unspent_deferred_fresh f
    WHERE s.outpoint_transaction_hash = f.h AND s.outpoint_index = f.i;

  -- 8. watermarks (same transaction as the updates)
  UPDATE unspent_tracking_progress p
    SET input_transaction_internal_id = upper_tx,
        node_transaction_transaction_internal_id = upper_tx,
        node_block_block_internal_id = upper_block,
        consumed_event_id = GREATEST(p.consumed_event_id, COALESCE(max_event, 0)),
        updated_at = now()
    WHERE p.tracking_kind = kind;
  INSERT INTO unspent_tracking_progress (tracking_kind, node_internal_id, input_transaction_internal_id,
      node_transaction_transaction_internal_id, node_block_block_internal_id, consumed_event_id)
    SELECT kind, n.internal_id, upper_tx, upper_tx, upper_block, COALESCE(max_event, 0) FROM node n
    ON CONFLICT ON CONSTRAINT unspent_tracking_progress_pkey DO NOTHING;

  RETURN jsonb_build_object(
    'inputWatermark', upper_tx, 'previousInputWatermark', w,
    'blockWatermark', upper_block, 'previousBlockWatermark', wb,
    'inputs', n_inputs, 'stalledAt', stall_tx, 'skippedInputs', skipped_inputs,
    'events', n_events, 'eventTransactions', n_event_txs,
    'blocks', n_blocks, 'blockTransactions', n_block_txs,
    'watchAdded', n_watch_added, 'watchChanged', n_watch_changed, 'watchExpired', n_watch_expired,
    'sweep', n_sweep, 'affected', n_affected, 'changed', n_changed, 'fresh', n_fresh);
END;
$$;
CREATE OR REPLACE FUNCTION unspent_deferred_initialize (kind text, input_watermark bigint, block_watermark bigint)
  RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  inserted bigint;
BEGIN
  IF kind NOT IN ('marker', 'bitmask', 'array') THEN
    RAISE EXCEPTION 'unknown tracking kind %', kind;
  END IF;
  INSERT INTO unspent_tracking_progress (tracking_kind, node_internal_id, input_transaction_internal_id,
      node_transaction_transaction_internal_id, node_block_block_internal_id, consumed_event_id)
    VALUES (kind, 0, input_watermark, input_watermark, block_watermark, 0)
    ON CONFLICT ON CONSTRAINT unspent_tracking_progress_pkey DO NOTHING;
  GET DIAGNOSTICS inserted = ROW_COUNT;
  IF inserted > 0 THEN
    -- child-before-parent across the start of tracking: recent inputs whose
    -- output is not saved yet (bounded: the last 100,000 transactions)
    INSERT INTO unspent_tracking_skipped (outpoint_transaction_hash, outpoint_index, transaction_internal_id)
      SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
        FROM input i
        WHERE i.transaction_internal_id > input_watermark - 100000 AND i.transaction_internal_id <= input_watermark
          AND i.outpoint_transaction_hash <> '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
          AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index)
      ON CONFLICT ON CONSTRAINT unspent_tracking_skipped_pkey DO NOTHING;
  END IF;
  INSERT INTO unspent_tracking_progress (tracking_kind, node_internal_id, input_transaction_internal_id,
      node_transaction_transaction_internal_id, node_block_block_internal_id, consumed_event_id)
    SELECT kind, n.internal_id, p.input_transaction_internal_id, p.node_transaction_transaction_internal_id,
        p.node_block_block_internal_id, p.consumed_event_id
      FROM node n CROSS JOIN unspent_tracking_progress p
      WHERE p.tracking_kind = kind AND p.node_internal_id = 0
    ON CONFLICT ON CONSTRAINT unspent_tracking_progress_pkey DO NOTHING;
  RETURN inserted > 0;
END;
$$;
CREATE OR REPLACE FUNCTION unspent_deferred_backfill (kind text, category bytea, bytecode_prefix bytea)
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET jit = off AS $$
DECLARE
  w bigint;
BEGIN
  SELECT input_transaction_internal_id INTO w FROM unspent_tracking_progress
    WHERE tracking_kind = kind AND node_internal_id = 0;
  IF w IS NULL THEN
    RAISE EXCEPTION 'tracking kind % is not initialized', kind;
  END IF;
  CREATE TEMP TABLE IF NOT EXISTS unspent_deferred_affected (h bytea NOT NULL, i bigint NOT NULL) ON COMMIT DELETE ROWS;
  DELETE FROM unspent_deferred_affected;
  IF kind = 'marker' THEN
    INSERT INTO unspent_deferred_affected (h, i)
      SELECT o.transaction_hash, o.output_index FROM output o
        WHERE o.spent_by_transaction_internal_id IS NULL
          AND (category IS NULL OR o.token_category = category)
          AND (bytecode_prefix IS NULL OR substring(o.locking_bytecode, 0, 26) = bytecode_prefix);
    RETURN unspent_deferred_recompute_marker();
  ELSIF kind = 'bitmask' THEN
    INSERT INTO unspent_deferred_affected (h, i)
      SELECT o.transaction_hash, o.output_index FROM output o
        WHERE o.unspent_node_bits IS NULL
          AND (category IS NULL OR o.token_category = category)
          AND (bytecode_prefix IS NULL OR substring(o.locking_bytecode, 0, 26) = bytecode_prefix);
    RETURN unspent_deferred_recompute_bits();
  ELSE
    INSERT INTO unspent_deferred_affected (h, i)
      SELECT o.transaction_hash, o.output_index FROM output o
        WHERE o.unspent_node_ids IS NULL
          AND (category IS NULL OR o.token_category = category)
          AND (bytecode_prefix IS NULL OR substring(o.locking_bytecode, 0, 26) = bytecode_prefix);
    RETURN unspent_deferred_recompute_array();
  END IF;
END;
$$;
-- (Re)create unspent_output_deferred_array(node_name) for the current nodes: one stored-set arm pair per node with
-- the node id as a literal (so `<id> = ANY (unspent_node_ids)` matches that node's partial indexes; the other nodes'
-- arms are removed at plan time by `node_name = '<name>'`), plus the shared backlog / fallback arms. The agent calls
-- it at start-up (deferred-array mode).
CREATE FUNCTION unspent_deferred_array_build_root ()
  RETURNS void LANGUAGE plpgsql AS $build$
DECLARE
  arms text := '';
  node record;
BEGIN
  FOR node IN SELECT internal_id, name FROM node ORDER BY internal_id LOOP
    arms := arms || format($tmpl$  SELECT o.* FROM output o
  WHERE node_name = %1$L AND %2$s = ANY (o.unspent_node_ids)
    AND (SELECT unspent_deferred_read_tier('array')) = 'hash'
    AND CASE
      WHEN (o.transaction_hash, o.output_index) IN (
             SELECT r.outpoint_transaction_hash, r.outpoint_index FROM unspent_deferred_recent_spends('array') r)
        OR o.transaction_hash IN (SELECT c.hash FROM unspent_deferred_dirty_creators('array') c)
      THEN CASE
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
      END
      ELSE true
    END
  UNION ALL
  SELECT o.* FROM output o
  WHERE node_name = %1$L AND %2$s = ANY (o.unspent_node_ids)
    AND (SELECT unspent_deferred_read_tier('array')) = 'probe'
    AND CASE
      WHEN EXISTS (
             SELECT 1 FROM input i
             WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
               AND (i.transaction_internal_id > (SELECT p.input_transaction_internal_id FROM unspent_tracking_progress p
                                                   WHERE p.tracking_kind = 'array' AND p.node_internal_id = 0)
                 OR i.transaction_internal_id IN (SELECT d.id FROM unspent_deferred_dirty_transactions('array') d))
          OFFSET 0)
        OR o.transaction_hash IN (SELECT c.hash FROM unspent_deferred_dirty_creators('array') c)
      THEN CASE
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
      END
      ELSE true
    END
  UNION ALL
$tmpl$, node.name, node.internal_id);
  END LOOP;
  EXECUTE 'CREATE OR REPLACE FUNCTION unspent_output_deferred_array (node_name text) RETURNS SETOF output LANGUAGE sql STABLE AS $root$'
    || chr(10) || arms || $shared$  SELECT o.* FROM output o
  WHERE o.unspent_node_ids IS NULL
    AND (SELECT unspent_deferred_read_tier('array')) <> 'fallback'
    AND CASE
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
    END
  UNION ALL
  SELECT o.* FROM unspent_deferred_released_spends('array', node_name) r
  CROSS JOIN LATERAL (
    SELECT * FROM output
      WHERE output.transaction_hash = r.outpoint_transaction_hash AND output.output_index = r.outpoint_index OFFSET 0) o
  WHERE NOT ((SELECT n.internal_id FROM node n WHERE n.name = node_name) = ANY (o.unspent_node_ids))
    AND (SELECT unspent_deferred_read_tier('array')) <> 'fallback'
    AND CASE
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
    END
  UNION ALL
  SELECT o.* FROM unspent_deferred_dirty_creators('array') c
  CROSS JOIN LATERAL (
    SELECT * FROM output WHERE output.transaction_hash = c.hash OFFSET 0) o
  WHERE NOT ((SELECT n.internal_id FROM node n WHERE n.name = node_name) = ANY (o.unspent_node_ids))
    AND NOT ((o.transaction_hash, o.output_index) IN (
      SELECT r.outpoint_transaction_hash, r.outpoint_index FROM unspent_deferred_released_spends('array', node_name) r))
    AND (SELECT unspent_deferred_read_tier('array')) <> 'fallback'
    AND CASE
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
    END
  UNION ALL
  SELECT o.* FROM output o
  WHERE (SELECT unspent_deferred_read_tier('array')) = 'fallback'
    AND CASE
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
    END
$shared$ || '$root$';
END;
$build$;
SELECT unspent_deferred_array_build_root();
