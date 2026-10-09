-- Experiment (E17): deferred unspent tracking, selected with
-- CHAINGRAPH_UNSPENT_TRACKING=deferred-marker|deferred-bitmask.
--
-- Ingestion stays append-only: outputs are inserted with
-- output.spent_by_transaction_internal_id (marker) / output.unspent_node_bits
-- (bitmask) = NULL ("unprocessed"). Nothing is added to `input`. The only
-- write-path triggers are the two statement-level AFTER DELETE triggers below,
-- which append release events. The agent appends an "accepted" event only on
-- its re-acceptance paths (a node_block row for a block that already existed;
-- a node_transaction row for a transaction that already existed).
--
-- A recurring job inside the agent (unspent_deferred_run_batch, one batch per
-- REPEATABLE READ transaction, never concurrent with itself) brings the stored
-- values up to date behind watermarks kept in unspent_tracking_progress:
--   - the input range above the input watermark (input_pkey), in bounded
--     batches; an input whose output row does not exist yet stops the batch
--     before its transaction (child-before-parent: retried next pass);
--   - the outputs created by the same transaction range (NULL -> value);
--   - blocks above the block watermark (newly accepted blocks whose
--     transactions were saved earlier, e.g. from a mempool);
--   - unspent_tracking_events, consumed in id order and deleted;
--   - a "recently released" watch set, re-checked each pass for 100 blocks;
--   - optionally a sweep of NULL token outputs (backfill).
-- Every step only collects outpoints; one statement then recomputes each
-- collected output from scratch (F1g semantics), so all updates are
-- idempotent and order-independent. The agent only advances the input/block
-- watermarks to sequence values whose allocating transactions have all
-- finished, so ids at or below a watermark can never commit later.
--
-- marker semantics (as E15-B): NULL = unprocessed, 0 = no spender accepted by
-- any node, > 0 = an accepted spender (block-accepted preferred, then lowest
-- id). The "created by an accepted transaction" half stays a read-time check.
-- bitmask semantics (as E15-B): NULL = unprocessed, bit n = created by a
-- transaction node n accepts and no spender node n accepts.
--
-- Reads: unspent_output_deferred_marker(node) / _bitmask(node) return the
-- stored set corrected for everything the job has not processed yet (inputs
-- above the watermark, newly accepted blocks, unconsumed events), and fall
-- back to the F1g predicate when the backlog is too large.

CREATE TABLE unspent_tracking_events (
  id bigserial PRIMARY KEY,
  event_kind text NOT NULL,
  node_internal_id bigint NOT NULL,
  block_internal_id bigint,
  transaction_internal_id bigint,
  created_at timestamp NOT NULL DEFAULT now()
);
COMMENT ON TABLE unspent_tracking_events IS 'Experiment (E17, CHAINGRAPH_UNSPENT_TRACKING=deferred-*): append-only acceptance changes not yet consumed by the tracking job. event_kind: released (node_block / node_transaction row deleted) or accepted (agent re-acceptance paths). Not tracked by Hasura.';

CREATE TABLE unspent_tracking_progress (
  tracking_kind text NOT NULL,
  node_internal_id bigint NOT NULL,
  input_transaction_internal_id bigint NOT NULL,
  node_transaction_transaction_internal_id bigint NOT NULL,
  node_block_block_internal_id bigint NOT NULL,
  consumed_event_id bigint NOT NULL DEFAULT 0,
  updated_at timestamp NOT NULL DEFAULT now(),
  CONSTRAINT unspent_tracking_progress_pkey PRIMARY KEY (tracking_kind, node_internal_id)
);
COMMENT ON TABLE unspent_tracking_progress IS 'Experiment (E17): tracking job watermarks per tracking kind (marker, bitmask). node_internal_id 0 is the kind-wide row (input watermark, block watermark, highest consumed event id) read by the query roots; one row per node holds that node''s node_transaction / node_block watermarks. Not tracked by Hasura.';

CREATE TABLE unspent_tracking_watch (
  tracking_kind text NOT NULL,
  transaction_internal_id bigint NOT NULL,
  released_at_height bigint NOT NULL,
  accepted_node_bits bigint NOT NULL,
  CONSTRAINT unspent_tracking_watch_pkey PRIMARY KEY (tracking_kind, transaction_internal_id)
);
COMMENT ON TABLE unspent_tracking_watch IS 'Experiment (E17): transactions released recently (watch set), with their acceptance when last checked; re-checked by every job pass until 100 blocks after the release, so a re-acceptance through a path that writes no event is still applied. Not tracked by Hasura.';

-- Release events: statement-level, one INSERT ... SELECT per DELETE, whichever
-- code path deletes the rows (re-org, mempool drop, replacement, cascade,
-- mempool cleaning on block acceptance). Created disabled; the agent enables
-- them in the deferred modes.
CREATE FUNCTION trigger_unspent_deferred_node_block_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO unspent_tracking_events (event_kind, node_internal_id, block_internal_id)
    SELECT 'released', node_internal_id, block_internal_id FROM old_rows;
  RETURN NULL;
END;
$$;
CREATE FUNCTION trigger_unspent_deferred_node_transaction_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO unspent_tracking_events (event_kind, node_internal_id, transaction_internal_id)
    SELECT 'released', node_internal_id, transaction_internal_id FROM old_rows;
  RETURN NULL;
END;
$$;
CREATE TRIGGER trigger_unspent_deferred_node_block_delete AFTER DELETE ON node_block
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_deferred_node_block_delete();
CREATE TRIGGER trigger_unspent_deferred_node_transaction_delete AFTER DELETE ON node_transaction
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_deferred_node_transaction_delete();
ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_deferred_node_block_delete;
ALTER TABLE node_transaction DISABLE TRIGGER trigger_unspent_deferred_node_transaction_delete;

-- Current acceptance of a transaction: bit n set if node n (internal_id < 64)
-- accepts it, in a block or in its mempool.
CREATE FUNCTION unspent_deferred_acceptance_bits (transaction_internal_id bigint)
  RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT (SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0)
            FROM block_transaction bt
            CROSS JOIN node n
            JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
            WHERE bt.transaction_internal_id = $1 AND n.internal_id < 64)
       | (SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0)
            FROM node_transaction nt
            WHERE nt.transaction_internal_id = $1 AND nt.node_internal_id < 64)
$$;
-- marker: 1 if any node accepts the transaction, else 0.
CREATE FUNCTION unspent_deferred_any_acceptance (transaction_internal_id bigint)
  RETURNS bigint LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN EXISTS (
      SELECT 1 FROM block_transaction bt CROSS JOIN node n
        JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
        WHERE bt.transaction_internal_id = $1)
    OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = $1)
  THEN 1 ELSE 0 END::bigint
$$;

-- Recompute every output collected in pg_temp.unspent_deferred_affected from
-- scratch; only rows whose value changes are written, in key order.
-- Returns the number of rows changed.
CREATE FUNCTION unspent_deferred_recompute_marker ()
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET jit = off AS $$
DECLARE
  changed bigint;
BEGIN
  UPDATE output o SET spent_by_transaction_internal_id = v.marker
    FROM (SELECT a.h, a.i, COALESCE(s.tx, 0) AS marker
            FROM (SELECT DISTINCT h, i FROM pg_temp.unspent_deferred_affected ORDER BY h, i) a
            LEFT JOIN LATERAL (
              SELECT c.tx FROM (
                SELECT x.transaction_internal_id AS tx,
                       EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                                 JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                                 WHERE bt.transaction_internal_id = x.transaction_internal_id) AS in_block
                  FROM input x
                  WHERE x.outpoint_transaction_hash = a.h AND x.outpoint_index = a.i OFFSET 0) c
              WHERE c.in_block OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = c.tx)
              ORDER BY c.in_block DESC, c.tx
              LIMIT 1) s ON true
          OFFSET 0) v
    WHERE o.transaction_hash = v.h AND o.output_index = v.i
      AND o.spent_by_transaction_internal_id IS DISTINCT FROM v.marker;
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed;
END;
$$;

CREATE FUNCTION unspent_deferred_recompute_bits ()
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET jit = off AS $$
DECLARE
  changed bigint;
BEGIN
  UPDATE output o SET unspent_node_bits = v.bits
    FROM (SELECT a.h, a.i, (cr.bits & ~sp.bits) AS bits
            FROM (SELECT DISTINCT h, i FROM pg_temp.unspent_deferred_affected ORDER BY h, i) a
            CROSS JOIN LATERAL (
              SELECT internal_id FROM transaction WHERE transaction.hash = a.h OFFSET 0) ct
            CROSS JOIN LATERAL (
              SELECT (SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0)
                        FROM block_transaction bt
                        CROSS JOIN node n
                        JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                        WHERE bt.transaction_internal_id = ct.internal_id AND n.internal_id < 64)
                   | (SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0)
                        FROM node_transaction nt
                        WHERE nt.transaction_internal_id = ct.internal_id AND nt.node_internal_id < 64) AS bits) cr
            CROSS JOIN LATERAL (
              SELECT COALESCE(bit_or(
                       (SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0)
                          FROM block_transaction bt
                          CROSS JOIN node n
                          JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                          WHERE bt.transaction_internal_id = x.transaction_internal_id AND n.internal_id < 64)
                     | (SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0)
                          FROM node_transaction nt
                          WHERE nt.transaction_internal_id = x.transaction_internal_id AND nt.node_internal_id < 64)), 0) AS bits
                FROM (SELECT transaction_internal_id FROM input
                        WHERE input.outpoint_transaction_hash = a.h AND input.outpoint_index = a.i OFFSET 0) x) sp
          OFFSET 0) v
    WHERE o.transaction_hash = v.h AND o.output_index = v.i
      AND o.unspent_node_bits IS DISTINCT FROM v.bits;
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed;
END;
$$;

-- Start tracking for `kind` (no-op if already started): the watermarks start
-- at the given settled sequence values. Outputs of earlier transactions stay
-- NULL (pre-tracking, "unprocessed") until a sweep or backfill reaches them.
CREATE FUNCTION unspent_deferred_initialize (kind text, input_watermark bigint, block_watermark bigint)
  RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
  inserted bigint;
BEGIN
  IF kind NOT IN ('marker', 'bitmask') THEN
    RAISE EXCEPTION 'unknown tracking kind %', kind;
  END IF;
  INSERT INTO unspent_tracking_progress (tracking_kind, node_internal_id, input_transaction_internal_id,
      node_transaction_transaction_internal_id, node_block_block_internal_id, consumed_event_id)
    VALUES (kind, 0, input_watermark, input_watermark, block_watermark, 0)
    ON CONFLICT ON CONSTRAINT unspent_tracking_progress_pkey DO NOTHING;
  GET DIAGNOSTICS inserted = ROW_COUNT;
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

-- One job batch. Call inside a REPEATABLE READ transaction (one snapshot for
-- every step) and commit; `tx_limit` / `block_limit` must be settled sequence
-- values (every transaction that allocated an id at or below them has
-- finished). `skip_stall_through`: inputs of transactions up to this id whose
-- output row is missing no longer stop the batch (the agent passes the stalled
-- id once the stall exceeds its limit; 0 = never skip). `check_watch`: re-check
-- the watch set in this batch (the agent does it once per pass). Returns counters.
CREATE FUNCTION unspent_deferred_run_batch (kind text, tx_limit bigint, block_limit bigint,
  max_inputs integer, max_blocks integer, max_events integer, sweep_rows integer, skip_stall_through bigint,
  check_watch boolean DEFAULT true)
  RETURNS jsonb LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET jit = off AS $$
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
  max_event bigint;
  tip bigint;
BEGIN
  IF kind NOT IN ('marker', 'bitmask') THEN
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
      SELECT count(*) INTO skipped_inputs
        FROM unspent_deferred_inputs c
        WHERE c.tx <= skip_stall_through
          AND NOT EXISTS (SELECT 1 FROM output o WHERE o.transaction_hash = c.h AND o.output_index = c.i);
    END IF;
    SELECT count(*) INTO n_inputs FROM unspent_deferred_inputs;
    INSERT INTO unspent_deferred_affected (h, i) SELECT h, i FROM unspent_deferred_inputs;
    IF upper_tx > w THEN
      INSERT INTO unspent_deferred_affected (h, i)
        SELECT o.transaction_hash, o.output_index
          FROM transaction t
          CROSS JOIN LATERAL (SELECT transaction_hash, output_index FROM output
                                WHERE output.transaction_hash = t.hash OFFSET 0) o
          WHERE t.internal_id > w AND t.internal_id <= upper_tx;
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
  IF kind = 'bitmask' THEN
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
    ELSE
      INSERT INTO unspent_deferred_affected (h, i)
        SELECT o.transaction_hash, o.output_index
          FROM (SELECT transaction_hash, output_index FROM output
                  WHERE unspent_node_bits IS NULL AND token_category IS NOT NULL
                  ORDER BY token_category) o
          CROSS JOIN LATERAL (SELECT internal_id FROM transaction WHERE transaction.hash = o.transaction_hash OFFSET 0) t
          WHERE t.internal_id <= upper_tx
          LIMIT sweep_rows;
    END IF;
    GET DIAGNOSTICS n_sweep = ROW_COUNT;
  END IF;

  -- 7. recompute
  SELECT count(*) INTO n_affected FROM unspent_deferred_affected;
  IF n_affected > 0 THEN
    IF kind = 'marker' THEN
      n_changed := unspent_deferred_recompute_marker();
    ELSE
      n_changed := unspent_deferred_recompute_bits();
    END IF;
  END IF;

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
    'sweep', n_sweep, 'affected', n_affected, 'changed', n_changed);
END;
$$;

-- Backfill (golden reads, first query of a category): recompute every NULL
-- output of a token category / of a locking bytecode prefix (25 bytes, as
-- output_search_index), below the input watermark. Run in its own transaction.
CREATE FUNCTION unspent_deferred_backfill (kind text, category bytea, bytecode_prefix bytea)
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
  ELSE
    INSERT INTO unspent_deferred_affected (h, i)
      SELECT o.transaction_hash, o.output_index FROM output o
        WHERE o.unspent_node_bits IS NULL
          AND (category IS NULL OR o.token_category = category)
          AND (bytecode_prefix IS NULL OR substring(o.locking_bytecode, 0, 26) = bytecode_prefix);
    RETURN unspent_deferred_recompute_bits();
  END IF;
END;
$$;

-- Read side. The tier is chosen once per query from the watermarks (read in
-- the caller's snapshot): 'hash' (small backlog: the outpoints spent by
-- unprocessed / dirty transactions are hashed once), 'probe' (larger backlog:
-- one spent_by_index probe per stored-unspent row) or 'fallback' (backlog
-- above the thresholds or tracking not started: the F1g predicate).
-- `SET chaingraph.unspent_deferred_read_tier = hash|probe|fallback` forces a
-- tier (tests and measurements).
CREATE FUNCTION unspent_deferred_read_tier (kind text)
  RETURNS text LANGUAGE plpgsql STABLE AS $$
DECLARE
  forced text := current_setting('chaingraph.unspent_deferred_read_tier', true);
  w bigint;
  max_tx bigint;
  events bigint;
BEGIN
  SELECT input_transaction_internal_id INTO w FROM unspent_tracking_progress
    WHERE tracking_kind = kind AND node_internal_id = 0;
  IF w IS NULL THEN
    RETURN 'fallback';
  END IF;
  IF forced IN ('hash', 'probe', 'fallback') THEN
    RETURN forced;
  END IF;
  SELECT count(*) INTO events FROM (SELECT 1 FROM unspent_tracking_events LIMIT 10001) e;
  IF events > 10000 THEN
    RETURN 'fallback';
  END IF;
  SELECT max(internal_id) INTO max_tx FROM transaction;
  -- ~2.5 inputs per transaction: 800,000 transactions ~ 2 M inputs
  IF COALESCE(max_tx, 0) - w > 800000 THEN
    RETURN 'fallback';
  END IF;
  IF COALESCE(max_tx, 0) - w > 100000 THEN
    RETURN 'probe';
  END IF;
  RETURN 'hash';
END;
$$;

-- Transactions whose acceptance changed after the job's snapshot: those of
-- unconsumed events and of blocks above the block watermark.
CREATE FUNCTION unspent_deferred_dirty_transactions (kind text)
  RETURNS TABLE (id bigint) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off AS $$
DECLARE
  wb bigint;
BEGIN
  SELECT node_block_block_internal_id INTO wb FROM unspent_tracking_progress
    WHERE tracking_kind = kind AND node_internal_id = 0;
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

-- Outpoints spent by inputs the job has not processed (above the input
-- watermark) or by dirty transactions.
CREATE FUNCTION unspent_deferred_recent_spends (kind text)
  RETURNS TABLE (outpoint_transaction_hash bytea, outpoint_index bigint) LANGUAGE plpgsql STABLE ROWS 1000
  SET enable_hashjoin = off SET enable_mergejoin = off AS $$
DECLARE
  w bigint;
BEGIN
  SELECT input_transaction_internal_id INTO w FROM unspent_tracking_progress
    WHERE tracking_kind = kind AND node_internal_id = 0;
  IF w IS NULL THEN
    RETURN;
  END IF;
  RETURN QUERY
    SELECT i.outpoint_transaction_hash, i.outpoint_index FROM input i WHERE i.transaction_internal_id > w
    UNION ALL
    SELECT i.outpoint_transaction_hash, i.outpoint_index
      FROM unspent_deferred_dirty_transactions(kind) d
      CROSS JOIN LATERAL (SELECT input.outpoint_transaction_hash, input.outpoint_index FROM input
                            WHERE input.transaction_internal_id = d.id OFFSET 0) i
      WHERE d.id <= w;
END;
$$;

-- Hashes of dirty transactions (bitmask: their outputs' creator acceptance
-- may have changed).
CREATE FUNCTION unspent_deferred_dirty_creators (kind text)
  RETURNS TABLE (hash bytea) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off AS $$
BEGIN
  RETURN QUERY
    SELECT DISTINCT t.hash FROM unspent_deferred_dirty_transactions(kind) d
      CROSS JOIN LATERAL (SELECT transaction.hash FROM transaction WHERE transaction.internal_id = d.id OFFSET 0) t;
END;
$$;

-- Outpoints spent by transactions of unconsumed release events that are not
-- accepted any more (marker: by no node; bitmask: not by `node_name`).
CREATE FUNCTION unspent_deferred_released_spends (kind text, node_name text)
  RETURNS TABLE (outpoint_transaction_hash bytea, outpoint_index bigint) LANGUAGE plpgsql STABLE ROWS 100
  SET enable_hashjoin = off SET enable_mergejoin = off AS $$
DECLARE
  node_id bigint;
BEGIN
  SELECT n.internal_id INTO node_id FROM node n WHERE n.name = node_name;
  RETURN QUERY
    SELECT DISTINCT i.outpoint_transaction_hash, i.outpoint_index
      FROM (SELECT e.transaction_internal_id AS id FROM unspent_tracking_events e
              WHERE e.event_kind = 'released' AND e.transaction_internal_id IS NOT NULL
            UNION
            SELECT bt.transaction_internal_id
              FROM (SELECT DISTINCT e.block_internal_id FROM unspent_tracking_events e
                      WHERE e.event_kind = 'released' AND e.block_internal_id IS NOT NULL) b
              CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                                    WHERE block_transaction.block_internal_id = b.block_internal_id OFFSET 0) bt) r
      CROSS JOIN LATERAL (SELECT input.outpoint_transaction_hash, input.outpoint_index FROM input
                            WHERE input.transaction_internal_id = r.id OFFSET 0) i
      WHERE CASE WHEN kind = 'marker'
              THEN NOT (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                                  JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                                  WHERE bt.transaction_internal_id = r.id)
                        OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = r.id))
              ELSE NOT (EXISTS (SELECT 1 FROM block_transaction bt
                                  JOIN node_block nb ON nb.node_internal_id = node_id AND nb.block_internal_id = bt.block_internal_id
                                  WHERE bt.transaction_internal_id = r.id)
                        OR EXISTS (SELECT 1 FROM node_transaction nt
                                     WHERE nt.transaction_internal_id = r.id AND nt.node_internal_id = node_id))
            END;
END;
$$;

-- The deferred-marker query root: any-node spend semantics (as the stored
-- marker), the creating transaction checked for `node_name` (as F1g).
-- (a) marker = 0 rows (partial indexes WHERE marker = 0) with the creator
--     check; rows spent by an unprocessed or dirty input are re-checked live;
-- (b) NULL (unprocessed) rows, F1g;
-- (d) marker > 0 rows spent by a released transaction, F1g (driven by the
--     small event set, never by the caller's category);
-- (f) fallback: F1g.
CREATE FUNCTION unspent_output_deferred_marker (node_name text)
  RETURNS SETOF output LANGUAGE sql STABLE AS $$
  SELECT o.* FROM output o
  WHERE o.spent_by_transaction_internal_id = 0
    AND (SELECT unspent_deferred_read_tier('marker')) = 'hash'
    AND EXISTS (
      SELECT 1 FROM transaction t
      WHERE t.hash = o.transaction_hash
        AND (EXISTS (SELECT 1 FROM block_transaction bt
                     JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
                     WHERE bt.transaction_internal_id = t.internal_id
                       AND nb.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))
          OR EXISTS (SELECT 1 FROM node_transaction nt
                     WHERE nt.transaction_internal_id = t.internal_id
                       AND nt.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))))
    AND CASE
      WHEN (o.transaction_hash, o.output_index) IN (
        SELECT r.outpoint_transaction_hash, r.outpoint_index FROM unspent_deferred_recent_spends('marker') r)
      THEN NOT EXISTS (
        SELECT 1 FROM input i
        WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
          AND (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                       JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = i.transaction_internal_id)
            OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = i.transaction_internal_id)))
      ELSE true
    END
  UNION ALL
  SELECT o.* FROM output o
  WHERE o.spent_by_transaction_internal_id = 0
    AND (SELECT unspent_deferred_read_tier('marker')) = 'probe'
    AND EXISTS (
      SELECT 1 FROM transaction t
      WHERE t.hash = o.transaction_hash
        AND (EXISTS (SELECT 1 FROM block_transaction bt
                     JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
                     WHERE bt.transaction_internal_id = t.internal_id
                       AND nb.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))
          OR EXISTS (SELECT 1 FROM node_transaction nt
                     WHERE nt.transaction_internal_id = t.internal_id
                       AND nt.node_internal_id = (SELECT n.internal_id FROM node n WHERE n.name = node_name))))
    AND CASE
      WHEN EXISTS (
        SELECT 1 FROM input i
        WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
          AND (i.transaction_internal_id > (SELECT p.input_transaction_internal_id FROM unspent_tracking_progress p
                                              WHERE p.tracking_kind = 'marker' AND p.node_internal_id = 0)
            OR i.transaction_internal_id IN (SELECT d.id FROM unspent_deferred_dirty_transactions('marker') d))
          OFFSET 0)
      THEN NOT EXISTS (
        SELECT 1 FROM input i
        WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
          AND (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                       JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = i.transaction_internal_id)
            OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = i.transaction_internal_id)))
      ELSE true
    END
  UNION ALL
  SELECT o.* FROM output o
  WHERE o.spent_by_transaction_internal_id IS NULL
    AND (SELECT unspent_deferred_read_tier('marker')) <> 'fallback'
    AND CASE
      WHEN EXISTS (
        SELECT 1 FROM input i
        WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
          AND (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                       JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = i.transaction_internal_id)
            OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = i.transaction_internal_id)))
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
  SELECT o.* FROM unspent_deferred_released_spends('marker', node_name) r
  CROSS JOIN LATERAL (
    SELECT * FROM output
      WHERE output.transaction_hash = r.outpoint_transaction_hash AND output.output_index = r.outpoint_index OFFSET 0) o
  WHERE o.spent_by_transaction_internal_id > 0
    AND (SELECT unspent_deferred_read_tier('marker')) <> 'fallback'
    AND CASE
      WHEN EXISTS (
        SELECT 1 FROM input i
        WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
          AND (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                       JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = i.transaction_internal_id)
            OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = i.transaction_internal_id)))
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
  WHERE (SELECT unspent_deferred_read_tier('marker')) = 'fallback'
    AND CASE
      WHEN EXISTS (
        SELECT 1 FROM input i
        WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
          AND (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                       JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = i.transaction_internal_id)
            OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = i.transaction_internal_id)))
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
$$;

-- The deferred-bitmask query root: exact per node (F1g).
-- (a) rows with the node's bit (partial indexes WHERE unspent_node_bits <> 0);
--     rows spent by an unprocessed or dirty input, or created by a dirty
--     transaction, are re-checked with F1g;
-- (b) NULL (unprocessed) rows, F1g;
-- (d) rows without the node's bit spent by a released transaction or created
--     by a dirty transaction, F1g (driven by the small event sets);
-- (f) fallback: F1g.
CREATE FUNCTION unspent_output_deferred_bitmask (node_name text)
  RETURNS SETOF output LANGUAGE sql STABLE AS $$
  SELECT o.* FROM output o
  WHERE o.unspent_node_bits <> 0
    AND (o.unspent_node_bits & (SELECT 1::bigint << n.internal_id::integer FROM node n WHERE n.name = node_name)) <> 0
    AND (SELECT unspent_deferred_read_tier('bitmask')) = 'hash'
    AND CASE
      WHEN (o.transaction_hash, o.output_index) IN (
             SELECT r.outpoint_transaction_hash, r.outpoint_index FROM unspent_deferred_recent_spends('bitmask') r)
        OR o.transaction_hash IN (SELECT c.hash FROM unspent_deferred_dirty_creators('bitmask') c)
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
  WHERE o.unspent_node_bits <> 0
    AND (o.unspent_node_bits & (SELECT 1::bigint << n.internal_id::integer FROM node n WHERE n.name = node_name)) <> 0
    AND (SELECT unspent_deferred_read_tier('bitmask')) = 'probe'
    AND CASE
      WHEN EXISTS (
             SELECT 1 FROM input i
             WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
               AND (i.transaction_internal_id > (SELECT p.input_transaction_internal_id FROM unspent_tracking_progress p
                                                   WHERE p.tracking_kind = 'bitmask' AND p.node_internal_id = 0)
                 OR i.transaction_internal_id IN (SELECT d.id FROM unspent_deferred_dirty_transactions('bitmask') d))
          OFFSET 0)
        OR o.transaction_hash IN (SELECT c.hash FROM unspent_deferred_dirty_creators('bitmask') c)
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
  WHERE o.unspent_node_bits IS NULL
    AND (SELECT unspent_deferred_read_tier('bitmask')) <> 'fallback'
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
  SELECT o.* FROM unspent_deferred_released_spends('bitmask', node_name) r
  CROSS JOIN LATERAL (
    SELECT * FROM output
      WHERE output.transaction_hash = r.outpoint_transaction_hash AND output.output_index = r.outpoint_index OFFSET 0) o
  WHERE (o.unspent_node_bits & (SELECT 1::bigint << n.internal_id::integer FROM node n WHERE n.name = node_name)) = 0
    AND (SELECT unspent_deferred_read_tier('bitmask')) <> 'fallback'
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
  SELECT o.* FROM unspent_deferred_dirty_creators('bitmask') c
  CROSS JOIN LATERAL (
    SELECT * FROM output WHERE output.transaction_hash = c.hash OFFSET 0) o
  WHERE (o.unspent_node_bits & (SELECT 1::bigint << n.internal_id::integer FROM node n WHERE n.name = node_name)) = 0
    AND NOT ((o.transaction_hash, o.output_index) IN (
      SELECT r.outpoint_transaction_hash, r.outpoint_index FROM unspent_deferred_released_spends('bitmask', node_name) r))
    AND (SELECT unspent_deferred_read_tier('bitmask')) <> 'fallback'
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
  WHERE (SELECT unspent_deferred_read_tier('bitmask')) = 'fallback'
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
$$;

-- Generic name; the agent re-points it at the active kind at start-up.
CREATE FUNCTION unspent_output_deferred (node_name text)
  RETURNS SETOF output LANGUAGE sql STABLE AS $$
  SELECT * FROM unspent_output_deferred_marker(node_name)
$$;
