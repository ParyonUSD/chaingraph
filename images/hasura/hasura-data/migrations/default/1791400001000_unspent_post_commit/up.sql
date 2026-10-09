-- Experiment (E16), on top of 1791400000000_unspent_tracking:
--
-- 1. Post-commit pass (all modes). The agent saves up to 16 blocks at once.
--    Under READ COMMITTED a child's spend statement cannot see a parent's
--    uncommitted outputs, and the parent's POLICY A resolve cannot see the
--    child's uncommitted inputs, so both miss. After each save commits, the
--    agent runs unspent_tracking_post_commit() for that save's transactions in
--    a short follow-up transaction: (a) resolve its new outputs against now
--    committed accepted spenders; (b) re-apply the spend of every outpoint its
--    inputs spend. Whichever side commits second sees the other. Both
--    statements are idempotent and only touch rows that are still wrong.
--    Every statement locks its rows in (transaction_hash, output_index) order
--    (a sorted, fenced driver of a nested loop), so a parent's and a child's
--    passes cannot deadlock on each other's rows.
--
-- 2. Plans pinned to keyed probes. With production statistics the planner
--    estimates ~1,830 inputs and ~1,100 outputs per transaction (actual ~2.5),
--    so joins driven by an unnest() of a large block's transactions are planned
--    as hash joins over sequential scans of input and output. Every function
--    here (and the reworked bitmask functions) drives each lookup through a
--    LATERAL ... OFFSET 0 probe and disables hash/merge joins and sequential
--    scans for its own statements. Per-row acceptance probes on hot paths are
--    written inline: the unspent_tracking_*accepted* SQL functions contain
--    sub-queries, so Postgres never inlines them and every call pays an
--    executor start-up (~70 us; 100,000 spenders = 7 s on the gate).

-- bitmask, POLICY A for all nodes in one statement: on the outputs of the given
-- transactions, clear the bit of every node that accepts one of their spenders.
-- Returns the number of outputs changed.
CREATE FUNCTION unspent_bits_resolve_outputs (transaction_hashes bytea[])
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  changed bigint;
BEGIN
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~s.clear_bits
    FROM (SELECT c.outpoint_transaction_hash, c.outpoint_index,
                 bit_or(in_blocks.bits | in_mempools.bits) AS clear_bits
            FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
                    FROM unnest(transaction_hashes) h(hash)
                    CROSS JOIN LATERAL (
                      SELECT outpoint_transaction_hash, outpoint_index, transaction_internal_id FROM input
                        WHERE input.outpoint_transaction_hash = h.hash OFFSET 0) i
                    CROSS JOIN LATERAL (
                      SELECT 1 FROM output
                        WHERE output.transaction_hash = i.outpoint_transaction_hash
                          AND output.output_index = i.outpoint_index
                          AND output.unspent_node_bits <> 0 OFFSET 0) still_set
                  OFFSET 0) c
            -- unspent_tracking_accepting_bits(), inlined (a SQL function with
            -- sub-queries is not inlined: one executor start-up per call)
            CROSS JOIN LATERAL (
              SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0) AS bits
                FROM block_transaction bt
                CROSS JOIN node n
                JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                WHERE bt.transaction_internal_id = c.transaction_internal_id AND n.internal_id < 64) in_blocks
            CROSS JOIN LATERAL (
              SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0) AS bits
                FROM node_transaction nt
                WHERE nt.transaction_internal_id = c.transaction_internal_id AND nt.node_internal_id < 64) in_mempools
            GROUP BY c.outpoint_transaction_hash, c.outpoint_index
            ORDER BY c.outpoint_transaction_hash, c.outpoint_index OFFSET 0) s
    WHERE o.transaction_hash = s.outpoint_transaction_hash
      AND o.output_index = s.outpoint_index
      AND (o.unspent_node_bits & s.clear_bits) <> 0;
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed;
END;
$$;

-- The post-commit pass for one save (see 1. above). `transaction_ids`: the
-- save's transactions (all transactions of a block, or a mempool transaction).
-- `block_path`: in marker mode a block-accepted spender replaces a mempool-only
-- one (as in the block save); otherwise only unset markers are filled.
-- Returns the rows fixed by (a) and (b).
CREATE FUNCTION unspent_tracking_post_commit (mode text, transaction_ids bigint[], block_path boolean,
  OUT new_outputs_fixed bigint, OUT spent_outputs_fixed bigint)
  LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  hashes bytea[];
BEGIN
  new_outputs_fixed := 0;
  spent_outputs_fixed := 0;
  IF transaction_ids IS NULL OR cardinality(transaction_ids) = 0 THEN
    RETURN;
  END IF;
  SELECT array_agg(t.hash) INTO hashes
    FROM unnest(transaction_ids) x(id)
    CROSS JOIN LATERAL (SELECT hash FROM transaction WHERE transaction.internal_id = x.id OFFSET 0) t;
  IF mode = 'marker' THEN
    -- (a) new outputs still marked unspent (0) that have an accepted spender
    UPDATE output o SET spent_by_transaction_internal_id = s.spender_internal_id
      FROM (SELECT DISTINCT ON (c.outpoint_transaction_hash, c.outpoint_index)
                   c.outpoint_transaction_hash, c.outpoint_index, c.transaction_internal_id AS spender_internal_id
              FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
                      FROM unnest(hashes) h(hash)
                      CROSS JOIN LATERAL (
                        SELECT outpoint_transaction_hash, outpoint_index, transaction_internal_id FROM input
                          WHERE input.outpoint_transaction_hash = h.hash OFFSET 0) i
                      CROSS JOIN LATERAL (
                        SELECT 1 FROM output
                          WHERE output.transaction_hash = i.outpoint_transaction_hash
                            AND output.output_index = i.outpoint_index
                            AND output.spent_by_transaction_internal_id = 0 OFFSET 0) still_unspent
                    OFFSET 0) c
              WHERE unspent_tracking_transaction_is_accepted(c.transaction_internal_id)
              ORDER BY c.outpoint_transaction_hash, c.outpoint_index,
                unspent_tracking_transaction_is_block_accepted(c.transaction_internal_id) DESC OFFSET 0) s
      WHERE o.transaction_hash = s.outpoint_transaction_hash
        AND o.output_index = s.outpoint_index
        AND o.spent_by_transaction_internal_id = 0;
    GET DIAGNOSTICS new_outputs_fixed = ROW_COUNT;
    -- (b) outpoints spent by these transactions whose marker missed the spend
    -- (the spender must still be accepted: a re-org may have released it)
    UPDATE output o SET spent_by_transaction_internal_id = i.transaction_internal_id
      FROM (SELECT spends.transaction_internal_id, spends.outpoint_transaction_hash, spends.outpoint_index
              FROM unnest(transaction_ids) x(id)
              CROSS JOIN LATERAL (
                SELECT transaction_internal_id, outpoint_transaction_hash, outpoint_index FROM input
                  WHERE input.transaction_internal_id = x.id OFFSET 0) spends
              ORDER BY spends.outpoint_transaction_hash, spends.outpoint_index OFFSET 0) i
      WHERE o.transaction_hash = i.outpoint_transaction_hash
        AND o.output_index = i.outpoint_index
        AND CASE
          WHEN o.spent_by_transaction_internal_id = i.transaction_internal_id THEN false
          WHEN o.spent_by_transaction_internal_id IS NULL OR o.spent_by_transaction_internal_id = 0
            THEN unspent_tracking_transaction_is_accepted(i.transaction_internal_id)
          WHEN block_path
            THEN NOT unspent_tracking_transaction_is_block_accepted(o.spent_by_transaction_internal_id)
              AND unspent_tracking_transaction_is_block_accepted(i.transaction_internal_id)
          ELSE false
        END;
    GET DIAGNOSTICS spent_outputs_fixed = ROW_COUNT;
  ELSIF mode = 'settable' THEN
    -- (a) set rows of new outputs that have an accepted spender
    DELETE FROM unspent_output_set u
      USING (SELECT c.outpoint_transaction_hash, c.outpoint_index
               FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
                       FROM unnest(hashes) h(hash)
                       CROSS JOIN LATERAL (
                         SELECT outpoint_transaction_hash, outpoint_index, transaction_internal_id FROM input
                           WHERE input.outpoint_transaction_hash = h.hash OFFSET 0) i
                       CROSS JOIN LATERAL (
                         SELECT 1 FROM unspent_output_set r
                           WHERE r.transaction_hash = i.outpoint_transaction_hash
                             AND r.output_index = i.outpoint_index OFFSET 0) still_in_set
                     OFFSET 0) c
               WHERE unspent_tracking_transaction_is_accepted(c.transaction_internal_id)
               ORDER BY c.outpoint_transaction_hash, c.outpoint_index OFFSET 0) s
      WHERE u.transaction_hash = s.outpoint_transaction_hash
        AND u.output_index = s.outpoint_index;
    GET DIAGNOSTICS new_outputs_fixed = ROW_COUNT;
    -- (b) set rows of outpoints spent by these (still accepted) transactions
    DELETE FROM unspent_output_set u
      USING (SELECT spends.transaction_internal_id, spends.outpoint_transaction_hash, spends.outpoint_index
               FROM unnest(transaction_ids) x(id)
               CROSS JOIN LATERAL (
                 SELECT transaction_internal_id, outpoint_transaction_hash, outpoint_index FROM input
                   WHERE input.transaction_internal_id = x.id OFFSET 0) spends
               ORDER BY spends.outpoint_transaction_hash, spends.outpoint_index OFFSET 0) i
      WHERE u.transaction_hash = i.outpoint_transaction_hash
        AND u.output_index = i.outpoint_index
        AND CASE WHEN u.transaction_hash IS NULL THEN false
          ELSE unspent_tracking_transaction_is_accepted(i.transaction_internal_id) END;
    GET DIAGNOSTICS spent_outputs_fixed = ROW_COUNT;
  ELSIF mode = 'bitmask' THEN
    -- (a)
    new_outputs_fixed := unspent_bits_resolve_outputs(hashes);
    -- (b) outpoints spent by these transactions still carrying the bit of a
    -- node that accepts the spender
    UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~s.clear_bits
      FROM (SELECT c.outpoint_transaction_hash, c.outpoint_index,
                   bit_or(in_blocks.bits | in_mempools.bits) AS clear_bits
              FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
                      FROM unnest(transaction_ids) x(id)
                      CROSS JOIN LATERAL (
                        SELECT transaction_internal_id, outpoint_transaction_hash, outpoint_index FROM input
                          WHERE input.transaction_internal_id = x.id OFFSET 0) i
                      CROSS JOIN LATERAL (
                        SELECT 1 FROM output
                          WHERE output.transaction_hash = i.outpoint_transaction_hash
                            AND output.output_index = i.outpoint_index
                            AND output.unspent_node_bits <> 0 OFFSET 0) still_set
                    OFFSET 0) c
              CROSS JOIN LATERAL (
                SELECT COALESCE(bit_or(1::bigint << n.internal_id::integer), 0) AS bits
                  FROM block_transaction bt
                  CROSS JOIN node n
                  JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                  WHERE bt.transaction_internal_id = c.transaction_internal_id AND n.internal_id < 64) in_blocks
              CROSS JOIN LATERAL (
                SELECT COALESCE(bit_or(1::bigint << nt.node_internal_id::integer), 0) AS bits
                  FROM node_transaction nt
                  WHERE nt.transaction_internal_id = c.transaction_internal_id AND nt.node_internal_id < 64) in_mempools
              GROUP BY c.outpoint_transaction_hash, c.outpoint_index
              ORDER BY c.outpoint_transaction_hash, c.outpoint_index OFFSET 0) s
      WHERE o.transaction_hash = s.outpoint_transaction_hash
        AND o.output_index = s.outpoint_index
        AND (o.unspent_node_bits & s.clear_bits) <> 0;
    GET DIAGNOSTICS spent_outputs_fixed = ROW_COUNT;
  END IF;
END;
$$;

-- Wrappers the agent calls after COMMIT.
CREATE FUNCTION unspent_tracking_post_commit_blocks (mode text, block_hashes bytea[],
  OUT new_outputs_fixed bigint, OUT spent_outputs_fixed bigint)
  LANGUAGE sql AS $$
  SELECT * FROM unspent_tracking_post_commit(mode, ARRAY(
    SELECT bt.transaction_internal_id
      FROM unnest(block_hashes) h(hash)
      CROSS JOIN LATERAL (SELECT internal_id FROM block WHERE block.hash = h.hash OFFSET 0) b
      CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                            WHERE block_transaction.block_internal_id = b.internal_id OFFSET 0) bt), true)
$$;
CREATE FUNCTION unspent_tracking_post_commit_transactions (mode text, transaction_hashes bytea[],
  OUT new_outputs_fixed bigint, OUT spent_outputs_fixed bigint)
  LANGUAGE sql AS $$
  SELECT * FROM unspent_tracking_post_commit(mode, ARRAY(
    SELECT t.internal_id
      FROM unnest(transaction_hashes) h(hash)
      CROSS JOIN LATERAL (SELECT internal_id FROM transaction WHERE transaction.hash = h.hash OFFSET 0) t), false)
$$;

-- Post-commit pass for losing acceptance (marker/settable): the release
-- triggers decide "no node accepts this spender any more" inside the deleting
-- transaction, so two nodes dropping the same block (or mempool transaction)
-- concurrently each still see the other's acceptance and neither releases.
-- After the delete commits, the agent re-runs the (idempotent) release for the
-- same transactions. bitmask releases per node and needs no pass.
CREATE FUNCTION unspent_tracking_post_commit_release (mode text, transaction_ids bigint[])
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF mode IN ('marker', 'settable') AND cardinality(transaction_ids) > 0 THEN
    PERFORM unspent_tracking_release_spenders(transaction_ids, mode);
  END IF;
END;
$$;
CREATE FUNCTION unspent_tracking_post_commit_release_blocks (mode text, block_hashes bytea[])
  RETURNS void LANGUAGE sql AS $$
  SELECT unspent_tracking_post_commit_release(mode, ARRAY(
    SELECT bt.transaction_internal_id
      FROM unnest(block_hashes) h(hash)
      CROSS JOIN LATERAL (SELECT internal_id FROM block WHERE block.hash = h.hash OFFSET 0) b
      CROSS JOIN LATERAL (SELECT transaction_internal_id FROM block_transaction
                            WHERE block_transaction.block_internal_id = b.internal_id OFFSET 0) bt))
$$;

-- bitmask, block save: clear the accepting nodes' bits (`mask`) on every
-- outpoint the saved transactions spend; keyed like the marker's spend
-- statement, one statement for all nodes. Call after inserting the block's
-- transactions. Returns the number of outputs changed.
CREATE FUNCTION unspent_bits_clear_spent (mask bigint, transaction_ids bigint[])
  RETURNS bigint LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  changed bigint;
BEGIN
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~mask
    FROM (SELECT spends.outpoint_transaction_hash, spends.outpoint_index
            FROM unnest(transaction_ids) x(id)
            CROSS JOIN LATERAL (
              SELECT outpoint_transaction_hash, outpoint_index FROM input
                WHERE input.transaction_internal_id = x.id OFFSET 0) spends
            ORDER BY spends.outpoint_transaction_hash, spends.outpoint_index OFFSET 0) i
    WHERE o.transaction_hash = i.outpoint_transaction_hash
      AND o.output_index = i.outpoint_index
      AND (o.unspent_node_bits & mask) <> 0;
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed;
END;
$$;

-- Re-created with keyed probes and pinned plans (same semantics as in
-- 1791400000000_unspent_tracking).
CREATE OR REPLACE FUNCTION unspent_bits_accept (node bigint, transaction_hashes bytea[], exclude_block_ids bigint[] DEFAULT '{}')
  RETURNS void LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
DECLARE
  bit bigint := (1::bigint << node::integer);
  accepted_hashes bytea[];
  accepted_ids bigint[];
BEGIN
  SELECT array_agg(t.hash), array_agg(t.internal_id) INTO accepted_hashes, accepted_ids
    FROM unnest(transaction_hashes) h(hash)
    CROSS JOIN LATERAL (SELECT internal_id, hash FROM transaction WHERE transaction.hash = h.hash OFFSET 0) t
    WHERE NOT EXISTS (
        SELECT 1 FROM block_transaction bt
          JOIN node_block nb ON nb.node_internal_id = node AND nb.block_internal_id = bt.block_internal_id
          WHERE bt.transaction_internal_id = t.internal_id AND NOT (bt.block_internal_id = ANY (exclude_block_ids)))
      AND NOT EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = t.internal_id AND nt.node_internal_id = node);
  IF accepted_ids IS NULL THEN RETURN; END IF;
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits | bit
    FROM unnest(accepted_hashes) h(hash)
    CROSS JOIN LATERAL (
      SELECT transaction_hash, output_index FROM output
        WHERE output.transaction_hash = h.hash AND (output.unspent_node_bits & bit) = 0 OFFSET 0) k
    WHERE o.transaction_hash = k.transaction_hash AND o.output_index = k.output_index
      AND (o.unspent_node_bits & bit) = 0;
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~bit
    FROM unnest(accepted_ids) x(id)
    CROSS JOIN LATERAL (
      SELECT outpoint_transaction_hash, outpoint_index FROM input
        WHERE input.transaction_internal_id = x.id OFFSET 0) i
    WHERE o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index
      AND (o.unspent_node_bits & bit) <> 0;
END;
$$;

-- POLICY A per node, now the all-node statement (clearing the bits of every
-- node that accepts a spender is a superset of clearing `node`'s bit, and
-- equally correct).
CREATE OR REPLACE FUNCTION unspent_bits_resolve (node bigint, transaction_hashes bytea[])
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  PERFORM unspent_bits_resolve_outputs(transaction_hashes);
END;
$$;

CREATE OR REPLACE FUNCTION unspent_bits_release (node_ids bigint[], transaction_ids bigint[])
  RETURNS void LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS unspent_bits_lost (node bigint, transaction_internal_id bigint) ON COMMIT DELETE ROWS;
  DELETE FROM unspent_bits_lost;
  -- (node, transaction) pairs the node no longer accepts at all; the
  -- acceptance probes are inlined (unspent_tracking_accepted_by_node() has
  -- sub-queries, so it is never inlined: one executor start-up per call)
  INSERT INTO unspent_bits_lost
    SELECT DISTINCT u.node, u.tx FROM unnest(node_ids, transaction_ids) u(node, tx)
      WHERE u.node < 64
        AND NOT EXISTS (
          SELECT 1 FROM block_transaction bt
            JOIN node_block nb ON nb.node_internal_id = u.node AND nb.block_internal_id = bt.block_internal_id
            WHERE bt.transaction_internal_id = u.tx)
        AND NOT EXISTS (
          SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = u.tx AND nt.node_internal_id = u.node);
  -- one statement for all nodes: per transaction, the bits of the nodes that lost it
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~l.bits
    FROM (SELECT transaction_internal_id, bit_or(1::bigint << node::integer) AS bits
            FROM unspent_bits_lost GROUP BY transaction_internal_id) l
    CROSS JOIN LATERAL (SELECT hash FROM transaction WHERE transaction.internal_id = l.transaction_internal_id OFFSET 0) t
    CROSS JOIN LATERAL (
      SELECT transaction_hash, output_index FROM output
        WHERE output.transaction_hash = t.hash AND (output.unspent_node_bits & l.bits) <> 0 OFFSET 0) k
    WHERE o.transaction_hash = k.transaction_hash AND o.output_index = k.output_index
      AND (o.unspent_node_bits & l.bits) <> 0;
  -- set the bit back on the outpoints the lost transactions spent, only if the
  -- node accepts the creator (checked first: in a re-org the creator is often
  -- lost too) and accepts no other spender
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits | s.bits
    FROM (SELECT c.outpoint_transaction_hash, c.outpoint_index, bit_or(1::bigint << c.node::integer) AS bits
            FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id, l.node
                    FROM unspent_bits_lost l
                    CROSS JOIN LATERAL (
                      SELECT transaction_internal_id, outpoint_transaction_hash, outpoint_index FROM input
                        WHERE input.transaction_internal_id = l.transaction_internal_id OFFSET 0) i
                    CROSS JOIN LATERAL (
                      SELECT 1 FROM output
                        WHERE output.transaction_hash = i.outpoint_transaction_hash
                          AND output.output_index = i.outpoint_index
                          AND (output.unspent_node_bits & (1::bigint << l.node::integer)) = 0 OFFSET 0) bit_clear
                    CROSS JOIN LATERAL (
                      SELECT internal_id FROM transaction WHERE transaction.hash = i.outpoint_transaction_hash OFFSET 0) ct
                    WHERE (EXISTS (
                            SELECT 1 FROM block_transaction bt
                              JOIN node_block nb ON nb.node_internal_id = l.node AND nb.block_internal_id = bt.block_internal_id
                              WHERE bt.transaction_internal_id = ct.internal_id)
                        OR EXISTS (
                            SELECT 1 FROM node_transaction nt
                              WHERE nt.transaction_internal_id = ct.internal_id AND nt.node_internal_id = l.node))
                  OFFSET 0) c
            WHERE NOT EXISTS (
                SELECT 1 FROM input other
                  WHERE other.outpoint_transaction_hash = c.outpoint_transaction_hash
                    AND other.outpoint_index = c.outpoint_index
                    AND other.transaction_internal_id <> c.transaction_internal_id
                    AND (EXISTS (
                          SELECT 1 FROM block_transaction bt
                            JOIN node_block nb ON nb.node_internal_id = c.node AND nb.block_internal_id = bt.block_internal_id
                            WHERE bt.transaction_internal_id = other.transaction_internal_id)
                      OR EXISTS (
                          SELECT 1 FROM node_transaction nt
                            WHERE nt.transaction_internal_id = other.transaction_internal_id AND nt.node_internal_id = c.node)))
            GROUP BY 1, 2
            ORDER BY 1, 2 OFFSET 0) s
    WHERE o.transaction_hash = s.outpoint_transaction_hash AND o.output_index = s.outpoint_index;
END;
$$;

-- Re-created: rows whose marker still names the spender (or, for settable,
-- outpoints missing from the set) are found first, and only those pay the
-- acceptance probe, written inline (unspent_tracking_transaction_is_accepted()
-- is never inlined: one executor start-up per call). So the post-commit re-run
-- of a release that the trigger already applied costs keyed lookups only.
-- Rows are locked in (transaction_hash, output_index) order.
CREATE OR REPLACE FUNCTION unspent_tracking_release_spenders (spender_ids bigint[], mode text)
  RETURNS void LANGUAGE plpgsql
  SET enable_hashjoin = off SET enable_mergejoin = off SET enable_seqscan = off SET jit = off AS $$
BEGIN
  IF mode = 'marker' THEN
    WITH released AS (
      SELECT c.outpoint_transaction_hash, c.outpoint_index, c.transaction_internal_id
        FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
                FROM (SELECT DISTINCT unnest(spender_ids) AS id) s
                CROSS JOIN LATERAL (
                  SELECT outpoint_transaction_hash, outpoint_index, transaction_internal_id FROM input
                    WHERE input.transaction_internal_id = s.id OFFSET 0) i
                CROSS JOIN LATERAL (
                  SELECT 1 FROM output
                    WHERE output.transaction_hash = i.outpoint_transaction_hash
                      AND output.output_index = i.outpoint_index
                      AND output.spent_by_transaction_internal_id = i.transaction_internal_id OFFSET 0) still_marked
              OFFSET 0) c
        WHERE NOT (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                       JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = c.transaction_internal_id)
               OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = c.transaction_internal_id))
        ORDER BY c.outpoint_transaction_hash, c.outpoint_index
    )
    UPDATE output o SET spent_by_transaction_internal_id = COALESCE((
        SELECT other.transaction_internal_id FROM input other
          WHERE other.outpoint_transaction_hash = o.transaction_hash
            AND other.outpoint_index = o.output_index
            AND other.transaction_internal_id <> released.transaction_internal_id
            AND unspent_tracking_transaction_is_accepted(other.transaction_internal_id)
          ORDER BY unspent_tracking_transaction_is_block_accepted(other.transaction_internal_id) DESC
          LIMIT 1), 0)
      FROM released
      WHERE o.transaction_hash = released.outpoint_transaction_hash
        AND o.output_index = released.outpoint_index
        AND o.spent_by_transaction_internal_id = released.transaction_internal_id;
  ELSIF mode = 'settable' THEN
    WITH released AS (
      SELECT c.outpoint_transaction_hash, c.outpoint_index, c.transaction_internal_id
        FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
                FROM (SELECT DISTINCT unnest(spender_ids) AS id) s
                CROSS JOIN LATERAL (
                  SELECT outpoint_transaction_hash, outpoint_index, transaction_internal_id FROM input
                    WHERE input.transaction_internal_id = s.id OFFSET 0) i
                WHERE NOT EXISTS (
                  SELECT 1 FROM unspent_output_set r
                    WHERE r.transaction_hash = i.outpoint_transaction_hash AND r.output_index = i.outpoint_index)
              OFFSET 0) c
        WHERE NOT (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
                       JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
                       WHERE bt.transaction_internal_id = c.transaction_internal_id)
               OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = c.transaction_internal_id))
        ORDER BY c.outpoint_transaction_hash, c.outpoint_index
    )
    INSERT INTO unspent_output_set (transaction_hash, output_index, token_category, locking_bytecode_prefix)
      SELECT o.transaction_hash, o.output_index, o.token_category, substring(o.locking_bytecode, 0, 26)
        FROM released
        JOIN output o ON o.transaction_hash = released.outpoint_transaction_hash
          AND o.output_index = released.outpoint_index
        WHERE NOT EXISTS (
          SELECT 1 FROM input other
            WHERE other.outpoint_transaction_hash = o.transaction_hash
              AND other.outpoint_index = o.output_index
              AND other.transaction_internal_id <> released.transaction_internal_id
              AND unspent_tracking_transaction_is_accepted(other.transaction_internal_id))
      ON CONFLICT ON CONSTRAINT unspent_output_set_pkey DO NOTHING;
  END IF;
END;
$$;
