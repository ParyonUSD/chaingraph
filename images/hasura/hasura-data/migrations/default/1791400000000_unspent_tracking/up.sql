-- Experiment (E15-B): a stored "unspent" read model maintained by the agent,
-- selected with CHAINGRAPH_UNSPENT_TRACKING=off|marker|settable.
--
-- Both designs track the spend side only, across all nodes: an output counts as
-- spent while ANY node accepts a spender (in an accepted block or in its
-- mempool). The "created by an accepted transaction" half of the unspent test
-- stays a per-row check on the (few) survivors, as in unspent_output (F1g).
--
-- marker: output.spent_by_transaction_internal_id
--   NULL = unaudited (rows that existed before the migration, or written in
--          `off` mode), 0 = unspent, > 0 = internal_id of an accepted spender.
--   ADD COLUMN without a default only touches the catalog (no table rewrite).
--   The agent writes 0 for new outputs and sets the spender in the block's DB
--   transaction; the triggers below put it back to 0 (or to another accepted
--   spender) when a spender loses its last acceptance (reorg, mempool drop).
--
-- settable: unspent_output_set, one row per unspent output (no backfill: rows
--   exist only for outputs created after tracking started); `output` stays
--   append-only.
ALTER TABLE output ADD COLUMN spent_by_transaction_internal_id bigint;
COMMENT ON COLUMN output.spent_by_transaction_internal_id IS 'Experiment (CHAINGRAPH_UNSPENT_TRACKING=marker): NULL = unaudited, 0 = unspent, > 0 = internal_id of a transaction spending this output that some node accepts (in an accepted block or in its mempool).';

CREATE INDEX output_unspent_token_category_index ON output USING btree (token_category)
  WHERE spent_by_transaction_internal_id = 0;
CREATE INDEX output_unspent_search_index ON output USING btree (substring(locking_bytecode, 0, 26))
  WHERE spent_by_transaction_internal_id = 0;

CREATE TABLE unspent_output_set (
  transaction_hash bytea NOT NULL,
  output_index bigint NOT NULL,
  token_category bytea,
  locking_bytecode_prefix bytea NOT NULL,
  CONSTRAINT unspent_output_set_pkey PRIMARY KEY (transaction_hash, output_index)
);
COMMENT ON TABLE unspent_output_set IS 'Experiment (CHAINGRAPH_UNSPENT_TRACKING=settable): outputs (created after tracking started) with no spender accepted by any node. locking_bytecode_prefix = substring(locking_bytecode, 0, 26), as output_search_index.';
CREATE INDEX unspent_output_set_token_category_index ON unspent_output_set USING btree (token_category);
CREATE INDEX unspent_output_set_prefix_index ON unspent_output_set USING btree (locking_bytecode_prefix);

-- true if any node accepts the transaction: in a block the node accepts, or in
-- its mempool. node_block's key is (node_internal_id, block_internal_id), so
-- the block test probes it once per node.
CREATE FUNCTION unspent_tracking_transaction_is_accepted (transaction_internal_id bigint)
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (
      SELECT 1 FROM block_transaction bt
        CROSS JOIN node n
        JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
        WHERE bt.transaction_internal_id = $1)
    OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = $1)
$$;

-- true if any node accepts the transaction in a block (mempool acceptance not
-- counted): a block-accepted spender is preferred over a mempool-only one.
CREATE FUNCTION unspent_tracking_transaction_is_block_accepted (transaction_internal_id bigint)
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (
      SELECT 1 FROM block_transaction bt
        CROSS JOIN node n
        JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
        WHERE bt.transaction_internal_id = $1)
$$;

-- Called when acceptance rows of `spender_ids` were deleted: for each spender no
-- node accepts any more, release the outputs it spent, unless another accepted
-- spender exists (one probe of spent_by_index per released outpoint).
CREATE FUNCTION unspent_tracking_release_spenders (spender_ids bigint[], mode text)
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  IF mode = 'marker' THEN
    WITH released AS (
      SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
        FROM (SELECT DISTINCT unnest(spender_ids) AS id) s
        JOIN input i ON i.transaction_internal_id = s.id
        WHERE NOT unspent_tracking_transaction_is_accepted(s.id)
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
      SELECT i.outpoint_transaction_hash, i.outpoint_index, i.transaction_internal_id
        FROM (SELECT DISTINCT unnest(spender_ids) AS id) s
        JOIN input i ON i.transaction_internal_id = s.id
        WHERE NOT unspent_tracking_transaction_is_accepted(s.id)
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

-- Statement-level, so they run once per DELETE (bounded by the reorg or drop
-- size), in the same DB transaction, whichever code path deletes the rows: the
-- agent (stale blocks, expiry, archive) or the mempool triggers (replacement,
-- descendant cascade). Created disabled; the agent enables the pair for its mode.
CREATE FUNCTION trigger_unspent_tracking_node_block_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  PERFORM unspent_tracking_release_spenders(
    ARRAY(SELECT DISTINCT bt.transaction_internal_id
            FROM (SELECT DISTINCT block_internal_id FROM old_rows) b
            JOIN block_transaction bt ON bt.block_internal_id = b.block_internal_id),
    TG_ARGV[0]);
  RETURN NULL;
END;
$$;
CREATE FUNCTION trigger_unspent_tracking_node_transaction_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  PERFORM unspent_tracking_release_spenders(
    ARRAY(SELECT DISTINCT transaction_internal_id FROM old_rows), TG_ARGV[0]);
  RETURN NULL;
END;
$$;
CREATE TRIGGER trigger_unspent_marker_node_block_delete AFTER DELETE ON node_block
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_tracking_node_block_delete('marker');
CREATE TRIGGER trigger_unspent_marker_node_transaction_delete AFTER DELETE ON node_transaction
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_tracking_node_transaction_delete('marker');
CREATE TRIGGER trigger_unspent_settable_node_block_delete AFTER DELETE ON node_block
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_tracking_node_block_delete('settable');
CREATE TRIGGER trigger_unspent_settable_node_transaction_delete AFTER DELETE ON node_transaction
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_tracking_node_transaction_delete('settable');
ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_marker_node_block_delete;
ALTER TABLE node_transaction DISABLE TRIGGER trigger_unspent_marker_node_transaction_delete;
ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_settable_node_block_delete;
ALTER TABLE node_transaction DISABLE TRIGGER trigger_unspent_settable_node_transaction_delete;

-- bitmask (CHAINGRAPH_UNSPENT_TRACKING=bitmask): per-node unspent bits.
--   NULL = unaudited; bit n (1 << node internal_id, ids < 64) = unspent for node n:
--   created by a transaction node n accepts AND no spender node n accepts.
-- All updates are relative (| and & ~), so concurrent saves need no lock.
-- Per-node partial indexes are created by the agent at start-up (one pair per node).
ALTER TABLE output ADD COLUMN unspent_node_bits bigint;
COMMENT ON COLUMN output.unspent_node_bits IS 'Experiment (CHAINGRAPH_UNSPENT_TRACKING=bitmask): NULL = unaudited; bit n set = unspent for the node with internal_id n (created by a transaction that node accepts, no spender that node accepts).';

CREATE FUNCTION unspent_tracking_accepted_by_node (transaction_internal_id bigint, node_internal_id bigint)
  RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS (
      SELECT 1 FROM block_transaction bt
        JOIN node_block nb ON nb.node_internal_id = $2 AND nb.block_internal_id = bt.block_internal_id
        WHERE bt.transaction_internal_id = $1)
    OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = $1 AND nt.node_internal_id = $2)
$$;

-- Node `node` starts accepting the given transactions: transactions it already
-- accepts are skipped; set its bit on their outputs, then clear it on the
-- outputs they spend (same-block create-and-spend ends cleared). Call it before
-- recording the acceptance, or after recording it with the newly accepted
-- blocks in `exclude_block_ids` (acceptance through them is ignored).
CREATE FUNCTION unspent_bits_accept (node bigint, transaction_hashes bytea[], exclude_block_ids bigint[] DEFAULT '{}')
  RETURNS void LANGUAGE plpgsql AS $$
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
    WHERE o.transaction_hash = h.hash AND (o.unspent_node_bits & bit) = 0;
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~bit
    FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index
            FROM unnest(accepted_ids) x(id) JOIN input i ON i.transaction_internal_id = x.id) s
    WHERE o.transaction_hash = s.outpoint_transaction_hash AND o.output_index = s.outpoint_index
      AND (o.unspent_node_bits & bit) <> 0;
END;
$$;

-- POLICY A for bitmask (call AFTER recording the acceptance): clear node's bit on
-- outputs of the given (new) transactions that already have a spender the node
-- accepts (child-before-parent). One spent_by_index probe per transaction.
CREATE FUNCTION unspent_bits_resolve (node bigint, transaction_hashes bytea[])
  RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  bit bigint := (1::bigint << node::integer);
BEGIN
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~bit
    FROM (SELECT DISTINCT i.outpoint_transaction_hash, i.outpoint_index
            FROM unnest(transaction_hashes) h(hash)
            CROSS JOIN LATERAL (
              SELECT outpoint_transaction_hash, outpoint_index, transaction_internal_id FROM input
                WHERE input.outpoint_transaction_hash = h.hash OFFSET 0) i
            WHERE unspent_tracking_accepted_by_node(i.transaction_internal_id, node)) s
    WHERE o.transaction_hash = s.outpoint_transaction_hash AND o.output_index = s.outpoint_index
      AND (o.unspent_node_bits & bit) <> 0;
END;
$$;

-- Loss of acceptance of (node, transaction) pairs: for pairs the node no longer
-- accepts at all, clear the node's bit on the transaction's outputs, then set it
-- on the outputs it spent unless the node accepts another spender, and only if
-- the node accepts the creating transaction.
CREATE FUNCTION unspent_bits_release (node_ids bigint[], transaction_ids bigint[])
  RETURNS void LANGUAGE plpgsql AS $$
BEGIN
  CREATE TEMP TABLE IF NOT EXISTS unspent_bits_lost (node bigint, transaction_internal_id bigint) ON COMMIT DELETE ROWS;
  DELETE FROM unspent_bits_lost;
  INSERT INTO unspent_bits_lost
    SELECT DISTINCT u.node, u.tx FROM unnest(node_ids, transaction_ids) u(node, tx)
      WHERE u.node < 64 AND NOT unspent_tracking_accepted_by_node(u.tx, u.node);
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits & ~(1::bigint << l.node::integer)
    FROM unspent_bits_lost l JOIN transaction t ON t.internal_id = l.transaction_internal_id
    WHERE o.transaction_hash = t.hash AND (o.unspent_node_bits & (1::bigint << l.node::integer)) <> 0;
  UPDATE output o SET unspent_node_bits = o.unspent_node_bits | s.bits
    FROM (SELECT i.outpoint_transaction_hash, i.outpoint_index, bit_or(1::bigint << l.node::integer) AS bits
            FROM unspent_bits_lost l JOIN input i ON i.transaction_internal_id = l.transaction_internal_id
            WHERE NOT EXISTS (
                SELECT 1 FROM input other
                  WHERE other.outpoint_transaction_hash = i.outpoint_transaction_hash
                    AND other.outpoint_index = i.outpoint_index
                    AND other.transaction_internal_id <> i.transaction_internal_id
                    AND unspent_tracking_accepted_by_node(other.transaction_internal_id, l.node))
              AND EXISTS (
                SELECT 1 FROM transaction ct
                  WHERE ct.hash = i.outpoint_transaction_hash
                    AND unspent_tracking_accepted_by_node(ct.internal_id, l.node))
            GROUP BY 1, 2) s
    WHERE o.transaction_hash = s.outpoint_transaction_hash AND o.output_index = s.outpoint_index;
END;
$$;

CREATE FUNCTION trigger_unspent_bits_node_block_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  PERFORM unspent_bits_release(array_agg(p.node), array_agg(p.tx))
    FROM (SELECT DISTINCT o.node_internal_id AS node, bt.transaction_internal_id AS tx
            FROM old_rows o JOIN block_transaction bt ON bt.block_internal_id = o.block_internal_id) p
    HAVING count(*) > 0;
  RETURN NULL;
END;
$$;
CREATE FUNCTION trigger_unspent_bits_node_transaction_delete() RETURNS trigger
  LANGUAGE plpgsql AS $$
BEGIN
  PERFORM unspent_bits_release(array_agg(node_internal_id), array_agg(transaction_internal_id))
    FROM old_rows HAVING count(*) > 0;
  RETURN NULL;
END;
$$;
CREATE TRIGGER trigger_unspent_bitmask_node_block_delete AFTER DELETE ON node_block
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_bits_node_block_delete();
CREATE TRIGGER trigger_unspent_bitmask_node_transaction_delete AFTER DELETE ON node_transaction
  REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT
  EXECUTE FUNCTION trigger_unspent_bits_node_transaction_delete();
ALTER TABLE node_block DISABLE TRIGGER trigger_unspent_bitmask_node_block_delete;
ALTER TABLE node_transaction DISABLE TRIGGER trigger_unspent_bitmask_node_transaction_delete;
