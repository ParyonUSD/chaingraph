CREATE OR REPLACE FUNCTION trigger_node_block_insert() RETURNS trigger
  LANGUAGE plpgsql
AS $$
DECLARE
  inserted_node_ids integer[];
  inserted_block_ids bigint[];
  inserted_timestamps timestamp without time zone[];
  confirmed_node_ids integer[];
  confirmed_transaction_ids bigint[];
  confirmed_timestamps timestamp without time zone[];
  spent_node_ids integer[];
  spent_transaction_ids bigint[];
  spent_timestamps timestamp without time zone[];
  spent_outpoint_hashes bytea[];
  spent_outpoint_indexes bigint[];
BEGIN
  /*
   * Keep each lookup bounded by the rows in this statement. OFFSET 0 prevents
   * Postgres from flattening the per-row lookups into scans of the historical
   * block_transaction and input tables.
   */
  SELECT array_agg(new_table.node_internal_id ORDER BY new_table.node_internal_id, new_table.block_internal_id),
         array_agg(new_table.block_internal_id ORDER BY new_table.node_internal_id, new_table.block_internal_id),
         array_agg(new_table.accepted_at ORDER BY new_table.node_internal_id, new_table.block_internal_id)
    INTO inserted_node_ids, inserted_block_ids, inserted_timestamps
    FROM new_table;

  SELECT array_agg(inserted.node_internal_id ORDER BY inserted.ordinality, block_transaction.transaction_internal_id),
         array_agg(block_transaction.transaction_internal_id ORDER BY inserted.ordinality, block_transaction.transaction_internal_id),
         array_agg(inserted.accepted_at ORDER BY inserted.ordinality, block_transaction.transaction_internal_id)
    INTO confirmed_node_ids, confirmed_transaction_ids, confirmed_timestamps
    FROM unnest(inserted_node_ids, inserted_block_ids, inserted_timestamps)
      WITH ORDINALITY AS inserted(node_internal_id, block_internal_id, accepted_at, ordinality)
    CROSS JOIN LATERAL (
      SELECT block_transaction.transaction_internal_id
        FROM block_transaction
        WHERE block_transaction.block_internal_id = inserted.block_internal_id
        OFFSET 0
    ) AS block_transaction;

  IF confirmed_transaction_ids IS NULL THEN
    RETURN NEW;
  END IF;

  SELECT array_agg(accepted.node_internal_id ORDER BY accepted.ordinality, input.input_index),
         array_agg(accepted.transaction_internal_id ORDER BY accepted.ordinality, input.input_index),
         array_agg(accepted.accepted_at ORDER BY accepted.ordinality, input.input_index),
         array_agg(input.outpoint_transaction_hash ORDER BY accepted.ordinality, input.input_index),
         array_agg(input.outpoint_index ORDER BY accepted.ordinality, input.input_index)
    INTO spent_node_ids, spent_transaction_ids, spent_timestamps,
         spent_outpoint_hashes, spent_outpoint_indexes
    FROM unnest(confirmed_node_ids, confirmed_transaction_ids, confirmed_timestamps)
      WITH ORDINALITY AS accepted(node_internal_id, transaction_internal_id, accepted_at, ordinality)
    CROSS JOIN LATERAL (
      SELECT input.input_index,
             input.outpoint_transaction_hash,
             input.outpoint_index
        FROM input
        WHERE input.transaction_internal_id = accepted.transaction_internal_id
          AND input.outpoint_transaction_hash != '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
        OFFSET 0
    ) AS input;

  IF spent_transaction_ids IS NULL THEN
    RETURN NEW;
  END IF;

  WITH spent_outpoints AS MATERIALIZED (
      SELECT spent.node_internal_id,
             spent.transaction_internal_id,
             spent.accepted_at,
             spent.outpoint_transaction_hash,
             spent.outpoint_index
        FROM unnest(
          spent_node_ids,
          spent_transaction_ids,
          spent_timestamps,
          spent_outpoint_hashes,
          spent_outpoint_indexes
        ) AS spent(
          node_internal_id,
          transaction_internal_id,
          accepted_at,
          outpoint_transaction_hash,
          outpoint_index
        )
  ),
  archive_candidates AS (
      SELECT spent_outpoints.node_internal_id,
             conflicting.transaction_internal_id,
             bool_or(conflicting.transaction_internal_id = spent_outpoints.transaction_internal_id) AS confirmed,
             min(spent_outpoints.accepted_at) FILTER (
               WHERE conflicting.transaction_internal_id != spent_outpoints.transaction_internal_id
             ) AS replaced_at
        FROM spent_outpoints
        CROSS JOIN LATERAL (
          SELECT input.transaction_internal_id
            FROM input
            WHERE input.outpoint_transaction_hash = spent_outpoints.outpoint_transaction_hash
              AND input.outpoint_index = spent_outpoints.outpoint_index
            OFFSET 0
        ) AS conflicting
        GROUP BY spent_outpoints.node_internal_id, conflicting.transaction_internal_id
  ),
  deleted_node_transactions AS (
      DELETE FROM node_transaction
        USING archive_candidates
        WHERE node_transaction.node_internal_id = archive_candidates.node_internal_id
          AND node_transaction.transaction_internal_id = archive_candidates.transaction_internal_id
        RETURNING node_transaction.node_internal_id,
                  node_transaction.transaction_internal_id,
                  node_transaction.validated_at,
                  CASE WHEN archive_candidates.confirmed
                    THEN NULL::timestamp without time zone
                    ELSE archive_candidates.replaced_at
                  END AS replaced_at
  )
  INSERT INTO node_transaction_history (
    node_internal_id, transaction_internal_id, validated_at, replaced_at
  )
    SELECT node_internal_id, transaction_internal_id, validated_at, replaced_at
      FROM deleted_node_transactions;

  RETURN NEW;
END;
$$;
