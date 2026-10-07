CREATE OR REPLACE FUNCTION trigger_node_block_insert() RETURNS trigger
  LANGUAGE plpgsql
AS $$
BEGIN
WITH accepted_transactions AS (
	SELECT node_internal_id, transaction_internal_id, accepted_at FROM block_transaction INNER JOIN new_table ON block_transaction.block_internal_id = new_table.block_internal_id
),
newly_spent AS (
	SELECT accepted_transactions.node_internal_id, accepted_transactions.transaction_internal_id, accepted_transactions.accepted_at, input.outpoint_transaction_hash, input.outpoint_index
		FROM input INNER JOIN accepted_transactions ON input.transaction_internal_id = accepted_transactions.transaction_internal_id
),
accepted_and_replaced_transactions AS (
    SELECT newly_spent.node_internal_id, input.transaction_internal_id, CASE WHEN input.transaction_internal_id != newly_spent.transaction_internal_id THEN newly_spent.accepted_at ELSE NULL END AS replaced_at
        FROM input INNER JOIN newly_spent ON input.outpoint_transaction_hash = newly_spent.outpoint_transaction_hash AND input.outpoint_index = newly_spent.outpoint_index
        WHERE input.outpoint_transaction_hash != '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
),
deleted_node_transactions AS (
    DELETE FROM node_transaction
        USING accepted_and_replaced_transactions
        WHERE node_transaction.node_internal_id = accepted_and_replaced_transactions.node_internal_id
            AND node_transaction.transaction_internal_id = accepted_and_replaced_transactions.transaction_internal_id
        RETURNING node_transaction.node_internal_id, node_transaction.transaction_internal_id, validated_at, accepted_and_replaced_transactions.replaced_at
)
INSERT INTO node_transaction_history (node_internal_id, transaction_internal_id, validated_at, replaced_at)
	SELECT node_internal_id, transaction_internal_id, validated_at, replaced_at FROM deleted_node_transactions;
RETURN NEW;
END;
$$;
