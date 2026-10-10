-- Projections, plan §2.1 and §2.4. Separate from the CREATE TABLEs so Phase 1 can drop/vary them.
-- All are full-column (contiguous reads) except where the plan excludes unlocking_bytecode.
-- ADD PROJECTION on an empty table needs no MATERIALIZE; on a filled table run
-- ALTER TABLE ... MATERIALIZE PROJECTION <name> afterwards (a mutation).
-- MATERIALIZED columns are listed explicitly because SELECT * omits them.
-- output's projections set their own granularity and block sizes (the table itself is tuned for
-- point lookups by outpoint, 010_core.sql); they keep the table defaults of the other tables.

ALTER TABLE cg.block ADD PROJECTION IF NOT EXISTS p_height
(
    SELECT * ORDER BY height
);

ALTER TABLE cg.transaction ADD PROJECTION IF NOT EXISTS p_id
(
    SELECT * ORDER BY internal_id
);

ALTER TABLE cg.block_transaction ADD PROJECTION IF NOT EXISTS p_tx
(
    SELECT * ORDER BY transaction_hash
);

ALTER TABLE cg.output ADD PROJECTION IF NOT EXISTS p_script
(
    SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key
    ORDER BY (locking_bytecode_prefix, transaction_hash, output_index)
) WITH SETTINGS (index_granularity = 1024, min_compress_block_size = 65536, max_compress_block_size = 1048576);

ALTER TABLE cg.output ADD PROJECTION IF NOT EXISTS p_category
(
    SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key
    ORDER BY (token_category, nonfungible_token_commitment_key, transaction_hash, output_index)
) WITH SETTINGS (index_granularity = 1024, min_compress_block_size = 65536, max_compress_block_size = 1048576);

ALTER TABLE cg.input ADD PROJECTION IF NOT EXISTS p_outpoint
(
    SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index,
           sequence_number, value_satoshis, token_category, fungible_token_amount, nonfungible_token_capability,
           nonfungible_token_commitment, locking_bytecode, commit_seq, locking_bytecode_prefix,
           nonfungible_token_commitment_key
    ORDER BY (outpoint_transaction_hash, outpoint_index)
);

ALTER TABLE cg.input ADD PROJECTION IF NOT EXISTS p_spent_script
(
    SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index,
           sequence_number, value_satoshis, token_category, fungible_token_amount, nonfungible_token_capability,
           nonfungible_token_commitment, locking_bytecode, commit_seq, locking_bytecode_prefix,
           nonfungible_token_commitment_key
    ORDER BY (locking_bytecode_prefix, transaction_hash, input_index)
);

ALTER TABLE cg.input ADD PROJECTION IF NOT EXISTS p_spent_category
(
    SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index,
           sequence_number, value_satoshis, token_category, fungible_token_amount, nonfungible_token_capability,
           nonfungible_token_commitment, locking_bytecode, commit_seq, locking_bytecode_prefix,
           nonfungible_token_commitment_key
    ORDER BY (token_category, nonfungible_token_commitment_key, transaction_hash, input_index)
);

ALTER TABLE cg.tx_acceptance ADD PROJECTION IF NOT EXISTS p_node_height
(
    SELECT * ORDER BY (node_internal_id, height, transaction_internal_id)
);
