-- Visibility gate views, plan §3.2. The API reads only these views, never base tables.
--
-- Node-scoped views are parameterised: SELECT ... FROM cg.<view>(node = <node_internal_id>).
-- They show a row only if its commit_seq <= visible(n) (the node's watermark in cg.visibility) and
-- its commit was not aborted. Membership views then collapse signs: GROUP BY key HAVING sum(sign) > 0.
--
-- Node-agnostic views (block_v, transaction_v, block_transaction_v, output_v, input_v) carry no
-- acceptance facts. They show rows of seq <= visible(0) that were not aborted, plus rows above
-- visible(0) whose commit is 'committed' (the committed tail), so a stalled save on one network never
-- hides committed data from another.
--
-- Choices where the plan is silent (also in README.md):
--   * The parameter is the node's internal id ({node:UInt32}); the API layer resolves the GraphQL
--     node name to the id (node_v) and caches it. Views do not accept a name.
--   * The aborted-set subquery is not bounded by visible(n) (the set is tiny: crashes only).
--   * A missing visibility row means watermark 0: nothing is visible for that node.
--   * Non-key columns of collapsed rows use any() (+1/-1 rows carry identical values) or
--     argMaxIf(..., version, sign > 0) for per-acceptance timestamps.

CREATE VIEW IF NOT EXISTS cg.node_v AS
SELECT internal_id, name, protocol_version, user_agent, first_connected_at, latest_connection_began_at
FROM cg.node FINAL;

-- block-accepted(n, b)
CREATE VIEW IF NOT EXISTS cg.node_block_v AS
SELECT
    node_internal_id,
    block_internal_id,
    any(block_hash) AS block_hash,
    any(height) AS height,
    argMaxIf(accepted_at, version, sign > 0) AS accepted_at
FROM cg.node_block
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted')
GROUP BY node_internal_id, block_internal_id
HAVING sum(sign) > 0;

-- n's current mempool
CREATE VIEW IF NOT EXISTS cg.node_transaction_v AS
SELECT
    node_internal_id,
    transaction_internal_id,
    any(transaction_hash) AS transaction_hash,
    argMaxIf(validated_at, version, sign > 0) AS validated_at
FROM cg.node_transaction
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted')
GROUP BY node_internal_id, transaction_internal_id
HAVING sum(sign) > 0;

-- tx-accepted(n, t): one row per (tx, accepting block of n, or 0 = n's mempool)
CREATE VIEW IF NOT EXISTS cg.tx_acceptance_v AS
SELECT
    transaction_hash,
    node_internal_id,
    block_internal_id,
    any(transaction_internal_id) AS transaction_internal_id,
    any(height) AS height,
    argMaxIf(accepted_at, version, sign > 0) AS accepted_at
FROM cg.tx_acceptance
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted')
GROUP BY transaction_hash, node_internal_id, block_internal_id
HAVING sum(sign) > 0;

-- unspent(n, o), by category + commitment
CREATE VIEW IF NOT EXISTS cg.utxo_v AS
SELECT
    node_internal_id,
    token_category,
    nonfungible_token_commitment_key,
    transaction_hash,
    output_index,
    any(transaction_internal_id) AS transaction_internal_id,
    any(created_height) AS created_height,
    any(value_satoshis) AS value_satoshis,
    any(locking_bytecode) AS locking_bytecode,
    any(fungible_token_amount) AS fungible_token_amount,
    any(nonfungible_token_capability) AS nonfungible_token_capability,
    any(nonfungible_token_commitment) AS nonfungible_token_commitment
FROM cg.utxo
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted')
GROUP BY node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index
HAVING sum(sign) > 0;

-- unspent(n, o), by locking bytecode prefix
CREATE VIEW IF NOT EXISTS cg.utxo_by_script_v AS
SELECT
    node_internal_id,
    locking_bytecode_prefix,
    transaction_hash,
    output_index,
    any(transaction_internal_id) AS transaction_internal_id,
    any(created_height) AS created_height,
    any(value_satoshis) AS value_satoshis,
    any(locking_bytecode) AS locking_bytecode,
    any(token_category) AS token_category,
    any(fungible_token_amount) AS fungible_token_amount,
    any(nonfungible_token_capability) AS nonfungible_token_capability,
    any(nonfungible_token_commitment) AS nonfungible_token_commitment
FROM cg.utxo_by_script
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted')
GROUP BY node_internal_id, locking_bytecode_prefix, transaction_hash, output_index
HAVING sum(sign) > 0;

CREATE VIEW IF NOT EXISTS cg.node_block_history_v AS
SELECT node_internal_id, removed_at, block_internal_id, internal_id, accepted_at
FROM cg.node_block_history
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted');

CREATE VIEW IF NOT EXISTS cg.node_transaction_history_v AS
SELECT node_internal_id, transaction_internal_id, internal_id, validated_at, replaced_at
FROM cg.node_transaction_history
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted');

-- Node-agnostic views (no acceptance fields; checklist item 7: their names carry no node).
CREATE VIEW IF NOT EXISTS cg.block_v AS
SELECT * FROM cg.block
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted'))
   OR commit_seq IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'committed'
                     AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0));

CREATE VIEW IF NOT EXISTS cg.transaction_v AS
SELECT * FROM cg.transaction
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted'))
   OR commit_seq IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'committed'
                     AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0));

CREATE VIEW IF NOT EXISTS cg.block_transaction_v AS
SELECT * FROM cg.block_transaction
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted'))
   OR commit_seq IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'committed'
                     AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0));

CREATE VIEW IF NOT EXISTS cg.output_v AS
SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key FROM cg.output
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted'))
   OR commit_seq IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'committed'
                     AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0));

CREATE VIEW IF NOT EXISTS cg.input_v AS
SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key FROM cg.input
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'aborted'))
   OR commit_seq IN (SELECT commit_seq FROM cg.commit_log FINAL WHERE state = 'committed'
                     AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0));
