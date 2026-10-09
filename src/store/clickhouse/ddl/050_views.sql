-- Visibility gate views, plan §3.2; WP4 semantics in docs/clickhouse-port/wp4-commit-and-visibility.md.
-- The API reads only these views, never base tables.
--
-- Two families, identical except for where the watermark comes from:
--   * <name>_v(node = n)               convenience: reads visible(n) from cg.visibility per view.
--   * <name>_at(node = n, visible = W) pinned: the reader reads W once (readSnapshot) and passes it to every
--                                      view of one request, so a query joining two views sees one watermark
--                                      (no torn reads across views). W is clamped to the live watermark, so a
--                                      forged W can never expose an unresolved commit.
--   Node-agnostic views: <name>_v (live) and <name>_at(visible0 = V0, tail = [committed seqs above V0]).
--
-- A row is visible iff all of:
--   1. commit_seq <= the watermark (node views: visible(n); node-agnostic: visible(0), or the commit is
--      'committed' above it: the committed tail, so a stalled save on one network hides nothing elsewhere);
--   2. commit_seq is not void (cg.commit_void: aborted commits, including those aborted by recovery);
--   3. commit_seq is not fenced: epoch = commit_seq >> 40; if the epoch has a fence (it is older than the
--      current lease) then commit_seq <= its max_valid_seq. fence_max_seq is dense from epoch 1.
-- Membership views then collapse signs: GROUP BY key HAVING sum(sign) > 0.
-- A missing visibility row means watermark 0 (nothing visible). Querying a node view without `node` fails.
-- Non-key columns of collapsed rows use any() (+1/-1 rows carry identical values) or
-- argMaxIf(..., version, sign > 0) for per-acceptance timestamps.
-- Views are CREATE OR REPLACE so re-applying picks up amendments.


CREATE OR REPLACE VIEW cg.node_v AS
SELECT internal_id, name, protocol_version, user_agent, first_connected_at, latest_connection_began_at
FROM cg.node FINAL;

-- block-accepted(n, b)
CREATE OR REPLACE VIEW cg.node_block_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id,
    block_internal_id,
    any(block_hash) AS block_hash,
    any(height) AS height,
    argMaxIf(accepted_at, version, sign > 0) AS accepted_at
FROM cg.node_block
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, block_internal_id
HAVING sum(sign) > 0;

-- block-accepted(n, b)
CREATE OR REPLACE VIEW cg.node_block_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id,
    block_internal_id,
    any(block_hash) AS block_hash,
    any(height) AS height,
    argMaxIf(accepted_at, version, sign > 0) AS accepted_at
FROM cg.node_block
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, block_internal_id
HAVING sum(sign) > 0;

-- n's current mempool
CREATE OR REPLACE VIEW cg.node_transaction_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id,
    transaction_internal_id,
    any(transaction_hash) AS transaction_hash,
    argMaxIf(validated_at, version, sign > 0) AS validated_at
FROM cg.node_transaction
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, transaction_internal_id
HAVING sum(sign) > 0;

-- n's current mempool
CREATE OR REPLACE VIEW cg.node_transaction_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id,
    transaction_internal_id,
    any(transaction_hash) AS transaction_hash,
    argMaxIf(validated_at, version, sign > 0) AS validated_at
FROM cg.node_transaction
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, transaction_internal_id
HAVING sum(sign) > 0;

-- tx-accepted(n, t): one row per (tx, accepting block of n, or 0 = n's mempool)
CREATE OR REPLACE VIEW cg.tx_acceptance_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
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
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY transaction_hash, node_internal_id, block_internal_id
HAVING sum(sign) > 0;

-- tx-accepted(n, t): one row per (tx, accepting block of n, or 0 = n's mempool)
CREATE OR REPLACE VIEW cg.tx_acceptance_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    transaction_hash,
    node_internal_id,
    block_internal_id,
    any(transaction_internal_id) AS transaction_internal_id,
    any(height) AS height,
    argMaxIf(accepted_at, version, sign > 0) AS accepted_at
FROM cg.tx_acceptance
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY transaction_hash, node_internal_id, block_internal_id
HAVING sum(sign) > 0;

-- unspent(n, o), by category + commitment
CREATE OR REPLACE VIEW cg.utxo_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
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
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index
HAVING sum(sign) > 0;

-- unspent(n, o), by category + commitment
CREATE OR REPLACE VIEW cg.utxo_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
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
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index
HAVING sum(sign) > 0;

-- unspent(n, o), by locking bytecode prefix
CREATE OR REPLACE VIEW cg.utxo_by_script_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
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
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, locking_bytecode_prefix, transaction_hash, output_index
HAVING sum(sign) > 0;

-- unspent(n, o), by locking bytecode prefix
CREATE OR REPLACE VIEW cg.utxo_by_script_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
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
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)))
GROUP BY node_internal_id, locking_bytecode_prefix, transaction_hash, output_index
HAVING sum(sign) > 0;

-- per-node block history
CREATE OR REPLACE VIEW cg.node_block_history_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id, removed_at, block_internal_id, internal_id, accepted_at
FROM cg.node_block_history
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

-- per-node block history
CREATE OR REPLACE VIEW cg.node_block_history_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id, removed_at, block_internal_id, internal_id, accepted_at
FROM cg.node_block_history
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

-- per-node transaction history (replaced_at NULL = confirmed)
CREATE OR REPLACE VIEW cg.node_transaction_history_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id, transaction_internal_id, internal_id, validated_at, replaced_at
FROM cg.node_transaction_history
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32})
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

-- per-node transaction history (replaced_at NULL = confirmed)
CREATE OR REPLACE VIEW cg.node_transaction_history_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT
    node_internal_id, transaction_internal_id, internal_id, validated_at, replaced_at
FROM cg.node_transaction_history
WHERE node_internal_id = {node:UInt32}
  AND commit_seq <= least({visible:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = {node:UInt32}))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

-- Node-agnostic views (no acceptance fields; checklist item 7: their names carry no node).

CREATE OR REPLACE VIEW cg.block_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT * FROM cg.block
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.block_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT * FROM cg.block
WHERE (commit_seq <= least({visible0:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0))
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND has({tail:Array(UInt64)}, commit_seq)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.transaction_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT * FROM cg.transaction
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.transaction_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT * FROM cg.transaction
WHERE (commit_seq <= least({visible0:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0))
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND has({tail:Array(UInt64)}, commit_seq)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.block_transaction_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT * FROM cg.block_transaction
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.block_transaction_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT * FROM cg.block_transaction
WHERE (commit_seq <= least({visible0:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0))
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND has({tail:Array(UInt64)}, commit_seq)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.output_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key FROM cg.output
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.output_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key FROM cg.output
WHERE (commit_seq <= least({visible0:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0))
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND has({tail:Array(UInt64)}, commit_seq)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.input_v AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key FROM cg.input
WHERE (commit_seq <= (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND commit_seq > (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));

CREATE OR REPLACE VIEW cg.input_at AS
WITH
    (SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM cg.epoch_fence GROUP BY epoch)) AS fence_max_seq
SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key FROM cg.input
WHERE (commit_seq <= least({visible0:UInt64}, (SELECT max(visible_seq) FROM cg.visibility WHERE node_internal_id = 0))
       OR commit_seq IN (SELECT commit_seq FROM cg.commit_log
                         WHERE state = 'committed' AND has({tail:Array(UInt64)}, commit_seq)))
  AND commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)
  AND (bitShiftRight(commit_seq, 40) > length(fence_max_seq)
       OR commit_seq <= arrayElement(fence_max_seq, bitShiftRight(commit_seq, 40)));
