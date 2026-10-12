-- The stored per-node UTXO set, maintained off the commit path (owner decision 2026-10-12, item 3).
-- Design, invariants, read rule and measurements: docs/clickhouse-port/utxo-async.md. Code: utxo-set.ts.
--
-- utxo_set: signed rows per (node, generation), tagged with the batch (a commit seq) that wrote them. The node's
-- set at a published batch end U of generation g is GROUP BY outpoint HAVING sum(sign) > 0 over the rows of
-- (n, g) with batch <= U; every outpoint's sum there is 0 or 1. A generation starts with the whole set at its base
-- (a bulk build or a rebase: sign +1, batch = base) or empty (replayed from seq 0); later batches add change rows
-- (+1 entered, -1 left). Plain MergeTree: no merge ever cancels rows, so a reader holding an older U (an older
-- snapshot) still reads exactly the set at U.
--
-- utxo_total: the same rows summed per (node, generation, batch, token category) by the materialized view
-- utxo_total_mv (count, value, fungible count and amount, non-fungible count); whole-set and per-category totals
-- read a few thousand rows.
--
-- utxo_watermark: one row per published batch end (and per bulk build / rebase). A snapshot takes, per node, the
-- newest (watermark, generation) at or below its visible(n) (visibility.ts snapshotSql); readers add the change
-- rows of (U, visible(n)] computed at query time, so answers are exact whatever the maintainer's lag.
--
-- Choices:
--   * Partitioned by (node, generation): an old generation is dropped as whole partitions (DROP PARTITION) some
--     minutes after its successor was published; a crashed build or rebase (generation above the published one) too.
--   * Keys stop at the filter columns (planning-floor.md): primary key (token_category, commitment key); projection
--     p_script by locking_bytecode_prefix. Rows sort by the outpoint within them.
--   * mm_batch: the restart cleanup (`DELETE WHERE batch > U`) finds the unfinished batch's parts by minmax.
--   * node_block / node_transaction get a minmax index on commit_seq: the change rows of (U, visible(n)] start
--     from the node's acceptance rows in that seq range (the newest granules; block ids grow with time).

CREATE TABLE IF NOT EXISTS cg.utxo_set
(
    node_internal_id                  UInt32,
    generation                        UInt32,
    batch                             UInt64,
    token_category                    FixedString(32),
    transaction_hash                  FixedString(32),
    output_index                      UInt32,
    transaction_internal_id           UInt64,
    value_satoshis                    Int64,
    locking_bytecode                  String,
    fungible_token_amount             Nullable(Int64),
    nonfungible_token_capability      Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3)),
    nonfungible_token_commitment      Nullable(String),
    sign                              Int8,
    locking_bytecode_prefix           String MATERIALIZED substring(locking_bytecode, 1, 25),
    nonfungible_token_commitment_key  String MATERIALIZED ifNull(nonfungible_token_commitment, ''),
    INDEX mm_batch batch TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY (node_internal_id, generation)
PRIMARY KEY (token_category, nonfungible_token_commitment_key)
ORDER BY (token_category, nonfungible_token_commitment_key, transaction_hash, output_index)
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

ALTER TABLE cg.utxo_set ADD PROJECTION IF NOT EXISTS p_script
(
    SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key
    ORDER BY locking_bytecode_prefix
);

CREATE TABLE IF NOT EXISTS cg.utxo_total
(
    node_internal_id       UInt32,
    generation             UInt32,
    batch                  UInt64,
    token_category         FixedString(32),
    outputs                Int64,
    value_satoshis         Int64,
    fungible_outputs       Int64,
    fungible_token_amount  Int128,
    nonfungible_outputs    Int64
)
ENGINE = MergeTree
PARTITION BY (node_internal_id, generation)
ORDER BY (token_category, batch)
SETTINGS non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

CREATE MATERIALIZED VIEW IF NOT EXISTS cg.utxo_total_mv TO cg.utxo_total AS
SELECT node_internal_id, generation, batch, token_category,
       sum(toInt64(s)) AS outputs,
       sum(s * v) AS value_satoshis,
       sum(if(isNull(f), 0, toInt64(s))) AS fungible_outputs,
       sum(toInt128(s) * ifNull(f, 0)) AS fungible_token_amount,
       sum(if(isNull(c), 0, toInt64(s))) AS nonfungible_outputs
FROM (SELECT node_internal_id, generation, batch, token_category, sign AS s, value_satoshis AS v,
             fungible_token_amount AS f, nonfungible_token_capability AS c
      FROM cg.utxo_set)
GROUP BY node_internal_id, generation, batch, token_category;

CREATE TABLE IF NOT EXISTS cg.utxo_watermark
(
    node_internal_id  UInt32,
    watermark         UInt64,
    generation        UInt32,
    batch_from        UInt64,
    rows              Int64,
    kind              LowCardinality(String),
    writer_epoch      UInt64,
    filled_at         DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (node_internal_id, watermark, generation)
SETTINGS non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

-- Work table of a bulk build (utxo-set.ts `build`): the outpoints spent by a node's accepted transactions, written per
-- slice of spender hashes and read per slice of outpoint hashes, so no statement holds a node's whole accepted or
-- spent set. Emptied when the build ends (DROP PARTITION) and at the maintainer's start.
CREATE TABLE IF NOT EXISTS cg.utxo_build_spent
(
    node_internal_id           UInt32,
    generation                 UInt32,
    outpoint_transaction_hash  FixedString(32),
    outpoint_index             UInt32
)
ENGINE = MergeTree
PARTITION BY (node_internal_id, generation)
ORDER BY (outpoint_transaction_hash, outpoint_index)
SETTINGS non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

ALTER TABLE cg.node_block ADD INDEX IF NOT EXISTS mm_commit_seq commit_seq TYPE minmax GRANULARITY 1;

ALTER TABLE cg.node_transaction ADD INDEX IF NOT EXISTS mm_commit_seq commit_seq TYPE minmax GRANULARITY 1;

-- The node's set at a snapshot for ad-hoc readers (GENERATED by utxo-set.ts utxoSetViewStatement; utxo-set.spec.ts checks it):
-- parameters node, visible, fence, void, visible0, tail and the snapshot's utxo (U) and utxo_generation.
CREATE OR REPLACE VIEW cg.utxo_set_at AS
WITH (SELECT groupUniqArray(transaction_hash) FROM (
      SELECT transaction_hash FROM cg.block_transaction
      WHERE block_internal_id IN (SELECT block_internal_id FROM cg.node_block WHERE commit_seq > {utxo:UInt64} AND node_internal_id = {node:UInt32} AND commit_seq <= {visible:UInt64} AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)))
        AND (commit_seq <= {visible0:UInt64} OR has({tail:Array(UInt64)}, commit_seq)) AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void))
      UNION ALL
      SELECT transaction_hash FROM cg.node_transaction WHERE commit_seq > {utxo:UInt64} AND node_internal_id = {node:UInt32} AND commit_seq <= {visible:UInt64} AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)))) AS utxo_touched_1,
(SELECT groupArray(transaction_hash) FROM (SELECT transaction_hash, max(b) AS b, max(a) AS a FROM (
    SELECT transaction_hash, sumIf(sign, commit_seq <= {utxo:UInt64}) > 0 AS b, sum(sign) > 0 AS a
    FROM cg.tx_acceptance
    WHERE transaction_hash IN (SELECT arrayJoin(utxo_touched_1)) AND node_internal_id = {node:UInt32} AND commit_seq <= {visible:UInt64} AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void))
    GROUP BY transaction_hash, block_internal_id)
  GROUP BY transaction_hash) WHERE b != a) AS utxo_changed_2,
(SELECT arrayFilter(x -> x.1 != toFixedString(unhex(repeat('00', 32)), 32), groupUniqArray((h, i))) FROM (
      SELECT transaction_hash AS h, output_index AS i FROM cg.output
      WHERE transaction_hash IN (SELECT arrayJoin(utxo_changed_2)) AND (commit_seq <= {visible0:UInt64} OR has({tail:Array(UInt64)}, commit_seq)) AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void))
      UNION ALL
      SELECT outpoint_transaction_hash AS h, outpoint_index AS i FROM cg.input
      WHERE transaction_hash IN (SELECT arrayJoin(utxo_changed_2)) AND (commit_seq <= {visible0:UInt64} OR has({tail:Array(UInt64)}, commit_seq)) AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void)))) AS utxo_affected_3,
(SELECT groupUniqArray((outpoint_transaction_hash, outpoint_index, transaction_hash)) FROM cg.input
      WHERE (outpoint_transaction_hash, outpoint_index) IN (SELECT tupleElement(__x, 1) AS h, tupleElement(__x, 2) AS i FROM (SELECT arrayJoin(utxo_affected_3) AS __x))
        AND (commit_seq <= {visible0:UInt64} OR has({tail:Array(UInt64)}, commit_seq)) AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void))) AS utxo_spenders_4,
(SELECT groupArray((transaction_hash, b, a)) FROM (SELECT transaction_hash, max(b) AS b, max(a) AS a FROM (
    SELECT transaction_hash, sumIf(sign, commit_seq <= {utxo:UInt64}) > 0 AS b, sum(sign) > 0 AS a
    FROM cg.tx_acceptance
    WHERE transaction_hash IN (SELECT h FROM (SELECT tupleElement(__x, 1) AS h, tupleElement(__x, 2) AS i FROM (SELECT arrayJoin(utxo_affected_3) AS __x)) UNION ALL SELECT spender FROM (SELECT tupleElement(__x, 1) AS h, tupleElement(__x, 2) AS i, tupleElement(__x, 3) AS spender FROM (SELECT arrayJoin(utxo_spenders_4) AS __x))) AND node_internal_id = {node:UInt32} AND commit_seq <= {visible:UInt64} AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void))
    GROUP BY transaction_hash, block_internal_id)
  GROUP BY transaction_hash)) AS utxo_involved_5
SELECT transaction_hash, output_index, any(transaction_internal_id) AS transaction_internal_id, any(value_satoshis) AS value_satoshis, any(locking_bytecode) AS locking_bytecode, any(token_category) AS token_category, any(fungible_token_amount) AS fungible_token_amount, any(nonfungible_token_capability) AS nonfungible_token_capability, any(nonfungible_token_commitment) AS nonfungible_token_commitment, any(locking_bytecode_prefix) AS locking_bytecode_prefix, any(nonfungible_token_commitment_key) AS nonfungible_token_commitment_key
  FROM (SELECT transaction_hash, output_index, transaction_internal_id, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, locking_bytecode_prefix, nonfungible_token_commitment_key, sign FROM cg.utxo_set
  WHERE node_internal_id = {node:UInt32}
    AND generation = {utxo_generation:UInt32}
    AND batch <= {utxo:UInt64}
    AND (1)
  UNION ALL
  SELECT transaction_hash, output_index, transaction_internal_id, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, locking_bytecode_prefix, nonfungible_token_commitment_key, sign FROM (SELECT o.transaction_hash AS transaction_hash, o.output_index AS output_index, o.transaction_internal_id AS transaction_internal_id, o.value_satoshis AS value_satoshis, o.locking_bytecode AS locking_bytecode, o.token_category AS token_category, o.fungible_token_amount AS fungible_token_amount, o.nonfungible_token_capability AS nonfungible_token_capability, o.nonfungible_token_commitment AS nonfungible_token_commitment, o.locking_bytecode_prefix AS locking_bytecode_prefix, o.nonfungible_token_commitment_key AS nonfungible_token_commitment_key,
    toInt8(ifNull(c.a, 0) AND NOT ifNull(s.a, 0)) - toInt8(ifNull(c.b, 0) AND NOT ifNull(s.b, 0)) AS sign
  FROM (SELECT transaction_hash, output_index, transaction_internal_id, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, locking_bytecode_prefix, nonfungible_token_commitment_key FROM cg.output
        WHERE (transaction_hash, output_index) IN (SELECT tupleElement(__x, 1) AS h, tupleElement(__x, 2) AS i FROM (SELECT arrayJoin(utxo_affected_3) AS __x))
          AND (commit_seq <= {visible0:UInt64} OR has({tail:Array(UInt64)}, commit_seq)) AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40)) AND NOT has({void:Array(UInt64)}, commit_seq) AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void))
        LIMIT 1 BY transaction_hash, output_index) AS o
  LEFT JOIN (SELECT tupleElement(__x, 1) AS transaction_hash, tupleElement(__x, 2) AS b, tupleElement(__x, 3) AS a FROM (SELECT arrayJoin(utxo_involved_5) AS __x)) AS c ON c.transaction_hash = o.transaction_hash
  LEFT JOIN (SELECT sp.h AS h, sp.i AS i, max(ac.b) AS b, max(ac.a) AS a
             FROM (SELECT tupleElement(__x, 1) AS h, tupleElement(__x, 2) AS i, tupleElement(__x, 3) AS spender FROM (SELECT arrayJoin(utxo_spenders_4) AS __x)) AS sp INNER JOIN (SELECT tupleElement(__x, 1) AS transaction_hash, tupleElement(__x, 2) AS b, tupleElement(__x, 3) AS a FROM (SELECT arrayJoin(utxo_involved_5) AS __x)) AS ac ON ac.transaction_hash = sp.spender
             GROUP BY h, i) AS s
    ON s.h = o.transaction_hash AND s.i = o.output_index
  WHERE sign != 0) WHERE 1)
  GROUP BY transaction_hash, output_index
  HAVING sum(sign) > 0;
