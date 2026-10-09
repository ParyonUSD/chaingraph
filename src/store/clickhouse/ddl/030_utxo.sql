-- Per-node UTXO set, plan §2.3. Two tables (not one + projection) so neither needs projection
-- support on a collapsing engine. Every row is written to both, in the same commit.
-- Reads: GROUP BY key HAVING sum(sign) > 0 (order-independent; correct with orphan -1 rows).
--
-- Choices where the plan is silent (also in README.md):
--   * `version` is a constant written by the agent (1, as in #83's build) for every utxo row: VCMT
--     only collapses equal-version pairs, and the sum semantics need no ordering. A per-transition
--     version would stop -1/+1 pairs from ever collapsing.
--   * created_height UInt32, 0 = mempool. Unpartitioned (as #83; VCMT collapses within a partition).
--   * Granularity left at 8192 (#83 measured 4 ms UTXO reads at the default); Phase 1 may revisit.

CREATE TABLE IF NOT EXISTS cg.utxo
(
    node_internal_id                  UInt32,
    token_category                    FixedString(32),
    nonfungible_token_commitment_key  String MATERIALIZED ifNull(nonfungible_token_commitment, ''),
    transaction_hash                  FixedString(32),
    output_index                      UInt32,
    transaction_internal_id           UInt64,
    created_height                    UInt32,
    value_satoshis                    Int64,
    locking_bytecode                  String,
    fungible_token_amount             Nullable(Int64),
    nonfungible_token_capability      Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3)),
    nonfungible_token_commitment      Nullable(String),
    sign                              Int8,
    version                           UInt64,
    commit_seq                        UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index)
SETTINGS non_replicated_deduplication_window = 10000;

-- locking_bytecode_prefix = substring(locking_bytecode, 1, 25), matching Postgres's
-- output_search_index key substring(..., 0, 26).
CREATE TABLE IF NOT EXISTS cg.utxo_by_script
(
    node_internal_id                  UInt32,
    locking_bytecode_prefix           String MATERIALIZED substring(locking_bytecode, 1, 25),
    transaction_hash                  FixedString(32),
    output_index                      UInt32,
    transaction_internal_id           UInt64,
    created_height                    UInt32,
    value_satoshis                    Int64,
    locking_bytecode                  String,
    token_category                    FixedString(32),
    fungible_token_amount             Nullable(Int64),
    nonfungible_token_capability      Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3)),
    nonfungible_token_commitment      Nullable(String),
    sign                              Int8,
    version                           UInt64,
    commit_seq                        UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (node_internal_id, locking_bytecode_prefix, transaction_hash, output_index)
SETTINGS non_replicated_deduplication_window = 10000;
