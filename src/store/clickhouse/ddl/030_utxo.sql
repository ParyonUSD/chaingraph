-- Per-node UTXO set, plan §2.3. Two tables (not one + projection) so neither needs projection
-- support on a collapsing engine. Every row is written to both, in the same commit.
-- Reads: GROUP BY key HAVING sum(sign) > 0 (order-independent; correct with orphan -1 rows).
--
-- Choices where the plan is silent (also in README.md):
--   * WP5a: `version` is the row's own commit_seq (WP2 had a constant 1, which lets a merge delete a
--     committed +1 together with an uncommitted or aborted -1; see 020_acceptance.sql). Pairs written
--     by one commit (an output created and spent in one block, the horizon build) still collapse.
--   * WP5a: created_height is always 0 (unspent_output / F1g returns output columns only; every row
--     of an outpoint must carry identical non-key values so the views' any() is exact). Take the
--     height from tx_acceptance. Kept for compatibility; a candidate for removal.
--   * Unpartitioned (as #83; VCMT collapses within a partition).
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
