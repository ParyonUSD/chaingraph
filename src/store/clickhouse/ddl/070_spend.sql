-- The asynchronous spend index (option C of the input denormalisation plan, owner decision 2026-10-11).
-- Design, read rule and lag: docs/clickhouse-port/spend-index.md.
--
-- `input` stores the input's own fields only. Filters on the spent output (an address's spends, a token category's
-- spends, an NFT commitment prefix) would otherwise join output -> input by outpoint, which costs seconds for busy
-- addresses and categories on mainnet. `spend` holds, per input whose spent output is stored, the input key, the
-- outpoint and the spent output's attributes, sorted by script prefix (table) and by category + commitment
-- (projection p_category).
--
-- It is filled server-side by `INSERT ... SELECT` from input JOIN output (spend-index.ts), off the commit path, in
-- batches of commit seqs (from, to] at or below the node-agnostic watermark visible(0). A row is written only when the
-- input's commit and the spent output's commit are both committed: it never holds a placeholder. `batch` is the
-- batch's upper seq; readers see a row iff batch <= the spend watermark they read in their snapshot
-- (spend_watermark), and take every pair above that watermark from the join (the tail).
--
-- Choices:
--   * commit_seq = the input row's commit, spent_commit_seq = the output row's commit (both kept for checks).
--   * Partitioned like the data tables but by `batch`, so a crashed batch's leftovers (batch > watermark) are found by
--     partition/part pruning and deleted at the next start.
--   * spend_orphan: inputs a forward pass found no stored output for (child-before-parent in the mempool, spends of
--     outputs Chaingraph never sees). A later batch that stores the output writes their spend rows (reverse pass).
--     Small; never pruned (an orphan matches at most one later batch).
--   * spend_watermark: one row per finished batch; readers take max(watermark).

CREATE TABLE IF NOT EXISTS cg.spend
(
    transaction_hash                  FixedString(32),
    input_index                       UInt32,
    transaction_internal_id           UInt64,
    outpoint_transaction_hash         FixedString(32),
    outpoint_index                    UInt32,
    value_satoshis                    Int64,
    locking_bytecode                  String,
    token_category                    FixedString(32),
    fungible_token_amount             Nullable(Int64),
    nonfungible_token_capability      Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3)),
    nonfungible_token_commitment      Nullable(String),
    commit_seq                        UInt64,
    spent_commit_seq                  UInt64,
    batch                             UInt64,
    locking_bytecode_prefix           String MATERIALIZED substring(locking_bytecode, 1, 25),
    nonfungible_token_commitment_key  String MATERIALIZED ifNull(nonfungible_token_commitment, '')
)
ENGINE = MergeTree
PARTITION BY intDiv(batch, 1048576)
ORDER BY (locking_bytecode_prefix, transaction_hash, input_index)
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

ALTER TABLE cg.spend ADD PROJECTION IF NOT EXISTS p_category
(
    SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key
    ORDER BY (token_category, nonfungible_token_commitment_key, transaction_hash, input_index)
);

CREATE TABLE IF NOT EXISTS cg.spend_orphan
(
    outpoint_transaction_hash  FixedString(32),
    outpoint_index             UInt32,
    transaction_hash           FixedString(32),
    input_index                UInt32,
    transaction_internal_id    UInt64,
    commit_seq                 UInt64,
    batch                      UInt64
)
ENGINE = MergeTree
PARTITION BY intDiv(batch, 1048576)
ORDER BY (outpoint_transaction_hash, outpoint_index)
SETTINGS non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

CREATE TABLE IF NOT EXISTS cg.spend_watermark
(
    watermark   UInt64,
    batch_from  UInt64,
    rows        UInt64,
    orphans     UInt64,
    writer_epoch UInt64,
    filled_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(watermark)
ORDER BY watermark
SETTINGS non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

-- The spend index as of a reader's snapshot (generated text: view-ddl.ts; checked by view-ddl.spec.ts): rows of finished
-- batches up to the spend watermark the snapshot read
-- (`spend`, from readSnapshot). Every pair above it comes from the join (docs/clickhouse-port/spend-index.md).
CREATE OR REPLACE VIEW cg.spend_at AS
SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key
FROM cg.spend
WHERE batch <= {spend:UInt64};
