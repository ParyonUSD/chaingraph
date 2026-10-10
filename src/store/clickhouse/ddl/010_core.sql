-- Core (node-agnostic) tables, plan §2.1.
--
-- Conventions (plan §2 "Conventions"):
--   * hashes FixedString(32), bytewise order = Postgres bytea order; token_category zeros = no token.
--   * bytecode String (raw bytes); unlocking_bytecode ZSTD(1).
--   * UInt32 heights/indexes/locktime/sequence/bits; Int64 satoshis and FT amounts; UInt64 internal ids.
--   * Nullable only on non-key columns.
--   * every row carries commit_seq UInt64 (the save that wrote it, §3.1); the gate views (050) hide
--     rows of unresolved/aborted commits, so these engines never need FINAL.
--   * index_granularity = 1024 on point-lookup/join tables (plan §2). 256 is the Phase 1 comparison arm.
--   * output: index_granularity = 128 and 4 KiB compressed blocks. The writer resolves every spent
--     output by (transaction_hash, output_index) (one random granule per spent tx per part); at 1024
--     rows and 64 KiB-1 MiB blocks that read and decompressed ~1,500 rows / ~100 KB per outpoint
--     (G1 lab: 0.5-2.7 s per lookup; g1-fix-pass-2.md §2). Its projections (060) keep 1024 and the
--     default block sizes via WITH SETTINGS, so script/category reads are unchanged.
--
-- Choices where the plan is silent (also listed in README.md):
--   * Timestamps are DateTime64(3, 'UTC'): the agent writes JS Dates (ms), so ms is exact.
--   * Derived sort columns (locking_bytecode_prefix, nonfungible_token_commitment_key) are MATERIALIZED,
--     so the writer cannot get them wrong and -1 rows always match +1 rows.
--   * non_replicated_deduplication_window = 10000 on every agent-written MergeTree table, so
--     insert_deduplication_token works on plain MergeTree (§3.4); 10000 mirrors the replicated default.
--   * old_parts_lifetime 30 s and fast cleanup on every table (fix-pass-3.md §3): with the server defaults
--     (480 s) a small-block sync filled the disk with merged-away parts. The DDL CLI ALTERs them in place.
--   * block and block_transaction also get granularity 1024 (point lookups / joins);
--     block_transaction is partitioned like transaction (420 M rows, GC by commit_seq); block is not (1.3 M).
--   * transaction/block version is Int32 (plan), Postgres stored bigint; values fit the 4-byte field.

CREATE TABLE IF NOT EXISTS cg.block
(
    hash                      FixedString(32),
    internal_id               UInt64,
    height                    UInt32,
    version                   Int32,
    timestamp                 UInt32,
    previous_block_hash       FixedString(32),
    merkle_root               FixedString(32),
    bits                      UInt32,
    nonce                     UInt32,
    size_bytes                UInt32,
    transaction_count         UInt32,
    output_value_satoshis     Int64,
    generated_value_satoshis  Int64,
    commit_seq                UInt64
)
ENGINE = MergeTree
ORDER BY hash
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

CREATE TABLE IF NOT EXISTS cg.transaction
(
    hash                   FixedString(32),
    internal_id            UInt64,
    version                Int32,
    locktime               UInt32,
    size_bytes             UInt32,
    is_coinbase            Bool,
    input_count            UInt32,
    output_count           UInt32,
    output_value_satoshis  Int64,
    commit_seq             UInt64
)
ENGINE = MergeTree
PARTITION BY intDiv(commit_seq, 1048576)
ORDER BY hash
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

CREATE TABLE IF NOT EXISTS cg.block_transaction
(
    block_internal_id        UInt64 CODEC(Delta, ZSTD(1)),
    transaction_index        UInt32 CODEC(Delta, ZSTD(1)),
    transaction_internal_id  UInt64,
    transaction_hash         FixedString(32),
    commit_seq               UInt64
)
ENGINE = MergeTree
PARTITION BY intDiv(commit_seq, 1048576)
ORDER BY (block_internal_id, transaction_index)
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

CREATE TABLE IF NOT EXISTS cg.output
(
    transaction_hash                  FixedString(32),
    output_index                      UInt32,
    transaction_internal_id           UInt64,
    value_satoshis                    Int64,
    locking_bytecode                  String,
    token_category                    FixedString(32),
    fungible_token_amount             Nullable(Int64),
    nonfungible_token_capability      Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3)),
    nonfungible_token_commitment      Nullable(String),
    commit_seq                        UInt64,
    locking_bytecode_prefix           String MATERIALIZED substring(locking_bytecode, 1, 25),
    nonfungible_token_commitment_key  String MATERIALIZED ifNull(nonfungible_token_commitment, ''),
    INDEX bf_locking_bytecode locking_bytecode TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY intDiv(commit_seq, 1048576)
ORDER BY (transaction_hash, output_index)
SETTINGS index_granularity = 128, min_compress_block_size = 4096, max_compress_block_size = 4096,
         non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

-- input carries the spent output's attributes (plan §2.1 "Why input carries the spent output's attributes").
-- Rows for inputs whose outpoint is not yet stored are written by the fill_pending commit (see README).
CREATE TABLE IF NOT EXISTS cg.input
(
    transaction_hash                  FixedString(32),
    input_index                       UInt32,
    transaction_internal_id           UInt64,
    outpoint_transaction_hash         FixedString(32),
    outpoint_index                    UInt32,
    sequence_number                   UInt32,
    unlocking_bytecode                String CODEC(ZSTD(1)),
    value_satoshis                    Int64,
    token_category                    FixedString(32),
    fungible_token_amount             Nullable(Int64),
    nonfungible_token_capability      Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3)),
    nonfungible_token_commitment      Nullable(String),
    locking_bytecode                  String,
    commit_seq                        UInt64,
    locking_bytecode_prefix           String MATERIALIZED substring(locking_bytecode, 1, 25),
    nonfungible_token_commitment_key  String MATERIALIZED ifNull(nonfungible_token_commitment, ''),
    INDEX bf_outpoint_transaction_hash outpoint_transaction_hash TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY intDiv(commit_seq, 1048576)
ORDER BY (transaction_hash, input_index)
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

-- WP5a: the spent output's fungible_token_amount (plan gap found by WP2), so inputs.outpoint FT filters need
-- no join. Databases created before WP5a get the column here; see README item 23 for their projections.
ALTER TABLE cg.input ADD COLUMN IF NOT EXISTS fungible_token_amount Nullable(Int64) AFTER token_category;

-- node: a handful of rows. internal_id UInt32 (Postgres integer). Read with FINAL / argMax(updated_at).
CREATE TABLE IF NOT EXISTS cg.node
(
    internal_id                 UInt32,
    name                        String,
    protocol_version            Int32,
    user_agent                  String,
    first_connected_at          DateTime64(3, 'UTC'),
    latest_connection_began_at  DateTime64(3, 'UTC'),
    updated_at                  DateTime64(3, 'UTC'),
    commit_seq                  UInt64
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY internal_id
SETTINGS non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;
