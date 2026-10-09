-- Per-node acceptance tables, plan §2.2. Every key starts with or contains node_internal_id
-- (checklist item 1). Membership tables are VersionedCollapsingMergeTree(sign, version); reads use
-- sum(sign) > 0 per key, which is order-independent (unmerged pairs, orphan -1 rows, double re-orgs).
--
-- Choices where the plan is silent (also in README.md):
--   * WP5a: `version` is the row's OWN commit_seq, for +1 and -1 rows alike. (WP2 had a -1 copy the
--     version of the +1 it cancels; a background merge then deletes both rows physically, even
--     while the -1's commit is uncommitted or after it was aborted, and the gate cannot bring the +1
--     back.) VCMT therefore only collapses pairs written by one commit; cross-commit pairs stay
--     until a compaction job removes them (docs/clickhouse-port/wp5a-core.md §2).
--   * Collapsing tables are unpartitioned: VCMT only collapses within a partition.
--   * tx_acceptance has a projection on a VCMT table, which ClickHouse refuses unless
--     deduplicate_merge_projection_mode is set; 'rebuild' keeps the projection exact after collapses.
--   * History tables carry ids only (plan columns); joins to block/transaction go via internal ids.

CREATE TABLE IF NOT EXISTS cg.node_block
(
    node_internal_id   UInt32,
    block_internal_id  UInt64 CODEC(Delta, ZSTD(1)),
    block_hash         FixedString(32),
    height             UInt32,
    accepted_at        Nullable(DateTime64(3, 'UTC')),
    sign               Int8,
    version            UInt64,
    commit_seq         UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (node_internal_id, block_internal_id)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS cg.node_transaction
(
    node_internal_id         UInt32,
    transaction_internal_id  UInt64,
    transaction_hash         FixedString(32),
    validated_at             Nullable(DateTime64(3, 'UTC')),
    sign                     Int8,
    version                  UInt64,
    commit_seq               UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (node_internal_id, transaction_internal_id)
SETTINGS non_replicated_deduplication_window = 10000;

-- tx-accepted(n, t) as one table: one row per (tx, node, accepting block or 0 = mempool).
CREATE TABLE IF NOT EXISTS cg.tx_acceptance
(
    transaction_hash         FixedString(32),
    node_internal_id         UInt32,
    block_internal_id        UInt64,
    transaction_internal_id  UInt64,
    height                   UInt32,
    accepted_at              Nullable(DateTime64(3, 'UTC')),
    sign                     Int8,
    version                  UInt64,
    commit_seq               UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (transaction_hash, node_internal_id, block_internal_id)
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         deduplicate_merge_projection_mode = 'rebuild';

CREATE TABLE IF NOT EXISTS cg.node_block_history
(
    node_internal_id   UInt32,
    removed_at         DateTime64(3, 'UTC'),
    block_internal_id  UInt64,
    internal_id        UInt64,
    accepted_at        Nullable(DateTime64(3, 'UTC')),
    commit_seq         UInt64
)
ENGINE = MergeTree
ORDER BY (node_internal_id, removed_at, block_internal_id)
SETTINGS non_replicated_deduplication_window = 10000;

-- replaced_at NULL = confirmed (moved from mempool into an accepted block).
CREATE TABLE IF NOT EXISTS cg.node_transaction_history
(
    node_internal_id         UInt32,
    transaction_internal_id  UInt64,
    internal_id              UInt64,
    validated_at             Nullable(DateTime64(3, 'UTC')),
    replaced_at              Nullable(DateTime64(3, 'UTC')),
    commit_seq               UInt64
)
ENGINE = MergeTree
ORDER BY (node_internal_id, transaction_internal_id, internal_id)
SETTINGS non_replicated_deduplication_window = 10000;
