-- Bookkeeping tables, plan §2.6 and §3.1-§3.5.
--
-- Choices where the plan is silent (also in README.md):
--   * commit_log.state adds 'incomplete' (§3.5: a commit with unresolved pending spends). Ranks:
--     intent 1 < incomplete 2 < committed 3 < aborted 4; state_rank is MATERIALIZED from the enum, so
--     FINAL always yields the furthest state. committed and aborted are both terminal and exclusive.
--   * commit_log.kind adds 'horizon_switch' (§3.8 "records the switch in commit_log").
--   * commit_log.row_counts is Map(table -> rows); writer_epoch ties each commit to a lease epoch.
--   * writer_lease uses ReplacingMergeTree(heartbeat_at), not KeeperMap: the local docker server has no
--     Keeper. It is advisory (no compare-and-set in ClickHouse); see README "at risk".
--   * pending_spend key is (outpoint_transaction_hash, outpoint_index, node_internal_id, spender...):
--     it is probed by outpoint when a parent's outputs arrive. version = commit_seq of the +1 row.
--   * id_reservation ranges are half-open [range_start, range_end).

CREATE TABLE IF NOT EXISTS cg.commit_log
(
    commit_seq    UInt64 CODEC(Delta, ZSTD(1)),
    state         Enum8('intent' = 1, 'incomplete' = 2, 'committed' = 3, 'aborted' = 4),
    state_rank    UInt8 MATERIALIZED toUInt8(state),
    node_scope    Array(UInt32),
    kind          Enum8('block' = 1, 'mempool_batch' = 2, 'reorg' = 3, 'header_accept' = 4, 'expiry' = 5,
                        'utxo_build' = 6, 'fill_pending' = 7, 'horizon_switch' = 8),
    block_hash    FixedString(32),
    row_counts    Map(LowCardinality(String), UInt64),
    writer_epoch  UInt64,
    started_at    DateTime64(3, 'UTC'),
    finished_at   Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree(state_rank)
ORDER BY commit_seq
SETTINGS non_replicated_deduplication_window = 10000;

-- Per-node watermark. node_internal_id 0 = the node-agnostic data watermark.
CREATE TABLE IF NOT EXISTS cg.visibility
(
    node_internal_id  UInt32,
    visible_seq       UInt64,
    updated_at        DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(visible_seq)
ORDER BY node_internal_id
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS cg.id_reservation
(
    id_kind       Enum8('block' = 1, 'transaction' = 2, 'node' = 3, 'node_block_history' = 4,
                        'node_transaction_history' = 5),
    range_start   UInt64,
    range_end     UInt64,
    writer_epoch  UInt64,
    reserved_at   DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (id_kind, range_start)
SETTINGS non_replicated_deduplication_window = 10000;

CREATE TABLE IF NOT EXISTS cg.writer_lease
(
    lease_name    LowCardinality(String),
    agent_id      String,
    epoch         UInt64,
    heartbeat_at  DateTime64(3, 'UTC'),
    expires_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(heartbeat_at)
ORDER BY lease_name
SETTINGS non_replicated_deduplication_window = 10000;

-- Inputs whose spent output is not yet stored (child-before-parent, §3.5), per node.
CREATE TABLE IF NOT EXISTS cg.pending_spend
(
    outpoint_transaction_hash  FixedString(32),
    outpoint_index             UInt32,
    node_internal_id           UInt32,
    spender_transaction_hash   FixedString(32),
    spender_input_index        UInt32,
    spender_commit_seq         UInt64,
    sign                       Int8,
    version                    UInt64,
    commit_seq                 UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY (outpoint_transaction_hash, outpoint_index, node_internal_id, spender_transaction_hash, spender_input_index)
SETTINGS non_replicated_deduplication_window = 10000;
