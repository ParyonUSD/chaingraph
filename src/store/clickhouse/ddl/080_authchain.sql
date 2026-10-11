-- The authchain index: identity-output chains as heavy-path segments (Phase 2 stage 2,
-- docs/clickhouse-port/phase2-api.md §8, authchain-index.ts).
--
-- A transaction c continues p when c spends p's output 0 (the identity output). Following those spends from a base
-- transaction to the first unspent output 0 is the base's authchain (v1 collect_authchains); chipnet has chains of
-- 56,903 links, so a per-link walk cannot answer them. Every transaction that spends at least one output 0 gets one
-- row placing it on a segment: c continues the segment of its heaviest output-0 parent (the parent through which the
-- most transactions' chains pass, `size`), at position(parent) + 1; a transaction without a row starts its own
-- segment (segment = its transaction internal id, position 0). A chain from any transaction then crosses few segment
-- boundaries (heavy-path decomposition: at most log2 of the transactions; chipnet: at most 6 over every chain), and
-- the reader walks a segment in one keyed read (ORDER BY segment, position).
--
-- Rows only place transactions; they are not the chain. The reader keeps the members its node accepts, in position
-- order, and follows the input table (any commit, also above the spend watermark) where a segment ends. So a row
-- written with stale or missing parent information (orphans, a parent indexed later, a concurrent backfill) costs
-- an extra boundary, never a wrong chain.
--
-- Filled by the spend index's background filler with each batch (`batch` = the spend batch, the same restart rule:
-- rows above the spend watermark are deleted at the next start), and for existing databases by the backfill
-- (bin/chaingraph-clickhouse-authchain-backfill.js). Readers see rows with batch <= their snapshot's spend watermark.

CREATE TABLE IF NOT EXISTS cg.authchain_link
(
    transaction_hash  FixedString(32),
    segment           UInt64,
    position          UInt32,
    size              UInt64,
    commit_seq        UInt64,
    batch             UInt64
)
ENGINE = MergeTree
PARTITION BY intDiv(batch, 1048576)
ORDER BY (segment, position, transaction_hash)
SETTINGS index_granularity = 1024, non_replicated_deduplication_window = 10000,
         old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
         cleanup_delay_period_random_add = 5;

ALTER TABLE cg.authchain_link ADD PROJECTION IF NOT EXISTS p_tx
(
    SELECT * ORDER BY transaction_hash
);

-- The authchain index as of a reader's snapshot (rows of finished spend batches up to its spend watermark).
CREATE OR REPLACE VIEW cg.authchain_link_at AS
SELECT *
FROM cg.authchain_link
WHERE batch <= {spend:UInt64};
