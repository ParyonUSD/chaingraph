-- DEFAULT ON (owner decision 2026-10-12, item 1; it was opt-in since 2026-10-11, item 3): the per-node acceptance
-- projection p_node_tx. Applied by default by the DDL CLI (bin/chaingraph-clickhouse-ddl.js), applyClickHouseDdl
-- (the e2e harness) and apply.sh. Opt out with CHAINGRAPH_CLICKHOUSE_DDL_SKIP=p_node_tx (or `--skip p_node_tx` on
-- the DDL CLI); the API then falls back to the id range (src/api/roots.ts), same results.
--
-- tx_acceptance is ordered by transaction hash, so "the transactions node n accepts, in internal-id order" (an
-- unselective per-node root: `transactions(node:)` paged by internal id) reads the node's whole acceptance set.
-- This projection orders the same rows by (node, transaction internal id): an id window or a keyset page reads only
-- its range. The API notices the projection (system.projections) and then compiles those roots so the projection
-- is used and read in order (src/api/roots.ts); without it they fall back to the id range applied to the base rows.
-- Chipnet: +102.7 MiB (+43 % of tx_acceptance).
--
-- ADD PROJECTION covers new parts. Existing parts need `ALTER TABLE cg.tx_acceptance MATERIALIZE PROJECTION p_node_tx`
-- (a mutation, asynchronous): the DDL CLI runs it once, when it adds the projection to a table that has rows
-- (ddl-apply.ts); apply.sh does not (it creates scratch databases, whose tables are empty). Re-applying the ADD is a
-- no-op (IF NOT EXISTS).

ALTER TABLE cg.tx_acceptance ADD PROJECTION IF NOT EXISTS p_node_tx
(
    SELECT * ORDER BY (node_internal_id, transaction_internal_id)
);
