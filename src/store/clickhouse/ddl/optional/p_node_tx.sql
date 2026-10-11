-- OPTIONAL (off by default): the per-node acceptance projection p_node_tx (owner decision 2026-10-11, item 3).
-- Applied only when asked for: CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL=p_node_tx (bin/chaingraph-clickhouse-ddl.js, the
-- e2e harness and apply.sh), or `--optional p_node_tx` on the DDL CLI. Whether it stays is decided by the mainnet
-- numbers (docs/clickhouse-port/phase2-api.md §6.4).
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
