# Deferred output node membership candidate

This branch changes the rollout: complete normalized initial sync first, then
build `accepted_node_ids`, `unspent_node_ids`, and their indexes with ingestion
paused. Until that build completes, array-backed GraphQL roots remain unavailable.
The candidate is experimental; its agent integration and mature-database
large-block/reorg performance gate are not yet complete.

The deferred-maintenance migration disables the 22 legacy maintenance triggers.
The seven TRUNCATE guards remain enabled. It also retires the old `backfill`
procedure and writer-lock function; the old `finalize.sql` now fails closed.
Do not re-enable the legacy triggers after backfill.

## Initial sync and offline build

Set `CHAINGRAPH_OUTPUT_MEMBERSHIP_MODE=deferred` for initial sync. Record both mainnet and chipnet
normalized completion and managed-index completion before starting the build.
Pause all writers and use the fenced lock-free runner in the infrastructure
experiment worktree:

- `experiments/chaingraph/gke-test/fresh-sync/prepare_lock_free_array_backfill.py`
- `experiments/chaingraph/gke-test/fresh-sync/prepare_lock_free_array_backfill.sql`
- `experiments/chaingraph/gke-test/fresh-sync/performance_finalize_lock_free_arrays.sql`

Those tools validate the original run/cluster/volume identity, fence ordinary
writers, rebuild the entire then-current heap, retain durable batch progress,
and build the array indexes. A historical partial build cannot be reused as
complete coverage after deferred ingestion has resumed. Preserve the original
experiment start time and interruptions in performance reporting.

## Ongoing maintenance after build

`scripts/output-node-membership/incremental.sql` is a separate candidate for
targeted maintenance. It uses ordinary node/output row locks and no advisory
locks. The agent must acquire node locks before normalized writes, collect all
explicit and implicit acceptance changes, and publish array changes in the
same database transaction. Duplicate acceptance skips unchanged array values;
spends and reorgs can remove membership. Late creators must include every
already-accepting affected node, not only the announcing node.

After the offline build, the candidate agent uses
`CHAINGRAPH_OUTPUT_MEMBERSHIP_MODE=incremental`. Startup rejects enabled legacy
triggers, missing collectors or array indexes, and incompatible readiness.
The compatibility `baseline` mode is restricted to databases without legacy
array triggers; it cannot reactivate the old advisory-lock path.

This SQL must not be used alone to enable production queries. Prove the actual
agent path on a fully backfilled benchmark clone first: dense and byte-heavy
approximately 32 MB blocks, long reorgs, overlapping nodes, replay/restart,
and bounded memory/backlog. Small warm SQL timings are preliminary evidence.
Keep the original database fenced until the reviewed performance gate permits
release. Do not silently switch an existing initial-sync image to this candidate.

`unspent_node_ids` excludes locking bytecode beginning with `OP_RETURN` (`0x6a`).
Such outputs can remain accepted. Normalized mempool/block acceptance and all
accepted spenders remain authoritative; the arrays are derived query state.
