# ClickHouse schema (Phase 1, WP2)

DDL for Chaingraph's ClickHouse primary store, per the plan
`paryon_kubernetes/docs/chaingraph/plans/clickhouse-primary-store.md` §1–§3 and §6.

```sh
./apply.sh http://localhost:18123                 # local docker, database cg
CH_USER=... CH_PASSWORD=... ./apply.sh <url> ch1_wp2_scratch   # rewrite db name; creds via env only
```

Files run in order; every statement is `IF NOT EXISTS`, so re-running is safe.

| File | Contents |
|---|---|
| `001_database.sql` | `cg` |
| `010_core.sql` | block, transaction, block_transaction, output, input, node |
| `020_acceptance.sql` | node_block, node_transaction, tx_acceptance, node_block_history, node_transaction_history |
| `030_utxo.sql` | utxo, utxo_by_script |
| `040_bookkeeping.sql` | commit_log, commit_void, epoch_fence, visibility, id_reservation, writer_lease, pending_spend |
| `050_views.sql` | visibility gate views: `*_v` (live watermark) and `*_at` (pinned watermark, WP4); `CREATE OR REPLACE` |
| `060_projections.sql` | the §2.1/§2.4 projections as `ALTER TABLE … ADD PROJECTION IF NOT EXISTS` |

Server-fact checks are in `../checks/` (`run.sh <url> <file>`); results below.

## Tables

Checklist items refer to plan §1 (1 keys, 2 isolation, 3 no any-node shortcuts, 4 exact under every
transition, 5 per-node visibility, 6 per-node history, 7 the API names the node).
"Engine on Cloud" is what `system.tables.engine` reports on ClickHouse Cloud 26.6; local 26.8 reports the
engine as written. VersionedCollapsingMergeTree appends `version` to the sorting key on both servers.

| Table | Engine (local 26.8 / Cloud 26.6) | Sort key | Partition | Granularity | Checklist |
|---|---|---|---|---|---|
| block | MergeTree / SharedMergeTree | `hash` | none | 1024 | node-agnostic data (7) |
| transaction | MergeTree / SharedMergeTree | `hash` | `intDiv(commit_seq, 2^20)` | 1024 | node-agnostic (7) |
| block_transaction | MergeTree / SharedMergeTree | `block_internal_id, transaction_index` | `intDiv(commit_seq, 2^20)` | 1024 | node-agnostic (7) |
| output | MergeTree / SharedMergeTree | `transaction_hash, output_index` | `intDiv(commit_seq, 2^20)` | 1024 | node-agnostic (7) |
| input | MergeTree / SharedMergeTree | `transaction_hash, input_index` | `intDiv(commit_seq, 2^20)` | 1024 | node-agnostic (7) |
| node | ReplacingMergeTree(updated_at) / SharedReplacingMergeTree | `internal_id` | none | 8192 | registry; resolves name → id (7) |
| node_block | VersionedCollapsingMergeTree(sign, version) / SharedVersionedCollapsingMergeTree | `node_internal_id, block_internal_id, version` | none | 8192 | 1, 2, 4 |
| node_transaction | VCMT / SharedVCMT | `node_internal_id, transaction_internal_id, version` | none | 8192 | 1, 2, 4 |
| tx_acceptance | VCMT / SharedVCMT | `transaction_hash, node_internal_id, block_internal_id, version` | none | 1024 | 1, 2, 4 (tx-accepted(n, t)) |
| node_block_history | MergeTree / SharedMergeTree | `node_internal_id, removed_at, block_internal_id` | none | 8192 | 1, 6 |
| node_transaction_history | MergeTree / SharedMergeTree | `node_internal_id, transaction_internal_id, internal_id` | none | 8192 | 1, 6 |
| utxo | VCMT / SharedVCMT | `node_internal_id, token_category, nonfungible_token_commitment_key, transaction_hash, output_index, version` | none | 8192 | 1, 2, 3, 4 (unspent(n, o)) |
| utxo_by_script | VCMT / SharedVCMT | `node_internal_id, locking_bytecode_prefix, transaction_hash, output_index, version` | none | 8192 | 1, 2, 3, 4 |
| commit_log | ReplacingMergeTree(state_rank) / SharedReplacingMergeTree | `commit_seq` | none | 8192 | 4 (crash), 5 |
| visibility | ReplacingMergeTree(visible_seq) / SharedReplacingMergeTree | `node_internal_id` | none | 8192 | 1, 5 |
| id_reservation | MergeTree / SharedMergeTree | `id_kind, range_start` | none | 8192 | 4 (crash never reuses ids) |
| commit_void (WP4) | MergeTree / SharedMergeTree | `commit_seq` | none | 8192 | 4 (aborted seqs; read by the gate) |
| epoch_fence (WP4) | MergeTree / SharedMergeTree | `epoch` | none | 8192 | 4 (stale-writer fencing) |
| writer_lease | ReplacingMergeTree(heartbeat_at) / SharedReplacingMergeTree | `lease_name, epoch, agent_id` (WP4) | none | 8192 | single writer (precondition of 4) |
| pending_spend | VCMT / SharedVCMT | `outpoint_transaction_hash, outpoint_index, node_internal_id, spender_transaction_hash, spender_input_index, version` | none | 8192 | 1, 2, 4 (child-before-parent) |

Projections (060): `block.p_height (height)`, `transaction.p_id (internal_id)`,
`block_transaction.p_tx (transaction_hash)`, `output.p_script (locking_bytecode_prefix, transaction_hash, output_index)`,
`output.p_category (token_category, nonfungible_token_commitment_key, transaction_hash, output_index)`,
`input.p_outpoint (outpoint_transaction_hash, outpoint_index)`, `input.p_spent_script (locking_bytecode_prefix, transaction_hash, input_index)`,
`input.p_spent_category (token_category, nonfungible_token_commitment_key, transaction_hash, input_index)`,
`tx_acceptance.p_node_height (node_internal_id, height, transaction_internal_id)`. All full-column; the three
`input` ones omit `unlocking_bytecode` (plan). All accepted by both servers. Skip indexes: `bloom_filter` on
`output.locking_bytecode` and `input.outpoint_transaction_hash` (plan §2.4 fallbacks).

## Views (050)

| View | Parameter | Gate | Collapse | Checklist |
|---|---|---|---|---|
| `node_block_v` | `node` | `commit_seq <= visible(n)`, not void, not fenced | `HAVING sum(sign) > 0` per (n, block) | 1, 2, 4, 5, 7 |
| `node_transaction_v` | `node` | same | per (n, tx) | 1, 2, 4, 5, 7 |
| `tx_acceptance_v` | `node` | same | per (tx, n, block/0) | 1, 2, 4, 5, 7 |
| `utxo_v` | `node` | same | per utxo key | 1, 2, 3, 4, 5, 7 |
| `utxo_by_script_v` | `node` | same | per utxo key | 1, 2, 3, 4, 5, 7 |
| `node_block_history_v` | `node` | same | none | 5, 6, 7 |
| `node_transaction_history_v` | `node` | same | none | 5, 6, 7 |
| `block_v`, `transaction_v`, `block_transaction_v`, `output_v`, `input_v` | none | (`seq <= visible(0)` or in the committed tail above `visible(0)`), not void, not fenced | none | 7 (node-agnostic; no acceptance fields) |
| every node-scoped `*_at` (WP4) | `node`, `visible` | as `*_v`, with `least(visible, live visible(n))` | as `*_v` | pins one watermark across views (no torn reads) |
| `block_at`, `transaction_at`, `block_transaction_at`, `output_at`, `input_at` (WP4) | `visible0`, `tail` | (`seq <= least(visible0, live visible(0))` or `seq` in `tail` and committed), not void, not fenced | none | pinned node-agnostic snapshot |
| `node_v` | none | `FINAL` | Replacing | 7 (name → id) |

Querying a node-scoped view without `node` is an error, so no acceptance answer can be had without naming
a node. Verified on both servers: every view executes; the outer `WHERE` on a key column is pushed into the
view's `PREWHERE` and uses the primary key (check d: `Granules: 2/392`); the scalar `visible(n)` subquery is
folded to a constant (`commit_seq <= 5`); `output_v` filtered on `locking_bytecode_prefix` reads projection
`p_script` (`EXPLAIN projections = 1`: `ReadFromMergeTree (p_script)`, `Granules: 1/20`).

WP4 re-verified this for the `*_at` views (`visibility.spec.ts`, `[e2e] pinned views keep primary-key and
projection use`): `utxo_at` reads 1/13 granules on `(node_internal_id, token_category)`, `output_at` reads
projection `p_script`; the fence array and watermark fold to constants in `PREWHERE`.

Note for WP4: `force_optimize_projection = 1` gives a false "No projection is used" through the gated views,
because it also applies to the `visibility`/`commit_log` subqueries. Use `EXPLAIN projections = 1` instead.

## Server facts (checks/, run 2026-10-09)

Local: `26.8.22.13 (official build)`, docker `ch1-local`. Cloud: `26.6.1.2326`, SharedMergeTree.
Both servers default `async_insert = 1`, `insert_deduplicate = 1`, `deduplicate_insert = 'enable'`,
`async_insert_deduplicate = 0`, `replicated_deduplication_window = 10000` / `_seconds = 3600`,
`non_replicated_deduplication_window = 0`.

| Check | Local 26.8 (MergeTree) | Cloud 26.6 (SharedMergeTree) |
|---|---|---|
| a. same `insert_deduplication_token` twice, table defaults, sync | **not deduplicated** (4 rows) | **deduplicated** (2) |
| a. same, async insert (`wait_for_async_insert = 1`) | not deduplicated | deduplicated |
| a. `non_replicated_deduplication_window = 100`: sync / async / `INSERT … SELECT` | deduplicated / deduplicated / deduplicated | setting accepted (no-op); all deduplicated |
| a. same token, different data | dropped (token wins) | dropped |
| a. same data, new token | inserted | inserted |
| a. `replicated_deduplication_window = 100` on MergeTree | accepted, **no effect** (4 rows) | deduplicated |
| a. VersionedCollapsingMergeTree + token (window 100) | deduplicated | deduplicated |
| b. lightweight `DELETE` with a projection, default mode | error 344 `SUPPORT_IS_DISABLED`: "DELETE query is not allowed … because as it has projections and setting lightweight_mutation_projection_mode is set to THROW" | same error |
| b. `lightweight_mutation_projection_mode` as a per-query `SETTINGS` | ignored (same error): it is a table setting | same |
| b. table setting `'drop'` | DELETE applies; the projection parts are dropped (0 active), queries stop using it | same |
| b. table setting `'rebuild'` | DELETE applies; projection rebuilt and used | same |
| b. lightweight `UPDATE … SET` | error 48: needs `enable_block_number_column = 1` | same |
| b. `ALTER TABLE … DELETE` / `ALTER TABLE … UPDATE` (mutations) with a projection | work; projection rebuilt and used | same |
| c. `SET allow_experimental_transactions = 1` | error 115 UNKNOWN_SETTING (it is a server-config setting) | same |
| c. `BEGIN TRANSACTION` / `COMMIT` | error 48 "Transactions are not supported"; COMMIT: 649 "There is no current transaction"; the INSERT between them committed alone | same |
| c. `implicit_transaction = 1` | error 48 (also "Async inserts with 'implicit_transaction' are not supported") | error 48 |
| d. parameterised view `WHERE node_internal_id = {node:UInt32}`, `SELECT … FROM v(node = 1)` | works | works |
| d. gate view (scalar watermark subquery, aborted `NOT IN`, `GROUP BY … HAVING sum(sign) > 0`) | correct per node (node 1: `[20]`, node 2: `[10]`; aborted, above-watermark and other-node rows excluded) | same |
| d. sort key used through the view (`EXPLAIN indexes = 1`) | yes: PK on `(node_internal_id, token_category)`, Granules 2/392; passes `max_rows_to_read = 1000` of 100,006 | same |
| e. `index_granularity = 256` accepted | yes | yes |
| e. 100k rows (FixedString(32) + UInt64): marks 256 / 1024 / 8192 | 392 / 99 / 13 marks; marks 1989 / 670 / 186 B; PK in memory 12552 / 3176 / 424 B | 392 / 99 / 13 marks; marks 1900 / 595 / 180 B; PK in memory 0 / 0 / 424 B (Cloud loads the PK lazily) |
| e. point lookup reads ≤ 1 granule | yes (passes `max_rows_to_read` = granularity) | yes |

**Consequences for the design.**
- Dedup tokens (§3.4): on local / self-hosted plain MergeTree they need `non_replicated_deduplication_window > 0`,
  which every agent-written table sets (10000). On Cloud they work by default. Self-hosted production uses
  ReplicatedMergeTree (§6.1), where the replicated window applies (not tested here: no Keeper locally).
- Lightweight DELETE stays off the projected tables (as the plan says). GC by `ALTER TABLE … DELETE` mutation
  works with projections on both. `'rebuild'` mode is an option if lightweight deletes are ever wanted.
- Transactions are unavailable on both, confirming §3's design without them.

## Choices made where the plan was silent

1. Timestamps are `DateTime64(3, 'UTC')` (the agent writes JS `Date`s, ms; Postgres stored µs).
2. Derived sort columns `locking_bytecode_prefix = substring(locking_bytecode, 1, 25)` and
   `nonfungible_token_commitment_key = ifNull(nonfungible_token_commitment, '')` are `MATERIALIZED`, so the
   writer cannot get them wrong and −1 rows always match +1 rows. Projections list them explicitly
   (`SELECT *` omits MATERIALIZED columns). Insert with an explicit column list.
3. `non_replicated_deduplication_window = 10000` on every agent-written table (mirrors the replicated default).
4. `block` and `block_transaction` also get granularity 1024 (point lookups / joins). 256 is the Phase 1
   comparison arm (`sed 's/index_granularity = 1024/index_granularity = 256/'`).
5. Partitioning: `block_transaction` is partitioned like `transaction` (420 M rows; GC by `commit_seq`).
   `block` (1.3 M) and every collapsing / Replacing table are unpartitioned: collapsing only happens within a
   partition.
6. ~~VCMT `version` on acceptance tables is the `commit_seq` of the +1 row; a −1 copies it.~~
   **Superseded by item 24 (WP5a).**
7. ~~On `utxo` / `utxo_by_script`, `version` is a constant (1, as #83).~~ **Superseded by item 24 (WP5a).**
8. `tx_acceptance` has a projection on a VCMT table, which ClickHouse refuses unless
   `deduplicate_merge_projection_mode` is set; it is `'rebuild'`.
9. `commit_log.state` adds `incomplete` (§3.5). `state_rank` is MATERIALIZED from the enum
   (intent 1 < incomplete 2 < committed 3 < aborted 4), so `FINAL` yields the furthest state.
   `kind` adds `horizon_switch` (§3.8). Also `row_counts Map(LowCardinality(String), UInt64)` and `writer_epoch`.
10. `writer_lease` is `ReplacingMergeTree(heartbeat_at)`, not KeeperMap (the local server has no Keeper).
11. `pending_spend` is keyed by outpoint first (probed when a parent's outputs arrive), then node, then spender.
12. `id_reservation` ranges are half-open `[range_start, range_end)`, with `writer_epoch`.
13. Node-scoped views take the node's internal id (`{node:UInt32}`); the API resolves the name via `node_v`.
    A missing `visibility` row means watermark 0 (nothing visible).
14. The aborted-set subquery is not bounded by `visible(n)` (the set is tiny).
15. Input rows whose outpoint is not yet stored: proposed for WP5 that the `fill_pending` commit writes the
    complete `input` row (the child's commit writes only `pending_spend`), because `input` is immutable
    MergeTree and its spent-output attributes cannot be filled in later without a mutation.
16. `node.internal_id` is UInt32 (Postgres `integer`); `node` gains `updated_at` (the Replacing version) and
    `commit_seq`.
17. History tables carry ids only (plan columns).

WP4 amendments (design: `docs/clickhouse-port/wp4-commit-and-visibility.md`):

18. `commit_seq = (writer_epoch << 40) | counter`, counter ≥ 1: each lease epoch owns a disjoint seq range.
19. `commit_void` (aborted seqs) replaces the gate's `commit_log FINAL WHERE state = 'aborted'` scan, which
    would grow with every commit; `commit_log` gains `abort_reason` (`ALTER … ADD COLUMN IF NOT EXISTS`).
20. `epoch_fence(epoch, max_valid_seq)`: rows of an older epoch above its fence are invisible (stale writer).
    Written densely for every epoch below the new lease epoch at takeover; the gate builds a dense array once
    per query (`fence_max_seq`) and checks `commit_seq <= fence_max_seq[commit_seq >> 40]`.
21. `writer_lease` is keyed `(lease_name, epoch, agent_id)` with a server-stamped `claimed_at`, so every claim
    survives merges and the tie-break is deterministic. **Databases created before WP4: `DROP TABLE
    writer_lease` before re-applying** (the `CREATE … IF NOT EXISTS` keeps the old key otherwise).
22. Views are `CREATE OR REPLACE`; every node-scoped and node-agnostic view has a pinned `*_at` twin.

WP5a-core amendments (design: `docs/clickhouse-port/wp5a-core.md`):

23. `input.fungible_token_amount Nullable(Int64)` (after `token_category`): the spent output's FT amount,
    the plan gap WP2 found. `010_core.sql` creates it and also runs `ALTER TABLE … ADD COLUMN IF NOT EXISTS`
    for older databases. The three `input` projections list it. **Databases created before WP5a** keep their
    old projections (`ADD PROJECTION IF NOT EXISTS`): on an empty table run
    `ALTER TABLE input DROP PROJECTION p_outpoint` (and `p_spent_script`, `p_spent_category`) and re-apply;
    on a filled one, re-add and `MATERIALIZE PROJECTION`. Local `cg` was migrated this way (empty) on 2026-10-09.
24. **`version` is the row's own `commit_seq` on every per-node VersionedCollapsingMergeTree table**
    (`node_block`, `node_transaction`, `tx_acceptance`, `utxo`, `utxo_by_script`, `pending_spend`), for
    +1 and −1 rows alike. Items 6/7 let a background merge delete a committed +1 together with a −1 whose
    commit is still open or was aborted: merges ignore `commit_seq`, and the gate cannot restore deleted
    rows (seen in the WP5a e2e re-org test; regression test `[e2e] … merges never collapse an uncommitted
    or aborted −1`). Readers already use `sum(sign)`, so answers are unchanged; pairs written by ONE commit
    still collapse. Cross-commit pairs (re-orgs, tip-mode spends of older outputs, and mempool rows in
    WP5a-mempool) accumulate until a compaction job removes them (wp5a-core.md §2: proposed, not built).
25. `utxo.created_height` / `utxo_by_script.created_height` are always written as 0 (wp5a-core.md
    decision (i)): `unspent_output(node)` (F1g) returns output columns only, and every row of one outpoint
    must carry identical non-key values for the views' `any()` to be exact. Use `tx_acceptance_at` for heights.

## Differences: Cloud 26.6 vs local 26.8

- Every statement in 001–060 was accepted unchanged by both. No statement was rejected by 26.6.
- Cloud rewrites engines to `Shared*` (`SharedMergeTree`, `SharedReplacingMergeTree`,
  `SharedVersionedCollapsingMergeTree`); sort keys, partition keys and projections are identical.
- Dedup tokens work by default on Cloud; locally they need `non_replicated_deduplication_window > 0`.
  Cloud accepts `non_replicated_deduplication_window` silently.
- Cloud loads the primary index lazily (`primary_key_bytes_in_memory` 0 right after insert).
- Lightweight DELETE / UPDATE, mutations, transactions and parameterised views behave the same on both.

## Design review against the per-node checklist

**Satisfied by the schema.**
- **1 Keys.** `node_internal_id` is in the key of node_block, node_transaction, tx_acceptance, utxo,
  utxo_by_script, both histories, visibility and pending_spend.
- **2 Isolation.** VCMT only collapses rows with equal keys, which include the node, so a −1 for node A can
  never cancel node B's +1. Watermarks are per node. Check d shows a node-1 spend not leaking into node 2.
- **3 No any-node shortcuts.** There is no summarised marker (no E15-B column). `utxo` rows are per node.
- **5 Visibility.** Every node-scoped view gates on `visible(n)` and the aborted set.
- **6 History.** Both history tables are keyed by node and carry `removed_at` / `validated_at` / `replaced_at`
  (NULL = confirmed).
- **7 Naming.** Node-scoped views cannot be queried without `node`.

**At risk (for WP3–WP5 to resolve).** *WP5a-core answers: item 25 (created_height), item 24 (collapse
safety), wp5a-core.md §6 (checklist review).*
- **4: `utxo.created_height` is not exact.** The plan writes no UTXO rows when a tx moves from mempool to a
  block, so an output created in the mempool keeps `created_height = 0` after confirmation. Either drop the
  column from answers or take the height from `tx_acceptance_v`.
- **4: exactly-once UTXO rows.** With a constant `version`, a duplicated +1 (an agent bug, not a retry: retries
  are covered by tokens, verified in check a) leaves an outpoint unspent forever. The verifier (§2.3) is the
  only guard.
- **4/5: incomplete commits block the watermark.** *WP4: by design (mempool commits are never incomplete;
  block commits have a bounded lifetime, `staleIncomplete`), see the WP4 doc.* A child whose parent never arrives (an orphan mempool tx)
  keeps its commit `incomplete`, which holds `visible(n)` back indefinitely. WP5 should keep such txs in the
  agent and out of commits, or time them out to `aborted`.
- **5: torn reads across views in one query.** *WP4: resolved by the `*_at` views and `readSnapshot`.* Each view evaluates `max(visible_seq)` on its own, so a query
  joining two node-scoped views (e.g. `utxo_v` and `tx_acceptance_v`) can see two different watermarks if one
  advances mid-query. Option for WP4: give the views a second parameter `{visible:UInt64}` read once per
  request by the API (also the key for §2.4 watermark caching). Verified locally that a watermark parameter
  still uses projection `p_script`.
- **7: base tables reachable.** The API must be granted `SELECT` on the `*_v` views only, and the views need
  `SQL SECURITY DEFINER` (WP4) so the API user needs no rights on base tables. Otherwise `tx_acceptance`
  (sorted by `transaction_hash` first) answers "accepted by some node" for a hash.
- **writer_lease is advisory.** *WP4: epoch fencing (`epoch_fence`) makes a stale writer's later commits
  invisible; residual window in the WP4 doc.* ClickHouse has no compare-and-set, so two agents can both believe they hold
  it. Single-writer is enforced operationally (one deployment) plus the verifier's duplicate-hash count.

**Elements that could answer "accepted by any node" without a node in the key.**
- `tx_acceptance` base table queried by `transaction_hash` alone (key prefix is the hash). Mitigation: views only.
- `block_v`, `transaction_v`, `block_transaction_v`, `output_v`, `input_v`: they answer "stored by
  Chaingraph", which includes replaced, expired and re-orged data; a client could misread presence as
  acceptance. They carry no acceptance fields and their names carry no node (item 7), as today's Postgres
  `transaction`/`block` roots.
- `visibility` row 0 (the node-agnostic watermark): used only by the node-agnostic views, never by a
  node-scoped answer.
- `commit_log.node_scope` (an array, not a key): bookkeeping for the watermark, not an answer.
- `input`'s denormalised spent-output attributes are immutable output facts, not acceptance facts.
- Plan gap: `input` did not denormalise `fungible_token_amount` (the plan's list omits it). *Fixed in WP5a (item 23).*

## UTXO growth and compaction (WP5c, design only, not built)

Full design, cost estimate and test plan: `docs/clickhouse-port/wp5c-hardening.md` §3.

- **Growth.** `version` = own `commit_seq` (item 24) means a +1 and a −1 written by different commits never
  collapse. At mainnet rates (6,000 spends/block, 144 blocks/day) that is up to 1.73 M dead rows per node per
  table per day (≈ 95 MB at #83's 55 B/row), equal to the live set (59.1 M) after about a month.
- **Rejected:** `ALTER … DELETE` of net-zero keys (not atomic across parts: deleting the −1 before the +1
  resurrects a spent output; safe only as two ordered phases, each rewriting most parts) and the anti-pair
  insert (reclaim depends on merges).
- **Proposed:** add `generation UInt32 DEFAULT 0` and `PARTITION BY (generation, intDiv(commit_seq, 65536))` to
  `utxo` / `utxo_by_script`, plus a tiny `utxo_compaction (generation, upto_seq, live_generations, …)` table.
  Gate: `(generation = 0 AND commit_seq > upto) OR has(live_generations, generation)`. A job picks
  S ≤ min `visible(n)` after a safety margin (aligned to a bucket), writes the per-key net (−1/0/+1; nothing for
  0) of the live generations plus agent rows in `(upto, S]` into generation g+1 with `commit_seq = version =`
  the highest committed seq ≤ S, flips with one `utxo_compaction` row (atomic for new snapshots; old snapshots
  keep the old generation), and after another margin drops the old buckets and generations with
  `DROP PARTITION` (also reclaiming aborted and fenced rows). Minor runs daily (seconds), major runs weekly
  (≈ the #83 UTXO build, 5–15 min on 16 vCPU per node).
