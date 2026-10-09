# WP5a-core: the ClickHouse store for blocks, headers, re-orgs and the UTXO set

Plan: `paryon_kubernetes/docs/chaingraph/plans/clickhouse-primary-store.md` §1 (per-node checklist), §2.2–§2.3,
§3.5–§3.8, §5.1. Builds on WP4 (`wp4-commit-and-visibility.md`).

Code (`src/store/clickhouse/`):

| File | Contents |
|---|---|
| `clickhouse-store.ts` | `ClickHouseStore implements ChaingraphStore`: lifecycle, `registerNode`, `getAllKnownBlockHashes`, `acceptBlocksViaHeaders`, `removeStaleBlocksForNode`, `getIncompleteBlocks`, the bulk horizon; mempool methods: see `wp5a-mempool.md` (they threw `MempoolNotImplementedError` in WP5a-core) |
| `block-commit.ts` | `saveBlock` as one commit: ids, spend resolution, pending spends, per-node rows; `TransactionRegistry` |
| `utxo.ts` | transition rules, `AcceptanceCounter` (reference model), UTXO row encoders, server-side delta SQL, `OutputRegistry` |
| `node-state.ts` | node registry, operation registry (order, rows-written, commit dependencies), acceptance-table encoders |
| `mempool-state.ts` | skeleton for WP5a-mempool; the block-acceptance hook |
| `index.ts` | exports; `src/store/index.ts` wires `CHAINGRAPH_STORE=clickhouse` |

DDL amendments (`ddl/README.md` items 23–25): `input.fungible_token_amount`; `version` = own `commit_seq`;
`created_height` always 0.

## 1. One call, one commit, per node

Every store call that changes facts is one WP4 commit whose `node_scope` is exactly the nodes whose facts it
writes. Rows go in plan §3.1 order (`output`, `input`, `transaction`, `block`, `block_transaction`, then
`node_block`, `tx_acceptance`, `node_block_history`, then `utxo`/`utxo_by_script`), each one deduplicated insert
`seq:table:chunk`; then `markCommitted`. A node's rows are keyed by that node only.

| Call | Commit | Per-node rows (node *n* only) |
|---|---|---|
| `saveBlock` | `block`, scope = accepting nodes not already accepting the block | `node_block` +1, `tx_acceptance` +1 per block tx, UTXO transitions (§3), `pending_spend` ±1 |
| `acceptBlocksViaHeaders` | `header_accept`, scope [n] | `node_block` +1 (RowBinary), `tx_acceptance` +1 and UTXO +/− (server-side `INSERT … SELECT` from `block_transaction` / `output` / `input`) |
| `removeStaleBlocksForNode` | `reorg`, scope [n] | `node_block` −1, `node_block_history`, `tx_acceptance` −1, UTXO inverse (server side) |
| `prepareForInitialSync` | `horizon_switch`, scope [] | none |
| `finishInitialSync` | `utxo_build`, scope = nodes with bulk-period rows | UTXO rows the bulk period skipped |

Postgres semantics kept: `saveBlock` returns `attemptedSavedTransactions` = txs with `isSavedTransaction` false and
`transactionCacheMisses` = those that already existed; a block a node already accepts is a no-op for that node
(`ON CONFLICT DO NOTHING`); `acceptBlocksViaHeaders` returns the number of acceptances added and writes
`accepted_at` NULL for blocks with `timestamp < acceptedAt − 2 h` (same rounding); `removeStaleBlocksForNode`
does not return txs to the mempool and ignores unknown or not-accepted hashes; `removedAt` defaults to now.
A block tx that is missing from the store is inserted even if `isSavedTransaction` says it was saved (Postgres
would fail the linked-count check).

## 2. `version` is the row's own `commit_seq` (finding; DDL item 24)

WP2's rule (a −1 copies the version of the +1 it cancels; `utxo.version` constant 1) is unsafe:
VersionedCollapsingMergeTree deletes an equal-key, equal-version ±1 pair in any background merge, ignoring
`commit_seq`. A re-org's −1 rows are on disk before the re-org commits, so a merge can delete the committed +1
together with a −1 whose commit is still open (the release becomes visible early) or later aborted (the
acceptance is lost forever; the gate cannot restore deleted rows). The first WP5a e2e run hit it: a merge
collapsed a re-org's `node_block` −1 with its +1 within milliseconds, so the re-org's own server-side
statements found no stale block.

Rule now: every per-node row's `version` is its own `commit_seq`. Collapsing happens only between rows of one
commit (an output created and spent in one block, the horizon build), which is atomic. Readers already use
`sum(sign)`, so answers do not change. Regression test: `[e2e] … merges never collapse an uncommitted or aborted
−1 with a committed +1` (forces `OPTIMIZE … FINAL` on all four tables mid-crash and after recovery).

Cost: cross-commit pairs no longer collapse by themselves (re-orgs: rare; tip-mode spends of older outputs:
about one pair per spend; mempool confirmations in WP5a-mempool: one pair per tx and node). **Proposed
compaction (not built):** for a +1 (version A) and −1 (version R) where both commits are committed and below
every node's published watermark, insert in ONE insert (one part, so physically all-or-none) the pair
(+1 version R, −1 version A) with `commit_seq = 0` (always visible, net zero). Merges then delete all four rows;
at every intermediate merge state the visible sum is unchanged. It must never pair with an open or aborted
commit. Until then `utxo` grows with tip-mode spends; WP6/WP7 should measure the read cost.

## 3. UTXO decisions

**(i) `created_height`.** F1g `unspent_output(node_name)` (`origin/perf/unspent-output-root`,
`1791300000000_unspent_output_root/up.sql`) `RETURNS SETOF output`: output columns only, no height. So parity
with F1g does not need it. Every row of one outpoint must carry identical non-key values for the views'
`any(...)` to be exact, and a spend's −1 row cannot cheaply know the creator's height for that node (it differs
per node and per re-org). Rule: **`created_height` is always 0**; heights come from `tx_acceptance_at`. The
column stays (WP4 test-support writes it); it is a candidate for removal.

**(ii) `input.fungible_token_amount`** added (DDL item 23), written from the resolved spent output, also in fill
rows. `row-encoders.spec.ts` updated (column check against the DDL, byte sizes).

**(iii) Sign rules.** For node *n* and tx *t*, `acc(n, t)` = *t* has at least one live container for *n*
(an accepted block of *n* containing *t*, or *n*'s mempool). Only `acc` transitions emit rows:
0 → 1: `+1` per output of *t*, `−1` per outpoint *t* spends; 1 → 0: the inverse. Each row copies the output facts
from `output` (or the in-memory registry, which holds the same facts), with `sign` and `version = commit_seq` the
only differences. Hence `sum(sign)` for (n, o) = acc(n, creator) − Σ acc(n, spender) over *every* ordering and
merge state, which is 0 or 1 whenever "spender accepted ⇒ creator accepted" holds and at most one spender of *o*
is accepted by *n* (true for a valid chain plus a consistent mempool). The store keeps the first condition at
every visible watermark by commit dependencies (§4). Where *acc* comes from:
- block save (tip mode): `acc` before = a live `tx_acceptance` row of *n* for *t* (another block or the mempool),
  read after every earlier operation of *n* has written its rows; new txs cannot be accepted yet;
- header acceptance: the same test server side, excluding the commit's own rows;
- re-org: *t* stops being accepted iff no live `tx_acceptance` row of *n* remains outside the stale blocks;
- horizon build: `acc(now) − acc(before bulk start)` per (n, t) (§5).
Duplicate txs (BIP30 coinbases, fork blocks sharing txs) count as containers, so they emit once.

Pure state-machine test (`utxo.spec.ts`): 40 seeds × 60 random accept / suffix-release / re-accept steps over two
nodes, a fork block sharing txs with the main chain and a BIP30 duplicate coinbase, rows applied in shuffled
order; after every step every (node, outpoint) sum is 0 or 1 and equals the definitional `unspent(n, o)`. A
second test shows a spender applied before its creator reads −1, which is why child commits depend on parents.

## 4. Ordering, dependencies and child-before-parent

**Operation registry** (`node-state.ts`). Each call registers an operation synchronously, so registration order
is the agent's call order. An operation's predecessors are the live operations registered earlier that share a
node. `rowsWritten` resolves once all its rows (including fill rows) are acknowledged; `committed` resolves on
commit and rejects on failure.
- Operations that decide per-node facts from stored state (tip-mode block saves, any re-save of a stored block,
  header acceptance, re-org) first wait for their predecessors' `rowsWritten`, so they read every earlier
  decision for their nodes. Bulk-mode saves of new blocks decide nothing from stored state and do not wait.
- An operation depends on every open operation whose rows it read: the owner of an in-flight transaction or
  output it reuses, the writer of a live `tx_acceptance`/`node_block` row it relied on, and (header acceptance,
  re-org) all predecessors. Before `markCommitted` it awaits their commits; if one failed it aborts itself
  (`DependencyFailedError`). Hence a commit is visible only with everything it read, and an abort never leaves
  a decision that assumed the aborted rows.
- Mode switches (`prepareForInitialSync`, `finishInitialSync`) are exclusive: new operations wait, live ones drain.

**Spend resolution.** For each input: (a) the output registry (outputs of open operations, pinned, then a
bounded cache of committed ones), (b) one batched `output` lookup (valid commits, chunked; rows of open commits
included, with a dependency), (c) the registry again. Transaction ids: the transaction registry (pinned while
the writing operation is live, so concurrent saves of the same tx share one id), then a batched `transaction`
lookup that ignores void and fenced rows (WP4 §8), then new ids from the allocator.

**Child-before-parent** (WP4 `fill` scheme). Unresolved spends are written as `pending_spend` +1 rows (one per
node whose UTXO −1 waits on it, or node 0), the commit is marked `incomplete`, and the save waits (bounded,
`pendingSpendTimeoutMs`, default 60 s) until any save registers the outpoint. Lookup and subscription happen in
one synchronous pass, so a registration in between is not missed. Then, under the child's seq with tokens
`seq:table:f0`: the missing `input` rows, the UTXO −1 rows, the `block` row (its `generated_value_satoshis` needs
every spent value), and `pending_spend` −1. The child depends on the parent, so it commits after the parent;
while the child is open it holds the watermark of its nodes, so readers see neither or both, whatever the seq
order. Outputs are registered when a save starts, before it waits for anything, so no wait cycle forms: a
parent registered after a child does not wait for the child's commit, only for its rows, and the child's rows
need only the parent's registration. On timeout the child aborts and `saveBlock` rejects (the agent treats
that as fatal and restarts; recovery voids it).

## 5. The bulk horizon (plan §3.8, §5.1)

`prepareForInitialSync` (called on every agent start) writes a `horizon_switch` commit; its seq S is the bulk
start, and `init()` resumes bulk mode if the latest committed horizon commit is a switch (a restart mid-sync).
In bulk mode commits write the base tables, `node_block` and `tx_acceptance` (and re-org rows) but no UTXO rows.
`finishInitialSync` drains, then writes ONE `utxo_build` commit: per node with `node_block` rows at or above S,
per height batch (`horizonBatchHeights`, default 10,000), `INSERT … SELECT` of `d = acc(now) − acc(before S)` for
every tx with bulk-period acceptance rows, assigned to the batch of its lowest bulk-period height (so each
(node, tx) is emitted once). Rows before S were emitted inline, so the result is exact; outputs created and spent
inside the bulk period collapse within the build commit. One commit makes the build all-or-none for readers (it
holds the watermark of the built nodes while it runs); a crash aborts it and the next start rebuilds. Projections
are declared on the empty tables and maintained on insert, so nothing is materialized; progress goes to
`onIndexProgress`. During the bulk period a node's `utxo` lags its acceptance facts (plan §3.8).

## 6. Review against plan §1 checklist items 1–6

| # | Item | How WP5a-core meets it | Residual risk |
|---|---|---|---|
| 1 | Keys | Every row written carries the node in its key: `node_block`, `tx_acceptance`, `utxo`, `utxo_by_script`, `node_block_history`, `pending_spend` (node 0 only for node-agnostic pending inputs). | None found. |
| 2 | Isolation | Commit scope = the nodes written; header acceptance and re-org are scoped to one node and their server-side SQL filters `node_internal_id = {node}` in every subquery. Tested: node 1 re-org and re-acceptance leave node 2's facts byte-identical; node 2 header acceptance leaves node 1 identical. | Node-agnostic rows (`input`, `output`) are shared by design (item 7). |
| 3 | No any-node shortcuts | No summarised value: "already accepted" reads `tx_acceptance` of the node itself; the mempool hook is per node. | None. |
| 4 | Exact under every transition | Sign rules §3 (random-order unit test), version rule §2 (merge test), dependencies §4, crash injection at every step of block save (12 points), re-org (6) and header acceptance (5): no partial facts before or after recovery, sums 0/1. Child-before-parent under concurrency tested with a torn-read poller. | Mempool transitions are WP5a-mempool. Compaction is not built, so storage grows with cross-commit pairs. A dependency cycle (child waits for a parent that depends, through a header acceptance, on the child) ends in the pending timeout, not a hang; needs a fork plus header acceptance plus out-of-order download at once. |
| 5 | Visibility per node | One commit per call; a node's watermark waits only for open commits whose scope contains it; incomplete children hold only their nodes. Torn-read poller saw no child-without-parent and no tx/UTXO mismatch per node. | A reader of node *n* waits for the slowest open commit of *n*, including a pending child (bounded by `pendingSpendTimeoutMs`) and the horizon build (minutes at mainnet scale). |
| 6 | History per node | `node_block_history` per node with `accepted_at` of the released acceptance and `removed_at` (argument or now), one id per released block; written in the re-org commit. | `node_transaction_history` is WP5a-mempool. |

## 7. In-memory state

- **Node registry:** name ↔ internal id (loaded from `node FINAL` at init, upserted by `registerNode`).
- **Operation registry:** live operations with kind, nodes, seq, predecessors (dropped once rows are written),
  `rowsWritten`/`committed`. The owner of a seq is looked up among live operations.
- **Output registry:** outpoint → output facts + id promise + owner (pinned while the owner is live), a bounded
  insertion-order cache of committed outputs (`recentOutputCapacity`, default 500,000), waiters per outpoint.
- **Transaction registry:** hash → id (+ owner while pinned), bounded cache of committed ids (default 1,000,000).
- **Mode:** `tip` or `bulk` with the bulk start seq; the epoch fence array (loaded at init); the exclusive gate.
- **Mempool state (skeleton):** per-node `Map<TxKey, MempoolEntry>` (empty), `orphans`, `tracking`, and
  `planBlockAcceptance(node, inclusions)`, which calls `planAcceptedBlockCleanup` when the node's mempool is not
  empty and refuses a non-empty plan (no rows written for it yet).

## 8. What WP5a-mempool must fill in

1. `saveMempoolTransaction`, `recordNodeValidation`, `archiveMempoolTransactionsAcceptedByBlocks`,
   `getMempoolTransactionsExpiringBefore`, `archiveMempoolTransaction` (all throw `MempoolNotImplementedError` now).
2. `MempoolState`: rebuild per-node mempools at `init()` from `node_transaction_at` ⋈ `input`; the orphan pool
   and its expiry; register mempool tx outputs in the output registry and ids in the transaction registry
   (owner = the mempool operation) so blocks and children resolve them; register mempool commits in the
   operation registry (kind `mempool`).
3. `planBlockAcceptance` (block-commit.ts calls it per accepting node, inside the block commit): write the
   planned archives as rows of the same commit: `node_transaction` −1, `node_transaction_history`
   (`replaced_at` NULL for confirmations), `tx_acceptance` mempool row −1, and for conflicts and their cascade
   the UTXO inverse rows. Confirmed txs are already "accepted before" through their mempool `tx_acceptance` row,
   so the block emits no UTXO rows for them.
4. `assertNoMempoolForHeaderAcceptance` → the same cleanup for header-accepted blocks (inclusions from the store).
5. Mempool rows must follow §2: `version` = own seq.

## 9. Deviations from the plan

1. `version` = own `commit_seq` everywhere (§2); the plan and WP2 relied on cross-commit collapse.
2. `created_height` is always 0 (§3 (i)).
3. Horizon build is one commit for all nodes, not one `utxo_build` commit per height range (all-or-none instead of
   resumable); bulk mode ends at `finishInitialSync`, not at a tip − 1,000 height horizon, and blocks are not
   coalesced into multi-block commits (WP6 measures whether that is needed).
4. `pending_spend` is bookkeeping (net zero after the fill); the backlog itself is in memory, and recovery
   re-runs the save rather than replaying `pending_spend`.
5. `getIncompleteBlocks` reports (a) committed blocks failing the Postgres size check and (b) blocks whose
   block commit was aborted and that are not stored since (linked size 0), not every `intent` commit.
6. `poolStats` reports live store operations (there is no connection pool); `max` is 0.
7. Row counts in `commit_log` cover RowBinary inserts only (server-side `INSERT … SELECT` counts are not returned
   by the client).

## 10. Test results (local ClickHouse 26.8.22.13, 2026-10-09)

`yarn build` clean; eslint, prettier and cspell clean for every WP5a file. `yarn test:unit`: 85 passed, 1 todo.
It exits 1 only because of AVA's "No tests found in build/store/clickhouse/test-support.js" (the default
`test-*.js` glob matches WP4's helper; present since WP4 commit 81ea332, not changed here). With
`CHAINGRAPH_E2E_CLICKHOUSE_URL=http://localhost:18123`, all `build/store/clickhouse/*.spec.js` (WP4, WP5b
checker, WP5a): 78 passed in two consecutive full runs (77 in two earlier runs before the last test was added); every WP5a `[e2e]` test creates and drops `ch1_wp5a_*`.

WP5a tests: 16 new unit tests (utxo 7, block-commit 3, node-state 3, mempool-state 1, store helpers 2; the
row-encoders spec is updated for the new column) and 8 `[e2e]`:

| `[e2e]` test | Checks |
|---|---|
| 3-block chain for 2 nodes | per-node blocks, txs, `utxo_at` and `utxo_by_script_at` equal the definitional sets; input FT amounts; `accepted_at`; Postgres return values; block values; chain restore; repair scan clean; mempool stubs |
| re-org of node 1 | node 2 byte-identical; node 1 blocks/txs/UTXOs; history row; competing block with a shared tx; re-org back via headers; sums 0/1 |
| header acceptance by node 2 | 3 acceptances, NULL `accepted_at` for old blocks, UTXOs equal node 1's, node 1 untouched, repeat returns 0 |
| child-before-parent | child commit `incomplete` while waiting; concurrent parent; poller: no torn read on either node; exact final state; `pending_spend` nets to 0 |
| crash injection | 23 crash points (block 12, re-org 6, header 5): all-or-none before recovery, exact after, commit aborted or committed, sums 0/1 |
| merges never collapse | aborted re-org + `OPTIMIZE FINAL` ×2 + committed re-org: facts exact |
| bulk horizon | no UTXO rows in bulk; restart resumes bulk; one build exact for both nodes (incl. a bulk re-org); tip mode after; a later empty bulk period changes nothing |
| aborted block repair scan | an aborted block is reported with linked size 0 and is not a known hash; re-saving it clears it |
