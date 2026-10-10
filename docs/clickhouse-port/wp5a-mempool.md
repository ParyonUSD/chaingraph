# WP5a-mempool: per-node mempools in the ClickHouse store

Builds on WP5a-core (`wp5a-core.md`) and WP4 (`wp4-commit-and-visibility.md`). The pure planner
(`src/store/mempool-graph.ts`, pinned by the 7 vectors in `mempool-cleanup.vectors.ts`) decides every archive; the
store only turns plans into rows. Nothing here forks the planner's logic.

| File | Contents |
|---|---|
| `src/store/clickhouse/mempool-state.ts` | `MempoolState`: per-node mempools + indexes, shared tx facts, orphan pool, planning (`planBlockAcceptance`, `planValidation`, `planExpiry`), `apply` |
| `src/store/clickhouse/mempool-commit.ts` | `MempoolCommitter`: the five store methods, the orphan pool, rebuild at `init()`, `changeRows` (plan → rows), the row encoders for `node_transaction` / `node_transaction_history` |
| `block-commit.ts`, `clickhouse-store.ts` | block save and header acceptance plan and write the accepting node's cleanup inside their own commit |
| `mempool-commit.spec.ts` | `[e2e]` tests (§7); fixtures shared with `clickhouse-store.spec.ts` in `spec-fixtures.ts` |

## 1. Rules

- **Every fact is per node.** A node's mempool is its live `node_transaction` rows; membership is also written to
  `tx_acceptance` with `block_internal_id = 0` (the checker's `forgetNodeValidation` and `txAccepted` rely on both).
  `node_transaction`, `tx_acceptance`, `node_transaction_history`, `utxo`, `utxo_by_script`, `pending_spend` rows all
  carry `node_internal_id`; a save for node A never writes a row for node B.
- **One call, one commit**, `node_scope` = the nodes whose facts it writes (`mempool_batch` for saves, validations
  and the sweep; `expiry`; block/header cleanup rides in the `block` / `header_accept` commit). Readers of node *n*
  see all of a save's facts for *n* or none (WP4 gate; crash-tested at 44 points, §7).
- **`version` = the row's own `commit_seq`** (WP5a-core rule 24) on every per-node row. Equal-key ±1 pairs inside
  one commit (e.g. `a:1 +1` from an archived spender and `a:1 −1` from its replacement) may collapse at insert or
  merge; they are in one commit, so that is atomic and the sums never change.
- **Mempool commits are never `incomplete`** (WP4 §3): a new tx spending an unknown output waits in the orphan pool
  (§4) and writes nothing until it is released.
- **Timestamps** are the agent's `Date`s (`validatedAt`, `replacedAt`, `acceptedAt`); confirmations write
  `replaced_at` NULL.

## 2. State shape

**Stored, per node *n*:**

| Table | Live row means | Key (node first) |
|---|---|---|
| `node_transaction` | tx in *n*'s mempool (`validated_at`) | (node, tx id) |
| `tx_acceptance`, block 0 | same membership, as an acceptance container (UTXO sign rules, WP5a-core §3) | (node, tx, block 0) |
| `node_transaction_history` | one archived validation (`validated_at`, `replaced_at`), insert-only | (node, tx id, history id) |
| `utxo` / `utxo_by_script` | *n*'s unspent outputs, mempool included | (node, …, outpoint) |
| `pending_spend` (node ≠ 0, spender) | a mempool tx of *n* spends an output that was unknown when it entered *n*'s mempool; its UTXO −1 is outstanding | (outpoint, node, spender) |

**In memory (writer only), rebuilt at `init()`:**

- `MempoolState.node(n)`: `NodeMempoolState` = `txs: Map<txHash, {internalId, validatedAt, spends, unresolved}>`
  plus incrementally kept indexes `spenders` (outpoint → txs), `children` (tx → txs), `waitingOnCreator`
  (creator tx → txs with an outstanding spend of it). This is exactly the planner's `NodeMempool` +
  `MempoolGraphIndex`.
- Shared facts of every tx some node holds (outputs as UTXO rows, spent outputs), reference-counted by membership.
- Orphan pool (`orphans`: hash → validations per node, missing parents, `done`), bounded by `maxOrphans`.
- `recentlyArchived` (tx → node → `replaced_at`, bounded): read only for released orphans (§4).
- Modifier registry: live operations that change node *n*'s mempool; later operations of *n* depend on them, and a
  failed one marks *n* `stale`: the next operation of *n* waits for *n*'s earlier operations and rebuilds *n* from the
  store before planning.

**Rebuild** (`init()`, and per stale node): live `node_transaction` rows (`sum(sign) > 0`, valid commits only, i.e.
through the gate: void and fenced seqs excluded), the members' `output` / `input` rows, the spent outputs, and live
`pending_spend` rows (→ `unresolved`). Orphans are memory only; a restart drops them (the node announces again).

## 3. End state per transition

Rows written for node *n* in one commit, all with `version = commit_seq` (observed in `mempool-commit.spec.ts`;
"tx" = the tx itself, "out" = each of its outputs, "spent" = each output it spends).

| Transition | `node_transaction` | `tx_acceptance` (block 0) | `node_transaction_history` | `utxo` + `utxo_by_script` | `pending_spend` |
|---|---|---|---|---|---|
| addition (save / `recordNodeValidation`) | tx +1 | tx +1 | – | out +1, known spent −1 | unknown spent +1 |
| replaced (by a new validation of *n*) | −1 | −1 | (validated_at, new tx's validated_at) | out −1, written spent +1 | outstanding −1 |
| descendant (cascade of a non-NULL archive) | −1 | −1 | (validated_at, seed's replaced_at; MIN over seeds) | out −1, written spent +1 | outstanding −1 |
| conflict (block of *n* spends an outpoint) | −1 | −1 | (validated_at, MIN(accepted_at)), NULL if all NULL (vector 6) | out −1, written spent +1 | outstanding −1 |
| confirmed (block of *n* contains the tx) | −1 | −1 | (validated_at, NULL) | none (still accepted via the block) | outstanding −1; or, if the output is known now, the resolution row |
| expired (`archiveMempoolTransaction`) | −1 | −1 | (validated_at, given replaced_at) | out −1, written spent +1 | outstanding −1 |
| resolution (creator becomes accepted by *n*: block, header or mempool) | – | – | – | spent −1 | −1 |
| validation of a tx *n* already confirms | – | – | (validated_at, NULL) | – | – |
| validation of a tx conflicting with a block of *n* | – | – | (validated_at, MIN(accepted_at) or NULL) + cascade | – | – |
| released orphan whose waited-for parent was archived for *n* (§4) | – | – | (validated_at, parent's replaced_at) + cascade | – | – |

Logged examples (spec run, node ids n1/n2):

- add p for n1, n2: `node_transaction n1 p +1, n2 p +1; tx_acceptance n1 p +1, n2 p +1; utxo n1 a:1 −1, p:0 +1; n2 a:1 −1, p:0 +1` (+ `utxo_by_script` mirror).
- r replaces p (n1), q cascades: `node_transaction n1 p −1, q −1, r +1; tx_acceptance` same; `utxo n1 q:0 −1, r:0 +1` (p:0 and a:1 pairs collapsed in-commit); `history n1 p, q replaced_at = r.validated_at`.
- block confirming p accepted by n1 (r conflicts): `node_transaction n1 r −1; tx_acceptance n1 r −1; utxo n1 p:0 +1, r:0 −1, coinbase:0 +1; history n1 r replaced_at = accepted_at` (+ the block's own `node_block` / `tx_acceptance` rows).
- header acceptance of that block by n2 (p confirmed): `node_transaction n2 p −1; tx_acceptance n2 p −1; history n2 p NULL; utxo n2 coinbase:0 +1`; n2's q stays.
- expiry of p for n2 (q cascades): `node_transaction n2 p −1, q −1; tx_acceptance` same; `utxo n2 a:1 +1, q:0 −1`; `history p, q replaced_at = given`.
- x with unknown parent u after the grace period: `node_transaction n1 x +1; tx_acceptance n1 x +1; utxo n1 x:0 +1; pending_spend n1 u:0 +1`; block creating u accepted by n1: `utxo n1 c1:0 −1` (u:0 +1/−1 collapsed), `pending_spend −1`.

## 4. Orphan pool

A new tx (not stored) with a spent output that is in neither the output registry nor `output` is parked: no commit,
`saveMempoolTransaction` resolves once the parked attempt is saved. Later saves/validations of it merge their
node validations (first per node wins). It is retried when any save registers a missing output (a block or another
mempool tx), or when the grace period since first receipt ends (`orphanGraceMs`, default 1 s); when the pool is full
(`maxOrphans`, default 10,000) the oldest is released early. A tx released by the grace period is saved as
Postgres saves it (at once, inputs unresolved): its unknown spends become `pending_spend` +1 rows, resolved when the
creator becomes accepted by that node (block, header acceptance or mempool), dropped if the tx leaves the mempool.

**Arrival order.** Postgres stores the child first; if the parent is then archived for node *n* with a non-NULL
`replaced_at` (conflict with an accepted block, replacement, expiry), the history cascade archives the child too. The
pool reverses that order, so a released orphan for which a waited-for parent was archived for *n* is archived on
arrival for *n* with the parent's `replaced_at` (precedence as in Postgres: confirmation NULL > its own direct conflict
> inherited). Tested: `[e2e] mempool: an orphan whose parent arrives conflicting …`.

## 5. Postgres parity (G1)

Mirrored exactly through the planner:

- `archiveMempoolTransactionsAcceptedByBlocks`: per node with a non-empty mempool, `loadInclusions` reads that node's
  accepted inclusions of mempool txs and of other txs spending mempool outpoints (`tx_acceptance` block ≠ 0, with the
  node's `accepted_at`), then `planAcceptedBlockCleanup`; one `mempool_batch` commit per node with work. Returns the
  direct rows (confirmed, conflict) as `{hash, nodeName, replacedAt}`, ordered by node name then hash (Postgres's
  `ORDER BY "nodeName", "hash"`); descendants are written but not returned (they come from the trigger in Postgres).
- `trigger_node_block_insert` → the same plan inside `saveBlock` (per accepting node not already accepting the block,
  inclusions = the block's txs, `accepted_at` as given) and `acceptBlocksViaHeaders` (inclusions read from
  `block_transaction`, `accepted_at` NULL for blocks older than accepted − 2 h, as `node_block`).
- Vector 6 (NULL `accepted_at` conflict → `replaced_at` NULL, no cascade) and vector 7 (a direct conflict keeps its
  own `replaced_at` inside a cascade) hold because the planner decides them; header-accepted old blocks are the real
  source of NULL `accepted_at`.
- `trigger_node_transaction_insert` → `planMempoolReplacement` with the new entry included (AFTER INSERT view):
  same-outpoint spenders archived `replaced` at the new tx's `validated_at`, then cascade.
- `archiveMempoolTransaction` → `planMempoolExpiry` (returns 1, or 0 if not in the node's mempool);
  `getMempoolTransactionsExpiringBefore` from memory, `expiresAt = validatedAt + expirationMs ≤ expiresBefore`,
  ordered by `expiresAt`, node name, hash.
- `saveMempoolTransaction`: base facts once (`output`, `input`, `transaction`; concurrent saves share one id via the
  transaction registry), then per node ON CONFLICT DO NOTHING (already in the node's mempool → nothing);
  `recordNodeValidation` of an unknown tx writes nothing (Postgres's join inserts nothing).

**End state, not intermediate state, for txs the node has already settled.** A validation of a tx that the node
already accepts in a block, or that conflicts with a block it accepts, never enters the mempool: it goes straight to
history with the sweep's `replaced_at`. Postgres inserts the `node_transaction` row and archives it at the next sweep
(or never, if no sweep runs), so between the save and the sweep Postgres shows it in the mempool and its sweep returns
it; the ClickHouse sweep does not list it. The settled end state (mempool, history, UTXOs) is the same.

**Nondeterministic in Postgres; deterministic rule here:**

1. Two validations of the same tx by the same node racing (two connections, ON CONFLICT DO NOTHING): the first in
   store call order wins (`validated_at` of the first call).
2. Concurrent `trigger_node_transaction_insert` for two conflicting txs on one node committed by different
   connections (each trigger may or may not see the other's row): here they are planned in call order, so the later
   one replaces the earlier.
3. A block accepted while a conflicting mempool insert is in flight (trigger vs sweep visibility): here the block and
   the mempool save are ordered by the operation registry (call order), and the later one plans against the
   earlier's result.
4. Orphan arrival (§4): Postgres's result depends on arrival order; the pool reproduces the child-first order.

## 6. Known deviations and residual risk

- **Stand-in spent values.** An input whose spent output was unknown when written (an orphan released by the grace
  period; a block input after `pendingSpendTimeoutMs`) carries `value_satoshis = 0`, empty bytecode, no token on its
  `input` row (`input` denormalises the spent output; Postgres joins at read time). If the parent arrives later the
  UTXO set is fixed (pending spend resolution) but the `input` row keeps the stand-in. Rare (BCHN does not relay
  orphans); a later repair could rewrite those input rows. **Fixed for mempool transactions** in
  [mempool-fill-fix.md](mempool-fill-fix.md): the stand-in rows have their own seq, which the snapshot hides once
  the commit storing the parent is visible; that commit carries the real rows. Not rare after all: the agent does
  not fetch the node's mempool when tracking starts, so every child announced after a start whose parents were
  announced before it gets stand-ins.
- **Orphan validations merged while parked** count as received at park time for the inherited-archive rule (§4); a
  node whose validation arrives after the parent's archive would, in Postgres, keep the child in its mempool.
- **`forgetNodeValidation`** (checker fault injection) cancels rows behind the writer's back; the writer's memory still
  holds the entry until the next restart (rebuild). Only used by the e2e double-validation check.
- Orphans and `recentlyArchived` are memory only (lost on restart; the node re-announces).
- Mempool UTXO pairs are cross-commit (+1 at validation, −1 at expiry/replacement): one pair per tx and node that
  leaves unconfirmed, until the WP5a-core compaction exists.

## 7. Other fixes made with this work

- `saveBlock` of a child waiting for a parent reports the block as handled once its commit is `incomplete` (the agent's
  bounded block buffer keeps downloading the parent); after `pendingSpendTimeoutMs` a still-unknown spent output is
  stored with the stand-in (§6) instead of failing the save.
- Shutdown: an `AbandonSignal` (SIGINT/SIGTERM via `createStore`, and `close()`) aborts waits and uncommitted work;
  abandoned block saves resolve (aborted, re-downloaded next start), orphans are dropped.
- Incomplete-block repair re-inserts missing `block_transaction` links (the e2e `dropBlockTransactionLink` case).
- Concurrent saves of one block (the agent saves genesis once per node) are serialised; one `block` row.
- `getAllKnownBlockHashes` works on a store that never ran `init()` (reads `block_v`; the e2e calls it that way).
- Array query parameters hold at most 1,000 hashes (2,000 exceeded `http_max_field_value_size`, 128 KiB, on a
  2,000-block header acceptance). The client keeps up to 64 sockets.

## 8. Tests and results (local ClickHouse 26.8.22.13, 2026-10-10)

`mempool-commit.spec.ts` (`[e2e]`, scratch `ch1_wp5a_*`, dropped):

| Test | Checks |
|---|---|
| replacement cascades per node; block and header acceptance clean only the accepting node | rows and `version` per transition; r replaces p, q cascades on n1 only; n2 byte-identical; block (n1: r conflict) and header acceptance (n2: p confirmed, q stays); sweep then returns nothing; memory = store |
| expiry | expiring list order; n2 p + q archived with the given `replaced_at`; second call 0; n1 untouched |
| orphan waits for its parent | no commit while parked; second node's validation merges; parent in the mempool releases it; parent first seen in a block releases it |
| orphan whose parent arrives conflicting | child archived for n1 with the parent's `replaced_at`; n2 keeps both |
| orphan after the grace period | saved with `pending_spend` +1; a later block creating the parent writes the −1 and nets `pending_spend` to 0 |
| two nodes validating concurrently | one `transaction` row, one `output` row, first validation per node wins, equal UTXO sets |
| crash between any two steps | 44 crash points over a new-tx save for 2 nodes, a replacement + cascade, a block confirming a mempool tx, an expiry: each node sees before or after (never partial) before and after recovery; recovered memory = store; UTXO sums 0/1 |

Every test also checks `badUtxoSums` (every (node, outpoint) sum is 0 or 1).

e2e harness changes (ClickHouse path only): `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS=1` (the mockchain's inputs
spend outputs that never exist, which Postgres stores at once); "completes initial sync" also waits for
`Agent: enabled mempool tracking.` on ClickHouse (on Postgres a skipped `[postgres]` test waited for it, and the next
tests announce txs that are ignored before tracking starts). `clickhouse-store.spec.ts`'s mempool-stub
assertion became a no-op save check. AVA's globs exclude `test-support.*`.
