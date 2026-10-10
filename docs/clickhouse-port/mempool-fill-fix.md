<!-- cspell:ignore chipnet seqs BCHN dedup subquery -->

# Mempool fill fix: stand-in input rows resolved exactly

Date 2026-10-10. Local only (ch1-local ClickHouse 26.8, ch1-pg). Found by the chipnet lab
([chipnet-lab.md](chipnet-lab.md), first run). Inputs: [wp4-commit-and-visibility.md](wp4-commit-and-visibility.md),
[wp5a-core.md](wp5a-core.md), [wp5a-mempool.md](wp5a-mempool.md), [g1-fix-pass.md](g1-fix-pass.md),
[fix-pass-3.md](fix-pass-3.md), [utxo-off.md](utxo-off.md).

## 1. The bug

`input` carries the attributes of the output it spends (value, locking bytecode, token category, FT amount, NFT
capability and commitment). Postgres joins `input` to `output` when it reads; ClickHouse copies the attributes when
it writes, into an immutable MergeTree.

Chipnet, `chipnet_ch`, transaction `d9444345…` (block 327,303):

| commit | what it wrote |
|---|---|
| `1099511662525` (`mempool_batch`, 18:53:18Z, epoch 1) | the child's 3 `input` rows with value 0, empty bytecode and no token (its parents `3263755e…:0` and `1911b769…:1,2` were unknown); 3 `pending_spend` +1 rows (node 1) |
| agent restart | |
| `2199023255554` (`block` 327,303, epoch 2) | the parents' `output` rows (positions 5 and 11); 3 `pending_spend` −1 rows. The child (position 39) was already stored ("new txs: 45/46"), so no `input` row |

Readers of `input_at` saw value 0 and empty bytecode for those three inputs, for ever. Parity reported it as an
`input_spent` mismatch. The new stand-in check (§4) on that database lists exactly these three inputs (0.9 s on the
full chipnet database).

**Root cause.** No code path ever rewrote a stand-in. wp5a-mempool.md §6 listed it as a known deviation ("a later
repair could rewrite those input rows"). The WP4 fill (rows under the child's own seq) exists only for block
children while their commit is `incomplete`. A mempool commit is never incomplete, so its stand-ins are committed and
visible at once. Pending-spend resolution (`changeRows` `resolutions`, the block fill) wrote only the per-node UTXO −1
and `pending_spend` −1 rows. The restart is not needed: the same happens in one process (test (b) below was red too).

### When a mempool transaction gets stand-ins instead of waiting in the orphan pool

`saveOrPark` (mempool-commit.ts) parks a transaction only if it is **new** (not stored) and some spent output is
in neither the output registry nor `output`. A parked transaction is retried when a save registers one of its missing
outputs (and parks again if others are still missing). It is saved **with stand-ins** when:

1. the grace period ends: `orphanGraceMs`, default 1 s since it was first received; or
2. the pool is full: `maxOrphans`, default 10,000, and the oldest orphan is released early.

A transaction that is already stored never parks and writes no `input` rows. Orphans live in memory only, so a
restart drops them. The node re-announces nothing by itself.

The chipnet case is (1). The agent saves inbound transactions only once its initial sync is complete
(`saveInboundTransactions`, set at 18:52:55Z). It does not ask the node for its mempool. The parents had been
announced earlier, while the sync was running, and never reached the store. Only the child was announced after
tracking began (18:53:18Z), so no parent could arrive within 1 s. This happens at every agent start where the node's
mempool holds chains of unconfirmed transactions, which is common on mainnet. So the stand-in path is not rare.

## 2. Design

The visibility gate works per seq. So the stand-in rows get a seq of their own, and the snapshot decides when that
seq is shown. Nothing on the API read path changes: the views and their parameters are unchanged, and a hidden seq is
one more element of the `void` array.

**Writes** (`stand-in.ts`):

- **Child (owner) C.** A mempool commit that writes a new transaction with unknown spent outputs writes the
  resolved inputs under C, as before. For each unknown parent transaction T it opens a `fill_pending` commit P
  (scope: none) and writes under P:
  - the stand-in `input` rows of the inputs spending T;
  - one `input_stand_in` row per such input: (P, owner C, input, outpoint).

  P commits before C.
- **Resolver R.** The commit that first writes T's outputs writes, under its own seq, the real `input` rows of P's
  inputs and `input_stand_in_resolution (P, R)`. R can be a block commit (`block-commit.ts`, step 4b, for the
  batch's new transactions) or a mempool commit saving T (the mempool path).
- **Rows go in before the `committed` row.** The `input_stand_in` rows are written before P commits; the resolution
  rows before R commits. Dedup tokens are `seq:table:chunk`: P uses chunk `0`, R chunk `s0`, and the owner
  resolving its own group (below) chunk `sc`.

**Read rule** (`visibility.ts` `snapshotSql` step 5; the same rule in `input_v`, and in the parity harness
snapshot `scripts/parity/lib/engines.mjs`). P is hidden in a snapshot iff:

- C is not visible in it, or
- some R resolving P is visible.

"Visible" is the node-agnostic rule of the views: at most `visible(0)` and not void, or in the committed tail; and
not fenced. The hidden seqs are read after the watermarks, tail, void and fences of the same statement, and are
appended to `void`.

**Exactness:**

| state | rows a reader sees for P's inputs |
|---|---|
| C not visible | none (C's transaction is not visible either) |
| C visible, no R visible | the stand-in |
| R visible | the real row only |

- A reader never sees the stand-in and the real row together.
- It never sees the stand-in once R (the commit that stores the parent) is visible.
- It never sees C's transaction without one row per input.

P's own rows exist whenever P is visible (they were written before P committed). R's resolution row exists whenever
R is visible. The `SnapshotCache` stays consistent: a cached snapshot's hidden set was computed from its own
watermarks and tail.

**Who resolves P (in memory).** The registry entry for P is a group (owner C, parent T, inputs). Every step that reads
or changes a group's state is synchronous, so exactly one commit resolves each P:

1. C decides which inputs are unknown in one synchronous pass with the output registry (a T registered since C's
   lookup is used directly), and registers the groups as `open` in the same tick.
2. A save of T that finds a group `open` only records T's outputs on it (`parentOutputs`).
3. Once C has awaited its dependencies, it settles its groups (synchronously):
   - a group with `parentOutputs` is resolved by C itself, under C, so P is never shown;
   - every other group becomes `live`.
4. A save of T that finds a group `live` claims it, writes its rows and depends on C if C is still open (no cycle: C
   has already awaited every dependency it has). If that save fails, the group is `live` again.
5. At startup the store loads every unresolved group:
   - groups whose parent outputs are stored are resolved in one `fill_pending` repair commit (an epoch that stopped
     before resolving them);
   - the rest are registered `live`.

**Failure and recovery.**

- A failed owner aborts its open P's; their rows are void.
- A P committed before its owner failed stays hidden, because its owner is void.
- Recovery aborts a P left at `intent`.
- The writer-side `liveStandInSql` keeps the horizon build (UTXO on, `utxoDeltaInsertSql`) from counting a
  stand-in and its real row twice.

**WP4 rules kept.**

- Every row carries the seq of the commit that wrote it.
- No row of a committed seq is ever changed or voided. Hiding a P is a snapshot decision, not a void.
- The watermark rules are unchanged: P is an ordinary commit, open until it commits.
- Mempool commits are still never `incomplete`.

**Scope.**

- Block children are not covered. Their spends are filled under their own incomplete seq (WP4). Only a block input
  whose parent never arrives within `pendingSpendTimeoutMs` gets a stand-in (under the block's seq, as before). With
  the G1 fixes, a parent of a valid chain cannot arrive later, so that case is test chains only.
- Databases written before this fix keep their stand-ins: those rows are under C and cannot be hidden. The stand-in
  check finds them. Re-sync such a database.

**DDL** (`040_bookkeeping.sql`, README item 27):

- `input_stand_in (stand_in_seq, owner_seq, transaction_hash, input_index, outpoint_transaction_hash,
  outpoint_index, commit_seq)`;
- `input_stand_in_resolution (stand_in_seq, commit_seq)`;
- `input_v` gets the rule.

The snapshot reads both tables, so **apply the DDL (the CLI) before starting this agent version** on an existing
database.

## 3. Cost and limits

- **Snapshot.** One more scalar subquery over two tiny tables per snapshot read. The API reuses snapshots while
  watermarks stand still (fix pass 3), so this is per watermark change, not per request.
- **Writes.** A child with unknown parents costs:
  - one extra commit (2 `commit_log` rows) per unknown parent transaction;
  - 2 small inserts in the child's commit, 2 in the resolver's.

  A block batch with no pending groups pays one map lookup (the registry is empty).
- **Hidden set.** It grows by one seq per resolved stand-in group and is never truncated (compaction would delete
  P's rows and its bookkeeping once the resolver is below every watermark: not built). `readSnapshotMulti` throws
  `GateParameterOverflowError` past 100 KB (about 5,000 seqs). At a few groups per agent restart this is far away, but
  it is the same open item as the void list (wp6b-gate-cost.md).

## 4. The stand-in check

`ClickHouseChecker.standInCheck()` reads one node-agnostic snapshot and returns two lists, which must both be empty:

- `mismatched`: visible inputs (not coinbase) whose visible spent output differs in value, locking bytecode, token
  category, FT amount, NFT capability or commitment;
- `duplicated`: inputs with more than one visible row.

For the lab: `scripts/chipnet-lab/ch1-chipnet-stand-in-check.mjs <agent dir> <url> <database>` prints JSON and exits
1 if either list is non-empty. On the full chipnet database it takes about 1 s.

## 5. Tests

New `stand-in.spec.ts`, `[e2e]`, 12 tests. The first four run with UTXO on and off. Each checks the child's
`input_at` rows per input (value, bytecode, category, FT, capability, commitment) and the stand-in check. The
fixture follows the chipnet case: child x spends u:0 (a token output: FT 50 + mutable NFT), u:1 and v:0, so it has two
unknown parents.

| test | before the fix (dda146c + the new spec and checker) | after |
|---|---|---|
| (a) x in the mempool with stand-ins, agent restart, block with u, v and x | red | green |
| (b) the same without restart; a poller (block steps slowed 50 ms) checks every snapshot: one row per input, real iff its parent output is visible; both states seen (~31 snapshots) | red | green |
| (c1) parents in an earlier block (x still in the mempool), then x's block | red | green |
| (c2) parents arrive in the mempool, u then v (partial resolution) | red | green |
| restart repair: registry forgotten, parents' block leaves 3 stand-ins (the old behavior), the next start repairs them in one commit | red | green |
| race: parents' block while x's commit is paused with its groups `open` (x resolves them) / `live` (the block resolves them and commits after x) | n/a (new fault steps) | green |
| crash at each of x's commit steps (13 points, including between P's and C's commits): stand-in check clean before and after recovery; x absent or with its stand-ins; then the parents resolve it | red | green |

The red run used a frozen export of dda146c with only the new spec and the checker method. The lines asserting the
in-memory registry were removed, and the race tests were skipped because their fault steps do not exist there.

## 6. Found on the way: a block requested while the node was saturated arrived 5 minutes late

The optional UTXO-on chipnet run of this pass (agent c967a0c with the defaults: unbounded in-flight saves, 64
blocks per commit, 200 ms linger) left **59 block inputs with stand-ins and 59 extra stored UTXOs**. These are block
children, not mempool children. `ch1-chipnet-utxo-check.mjs` compares the stored UTXO set with the v1 definition:
`onlyStored` = 59, `onlyV1` = 0. Sequence:

1. At 20:50:53Z the block buffer had 203 blocks reserved. The agent sends at most `maxPendingDownloads` = 200
   requests per node. A block that no node can take is retried after `downloadTimeout`, which is 300 s.
2. Heights 313,822–313,825 waited 5 minutes. The buffer filled with later blocks (976 blocks, 123.5 MB) and stopped
   moving.
3. The children of those four blocks waited `pendingSpendTimeoutMs` (60 s). Then (`pending_spend_timeout` ×2) they
   were committed with stand-in inputs and no UTXO −1 (wp5a-mempool.md §7, the "parent never arrives" rule).
4. At 20:52:30Z the agent logged "initial sync is complete" with the four blocks missing. At 20:55:57Z they arrived
   and were saved. By then the children were final: 59 inputs (37 transactions, 36 parents) show value 0, and 59
   spent outputs stay in `utxo`.

This is a race in the agent's download scheduling, and it is older than this pass. It can hit any ClickHouse sync of
small blocks. The UTXO-off run of the same build happened not to reserve more than 200 blocks at once. Postgres is
unaffected (it joins when it reads).

**Fix** (`agent.ts`): a block that no node can take is retried after 1 s (`noNodeRetryMs`), not 300 s. A download
sent to a node keeps its 300 s timeout.

**Still open.** A block-path stand-in is final. A parent that arrives after `pendingSpendTimeoutMs` does not repair
its children's `input` rows or their UTXO −1 rows. This would need the stand-in groups of §2 for block commits, plus a
per-node UTXO −1 when the parent arrives. After the agent fix, the trigger is a node that takes more than 60 s to
deliver a requested block.
