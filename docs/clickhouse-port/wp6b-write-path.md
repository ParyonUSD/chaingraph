# WP6b-A: the ClickHouse write path after the WP6 measurement

Fixes for `wp6-local-measurement.md` §9 items 1–6, 8, 9 (store side) and the lease-loss wedge (§9 item 3).
Code: `src/store/clickhouse/{block-commit,clickhouse-store,node-state}.ts`, `src/agent.ts` (log line).
Hard constraints kept throughout: every fact is per node; a node's readers see all of a save's facts or none;
`version` = own `commit_seq`; the gate is unchanged.

| # | WP6 finding | Fix | Regression test |
|---|---|---|---|
| 1 | `arr.push(...rows)`, `Math.max(...heights)` overflow the main-thread stack at ~130k elements (100k-tx block never committed) | `appendAll` / `minMax` loops; no spread-into-call on unbounded arrays left in `src/store/clickhouse` | child process (main-thread stack): spread throws, helpers do not, 300k rows; `[e2e]` 150k inputs + 150k outputs (300k UTXO rows) in one tip-mode commit (fails on the old code with `--no-worker-threads`) |
| 2 | `pendingRows` scanned all pending UTXO items per unresolved input (O(n²), ~60 s for 100k) | `pendingSpendRows`: index by `spender:inputIndex` | 100k unresolved spends × 2 nodes ≈ 0.1 s thread CPU; `[e2e]` child with 150k unresolved spends parks, parent completes it |
| 3 | `OperationRegistry.begin` copied every live operation sharing a node (O(n²) memory; 10k queued blocks → 4.2 GB heap, OOM) | per-node ordered queues with lazy heads + waiter min-heaps (§1) | heap of 10k/20k queued ops linear (~4.5 KB per waiting op); ordering, failure and ended-op tests |
| 4 | ~11 inserts/parts and ~13 round trips per block: catch-up 30.6 blocks/s, 20× merge amplification | multi-block commits (§2) | `[e2e]` 39 queued blocks → 2 commits, crash inside a multi-block commit is all-or-none for both nodes before and after recovery; child-before-parent with `maxBlocksPerCommit: 1` |

## 1. Operation ordering in O(1) per operation (item 3)

Semantics are those of `wp5a-core.md` §4: an operation's predecessors are the operations registered earlier
that share a node with it and were live at its registration. They are no longer materialized per operation.
Each node keeps a `NodeLane`:
- `rows`: its operations in id order (= registration order), with a lazily advanced head = the oldest one whose
  rows are not written (and not ended). "Every predecessor on node n has written its rows" ⇔ head id ≥ own id.
- `unsettled`: the same for "not committed / done / failed".
- a min-heap of waiters per queue keyed by operation id; a state change advances the head and wakes every waiter
  whose id ≤ the new head. Push and amortized head advance are O(1), a wait O(log n).
- `live`: for failure propagation. A failing operation marks (`poisoned`) every live later operation on its nodes:
  exactly the set that had it as a predecessor. Failures are rare, so the O(live) scan there is fine.

`StoreOperation.predecessors` is now one barrier dependency (`PredecessorBarrier`, state `done`, so it never adds a
`dependsOn` seq) whose `committed` resolves when every earlier unsettled operation of the node has settled and
rejects with `DependencyFailedError` if one failed. The callers (header acceptance, re-org, mempool) are unchanged.
The wait is transitive by construction: an operation waits for *every* earlier operation of its nodes, including
one behind a later bulk-mode save that committed early (tested). `operationOfSeq` is a map (it scanned a copy of
all live operations per call, per stored row).

## 2. Multi-block commits (item 4)

**Shape.** Consecutive `saveBlock` calls for the same node set become one `BlockBatch` and ONE commit: one
`commit_log` intent/committed pair, one RowBinary insert per table for the whole batch (`output`, `input`,
`transaction`, `block`, `block_transaction`, `node_block`, `tx_acceptance`, `utxo`, `utxo_by_script`, plus
`pending_spend` and the `f0` fill set if it parks). `commit_log.block_hash` is the last block's hash; kind `block`.
A batch of one block writes exactly what a single save wrote before (same steps, same fault-injection points).

**When blocks join a batch.** Per node set there is a lane: batches in creation order, at most
`tipRunningBatches = 1` running in tip mode (`bulkRunningBatches = 4` in bulk mode). A batch runs until its rows are
written, it parks, or it finishes (`StoreOperation.yieldLane`), then the next starts. A call joins the open (last,
not started) batch iff:
- no other operation was registered on these nodes since the batch was created (`isLatestOnItsNodes`), so the
  agent-visible call order and every predecessor relation are unchanged (the batch's single operation stands where
  its first block was called);
- `CHAINGRAPH_CLICKHOUSE_MAX_BLOCKS_PER_COMMIT` (default 64) and `CHAINGRAPH_CLICKHOUSE_MAX_BYTES_PER_COMMIT`
  (default 32 MiB of `sizeBytes`; a larger block is always alone) are respected;
- the block is not already in the batch and not being saved by another live operation (that block gets a batch of
  its own that first waits for the other save, as before).

So a block arriving while nothing runs for its nodes starts at once and commits alone (the tip case). Blocks
arriving while a batch works accumulate behind it (catch-up, initial sync, bursts).

**Ids without waiting.** A block's transaction ids and outputs are pinned in the registries when it is appended
(synchronously, in call order), and an *id phase* (stored-tx lookup + id allocation, I/O only) is scheduled at once
for the hashes pinned so far. So nobody ever waits on a batch that has not started: a child parked on a parent in a
later batch sees the parent's outputs at the parent's call and its ids after one lookup. The wait-for graph is
WP5a's plus "a batch waits for the earlier batch of its lane", which only points to older operations: no cycle.
The in-flight cap is unchanged (one slot per batch).

**Facts per node, all-or-none.** Per block: stored or new, the nodes that do not accept it yet, missing links (as
before). Per node, one UTXO transition per transaction over all its blocks in the batch, in batch order, with
`acceptedBefore` read once: a tx in two blocks (or already accepted) transitions once; an output created in block
k and spent in block k+1 gets its +1 and −1 in the same commit (they collapse within the commit, which is allowed:
same seq). The mempool cleanup plans over the inclusions of all the node's blocks in the batch (as header
acceptance does). One commit: a node sees the whole batch or none of it, before and after a crash
(`[e2e] … coalesced …`: crash at `utxo` inside a 19-block commit; neither node sees any of the 19, before or after
recovery; the re-save of all 39 is exact).

**Results.** Each call resolves with its own block's `attemptedSavedTransactions` / `transactionCacheMisses`
(a tx inserted by an earlier block of the same batch counts as a cache miss for a later one, as Postgres would).
If the batch parks, every call of it is answered as parked (see item 8 for the log).

## 3. Re-org convergence (item 5)

**Profile** (gate `reorg`, agent under the V8 sampling profiler, 0.5 ms; ClickHouse `query_log` per 100 ms).
With items 1–4 in, converge was 4.3 s (WP6: 7.3 s; the 2 × 101 per-block commits had been 1,210 parts and 708 MB
of merges, now 78 parts and 44 MB). In the 4.3 s window:
- `removeStaleBlocksForNode` for both nodes: ~1.1 s of server time (the `INSERT … SELECT` inverses write
  ~300k `utxo` and `utxo_by_script` rows and ~100k `tx_acceptance` rows per node), run concurrently for the two
  nodes and overlapping the agent's own work. The ClickHouse server was idle most of the window.
- The agent's single JS thread was the bottleneck: block parsing (`bitcoreBlockToChaingraphBlock`, agent code,
  ~0.75 s for the 101 × 1,000-tx blocks), pinning/registering at append (~0.3 s), then the two batches (~1.7 s:
  RowBinary encoding of `utxo`/`utxo_by_script` 0.3 s, inputs 0.16 s, outputs 0.1 s, the UTXO delta 0.17 s,
  output-registry release 0.13 s, GC 0.4 s), with ~0.25 s idle between the two batches.

**Changes.**
- A batch of new blocks writes its node-agnostic rows (`output`, `input`, `transaction`, `block`,
  `block_transaction`) right after `intent`, *before* it waits for the earlier operations of its nodes, and only
  then decides and writes the per-node rows (`node_block`, `tx_acceptance`, mempool rows, `utxo`). Those rows decide
  nothing from per-node state; spends are resolved in two rounds (the inputs the node-agnostic rows need first;
  in tip mode the remaining inputs of transactions that become accepted after the wait). The commit is open the
  whole time, so the node's watermark is held exactly as before. A batch re-saving a stored block waits first
  (its node scope comes from stored state). Step order (fault-injection points) is unchanged.
- Two running batches per node set in tip mode (`tipRunningBatches = 2`): the next batch resolves and encodes its
  node-agnostic rows while the previous one waits on the server.

Result: converge 3.9–4.1 s (limit 6 s; target ≤ 3 s not reached). The remainder is JS CPU on the agent's one
thread (parsing in the agent plus RowBinary encoding), not store round trips or server time; getting under 3 s
needs encoding off the main thread (a worker for `encodeUtxoRows` / input rows) or a cheaper row encoder
(`row-binary.ts` `uint64` via `writeBigUInt64LE` and hex `fixedString32` are ~25 % of the store's CPU). Not done.
Catch-up `--quick` with two running batches: 636 blocks/s, 0.59 parts per block (was 0.33 with one).

Test: `[e2e] … a new block behind a running re-org writes its node-agnostic rows first, its node facts after`
(the re-org held at `node_block`; the new block's `output` row exists while the node sees nothing of it; final
state exact).

## 4. Truthful "Saved new block" (item 8)

A save that parks (child before parent: its commit is `incomplete`, invisible) still answers early, so the
agent releases the block from its bounded buffer and keeps downloading (the parent may be behind it). The answer
now carries `committed: Promise<void>` (optional field on the `saveBlock` result in `src/store/types.ts`;
Postgres never sets it). The agent (`Agent.reportSavedBlock`) logs
`Parked block – height: H | hash: X – waiting for a parent block's outputs before it is committed.` at once and the
unchanged `Saved new block – …` line only when `committed` resolves (an error is logged if the commit fails; the
store also reports it through `onError`). A save that commits normally logs exactly as before. Sync-state
bookkeeping (`markHeightAsSynced`, buffer removal) is unchanged. Test: `[e2e] a parked save is answered with
committed …` (not resolved while the parent is missing and the node sees nothing; resolves after the parent).

## 5. In-flight cap: the stall after initial sync

WP6b-C saw the agent stop after "Agent: initial sync is complete." with `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES=16`
(reproduced at 2276ee1 with the e2e suite and the cap passed to the agent: initial sync fell to ~13 blocks/s at the
tail and the test timed out). Cause: WP5c re-armed a parked child's pending-spend timeout while any call was queued
for a slot, and children resuming after their own wait are such calls, so under a cap the e2e mockchain's children
(every input spends an unknown outpoint) completed one at a time. With multi-block commits a re-arm keyed on
"something not yet registered" deadlocked instead (state dump with cap 2: a parked child on node 1, two `1,2`
batches waiting for its rows holding the lane, a queued re-save of a block in flight keeping the re-arm alive).
Fix: no re-arm. Every save registers its outputs when it is called, before any slot or lane wait, and a save that
waits for another save of the same block registers nothing new, so a parent that exists is visible to the child at
once; the timeout fires once. Tests: `[e2e] initial sync of blocks spending unknown outputs completes with in-flight
cap 2 / 16` (300 blocks, per-block and batched commits, then `finishInitialSync` and `enableMempoolTracking`);
agent e2e 45/45 with cap 2 and with cap 16 (frozen copy with the cap added to the e2e agent environment; the
harness itself does not pass the cap through yet, see the report).

## 6. Concurrency ratio (item 6)

**Profile** (gate `concurrent`: mainnet-like 8 × 12,500 tx on node A, chipnet-like 50 × 2,000 tx on node B; CPU
profiles of the three agent runs; `commit_log` timeline per node scope):
- Writes are not serialised across nodes: A and B are separate batch lanes and their commits interleave every
  0.5 s in the "together" phase. `VisibilityPublisher` (one publish in flight, ≥ 100 ms apart) and the id
  allocator chains were never on the critical path (one `visibility` insert per 100 ms; id ranges of 100,000).
- Chipnet blocks commit one per commit even alone (~9.5 blocks/s): they arrive no faster than that, so the store
  keeps up with arrivals and batching has nothing to coalesce. The agent's single JS thread is ~50 % busy alone
  and ~65 % together (block parsing in the agent ~35 % of busy time, RowBinary encoding/UTXO derivation in the
  store ~45 %, GC ~10 %); when mainnet runs, chipnet arrivals slow to ~5 blocks/s. The Postgres store does no
  per-row encoding in JS, which is why the same agent reaches 0.79 there.

**Changes.**
- The inserts of one phase go out concurrently (node-agnostic: `output`, `input`, `transaction`, `block`,
  `block_transaction`; per node: `node_block`, `tx_acceptance`, mempool rows, `utxo`, `utxo_by_script`); only the
  phases are ordered (intent → node-agnostic → wait for earlier operations → per-node → fill → committed). They are
  rows of one open commit, so their order is invisible to readers and to recovery. Per-batch latency drops from
  ~9 sequential inserts to two rounds.
- Back to one running batch per node set in tip mode (two cost batching without helping once inserts are parallel).

Result: everything faster (alone 28.0k + 19.2k tx/s, together 26.6–27.2k tx/s; WP6 25.1k + 15.4k / 22.3k) but the
ratio stays 0.55–0.58 (limit 0.6, target 0.7 not reached): together is bounded by the agent's JS thread, which both
networks share. Re-org converge with these changes: 3.73 s. Next step (not done): RowBinary encoding and UTXO row
derivation in a worker thread, or a cheaper encoder (`row-binary.ts`).
