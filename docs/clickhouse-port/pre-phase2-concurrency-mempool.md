<!-- cspell:ignore chipnet BCHN getrawmempool getdata notfound peerbloomfilters subarray Varint acks bigints respawns unreversed -->

# Pre-Phase-2: block workers, the concurrency ratio, and the mempool fetch

Date 2026-10-11. Local only (ch1-local ClickHouse 26.8, ch1-pg Postgres 14, local BCHN chipnet). Inputs:
[g1-fix-pass-2.md](g1-fix-pass-2.md), [fix-pass-3.md](fix-pass-3.md), [spend-index.md](spend-index.md),
[mempool-fill-fix.md](mempool-fill-fix.md), [wp6b-write-path.md](wp6b-write-path.md).

| # | Item | Commit |
|---|---|---|
| 1 | Block worker pool (conversion and RowBinary rows off the main thread); no batch linger on an idle lane | `18be134` |
| 2 | Fetch each node's mempool when tracking starts and on reconnect, parents first | `32ec52b` |
| 3 | This document | docs commit |

Short version:

- **The premise did not hold on this host.** In the gate's `concurrent` scenario the baseline agent's main thread
  was idle 70 % of the time (busy 4.9 s of 16.3 s in the "together" run). The ratio was bound by three other
  things: the batch linger, which held every block of a tip followed block by block for up to 200 ms; the UTXO-on
  per-block server-side −1 join (~100 ms per 2,000-transaction block); and the metric itself.
- **Block workers ship as asked** (`CHAINGRAPH_ENCODE_WORKERS`, default min(4, cores − 1), 0 = off). Conversion
  and node-agnostic RowBinary encoding left the main thread (main-thread conversion 1,480 ms → 2 ms, encoding
  493 → 82 ms in the together run). max-block 2.54 → 2.05 s, burst 5.46 → 3.79 s (UTXO off), peak heap
  713 → 328 MB, max loop delay lower everywhere but UTXO-off burst.
- **The linger fix moved the ratio**: UTXO off 0.44 → **0.58** (0.60 / 0.58 / 0.58; 0.56–0.61 over the session).
  That is above 0.6 in some runs, but the median is just below. UTXO on stays at 0.45: that path is bound by the
  join. With these per-network speeds the metric cannot exceed ~0.66 (§1.5), so 0.7 is out of reach whatever the
  agent does.
- **Mempool fetch**: on tracking start and on every reconnect the agent sends P2P `mempool` to each node and saves
  what that node announces, for that node, parents before children. It works on both stores. e2e: new
  tracking-start and reconnect scenarios. Chipnet: after a stop and restart, `node_transaction` equals
  `getrawmempool` exactly on ClickHouse and on Postgres (11 of 11, 8 of them chained; 54 of 54 at the end of the
  sync, 34 chained).

## 1. Concurrency

### 1.1 Profile before (fc2faf6, `--cpu-prof`, concurrent scenario, UTXO on, gate default)

The run used the gate's `concurrent` scenario. Times are self time on the agent's main thread over the "together"
run (16.3 s of profile).

| Category | ms |
|---|---|
| idle | 11,343 |
| block conversion (bitcore → Chaingraph: `toObject`, per-tx `toBuffer` and hash) | 1,480 |
| store (registry pin/release, UTXO JS, lookups' JS) | 899 |
| RowBinary encoding | 493 |
| ClickHouse client | 385 |
| GC | 379 |
| P2P parse (bitcore `Block.fromBuffer` in the message handler) | 192 |

The chipnet-alone run had the same shape: idle 11.1 of 14.5 s. A timeline of one 2,000-transaction chipnet block
(~250 ms cycle) showed ~35 ms of conversion, ~10 ms of encoding, then ~120 ms idle while the server-side UTXO −1
join ran (`query_log`: 100 ms, ~476k rows read per block, and growing with the database).

With UTXO off the idle stretch was a timer, not I/O. The block arrived ~190 ms after the previous batch had started,
and the batch linger (fix pass 3: "the lane started a batch less than 200 ms ago") held it until 200 ms after that
start. Measured: chipnet alone 10.0k tx/s with the linger, **19.6k with `CHAINGRAPH_CLICKHOUSE_BATCH_LINGER_MS=0`**;
ratio 0.44 → 0.56. This accounts for the drop from g1-fix-pass-2's 0.57 (before the linger) to 0.43–0.45 since.

### 1.2 Block workers (item 1)

- **Raw payloads.** bitcore block messages keep their payload and parse it lazily (`raw-block-messages.ts` patches
  the `block` message's `setPayload`). With workers, the main thread parses only the 80-byte header (hash, height
  lookup, un-requested blocks).
- **Workers** (`block-worker.ts`, `block-workers.ts`). The pool copies the raw block once into a `SharedArrayBuffer`.
  The worker:
  - copies the bytes again into a buffer of its own: bitcore's parse writes to its input, since it reverses token
    categories in place, which also bit the first spec;
  - parses with the code the main thread used (`Transaction#fromBufferReader`, `bitcoreTransactionToChaingraphTransaction`,
    `bitcoreHeaderToChaingraph`);
  - encodes the **row templates**: the `output`, `input`, `transaction` and `block_transaction` rows with every id and
    `commit_seq` set to 0.
- **Large blocks are split.** A block of ≥ 25k transactions is split by transaction range across up to all workers.
  Each part skips earlier transactions by their serialized length (`transactionEnd`, a CompactSize walk). The first
  part also returns the header fields. The block size is the end offset of the last transaction, which equals
  bitcore's re-serialized length for canonical CompactSizes, as a node relays them.
- **Back to the main thread.** Transactions come back by structured clone in slices of 5,000, one slice per
  acknowledgement. Without acks, a port's queued messages are delivered in one event-loop turn: 117 ms of
  deserialization in one task on max-block. Templates come back as transferred `ArrayBuffer`s and are attached to
  the block object (`blockRowTemplates`, a WeakMap).
- **Ordering unchanged.** Conversions run concurrently, but blocks are still buffered in arrival order (the
  `blockParses` chain), one per event-loop turn.
- **Commits** (`block-commit.ts`, `row-templates.ts`). A batch whose new transactions all have templates copies their
  row ranges and patches the transaction id (and block id) and `commit_seq` at fixed offsets: two 8-byte writes per
  row. The bytes are exactly those of the encoders (spec: random mock blocks with tokens, split into parts, any
  subset, ids above 2^32, numbers or bigints). Anything else encodes as before: blocks without templates (genesis,
  workers off, the Postgres store, which skips templates) and the per-node rows (`node_block`, `tx_acceptance`,
  mempool rows). The main thread still owns every commit; the worker only computes bytes.
- **Failure.** A worker error or exit rejects its conversions; the agent converts that block on the main thread and
  the pool respawns the worker.

`CHAINGRAPH_ENCODE_WORKERS=0` restores the old path exactly (e2e ClickHouse 47/47 with it).

### 1.3 The linger rule (item 1)

The linger now applies only to a batch **opened behind other work of its lane** (a running or queued batch). A block
that arrives on an idle lane starts at once, even right after a commit. Catch-up and initial sync (blocks queue
behind the running batch) linger as before: the gate's catch-up makes the same parts (92 vs 94) at the same speed
(642 vs 641 blocks/s), and fix pass 3's spec still gets 4 commits for 150 streamed blocks.

New spec (`g1-fix.spec`): a tip followed block by block with a 300 ms linger. Per-block saves were 69–97 ms;
without the fix they were 281–307 ms (red).

### 1.4 Gate, before and after

Host: MacBook Pro (Apple M5 Pro, 18 cores), Docker VM 14 CPUs. `--store clickhouse --quick`,
`CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS=1000`. Before = frozen fc2faf6. After = frozen 32ec52b (workers default 4).
Medians of 3. No lab agent or other gate ran; the host 1-minute load was 2.7–4.9 (desktop apps; the Docker VM
process ~30 %, ClickHouse ~6 % idle). Raw JSONs in `/tmp/gate-series/` (not committed).

UTXO off (v2):

| Scenario | Before | After |
|---|---|---|
| max-block wall | 2.54 s (2.54 / 2.53 / 2.54) | **2.05 s** (2.05 / 2.10 / 2.04) |
| max-block loop max / peak heap | 233 ms / 713 MB | **91 ms / 328 MB** |
| burst drain | 5.46 s (5.47 / 5.36 / 5.46) | **3.79 s** (3.81 / 3.79 / 3.69) |
| burst loop max | 284 ms | 333 ms (333 / 257 / 341) |
| re-org converge | 2.05 s (2.08 / 2.05 / 1.93) | **1.51 s** (1.51 / *6.22* / 1.23) |
| concurrent ratio | 0.44 (0.44 / 0.44 / 0.44) | **0.58** (0.60 / 0.58 / 0.58) |
| concurrent together / alone | 19.6k vs 34.7k + 10.0k tx/s | 34.1k vs 39.0k + 19.3k tx/s |
| concurrent loop max | 119 ms | **25 ms** |
| catch-up | 640.6 blocks/s, 94 parts | 641.8 blocks/s, 92 parts |

UTXO on (gate default):

| Scenario | Before | After |
|---|---|---|
| max-block wall | 3.01 s | **2.60 s** |
| max-block loop max / peak heap | 316 ms / 824 MB | **156 ms / 405 MB** |
| burst drain / loop max | 7.57 s / 387 ms | **5.92 s / 257 ms** |
| re-org converge | 4.03 s | **3.53 s** (3.53 / 4.55 / 3.47) |
| concurrent ratio | 0.46 (0.45 / 0.47 / 0.46) | 0.45 (0.45 / 0.44 / 0.45) |
| concurrent together / alone | 15.4k vs 24.7k + 8.6k | 15.5k vs 26.1k + 8.5k |
| concurrent loop max | 112 ms | **33 ms** |
| catch-up | 588.6 blocks/s, 110 parts | 589.6 blocks/s, 108 parts |

Reading:

- **No regression in the medians.** The one fail in the after-series is UTXO-off re-org run 2: 6.22 s against a
  6 s limit. In it, one `block_transaction` insert reached the server 5.0 s after its four sibling inserts, which
  had left the agent in the same synchronous step. The agent was idle, the server had nothing slow in
  `query_log`, and the client logged no retry. Six re-runs gave 1.20–1.33 s. It looks like a transport/socket-pool
  stall in the HTTP client, not the change; it is listed under §4.
- **Ratio, UTXO off.** It crosses 0.6 in some runs, but the median is 0.58. Over the whole session the after-code
  measured 0.56–0.61 (9 runs).
- **Ratio, UTXO on.** No change: chipnet blocks wait ~100 ms each for the server-side −1 join.
- **Burst loop max, UTXO off (333 vs 284 ms).** It is now store-side work for three 100k-transaction blocks: `prepare`
  (pinning 100k hashes, ~45 ms per block), the lookup's dedupe and sort (~30 ms per block), slice deserialization
  and GC with a larger young heap. It is no longer conversion or encoding.

### 1.5 Why 0.7 is not reachable on this workload

Each network is a sequential writer, so `ratio = (T / max(tA, tB)) / (TA/tA + TB/tB)`, where TA and TB are each
network's transactions (100k each) and tA and tB its alone times. Even with perfect isolation (each network as fast
together as alone), the ratio is `max` over the sum. After the change, the alone rates are 39.0k and 19.3k tx/s, so
tA = 2.6 s and tB = 5.2 s, and the **ceiling is 0.66**. Before the linger fix it was 0.52 (34.7k and 10.0k). The
measured 0.58 is 88 % of the ceiling. The rest is cross-network contention: chipnet's header → saved latency rises
from 71 to 77–83 ms on average when mainnet commits alongside (ClickHouse server work, not the main thread).

Two ways to raise it:
- cut the small-block commit latency (chipnet alone: ~70 ms from header to committed, ~33 ms from committed to
  the next header through publish, poll and announce);
- change the metric to per-network slowdown (each network's together rate over its alone rate), which this
  workload would put at ~0.88 / ~0.88.

Both are outside this pass.

### 1.6 Profile after (32ec52b, together run, UTXO off)

| Thread | Busy / profile |
|---|---|
| main | 1,882 / 9,625 ms (20 %; before 30 %). Conversion 2 ms, encoding (per-node rows, patching) 82 ms, P2P parse 30 ms; the rest is store bookkeeping, the HTTP client and GC |
| workers (4) | 1,449 + 578 + 131 + 129 ms (least-busy dispatch favours the first). Conversion 661 ms, template encoding 286 ms on the busiest |

## 2. Mempool fetch (item 2)

### 2.1 Before

The agent ignored every `inv` and `tx` until "enabled mempool tracking". Transactions that were already in a node's
mempool then were never recorded, and a child announced later reached the store without its parents. Before the
spend index, that meant mempool stand-ins; now the output is only missing.

### 2.2 Design

- **Fetch.** `requestMempool(node)` sends P2P `mempool` (bitcore-p2p-cash has `MemPoolMessage`). It runs once for
  every node when tracking starts, and again in a node's registration callback after every reconnect while
  tracking. The node answers with `inv`s, handled by the normal path. Unknown transactions are requested with one
  `getdata` per `inv`, at most 50,000 items (it used to be one `getdata` per item). Each is saved with that node's
  validation. So **each node's mempool comes from that node**: a transaction only node 1 announced gets no node 2 row.
- **Parents first, per node** (`RequestedTransactions`). Every `getdata` is recorded per node. A transaction
  received from a node waits while any of its inputs' transactions is still requested from that node and not
  handled (saved or found saved), then saves. So a node's chain is saved parent first, whatever order the `inv`
  or `tx` messages come in; this holds for normal relay too. A request that is never answered releases its children
  after 10 s, on `notfound` or on disconnect. Waiting for the parent's save (not just its call) gives the order on
  Postgres as well, where saves are concurrent transactions.
- **Byte order fix.** `inv` hashes are internal byte order. The agent used them unreversed as Chaingraph (display
  order) hashes. With a real node, its transaction-cache check and inv-time validation never matched (every
  announced transaction was requested again), and the parent map would have missed every parent. The agent now
  reverses them.
  - The e2e mock used the same inverted convention: display-order bytes in `inv`, and a mempool keyed by swapped
    hashes. It now speaks protocol order, and its `getdata` handler no longer reverses the request's buffer in place.
    One old assertion depended on that mutation.
- **Refusal.** A node answers `mempool` only for whitelisted peers (`whitelist=`, the lab's and the Kubernetes
  charts' setting) or with bloom filters on. Otherwise BCHN disconnects. A node that disconnects within 10 s of the
  request, before announcing anything, is not asked again by the process (warning logged).
  `CHAINGRAPH_MEMPOOL_FETCH=off` disables the fetch.
- **Logs.** `<node>: requested the node's mempool.` and, after 2 s without further announcements,
  `<node>: mempool fetch – the node announced N transaction(s), M requested.`
- **Both stores.** The agent code is store-independent: the save is the existing `saveMempoolTransaction`.

Limit: if a node's mempool exceeds one `inv` (50,000) and the node lists a child in an earlier `inv` than its parent,
the child can arrive before the parent's `inv` and is not held. BCHN lists its mempool in entry order, which is
topological.

### 2.3 Tests

- `requested-transactions.spec.ts` (unit, 3 tests):
  - a chain announced and received child first is saved parents first;
  - requests are per node;
  - timeout, `notfound` and disconnect release.
- e2e (`e2e.spec.ts`, both stores):
  - **tracking start**: node1's mempool is A → B → C, announced C, B, A; node2's is one unrelated transaction; node3's
    is empty. Each node's `node_transaction` holds exactly its own; A's validating nodes = [node1].
  - **reconnect**: the mock drops node1; node1 then announces F, E (F spends E) and sends E 400 ms late. Checked: the
    agent re-requests the mempool, saves E before F (stdout order), logs that F waited for 1 parent, and only node1
    gets them.
  - Without the parent wait (patched build) the reconnect test fails ("the parent was saved").
  - Both tests then clear their rows, so the exact-mempool assertions of later tests are unchanged.
- **Chipnet live** (`scripts/chipnet-lab/ch1-chipnet-mempool-check.mjs`): reads the node's `getrawmempool` (RPC
  credentials from `bitcoin.conf` into environment variables, never printed) and compares it with the store's
  mempool for the node, using the frozen agent's checker for ClickHouse and SQL for Postgres.

| When | Node mempool (chained) | ClickHouse | Postgres |
|---|---|---|---|
| end of the ClickHouse sync from genesis (tracking start) | 54 (34) | 54 = 54, no difference | – |
| both agents restarted at the tip | 29 | 29 = 29 (one hash missing for one read, 3 s after the fetch, then equal) | 29 = 29 |
| both agents stopped 10 min (a block confirmed the 29; 11 new arrived), then restarted | 11 (8) | **11 = 11** | **11 = 11** |
| end of the 30-min tip follow (10:37Z) | 15 | **15 = 15** | **15 = 15** |

## 3. Suites and chipnet (final code 32ec52b)

| Suite | Result |
|---|---|
| `tsc` build | ok |
| unit (`ava --match='!*[e2e]*'`) | 134 passed, 1 todo (+3 block workers, +3 requested transactions) |
| ClickHouse store + API specs (`build/store/clickhouse/*.spec.js`, `build/api/*.spec.js`) | 172/172 (+1 tip-follow linger) |
| e2e ClickHouse, UTXO on | 47/47 (47 `[postgres]` skipped; +2 mempool fetch) |
| e2e ClickHouse, UTXO off | 47/47 |
| e2e ClickHouse, `CHAINGRAPH_ENCODE_WORKERS=0` | 47/47 |
| e2e Postgres (`e2e.spec` + `mempool-cleanup.spec`) | 102/102 |
| eslint / prettier / cspell on changed files | clean |

Chipnet: the agent ran from a frozen export of 32ec52b (`agent-32ec52b`), with the lab settings (UTXO off, cap 16, 64
blocks per commit), into a fresh database `chipnet_ch_pp2`.

| | spend-index pass (7ee8602) | this pass (32ec52b, 4 workers) |
|---|---|---|
| sync from genesis to tip (every block accepted) | 483 s, 5,893 tx/s | **423 s, 6,730 tx/s** (327,390 blocks, 2,846,870 txs) |
| max event-loop delay (10 s windows) | 766 ms | 870 ms, no window ≥ 1 s |
| errors / warnings | 0 / 0 | 0 / 0 |

**Tip follow.** Both agents (ClickHouse and Postgres, both 32ec52b) followed the tip 10:07–10:37Z, after the restart
test. At the end both were at the node's height 327,396 and had logged 0 errors and 0 warnings. Max loop delay was
91 ms (ClickHouse) and 58 ms (Postgres).

**Parity at H = 327,396** (`ch1-chipnet-parity.sh`, `chipnet_pg` vs `chipnet_ch_pp2`):

| Table | Rows | Result |
|---|---|---|
| block, block_transaction, transaction, output, input | 327,397 / 2,847,042 / 2,847,042 / 7,840,127 / 6,957,495 | match |
| spend, spend_join | 6,630,098 | match |
| tx_acceptance, node_block_history | 2,847,042 / 0 | match |
| node_block | 327,397 | 45 blocks have `accepted_at` set on Postgres and NULL on ClickHouse: synced at different times (the agent's 2 h rule, as in spend-index.md §5.2) |
| node_transaction_history | 817 vs 148 | mempool history depends on when each agent ran (Postgres since 2026-10-10 with gaps, ClickHouse since this sync) |

Spend check (`ch1-chipnet-spend-check.mjs`): 6,630,134 index rows = joined pairs; nothing missing, extra, duplicated
or mismatched; watermark = visible(0).

The agents were stopped after the parity run. `chipnet_ch_pp2` (~4.7 GiB) is kept on ch1-local next to the
older `chipnet_ch`. Raw results are in `chaingraph-performance/2026-10-11-pre-phase2/` (not committed).

## 4. Open items

- **5 s insert stall** (re-org outlier, §1.4): add per-request latency and retry logging to the ClickHouse client
  (queue wait vs server time) before the GKE runs, and check the socket pool size against the number of inserts
  one commit phase sends in parallel.
- **UTXO-on tip latency**: the per-block server-side −1 join reads ~470k rows for a 2,000-transaction block on a
  small database. It grows with the data and bounds UTXO-on tip following.
- **Concurrency metric**: per-network slowdown would measure interference without the alone-speed asymmetry (§1.5).
- **Remaining main-thread cost for 100k-transaction blocks**: store `prepare` (pin) and the lookup's sort.
  Slicing them would take the burst loop max below ~250 ms.
- **Worker memory**: each part's worker parses its own copy of the raw block. For a 32 MB block that is up to 4 × 32 MB
  of transient buffers plus the bitcore objects of one part each. Templates (~1.3 × the raw size) live with the
  block object until it is saved.
