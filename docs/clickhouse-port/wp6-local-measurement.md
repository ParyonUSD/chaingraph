# WP6: local measurement and kill-criteria check (before any lab spend)

Plan: `paryon_kubernetes/docs/chaingraph/plans/clickhouse-primary-store.md` §8, Phase 1 "Measure, locally", Gate G1
items 2, 3, 5 and the three kill criteria. Raw results (gate JSON, poller JSON, logs) stayed in the job scratch
directory and are not committed; this page is the summary.

**Bottom line.** The ClickHouse store beats Postgres on the dense-block write path (max block 1.6× faster, burst on
par with the reference) and the visibility gate held: zero torn reads, HOL p95 ≤ 0.15 s on the max block. But it
is not ready for the lab. Two crash bugs and one quadratic stall stop a 100k-tx block outright (they were patched in
a scratch copy only, to measure). Per-block fixed cost makes catch-up 30× slower than Postgres, and 10,000 queued
small blocks OOM the agent. Re-org convergence (7.3 s) and the concurrency ratio (0.55) miss the Postgres
thresholds. The gate costs reads +140–400 % on point lookups, a fixed ~2.5–6 ms per view. Fix list at the end.

## 1. Environment

| Item | Value |
|---|---|
| Host | MacBook Pro, Apple M-series, 10 cores, Node 24.14.1 |
| Docker VM | 10 CPUs, 7.75 GiB RAM, 94 GB disk (shared by `ch1-local` and `ch1-pg` and other agents' builds) |
| ClickHouse | `ch1-local`, 26.8.22.13, http://localhost:18123, default settings (`parts_to_delay_insert` 1000) |
| Postgres | `ch1-pg`, postgres:14 stock (shared_buffers 128 MB), localhost:15432, used via `--pg-url` |
| Agent under test | `bc42695` (branch HEAD at the start) built in a frozen copy (`git archive` + `tsc`), so concurrent edits to `src/` did not leak in |
| Measurement patch | `bc42695+wp6-measurement-patch`: the two fixes of §9 items 1–2 applied **only in the scratch copy** (not committed; `src/` is owned elsewhere). Every gate number below uses it unless marked "unpatched" |
| Isolation | One backend at a time (the other container idle). `scripts/measure/wait-quiet.sh` waited for other checkouts' gate/ava runs before every timing run. Other agents did run e2e/gates and image builds on the same machine during the session, so treat ±10 % as noise |

Environment fixes made during the session:
- **Docker VM disk filled to 0 B twice.** ClickHouse `system.text_log` at the image's default `trace` level grew about
  3 GB per ClickHouse e2e run, and each e2e database is about 1.6 GB. `ch1-local` now has
  `/etc/clickhouse-server/config.d/ch1-wp6-logs.xml` (logger `information`, text_log `warning`, reloaded with
  `SYSTEM RELOAD CONFIG`). `text_log`, `trace_log`, `processors_profile_log` and `query_log` were truncated, and the
  dead e2e databases `cg_e2e_6321` and `cg_e2e_18161` were dropped. Free disk is still only about 5 GB, mostly
  image and build cache from other work.
- The gate's fixed ports (agent API 3299, mock nodes 19433+) collided with another checkout's gate. They are now
  overridable with `INGESTION_GATE_API_PORT` and `INGESTION_GATE_NODE_PORT` (this session used 3399 and 19633).

## 2. e2e and spec suites (G1 item 2)

Command: `scripts/measure/e2e-timing.sh <agent> <out> <label> 3 -- --match='*[e2e]*' --timeout=60s build/e2e/e2e.spec.js`
(+ `CHAINGRAPH_E2E_POSTGRES_HOST/PORT` or `CHAINGRAPH_E2E_STORE=clickhouse CHAINGRAPH_E2E_CLICKHOUSE_URL`).

| Suite | Result | Wall (median of 3) |
|---|---|---|
| `e2e.spec.ts` on Postgres | 92/92, 3 of 3 runs green | **6.7 s** (6.4 / 6.7 / 6.7) |
| `e2e.spec.ts` on ClickHouse | 45 passed + 47 skipped when green; **green in 4 of 9 runs** with enough disk free | **65.9 s** (median of the green runs: 59.4 / 65.1 / 66.7 / 67.5) |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` (unit + `[e2e]`, incl. crash injection) | 85/85, 3 of 3 runs | **98.8 s** (98.5 / 98.8 / 115.0) |

ClickHouse e2e failures, all timing-dependent:
1. `[e2e] saves block transactions if previously announced tx is seen but not yet saved` reads 0 of 3 links. A block
   that waits for pending spends is reported as saved once its commit is `incomplete` (`clickhouse-store.ts`
   `saveBlock`, `onParked`), so the agent logs "Saved new block" before the commit is visible. The test reads at once.
2. 10 s `waitForStdout` timeouts ("Agent: initial sync is complete.", "Exiting...", "Saved new block … 3200"). The
   ClickHouse initial sync of the e2e mockchain takes about 29 s, against 2.1 s on Postgres (see catch-up, §3), so
   fixed 10 s waits are marginal.

ClickHouse e2e is about 10× slower than Postgres; nearly all of the gap is `completes initial sync` (28.9 s vs 2.1 s)
plus the re-org tests (5.5 s vs 0.2 s).

Crash injection (cited, not re-run): 23 crash points in WP5a-core (block save 12, re-org 6, header acceptance 5) and
44 in WP5a-mempool (new tx for 2 nodes, replacement and cascade, block confirming a mempool tx, expiry). Both
passed inside the spec runs above. **Verdict G1-2: local fail** (flaky; all failures are timing and read-after-park,
none is a wrong final state).

## 3. Ingestion gate, side by side (G1 item 3)

Command: `scripts/measure/gate-with-poller.sh <agent> <out> <label> yes|no -- --store clickhouse|postgres --scenarios <s> [--quick] [--keep-pg] [--pg-url postgres://chaingraph:…@localhost:15432]`.
Three runs per cell, median shown (burst: one attempt each).

| Scenario | Postgres 14 (ch1-pg) | ClickHouse 26.8 (ch1-local) | CH / PG | G1 limit | CH verdict |
|---|---|---|---|---|---|
| max-block (31.80 MB, 100,001 tx) wall | 7.58 s (6.39–7.69) | **4.67 s** (4.63–4.73) | 0.62× time | ≤ Postgres | **local pass** |
| max-block tx/s | 13,191 | **21,404** | 1.62× | | |
| burst (3 × 31.8 MB, chained) drain | **OOM**: Postgres backend killed in the 7.75 GB VM (`OOMKilled=true`), as `docs/ingestion-gate.md` predicts | 12.34 s, 24,307 tx/s | n/a (host PG 18 reference: 11.5–12.4 s, 24.7–26.0k tx/s) | ≥ Postgres | **lab-only** (about equal to the host reference) |
| reorg converge (2 nodes, 101-block switch) | 1.51 s (1.50–1.56), PASS* | **7.27 s** (7.25–7.45) | 4.8× | ≤ 6 s | **local fail** |
| reorg tx/s | 66,137 | 13,775 | 0.21× | | |
| concurrent ratio (no poller) | 0.79 (0.79–0.83) | **0.55** (0.55–0.56) | | ≥ 0.6 | **local fail** |
| concurrent together tx/s | 28,547 (alone 18,9k + 17,0k) | 22,286 (alone 25.1k + 15.4k) | 0.78× | | |
| catch-up 10,000 × 20-tx blocks | 633.8 blocks/s (629.7–738.7) | **agent OOM** (V8 heap 4.2 GB after 202 blocks, 3 of 3 runs) | — | ≥ 300 blocks/s | **local fail** |
| catch-up `--quick` 1,000 blocks | 904.2 blocks/s | **30.6 blocks/s** (30.3–30.7), 644 tx/s | 0.034× | ≥ 300 | **local fail** |

ClickHouse write metrics (part_log / query_log / events, per scenario window):

| Scenario | NewPart bytes | merge bytes / merges / merge s | parts created | max active parts (one partition) | delayed / rejected inserts | peak insert memory | Postgres WAL |
|---|---|---|---|---|---|---|---|
| max-block | 160.8 MB (deterministic, 3/3) | 4 KB / 1 / 0.001 s | 17 | 7 | 0 / 0 | 366 MB | 224.9 MB |
| burst | 527.4 MB | 13 KB / 5 / 0.007 s | 44 | 7 | 0 / 0 | 377 MB | (OOM) |
| reorg | 282.5 MB | 707.6 MB / 236 / 3.5 s | 1,210 | 10 | 0 / 0 | 327 MB | 223.4 MB |
| concurrent (together) | 371.8 MB | — | 707 | 10–11 | 0 / 0 | — | — |
| catch-up `--quick` | 97.4 MB | **1,920 MB** (20× the inserted bytes) | 11,332 (about 11 per block) | — | 0 / 0 | — | 46.8 MB |

Peak agent heap: max-block 620–639 MB (Postgres 739–860 MB); burst 1.48 GB.

**"No global serialisation", defined and checked.** Two parts:
- (a) the gate's `concurrent` ratio ≥ 0.6;
- (b) no store-level writer lock across nodes in `ClickHouseStore`.

(a) fails: 0.55. The ratio formula understates it, though. With alone durations of 4.0 s and 6.5 s, perfect
overlap caps the ratio at 0.76. A fairer measure is parallel efficiency = (serial − together) / (serial − longest
alone):

| Backend | Alone (mainnet / chipnet) | Together (per network) | Efficiency |
|---|---|---|---|
| Postgres | 5.28 / 5.88 s | 5.6 / 7.0 s | 0.79 |
| ClickHouse | 4.0 / 6.5 s | 4.9 / 9.0 s | 0.37 |

On ClickHouse both networks progress at the same time (mainnet +20 %, chipnet +38 %), so the writes are not
serialised. They contend more than on Postgres.

(b) Code inspection of `src/store/clickhouse/`:
- `runExclusive` drains all operations, but only for the mode switches `prepareForInitialSync` and
  `finishInitialSync`, which are off the hot path.
- `OperationRegistry` predecessors are only live operations that **share a node**, so the ordering is per node.
- `IdAllocator` keeps one promise chain per id kind. It is global, but each reservation is one insert per 100,000
  ids.
- `VisibilityPublisher` allows one publish in flight (batched, ≥ 100 ms apart) for all nodes. This is the only
  global point. It serialises visibility, not writes: a slow publish delays every node's watermark.
- The HTTP pool (`maxOpenConnections` 64) is shared.

There is no store-level mutex. The likely contention is the agent's single JS thread (the ClickHouse path encodes
RowBinary and derives UTXO rows in JS) plus ClickHouse CPU in the shared VM. That needs a profile.

## 4. Torn reads and head-of-line wait (G1 item 5)

`scripts/measure/torn-read-poller.mjs` runs in a separate process every 50 ms. For every node it takes one
`readSnapshot(n)` and, with those parameters, checks three things:
- (a) every block in `node_block_at(n)` has its `block_at` row, `block_transaction_at` links equal to
  `transaction_count`, and n's `tx_acceptance_at` rows for the block equal to `transaction_count`;
- (b) no `tx_acceptance_at(n)` row (block ≠ 0) points to a block missing from `node_block_at(n)`;
- (c) for a rotating 1/16 sample of `utxo` keys, `sum(sign)` over the gated base rows at the snapshot is 0 or 1.

HOL wait is the time from a commit's `finished_at` (agent clock) to the first poll that saw `max(visible_seq)` of the
node ≥ seq. Resolution is the tick, about 50 ms. The poll is needed because `visibility` is a ReplacingMergeTree,
so its history merges away and cannot be read after the run.

| Run | Polls (all nodes) | Torn reads | HOL p50 / p95 / max | Commit → visible incl. save (p95) |
|---|---|---|---|---|
| max-block ×3 | 135 / 152 / 136 | **0** | 27–46 / **134–148** / 148 ms | 1.8–1.9 s |
| concurrent ×3, "together" case (2 nodes, 58 sequential blocks) | 136 / 113 / 115 | **0** | 72–103 / **203–664** / 799 ms | 0.5–1.1 s |
| concurrent alone cases (6 DBs) | 106–198 each | **0** | p95 68–624 ms | |

UTXO keys sampled: 100k–300k per max-block run. **Verdict G1-5: local pass** (zero torn reads; HOL p95 0.15 s on the
max block, far under 5 s). Caveat: the first, unpatched max-block attempts never made the block visible (§9 items
1–3), and the poller correctly saw nothing.

## 5. Gate cost on reads (kill criterion 3)

`scripts/measure/gate-cost.mjs` on the max-block database (1 node, 100k tx, 200k outputs and UTXOs), 41 alternating
runs after 5 warm-ups, server `elapsed_ns` median. "Base" is the same query on base tables with the watermark as a
literal (`commit_seq <= W`) and the same `GROUP BY … HAVING sum(sign) > 0`. Both variants returned identical rows.

| Query | Gated (pinned views) | Base + literal filter | Overhead |
|---|---|---|---|
| UTXOs by locking bytecode (`utxo_by_script_at`) | 7.82 ms | 1.58 ms | +397 % |
| transaction by hash + outputs (`transaction_at` ⋈ `output_at`) | 7.61 ms | 1.56 ms | +388 % |
| block transactions, first 1,000 (`block_transaction_at` ⋈ `transaction_at`) | 14.55 ms | 6.06 ms | +140 % |
| acceptance of a tx by the node (`tx_acceptance_at`) | 3.57 ms | 0.90 ms | +295 % |
| node UTXO aggregate, full scan (`utxo_at`) | 19.57 ms | 15.69 ms | +25 % |
| spender of an outpoint (`input_at`) | 5.08 ms | 1.66 ms | +206 % |

**Why.** EXPLAIN shows the same primary-key use in both variants: the watermark and fence fold into PREWHERE, as
WP4 found. The cost is fixed, not per row. Every view instance runs these as separate pipelines before the main
query:
- the `max(visible_seq)` scalar subquery (the clamp);
- the `epoch_fence` array subquery;
- the `commit_void` set;
- on node-agnostic views, the committed-tail `IN (SELECT … FROM commit_log …)`.

A `clickhouse-benchmark` breakdown on the `tx_acceptance` lookup: base 1 ms, +watermark 2 ms, +fence 2 ms, +void
1 ms, full view 3 ms. Queries that join two views pay twice. The overhead is about 2.5–6 ms per request, so it
shrinks in relative terms as base latency grows (+25 % on the 16 ms scan). On golden, the base reads will be
10–100 ms.

**Verdict kill-3: local fail** (> 20 % on every query shape). It is a fixed cost with a known fix (§9 item 7), so
re-measure on golden after the fix before treating it as a kill.

## 6. Keyset pagination (R7)

`scripts/measure/keyset-scan.mjs` holds one pinned node-agnostic snapshot and pages by 1,000 rows. Run on the
max-block database:

| Scan | Rows | Pages | Duplicates | Out of order | Count, uniq, sum and xor fingerprint vs one full aggregate |
|---|---|---|---|---|---|
| `transaction_at` by `internal_id` | 100,017 | 101 | 0 | 0 | match |
| `output_at` by `(transaction_internal_id, output_index)` | 200,027 | 201 | 0 | 0 | match |

**Result: no gaps or duplicates.** Caveat for the API: ids are allocated before commit, so commit order ≠ id order.
2 of the 7 commits had ids below an earlier commit's max id (the base-chain blocks are saved concurrently). A cursor
that is **not** pinned to one snapshot across pages can skip rows that become visible behind it. Page cursors must
carry the snapshot (`visible0`, tail), or the API must page by commit order.

## 7. Parts and merges (R6)

From `scripts/measure/db-stats.mjs`:

| After | Active parts per table | Merges in flight | Delayed / rejected inserts (window) |
|---|---|---|---|
| max-block | 1–6 (`pending_spend` 6, everything else ≤ 4) | 0 | 0 / 0 |
| burst | ≤ 5 per table | 0 | 0 / 0 |
| catch-up `--quick` | ≤ 4 per table at the end; 11,332 parts created and 1.92 GB of merges for 97 MB of inserts | 0 | 0 / 0 |

The max-block and burst part counts are far from `parts_to_delay_insert` (1000). Per-block part creation (about 11
inserts per commit, each a new part per table) is what drives catch-up merge write amplification (20×). That is a
lab concern at mainnet tip rates for fan-out tables, and it is the case for multi-block commits (§9 item 5). The
server-wide `DelayedInserts` counter shows 529 since server start, all from before or outside these windows (every
gate window delta is 0).

## 8. UTXO growth (compaction decision)

Rows stored per live key (`sum(sign) = 1`), per node. `OPTIMIZE … FINAL` changes nothing, because pairs from
different commits never collapse (version = own `commit_seq`, WP5a-core §2):

| Database | `utxo` / `utxo_by_script` rows per live UTXO | Dead keys | `tx_acceptance` rows per live | `node_block` rows per live |
|---|---|---|---|---|
| reorg (2 nodes, A 100 blocks then B 101, 40 mempool tx) | **6.91** (699,457 rows / 101,207 live) | 299,100 | 3.00 | 2.96 |
| burst (3 chained blocks, tip-mode spends of the previous block) | 2.00 (800,029 / 400,029) | 200,000 | 1.00 | 1.00 |
| catch-up `--quick` (1,000 chained blocks) | 2.90 (61,006 / 21,046) | 19,980 | 1.00 | 1.00 |

Compressed bytes per row (reorg DB): `utxo` 37.7, `utxo_by_script` 63.9.

**Extrapolation per 1,000 mainnet blocks, per node.** Assumptions:
- 2,500 tx, about 7,000 outputs (2.8 per tx) and 6,000 spends per block;
- every tx enters through the mempool, whose commit writes the UTXO +1/−1 rows. Confirmation writes none
  (WP5a-core §8.3); it writes `tx_acceptance` −1 for the mempool row and +1 for the block, and `node_transaction` −1;
- no re-orgs, no compaction.

Per block, each UTXO table gets 13,000 rows (7,000 +1 and 6,000 −1). Of these, 12,000 are permanently dead pairs,
and the live set grows by 1,000. Per 1,000 blocks, that is:

| Table(s) | Rows per node | Dead | Size |
|---|---|---|---|
| `utxo` and `utxo_by_script` | 13.0 M each | 12.0 M each | about 1.3 GB compressed for the pair |
| `tx_acceptance` (3 rows per confirmed tx) | 7.5 M | 5.0 M | |
| `node_transaction` (+1/−1 per tx) | 5.0 M | 5.0 M | |

For a node with live set L after B blocks, rows/live = (L + 13,000·B) / (L + 1,000·B). That tends to 13 while L is
small relative to B, and is about 1 + 12,000·B / L for a large live set. Every node pays this separately. Reads
aggregate all rows of a key, so read cost grows with the dead pairs. **Recommendation:** build the WP5a-core
compaction (inserting the swapped-version pair as `commit_seq = 0` for pairs below every watermark) before the
lab replay. Otherwise P1/P2 on golden plus 2,137 replayed blocks will already carry about 28 M dead rows per node
in the UTXO tables.

## 9. Must be fixed before the lab

Items 1–2 were patched in the scratch copy only, to get past them; the rest are open.
1. **Stack overflow on large blocks.** `block-commit.ts` has `utxoRows.push(...delta.rows)` and
   `utxoRows.push(...mempoolRows.utxo)`. With 300k rows, the 100k-tx block throws `RangeError: Maximum call stack
   size exceeded`, the commit aborts and the block is never visible. `clickhouse-store.ts`
   `Math.max(...heights)` / `Math.min(...heights)` has the same hazard for very long header acceptances.
   Use loops.
2. **O(n²) `pendingRows`.** In `block-commit.ts`, each pending input does `pendingUtxo.filter` over all pending
   items. With 100k unresolved spends this blocks the event loop for about 60 s (twice: the +1 and −1
   `pending_spend` rows). Index the items by `spender:inputIndex` (scratch patch: a `Map`). Real chains hit this
   path during out-of-order sync of big blocks.
3. **Writer lease lost while the event loop is busy, then wedged.** The heartbeat shares the main thread, so any
   synchronous stretch longer than ttl − margin (30 s − 5 s) causes `LeaseLostError`. The agent then keeps running
   but cannot commit ("Writer lease … is not held"), so it neither recovers nor exits. Exit (or re-acquire and
   re-run recovery) on lease loss, and bound synchronous work per tick.
4. **Catch-up OOM** with 10,000 queued small blocks: V8 heap 4.2 GB after 202 saved blocks, in
   `Array.filter` (`OperationRegistry.begin` copies every live operation sharing a node into `predecessors`, which
   is quadratic in queued operations). It needs the in-flight cap in progress in `clickhouse-store.ts` and a
   predecessor representation that is not O(live).
5. **Per-block fixed cost.** 30.6 vs 904 blocks/s (33 ms per block; about 11 inserts and parts per block; 20×
   merge amplification). Postgres-relative catch-up is 0.034×. Coalesce consecutive blocks into multi-block commits
   and inserts in sync mode (WP5a-core deviation 3), and possibly in tip mode under backlog. This is the main
   throughput risk for the replay.
6. **Re-org converge 7.3 s** (Postgres 1.5 s, limit 6 s; 1,210 parts and 708 MB of merges for 2 × 101 blocks).
   Profile the re-org / header-acceptance path (per-block server-side `INSERT … SELECT`s).
7. **Gate read cost.**
   - Pass the fence array and the node watermark as parameters of `*_at` views. The snapshot already reads them;
     clamp in the API layer or with one cheap check.
   - Replace the committed-tail `IN (SELECT … commit_log …)` with the `tail` parameter alone (validated once per
     snapshot).
   - Re-measure on golden.
8. **Concurrency 0.55** (efficiency 0.37 vs Postgres 0.79). Profile agent CPU per tx on the ClickHouse path and the
   shared `VisibilityPublisher`.
9. **e2e flakiness** (green 4/9). A parked (incomplete) save is logged and returned as saved before it is visible.
   Either do not log "Saved new block" until commit, or make the test poll with `eventually()`. Also raise or derive
   the 10 s stdout waits for the slower ClickHouse initial sync, after item 5.
10. **UTXO compaction** (§8) before the replay.
11. **Environment** (done locally; repeat on any new host):
    - keep ClickHouse logs off `trace`;
    - e2e databases are about 1.6 GB each, and a crashed run leaves one behind until the next run;
    - the Docker VM cannot run the Postgres burst (OOM), so the burst comparison needs the lab or a ≥ 14 GB VM.

## 10. G1 items and kill criteria

| Item | Verdict | Evidence |
|---|---|---|
| G1-2 e2e green on ClickHouse incl. crash injection | **local fail** (flaky) | 45/45 when green, 4/9 runs green; specs 85/85 ×3; crash points 23 + 44 pass |
| G1-3a max-block ≤ Postgres, same host | **local pass** (patched build) | 4.67 s vs 7.58 s; the unpatched `bc42695` fails (stack overflow) |
| G1-3b re-org ≤ 6 s | **local fail** | 7.27 s (Postgres 1.51 s) |
| G1-3c burst catch-up ≥ Postgres | **lab-only** | CH 12.34 s / 24.3k tx/s; Postgres OOM in the 7.75 GB VM; host PG 18 reference 11.5–12.4 s |
| G1-3d no global serialisation | **local fail** on the ratio (0.55 < 0.6) | no store-level mutex (§3); efficiency 0.37 vs 0.79 |
| G1-3 small-block catch-up (Postgres threshold) | **local fail** | 30.6 blocks/s; OOM at 10,000 blocks |
| G1-5 zero torn reads, HOL p95 < 5 s | **local pass** | 0 torn reads over 2,156 per-node snapshot checks (block, acceptance and UTXO-sum invariants); max-block HOL p95 0.15 s |
| **Kill 1** R1 parity not reachable within 1.5× estimate | **not triggered; lab-only** | Per-node exactness (G1-1, md5 vs Postgres on the replay) is not measured locally. Local correctness is intact: every gate correctness check passes, including the deep-re-org check that is allow-listed on Postgres; UTXO sums are always 0/1. Schedule risk comes from §9, not from a parity gap |
| **Kill 2** throughput < 23k tx/s with the store as bottleneck | **not comparable locally; at risk** | The laptop is not a C4D and the VM is shared, so absolute tx/s is not comparable with 23k. Local ClickHouse/Postgres ratios as the proxy: dense block **1.62×**, burst about 1.0× (vs reference), two-network concurrent 0.78×, re-org 0.21×, small-block catch-up **0.034×**. The store is the bottleneck on per-block fixed cost (§9 item 5); on dense blocks it is not. Decide after items 4–5 |
| **Kill 3** gate cost on reads > 20 % | **local fail, fixable** | +140 to +400 % on point lookups, +25 % on a scan; a fixed 2.5–6 ms of scalar subqueries per view (§5). Re-measure on golden after §9 item 7 |

**Recommendation:** do not start lab spend yet. Fix §9 items 1–5 and 7 first (1–3 are blockers for any replay),
then repeat this page's runs. The scripts are in place.

## 11. Thresholds calibrated (`scripts/ingestion-gate/thresholds.clickhouse.json`)

Limits use the measured references and the 1.5–2× rule in `docs/ingestion-gate.md`:

| Scenario | Bytes (1.5×) | Parts (2×) | Merge seconds |
|---|---|---|---|
| max-block | 245 MB (ref 160.8 MB, identical in 3 runs) | 34 (ref 17) | 2 s (ref 0.001 s, so a floor rather than 2×) |
| burst | 800 MB (ref 527.4 MB, 1 run) | 88 (ref 44) | 2 s (ref 0.007 s) |

Time, ratio and heap limits stay copied from Postgres (G1: no worse than Postgres). Delayed and rejected inserts
stay at 0. `docs/ingestion-gate.md` still says these are `null`; update that line when it is next edited.

## 12. Harness changes (committed with this page)

- `lib/postgres.mjs` `waitFor({ agent })` and every scenario `waitFor`: fail at once when the agent exits
  mid-scenario. Before this, a crash on the max block waited the full 600 s timeout.
- `lib/clickhouse.mjs`: the agent gets `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS=1` (override with
  `INGESTION_GATE_CH_PENDING_SPEND_TIMEOUT_MS`). The fixtures spend outpoints that never exist, so every scenario
  otherwise included a 60 s pending-spend wait that held the watermark. Spends of blocks the gate does provide
  (burst, catch-up, re-org) still resolve normally. Consequence: the max-block number does not include resolving
  100k real spends; burst does (2 × 100k).
- `run.mjs` / `lib/scenarios.mjs`: `INGESTION_GATE_API_PORT`, `INGESTION_GATE_NODE_PORT`.
- Note: `lib/heap-sampler.mjs` is preloaded with `--import`, so it also runs in pino's worker thread and both
  threads append to the same samples file. Peak heap (max) is unaffected, but a stalled main thread hides behind
  the worker's 8 MB samples.
- New `scripts/measure/`:

  | Script | Purpose |
  |---|---|
  | `e2e-timing.sh` | timed repeated ava runs |
  | `gate-with-poller.sh` | one gate run, optionally with the poller |
  | `wait-quiet.sh` | wait for other gate/ava runs |
  | `torn-read-poller.mjs` | G1-5 |
  | `gate-cost.mjs` | kill 3 |
  | `keyset-scan.mjs` | R7 |
  | `db-stats.mjs` | parts, merges, UTXO growth |
