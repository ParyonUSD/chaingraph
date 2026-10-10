# G1 fix pass 2: the event-loop stall and the spent-output lookup

Date 2026-10-10. Input: [g1-fix-pass.md](g1-fix-pass.md) (pass 1), [ch1-run-log.md](ch1-run-log.md), the G1 verdict
(`paryon_kubernetes-consolidation/docs/chaingraph/decisions/2026-10-10_clickhouse-phase1-g1.md`, must-fix 3, 5, 6),
[wp6b-write-path.md](wp6b-write-path.md) and the c1/c4 evidence in `chaingraph-performance/2026-10-10-ch-replay-nvme/`
(local only: `logs/agent-c1.final.log`, `results/sampler-cgroup-c1.tsv`, `results/sampler-cgroup-c4.tsv`,
`results/commits-c1.tsv`; the cg_tip DDL logs in `2026-10-10-ch-backfill/logs/`).

Short version:

- **The ~30 s stall is found and fixed.** The recent-output cache evicted with `map.delete(map.keys().next().value)`.
  Once the cache was full, every eviction cost O(evictions since the last rehash), so evictions were quadratic. After
  every large commit the agent spent ~30 s on one core inside `OutputRegistry.release`. A local mainnet-like replay
  reproduces it: max event-loop delay 30.5 s; `release` took 193 of 249.5 s of CPU. After the fix, the max delay is
  0.81 s on that replay and 0.34–0.46 s on max-block and burst. The replay now runs 6× faster (5.2k → 31.6k tx/s).
- **There was no 8,192 granularity.** The DDL that the lab applied to cg_base and cg_tip set `output` to 1,024 (DDL
  logs). 8,192 is the server default; `clickhouse-version.txt` recorded it from `system.merge_tree_settings`. Nothing
  dropped the setting. The real cost: each spent outpoint touches one random granule in every part, and each touched
  granule is decompressed in 64 KiB–1 MiB blocks for every column, one chunk after another. Fix: `output` at
  granularity 128 with 4 KiB blocks (the projections keep 1,024), lookups forced onto the base table, one tuple `IN`,
  sorted chunks run concurrently. The DDL CLI now fails when an existing table's settings differ from the DDL.
  Result for 3,000 outpoints on 12 M outputs: 225 ms → 44 ms, rows read 5.6 M → 0.38 M.
- **The concurrency ratio has not improved.** It stays at 0.57 (limit 0.6): the gate's `concurrent` scenario never
  filled the cache, so it never hit the stall. Its bound is still JS CPU (parsing and encoding).

## 1. Item A: the event-loop stall

### 1.1 Lab evidence

The cgroup sampler (`sampler-cgroup-c1.tsv`, ~3.5 s resolution) shows the signature. The agent ran at exactly one
core with flat memory for 30–36 s, starting right after a commit landed in ClickHouse. Its "Saved" lines appeared
only when that run ended.

| Commit landed (commit_log) | Agent at ~1.0 core, memory flat | Agent logs the commit | Next event |
|---|---|---|---|
| ...567 at 08:33:45.852 | 08:33:48 → 08:34:22 (5.27 GB flat) | 08:34:23.402 | 08:34:23.659 `socket hang up` on the next batch's read: a keep-alive socket the server had closed during the stall (the 08:34 hole of pass 1) |
| ...587 at 11:57:36.597 | 11:57:34 → 11:58:10 (4.13 GB flat) | 11:58:09.573 | 11:58:09 error 209: a request body was in flight when the loop stopped, and the server gave up after 30 s (the ...588 abort of pass 1) |

In c1, seven such runs lasted ≥ 15 s (17–36 s). c4 (UTXO off) had eight (17–35 s). The stall therefore affects
both arms, which explains why HOL p95 failed on both (33.8 s and 25.6 s). Single-threaded and allocation-free points
to a pure-JS loop over existing data, not encoding, GC or parsing (those show 2.5–3 cores and a growing heap in the
same samples).

### 1.2 Root cause (confirmed by profile)

`OutputRegistry` (500k outputs) and `TransactionRegistry` (1M hashes) are bounded insertion-order caches.
`remember()` evicted with `this.recent.delete(this.recent.keys().next().value)`. V8's ordered hash table keeps a hole
for each deleted entry until the next rehash, and a fresh iterator has to skip every hole from the start. Once the
cache is full, each eviction therefore costs O(evictions since the last rehash), and a commit's `release()`
(re-remembering every output it created) is quadratic.

Microbenchmark, 500k-entry `Map`, insert + evict:

| Evictions | `keys().next()` per eviction | Persistent iterator (fix) |
|---|---|---|
| 10,000 | 22 ms | 4 ms |
| 100,000 | 2,909 ms | 31 ms |
| 200,000 (cumulative 360k) | 15,998 ms | 63 ms |
| 1,000,000 | – | 330 ms |

**Local reproduction.** A new opt-in gate scenario `replay` sends 64 blocks × 20,000 tx (407 MB, 1.28 M tx; each
block spends output 0 of every transaction in the previous block) back-to-back, with
`CHAINGRAPH_CLICKHOUSE_MAX_BLOCKS_PER_COMMIT=64`, `CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS=1000` and `--cpu-prof`. On
07a3f32 (pass 1), the event-loop delay windows reached 2.9 s, then 11.8 s, then **30.5 s**, and the drain took 246 s
(5,196 tx/s). The CPU profile covers 249.5 s; `OutputRegistry.release` → `remember` accounts for **193.1 s (77 %)**
and `TransactionRegistry.remember` for 10.9 s. Everything else was small: block parsing 8.2 s, GC 10.2 s, RowBinary
encoding ~3 s.

### 1.3 Fixes

| # | Change | Where |
|---|---|---|
| A1 | `RecentCache`: a bounded FIFO that keeps one live `Map` iterator across calls. Map iterators survive insertion, deletion and rehash. Every entry before the cursor has already been evicted, and a refreshed key is re-appended after it, so each hole is skipped once: O(1) amortized. Used by both registries; capacities unchanged | `recent-cache.ts`, `utxo.ts`, `block-commit.ts` |
| A2 | `CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS` (default off): every interval, one `eventLoopDelay` log line with p50, p99 and max loop delay (`perf_hooks.monitorEventLoopDelay`) and heap | `components/event-loop-monitor.ts`, `config.ts`, `index.ts` |
| A3 | The agent converts a block to Chaingraph form in slices of 5,000 transactions with a `setImmediate` yield between slices, one block at a time in arrival order. A 32 MB / 100k-tx block was ~0.8 s in one piece, the largest stretch left after A1 (max-block loop max 0.82 s → 0.34 s) | `bitcore.ts` (`bitcoreBlockToChaingraphBlockInSlices`), `agent.ts` (`scheduleBlockParse`) |
| A4 | Gate: opt-in `replay` scenario (`INGESTION_GATE_REPLAY_BLOCKS`, `INGESTION_GATE_REPLAY_TX`). The harness passes `CHAINGRAPH_CLICKHOUSE_*`, the diagnostic and `INGESTION_GATE_AGENT_NODE_OPTIONS` (as the agent's `NODE_OPTIONS`, e.g. `--cpu-prof`) through to the agent, and reports each scenario's worst loop delay (`eventLoopMaxMs`, also shown in the headline) | `scripts/ingestion-gate/` |

Not needed: chunked or streamed RowBinary encoding and a worker thread. After A1 and A3 no encoding stretch exceeds
~0.35 s on these workloads (`encodeUtxoRows` for a 100k-tx batch is the largest).

Regression test: `recent-cache.spec.ts`. It checks FIFO semantics against a reference model, and that 300k evictions
from a full 500k `OutputRegistry` finish in < 3 s. The test failed on the old eviction (checked by patching the
compiled old loop back in) and passes on the new one (~0.9 s including setup).

### 1.4 Before and after (event-loop delay, max over the scenario, 1 s windows)

| Scenario | 07a3f32 (pass 1) | After A1 only | After A1 + A3 (medians of 3) |
|---|---|---|---|
| replay 64 × 20k tx, 64 blocks/commit | **30,501 ms**, drain 246.3 s, 5,196 tx/s | 678 ms, 42.7 s, 30.0k tx/s | **813 ms** (764 / 852 / 813), drain **40.5 s**, **31.6k tx/s** |
| max-block (100,001 tx) | 779 ms | 818 ms (818 / 802 / 844) | **338 ms** (339 / 322 / 338) |
| burst (3 × 100,001 tx) | 808–969 ms | 870 ms (839 / 1,032 / 870) | **458 ms** (465 / 454 / 458) |
| reorg | 600 ms | 548 ms | 603 ms |
| concurrent | 137 ms | 134 ms | 112 ms |
| catch-up `--quick` | 28 ms | 23 ms | 27 ms |

The "07a3f32" column comes from a frozen build of that revision run through the current harness, with a preload that
logs the same histogram (`--import loop-preload.mjs`). The replay's remaining ~0.8 s windows are GC pauses with a
2–3.5 GB heap plus a batch's encode. They are not one long synchronous stretch: the profile's longest stretch without
an idle sample mixes several tasks.

Target "max event-loop delay < 1 s during max-block and burst": met (0.34 s and 0.46 s).

## 2. Item B: the spent-output lookup

### 2.1 The granularity discrepancy

- [ch1-run-log.md](ch1-run-log.md) at 08:29Z says `cg_tip.output … index_granularity 8192`.
- `2026-10-10-ch-backfill/logs/ddl-clone-cg-tip.log` (and `ddl-cg-base`) shows that the DDL applied to cg_tip had
  `index_granularity = 1024` on the five core tables plus `tx_acceptance`. cg_base came from the same DDL, and
  `ATTACH PARTITION FROM` needs identical table settings anyway.
- `logs/clickhouse-version.txt` line 3, `index_granularity 8192`, is the **server default** from
  `system.merge_tree_settings`. That default applies only to tables that do not set the value (`utxo`, `node_block`,
  `commit_log` and so on).

Conclusion: no evidence that `output` was at 8,192. The run log most likely quoted the server default. The DDL path
did not drop the setting.

The DDL does have a real weak spot: `CREATE TABLE IF NOT EXISTS` never changes an existing table. A database created
by an older DDL or by hand silently keeps its old settings. The fix makes the DDL the enforced source of truth (B4).

790k rows per call is what 1,024 predicts. Each distinct spent transaction hits one granule in **every** part, because
all parts span the whole hash range. So rows read ≈ distinct hashes × parts × 1,024, and every touched granule
decompresses a 64 KiB–1 MiB block of every column.

### 2.2 Measurements (local, 12 M synthetic outputs, 4 M transactions, 2 parts, warm)

Bench: 3,000 random existing outpoints (≈ one 3k-input block), the real `lookupStoredOutputs` or its SQL, and
`system.query_log` (`read_rows`, `read_bytes`, ProfileEvents) per call. Medians of 9.

| Variant | Wall | Rows read | Bytes read | Notes |
|---|---|---|---|---|
| **Before**: 1,024 / default blocks, 3 × 1,000 chunks serial, unsorted | **225 ms** | **5.62 M** | **353 MB** | ~1,870 rows per outpoint |
| same table, chunks concurrent (4) | 86–98 ms | 4.71 M | 308 MB | |
| granularity 256, default blocks | 92 ms | 0.73 M | 66 MB | wall unchanged: still decompresses whole 64 KiB+ blocks (OSReadChars ~120 MB per query) |
| granularity 128, default blocks | 91 ms | 0.38 M | 33 MB | same |
| granularity 256, 8 KiB blocks | 52 ms | 0.73 M | 66 MB | |
| granularity 128, 4 KiB blocks | 46 ms | 0.38 M | 33 MB | |
| 128 / 4 KiB, new DDL with projections, no hint | 90 ms | 2.66 M | 242 MB | the optimizer picked **p_category** (zero category, then hash: looks cheaper by marks) |
| + `optimize_use_projections = 0` | 50–59 ms | 0.38 M | 33 MB | key analysis of `hash IN … AND (hash, index) IN …` ~21 ms per query |
| + one tuple `IN` only | 50 ms (chunks of 1,000) | 0.38 M | 33 MB | key analysis ~10 ms per query |
| **After**: + chunks of 750, 4 concurrent | **44 ms** | **0.38 M** | **33 MB** | ~125 rows per outpoint |

Full scans of `output` are unaffected (`sum(value), sum(length(locking_bytecode))` over 12 M rows: 40 ms vs 43 ms).
The compressed size grows ~3 % (768 → 793 MiB on this random data).

### 2.3 Fixes

| # | Change | Where |
|---|---|---|
| B1 | `output`: `index_granularity = 128, min_compress_block_size = 4096, max_compress_block_size = 4096`. `p_script` and `p_category` keep `index_granularity = 1024` and the default block sizes via `WITH SETTINGS` (ClickHouse 26.8 supports per-projection settings; checked: table 128 / projections 1,024 marks) | `ddl/010_core.sql`, `ddl/060_projections.sql`, `ddl/README.md` |
| B2 | The spent-output lookup reads the base table (`optimize_use_projections = 0`) with a single `(transaction_hash, output_index) IN (…)` | `block-commit.ts` (`lookupStoredOutputs`) |
| B3 | Spent-output and tx-hash lookups deduplicate and sort their keys (one contiguous key range per chunk), and run chunks up to 4 at a time (`mapChunksConcurrently`, `lookupConcurrency`). Default chunk size 1,000 → 750, so a 3k-input batch is one round of four queries | `block-commit.ts`, `clickhouse-store.ts` |
| B4 | DDL CLI: after applying, compares `index_granularity` and the block sizes of every table with its `CREATE TABLE` in the DDL. It fails and names the tables if they differ, and otherwise logs the settings in force (`table settings as in the DDL: … output.index_granularity=128 …`) | `ddl-apply.ts` (`ddlTableSettings`, `checkTableSettings`), `ddl-cli.ts` |

Lookups are already per batch: one `resolveSpends` call per batch for every outpoint that misses the registry. Within
a call they are now concurrent. Across batches, tip mode still runs one batch per node set at a time (WP6b). The
c1 serialisation was the chunk loop plus the stall.

Tests: `g1-fix-2.spec.ts`. Unit tests cover `mapChunksConcurrently` (order kept, ≤ concurrency in flight),
`sortedOutpoints`, and the DDL pinning `output` to 128 / 4 KiB. Two `[e2e]` tests: `checkTableSettings` reports an
`input` table recreated at 8,192; and a lookup of 300 outpoints in 40k transactions finds every value, uses no
projection and reads ≤ 2 × 128 rows per outpoint. Both e2e tests fail on the old DDL or SQL: 1,024-row granules, or
p_category chosen.

## 3. Item C: gate re-measure

Same host as WP6b and pass 1 (MacBook, 10 cores, Docker VM 8 GB, ClickHouse 26.8 ch1-local), `--store clickhouse
--quick`, diagnostic on (1 s windows), final code. Medians of 3.

| Scenario | WP6b | Pass 1 (07a3f32) | 07a3f32 re-run here (n = 1) | **Pass 2** | Limit |
|---|---|---|---|---|---|
| max-block wall (100,001 tx) | 4.43 s | 4.44 s | 4.52 s | **3.40 s** (3.40 / 3.30 / 3.40), 29.4k tx/s | ≤ Postgres 7.58 s: pass |
| burst drain (3 × 100,001 tx) | – | – | 11.07 s (16.58 s in an earlier run) | **9.16 s** (9.05 / 9.16 / 9.31), 32.7k tx/s | pass |
| re-org converge | 3.54 s | 3.82 s | 3.75 s | **3.71 s** (3.65 / 3.89 / 3.71) | ≤ 6 s: pass |
| concurrent ratio | 0.57 | – | 0.56 | **0.57** (0.57 / 0.58 / 0.57); together 27.9k, alone 29.2k + 19.6k tx/s | ≥ 0.6: **fail** |
| catch-up `--quick` (1,000 blocks) | 645.6 blocks/s | 644.7 blocks/s | 709 blocks/s | **699 blocks/s** (699 / 699 / 761), ~667 parts | ≥ 300: pass |
| replay (opt-in, 64 × 20k tx) | – | – | 246.3 s, 5.2k tx/s, loop 30.5 s | **40.5 s**, 31.6k tx/s, loop 0.81 s | – |
| max loop delay, max-block / burst | – | – | 0.78 / 0.81 s | **0.34 / 0.46 s** | < 1 s: pass |

**Concurrency ratio: not improved.** The gate's concurrent scenario (~180k outputs) never fills the 500k cache, so the
stall did not affect it. Its bound is the agent's one JS thread doing block conversion and RowBinary encoding for both
networks (WP6b §6). After A1 alone the ratio read 0.52, because "alone" got faster (34.4k) while "together" stayed at
~28k. With A3 the "alone" runs land at 27–31k and the ratio is back to 0.57. Fixing must-fix 6 still needs encoding or
conversion off the main thread.

Suites (final code): see §5. Raw gate JSONs stayed local (`/tmp`), as raw results are not committed.

## 4. What the lab re-run should watch

- **Enable the diagnostic:** `CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS=10000`. Every `eventLoopDelay` line should have
  `maxMs` < 1,000. Expect occasional 1–2 s windows from GC with a multi-GB heap (`--max-old-space-size=12288`). A
  window ≥ 5 s is a new stall: take a `--cpu-prof` and compare with §1.2.
- **No 209, no `socket hang up`.** Both were side effects of the stall. They still retry (pass 1), so check the log for
  retries too.
- **Apply the DDL with the CLI and keep its output.** The `table settings as in the DDL` line should list
  `output.index_granularity=128 output.min_compress_block_size=4096 output.max_compress_block_size=4096`. A clone into
  a pre-existing database fails loudly now. `ATTACH PARTITION FROM` between databases needs the same settings on both
  sides, so clone from a base built with this DDL.
  - Kit note: `ch1-clone.sh`'s granularity filter and the golden-256 `sed` (`index_granularity = 1024 → N`) now also
    rewrite the two output projections' `WITH SETTINGS`. Adjust the kit if the 256 arm is run.
- **Lookup cost on mainnet:** query `system.query_log` for the spent-output query (`FROM output WHERE
  (transaction_hash, output_index) IN`). Expect `read_rows` ≈ distinct spent txs × parts × ≤ 128, and per-query
  durations in the tens of ms. `projections` must be empty.
- **Memory and parts:** `output` at 128 rows per granule has 8× the marks. At mainnet scale that is 1.05 B / 128 ≈
  8.2 M granules: a ~300 MB primary index if fully loaded, plus mark-cache pressure. Watch ClickHouse memory and the
  `MarkCacheMisses` / `PrimaryIndexCache*` events. The merged backfill partition should keep the part count low; each
  extra part multiplies rows read per lookup.
- **Throughput limits left:** the tx-hash lookup (`transaction`, 1,024 rows per granule, per-block id phase; 2,056
  calls = 55 s in c1) is unchanged apart from sorted, concurrent chunks. The pending-spend timeout gating (pass 1) and
  the JS thread (§3) remain. If c4-style replay stays far below 23k tx/s, profile again: the next costs on the agent
  are block conversion (~8 of 41 busy CPU-seconds on the replay), GC (~9 s) and RowBinary encoding.
- **HOL p95:** expected to drop well below the 25–34 s seen, since those were the stall. The 5 s target still depends
  on commit cadence, which in tip mode is one batch at a time per node set.

## 5. Results

| Suite | Result |
|---|---|
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 143/143 (pass 1: 134; +4 `recent-cache`, +5 `g1-fix-2`) |
| e2e ClickHouse, UTXO on | 45/45 (47 `[postgres]` skipped) |
| e2e ClickHouse, UTXO off | 45/45 (`CHAINGRAPH_CLICKHOUSE_UTXO=off`) |
| e2e Postgres (ch1-pg) | 92/92 |
| `yarn build`, `yarn test:unit` | ok; 114 passed + 1 todo |
| eslint / prettier / cspell on changed files | clean |

Housekeeping: Docker Desktop restarted once during the session (16:42Z) and stopped ch1-local and ch1-pg. Both were
restarted with `docker start`; the spec run that was in flight died with them (ClickHouse unreachable → lease lost →
`onFatal` SIGTERM) and was re-run green. Two of its scratch databases (`ch1_wp5a_crash_5d24b495`,
`ch1_wp5a_mcrash_2d6129bd`, ~200 KB) are still on ch1-local: dropping them was refused by the session's permission
policy. Scratch databases `ch1_fix2_*` were dropped.
