# Ingestion gate

A reproducible local harness that every schema or agent change must pass
**before** query benchmarking. It exists because an earlier schema experiment
(since abandoned) passed every query benchmark and then turned out to serialise
ingestion (6,578 tx/s → 2 tx/s in production; a 31.75 MB block went from 4.2 s
to 27.4 s). Query benchmarks only look at reads; this gate looks at writes.

The gate runs the real agent (`bin/chaingraph.js` of any built checkout)
against mock P2P nodes (the same `@chaingraph/bitcore-p2p-cash` approach as
`src/e2e/e2e.spec.ts`, no BCHN needed) and a throwaway Postgres, then prints a
table, writes `ingestion-gate.json`, and exits non-zero if any scenario errors,
fails a correctness check or crosses a threshold.

## Running

```sh
yarn test:ingestion-gate        # yarn build, then all scenarios (~1 min on an M-series laptop; ~5 min for a regressed build)
yarn ingestion-gate             # same, without rebuilding
yarn ingestion-gate --scenarios max-block,burst --quick
```

Options (`node scripts/ingestion-gate/run.mjs --help`):

| Option | Default | Meaning |
| --- | --- | --- |
| `--agent-dir <dir>` | this checkout | Built chaingraph checkout to test (needs `build/` and `node_modules/`). Migrations are taken from the same dir. |
| `--scenarios a,b` | all | Subset of `max-block,burst,reorg,concurrent,catch-up`. |
| `--quick` | off | Catch-up with 1,000 instead of 10,000 blocks. |
| `--thresholds <file>` | `scripts/ingestion-gate/thresholds.json` | Threshold file (see below). |
| `--out <file>` | `ingestion-gate.json` | Machine-readable report (gitignored). |
| `--pg auto\|docker\|host` | `auto` | Postgres backend (see below). |
| `--pg-image` | `postgres:18` | Docker image (`INGESTION_GATE_PG_IMAGE`). |
| `--pg-bin` | `/opt/homebrew/opt/postgresql@18/bin` | Host binaries for `--pg host` (`INGESTION_GATE_PG_BIN`). |
| `--pg-port` | `55432` | Port for the throwaway server. |
| `--pg-url` | – | Use an existing server instead (`postgres://user:pass@host:port`; the gate creates/drops `chaingraph_ingestion_gate`). |
| `--keep-pg` | off | Leave the server and last scenario DB running for inspection. |
| `--store postgres\|clickhouse` | `postgres` | Store backend of the agent under test (`INGESTION_GATE_STORE`); see [ClickHouse backend](#clickhouse-backend---store-clickhouse). |
| `--ch-url` | `http://localhost:18123` | ClickHouse HTTP endpoint for `--store clickhouse` (`INGESTION_GATE_CH_URL`; credentials may be in the URL). |
| `--seed` | `1` | Fixture seed. |
| `--label` | – | Free text stored in the report. |

**Postgres backend.** Saving one 32 MB block costs Postgres ~3 GB of backend
memory (the agent sends each block as a few very large statements), so the
3-block burst needs ~10 GB. On a Docker Desktop VM with the default 8 GB the
burst is OOM-killed. `--pg auto` therefore uses Docker only when the VM has
≥ 14 GB, otherwise a private host cluster (`initdb` into
`data/ingestion-gate/pg-<pid>`, trust auth on 127.0.0.1, deleted afterwards).
Both use `shared_buffers=256MB, max_wal_size=8GB, checkpoint_timeout=30min`.

**What is pinned** so runs are comparable: agent env
`CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB=128` (auto-sizing depends on free
memory), `CHAINGRAPH_POSTGRES_MAX_CONNECTIONS=8` (default is CPU count),
`NODE_ENV=production`, stdout at `info`, a debug log per scenario under
`data/ingestion-gate/runs/<timestamp>/<scenario>/`.

**Fixtures** are generated deterministically (AES-CTR stream keyed by the
seed) and cached under `data/ingestion-gate/fixtures/` (gitignored, ~320 MB).
Only transaction payloads are cached; headers are built at run time with a
fresh timestamp so blocks attach to each scenario's base chain and count as
"live" blocks. Transactions are 1-input/2-output, ~318 bytes, P2PKH outputs,
~199-byte unlocking scripts (the shape of the production-schema dense-block
baseline fixture); dependent blocks spend output 0 of every transaction of the previous
block. Bump `fixtureGeneratorVersion` in `lib/fixtures.mjs` when changing
the generator.

## How measurements are taken

Every scenario: fresh database with **all** `up.sql` migrations of the agent
under test, fresh mock nodes with genesis + 5 recent blocks, fresh agent; wait
for `initial sync is complete` and `enabled mempool tracking` (managed
indexes built, triggers enabled – the production write path), then measure.

- **Wall time** – from the mock node sending the announcement to the moment a
  polling connection (every 5–100 ms) sees the `node_block` row(s). It
  includes download and block decoding, so it is ~1 s higher than the agent's
  own "receipt→saved" figure.
- **tx/s** – `block_transaction` rows of the measured blocks ÷ wall time. The
  agent's "active seconds" statistics are deliberately not used.
- **WAL** – `pg_wal_lsn_diff(pg_current_wal_lsn(), start)` across the window
  (cluster-wide; the server is private to the gate).
- **Peak heap** – the agent is started with `node --import
  lib/heap-sampler.mjs`, which logs `process.memoryUsage()` every 100 ms; the
  report takes the max `heapUsed` in the window. Sampling pauses while the main
  thread is busy, so it is an observed lower bound.

## Scenarios

| Scenario | What it does | Detects |
| --- | --- | --- |
| `max-block` | One 31.80 MB block (100,001 txs, 200,001 outputs) announced via `headers`. Fails on wall time, WAL bytes, peak heap; correctness = `block_transaction` row count. | Per-block write amplification (extra UPDATEs, maintenance triggers, new indexes or triggers on hot tables), WAL growth, agent memory blow-ups. |
| `burst` | Three such blocks (block n+1 spends block n) in one `headers` message; total drain time. | Contention between concurrent block saves (locks, advisory locks, hot rows), memory pressure under backlog. |
| `reorg` | Two nodes follow a 100-block branch A (1,000 txs/block), receive 40 mempool txs, then both switch to a 101-block branch B (B[0] confirms 30 of the 40). Times convergence; checks each node's accepted chain (B accepted, A not, blocks ≤ fork still accepted), `block_transaction` for B, confirmed txs removed from `node_transaction` and archived in `node_transaction_history`, unconfirmed txs still in mempool, and no mempool row confirmed in a block of the same node. | Slow or incorrect stale-block removal/re-acceptance, mempool cleanup trigger regressions. |
| `concurrent` | A mainnet-like node (8 × 12,500-tx blocks) and a chipnet-like node (different magic/genesis, 50 × 2,000-tx blocks), each a *sequential* writer (announce a block, wait until saved, next). Run alone, alone, then together on one agent/DB. Ratio = together tx/s ÷ (sum of alone tx/s). | Cross-network serialisation (e.g. a global advisory lock: ratio → ~0.5). Perfect parallelism → 1.0; the agent's single JS thread keeps it below that. |
| `catch-up` | 10,000 small blocks (20 txs each, chained) announced via `inv` after initial sync, so they go through the live path with triggers enabled. blocks/s. | Per-block fixed costs: trigger/confirmation-path regressions, per-block round trips. |

## Thresholds

`scripts/ingestion-gate/thresholds.json`:

```json
{
  "max-block": { "maxWallSeconds": 10, "maxWalBytes": 340000000, "maxPeakHeapBytes": 1700000000 },
  "burst": { "maxDrainSeconds": 30, "maxWalBytes": 1000000000 },
  "reorg": { "maxConvergeSeconds": 6, "knownFailingChecks": ["still accepts genesis+base blocks at or below the fork point"] },
  "concurrent": { "minConcurrencyRatio": 0.6 },
  "catch-up": { "minBlocksPerSecond": 300 }
}
```

`max*` keys fail when the metric is above the limit, `min*` keys when below.
`knownFailingChecks` lists substrings of correctness checks that are known to
fail on master; they are reported as `PASS*` instead of failing the gate.
Remove an entry as soon as the underlying bug is fixed.

**How the defaults were chosen.** From three master runs and one baseline b19783b run
on the reference laptop (table below): time limits ≈ 2× the slowest
reference run (max-block 10 s vs 5.2 s; burst 30 s vs 12.4 s; reorg 6 s vs
1.4 s), WAL ≈ 1.5× (WAL is deterministic: 224.5 MB / 675 MB every run), heap
≈ 2×, catch-up ≈ 0.4× of the slowest run (300 vs 715 blocks/s – catch-up is
bimodal at ~715 or ~950 blocks/s run-to-run on this machine), concurrency
ratio 0.6 (reference 0.79–0.80; full serialisation gives ≤ 0.5). A plain 3×
time limit would *not* have caught the abandoned experiment e800183 on max-block wall time alone
(2.4×), which is why WAL is gated tighter.

**Calibrating on a new machine.**

1. Build the reference revision (baseline b19783b, tag
   `archive/pre-array-master`) and
   current master into separate directories, e.g.
   `git archive b19783b | tar -x -C /tmp/agents/b19783b`, symlink
   `node_modules`, run `./node_modules/.bin/tsc`.
2. Run the gate on each two or three times with `--agent-dir` and keep the
   JSON reports.
3. Set time limits to ~2× the slowest reference run, WAL to ~1.5×, heap to
   ~2×, `minBlocksPerSecond` to ~0.4× of the slowest run; keep
   `minConcurrencyRatio` at 0.6 unless the reference is below 0.7.
   Optionally confirm the regression build still fails (step 2 with
   abandoned experiment e800183, tag `archive/output-membership-arrays`).
4. Commit the new `thresholds.json` (or pass `--thresholds` for a
   machine-local file).

## Reference numbers

MacBook Pro (Apple M-series, 10 cores, 64 GB), Node 24.14, host PostgreSQL
18.3 (`--pg auto` → host), default settings. Master: three runs (range);
others: one run. **Bold** = fails the default thresholds – abandoned experiment e800183 fails all
five scenarios.

| Scenario | baseline b19783b | master 704aa3b | abandoned experiment e800183 |
| --- | --- | --- | --- |
| max-block wall / tx/s / WAL / heap | 5.07 s / 19,740 / 224.5 MB / 837 MB | 4.97–5.15 s / ~19,400–20,100 / 224.5 MB / 836–869 MB | **12.02 s** / 8,322 / **500.3 MB** / 840 MB |
| burst drain / tx/s / WAL | 12.40 s / 24,186 / 675 MB | 11.53–12.14 s / 24,700–26,000 / 675 MB | **44.90 s** / 6,682 / **1,684 MB** |
| reorg converge | 1.26 s | 1.31–1.36 s | **35.34 s** |
| concurrent ratio (together vs alone+alone tx/s) | 0.79 (32,953 vs 20,935 + 20,984) | 0.79–0.80 (~33,000 vs ~20,700 + ~21,100) | **0.39** (4,696 vs 7,305 + 4,653) |
| catch-up 10,000 blocks | 950 blocks/s | 715–950 blocks/s | **130 blocks/s** |
| whole gate | 58 s | 58–62 s | 288 s |

## Adding a scenario

1. Write `async (context) => result` in `scripts/ingestion-gate/lib/scenarios.mjs`
   and register it in `scenarios`. Use `startEnvironment(context, label,
   [nodeSpec…])` to get `{ agent, client, nodes, cleanup }` in steady state,
   `loadOrGenerateBlockSequence` for cached fixtures, `linkBlocks` to attach
   payloads to a node's tip, and `measureIngestion` for the standard
   announce → wait → metrics flow. Always `cleanup()` in `finally`.
2. Return `correct` (or `checks` + `failedChecks`) plus the metrics you want to
   gate; compute rates from DB row counts and wall clock.
3. Add `[metric, thresholdKey, 'max'|'min']` rules to `thresholdRules` in
   `run.mjs`, a headline in `summaryColumns`, and defaults in
   `thresholds.json`. Calibrate as above and document it here.

## ClickHouse backend (`--store clickhouse`)

```sh
docker run -d --name ch1-local -p 18123:8123 clickhouse/clickhouse-server:26.8   # once; any private 26.8 server
yarn build && node scripts/ingestion-gate/run.mjs --store clickhouse --ch-url http://localhost:18123
```

Same scenarios, mock nodes and fixtures; what changes (`lib/clickhouse.mjs`, same
surface as `lib/postgres.mjs`, selected in `run.mjs`):

- **Database.** Per scenario `chaingraph_ingestion_gate` is dropped and recreated with
  the agent's own DDL (`<agent-dir>/src/store/clickhouse/ddl/NNN_*.sql`, applied by the
  agent's compiled `build/store/clickhouse/ddl-apply.js`). The agent runs with
  `CHAINGRAPH_STORE=clickhouse`, `CHAINGRAPH_CLICKHOUSE_URL/_DATABASE` (and a dummy
  `CHAINGRAPH_POSTGRES_CONNECTION_STRING`, which the config still requires). The server
  is not started by the gate: point `--ch-url` at a private local server, never Cloud
  (the event counters below are server-wide).
- **Correctness reads and the clock** go through the agent's compiled ClickHouse checker
  (`build/store/clickhouse/checker.js`) or the pinned gated views with one
  `readSnapshot` per read: `acceptedBlockCount` (the clock), `acceptedChain`,
  `mempoolRowCount`, `historyNodeCount` and `confirmedButInMempoolCount` call the
  checker; `blockTransactionCount`, `countRows` and the catch-up `nodeBlockCount` are
  single pinned-view counts (`block_transaction_at`, `transaction_at`,
  `node_block_at`). A block therefore counts as saved only when its commit is
  `committed` **and** the node's watermark is published. Every per-node check is per
  node (the Postgres `confirmedButInMempoolCount` is per node too now).
- **Write metrics replace WAL** (taken around the same window; `SYSTEM FLUSH LOGS`
  before the log tables are read; all filtered to the gate database and
  `event_time_microseconds >=` the server time at the window start):

  | Report field | Source |
  | --- | --- |
  | `bytesWritten` | `sum(size_in_bytes)` of `system.part_log` `NewPart` (compressed bytes inserted) |
  | `mergeBytesWritten`, `merges` | the same for `MergeParts` (write amplification), and their count |
  | `partsCreated` | `count()` of `NewPart`; per table in `clickhouse.partLogByTable` |
  | `mergeSeconds` | `sum(duration_ms)` of `MergeParts` |
  | `maxActiveParts` | max active parts of one (table, partition) in `system.parts`, sampled every 1 s and at the end (compare with `parts_to_delay_insert`) |
  | `quiesceSeconds` | time after the window until `system.merges` has no merge of the database (capped at 120 s) |
  | `delayedInserts`, `rejectedInserts` | `system.events` deltas `DelayedInserts`, `RejectedInserts` (must be 0) |
  | `maxInsertMemoryBytes` | `max(memory_usage)` in `system.query_log`, `type = 'QueryFinish' AND query_kind = 'Insert'` |
  | `clickhouse.eventDeltas` | deltas of `InsertedBytes`, `InsertedRows`, `InsertQuery`, `DelayedInserts`, `DelayedInsertsMilliseconds`, `RejectedInserts`, `DuplicatedInsertedBlocks`, `MergedRows`, `MergedUncompressedBytes`, `MergeTotalMilliseconds` (server-wide) |

  Counter names were checked on 26.8.22.13 (`system_events_show_zero_values = 1`):
  there is no `MergesTimeMilliseconds`; `MergeTotalMilliseconds` is the merge-time
  counter. The agent inserts with `async_insert = 0`, so `asynchronous_insert_log`
  is not read.
- **Thresholds** default to `scripts/ingestion-gate/thresholds.clickhouse.json`: wall,
  drain, converge, ratio, catch-up and heap limits are copied from `thresholds.json`
  (G1: no worse than Postgres on the same host); `maxDelayedInserts` and
  `maxRejectedInserts` are 0. `maxBytesWritten`, `maxPartsCreated` and
  `maxMergeSeconds` were calibrated in WP6 (`docs/clickhouse-port/wp6-local-measurement.md`
  §11) from the local 26.8 reference runs: bytes 1.5×, parts 2×, merge time a 2 s floor
  (the references are milliseconds):

  | Scenario | `maxBytesWritten` | `maxPartsCreated` | `maxMergeSeconds` |
  |---|---|---|---|
  | max-block | 245 MB (ref 160.8 MB, 3 identical runs) | 34 (ref 17) | 2 s (ref 0.001 s) |
  | burst | 800 MB (ref 527.4 MB, 1 run) | 88 (ref 44) | 2 s (ref 0.007 s) |

  The other scenarios have no write-volume limits.
- **Status (WP5b, 2026-10-09).** The metric queries and the pinned correctness reads
  were verified on local 26.8 against a throwaway database (three 100k-row inserts plus
  an `OPTIMIZE`: 3 parts, 7.5 MB `NewPart`, 5.6 MB `MergeParts`, 0 delayed/rejected).
  No scenario can run yet: until the WP5 store lands the agent exits with
  `CHAINGRAPH_STORE=clickhouse is not implemented yet (WP5)`, which the gate now
  reports at once (an agent that exits before connecting no longer hangs
  `waitForPeer`).

## Known findings / TODOs

- **Deep-reorg node_block loss (pre-existing, also on b19783b).** BCHN answers
  `getheaders` from the last locator hash on its active chain; for a reorg
  deeper than ~10 blocks that hash is below the fork, so the first headers in
  the response are blocks the agent already has. `BlockTree.updateHeaders`
  splices them out as "stale" anyway, and the agent's async
  `removeStaleBlocksForNode` races `acceptBlocksViaHeaders`, leaving those
  blocks without `node_block` rows for the node. The gate's mock reproduces
  this (fork at height 5, locator falls back to genesis); allow-listed in
  `thresholds.json` until fixed.
- Postgres memory per 32 MB block save is ~3 GB (see backend note). Not
  gated yet; TODO: sample `docker stats`/backend RSS and gate on it.
- TODO: optionally pre-fill `transaction`/`input`/`output` with millions of
  rows so planner regressions that only appear on large tables (e.g. the
  block-confirmation conflict lookup scanning `input`) show up; currently all
  tables are small.
- TODO: mempool-heavy variant (confirming thousands of mempool txs per block).
- TODO: repeat each timing N times and gate on the median to reduce noise.
- TODO: run in CI (needs a ≥ 14 GB runner or `--quick` with a smaller dense block).
