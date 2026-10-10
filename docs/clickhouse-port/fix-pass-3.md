# Fix pass 3: shutdown, config, inactive parts, snapshot cost, multi-node snapshots

Date 2026-10-10. Local only (ch1-local ClickHouse 26.8, ch1-pg). Inputs: [g1-fix-pass.md](g1-fix-pass.md),
[g1-fix-pass-2.md](g1-fix-pass-2.md), [chipnet-lab.md](chipnet-lab.md) (smoke-run findings), [phase2-spike.md](phase2-spike.md).

| # | Item | Commit |
|---|---|---|
| 1 | SIGTERM no longer tries to void a committed commit | `689c461` |
| 2 | `.env` is optional | `50e8d2e` |
| 3 | Small-block syncs: short-lived inactive parts and fewer commits | `14d9a38` |
| 4 + 5 | Cached request snapshots; one snapshot pins every node of an operation | `142b057` |

Measurement conditions: the Docker VM was enlarged and restarted mid-session (both containers restarted with
`docker start`). From then on the chipnet lab's ClickHouse sync ran on the same VM (VM process at 5–11 cores), so
latency numbers taken after that point are noisy. Comparisons below are interleaved or same-session.

## 1. Shutdown voided committed commits

**Root cause.** On SIGTERM `createStore` calls `abandonInFlightWork`. A block batch whose `committed` row was
already in flight finished `markCommitted`, then hit the step hook after it: `fault('committed')` called
`abandonSignal.assertNotAbandoned()` and threw `AbandonedError`. The save's shutdown path then called `markAborted` on
the seq, which had just been committed. The guard refused it (`void_refused`, fatal) and the path logged `abort_failed`.
The commit itself stayed intact, which is why the lab's restarts were exact. The commit is untracked once committed,
so the diagnostic showed `nodeScope: []` ("node-agnostic").

**Fix.**
- Once a commit is durable, the step hook no longer checks abandonment (`clickhouse-store.ts` `fault`).
- If a batch's `committed` row landed, the batch resolves as saved whatever happens afterwards: no abort, no batch
  failure (`BlockBatch.committedResults`, `block-commit.ts`).
- Every failure or shutdown path now aborts through `CommitLog.abortIfOpen`. It voids a commit only if the commit is
  open in this process, not terminal, and its `committed` row was never sent. A commit whose `committed` row was sent
  stays open for the next start's recovery. This applies to block saves, mempool operations, header accept, re-org
  and the UTXO build. `markAborted` and its guard are unchanged and remain the invariant check.
- An in-flight `markCommitted` is never interrupted, so a committed commit always finishes. Commits that are still
  genuinely open are aborted as before (`commit_void`, warn). A clean SIGTERM logs nothing at fatal.
- Test hook only: `faultBeforeRequest` now receives the insert's dedup token.

**Tests.**
- `g1-fix.spec`, "fix pass 3: SIGTERM while a batch's committed row is in flight", utxo on and off. SIGTERM is
  injected exactly while the third batch's `…:commit_log:committed` insert is in flight. Before the fix:
  `void_refused` + `abort_failed` on that seq (red). After: no diagnostics, the batch is committed, no void at or
  below a published watermark, and a restart re-saves all 40 blocks with exact parity.
- `commit-log.spec`: `abortIfOpen` leaves committed and committed-row-sent seqs alone (no diagnostic) and aborts an
  open one.

## 2. `.env` optional

`config.ts` treats a missing `.env` (ENOENT) as empty: the defaults plus the process environment apply. When the file
exists, behaviour is the same as before: its values override the defaults, the process environment overrides both, and
any other read or parse error still throws.

Test: `src/config.spec.ts` imports the compiled config in child processes run in temp directories. Cases: no file, a
file, a file plus environment overrides, and `.env` as a directory (EISDIR still fails). The no-file case was red on
the old code. The chipnet lab's empty `.env` is no longer needed; `chipnet-lab.md` notes this.

## 3. Inactive parts on fast small-block syncs

### 3.1 What decides the inactive bytes

On disk, inactive bytes ≈ (rate of parts made inactive) × (`old_parts_lifetime` + cleanup delay). Parts become
inactive on every insert (one part per table per commit) and, mostly, on merges, which keep rewriting young parts. In
the gate's catch-up, merges wrote ~4× the inserted bytes. With the server defaults (480 s; cleanup every 30–300 s)
nothing is removed during a run of a few minutes, so inactive bytes grow linearly.

Two levers:
- **DDL:** on every table, `old_parts_lifetime = 30, cleanup_delay_period = 5, max_cleanup_delay_period = 10,
  cleanup_delay_period_random_add = 5`. The lifetime exists to survive a crash before the merged part reaches disk.
  30 s still outlives the Linux dirty-page expiry (30 s), so the setting keeps its purpose. The lab's 5 s gives up
  that margin. 60 s was measured too and doubles the steady state (below).
- **Fewer commits:** first tried raising the per-commit block cap for small blocks (up to 1,024 blocks within 4 MiB).
  Measured: **no effect**. In catch-up, batches never reached the old cap of 64. They held ~2–30 blocks, because a
  batch starts as soon as the lane is free and so takes only what arrived during the previous commit. What helps is
  a linger. While a lane streams small blocks (its previous batch started less than 200 ms ago), its open batch waits
  until 200 ms after that start. A batch stops lingering once it is no longer small (1,024 blocks or 4 MiB). A block
  that arrives on a quiet lane, such as the tip, starts at once.
  - Settings: `CHAINGRAPH_CLICKHOUSE_BATCH_LINGER_MS` (default 200, 0 = off), `…_SMALL_BLOCKS_PER_COMMIT` (default
    1024) and `…_SMALL_BATCH_BYTES` (default 4 MiB).
  - Both default to off when a test passes `maxBlocksPerCommit` explicitly.

### 3.2 Measurements (gate `--store clickhouse --scenarios catch-up`, 20 tx per block)

The gate now samples inactive parts and bytes every 0.5 s (`maxInactiveBytes`, `maxInactiveParts`, shown in the
headline). `INGESTION_GATE_CATCHUP_BLOCKS` sets the length. "Parts" = `NewPart` events (inserts).

10,000 blocks (~13 s; medians of 3 unless noted):

| Variant | blocks/s | inactive peak | parts created |
|---|---|---|---|
| **Before** (c8f62c7 DDL, no linger) | **757.3** (756.7 / 757.3 / 766.1) | **1,452 MB** (1,393 / 1,452 / 1,577) | 5,072 |
| block cap 1,024 for small blocks only | 760.6 | 1,470 MB | 5,273 |
| linger 50 ms (n = 2) | 749.4 | 1,140 MB | 3,031 |
| linger 100 ms (n = 2) | 730.3 | 901 MB | 1,638 |
| linger 200 ms, old DDL | 725.2 | 615 MB | 834 |
| DDL cleanup only (lifetime 60 then), no linger (n = 2) | 759.6 | 1,516 MB | 5,273 (the run is shorter than the lifetime) |
| **After** (DDL 30 / 5 / 10 / 5 + linger 200 ms) | **724.7** (725.0 / 714.5 / 724.7) | **613 MB** (613 / 620 / 601) | **834** |

Longer runs, where the lifetime matters (one run per row):

| Blocks | Variant | blocks/s | inactive peak |
|---|---|---|---|
| 30,000 | before | 741.8 | 5,484 MB |
| 30,000 | DDL lifetime 60, no linger | 746.1 | 5,476 MB |
| 30,000 | linger only | 714.7 | 2,601 MB |
| 30,000 | DDL lifetime 60 + linger | 705.0 / 715.1 | 2,582 / 2,575 MB |
| 60,000 | DDL lifetime 60 + linger | 698.7 | 5,514 MB |
| 60,000 | DDL lifetime 30 (final) + linger | 694.7 | 2,869 MB |
| 60,000, chipnet lab running | before | 706.6 / 626.7 | **9,639 / 7,343 MB** |
| 60,000, chipnet lab running | DDL final, no linger | 647.1 / 685.7 | 3,548 / 4,346 MB |
| 60,000, chipnet lab running | **after** (DDL final + linger) | 617.8 / 615.0 | **2,510 / 2,624 MB** |

**Reading.**
- The linger cuts insert parts 6× and halves the inactive peak.
- Lifetime 30 bounds the steady state: 60k blocks peak at ~2.5–2.9 GB, against 7–10 GB before and still growing.
- Cost: about −4 % blocks/s on the 10k run (757 → 725; bound ≥ 300). The 60k numbers taken during the lab sync
  are too noisy to say more. The DDL settings alone cost nothing measurable.
- For the chipnet regime (~900 blocks/s, emptier blocks) expect the same shape. The lab's 5 s settings remain an
  option where disk is tighter than speed (`CHIPNET_CH_PART_CLEANUP`).
- The linger is a trade, and the cost is configurable: 100 ms keeps ~97 % of the speed at 2× the parts.

### 3.3 DDL source of truth and re-apply

- `ddl-apply.ts` now checks the four cleanup settings next to granularity and block sizes.
- New `alignTableSettings` runs `ALTER TABLE … MODIFY SETTING` for every cleanup setting that differs (unset or
  another value) on an existing table. The DDL CLI calls it after applying and logs `altered to the DDL's settings: …`.
  Re-running the CLI is the re-apply path for existing databases (cg_base/cg_tip-style clones, the chipnet lab's
  `chipnet_ch`, which the CLI would set back to 30 s until the lab script re-applies its 5 s).
- Granularity and block sizes cannot be altered, so a differing table still fails the CLI as before.

Tests:
- `g1-fix-2.spec`: every one of the 20 tables carries the four settings. An old-DDL table (settings unset) is
  aligned and the granularity mismatch stays reported. A lab-altered table (5 / 1) is set back.
- `g1-fix.spec`: 150 blocks streamed every 4 ms make 41 commits without linger and 4 with 300 ms. A block after a
  quiet period is saved in 28 ms (no linger). Parity is exact.
- `clickhouse-store.spec`'s WP6b coalescing test now sets `batchLingerMs: 0`: it asserts that block 1 starts alone
  right after block 0.

## 4. readSnapshot cost

**Profile.**
- Setup: synthetic scratch database with 3,000 commits, 10 voids, 3 epochs and a 10-seq tail; 320 runs of the
  snapshot statement, `system.query_log`.
- Result: `query_duration_ms` p50 11 ms, of which `QueryAnalysisMicroseconds` was 8.9 ms (the four scalar subqueries
  are evaluated during analysis), plan optimise 1.0 ms, plan build 0.8 ms, parse 0.4 ms. 3,016 rows read.
- Reading the data costs nothing. Each scalar subquery costs ~2–3 ms of analysis. A bare `SELECT 1` round trip was
  3.3–3.6 ms (VM under load).
- A one-statement rewrite cannot drop the subqueries: their evaluation order is what makes the snapshot consistent
  (void and fence must be read after the watermarks).

**Change.** `SnapshotCache` (`visibility.ts`), used by the API for request snapshots:
- It first reads only the published watermarks of the request's nodes and node 0: one plain `GROUP BY` on
  `visibility`, no subquery.
- If they equal the cached snapshot's watermarks, that snapshot is returned. Otherwise a full `readSnapshotMulti`
  replaces it.

Why a reused snapshot is still exact for those watermarks:
- Every seq at or below a watermark is terminal, and its void row was written before it became terminal, so it was
  in the cached void set.
- Fences never cover a seq that a watermark passed.
- The tail only lacks commits that are above every watermark.

The cached snapshot's watermarks equal values read after the request started, so it is never staler than the
watermark published at request start. Parameters still come only from `readSnapshot(Multi)`. The live hub keeps
fresh full reads: it reads only after it has seen an advance.

**Numbers.**
- Interleaved bench, 400 runs, VM under load: full read 17.3 ms, cached 5.2 ms, `SELECT 1` 3.6 ms.
- `[e2e] API overhead` test, two runs:

| Document | API p50 | bare SQL p50 | API − SQL | full readSnapshot | cached request snapshot | cached snapshot + SQL |
|---|---|---|---|---|---|---|
| S5 AllHolders | 22.9–28.9 ms | 17.6–23.5 ms | **5.3–5.4 ms** (spike: 8.2–9.2) | 8.1–11.0 ms | **3.3–4.0 ms** | 20.8–27.5 ms |
| S3 HolderBalances | 21.0–21.6 ms | 15.9–16.2 ms | **5.1–5.3 ms** (spike: 7.9–8.8) | 8.0–8.3 ms | **3.2 ms** | 19.0–19.5 ms |

**Target ≤ 2 ms: not met in absolute terms here.**
- The cached path is one small round trip: ~1.5 ms above a bare `SELECT 1` on this loaded VM. The rest is the round
  trip itself.
- Removing that round trip needs the check folded into the data statement: bind the cached snapshot, add a scalar
  guard `(SELECT max(visible_seq) … ) = {visible}`, and re-run on a mismatch. With several roots, any one passing
  guard proves freshness.
- That folding is not done: it changes the one-statement-per-root shape and the live hub. It is the next step if the
  2 ms target is firm.
- A cache miss (watermark moved since the last request on that node set) costs the cheap read plus a full read.

## 5. Multi-node snapshots

- `readSnapshotMulti(client, nodeIds)` reads one snapshot pinning several nodes in one statement: `visible(n)` per node
  (one `maxMap` over `visibility`), plus one node-agnostic part (visible0, tail, void, fence).
- Void and fence are bounded by the highest seq any pinned node can show. A node that is behind therefore gets a
  void set that may include seqs above its own watermark. That is harmless: its views never show those seqs.
- `snapshotForNode` returns the per-node `VisibilitySnapshot`; the per-node rule is unchanged. `readSnapshot` is the
  one-node case (identical result, checked against the old implementation).
- The API's `PinnedSnapshot` pins every node an operation names. The spike's "must name the same node" rejection is
  gone. Subscriptions still pin one node.

Tests:
- `visibility.spec`: two nodes where node 2 is ahead (an aborted seq above visible(1)). Checks
  per-node watermarks, the void bound, equality with single-node reads and the cache hit/miss.
- `api.spec` S6: a two-node operation returns exactly the two single-node results, uses one snapshot, each root at its
  own node's watermark, with identical visible0/tail/fence/void.

## 6. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build` | ok |
| `yarn test:unit` | 122 passed, 1 todo (+3 `config.spec`) |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 150/150 (pass 2: 143). A first run had 1 failure: the in-flight-cap-16, 1-block-per-commit sync lost its 1.5 s test lease (`deadline`) while the chipnet lab saturated the VM; green on the re-run |
| API specs `build/api/*.spec.js` | 10/10 |
| e2e ClickHouse, UTXO on | 45/45 (47 `[postgres]` skipped) |
| e2e ClickHouse, UTXO off | 45/45 (`CHAINGRAPH_CLICKHOUSE_UTXO=off`) |
| e2e Postgres (ch1-pg) | 92/92 |
| eslint / prettier / cspell `src` | clean |

The e2e runs need `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`. The chipnet lab's Postgres agent holds 3201, the
e2e default, and every agent test timed out until the port became configurable (`e2e.spec.ts`).

Housekeeping: scratch databases `ch1_fp3_snap`, the gate's `chaingraph_ingestion_gate` and every spec scratch were
dropped. Pre-existing scratch databases from earlier sessions (`ch1_wp5a_crash_5d24b495`,
`ch1_wp5a_incomplete_991286ad`, `ch1_wp5a_mcrash_2d6129bd`) were left alone. Raw gate JSONs are in `/tmp`
(not committed). `scripts/chipnet-lab/*` had uncommitted changes from the lab's owner; they were not touched.
