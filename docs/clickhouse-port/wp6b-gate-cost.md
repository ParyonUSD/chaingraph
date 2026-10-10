# WP6b-B: gate read cost and lease recovery

Follow-up to `wp6-local-measurement.md` §5 (gate cost on reads, kill criterion 3) and §9 items 3 and 7.
Local only: ClickHouse 26.8.22.13 in Docker, no Cloud/GKE.

**Result.**
- The pinned `*_at` views now run no subqueries. Every gate input is a parameter, read once per request by a
  one-query `readSnapshot`.
- The fixed cost per request fell by about half: 2.7–8.7 ms became 1.3–6.2 ms. The scan is now +8 %.
- Point lookups are still +96–200 % locally, so **the ≤ 20 % target is not met on sub-2 ms point lookups**. The rest of
  the cost is fixed by the server: about 0.4–0.6 ms to resolve a parameterised view, and about 0.1–0.25 ms to analyse
  each array-valued gate clause. That cost does not grow with rows, so on golden (base reads of 10–100 ms) it is
  1–6 %. §4 lists what is left to try.
- Lease loss is now recoverable. See §6, including the contract the store must implement.

## 1. The pinned-view parameter contract

Every parameter is required. The values must come from `readSnapshot`, never from a client request (§1.3).

| View family | Parameters |
|---|---|
| node-scoped `node_block_at`, `node_transaction_at`, `tx_acceptance_at`, `utxo_at`, `utxo_by_script_at`, `node_block_history_at`, `node_transaction_history_at` | `node UInt32`, `visible UInt64`, `fence Array(UInt64)`, `void Array(UInt64)` |
| node-agnostic `block_at`, `transaction_at`, `block_transaction_at`, `output_at`, `input_at` | `visible0 UInt64`, `tail Array(UInt64)`, `fence Array(UInt64)`, `void Array(UInt64)` |

- `visible` = visible(n). `visible0` = visible(0). `tail` = the seqs committed above visible(0).
- `fence[e]`, 1-based and dense for epochs 1 up to the epoch of the snapshot's highest seq: the highest valid
  **counter** (`commit_seq & 0xFFFFFFFFFF`) of epoch e, or `0xFFFFFFFFFF` if e is not fenced. Counters rather than
  full seqs keep the parameter small: an epoch that never committed costs two bytes (`0,`).
- `void` = the aborted seqs up to the snapshot's highest seq. When there are more than `voidInlineLimit` (512), it
  is the overflow sentinel `[18446744073709551615]`.

The predicate, identical in every `*_at` view:

```sql
WHERE node_internal_id = {node:UInt32}                         -- node views
  AND commit_seq <= {visible:UInt64}                           -- node views
  -- node-agnostic instead: (commit_seq <= {visible0:UInt64} OR has({tail:Array(UInt64)}, commit_seq))
  AND bitAnd(commit_seq, 1099511627775) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40))
  AND NOT has({void:Array(UInt64)}, commit_seq)
  AND (NOT has({void:Array(UInt64)}, 18446744073709551615) OR commit_seq NOT IN (SELECT commit_seq FROM cg.commit_void))
```

- **Fence.** An epoch past the end of the array reads 0 and is invisible. Only seqs above the snapshot's bound
  have such epochs, so the watermark already hides them. A forged `visible` therefore cannot reach epochs the
  snapshot did not see.
- **Overflow fallback.** If `void` is not the sentinel, the last clause folds to `1` and `commit_void` is never
  read (checked with `system.query_log.tables`). If it is the sentinel, the `commit_void` subquery runs.
- **Helpers** in `visibility.ts`:
  - `pinnedView(name, db?)` gives the call text with placeholders, for example `utxo_at(node = {node:UInt32}, …)`.
  - `nodeViewArgs` and `agnosticViewArgs` give the argument lists.
  - `snapshotParams(snapshot)` gives every value. `nodeViewParams` and `agnosticViewParams` give the subsets.
  - `gateSql.visibleAt()` and `gateSql.pinnedValid()` give the same predicate for base-table reads (the checker's
    fault injection).
- **The `*_v` views are unchanged.** They compute everything with subqueries, for ad-hoc use. The API path uses
  `*_at`.

### 1.1 `readSnapshot`: one query

`snapshotSql` in `visibility.ts`. The harness mirrors it in `scripts/parity/lib/engines.mjs`. ClickHouse evaluates a
scalar subquery before any subquery that references it, so the order comes from data dependencies:

1. `marks` = (visible(n), visible(0)) from **one** read of `visibility`;
2. `tail` = committed seqs `> marks.2`;
3. `bound` = greatest(visible(n), visible(0), max(tail));
4. `void` = `commit_void` seqs `<= bound`, with `LIMIT voidInlineLimit + 1`;
5. `fence` = `epoch_fence` rows with `epoch <= bound >> 40`, expanded to the dense counter array.

I checked the ordering with `nowInBlock()` and `sleep(1)`: a dependent subquery runs one second after the subquery
it depends on, even when it comes first in the select list.

### 1.2 Why this is exactly the old rule

- **visible(n) and visible(0).** Reading both in the same instant is enough. The old code read visible(n) first
  and the tail later. What matters is that the tail is read after both watermarks, and the dependency in step 2
  guarantees that.
- **void.** An abort writes `commit_void` before the commit becomes terminal, and a watermark only passes
  terminal commits. Step 4 runs after step 1, so every aborted seq at or below either watermark is in `void`. A
  tail seq that was later aborted by a recovery race is also covered, because `bound` includes the tail. This is
  the same residual race as WP4 §5.
- **fence.** A new holder writes its fences before its first commit. Step 5 runs after step 1, so any watermark
  that covers new-epoch seqs comes with the fences that hide the stale epoch.
- **The `*_v` views** keep `fence_max_seq` positional from epoch 1. The `*_at` views look each epoch up by number,
  which gives the same result for the dense fences that recovery writes.

### 1.3 Trust boundary

The old `*_at` views clamped `visible` to the live watermark with a subquery, so a forged W could not expose an
unresolved commit. That clamp was a large part of the per-query cost. The views now trust their parameters: the
API must take them only from `readSnapshot`. `visibility.spec` checks two things:
- an old snapshot with a forged `visible = 2^64-1` still shows nothing, because its fence covers no epoch;
- with a later fence, a forged W shows only what is committed and valid.

### 1.4 Bounds

| Parameter | Size | Guard |
|---|---|---|
| `void` | about 1.1 µs per element per view, per query (measured: +0.3 ms at 256, +5 ms at 4,096 per view). ClickHouse rejects any single HTTP parameter over `http_max_field_value_size` (128 KiB, about 7,500 seqs). | Over 512: the sentinel and the `commit_void` fallback. Steady state needs compaction or recovery (A's area) to delete `commit_void` rows once their data rows are gone (aborted rows are GC'd), so the set stays small. |
| `fence` | One counter per lease epoch, about 2–14 bytes each. Within the 128 KiB limit that is ≥ 9,000 epochs worst case, about 60,000 for epochs that never committed (a crash loop). | Over 120,000 bytes, `readSnapshot` throws `GateParameterOverflowError`. There is no cheap fallback: a positional fence cannot be expressed as a lazily built `IN` set, and a scalar subquery would be evaluated even in a constant-false branch (measured). Fixes, if ever needed: raise `http_max_field_value_size`, or have compaction drop fences of epochs whose rows above the fence are gone. |
| `tail` | Grows only while visible(0) is stalled by an open commit. | Unchanged from WP4: the same 128 KiB parameter limit applies. |

## 2. EXPLAIN

`gate-cost.mjs` now saves `EXPLAIN indexes = 1` for both variants of every query.

**Key and projection use is unchanged.** Before (`ch1_wp6bb_old`, HEAD DDL) and after (`ch1_wp6bb_maxblock`) use
the same primary-key keys, conditions, parts and granules on every query:

| Query | Granules (before and after) |
|---|---|
| `tx_acceptance` (transaction_hash, node_internal_id) | 1/293 |
| `utxo_by_script` (node_internal_id, prefix) | 1/98 |
| `output` and `transaction` | 1/586 and 1/293 |
| `input` | projection `p_outpoint`, 1/293 |
| `utxo_at` full scan | 98/98 |

The partition and min-max keys on `commit_seq` are still evaluated. `visibility.spec` `[e2e] pinned views keep
primary-key and projection use` passes: `utxo_at` reads 1/13 granules, and `output_at` reads `p_script`.

**What changed:**
- The `CreatingSets` step (the `commit_void` and `commit_log` IN subqueries) is gone.
- The gate folds into `PREWHERE` as constants:
  `NOT has([], commit_seq) AND bitAnd(commit_seq, 1099511627775) <= [1099511627775][bitShiftRight(commit_seq, 40)] AND commit_seq <= 1099511627789`.
  The overflow clause shows as `AND 1`.
- `read_rows` gated = base exactly. Before, gated read 2 extra rows from the subqueries.
- Several plans now use the binary-search key algorithm (before: generic exclusion search). This happens on the
  `tx_acceptance` and `utxo_by_script` lookups and the `utxo_at` scan. It is equal or better.

## 3. Read cost, before and after

**Method.** WP6's `scripts/measure/gate-cost.mjs` on a copy of the max-block gate database: 1 node, 300k
transactions, 600k outputs, 800k UTXO rows, epoch 1, no void, no fence rows.
- I made two copies: `ch1_wp6bb_old` with the HEAD (WP4) DDL, read with `--legacy-views`, and
  `ch1_wp6bb_maxblock` with the new DDL.
- 41 alternating gated/base runs after 5 warm-ups. The figure is the server `elapsed_ns` median, taken as the
  median of two runs per side, interleaved in time.
- Gated and base returned identical rows in every run.
- One fix to the script: the address query compared the `String` prefix column with a `FixedString(25)` literal,
  which left the sort key unused (98/98 granules) in both variants. It now uses `unhex(...)` (1/98).
- The host was shared with other agents' gate runs (load 3–11), so absolute numbers drift by ±30 % between runs.
  The overhead ratios were stable within about ±15 points.

| Query (views) | Base before | Gated before | Overhead before | Base after | Gated after | Overhead after | Fixed cost (gated − base) |
|---|---|---|---|---|---|---|---|
| UTXOs by locking bytecode (`utxo_by_script_at`) | 1.35 ms | 4.06 ms | +200 % | 1.59 ms | 3.12 ms | **+96 %** | 2.70 → 1.53 ms |
| transaction by hash + outputs (`transaction_at` ⋈ `output_at`) | 1.60 ms | 7.01 ms | +338 % | 1.81 ms | 5.44 ms | **+200 %** | 5.41 → 3.63 ms |
| block transactions, first 1,000 (`block_transaction_at` ×2 ⋈ `transaction_at`) | 6.16 ms | 14.89 ms | +142 % | 6.78 ms | 12.95 ms | **+91 %** | 8.74 → 6.18 ms |
| acceptance of a tx by the node (`tx_acceptance_at`) | 1.20 ms | 4.14 ms | +244 % | 1.21 ms | 2.53 ms | **+110 %** | 2.93 → 1.32 ms |
| node UTXO aggregate, full scan (`utxo_at`) | 25.23 ms | 29.63 ms | +17 % | 27.81 ms | 30.08 ms | **+8 %** | 4.41 → 2.27 ms |
| spender of an outpoint (`input_at`) | 1.11 ms | 4.77 ms | +329 % | 1.17 ms | 2.87 ms | **+146 %** | 3.66 → 1.70 ms |

Earlier runs of the same pair agree:
- First runs, before the prefix fix, before → after: address +406 → +95 %, tx+outputs +299 → +196 %, block
  page +137 → +92 %, acceptance +207 → +111 %, scan +18 → +13 %, spender +308 → +176 %.
- The WP6 report's own figures: +397 / +388 / +140 / +295 / +25 / +206 %.

**Variants** (`gate-cost.mjs` flags, one run each):

| Variant | Point lookups | Scan | Notes |
|---|---|---|---|
| Views without the overflow clause (experiment, not shipped) | +77–164 % | +10 % | The dead `commit_void` subquery still costs about 0.1–0.3 ms to analyse. |
| `--void-pad 64` / `256` / `1024` / `4096` (fake aborted seqs in the parameter) | acceptance +134 / +148 / +200 / +386 % | +12 / +14 / +19 / +30 % | Hence `voidInlineLimit = 512`. |
| `--void-overflow` (sentinel: the `commit_void` fallback with an empty table) | +132–195 %; `utxo_by_script_at` +375 % | +13 % | About the old cost, as expected for the fallback. |
| Custom-settings plain views (`getSetting('SQL_cg_…')`, experiment, not shipped) | tx+outputs +142 %, acceptance +78 % | — | Faster on the single-table lookups, but `utxo_by_script` lost its key condition (7.6 ms). Dropped. |

`readSnapshot` costs about the same as before: 5.5 ms median for the new single query, which also reads void and
fence, against 6.1 ms for the old two queries.

## 4. Why point lookups are still over 20 %, and what is left

I priced each part of the gate on the `output` point lookup, where base is about 1.0 ms. Medians of 61 runs:

| Added to the base query | Cost |
|---|---|
| `commit_seq <= {visible0}` (scalar) | about 0 |
| `has(tail)`, `NOT has(void)`, the fence `arrayElement`, the folded overflow clause | about 0.1–0.25 ms each; the same with literals or parameters, and unchanged by `compile_expressions`, the query-condition cache, prewhere settings or the old analyzer |
| Resolving a parameterised view instead of the same SELECT inline | about 0.4–0.6 ms (`QueryAnalysisMicroseconds` 1,204 vs 263, `QueryPlanBuildMicroseconds` 632 vs 106) |
| Plain (non-parameterised) view | about 0.1 ms |

So one pinned view costs about 1.1–1.4 ms of fixed analysis on this server, and a query pays it once per view
instance. The gate's remaining cost is analysis time, not per-row work: `read_rows` is equal and the scan overhead
is 8 %. Options, none implemented:
1. **Inline the predicates in the API's own SQL** instead of calling views (`gateSql.visibleAt`). This saves the
   0.4–0.6 ms of view resolution per view; inline measured +50–70 % on a 1 ms lookup. It breaks "the API reads only
   views" (checklist), so it is a design decision for the lead.
2. **Drop the overflow clause** and refuse more than `voidInlineLimit` voids at the snapshot. This saves about
   0.1–0.3 ms, at the cost of availability if compaction falls behind.
3. **Re-measure on golden** before deciding. With 10–100 ms base reads, about 1.3 ms per view is 1–13 %. Kill
   criterion 3 was set for the golden instance, and this local measurement only bounds it.

## 5. Parity harness

`compare.mjs` takes one snapshot (`readClickHouseSnapshot`, one query) and passes `fence` and `void` as literals,
so it has no parameter size limit and never uses the overflow fallback.

**Output is unchanged.** On the selftest data I ran the HEAD harness against the HEAD views and the new harness
against the new views, in three modes:
- `summary.json` is identical, apart from durations.
- `parity.tsv` is identical as a set. Only the order of the timestamp-pass rows differs, and that order already
  varies between two runs of the same harness (parallel completion).

`node scripts/parity/selftest.mjs`: 25/25.

## 6. Lease

### 6.1 API (`writer-lease.ts`)

- **ttl** is `CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS` (an integer ≥ 1000, else `RangeError`), with **default 120 s**.
  The safety margin and the renew period both default to ttl / 6, which is 20 s each. `WriterLeaseOptions` still
  overrides all three.
  - Why: WP6 saw about 60 s synchronous stalls, twice per 100k-tx block (the O(n²) pending-spend pass, which A is
    removing), against the old 30 s ttl and 5 s margin.
  - A stall survives as long as stall + renew period < ttl − margin, which allows 80 s.
  - A dead writer is replaced within 120 s, plus `settleMs`.
- **Heartbeat.** `startHeartbeat(onLost)` runs a self-rescheduling timer (no `setInterval`). Each `tick()`:
  - If the monotonic clock is already at or past the local deadline (`renewal start + ttl − margin`), the loop
    stalled past the margin. It reports **lost (`stalled`)** at once and **does not write a late heartbeat**.
  - Otherwise it calls `renew()`. A `LeaseLostError` from the renewal is a loss (`taken-over`, or `deadline` if
    the renewal itself crossed the deadline).
  - Any other error (I/O) is retried on the next tick. It becomes a loss (`deadline`) only once the deadline
    passes.
- **`onLost` semantics.**
  - It runs exactly once per held epoch, with a `LeaseLostError { reason: 'stalled' | 'taken-over' | 'deadline'
    | 'not-held', epoch }`.
  - The heartbeat stops itself before calling `onLost`.
  - From then on `isHeld` is false and `assertHeld()` throws, so `CommitLog.beginCommit` / `markCommitted` /
    `markAborted` and `IdAllocator` refuse.
  - `onLost` is not called for `stopHeartbeat()` or `release()`.
- **`reacquire(): Promise<bigint>`.** Stops the heartbeat, drops the held state and claims a **new** epoch through
  the normal claim protocol. It never resumes the old epoch. While another agent's claim is unexpired it fails
  with `LeaseHeldError`, and a lost race fails with `LeaseLostError('taken-over')`. The old epoch is fenced by the
  next `CommitLog.init()` (`recoverIncomplete`), at the highest seq the log holds for it.

### 6.2 Recovery contract for the store (A)

`clickhouse-store.ts` today sets `this.fatal = error` in `onLost` and keeps running, which wedges the agent
(WP6 §9 item 3). Replace it with:

1. **In `onLost`** (synchronous, no awaits that need the lease):
   - stop the `VisibilityPublisher` timer (`publisher.stop()`);
   - stop accepting new saves;
   - move the store to a `lease-lost` state.
   Never call `renew`, `markCommitted` or `markAborted` for the old epoch; they would throw anyway. Do not
   publish watermarks from the old in-memory state.
2. **In-flight work of the old epoch is dead.** Reject each pending save with the `LeaseLostError`, or keep it to
   re-run after step 4. Do not try to finish it: its rows stay invisible, because recovery aborts every
   non-terminal commit above visible(0) and fences the epoch.
3. **Choose one.**
   - **(a) Exit non-zero** (simplest; Kubernetes restarts the pod and the normal startup recovers). Recommended
     until (b) is tested.
   - **(b) Recover in process.**
     1. `await lease.reacquire()`. Retry with backoff on `LeaseHeldError` or `LeaseLostError`, up to about one
        ttl, then exit. If another agent holds the lease, this agent must not write.
     2. Build a **new** `CommitLog(client, lease)` and `await init()`. That runs `recoverIncomplete(newEpoch)`: it
        aborts open commits, including our old epoch's, and fences every older epoch, including ours.
     3. Re-run the rest of `init()` against the new epoch: `loadFence()`, then a new `IdAllocator` /
        `ClickHouseReservationStore` (reservations are per epoch, so the old ranges are discarded).
     4. Drop every in-memory structure derived from the old epoch: open commits, the operation registry,
        pending-spend and mempool caches, batch lanes. Reload them as at startup.
     5. Build a new `VisibilityPublisher` with `init()`, `start()` and one `publishWatermark()`.
     6. `lease.startHeartbeat(onLost)` again.
     7. Leave `lease-lost` and re-enqueue the saves kept in step 2.
4. **Never** let a `CommitLog` or `IdAllocator` built for the old epoch write again. They check `assertHeld()`,
   but the epoch is also read when a commit begins, so the safe rule is to discard the instances.
5. **Keep synchronous stretches well under the 80 s stall budget** (WP6 §9 item 3, second half). With the new
   heartbeat, a longer stall costs one recovery or exit, never a wedge.

### 6.3 Tests (`writer-lease.spec.ts`)

- **Unit, with a fake clock and a fake store:**
  - a stall past the deadline gives `stalled` exactly once, writes no late heartbeat row, makes `assertHeld`
    throw and stops the heartbeat; `reacquire()` then takes epoch 2;
  - I/O errors are retried until a renewal crosses the deadline, then `deadline`;
  - a higher-epoch claim seen by a renewal gives `taken-over`, after which `reacquire()` is refused
    (`LeaseHeldError`);
  - `CHAINGRAPH_CLICKHOUSE_LEASE_TTL_MS` parsing and the 120 s default.
- **`[e2e]`:**
  - stall → `stalled` → `beginCommit` refused → `reacquire()` racing a second claimant leaves exactly one holder
    with epoch ≥ 2 → a new `CommitLog.init()` fences epoch 1 at the last committed seq. `readSnapshot().fence[0]`
    equals that counter, and the old epoch's committed save stays fully visible;
  - the existing "two (three) simultaneous claimants, exactly one wins" and "stale holder fenced" tests still
    pass.

## 7. Files and commits

- `ddl/050_views.sql` + `ddl/README.md`, `visibility.ts` (+ spec), `checker.ts`, `spec-fixtures.ts`, and the WP6
  poller and keyset scripts (both still accept a pre-WP6b build).
- `scripts/parity/{compare.mjs,lib/canonical.mjs,lib/engines.mjs}` and `parity-harness.md`.
- `writer-lease.ts` (+ spec).
- `scripts/measure/gate-cost.mjs`. New flags: `--legacy-views`, `--void-pad N`, `--void-overflow`. It now saves
  EXPLAIN for every query, and the address query uses the `String` prefix type.
- **Callers outside this work package** that still pass the WP4 arguments and must switch to
  `pinnedView` / `snapshotParams`: `scripts/ingestion-gate/lib/clickhouse.mjs` (C).
