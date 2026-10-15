<!-- cspell:ignore chipnet paryon subquery subqueries analysed analysing arrayJoin PUSD semispace behaviour qlog tsdoc worktree -->

# Fix pass 12: node 39's L27, node 39's median, L35's API side

Local only (2026-10-15): ch1-local ClickHouse 26.8.22.13 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` (container `ch1-fix12-hasura`, port 18085) over `chipnet_pg` for v1. Base: `d3289ed` (fix
pass 11; head `1ebe4f5` with the #106 run log). Input: the fix11 mainnet session (#106, 17 PASS / 2 FAIL [C1 node 39,
L3335], no data discrepancy), its `qlog-*.tsv`, `results/trace/L27-*`, `results/explain/L27-*` and phases. Scratch
databases: `ch1_fix12_chip` (a copy of `chipnet_ch_pp2` in the current DDL, authchain index backfilled, UTXO set built)
and `ch1_fix12_syn` (`synthetic/build.sh … golden`, 150 M transactions as node 1, **plus `ch1_fix12_chip` merged in as
node 2** with transaction / block ids offset: a golden-like store where the chipnet documents read golden-sized tables,
the local stand-in for golden node 39). Raw results stay local (`/tmp/fix12/`). Every timing is with `NODE_ENV=production`.

**No data discrepancy.** The chipnet catalogue is 63 / 63 equal against v1 (48 equal, 15 equal-unordered) at every commit
it ran on, on the chipnet copy and on the golden-like store (in process and through HTTP with admission on); the
synthetic L35 / L33 responses are byte-identical between base and head.

## 1. L27 on node 39 (main item 1)

**Cause: fix 11's candidate chain** (`fce1d3e`). With several output-side candidate sets, each set is read among the
transactions of the sets before it: `… FROM (set) WHERE transaction_hash IN (SELECT transaction_hash FROM (chain))`.
L27's first set is the user's script (`has_output.locking_bytecode`), the second the Paryon minting category. On node 39
the user's script has **8,627** transactions (7,106 on the chipnet copy). ClickHouse analyses that set against output's
base key (`transaction_hash`, granularity 128: 9.07 M marks on golden, a generic exclusion search of up to 69 k steps per
part) **while choosing a projection**, then takes `p_category` (122 marks) anyway: 174 ms of PK filtering (key CPU 176 ms,
statement 42 → 88 ms). Node 1's user has a few transactions, so node 1's L27 stayed at 36 ms. Not the implied conjuncts
and not the page: the page and levels of L27 are unchanged. Node 39's L1 / L29 have the same chain with 714 earlier
candidates (key CPU 24 / 33 ms on mainnet vs 7 / 14 ms in #105).

**Fix** (`36814d2`). A chain link takes the key on the earlier candidates only while they are at most
`chainKeySetMax` = **256**:

- **Normally** the link carries `SETTINGS use_index_for_in_with_subqueries_max_values = 256`: ClickHouse leaves a larger
  set out of key analysis (it still filters the rows) and the read takes its own projection, as fix 10's INTERSECT did.
  No extra read.
- The setting reaches every subquery set below the link, the client's lists included, so when a client list is longer
  than 256 (it would lose its own key read) the link is a **counted gate** instead: `if((SELECT count() FROM (SELECT 1
  FROM (chain) LIMIT 257)) <= 256, transaction_hash IN …, identity(transaction_hash) IN …)`. The scalar folds the `if`
  before key analysis, so the other branch's set is never analysed; it costs one more read of the chain (~5 ms on chipnet).
- 256: key analysis of a hash set on golden's output costs ~20 µs CPU per element (174 ms / 8,627); 256 bounds it at
  ~6 ms. The sets that profit from the key read (mainnet L1: a user's loan keys, a handful of transactions, keeping 7 of
  2.8 M minting outputs) stay far below it.

| L27 (chipnet copy, statement, median of 9) | base | head |
|---|---:|---:|
| statement wall | 46.5 ms | **30 ms** |
| `FilteringMarksWithPrimaryKeyMicroseconds` | 17,175 µs | **85–116 µs** |
| key condition of the category read | `column 0 in 7106-element set` | none (set out of key analysis) |

Same rows (63 / 63 equal). Golden-like store through HTTP: L27 **59 → 35 ms**, L1 106 → 84, L29 70 → 62. Chipnet copy:
L1 72 → 67, L29 61 → 54 (their 714-element sets are now above the limit too).

**Guard tests.** `sql.spec`: an L27-shaped filter's chain link carries the limit; with a client list longer than 256 it
is the counted gate (and no setting); a single set is no chain. `catalogue.spec` (`[e2e] [chipnet]`): L27's candidate
chain on the chipnet copy, with the earlier set checked to exceed the limit, run with and without the limit: the same
candidates, and the limited form's key analysis is under a quarter of the unlimited one's (measured 85 vs 17,175 µs).

Tried and dropped: `identity()` on every link (mainnet L1 would read every PUSD minting output again); the earlier set
as one array scalar (`length(arr) <= n`, `IN arrayJoin(arr)`: the analyzer folds and hashes the constant, 80 / 190 ms);
the counted gate on every link (+5–8 ms per chained document).

**Expected on mainnet:** node 39's L27 back to ~45–55 ms through HTTP (#105 54, fix 11 95), key CPU < 20 ms; node 1's L27
unchanged (36 ms); node 39's L1 / L29 a few ms faster.

## 2. Node 39's catalogue median (main item 2)

Per-document costs of node 39 against #105 and fix 11 (qlog): the median band (L38, L2, X2, L9, L24, L8, L11, L17, L21:
65–82 ms on mainnet) is 90 % statement, 0.05–0.12 CPU-s, 0.1–1.4 M rows, with `QueryPlanOptimize` ~30 ms each (the sets
built for key analysis, the projection candidates). Apart from L27 nothing regressed in fix 11; what is left is fixed
cost per read. On the golden-like store a real-time profile of L24's main thread shows it waiting for scalar subqueries
(~32 %) and analysing them; no single hot spot.

The lever that showed: **threads per small statement.** Every read of a small document is split over the server's
default thread count (14 locally, 48 on golden). Direct, on the golden-like store, the median-band node-39 statements
(5 runs each):

| `max_threads` | L38 | L2 | L24 | L8 | L21 | L17 | L11 | L9 | X3 | L29 | L39 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| default (14) | 48 | 50 | 51 | 55 | 58 | 59 | 56 | 61 | 63 | 63 | 67 |
| 8 | 46 | 48 | 49 | 51 | 56 | 55 | 54 | 56 | 58 | 58 | 61 |
| 4 | 44 | 46 | 48 | 50 | 51 | 53 | 52 | 54 | 56 | 58 | 59 |

with less CPU in every case. But 8 threads for every light request made node 1's data-reading light documents slower
on the synthetic copy (L2 84 → 102, L23 259 → 297, L4 145 → 168, L13 74 → 94 ms), and 4 slowed node 39's scans (X2 +5,
X11 +5, X7 +15 ms; X9, L30, L4 +23–76 ms).

**Fix** (`7926956`, `d438821`). A **small** request (admitted light and predicted at most
`CHAINGRAPH_API_SMALL_MAX_CPU_MS` = 150 CPU-ms) runs with `CHAINGRAPH_API_SMALL_MAX_THREADS` = **8** (0: the server's
default); other light and heavy requests keep their settings. The startup line gains `small<=150 CPU-ms:max_threads 8`.
Mainnet's median band is 0.05–0.12 CPU-s (qlog), node 1's data-reading light documents 0.15–0.5. (`3af0157`, 8 threads
for every light request, is superseded by it.)

Through HTTP, admission on, 2 workers × 1 lane thread, the catalogue on the golden-like store, two interleaved rounds:

| | base `d3289ed` | head | head, 8 threads for every light request |
|---|---:|---:|---:|
| node 39 (chipnet documents) median | 62.1 ms | **58.6 ms** | 58.3 ms |
| node 1 (synthetic documents) median | 67.1 ms | **66.9 ms** | 67.2 ms (12 documents slower) |

Node 39 faster than base by > max(5 ms, 10 %): L27 60 → 35, L1 106 → 84, L4 205 → 183; none slower. Node 1: no
consistent change (L15 / X7 outliers in single runs; L15 through HTTP 1,280 vs 1,339 ms base, X7 ~10 % slower, 350 vs
320 ms). The other changes of this pass (§3) move small documents by < 1 ms.

**Expected on mainnet:** 71.1 × 58.6 / 62.1 ≈ **67 ms** from the local ratio. Golden runs 48 threads per read where the
local server runs 14, so the thread change should save more there (per-read pipeline and thread start-up grow with the
count); **~63–67 ms**. The ≤ 65 target is plausible, not assured. Node 1: unchanged (~75 ms, ≤ 80), L35 faster.

## 3. L35 through HTTP (main item 3)

Where the API side goes (V8 CPU profile of synthetic L35 in a lane-like worker, the ClickHouse result replayed;
`scripts/measure/api-side.mjs`): **GC 26 %**, decoding 24 %, the fast executor's writer 20 %, `JSON.parse` 13 %, the
response body 7 %. The garbage is the 10,000-row page's short-lived arrays, rows and strings; with V8's default young
generation most of it was promoted and collected again by the old-generation GC.

| Commit | Change |
|---|---|
| `6e52083` | **128 MiB semi-space** (`CHAINGRAPH_API_SEMI_SPACE_MB`, default 128; 0: V8's default): cluster workers get `--max-semi-space-size`, heavy-lane threads a 3 × larger young generation (`resourceLimits.maxYoungGenerationSizeMb`). A single process takes it from `NODE_OPTIONS`. In a worker: L35's API side **73 → 62 ms** (64 MiB: no change; 256: as 128) |
| `898aa69` | **Shared values:** `Output.token_category` is `shared`: the final SELECT sends each statement's distinct categories once (`__dict`) and each row their 1-based number (0: NULL); the generated decoder maps them back. With the group-number form only (its outputs are rewritten anyway). L35's categories were ~22 % of its result bytes (a 64-character string per output and outpoint, ~200 distinct values) |
| `898aa69` | **0 for unread elements:** tuple elements the client does not read are sent as `0`, not `null` (L35: ~155 k of them, ~7 % of the result) |
| `898aa69` | **Generated record maker:** a level's scalars are built by one generated object literal (JSON-quoted names, integer positions only) instead of one property store and closure call per field and row; tuples are sorted only when out of order; grouped levels are not copied |

| synthetic L35 | base | head |
|---|---:|---:|
| ClickHouse result | 7.15 MB | **5.30 MB** |
| API side in a worker, replayed (median of 25 × 3 rounds) | 77–78 ms | **62–64 ms** |
| API side through HTTP (headers − last statement end, 2 × 15 runs) | 100 / 103 ms | **84 / 76 ms** |
| statement | 597 / 601 ms | 600 / 590 ms |
| total through HTTP | 731 / 731 ms | **713 / 700 ms** |
| response | 12,917,888 bytes, byte-identical | |

L33 the same way: API side 133 / 134 → 118 / 104 ms, total 646 / 656 → 629 / 612 ms.

**Expected on mainnet:** the API side 341 → **~270–285 ms** (−17 to −20 %), the ClickHouse result 24.2 → ~18 MB (less
transfer to the API), the client's parse unchanged (55 ms: the same 27.8 MB response). **L35 ≈ 0.97–0.99 s** through
HTTP (fix 11 1,050): ≥ 60 ms cut, < 1 s but without much margin. L33 918 → ~880 ms. Statements unchanged (619 / 685 ms).

Tried and dropped: a larger semi-space (256 MiB: no further gain). Not done: streaming the response (the runner's
total ends at its last byte and its parse, so it would save at most the 26 ms transfer, and the lane hands over whole
bodies); shared values for commitments (mostly distinct). Left: `JSON.parse` of the ClickHouse result (RowBinary for the levels); L35's statement
(625 ms of key CPU on mainnet, most of it the outpoint level's 50 k-tuple key set, needed for its key read).

## 4. Kit nit (warm-up log)

`ch1-api-run.sh warmup` (ch1-fix12) waits, bounded at 300 s, until no pod of either Deployment has a
`deletionTimestamp`, then records only pods without one (#106 recorded the terminating old pods' `0 of 0 entries`
lines). In the GKE kit, not here.

## 5. Settings tried that changed nothing

`use_query_condition_cache = 0` (the "Query condition cache has dropped 0/1133846 granules" lines on golden): within noise
on the synthetic store once warm (the first, cold comparison looked 5–15 ms better). The block read by id behind every
inclusion level: ~4 ms on the golden-like store (782 k blocks), not worth p_id's side effects (fix-pass-8b.md §2).

## 6. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 17 tsdoc warnings; prettier: `ddl/README.md` as before) |
| unit (`ava --match='!*[e2e]*'`) | 203 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | **183 passed** (fix 11: 181 and `g1-fix`'s two lease-deadline flakes; none this time) |
| API specs `build/api/*.spec.js` incl. the chipnet catalogue spec (`ch1_fix12_chip`, v1 Hasura) | **99 passed** (fix 11: 97 + the two L27 guard tests) |
| chipnet catalogue (`catalogue-run --latency 9`) | **63 / 63 equal** (48 + 15), median 47.3 → 47.5 ms (base / head, in process, admission off: §2's changes need admission) |
| golden-like catalogue through HTTP, admission on (kit `ch1-api-catalogue.mjs`) | **63 / 63 equal** every run; node-1 synthetic v2-only, no error but X11 (synthetic data, as before) |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed each (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed (123 ClickHouse-only skipped) |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`.

## 7. Design calls

| Call | Rationale |
|---|---|
| Limit a chain link's key set at 256 by the subquery setting, counted gate only with a long client list | ClickHouse decides on the set's real size for free; the gate costs a read and is needed only where the setting would also hide a client list |
| 256, not 1,000 | ~20 µs CPU per element on golden: 256 bounds the analysis at ~6 ms; the sets the key read helps are a handful |
| Above the limit, the read takes its projection (fix 10's behaviour) | never worse than the INTERSECT that #105 measured (L27 54 ms) |
| 8 threads for small requests (≤ 150 CPU-ms), not every light one | the median band gains 2–8 ms; light documents reading millions of rows lose up to 30 % with 8 |
| 8, not 4 | 4 gains 2–5 ms more on the median band but slows node 39's scans (X2, X11, X7) |
| Semi-space 128 MiB by default, in code | the API's allocation pattern (tens of MB per heavy page) is the code's; a 3 × 128 MiB young generation per isolate is small next to the pod's 12 GiB limit |
| Shared values for token categories only, with group numbers only | few distinct values over many rows; commitments and hashes are mostly distinct; the group-number form rewrites the outputs anyway |
| Unread elements as 0 | positions stay put, nothing reads them, 1 byte instead of 4 |
| A golden-like local store (synthetic + chipnet as a second node) | node 39's documents on golden-sized tables: the chipnet copy alone missed the per-read costs that scale with table size |

## 8. What the next GKE session should verify

1. **L27 node 39** through HTTP ≤ 60 ms (fix 11 95, #105 54), its key CPU < 20 ms; node 1's L27 ~36 ms.
2. **C1 node 39** median ≤ 65 ms (fix 11 71.1; expected ~63–67); node 1 ≤ 80 (fix 11 75.7; expected unchanged).
   Per document vs #106; equality as before (X11 / X6 known).
3. **L35 < 1 s** through HTTP (fix 11 1,050; expected ~0.97–0.99 s): API side vs #106's 341 ms (expected ~270–285), the
   ClickHouse result vs 24.2 MB (expected ~18), statements vs 619 ms (unchanged); L33 vs 918 ms.
4. **Small requests:** every admitting pod's startup line has `small<=150 CPU-ms:max_threads 8`; light-band `query_log`
   `Settings['max_threads']` = 8 for small statements.
5. **Regressions:** A1 / A3 admission arms (light p95), X6 / X5 rows, U5 whole-set checks, every spot scope, S39
   (7.8 s in #106), the soak's CPU per request.
6. **Kit:** `warmup` records only the live pods.

## 9. Commits

| Commit | |
|---|---|
| `36814d2` | perf(api): a candidate-chain link takes the key on at most 256 earlier candidates |
| `898aa69` | perf(api): smaller level results and cheaper decoding |
| `6e52083` | perf(api): 128 MiB semi-space for cluster workers and lane threads |
| `3af0157` | perf(api): light statements run with max_threads 8 by default (superseded by `7926956`) |
| `7926956` | perf(api): max_threads 8 for small requests only, light ones keep the default |
| `d438821` | style(api): admission option keys in order |

## 10. Housekeeping

`ch1_fix12_chip`, `ch1_fix12_syn`, the Hasura container `ch1-fix12-hasura` and its metadata database
`ch1_fix12_hasura_meta`, and the worktree `/tmp/fix12/wt-base` removed; every API process started for the measurements
stopped (by PID).
