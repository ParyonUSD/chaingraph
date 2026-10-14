# Fix pass 9: X6's scan, admission on an idle server, the heavy lane, API-side CPU, check chunks, deployment

Local only (2026-10-14): ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` (container `ch1-fix9-hasura`, port 18082) over `chipnet_pg` for v1. Base: `d30e8be`
(fix 8 + 8b, merge `e81d427`). Inputs: the #104 decision document
(`paryon_kubernetes-consolidation/docs/chaingraph/decisions/2026-10-14_clickhouse-fix8-verification.md`),
[fix8-run-log.md](fix8-run-log.md), [fix-pass-8.md](fix-pass-8.md), [fix-pass-8b.md](fix-pass-8b.md) and the raw
`2026-10-14-fix8-*` results. Scratch: `ch1_fix9_syn` (synthetic, mainnet scale, golden layout:
`synthetic/build.sh … golden` + the stored UTXO set; a second node added for §6), `ch1_fix9_chip` (a copy of
`chipnet_ch_pp2` in the current DDL, authchain index backfilled, UTXO set built). Raw results stay local
(`chaingraph-performance/2026-10-14-fix9-local/`).

**No data discrepancy.** The chipnet catalogue is 63 / 63 equal against v1; X6's and the 10,000-row pages' responses
are byte-identical before and after; the fast executor is checked against Yoga's executor on every API spec (§4).

## 1. X6 read the whole `input` table (top priority)

**Cause.** 8b reads a page's inputs by `transaction_internal_id IN (…)` alone when every part has `input.p_txid`.
`p_txid` is slim: it has no `unlocking_bytecode`. X6 (GetTransactionDetails) selects `unlocking_bytecode`, so ClickHouse
could not use the projection. The base table is ordered by transaction hash, and with no hash condition the id-only read
scanned every row: 1.04 B rows on golden (`evidence-x6-statement.txt`: `__l16` reads `input` by id alone, and
`input.p_txid` is absent from the projections). The `system.parts` feature check was correct (SF: every part has it).

**Fix** (`da696b3`). The inputs level reads by id alone only when every column it needs is in `p_txid`
(`inputsByIdCovers`). Otherwise it reads by hash and id, so the table's own key bounds the read, as in fix 7. The columns
a filtered level needs were "every column"; that would have sent L2 / L3 / L14 / L17 / L22 / L30 back to hash and id.
They are now the columns an `InputFilter` can read (`filterColumns`: the input's own key columns and sub-filters; no
filter reads `unlocking_bytecode`).

**Test** (`cb56cfe`, `api-inputs-projection.spec.ts`). `input` gets 400,000 filler rows in 4 parts. Each part's id range
spans the dataset's ids (0 and above 10⁹), as golden's outpoint-chunked parts do. Without that, ClickHouse 26's
per-part column statistics prune whole parts by `transaction_internal_id`, and a scan stays invisible. Seven shapes run:
key columns only, with outpoint (L33 / L35), filtered by `input_index` (L14 / L17), filtered by outpoint (L22 / L30),
the `inputs` root with nested inputs (L17), `unlocking_bytecode`, and X6 itself. Every statement must read fewer than
100,000 rows. Every statement that reads by id alone must list `input.p_txid` in `system.query_log.projections`.
Against the 8b behaviour the X6 shapes fail with 400,070 rows read.

| X6 on `ch1_fix9_syn` (in process, median of 3) | 8b (`d30e8be`) | this pass |
|---|---:|---:|
| rows read | 375,730,055 | **1,455,208** |
| statement | 1,363 ms | **703 ms** |
| CPU | 4.3 s | 0.75 s |
| response | 11,402 B | identical |

## 2. Admission: debt and contended throttling on an idle server

**Causes.**
- **Double charge.** `recharge` gave a ticket's charge back with `adjust` (capped at the share's capacity) and then took
  the actual cost. On a share that had refilled while the request ran, the give-back was lost to the cap, so a
  sequential heavy request was charged twice: at `finish`, and again when the `query_log` measurement reconciled it.
  Back-to-back L15s (61 CPU-s) drained the heavy share.
- **Contention from debt alone.** `contended` fired whenever the heavy share was below half. One heavy document repeated
  on an idle server therefore ran at `max_threads 4` (mainnet: 1.65 → 13.6 s).
- **Debt outlives the load.** The leftover debt refused L17 on an idle server.

**Fix** (`ac5e228`).
- **One net adjustment.** `recharge` applies one net adjustment when the share is unchanged.
- **Idle never refuses.** An idle process admits whatever the levels and forgives any debt. Idle means no request in
  flight and the loop below `loopGuard`; live re-runs run outside requests.
- **Contended means concurrent.** A heavy request is contended only with another heavy request in flight, and then only
  when the share is below `contendedFill` or half the in-flight cap is taken. Past debt alone never throttles.

**Tests.**
- 20 L15s back to back with mainnet's default budgets (48 cores) and the ledger reconciling two seconds late: every run
  gets the server's threads, none is refused, and L17 runs at full threads afterwards.
- A forced −500 CPU-s debt is forgiven on an idle server and still refuses while a request is in flight.
- A reconciled charge is one net adjustment.
- Against the old `admission.ts`, the L15 test is throttled from run 1 and the net-charge test charges twice.

## 3. Refusal latency and light p95: the HTTP front path and the heavy lane

**Diagnosis.** One worker's loop was the mainnet limit. Refusals were decided in Yoga's `onExecute`, after the request
had queued in that loop. Light documents waited behind L33 / L35's 0.3–0.6 s of decoding, GraphQL execution and
serialisation. Deciding earlier alone does not help: the request still waits for the loop before its handler runs. Run
locally, the front path without the lane gives refusal p95 211 ms and light p95 563 ms, the same as fix 8.

**Fix.**
- **Front path** (`front.ts`, `d91c956`). A POSTed query is decided before Yoga sees it. The handler reads the body,
  parses the document (an LRU cache by text) and computes the plan keys and nodes.
  - **Cache hit:** the stored body is written at once, with no admission.
  - **Refusal:** OVERLOADED, 503 and Retry-After at once, rendered as Yoga renders it.
  - **Admitted light request:** Yoga on the main thread, with the ticket.
  - **Admitted heavy request:** the heavy lane.
  - **Anything else** (GET, SSE subscriptions, mutations, invalid bodies, Accept types the regular processor does not
    serve) goes to Yoga as before. That path is streamed and aborts with the client.
- **Heavy lane** (`heavy-lane.ts`, `heavy-worker.ts`).
  - Worker threads, each with its own API instance (Yoga, ClickHouse client; admission only records).
  - A worker answers with the response bytes (transferred, not copied), the statements it ran (query ids and rows, for
    the CPU ledger), its loop time (learnt by the cost model) and the snapshot marks (for the main thread's response
    cache, which now stores bodies, `a6da60d`).
  - The client going away cancels the request on the worker.
  - `CHAINGRAPH_API_HEAVY_THREADS` (default 1, 0 = none; off when admission is off: the predicted class routes).
  - With lane threads, the loop budget's light share is the main loop and its heavy share is the lane threads.
  - The lane's ClickHouse sockets come out of the process's 64 (`c1f8073`), so the 72-per-process backstop holds.
- **Tests** (`api-heavy-lane.spec.ts`).
  - A heavy request answered on the lane is byte-identical to the main thread's answer, its statements reach the
    ledger and the model learns its loop time.
  - The lane's answer is stored, and the next request is a cache hit without admission.
  - Light requests stay on the main thread.
  - The `application/graphql-response+json` media type is kept.
  - A refusal is answered in under 250 ms with nothing sent.
  - Invalid documents get Yoga's own errors.

**Measured locally (loop-bound, as mainnet).** Light chipnet catalogue documents (L13 L20 L25 L40 L43 L46 X4 X8) plus
10,000-row L35-shape pages, which are API-heavy (~90 ms of API CPU) and cheap in ClickHouse, on `ch1_fix9_chip`. Open
loop for 40 s, cache off, admission on, one worker. Times in ms.

| light + heavy QPS | build | main loop busy | light p95 | refusals (p95) | heavy ok, p95 |
|---|---|---:|---:|---:|---:|
| 20 + 6 | fix 8 | 81 % | 335 | 0 | 240, 938 |
| 20 + 6 | **fix 9** | 5 % | **95** | 0 | 240, 708 |
| 30 + 10 | fix 8 | 83 % | 551 | 157 (181) | 244, 1,502 |
| 30 + 10 | **fix 9** | 4 % | **138** | 126 (**2**) | 275, 1,237 |
| 30 + 14 | fix 8 | 90 % | 479 | 299 (125) | 262, 1,378 |
| 30 + 14 | **fix 9** | 3 % | **156** | 323 (**1**) | 237, 1,489 |

**A 4-CPU pod.** The API runs in a container with `--cpus=4` (`node:24.14.1-bookworm-slim` over the mounted builds) at
30 + 10 QPS, the overload shape:

| build | workers | lane threads | light p95 | refusal p95 | heavy ok / 400 | heavy p95 |
|---|---:|---:|---:|---:|---:|---:|
| fix 8 | 1 | — | 516 | 139 | 210 | 1,190 |
| fix 8 | 3 | — | 256 (7 light refused) | 141–151 | 241 | 961 |
| fix 9 | 1 | 0 | 563 | 211 | 235 | 1,368 |
| fix 9 | 1 | 1 | 141 | 4 | 227 | 1,657 |
| fix 9 | 1 | 3 | 141 | 4 | 278 | 1,089 |
| fix 9 | 2 | 1 | 155 | 3 | 275 | 1,215 |
| **fix 9** | **3** | **1** | **129** | **4** | 250 | **950** |

The synthetic soak and overload B shapes (`scripts/measure/admission-load.mjs`, one worker) are not loop-bound locally:
the API process uses ~0.3 cores and ClickHouse is the limit. Even so, overload B light p95 goes 375 → 162 ms and heavy
p95 2,008 → 1,048 ms, with no errors in either build. Soak p95 at 3 / 6 / 9 QPS goes 939 / 889 / 889 → 899 / 873 /
864 ms.

## 4. L33 / L35 API-side CPU

Where the time went, from a CPU profile of a 10,000-row L35 page with the ClickHouse result replayed from memory
(`scripts/measure/api-side.mjs`):

| share | |
|---:|---|
| ~22 % | Yoga's executor (graphql-js algorithm, @graphql-tools) |
| 14 % | `JSON.stringify` |
| 12 % | GC |
| ~27 % | level decoding |
| 10 % | `JSON.parse` |

Changes:
- **Fast executor** (`fast-execute.ts`, `202e5a6`).
  - Query responses are written as JSON directly, in pieces joined once. It uses graphql-js's own field collection
    (fragments, `@skip` / `@include`), argument coercion and resolve info.
  - One-parameter resolvers, which can read only their parent, are called with the parent alone. The relationship
    resolvers (registered, `manyResolve` / `oneResolve` / `optionalResolve` in schema.ts) get the parent and the
    response key. Neither builds a resolve info per row and field.
  - A promise below a root, an error, a non-null violation or an abstract type gives up: Yoga's executor answers. Root
    values are memoised, so no statement runs twice and errors are reported exactly as before.
  - `CHAINGRAPH_API_FAST_EXECUTION=on|off|verify`. Verify runs both and fails the request on any difference.
  - **Design call:** root fields come in document order, which is the GraphQL spec's order. Yoga's executor
    (`@graphql-tools/executor` `promiseForObject`) ordered async root fields by completion, so the order varied with
    timing. Verify compares without object key order.
  - **Not graphql-jit:** several of its versions, including republished old version lines, were published on
    2026-10-12 and 2026-10-13.
- **Level keys** (`94d0bf7`). Key texts are `lower(hex(col))`, not `hex(toString(col))`.
  - Exact: keys are hashes or unsigned ids, and the text is equal across integer widths.
  - The key is the same element as a selected hash's, so it is sent once.
  - **Latent inexactness removed:** `toString(FixedString)` trimmed trailing zero bytes, so the all-zero outpoint hash
    keyed as `''`.
  - Page level 1.60 → 0.93 MB.
- **Decoding** (`03332b9`). The child level's groups are looked up once per result, not per parent row, and two-column
  keys are built without copies.

| | before | after |
|---|---:|---:|
| chipnet 10,000-row L35 shape, API-side CPU per request (replayed; median of 25, two interleaved rounds) | 92 / 93 ms | 82 / 89 ms with the executor, **77–80 ms** with the decoding changes |
| synthetic L33 in process (HTTP-equivalent total; statement 552 / 539 ms) | 855 ms | **755 ms** (API side ~303 → ~216 ms) |
| synthetic L35 in process (statement 758 / 697 ms) | 936 ms | **863 ms** |

**Not met as asked.** This is −15 % locally, not the ~2× L35 needs: mainnet's 581 ms of API side would have to fall
under ~300 ms. The lane takes the work off the loop, but the latency stays. What remains is decoding into rows (~38 % now)
and `JSON.parse` (~11 %): the transport. Next options, measured but not built:
- **Integer group ids instead of hash keys.** `transform(key, arrayDistinct(parent keys), range(…))` over the level's
  constant parent array. Child tuples would carry a small integer instead of a 64-character key: ~35 % fewer result
  bytes, and array indexing instead of string-hash `Map`s.
- **RowBinary for the levels.**

## 5. L17, L6, node-1 X4

None of the three reproduces on the synthetic data: L17 key CPU 1 ms, L6 23 ms, X4 466,843 rows, with fix 8 and this
pass equal. Mainnet's key CPU comes from golden's part layout (15 parts of `input`, outpoint-chunked; 2 block-ordered
parts of `block_transaction`), which the synthetic copy only approximates. Not changed. GKE probes are listed in §8.

## 6. Spot check and whole-set check

- **Node 1 memory** (`b24c715`). Memory per check statement follows the outputs a chunk reads, not the UTXOs it keeps.
  - Chunks are now the larger of the UTXO-based count (1 M per chunk) and ~8 M created outputs per chunk (fix 7's
    sizing). Created outputs are estimated as the node's accepted transactions times the database's outputs per
    transaction.
  - Mainnet node 1 (~1.16 B outputs) gets **256 chunks** again, as in fix 7 (2.6 GiB, 946 s; fix 8 had 128 chunks,
    5.2 GiB, 1,027 s). Small nodes are unchanged.
  - Synthetic node 1, one chunk: 128 chunks → 2.04 GiB and 5.0 s; 256 chunks → **0.94 GiB** and 2.3 s, about the same
    total.
- **Node 39 time: not fixed.** A synthetic second node accepting the last 3,030 blocks (999,720 transactions, 2,858,741
  UTXOs) shows the cause. Its totals reference reads ~0.9 B rows whatever the chunking: 1 / 2 / 4 / 16 chunks take
  18.4 / 16.2 / 19.4 / 24.4 s. Two choices drive that, both the optimiser comparing mark counts across granularities
  (128 vs 1024):
  - The outputs read uses `output.p_script` with no key condition (210,568 of 210,568 granules, 216 M rows). The base
    table would read 57 M rows.
  - The spenders read uses `input`'s base table with its bloom filter (236 M rows). `p_outpoint` is chosen only for
    narrow hash ranges (node 1's 256 chunks do use it).
  - Tried without gain:
    - a subquery `SETTINGS optimize_use_projections = 0` (ignored);
    - virtual columns to exclude the projection;
    - `preferred_optimize_projection_name`;
    - a set-first form (16.5 s).
  - Statement-level `optimize_use_projections = 0` is slower (9.1 s per half-range chunk vs 6.6 s).
  - A small-node formulation (driving both reads from the node's own acceptance by key) is the next step.

## 7. Deployment default

- **Kit** (`paryon_kubernetes-consolidation` `bcf7553`). The deployment is the kit: the fork's chart has no v2 API.
  `experiments/chaingraph/ch1-fix8` now deploys `CHAINGRAPH_API_WORKERS=3` and `CHAINGRAPH_API_HEAVY_THREADS=1` per
  4-CPU pod (both Deployments). Its `on` / `off` admission arms set one worker explicitly. The backstop renders
  6 processes: 6 × 72 + 10 = **442**.
- **Why 3 × 1:**
  - #104: one worker timed out 227 requests at 30 QPS, three carried it.
  - In the 4-CPU table (§3), 3 workers with one lane thread give the best light p95 (129 ms) and heavy p95 (950 ms).
  - 1 × 3 serves slightly more heavy requests but puts every light request on one loop. That loop is the resource that
    failed on mainnet.
- **Code defaults stay `CHAINGRAPH_API_WORKERS=1`, `CHAINGRAPH_API_HEAVY_THREADS=1`.** The worker count is a property
  of the pod (its CPUs, the backstop it is rendered against), not of the code. One lane thread is right for any
  process size.

## 8. Suites (final code, `c5b3737`)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: 17 tsdoc warnings, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 199 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | 183 passed |
| API specs `build/api/*.spec.js` incl. the chipnet spec (`ch1_fix9_chip`, v1 Hasura) | 92 passed |
| API specs with `CHAINGRAPH_API_FAST_EXECUTION=verify` | no answer differs; the 2 statement-count tests (S6, fix pass 3 cache) fail by design (verify runs every query twice) |
| chipnet catalogue (`catalogue-run --latency 9`, v1 Hasura) | **63 / 63 equal** (48 equal, 15 equal-unordered as before), v2 p50 median 56.1 ms |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed each (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed, 1 skipped |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`. One API run under host
load had the fix-pass-3 snapshot-cache test fail on timing; it passes alone and in the final run.

## 9. What the next GKE session should verify

1. **X6** on both nodes: rows read back near fix 7's 5.4 M; X6 ≈ 0.7 / 0.9 s.
   - Every statement whose inputs level reads by id alone lists `input.p_txid` (C1 qlog).
   - X6's inputs level reads by hash and id.
2. **Sequential catalogue, admission on:**
   - L15 through HTTP ≈ 1.7 s every run;
   - `stats.contended` stays 0;
   - no refusal (L17 runs).
3. **A1 / A3 with the deployment default** (3 workers × 1 lane thread): refusal p95 < 250 ms, light p95 < 500 ms.
   - Startup line ends `workers=3 heavy_threads=1`.
   - Backstop 442.
   - `GET /admission` `loopUtilisation` of the main loops stays low under overload.
   - Also one worker × 1 thread for comparison with #104.
4. **L33 / L35 phases** (as in #104): the API side vs 311 / 581 ms.
   - Expected about −15–30 %; under 1 s through HTTP is not expected for L35.
   - If the API side is still > 300 ms, build the integer group ids of §4.
5. **L17 / L6 / X4 probes** on golden, per level: `EXPLAIN indexes = 1` and the qlog key CPU.
   - L17: which level's IN set is analysed against which parts.
   - L6: the block_window group.
   - Node-1 X4: the block level by id, a full scan of `block` as noted in 8b §5.
6. **U5 / S39:**
   - node 1: 256 chunks, peak ≤ ~2.6 GiB per statement;
   - node 39: totals spot check time, plus `system.query_log.projections` of its statements (expect `output.p_script`
     unless §6's small-node form is built).

## 10. Design calls

| Call | Rationale |
|---|---|
| Inputs by hash and id when the level needs a column outside `p_txid` | the base table's key bounds the read (fix 7's plan); adding `unlocking_bytecode` to `p_txid` would double its size for one document |
| `filterColumns` for the inputs relation | an `InputFilter` cannot read `unlocking_bytecode`; "every column" would undo 8b for every filtered level |
| Idle = no request in flight and the loop below the guard | live re-runs load the loop outside requests |
| Forgive debt on idle; contended needs another heavy request in flight | debt rations concurrent load only; a sequential client must never be throttled |
| Decide before Yoga, but move heavy work to threads | deciding earlier alone left refusals at 211 ms locally; the wait is the loop, not Yoga |
| Lane threads in the process, not routing between processes | `node:cluster` cannot route by request content; threads keep one admission gate per process |
| Cache stores bodies (bytes) | lane answers come back serialised; a hit no longer stringifies per request |
| No graphql-jit | recently (re)published versions; an in-house executor checked against Yoga's on every spec instead |
| Root fields in document order | the spec's order and deterministic; Yoga's was completion order |
| Keys as `lower(hex(col))` | exact (the old text trimmed zero bytes), shorter for ids, shared with hash fields |
| Chunks by max(UTXOs, created outputs) | memory follows the outputs read; restores node 1's 2.6 GiB without changing small nodes |
| 3 workers × 1 lane thread per 4-CPU pod; code defaults 1 / 1 | §7 |

## 11. Commits

| Commit | |
|---|---|
| `da696b3` | fix(api): inputs read by id alone only when input.p_txid covers the level's columns |
| `cb56cfe` | test(api): inputs read by id alone use input.p_txid; no statement scans input |
| `ac5e228` | fix(api): admission never refuses or throttles an idle server; one net charge on reconcile |
| `a6da60d` | feat(api): response cache serves and stores serialised bodies; request scope flags |
| `d91c956` | feat(api): decide POSTed queries before Yoga; heavy requests on worker threads |
| `71b5ea9` | test(api): front path and heavy lane |
| `94d0bf7` | perf(api): level keys as lower-case hex of the column |
| `03332b9` | perf(api): level decoding: groups once per result, two-column keys without copies |
| `202e5a6` | perf(api): query responses written as JSON directly (fast executor) |
| `0c20fd2` | test(api): fast executor equals graphql-js (verify mode) |
| `b24c715` | perf(store): whole-set check chunks also sized by the node's created outputs |
| `c1f8073` | fix(api): the heavy lane's sockets come out of the process's 64 |
| `c5b3737` | style(api): spelling |
| kit `bcf7553` | ch1 kit deploys three API workers with one heavy-lane thread per 4-CPU pod |

Housekeeping: `ch1_fix9_syn`, `ch1_fix9_chip`, `ch1_fix9_probe`, the Hasura container `ch1-fix9-hasura` and its
metadata database `ch1_fix9_hasura_meta`, the baseline worktree `/tmp/fix9/wt-base` removed; the `node:24.14.1-bookworm-slim`
image (used for the 4-CPU runs) kept.
