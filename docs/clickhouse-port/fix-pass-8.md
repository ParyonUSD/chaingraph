<!-- cspell:ignore chipnet paryon backfill EWMA cgapi OVERLOADED qlog utilisation mispredicted unticketed uncontended worktree serialisation serialises misprediction mispredictions prio tsdoc -->

# Fix pass 8: request-level admission by CPU; spot-check chunks per node

Local only: ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs), ch1-pg Postgres 14. Evidence: the mainnet session of
2026-10-13 ([fix7-run-log.md](fix7-run-log.md); raw results `chaingraph-performance/2026-10-13-fix7-api/`, read
only). Scratch databases `ch1_fix8_syn` (synthetic, mainnet scale, golden layout: `synthetic/build.sh … golden` +
the stored UTXO set, 242,512,925 UTXOs), `ch1_fix8_chip` (chipnet copy); all dropped afterwards. Raw results stay
local (`/tmp/fix8/`, not committed).

## 0. Summary

| # | Session finding (fix7) | Cause (this pass) | Fix | Local result |
|---|---|---|---|---|
| 1 | Admission on worse than off at 20 QPS (56 OVERLOADED, p95 18.9 s vs 16.9 s) | the binding resource was the API pod's single event loop, not ClickHouse (≤ 15 of 48 cores busy, §1); per-statement lanes metered neither | request-level admission: predicted ClickHouse CPU **and** event-loop time against two token budgets with a light reserve (§2); optional worker processes | §4: at the loads the server carries, on = off; beyond, fast refusals and no timeouts |
| 2 | Refusals took seconds (p50 3.2 s, p95 18.5 s at 30 QPS) | refused per statement, after earlier statements ran; and every response, refusals included, queued behind a saturated loop | one decision per request, before any statement (< 1 ms, refusal p50 1 ms); the loop budget and loop guard keep the process from saturating | §4: refusal p50 1 ms, p95 117–278 ms (the loop's longest synchronous stretch, ~220 ms per L35 page) |
| 3 | Overload B: 181 client timeouts, 1 OVERLOADED | heavy lane never filled (statement count, not CPU) | CPU budget; heavy in-flight cap; contended heavy requests get ~2 cores of threads per slot | §4 A3: 0 timeouts (off 26–33), light p95 566 ms / 471 ms with 2 workers (off 8.6 s / 4.1 s) |
| 4 | Node 39's spot-check totals 9 s → 322 s | chunk count from the database's 1.16 B outputs (256) for a 1.2 M-UTXO node, and every chunk read the chain's inputs in its range | chunks per node from its stored UTXO count (1 M per chunk); a chunk reads only spenders of outpoints the node created | §5: a 1 M-transaction node in the 150 M-transaction synthetic database 92.4 s → 18.9 s, equal |

**No data discrepancy was found.** Every equality check of this pass is equal: chipnet catalogue 63/63, the
`checkUtxoSet` / spot-check specs (incl. the new per-node chunk spec), the four spot-check totals runs of §5 (same
3,505 categories, 2,856,907 outputs, digest `a480d0dacb4ff982` from every code path).

**Flags.**
- **The mainnet failure mode is not reproduced locally.** Here ClickHouse (14 cores) saturates before the API's loop
  (the API process uses at most 0.74 of a core in any run): the local runs exercise the CPU budget, the loop budget only in unit tests.
  The fix7 code (statement lanes, `ea2a20a`) does about as well as this pass on the local A3 (§4); its mainnet
  failure was the loop. GKE must show the loop-bound regime (§7).
- **At ~95 % CPU (A1 18 QPS here) admission refuses a little** (1 of 1,080; 6 with 2 workers) where off refused none,
  and its p95 is 2.6 s vs 2.3 s: the CPU target is 90 %. `CHAINGRAPH_API_CPU_TARGET=1` removes it, at the price of
  less headroom for light requests under overload.
- **Refusal p95 is ~250 ms in overload B, not < 250 ms** (251 / 278 ms): the decision takes < 1 ms, but a refusal
  is written by the same event loop that runs L35's ~220 ms synchronous stretches (relation assembly, GraphQL
  execution, JSON serialisation of a 12.9 MB page). Workers spread it (A1 24 QPS: 208 → 117 ms); shortening those
  stretches is compile.ts work (another branch is changing that file), recommended next.

## 1. What the mainnet session shows (fix7, 2026-10-13)

Read from `k8s/query_log-api_ro.tsv` (every API statement, with its CPU) and the soak / overload JSONs:

| Load (cache off, one API pod) | ClickHouse CPU of the API's statements | statement p50 / p95 / p99 | request p95 | light documents' request p95 |
|---|---:|---:|---:|---:|
| A1 10 QPS, admission off | 6.2 cores of 48 | 8 / 90 / 1,172 ms | 1,974 ms | L12 310, L43 1,390 ms |
| A1 20 QPS, admission off | **14.1 cores of 48** | 8 / 150 / 5,419 ms | 16,934 ms | **L12 14,848, L43 13,433 ms** |
| A1 30 QPS, admission off | 15.0 cores of 48 | 5 / 147 / 7,542 ms | 28,206 ms (381 timeouts) | all ~28 s |
| overload B, admission on | **9.1 cores of 48** | 7 / 2,610 / 7,442 ms | 24,762 ms | L13 23,189 ms |

(Statements finished in each level's minute, both data statements and snapshot reads; CPU = User + System time.)

- **ClickHouse was not the bottleneck.** At 20 QPS the server spent at most ~15 of its 48 cores on the API; light
  documents whose statements took ~10 ms waited **15 s**, uniformly across every document. A uniform wait while the
  database is two-thirds idle is a queue in front of it: the API pod.
- **The API pod's single event loop was.** One Node process (`replicas: 1`, no workers) decodes every ClickHouse
  result, executes GraphQL and serialises the response on one thread. L35 (10,000 rows) took 1,762 ms over HTTP
  against 962 ms direct (catalogue): ~0.8 s of the loop per L35. At 20 QPS the soak sends 1.67 L35/s (≥ 1.3 s of
  loop work per second, plus everything else): the loop saturates, and every request — light or heavy, admitted or
  refused — waits behind it. At 10 QPS that is roughly two-thirds of the loop (an estimate: the session did not record the pod's CPU), and
  the p95 is already 2 s.
- **Hence the refusal latency.** OVERLOADED p50 3.2 s at 20 QPS is not only "refused after earlier statements": a
  refusal is an HTTP response like any other, written by the saturated loop. A gate that only meters ClickHouse
  cannot keep refusals fast when the API process itself is the scarce resource.
- **Overload B: admission hardly refused** (1 OVERLOADED, 181 client timeouts): the heavy lane peaked at 20 of 24
  statements because the statements finished slowly behind the loop, not because the server was full; the run log's
  "the server oversubscribes its CPU" is not borne out by query_log (9.1 cores of 48 during the run).

So the redesign meters two resources — ClickHouse CPU and the API's event loop — and decides once per request.

## 2. Design (`src/api/admission.ts`, `server.ts`, `db.ts`)

**Request level.** A Yoga plugin admits or refuses every *query* operation in `onExecute`, after the response cache
(a hit costs nothing and is not admitted) and before the request snapshot or any statement. Refused: OVERLOADED, HTTP
503, `Retry-After` (the time the budget needs to repay its debt, at least 1 s), `extensions.reason` (`clickhouse`,
`loop`, `in_flight`, `loop_guard`). Admitted: the execution runs in a request scope carrying the **ticket**; every
statement records itself on it (query_id, rows, shape), with the class's ClickHouse settings. **Statements are never
gated**: an admitted request is never refused part-way (e2e: a request admitted, its budget then spent to −50
CPU-s, still runs all its statements). Mutations and subscriptions are not admitted; live re-runs and other
statements outside a request are charged, never refused.

**Predicted cost.** Per request, two numbers: ClickHouse CPU-seconds and event-loop milliseconds, an EWMA (α 0.3)
per **plan key**: document hash (sha256 of the printed document, cached per parsed document) + operation name +
pinned nodes + the *magnitude* of every numeric variable and list length (log2 buckets: a `limit` of 10 and 10,000
are different plans, two lists of 2 hashes the same plan; strings ignored). Fallbacks: the document key, then the
operation's root fields, then a default (250 CPU-ms, 20 loop-ms). A request with no history at all is heavy.

**Measured cost, reconciled.**
- *ClickHouse CPU* from `system.query_log` (`ProfileEvents` User + System time), polled every 2 s for the API's own
  query_ids (the log flushes every 7.5 s). Until a request's statements are measured its charge stands; when they
  are, the budget is reconciled with the sum and the model learns it. Without the grant (an old API user profile)
  the ledger stops and CPU is estimated from rows read × a CPU per row (learned per statement shape from the ledger
  while it ran; default 0.3 µs). The API user (`api-user.ts`) now gets `SELECT ON system.query_log`; XML users have
  every grant already.
- *Loop time* from `performance.eventLoopUtilization`, attributed at every transition (admitted, statement sent,
  response started, finished) and every 100 ms to the requests not waiting for ClickHouse, weighted by their
  predicted loop time (a light request queued behind L35's decoding is not charged for it; the sum is exact). The
  ClickHouse client reports when a response starts (`onResponse`), so decoding counts as loop time.
- A request that turns out heavier than its class is charged to the heavy share (a mispredicted light request must
  not put the light reserve into debt).

**Budgets.** Per resource a token bucket split into a light and a heavy share; each share refills at its part of the
rate up to its part of the capacity, and what a full share would gain **overflows into the other** (heavy requests
use the whole budget when light ones are idle; light ones borrow idle heavy tokens). A request is admitted while its
share is positive and charged its prediction (debt allowed: one request bigger than the burst still runs; the debt
refuses the next ones until repaid).

| | rate (per process) | burst | light share |
|---|---|---|---|
| ClickHouse CPU | server cores × 0.9 / API processes (43.2 CPU-s/s at 48 cores) | 4 s **of the server's rate**, whatever the processes | 25 % |
| event loop | 850 ms/s | 1 s | 25 % |

**Classes.** Light = predicted ≤ 500 CPU-ms and ≤ 25 loop-ms (mainnet's light documents cost 0.09–0.29 CPU-s).

**Guards kept or added.** Heavy requests in flight ≤ cores / 2 / processes (24 at 48 cores); heavy refused while
the loop is ≥ 95 % busy (EWMA over ~0.5 s) whatever the budget (a misprediction must not stall the loop); a heavy
request admitted **under contention** (its share under half full, or half the in-flight cap taken) runs with ~2 cores
of threads per heavy slot (`max_threads` 4 at 48 cores / 24 slots); uncontended heavy requests run with the server's
threads. The API user's `max_concurrent_queries_for_user` backstop and the cancellation of abandoned requests (KILL
by query_id; unsent statements never sent) are unchanged.

**Workers.** `CHAINGRAPH_API_WORKERS` (default 1) forks worker processes sharing the port (`node:cluster`), each with
its own loop, admission and share of the ClickHouse rate (`processes` = workers unless `CHAINGRAPH_API_PROCESSES`
says pods × workers). This is the capacity fix for the mainnet bottleneck; admission is what keeps a saturated
process fast and honest.

**Observability.** One startup line (`admission: on (request level) cores=48 (clickhouse) processes=1
cpu_budget=43.2 CPU-s/s burst 4 s loop_budget=850 ms/s burst 1 s light_share=0.25 light<=500 CPU-ms,25 loop-ms
heavy_in_flight=24 heavy_contended=0.5:max_threads 4 loop_guard=0.95 unknown=250 CPU-ms,20 loop-ms
cpu_source=query_log … workers=1`) and `GET /admission` (counters by class and refusal reason, budget fill, loop
utilisation, ledger state, per-document estimates).

**Configuration** (all optional): `CHAINGRAPH_API_ADMISSION`, `_CLICKHOUSE_CORES`, `_PROCESSES`, `_WORKERS`,
`_CPU_TARGET`, `_LOOP_TARGET`, `_LIGHT_SHARE`, `_BURST_SECONDS`, `_LOOP_BURST_SECONDS`, `_LIGHT_MAX_CPU_MS`,
`_LIGHT_MAX_LOOP_MS`, `_HEAVY_MAX_IN_FLIGHT`, `_HEAVY_CONTENDED_FILL`, `_HEAVY_CONTENDED_MAX_THREADS`, `_LOOP_GUARD`,
`_UNKNOWN_CPU_MS`, `_UNKNOWN_LOOP_MS`, `_CPU_SOURCE`, `_{LIGHT,HEAVY}_{MAX_THREADS,PRIORITY,WORKLOAD}`,
`_RETRY_AFTER_SECONDS` (all `CHAINGRAPH_API_…`). The fix-pass-4/7 lane variables (`_MAX_CONCURRENT`, `_MAX_QUEUED`,
`_MAX_WAIT_MS`, `_HEAVY_ROWS`, `_HEAVY_CONTENDED_AT`) are gone.

### 2.1 Design calls (autonomous), one line each

- **Gate in Yoga `onExecute`, after the response cache:** the earliest point where the plan key is known; a cache
  hit costs nothing and is not admitted.
- **Two metered resources (ClickHouse CPU, event loop):** fix7 shows the loop saturating with ClickHouse two-thirds
  idle; a gate on one resource cannot keep the other from queueing.
- **Plan key = document hash + operation + nodes + variable magnitudes:** a `limit` of 10 and of 10,000 differ by
  100×; two lists of hashes of one length cost the same.
- **A request with no history is heavy (default 250 CPU-ms, 20 loop-ms):** it may be anything, and only the heavy
  share may go deep into debt.
- **Symmetric EWMA (α 0.3), not fix 7's "peaks count at once":** the budgets are reconciled with measured costs, so
  the predictor should estimate the mean.
- **CPU from `system.query_log`, polled every 2 s:** `X-ClickHouse-Summary` carries no CPU (checked on 26.8); the
  log's 7.5 s lag is harmless because a request's charge stands until measured. Rows × CPU per row only without the
  grant (an old API user profile).
- **Loop time attributed at every transition, weighted by predicted loop time:** per-tick attribution missed requests
  shorter than a tick (light documents learned 0 ms); equal shares charged light requests for L35's decoding.
- **Token buckets with debt and overflow:** one request larger than the burst still runs (mainnet L15 is 59 CPU-s);
  idle light tokens go to heavy and back, so neither class wastes capacity.
- **CPU target 0.9 with a 4 s burst of the server's rate; loop 0.85 with a 1 s burst:** the first defaults (0.75,
  2 s, 0.5 s) gave A1 6 / 12 QPS a p95 of 1.7 / 1.9 s vs off's 1.1 / 1.2 s (inferred: after one L35 the heavy share
  was under half full, so the next L35 ran contended, at 4 threads) and refused at 18 QPS
  (clean runs `c-def` vs `c-t90`, /tmp/fix8).
- **Each of n processes bursts like the whole server, at 1/n of its rate:** a per-process slice of the burst refused
  L35 two workers could have run (24 refusals at 18 QPS, run `c-t90w2`).
- **Light = predicted ≤ 500 CPU-ms and ≤ 25 loop-ms:** mainnet's light documents cost 0.09–0.29 CPU-s; at 250 ms the
  synthetic X4 / X8 flapped across the bound.
- **Contended heavy requests get `max_threads` ≈ 2 cores per heavy slot (4), at fill < 0.5 or half the in-flight
  cap; no ClickHouse `priority` by default:** fix pass 7's priority pause starves heavy statements at normal load; a
  thread cap only under contention costs nothing when uncontended (A1 6/12 QPS: on = off). The priority variant was
  measured only during the contaminated window (below), so it stays configurable, off.
- **Heavy in flight ≤ cores / 2 per server, loop guard 0.95:** backstops against mispredictions while the model learns.
- **Live re-runs and other statements outside a request are charged, never refused; subscriptions and mutations are
  not admitted:** refusing a re-run would break a subscription; its CPU still counts.
- **`ApiDb` defaults to admission off; `main.ts` turns it on from the environment:** budgets need the server's cores,
  which only `main.ts` reads; embedded APIs (specs, `catalogue-run`) are not gated.
- **Workers optional (default 1):** a deployment choice (pod CPU requests), not a code default; recommended ≥ 2 on GKE.
- **Spot-check chunks from the node's stored UTXO count (1 M per chunk):** the count is in `utxo_total` already;
  mainnet node 1 gets 128 chunks (~9 M outputs each, the fix 7 target), node 39 2.
- **A chunk's spenders restricted to outpoints the node created in the range** (the outputs' acceptance subquery, one
  set): without it a small node's chunk reads the chain's inputs in its range; with it, what the node holds.

A note on the evidence: another agent ran catalogue replays against `ch1_fix8_syn` during the first local runs
(`r-*`, `v-w2`, `v-prio`, `v-cont*`); those numbers are discarded. Every number below comes from runs after it
stopped (`c-*`, `f-*`).

## 3. Local load test (`scripts/measure/admission-load.mjs`)

The fix-pass-7 harness, with per-level API process CPU (`ps`, primary and workers), per-document errors, the gate's
`GET /admission` state, two warm-up rounds and a 10 s pause for the CPU ledger. Database `ch1_fix8_syn` (golden
layout, 150 M transactions, 431 M outputs, stored UTXO set), API = `build/api/main.js`, response cache off, open
loop, 60 s per level, 30 s client timeout. Shapes as fix pass 7: **A1** = the soak mix L40–L47, L12, L31, L35, X2;
**A3** (overload B) = light L13 L20 L25 L40 L43 L46 X4 X8 at 10 QPS + heavy L23 L30 L33 L35 X12 X13 at 6 QPS.

Scaling to 14 local cores: admission off uses 4.4 / 8.8 / 13.2 / 17.9 cores at A1 6 / 12 / 18 / 24 QPS (31 / 63 / 95
/ 128 % of the server), so **6, 12 and 18 QPS are loads the server carries** (off: no error) and **24 QPS is beyond
it**, as mainnet's 10 QPS was carried and 20 / 30 were not. Mainnet's limit was the API loop, here it is ClickHouse
(the API process uses at most 0.74 of a core, GC and I/O threads included): see the flag in §0. A3 asks for ~18 cores' worth (130 %).

Arms, run back to back after the other agent's load had stopped: off / on, each with 1 and 2 API worker processes,
and the fix-7 code (`ea2a20a`, statement lanes) for reference.

## 4. On vs off

**a1 6 qps**

| Arm | ok / sent | OVERLOADED | refusal p50 / p95 ms | client timeouts | p50 / p95 ms | ClickHouse CPU-s |
|---|---:|---:|---:|---:|---:|---:|
| off | 360 / 360 | 0 | – | 0 | 43 / 1,118 | 262 |
| **on** | 360 / 360 | 0 | – | 0 | 43 / 1,116 | 261 |
| off, 2 workers | 360 / 360 | 0 | – | 0 | 41 / 1,133 | 264 |
| **on, 2 workers** | 360 / 360 | 0 | – | 0 | 40 / 1,123 | 253 |
| fix7 code (`ea2a20a`) on | 360 / 360 | 0 | – | 0 | 43 / 1,115 | 261 |

**a1 12 qps**

| Arm | ok / sent | OVERLOADED | refusal p50 / p95 ms | client timeouts | p50 / p95 ms | ClickHouse CPU-s |
|---|---:|---:|---:|---:|---:|---:|
| off | 720 / 720 | 0 | – | 0 | 57 / 1,169 | 527 |
| **on** | 720 / 720 | 0 | – | 0 | 56 / 1,172 | 534 |
| off, 2 workers | 720 / 720 | 0 | – | 0 | 59 / 1,151 | 527 |
| **on, 2 workers** | 720 / 720 | 0 | – | 0 | 59 / 1,152 | 527 |
| fix7 code (`ea2a20a`) on | 720 / 720 | 0 | – | 0 | 59 / 1,183 | 535 |

**a1 18 qps**

| Arm | ok / sent | OVERLOADED | refusal p50 / p95 ms | client timeouts | p50 / p95 ms | ClickHouse CPU-s |
|---|---:|---:|---:|---:|---:|---:|
| off | 1,080 / 1,080 | 0 | – | 0 | 254 / 2,271 | 794 |
| **on** | 1,079 / 1,080 | 1 | 2 / 2 | 0 | 168 / 2,586 | 714 |
| off, 2 workers | 1,080 / 1,080 | 0 | – | 0 | 109 / 2,136 | 790 |
| **on, 2 workers** | 1,074 / 1,080 | 6 | 1 / 3 | 0 | 79 / 2,242 | 696 |
| fix7 code (`ea2a20a`) on | 1,081 / 1,081 | 0 | – | 0 | 223 / 2,097 | 784 |

**a1 24 qps**

| Arm | ok / sent | OVERLOADED | refusal p50 / p95 ms | client timeouts | p50 / p95 ms | ClickHouse CPU-s |
|---|---:|---:|---:|---:|---:|---:|
| off | 1,423 / 1,440 | 0 | – | 17 | 469 / 12,265 | 1073 |
| **on** | 1,411 / 1,440 | 29 | 1 / 208 | 0 | 187 / 2,742 | 761 |
| off, 2 workers | 1,415 / 1,440 | 0 | – | 25 | 371 / 9,471 | 1077 |
| **on, 2 workers** | 1,411 / 1,440 | 29 | 1 / 117 | 0 | 85 / 2,576 | 750 |
| fix7 code (`ea2a20a`) on | 1,407 / 1,440 | 33 | 8 / 258 | 0 | 178 / 2,327 | 753 |

**a3 light 10 + heavy 6 qps**

| Arm | ok / sent | OVERLOADED | refusal p50 / p95 ms | client timeouts | p50 / p95 ms | ClickHouse CPU-s |
|---|---:|---:|---:|---:|---:|---:|
| off | 936 / 962 | 0 | – | 26 | light 953 / 8,571; heavy 8,226 / 24,460 | 1083 |
| **on** | 868 / 960 | 92 | 1 / 251 | 0 | light 161 / 566; heavy 1,020 / 3,141 | 733 |
| off, 2 workers | 927 / 960 | 0 | – | 33 | light 397 / 4,071; heavy 7,597 / 26,133 | 1065 |
| **on, 2 workers** | 863 / 960 | 97 | 1 / 278 | 0 | light 65 / 471; heavy 925 / 3,129 | 745 |
| fix7 code (`ea2a20a`) on | 871 / 960 | 89 | 9 / 258 | 0 | light 55 / 446; heavy 561 / 9,710 | 573 |

Light documents of A3, p95 per document (ms): on L13 527, L20 526, L25 424, L40 623, L43 439, L46 506, X4 505, X8 657
(X8 costs 0.76 CPU-s on the synthetic database: heavy by cost); on with 2 workers 288–523 (X8 656); off 7,826–11,549;
off with 2 workers 471–7,287.

Against the acceptance:

| Criterion | Result |
|---|---|
| never worse than off where the server carries the load | **6 / 12 QPS: equal** (p95 within 0.3 %, no refusal). **18 QPS (95 % CPU): 1 refusal of 1,080, p95 2.6 vs 2.3 s** (2 workers: 6 refusals, p95 2.24 vs 2.14 s): flagged in §0 |
| beyond capacity: refusals fast (p95 < 250 ms) | A1 24 QPS **208 ms** (2 workers **117 ms**); A3 **251 ms** (2 workers 278 ms); p50 1 ms everywhere: flagged in §0 |
| beyond capacity: no client timeouts | **0** in every "on" run (off: 17 at A1 24, 26 at A3; 2 workers: 25 / 33) |
| beyond capacity: light p95 under ~500 ms | A3 light mix **566 ms** (light-class documents 424–623 ms); **2 workers 471 ms** (288–523 ms); off 8,571 / 4,071 ms |

Refusals by reason (`GET /admission`, the answering worker): A1 24 QPS `clickhouse` 26, `in_flight` 4; A3
`clickhouse` 41, `in_flight` 78, `loop` 3; no `loop_guard`. Every refused request was a heavy document (L35 at A1;
L23 L30 L33 L35 X8 X13 at A3); no light-class document was refused.

What the model learned (run `f-on`, end of A3; per document, CPU-s / loop ms): L35 7.21 / 255, L33 3.88 / 304, L23
2.15 / 0.5, L30 1.10 / 23.1, X8 0.66 / 3.0, X13 0.52 / 0.7, X12 0.13 / 0.6, X2 0.13 / 0.7, L40 0.11 / 0.5, L43 0.06 /
0.7, L12 0.017 / 2.5, L20 0.008 / 0.3.

Where the loop goes (CPU profile of six in-process L35 requests, 12.9 MB responses): ~270 ms of loop per request,
the longest uninterrupted stretch 220 ms: relation assembly in compile.ts (`decode` / `withRelations` / `keyOf`, ~60
ms), GraphQL execution (~50 ms), decoding the ClickHouse rows (~45 ms), GC (~27 ms), Yoga's JSON serialisation (~22
ms).

## 5. Spot-check totals chunks per node (`utxo-set.ts`)

- `nodeCheckChunkCount(utxos, utxosPerChunk = 1_000_000)`: the smallest power of two keeping ≤ 1 M of the node's
  unspent outputs per hash range. `checkUtxoSet` and the spot-check's totals scope read the node's stored totals
  first and size the chunks from them (`options.chunks` / `totalsChunks` still override; `utxosPerChunk` /
  `totalsUtxosPerChunk` tune). Mainnet: node 1 (125.7 M) → 128 chunks (was 256), node 39 (1.19 M) → 2 (was 256).
- `unspentInHashRangeSql`: the spenders of a chunk are only those of outpoints whose transaction the node accepted
  in the range (`outpoint_transaction_hash IN (the outputs' acceptance subquery)`, one set for both sides).
- Tests: `nodeCheckChunkCount` on the mainnet counts; an e2e spec with node 1 holding three blocks and node 2 one:
  each node's whole-set check and totals scope run `nodeCheckChunkCount(its count)` chunks (counted statements),
  answer exactly as one chunk, equal; the smaller node runs fewer.

Measured on `ch1_fix8_syn` with a second node accepting 3,030 blocks (999,900 transactions; 2,856,907 unspent
outputs) of the 150 M-transaction chain, the spot-check totals reference (`referenceTotalsChunked`):

| Code | chunks | time | answer |
|---|---:|---:|---|
| fix 7 (`ea2a20a`): database-sized chunks, chain-wide spenders | 64 | **92.4 s** | 3,505 categories, 2,856,907 outputs, digest a480d0dacb4ff982 |
| this pass, database-sized chunks | 64 | 25.0 s | equal |
| this pass, one chunk | 1 | 23.3 s | equal |
| **this pass, per node** | 4 | **18.9 s** | equal |

Mainnet's node 39 (322 s with 256 chunks) should come back near its 9 s single-statement time; node 1's whole-set
check runs 128 chunks of ~9 M outputs (fix 7: 256 chunks, 946 s, 2.6 GiB peak).

## 6. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: 17 tsdoc warnings, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 196 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | 183 passed (incl. the two new chunk specs) |
| API specs `build/api/*.spec.js` | 83 passed, 3 skipped (the chipnet spec without its env); the admission spec has 13 tests (10 unit, 3 e2e) |
| chipnet spec + catalogue (`ch1_fix8_chip`: a copy of `chipnet_ch_pp2` in the current DDL, authchain index backfilled, UTXO set built; v1 = Hasura over `chipnet_pg`) | spec passed; `catalogue-run --latency 9`: **63 / 63 equal**, v2 p50 median 69.8 ms (fix 7: 73.6 ms) |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed each (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed, 1 skipped |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`.

Housekeeping: `ch1_fix8_syn`, `ch1_fix8_chip`, the Hasura container `ch1-fix8-hasura` and its metadata database
`ch1_fix8_hasura_meta`, the probe user `ch1_fix8_probe` and the worktree `/tmp/fix8/wt-fix7` removed.

## 7. What the next GKE session should verify

1. **Startup and the ledger.** Both pods: `admission: on (request level) cores=48 (clickhouse) processes=… cpu_budget=…
   burst 4 s loop_budget=850 ms/s burst 1 s … heavy_in_flight=… heavy_contended=0.5:max_threads 4 … cpu_source=query_log
   … workers=…`. `GET /admission` on a pod: `cpuSource` stays `query_log` and `stats.ledgerResolved` grows (the XML
   API user has every grant; an SQL user needs `GRANT SELECT ON system.query_log`, now in `api-user.ts`).
2. **Confirm the bottleneck.** A1 cache off 10 / 20 / 30 QPS with `CHAINGRAPH_API_ADMISSION=off`, one worker, while
   recording the API pod's CPU (`kubectl top pod` every 5 s, or `ps` in the pod): expected ~1 core (the loop) from
   20 QPS on, with ClickHouse at ~15 of 48 cores (§1). If the pod is not at one core, the §1 diagnosis is wrong:
   look at the ClickHouse client's 64 sockets (peak `api_ro` statements 60–64 in fix 7) before anything else.
3. **A1 on vs off, one worker, back to back.** 10 QPS: no refusal, p95 within ~10 % of off. 20 / 30 QPS: refusals are
   L35 / L33 (reason `loop` or `clickhouse`), OVERLOADED p95 < 250 ms, **no client timeout**, light documents
   (L40–L47, L12) p95 < 500 ms where off's were 13–28 s. `GET /admission` after each level: loop utilisation, refusal
   reasons, per-document estimates (L35's loop ms on mainnet: expected 300–800).
4. **Workers.** The same with `CHAINGRAPH_API_WORKERS=3` (the pod requests 4 CPUs) and `CHAINGRAPH_API_PROCESSES` =
   pods × 3 if several pods share the server; render the API user's backstop with `CHAINGRAPH_CLICKHOUSE_API_PODS` =
   pods × 3 (each process may hold 64 statements). Expected: 20 QPS carried without refusals; refusal p95 well under
   250 ms at 30 QPS.
5. **A3 overload B** (light 10 + heavy 8 QPS), one and three workers: light p95 < 500 ms, refusal p95 < 250 ms, no
   client timeout. If light p95 misses while refusals are fast, try `CHAINGRAPH_API_HEAVY_CONTENDED_FILL=0.75`; if
   carried loads refuse, `CHAINGRAPH_API_CPU_TARGET=1`.
6. **Spot-check and whole-set checks per node.** Node 39's totals scope (`spot39t`): 2 chunks, back near 9 s (fix 7:
   322 s, 256 chunks); node 1's `checkUtxoSet`: 128 chunks, equal, peak memory per statement a few GiB (fix 7: 256
   chunks, 946 s, 2.6 GiB); node 39's whole set: 2 chunks, equal. Any difference from fix 7's answers (125,659,151 /
   1,185,908 outpoints at the same marks) is a discrepancy: stop and report.
7. **Idle catalogue** (cache off, admission on): every document admitted (a sequential run never fills a budget), L15
   ≈ its direct time; no OVERLOADED.

## 8. Commits

| Commit | |
|---|---|
| `5029b11` | fix(store): whole-set check chunks per node from its UTXO count; chunk spenders follow the node (§5) |
| `ba03527` | feat(store): query `onResponse` hook; API user may read `system.query_log` (§2) |
| `ab66407` | feat(api): request-level admission by predicted CPU and event-loop budgets; workers; `GET /admission` (§2) |
| `7030939` | perf(api): admission defaults from the clean local load test; contended heavy threads; clean worker shutdown (§2.1, §4) |
| `bff2b7a` | style(api): sorted keys |
| (docs) | this document |

Merge note: `src/api/db.ts` changes only `query()` (no statement gate; the ticket's settings and records),
`pinnedSnapshot` (waiting marks), the admission field and `statementCpu`; branch `experiment/clickhouse-c1` also
edits `query()`.
