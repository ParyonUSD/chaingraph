<!-- cspell:ignore chipnet backfill subquery sats PUSD unhex rebase cgapi EWMA Retryable -->

# Fix pass 4: findings of the 2026-10-12 floor verification, owner decisions of 2026-10-12

Local only: ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura `chaingraph/hasura:dev-build`
over `chipnet_pg` for v1. Inputs: [floor-run-log.md](floor-run-log.md), the docs repo decision doc
`docs/chaingraph/decisions/2026-10-12_clickhouse-floor-verification.md`, raw results
`chaingraph-performance/2026-10-12-floor-*/` (read only). Raw results of this pass stay local (`/tmp/fix4/`, not
committed). Scratch databases `ch1_fix4_*` (dropped afterwards); `chipnet_ch_pp2` read only.

## 1. Merge

`origin/experiment/clickhouse-utxo-async` (`c9b51d1`: async per-node UTXO set, `p_node_tx` default, `block_inclusions`
height order, [utxo-async.md](utxo-async.md)) merged into `experiment/clickhouse-store` with a merge commit
(`17b80e5`). No conflicts: the store branch had only doc commits since `c61dc47` (run logs, `phase2-api.md` §9.7
decisions), and git merged `phase2-api.md` cleanly; both sets of notes are kept.

## 2. Owner decisions of 2026-10-12 (all recorded here, in utxo-async.md and in the docs repo plan §9)

| # | Decision | Where |
|---|---|---|
| D1 | **`spent_by` filter: opt-in `accepted_by: node`** (same semantics as the `spent_by(accepted_by:)` field) | §2.1; `phase2-api.md` §9.7 q3 decided |
| D2 | **`authchain_branches` capped at 1,000 branches per base**, `AUTHCHAIN_TOO_WIDE` above it, configurable | §2.2; `phase2-api.md` §9.7 q5 decided |
| D3 | UTXO set lag-correction cost accepted for now; measure on mainnet next GKE session, no optimisation now | `utxo-async.md` §9 q1, §6 below |
| D4 | Keep the 100,000 acceptance-row fallback bound (`CHAINGRAPH_API_UTXO_SET_MAX_LAG_ROWS`) | `utxo-async.md` §9 q2 |
| D5 | Keep the `p_script` projection on `utxo_set` | `utxo-async.md` §9 q3 |
| D6 | **New: periodic background spot-check in the UTXO job** | §2.3; `utxo-async.md` §11 |
| D7 | **F3 restated:** ≤ 50 ms key analysis applies only to statements that read under about 1 M rows; heavy statements are judged on wall time only | §3c, §6 |

### 2.1 D1: `spent_by: { accepted_by: node, … }` (`cc1705c`)

- Schema: `OutputFilter.spent_by` is now `SpentByFilter` = every `InputFilter` field plus `accepted_by: String`
  (`_and`/`_or`/`_not` inside stay `InputFilter`). The committed SDL is updated.
- SQL (`sql.ts` `outputPredicate`): without `accepted_by` unchanged (node-agnostic, structural, every stored spend).
  With it, the spending inputs matching the rest of the filter go through `Sql.acceptedRows` exactly as the
  relationship field's `accepted_by` does: a statement-level MATERIALIZED CTE of the inputs, then the named node's
  `tx_acceptance` read by key for only those transactions, at that node's `visible(n)` of the request snapshot (a block
  the node accepts or its mempool).
- Pinning: the node must be pinned by the request snapshot. `operationNodeNames` now also collects `accepted_by` fields
  inside argument input values, literal or variable (`where: $where`), so a node named only inside a filter is
  pinned (and keyed in the response cache). `Sql.nodeIdOf` resolves the name from the request's pinned nodes.
- Tests (`api-structural.spec.ts`, fixture with a fork at height 2, a stale block and conflicting mempool spends on two
  nodes), per node: equal to the expected spends from the fixture; **equal to the explicit
  `spent_by: { transaction: { block_window: {} } }` at the node's own root**; the same under a root of the other node;
  through a variable; `_not: { spent_by: { accepted_by } }`; per spender the mempool spends (m1/m2) and fork spends
  (x/y) only for their node and the stale-block spend (z) for none; an unknown node errors. A mutation check (filter
  ignoring `accepted_by`) fails the test.

### 2.2 D2: `authchain_branches` cap (`9acdacb`)

- `CHAINGRAPH_API_AUTHCHAIN_MAX_BRANCHES` (default 1,000; `createApi({ authchainMaxBranches })`).
- Bound inside the walk, not only on the result: each recursion step's new rows of one base are distinct paths that
  each end in at least one final path, so a step creating more of them than the cap proves the tree is over it; the
  walk stops there (`throwIf` over `count() OVER (PARTITION BY base)` in the step). A final check counts the distinct
  paths per base (paths ended in earlier steps). Window functions inside the recursive step work on 26.8 (checked).
- Error: `AUTHCHAIN_TOO_WIDE`, message `…: an identity-output tree has more than 1000 branches (authchain_branches cap
  per base); ask for authchains(node:) or a narrower base`, no data for the field: never a truncated tree.
- Tests: the two-branch tree of the structural fixture with cap 1 (error, code, cap in the message, no data) and cap 2
  (the whole tree). The chipnet catalogue (X6, X13 included) stays equal.

### 2.3 D6: UTXO set spot-check (`4b01f35`, `05ba2c7`)

Details in [utxo-async.md](utxo-async.md) §11. Every `CHAINGRAPH_CLICKHOUSE_UTXO_SET_SPOT_CHECK_MS` (default 1 h, 0 =
off) the maintainer compares, per node at one snapshot, 3 random token categories (rows + category totals), 3 random
locking-bytecode prefixes and the node's whole-set totals, stored read rule vs query time. Mismatch: diagnostic
`utxo_set_spot_check_mismatch` logged at **error** with node, scope, value and both answers; counters in
`utxoSetStatus()` and on the agent's internal API `/metrics` (Prometheus text). Tests include a mismatch forced with
`ClickHouseChecker.forgetNodeValidation` (the history-rewrite helper).

Cost on chipnet (`ch1_fix4_chip`, 1.21 M UTXOs; warm, 3 runs): a category scope **~75 ms**, a prefix scope ~0.3–0.4 s
(one prefix held 113,802 outputs), whole-set totals ~1.6 s: **~2.5 s per round**. The first version built the node's
whole acceptance set for every scope (1.8 s per category); the scopes now read `tx_acceptance` by key for their own
transactions. Chipnet rounds: all scopes equal, totals 1,210,030 UTXOs / 1,343,492,499,959,490 sats (as utxo-async.md
§6). On mainnet the totals scope is a whole-chain query-time count (U4 timed out at 30 s through the API on golden): its
cost must be measured on GKE (§7); `spotCheck: { totals: false }` turns it off.

## 3. Floor findings

### 3a. Abandoned work was not cancelled (`c6c0113`)

**Why the pod kept sending statements for 6 minutes.** Three things together:

1. No request signal reached ClickHouse statements: resolvers called `ApiDb.query` without any notion of the request,
   so a GraphQL request whose client had gone kept running every statement of every level, one after another.
2. Statements waited in the ClickHouse client's socket pool (64 sockets; a request is a chain of statements). Under
   overload the pool queue held hundreds of statements of requests whose clients had already timed out, and drained
   them in order: that is the 190–260 statements/min after the load ended.
3. **Closing the HTTP connection does not stop a POST SELECT on ClickHouse 26.8**, even with
   `cancel_http_readonly_queries_on_client_close = 1` (which the API already set): verified locally, a query whose
   client was gone after 1 s was still running 5 s later. The server only notices a closed socket when it writes to it
   (with `send_progress_in_http_headers = 1` it does, but progress headers on a 60 s statement would overflow Node's
   header size limit). So the client's own 65 s request timeout was the only bound.

**Fix.**
- `listen()` runs each HTTP request in a request scope (`AsyncLocalStorage`, `db.ts` `requestScope`) whose
  `AbortSignal` fires when the response closes before it finished (client disconnect, client timeout). graphql-ws
  operations get the socket's signal.
- `ApiDb.query`: a statement of an aborted request is never sent; a queued one leaves the admission queue (§3b); a
  running one is aborted on the HTTP side **and killed** (`KILL QUERY WHERE query_id = … ASYNC` over a 2-socket client
  of its own, so kills never queue behind the reads they stop). Every API statement has `query_id = cgapi-<uuid>`.
  The request fails with `REQUEST_ABORTED` (nobody reads it). An aborted read is not retried.
- Tests (`api-admission.spec.ts`): a running statement (minutes of CPU) whose request aborts fails in < 1 s, is gone
  from `system.processes`, and `query_log` shows code 394 QUERY_WAS_CANCELLED; over HTTP, three clients that
  disconnect while queued leave the queue and none of their statements is ever sent.

### 3b. Uncached overload: admission control (`c6c0113`, `ce3537e`)

**Design (`src/api/admission.ts`).** Every data statement passes a gate with two lanes:

| lane | statements | default per API process | ClickHouse settings |
|---|---|---|---|
| light | shapes that read < 1 M rows (EWMA), new shapes | 24 running, queue 256, wait ≤ 5 s | `priority = 1` |
| heavy | shapes that read ≥ 1 M rows, statements that timed out or ran out of memory, aggregates until measured | 6 running, queue 24, wait ≤ 10 s | `max_threads = 8`, `priority = 2` |

- Full queue or wait over the bound: **`OVERLOADED` at once, HTTP 503 with `Retry-After: 2`** (Yoga carries the
  status and header from the error's `extensions.http`), `extensions.lane`, `retryAfterSeconds`.
- Cost of a statement shape (label + SQL text, values excluded) = rows read as ClickHouse reports them in
  `X-ClickHouse-Summary` (max of `read_rows`, `total_rows_to_read`). A first version used wall time: under load every
  shape's wall time grew past the bound, point lookups moved to the heavy lane and were refused (light p95 4.7 s, 120
  OVERLOADED). Rows read do not grow with load. The 1 M-row bound is the F3 restatement (D7).
- `priority` makes ClickHouse pause heavy statements while light ones run; measured below (light p95 72 vs 504 ms
  without it). `CHAINGRAPH_API_{LIGHT,HEAVY}_WORKLOAD` can name ClickHouse workloads (CPU scheduling) when the
  operator has created them; not set by default.
- Env: `CHAINGRAPH_API_ADMISSION` (on/off), `CHAINGRAPH_API_{LIGHT,HEAVY}_{MAX_CONCURRENT,MAX_QUEUED,MAX_WAIT_MS,
  MAX_THREADS,PRIORITY,WORKLOAD}`, `CHAINGRAPH_API_HEAVY_ROWS`, `CHAINGRAPH_API_RETRY_AFTER_SECONDS`.
- Limits are per API process. `max_concurrent_queries_for_user` (a per-user backstop across pods) belongs in the
  ClickHouse user profile of the API user; the API does not set it (owner decision, §8).

**Load test (local, chipnet `ch1_fix4_chip`, API as a separate process, response cache off, stored set off so whole-chain
aggregates are heavy).** Open-loop generator (`/tmp/fix4/scripts/load.mjs`): light = 8 catalogue query documents
(L25, L43, L46, X8, X4, X7, L13, L20; 37–64 ms alone), heavy = 3 whole-chain unspent/output aggregates (0.6–2.7 s
alone, all cores). 60 s per run. "Before" = `9acdacb` (this branch before 3a/3b) from a temporary worktree.

Scenario A, clients give up (light 10 QPS + heavy 4 QPS, client timeout 5 s), the floor pattern:

| | light ok / 600 | light p50 / p95 ms | heavy ok / 240 | after the load ended |
|---|---:|---:|---:|---|
| before | 125 (475 timeouts) | 918 / 2,729 | 0 | **63 statements running, 89 more started**; drained after ~30 s |
| cancellation, admission off | 600 | 495 / 2,420 | 15 | **0 running, 0 started** (225 statements cancelled, 394) |
| cancellation + admission | 600 | **56** / 3,899 (point lookups p95 61–106; X7 is heavy, below) | 104 | 0 running, 0 started |

Scenario B, overload (light 10 QPS + heavy 8 QPS, client timeout 30 s, the floor soak's timeout):

| | light ok / 600 | light p50 / p95 ms | light errors | heavy ok / 480 | heavy errors |
|---|---:|---:|---|---:|---|
| before | 457 | 556 / 2,574 | 143 MEMORY_LIMIT_EXCEEDED | 25 | 455 MEMORY_LIMIT (p95 14.7 s) |
| admission off | 501 | 243 / 1,282 | 99 MEMORY_LIMIT | 93 | 387 MEMORY_LIMIT (p95 10.3 s) |
| **admission on** | 547 | **51 / 72** | 53 OVERLOADED, **p95 16 ms** (all X7) | **184** | 296 OVERLOADED, **p95 16 ms**; no memory error |
| admission on, no priority | 546 | 97 / 504 | 54 OVERLOADED | 183 | 297 OVERLOADED |

Peak statements started per second: before 109 (A) / 70 (B), after 32–37. X7 (GetBlockDetails) reads 1.14 M rows on
chipnet, so its main statement is heavy by D7 and shares the heavy lane; it is refused (fast) under heavy overload
rather than slowing the light lane. Light point lookups keep ~40–60 ms p50 under 3–5× CPU overload; errors are explicit
and arrive in milliseconds instead of client timeouts. A synthetic mainnet-scale load test was not needed to show the
behaviour; the mainnet numbers come from the next GKE soak (§7).

### 3c. Spend filler: single-block reads dominated by key analysis (`671a36a`, `fb24b45`)

**Finding (golden, floor-run-log 3.7):** the filler's 1-block shapes took 770 ms (outputs) / 1,635 ms (join) with
2.2 / 4.7 s of key CPU for 131 / 241 selected marks.

**Cause.** Not the outpoint-set search on `output.transaction_hash`. It is the coinbase guard on every `input` read,
`outpoint_transaction_hash != zero`: a NOT IN condition on the leading key column of `input.p_outpoint`
(granularity 128). While choosing a projection, ClickHouse runs a generic exclusion search with it over every
p_outpoint mark, excludes nothing, then reads the base table. Golden's processed-mark counts match exactly: outputs
20.4 M = p_outpoint 8.1 M + output 9.07 M + p_script/p_category 2 × 1.13 M + input 1 M; the join's second input read
adds 9.1 M (29.5 M). `identity()` does not hide the condition (26.8 looks through it); neither does an
`OR outpoint_index != 4294967295` form.

The same guard sat on the UTXO add-on's commit-path spends (`utxo.ts`: every commit with the add-on on paid it; trace
797 ms key CPU → 0), every spend-index batch (4 input reads), the UTXO-set build, the checker's spend join, and the
API's spend-relation tail above the spend watermark.

**Fix.** `notCoinbaseOutpointSql()` (`utxo.ts`): `toFixedString(toString(col), 32) != zero`, exact for
FixedString(32) and invisible to key analysis, at all sites. Results identical in every shape.

Synthetic scale (`ch1_fix4_filler_syn`, current DDL: output 431 M rows / 3.37 M marks / 10 parts; input 375 M rows,
p_outpoint 2.93 M marks / 9 parts; the kit's `filler-batch.sql` shapes; p50 of 5 warm runs, quiet ClickHouse):

| shape | before ms | after ms | key CPU before → after ms | rows read |
|---|---:|---:|---|---:|
| 1 block, outputs | 262 | **80** | 837 → 159 | 4.0 M |
| 1 block, join | 526 | **120** | 1,672 → 229 | 6.9 M |
| 64 blocks, outputs | 1,406 | 1,364 | 1,957 → 1,374 | 167 M |
| 64 blocks, join | 2,133 | 1,739 | 3,192 → 1,671 | 281 M |
| one input read (330 txs) | 246 | **31** | 800 → 62 | 2.9 M |

What remains is the useful part: the leading-column set search on `output` / `input` and the data read (64-block
batches are bound by reads). Expected on golden (2.6–2.8× the p_outpoint marks): the 1-block shapes lose ~0.6–1.2 s
each, a filler batch ~2–3 s, a commit with the UTXO add-on ~0.5–1 s of key CPU. Not reproduced locally: the real
filler's `commit_seq` range selection (a golden-like backfill gives every row the same `commit_seq`).

### 3d. Kit bugs

In the docs repo; not part of this pass.

## 4. Commits

| Commit | |
|---|---|
| `17b80e5` | merge `experiment/clickhouse-utxo-async` |
| `cc1705c` | feat(api): `spent_by` filter `accepted_by` (D1) |
| `9acdacb` | feat(api): `authchain_branches` cap (D2) |
| `4b01f35` | feat(store): UTXO set spot-check, `/metrics` (D6) |
| `c6c0113` | feat(api): admission control, cancellation of abandoned statements (3a, 3b) |
| `ce3537e` | fix(api): admission by rows read, not wall time |
| `671a36a` | perf(store): coinbase guard hidden from key analysis (3c) |
| `fb24b45` | perf(api): the same in the spend-relation tail |
| `05ba2c7` | perf(store): spot-check scopes read acceptance by key; utxo-async.md decisions and §11 |

## 5. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the tsdoc warnings as before, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 185 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | **177 passed** with `--concurrency=4` (incl. the two spot-check tests). In the default fully parallel run two lease-sensitive tests (1.5 s test lease) failed twice: `g1-fix` "lab conditions (utxo off)" (the known flake, also in the post-merge run) and `clickhouse-store` "an event-loop stall past the lease deadline" (`LeaseLostError` under load); both pass alone (2 × 36/36) |
| API specs `build/api/*.spec.js` | 75 passed, 3 skipped (the chipnet spec without its env) |
| chipnet spec (`CHAINGRAPH_API_CHIPNET_DATABASE=ch1_fix4_chip`: a copy of `chipnet_ch_pp2` into this DDL incl. `p_node_tx`, authchain index backfilled, UTXO set built; v1 = Hasura over `chipnet_pg`) | 3 passed: **63/63 equal**; 8 statements read the stored set |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed |

The e2e runs need `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301` and here `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`: another
worktree's e2e run (`chaingraph-unspent-node-ids`) held 19333–19335, and the post-merge ClickHouse e2e runs failed
with EADDRINUSE until the base moved. In one post-merge run under heavy parallel load (the synthetic 3c database being
built) two ClickHouse e2e tests timed out (100-block re-org, incomplete-historical-block restart); the final runs pass.

Housekeeping: `ch1_fix4_chip` and `ch1_fix4_filler_syn` dropped; the Hasura container `ch1-fix4-hasura` and its
metadata database removed; the temporary worktrees under `/tmp/fix4` removed. Pre-existing scratch databases of
earlier sessions were left alone.

## 6. F3 under the restatement (D7)

Re-read from the floor session's per-statement qlog (`chaingraph-performance/2026-10-12-floor-api/results/qlog-*.tsv`,
columns `key_cpu_ms`, `read_rows`): **F3 passes as restated on both nodes.**

| qlog | statements reading < 1 M rows | of them over 50 ms key CPU | max key CPU among them |
|---|---:|---:|---:|
| node 1 (`qlog-paryon`) | 12 / 64 | **0** | 8.5 ms |
| node 39 (`qlog-chipnet`) | 23 / 64 | **0** | 15 ms |
| node 1 with `p_node_tx` (`qlog-pnodetx`) | 12 / 64 | **0** | 8.6 ms |

Every statement over 50 ms of key CPU reads ≥ 1 M rows (the smallest, L3's, 1.7 M) and is judged on wall time only;
F6 (every heavy statement faster than 10-11) covers those. Fix 3c should lower the key CPU of statements that read
`input` with the coinbase guard (the API's spend-relation tail) further; to verify on GKE.

## 7. What the next GKE session should verify

1. **Soak with admission** (cache off, 10/20/30 QPS, 30 s client timeout): expect explicit `OVERLOADED` (503) in
   milliseconds instead of 30 s client timeouts, light documents' latency flat, ClickHouse concurrent statements bounded
   by `pods × (24 + 6)`, no MEMORY_LIMIT. Tune `CHAINGRAPH_API_HEAVY_MAX_CONCURRENT` / `_MAX_THREADS` for 96 vCPUs
   (defaults: 6 × 8 threads).
2. **Abandoned work:** after the soak ends, `system.processes` has no `cgapi-%` statement within seconds and
   `query_log` shows the abandoned ones as 394; the cache-on run right after is no longer contaminated (the kit's pause
   can stay at 120 s).
3. **Filler (3c):** re-run `ch1-floor-filler.sh`: key CPU and processed marks (expect ~11 M fewer processed marks per
   input read), the backfill / filler spend time, and per-commit latency with the UTXO add-on on.
4. **F3 restated:** key CPU of statements under 1 M rows read (§6).
5. **UTXO set:** the lag-correction cost (D3; utxo-async.md §8 item 4), and the spot-check round time on mainnet, the
   totals scope in particular; decide whether it stays on.
6. `spent_by(accepted_by:)` filter and `authchain_branches` cap: catalogue equality as before (L15/L16 unchanged; the
   filter form is new and not in the catalogue).

## 8. Owner decisions (2026-10-12; implemented in [fix-pass-5.md](fix-pass-5.md))

1. **`max_concurrent_queries_for_user`** for the API's ClickHouse user (a cluster-wide backstop across API pods; the
   API's own limits are per process). Proposal: `pods × 30 + 10` in the user's profile, set by the deployment, not by
   the API.
   **Decided: yes, `pods × 30 + 10`** in the API user's settings profile `chaingraph_api` (`const`), pod count
   configurable; rendered as users.d XML or SQL by `yarn clickhouse:api-user`, wired into the next GKE kit and the
   local Docker setup (local-dev.md). The API logs the value it sees. fix-pass-5.md §2.
2. **Default lane sizes for production** (24 light / 6 heavy, heavy `max_threads` 8 per process): keep until the GKE
   soak, or set from the node size?
   **Decided: derived from the ClickHouse server's cores by default, env overrides kept.** Cores = the server's
   `max_threads` for the API user (its usable cores; 48 on the GKE c4d node); heavy = max(1, round(cores / 8)),
   light = max(8, floor(cores / 2)), heavy queue 4 per heavy slot: 24 / 6 / 8 threads at 48 cores.
   fix-pass-5.md §3.
3. **X7-class documents** (light to clients, but > 1 M rows read on mainnet) share the heavy lane and are refused under
   heavy overload. Acceptable (they are refused fast), or give trusted documents a lane by document instead of by
   statement cost?
   **Decided: trusted documents share the heavy lane** (admission stays by statement cost); clients retry after
   `Retry-After`. Docs note only: fix-pass-5.md §4.
