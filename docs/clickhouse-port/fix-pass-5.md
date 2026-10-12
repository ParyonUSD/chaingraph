<!-- cspell:ignore chipnet backfill sats EWMA cgapi tsdoc cpuset -->

# Fix pass 5: owner decisions of 2026-10-12 on fix-pass-4.md §8

Local only: ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs, `max_threads` = auto(14)), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` over `chipnet_pg` for v1. Scratch: `ch1_fix5_chip` (a copy of the read-only
`chipnet_ch_pp2` into the current DDL incl. `p_node_tx`, authchain index backfilled, UTXO set built), user
`ch1_fix5_api`; all dropped afterwards. Raw results stay local (`/tmp/fix5/`, not committed).

## 1. Decisions (fix-pass-4.md §8)

| # | Owner decision 2026-10-12 | Done |
|---|---|---|
| 1 | ClickHouse-side backstop: `max_concurrent_queries_for_user = pods × 30 + 10` on the API user's profile | §2 |
| 2 | Lane sizes derived from the node's CPU count by default, env overrides kept | §3 |
| 3 | Trusted documents share the heavy lane (no lane by document); docs note only | §4 |

## 2. Backstop (`src/store/clickhouse/api-user.ts`, `yarn clickhouse:api-user`)

- Settings profile **`chaingraph_api`**: inherits `default`, `readonly = 2`,
  `max_concurrent_queries_for_user = pods × statementsPerPod + margin`, **`const`** (a session cannot raise it:
  `SETTING_CONSTRAINT_VIOLATION`). Over the limit ClickHouse refuses at once: code 202
  `TOO_MANY_SIMULTANEOUS_QUERIES`; the API reports it as any other statement error.
- Inputs: `CHAINGRAPH_CLICKHOUSE_API_PODS` (default 1), `CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_PER_POD` (default
  30 = 24 light + 6 heavy at 48 cores), `CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_MARGIN` (default 10). The pod count is
  the deployment's: the API cannot know how many replicas run. A deployment on a server with other than 48 cores
  should set the per-pod count to its derived light + heavy (the API logs both, §3).
- The margin covers statements outside the gate: watermark polls, live-query refreshes, store-feature probes and
  **`KILL QUERY` of abandoned statements**, which is itself a statement of the user and is refused like any other
  when the limit is full (checked: the spec fills the limit, then kills inside the margin).
- Renderings of one definition: `xml` (users.d file; the user's password as sha256 only; an XML user has every grant
  unless restricted) for config-file servers, i.e. the Kubernetes kits (mounted from a Secret); `sql` / `apply` for
  servers with SQL access management (the local Docker server, local-dev.md). The SQL user needs, besides
  `SELECT ON <db>.*`, `SELECT ON system.processes` (KILL QUERY of its own statements fails without it: found here)
  and `SELECT ON system.projections` (the API's store-feature probe).
- API startup reads the effective value (`getSetting('max_concurrent_queries_for_user')`) and logs it in the
  `admission:` line, so a pod shows whether the backstop is in force.
- Kubernetes: the docs repo kit `experiments/chaingraph/ch1-utxo` renders the API user's users.d profile from
  `CH1_API_PODS` (default: the API Deployment's replicas) × 30 + 10 and checks it as the API user. The fork's Helm
  chart (`charts/chaingraph`) has no ClickHouse server, so nothing to wire there.
- Tests (`api-user.spec.ts`): formula and env; both renderings carry the limit and never the password; e2e against
  ch1-local: the limit read back, a session's attempt to raise it refused, two statements running + a kill inside
  the margin works (394), three running fill the limit, the next is refused with 202 in milliseconds.

## 3. Lane sizes from the ClickHouse server's cores (`admission.ts` `laneConcurrencyForCores`)

**Which cores.** The ClickHouse server's, not the API pod's: the lanes bound work ClickHouse does, and an API pod
of 1–2 vCPUs says nothing about it. The API reads `getSetting('max_threads')` as its own user at startup. Its
`auto` value is ClickHouse's own usable-core count (`getNumberOfCPUCoresToUse`: physical cores when the host has ≥ 64
logical CPUs, else logical; limited by the container's cgroup quota/cpuset). On the GKE c4d node (96 vCPUs, the
ClickHouse pod without a CPU limit) that is **48**, the "48-thread ClickHouse" of the floor session. A profile that
pins `max_threads` gives that value, which is also what the API's statements get. If the read fails (server down at
startup) the static defaults apply (those of 48 cores). `CHAINGRAPH_API_CLICKHOUSE_CORES` overrides the read.

**Formulas** (cores = c, heavy `max_threads` = t, default 8):

| | formula | 48 cores | 14 (local) | 96 | why |
|---|---|---:|---:|---:|---|
| heavy slots | max(1, round(c / t)) | 6 | 2 | 12 | heavy statements are CPU bound and use all t threads: the full heavy lane asks for about every core once |
| heavy `max_threads` | t = 8 (env) | 8 | 8 | 8 | unchanged; keeps one heavy statement from taking the whole server |
| heavy queue | 4 × heavy slots | 24 | 8 | 48 | a deeper queue only turns fast refusals into 10 s waits (measured, below) |
| light slots | max(8, floor(c / 2)) | 24 | 8 | 48 | light statements are ~40–60 ms lookups, mostly one thread and partly waiting on marks / reads; half a slot per core keeps them under the core count while heavy work runs (they also have the higher priority); 8 floor for small servers |
| light queue | 256 | 256 | 256 | 256 | unchanged |

Explicit `CHAINGRAPH_API_{LIGHT,HEAVY}_MAX_CONCURRENT` / `_MAX_QUEUED` still win. `round`, not `floor`: the first
version used floor, which gave 1 heavy slot on 14 cores; under overload half the cores sat idle and heavy refusals
waited the full 10 s in a 24-deep queue (row "floor" below). Startup log, e.g. local:
`admission: on cores=14 (clickhouse) light=8 heavy=2 heavy_max_threads=8 max_concurrent_queries_for_user=40`.

## 4. Trusted documents (decision 3)

Admission is by statement cost (rows read), not by document. A trusted document whose statement reads ≥ 1 M rows
(X7 GetBlockDetails on mainnet and chipnet) shares the heavy lane with everything else heavy and is refused fast
(`OVERLOADED`, 503, `Retry-After`) when that lane is full. No per-document lane. Clients of such documents should
retry after `Retry-After`. Recorded in fix-pass-4.md §8.

## 5. Overload re-run (fix-pass-4.md §3b, same generator and documents)

API as a separate process reading as `ch1_fix5_api` (backstop 40), response cache off, stored set off, 60 s per run,
open loop. Scenario B = light 10 QPS + heavy 8 QPS, client timeout 30 s; A = light 10 + heavy 4, timeout 5 s.

| run | lanes (light / heavy, heavy queue) | light ok / 600 | light p50 / p95 ms | heavy ok | refusals p95 ms | after load |
|---|---|---:|---:|---:|---:|---|
| fix pass 4, B, admission on | 24 / 6, 24 | 547 | 51 / 72 | 184 / 480 | 16 | 0 running |
| **B, 24 / 6 forced** (this machine today) | 24 / 6, 24 | 537 | 55 / 105 | 134 / 480 | 17–18 | 0 running, 0 started |
| B, derived with floor (first version) | 8 / 1, 24 | 536 | 30 / 47 | 70 / 480 | **10,007** | 0 / 0 |
| **B, derived (final)** | 8 / 2, 8 | 532 | **34 / 67** | 106 / 480 | **10–12** | 0 / 0 |
| **A, derived (final)** | 8 / 2, 8 | 550 | 31 / 68 | 89 / 240 (123 OVERLOADED, 28 client timeouts) | 10 | 0 / 0; 27 statements cancelled (394) |

- No regression in what admission is for: light documents' latency stays flat (p95 67–68 ms under 3–5× CPU
  overload, better than the 24 / 6 run on the same machine), refusals arrive in ~10 ms, no MEMORY_LIMIT, no 202
  from the backstop, nothing running or started after the load ends. Peak statements started per second 30–31.
- The light refusals (50–68) are X7, heavy by rows read (§4).
- Heavy throughput on 14 cores is lower with 2 slots than with 6 (106 vs 134 ok): 6 × 8 threads on 14 cores ran
  more heavy work at once at the cost of light p95 (105 vs 67 ms). That is the intended trade; on the 48-core GKE
  server the derived sizes equal the fix-pass-4 defaults, so the GKE soak measures the same configuration.
- Scenario A's 28 heavy client timeouts: the heavy wait bound (10 s) is above that client's 5 s timeout; the
  abandoned requests left the queue or were cancelled (394), as in fix pass 4.

## 6. Commits

| Commit | |
|---|---|
| `24a59ee` | feat(api): lane sizes derived from the ClickHouse server's cores |
| `3808efe` | feat(store): API user profile with the `max_concurrent_queries_for_user` backstop |
| `93736c8` | fix(api): heavy slots round, heavy queue 4 per slot |
| (docs) | this document, fix-pass-4.md §8, local-dev.md |

## 7. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the tsdoc warnings as before, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 188 passed |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | **180 passed** (177 + 3 `api-user`). Two earlier runs at `--concurrency=4` hit the known lease-sensitive flakes of fix-pass-4.md §5 (`g1-fix` "lab conditions (utxo off)" both times, `clickhouse-store` "event-loop stall" once; 1.5 s test lease); both files pass alone (2 × 36/36) |
| API specs `build/api/*.spec.js` | 76 passed, 3 skipped (the chipnet spec without its env) |
| chipnet spec (`ch1_fix5_chip`, v1 = Hasura over `chipnet_pg`) | 3 passed: **63/63 equal**; 8 statements read the stored set |
| e2e ClickHouse, UTXO on / off | 56 / 56 passed |
| e2e Postgres (ch1-pg) | 102 passed |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`.

## 8. For the next GKE session

The kit (`experiments/chaingraph/ch1-utxo`) checks: the API's `admission:` line shows `cores=48 light=24 heavy=6
heavy_max_threads=8`, and the API user's `max_concurrent_queries_for_user` equals pods × 30 + 10. The soak at
10/20/30 QPS runs with those sizes (fix-pass-4.md §7 item 1).
