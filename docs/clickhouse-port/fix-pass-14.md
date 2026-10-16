<!-- cspell:ignore chipnet paryon qlog subquery subqueries analysed analyses optimises behaviour covariate tsdoc prewhere worktree syn arrayJoin windowless golden's optarm smallrows optimised serialises parenthesised -->

# Fix pass 14: X2's scan back at the server threads, node 39's median under 65 ms (lean statements)

Local only (2026-10-16): ch1-local ClickHouse 26.8.22.13 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` (container `ch1-fix14-hasura`, port 18085) over `chipnet_pg` for v1. Base: `36a9dfe` (code
of `906f1a6`, the #110 image). Input: the fix13b mainnet session (#110: 23 PASS, 1 FAIL [C1 node 1: X2]), its catalogues,
`qlog-*.tsv`, traces, `optarm/deltas-*.tsv` and the small-bound A/B (`smallrows/`). Scratch databases: `ch1_fix14_chip` (a
copy of `chipnet_ch_pp2` in the current DDL, authchain index backfilled, UTXO set built) and `ch1_fix14_syn`
(`synthetic/build.sh … golden`, 150 M transactions as node 1, plus `ch1_fix14_chip` merged in as node 2: the golden-like
store of fix passes 12–13). Raw results stay local (`/tmp/fix14/`). Every timing is with `NODE_ENV=production`.

**The goal: the original fixed C1 targets, no restatement or exception.** Node 1 ≤ 80 ms (#110 75: met), node 39 ≤ 65 ms
(#110 73.4), no per-document regression against #107 (#110: X2). **Expected on mainnet: node 39 ~58.5 ms (60.5
conservatively), node 1 ~68 ms, X2 back to ~47 ms** (§2.5).

**No data discrepancy.** The chipnet catalogue is 63 / 63 equal against v1 (48 equal, 15 equal-unordered); through HTTP with
admission on, the golden-like store's node-2 catalogue is 63 / 63 equal in all 9 runs (base and head, 48 + 15); node 1
(synthetic, v2 only) works as before. Every direct statement A/B read the same rows on both sides; a spec answers every
fast-executor document and a 2,000-transaction page byte for byte with lean statements on and off.

## 1. X2 at 8 threads (item 1)

**Cause.** Fix 13's small class (light, predicted to read ≤ 3 M rows: `max_threads 8`) took X2's node-tips statement:
1.22 M rows in **4 parts** on golden, one node-wide `node_block` collapse and one `block` scan, work the server's 48
threads split well (42 ms at the default, 67 ms at 8; #110 query_log). The documents the 8 threads help are many small
reads, each a few granules of many parts. Golden's #110 node-1 statements reading 1–3 M rows, by rows per selected part
(query_log `SelectedParts` summed over the statement), with the small-bound A/B's delta (default threads − 8 threads):

| doc | rows | parts | rows / part | default − 8 threads |
|---|---:|---:|---:|---:|
| X2 | 1.22 M | 4 | **305 k** | **−17.3 ms** |
| L43 | 2.10 M | 4 | 526 k | −2.9 |
| L41 / X14 / X15 | 2.27 M | 4 | 568 k | −9.0 / −2.9 / −4.1 |
| L6 | 2.44 M | 65 | 37.5 k | −17.6 |
| L4 / L24 / L39 | 2.62 / 1.19 / 1.08 M | 90 / 58 / 66 | 29.1 / 20.6 / 16.4 k | −2.5 / −2.2 / −2.1 |
| L34 | 1.42 M | 48 | 29.6 k | **+26.9** |
| X13 | 1.46 M | 167 | 8.7 k | **+55.5** |
| X3 / L1 / L11 / L22 | 1.37–2.38 M | 48–124 | 18.6–28.5 k | +2.4 to +4.3 |

The scans (≥ 305 k rows per part) all prefer the default; every statement of many parts sits at ≤ 37.5 k. A flat row
bound cannot separate X2 (1.22 M) from L34 / X13 (1.42 / 1.46 M); rows per part does.

**Fix** (`3e51984`). The CPU ledger reads `SelectedParts` from `query_log` with the CPU; the cost model learns a plan
key's parts like its rows; a light request is small only at ≤ 3 M predicted rows **and**, once its parts are measured,
≤ **100,000 rows per part** (`CHAINGRAPH_API_SMALL_MAX_ROWS_PER_PART`, 0: no bound; `isSmall`). 100 k is near the
geometric middle of golden's gap (37.5 k … 305 k). Parts, like rows, do not depend on the threads, so a document's class
changes at most once (when its parts arrive, a few seconds after its first request) and never flips back; the start-up
warm-up learns them before traffic (X2, L41, L43 are warm-up documents; the kit adds X14 and X15). Without the
`query_log` grant the rule is fix 13's. The startup line reads `small<=3000000 rows,<=100000 rows/part:max_threads 8`.
Specs: `api-admission` (X2 leaves the small class once its 4 parts are measured, X13 stays; the rule's edges; the ledger
reads CPU and parts from a real `query_log`).

L6 (37.5 k rows per part, −17.6 ms at the default in the A/B, +22 in #107's) stays small: it is inside the band of the
documents that gain, and the A/B's two arms disagree on it.

**Expected on mainnet:** X2 node 1 ~45–48 ms (#107 47, #110 74), so C1 node 1's per-document rule passes; L41 / L43 / X14 /
X15 −3 to −9 ms; X13 / L34 / L14 unchanged (still small). Node 39's X2, L41, L43, X14, X15 (326–408 k per part), X4 (146 k)
and X11 (106 k) leave the small class too; all sit below the median.

## 2. Node 39's median (item 2)

### 2.1 Where a small statement's time goes

Node 39's median-band documents (L2, L38, L24, L9, L21, L11, L29, L8, L17: 67–90 ms on golden) are 4–6 scalar levels
each, 0.1–1.6 M rows, 0.1–0.2 CPU-s. Their traces show one pattern on golden and locally: a level's own read takes 1–3 ms,
and the gap after each scalar (its result folded into the query tree, the next level analysed, planned and optimised) is
the rest:

| trace (`send_logs_level=trace`) | total | before the first read | gaps after each scalar | reads |
|---|---:|---:|---|---|
| golden node 39 L27 (#110) | 41 ms | 4.7 ms | 5.1, 5.8 ms; 4.3 to the end | 2–4 ms each |
| golden X4 (#110) | 39 ms | 3.8 ms | 6.8, 5.8, 5.2 ms; 3.5 to the end | 1–4 ms each |
| golden-like node 2 L38 | 50 ms | 5.2 ms | 3.7, 2.8, 3.3 ms; 3.7 to the end | 1–7 ms each |

A main-thread profile (real-time and CPU samples, `trace_log`) puts no single function on top: the analyzer, the
planner, `QueryPlan::optimize` (KeyCondition construction, PREWHERE's selectivity estimator, projection candidates per
part), and waiting for each scalar's pipeline. What the analysis and planning cost depends on is **how many query nodes
each level has**. One level, L38's outputs of the page, standalone on the golden-like store (median of 15–20, µs
precision from `query_log`):

| form | ms |
|---|---:|
| as compiled (view → source → `WHERE 1` filter → numbering → `SELECT *` wrapper; link set through two subqueries) | 4.65–4.79 |
| without the row-number window | 4.37 |
| without the wrappers | 4.34 |
| without both | 3.66–3.73 |
| one SELECT with the visibility condition | 2.85 |
| one SELECT without it | 2.25–2.55 |

The rows read are the same in every form. The golden-like store's node-2 statements carry ~2.3 ms of fixed cost per
level beyond a plain read, and golden's gaps are ~1.5× the local ones.

### 2.2 Tried and dropped

| idea | measured (golden-like node 2, direct, interleaved) | why dropped |
|---|---|---|
| `max_threads 4` for small requests | band median −1.0 ms | within noise; fix 12 saw 4 threads slow X2 / X11 / X7 |
| 40 query settings, each off (`optimize_*`, `query_plan_*`, statistics, condition cache, PREWHERE reordering …) | only `query_plan_remove_unused_columns = 0` (−1.3 ms) and `use_concurrency_control = 0` (−0.75) beyond the ~0.5 ms position bias | the first keeps unused columns (heavy reads read more), the second is server-wide; fix 13's single-setting run agrees |
| hide the page's subset sets (gate, spend, output) from key analysis with `identity()` | L24 45 → 64 ms, 0.67 → 1.09 M rows | the narrower sets prune granules |
| two sibling levels in one scalar (UNION ALL of tagged arrays) | L38 44 → 47 ms | the folded tuple costs what the scalar saved |
| `optimize_aggregation_in_order` for the node tips | X2 26 → 86 ms, L41 20 → 31 | serialises the collapse |
| per-node projection / id-range or partition pruning | — | 26.8 already prunes parts by column statistics where ids separate (golden tx_acceptance 2 → 1 part, block_transaction 7 → 1); golden's hash-chunked parts each span every id, so nothing more prunes |
| fewer parts (merge policy) | #110 OPTIMIZE arm: −8 ms node 39 | owner decision 2026-10-16: default merge policy |
| the API side | in process, (request − statement as the client sees it) ≈ 2 ms | not the lever |

### 2.3 The changes

1. **Lean statements** (`790b862`, `e29c43c`, `3cf00b3`; `lean-sql.ts`). Every data statement is sent through `leanSql`, a
   rewrite into the same query with fewer nodes, four rules applied until none applies:
   - a parenthesised `SELECT * FROM (X) WHERE 1` or `SELECT * FROM (X)` becomes `X`;
   - a view-like read (`SELECT * FROM t WHERE V`, extra columns of `t` after the star) is merged into the query reading
     it: `SELECT L FROM t WHERE (V) AND (C)`. Refused when the inner query has anything but its WHERE (ORDER BY, LIMIT,
     GROUP BY, JOIN, FINAL, SETTINGS, a set operation), when the subquery has an alias, when a star of `L` sits beside
     extra inner columns, or when an alias of `L` is named in `V` (it would capture the name);
   - a projection of plain columns of a subquery of plain columns reads the subquery's source itself (not over a set
     operation; the outer's ORDER BY / LIMIT only over an inner without its own);
   - an IN argument that reads tuple element n of a level's array through two subqueries reads
     `tupleElement(arrayJoin(level), n)` directly (only as an IN argument; as a FROM source its column name is used).

   Values are bound as parameters, so the text is the compiler's own; quoted text is skipped when matching parentheses.
   Memoised per statement text (~1 ms for a 10 kB statement once). `CHAINGRAPH_API_LEAN_SQL=off` sends the statements as
   compiled. 64 node-2 statements: 506 kB → 441 kB of SQL.
2. **Key texts for small requests** (`53428f5`). Group numbers (fix pass 10) shrink large results but add a final SELECT
   of `transform()` over every level; a request admitted small now compiles with key texts (byte-identical answers).
3. **Unbounded levels without a window** (`75cc5c0`). A level's `__rn` only orders a parent's children for the client
   unless a per-parent limit or offset bounds it; unbounded, no order → a constant, one ascending integer order term →
   that value (NULLs last, as ORDER BY), otherwise the window as before.

### 2.4 Measured

**Direct**, the statements as the API sends them, interleaved, `max_threads 8`, 10–12 rounds:

| change | 22 median-band node-2 statements | all 64 node-2 statements |
|---|---|---|
| key texts (group numbers off) | median −1.4 to −1.75 ms (ratio 0.965–0.971) | |
| lean statements, rules 1–3 (group numbers on) | | median −4.6 ms, ratio 0.905; **none slower**; rows identical |
| + rule 4 | −1.4 ms (0.971) | |
| + unbounded levels without a window | −1.0 ms (0.980) | |

Node-1 heavy synthetic documents in process (default threads, group numbers on), base / head, two rounds: L35 731 / 700,
716 / 693 ms; L33 637 / 581, 604 / 594; X7 285 / 265; L15 1,253 / 1,238; L8 505 / 476; L4 139 / 124; L30 183 / 166, 177 /
183. None slower beyond noise.

**Through HTTP** (the kit's catalogue runner, `build/api/main.js`, admission on, 2 workers × 1 lane thread, cache off, 9
warm runs), base `36a9dfe` against the head, interleaved, both nodes:

| round | node 2 median base → head | node 2 per-document ratio | node 1 median base → head | node 2 equal |
|---|---:|---:|---:|---|
| 1 (head before rule 4 and the windowless levels) | 57.1 → 49.3 ms | 0.913 | 52.1 → 46.4 | 63 / 63 both |
| 2 (the same) | 59.8 → 50.6 | 0.912 | 52.4 → 44.5 | 63 / 63 both |
| 3 (final head) | **61.1 → 48.0** | **0.865** | 56.1 → 46.2 | 63 / 63 both |
| 4 (final head) | **59.1 → 48.3** | **0.857** | 52.4 → 45.0 | 63 / 63 both |

No node-2 document slower than base by > max(5 ms, 10 %) in any round. On node 1 single heavy documents swung both ways
between rounds (L35 739 → 4,140 ms in round 1, 743 → 1,785 in round 4; X7, L28, L23, L33 in others): `query_log` shows 4 GB
read from disk in those runs and none in the base runs, the Docker VM's page cache evicted by the catalogue's other
heavy reads; in process with a warm cache the same documents are equal or faster (above).

Node 39's median band, final head (rounds 3 / 4, ms) and the expected mainnet value (#110 × the local ratio):

| doc | base | head | ratio | #110 | expected |
|---|---:|---:|---:|---:|---:|
| X2 | 30 / 30 | 25 / 25 | 0.84 | 64 | 54 |
| L2 | 66 / 60 | 48 / 48 | 0.77 | 67 | 51 |
| L38 | 57 / 59 | 53 / 51 | 0.89 | 73 | 65 |
| L24 | 55 / 55 | 48 / 48 | 0.88 | 73 | 64 |
| L9 | 67 / 65 | 50 / 51 | 0.77 | 76 | 58 |
| L21 | 61 / 62 | 45 / 44 | 0.72 | 80 | 57 |
| L11 | 70 / 58 | 51 / 50 | 0.80 | 82 | 66 |
| L29 | 67 / 64 | 54 / 53 | 0.82 | 83 | 68 |
| L8 | 70 / 59 | 47 / 47 | 0.73 | 86 | 63 |
| L17 | 71 / 62 | 50 / 51 | 0.77 | 90 | 69 |

### 2.5 Expected on mainnet

Each of #110's 63 documents scaled by its own local head / base ratio (median of rounds 3–4), then the median:

| | #110 | expected: ratio on the whole HTTP p50 | conservative: ratio on the statement only (qlog wall), the API side kept | paired median ratio |
|---|---:|---:|---:|---:|
| **node 39** | 73.4 ms | **58.5 ms** | **60.5 ms** | 1.69–1.80 (≤ 2.10) |
| **node 1** | 74.7 ms | 68.1 ms | 68.3 ms | 1.64–1.69 (≤ 2.30) |

Why the local ratio should hold or improve on golden: the saving is planning and analysis per query node, CPU on the
statement's main thread, and golden's per-scalar gaps are ~1.5× the local ones (§2.1) on the same statements. What could
erode it: golden's reads are a larger share of a level (more parts per read), so the fixed cost saved is a smaller
fraction. The conservative column already keeps the HTTP and API part of each document at #110's value. **Node 39 ≤ 65 ms
with a margin of ~4.5–6.5 ms; node 1 ≤ 80 with ~12.** The part layout (`PL`) still moves every small document by a few ms
(#110's OPTIMIZE arm); a layout like #110's or #107's is assumed.

## 3. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 17 tsdoc warnings of `cb2364d`) |
| unit (`ava --match='!*[e2e]*'`) | 210 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | **183 passed** |
| API specs `build/api/*.spec.js` incl. the chipnet catalogue spec (`ch1_fix14_chip`, v1 Hasura) | **109 passed** |
| chipnet catalogue (`catalogue-run --latency 9`, `ch1_fix14_chip`) | **63 / 63 equal** (48 + 15) |
| golden-like catalogue through HTTP, admission on (kit `ch1-api-catalogue.mjs`) | **63 / 63 equal** on node 2 in every run (48 + 15); node 1 synthetic v2-only works |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed each (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed (126 ClickHouse-only skipped) |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533` and the ch1-pg Postgres
(`CHAINGRAPH_E2E_POSTGRES_HOST`/`_PORT`, which the ClickHouse e2e needs as well).

## 4. Design calls

| Call | Rationale |
|---|---|
| Small by rows per selected part, measured from `query_log` | thread-independent like rows; the only feature that separates golden's scans from its many-part documents |
| 100,000 rows per part | near the geometric middle of golden's gap (37.5 k L6 … 305 k X2) |
| Parts unknown → fix 13's rule (small by rows) | X13 / L34 keep their 8 threads from the second request; the class changes once, when the parts arrive |
| A rewrite of the sent statement, not of every SQL generator | one place, four checked rules, memoised; the compiler's statements stay as they are (specs, the off switch, the A/B) |
| Refuse rule 2 on any alias capture, inner clause or extra-column star; rule 4 on set operations | each is a case where merging could change a column's meaning or the result's shape |
| Rule 3 only as an IN argument | as a FROM source the column name of the link is read (an early text-level draft broke X6 / X13 / L34 that way) |
| Key texts for small requests only | group numbers save the API time on large results (fix-pass-10.md §1); small results have no API cost to save |
| Window only for bounded levels or non-integer orders | the client sorts by `__rn`; an ascending integer order term is its own number |
| No settings change, no thread change, no merge policy | none beat the noise (§2.2); merge policy is the owner's decision |
| Expected value from #110 × the local per-document ratio | the same method as fix passes 12–13; the conservative variant scales the statement only |

## 5. What the next GKE session should verify

1. **C1, absolute and paired, both nodes:** v2 p50 median **≤ 80 ms (node 1) and ≤ 65 ms (node 39)** — expected ~68 and
   ~58.5 (≤ 60.5) — **and** median per-document v2 / v1 ≤ 2.30 / ≤ 2.10 with no document regressed against #107 (ratio
   > 1.25 × #107's and v2 > max(20 ms, 20 %) slower); equality as before (X11 / X6 known). Report PL beside it.
2. **X2:** node 1 X2 back near #107's 47 ms (≤ #107 + max(5 ms, 10 %)); L41 / L43 / X14 / X15 and node 39's X2 / X4 / X11
   reported.
3. **SM:** every admitting pod's startup line has `small<=3000000 rows,<=100000 rows/part:max_threads 8`.
4. **LS (reported):** the captured catalogue statements are lean (no `) WHERE 1)` wrapper left).
5. **Optional, if C1 node 39 misses:** the node-39 catalogue with `CHAINGRAPH_API_LEAN_SQL=off` (the A/B of the main
   change).
6. **Regressions:** everything #110 judged (U1–U5, S39, SP, A1–A6, L27, L17K, L3335, X5R, X6R, L15S, WU, SF), the soak's
   CPU per request (P5), the heavy documents (L15, L16, L33, L35, X6, X13) against #110.

## 6. Commits

| Commit | |
|---|---|
| `3e51984` | perf(api): a small request also reads at most 100 k rows per selected part |
| `790b862` | perf(api): send data statements with fewer query nodes (lean SQL) |
| `53428f5` | perf(api): small requests match relationship levels by key texts |
| `e29c43c` | perf(api): lean SQL rule 4: plain-column projections read the source |
| `75cc5c0` | perf(api): unbounded relationship levels number rows without a window |
| `3cf00b3` | fix(api): lean SQL rule 4 leaves a subquery with a set operation alone |
| `3bc29df` | test(api): inputs-projection matchers accept lean statements |

## 7. Housekeeping

`ch1_fix14_chip`, `ch1_fix14_syn`, the Hasura container `ch1-fix14-hasura` and its metadata database
`ch1_fix14_hasura_meta`, the worktree `/tmp/fix14/wt-base` and the spec scratch databases removed; every API process
started for the measurements stopped (by PID).
