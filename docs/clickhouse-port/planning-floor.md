<!-- cspell:ignore chipnet paryon unhex subquery analysed prewhere backfill backfilled materialise materialised parameterised chipcopy PUSD tsdoc UDFs unindexed -->

# The per-statement planning floor (mainnet API, 2026-10-12)

On golden (mainnet, 2026-10-11) every v2 API statement that touched `output` cost about 0.5–1 s before reading data
([api-run-log.md](api-run-log.md), 22:05Z): L25 LoanKeyOwner read 38,918 rows / 37 marks yet took ~470 ms, with
FilteringMarksWithPrimaryKeyMicroseconds 1.53 s (CPU), QueryPlanOptimizeMicroseconds 490 ms (≈ wall) and
FilteringMarksWithPrimaryKeyProcessedMarks 25.5 M. This pass reproduced that locally at mainnet-like mark counts,
found the mechanism, and fixed it in the DDL (keys) with a small compiler change and API time budgets.

## 1. Summary

- **Mechanism.** Primary-key analysis of `IN`-set (and `NOT IN`, `=`) conditions on **non-leading key columns**.
  ClickHouse cannot binary-search those, so it runs a "generic exclusion search" that visits about every mark of
  every part. It does this for the base table and **for every candidate projection while choosing one**, even
  where that projection can never win. Example: a relationship read `output WHERE transaction_hash IN (set)` analysed
  the set against `p_script` (key `locking_bytecode_prefix, transaction_hash, output_index`), about 248k index steps
  per part and 0.13–0.25 s of CPU, and against `p_category` (key `…, transaction_hash, output_index`), 28–58k steps.
  Then it read the base table. The same happened on `spend` (key `locking_bytecode_prefix, transaction_hash, …`),
  `output`'s `output_index NOT IN (set)` (all 3.4 M base marks) and `tx_acceptance`'s `node_internal_id = n`
  (all marks). The cost scales with marks, so mainnet pays about 2.7 times what the local copy pays.
- **Not the cause:** the output granularity of 128 (the projections are at 1024 and dominated), the query condition
  cache, implicit projections, skip indexes and `optimize_use_projection_filtering`. Each was toggled and none
  changed the cost (§4).
- **Fix (DDL; commits in §10):** keys stop at the columns that reads filter on.
  - `output` PRIMARY KEY `transaction_hash`, `tx_acceptance` PRIMARY KEY `transaction_hash`, `spend` PRIMARY KEY
    `locking_bytecode_prefix`. Each table's ORDER BY is unchanged.
  - Projections `output.p_script (locking_bytecode_prefix)`, `output.p_category` and `spend.p_category`
    `(token_category, nonfungible_token_commitment_key)`.
  - A condition on a column that is no longer in the key is "unknown" to the index, which costs nothing.
  - The output granularity stays 128: at 1024 the spend-index filler's outpoint batches take 2.3 times as long, and
    the planning gain after the key change is noise.
- **Compiler:** a commitment `_eq` / `_in` also constrains `nonfungible_token_commitment_key`, so `p_category`
  narrows to the commitment, not the whole category. This matters more once the hash is out of the projection key.
- **Effect at synthetic mainnet scale (37 % of golden's output marks), warm p50:**
  - L25 145 → 25 ms. FilteringMarksWithPrimaryKeyMicroseconds 252 → 3 ms. QueryPlanOptimizeMicroseconds 156 → 32 ms
    (which includes the CTE runs).
  - The 24 statements that spent ≥ 50 ms in key analysis: median 255 → 75 ms.
  - Whole catalogue: key-analysis CPU summed over all statements 8.8 s → 0.6 s, wall summed 20.7 → 15.8 s.
  - Heavy statements are no worse; four small ones gained 6–13 ms (§7).
- **API:**
  - Each statement now carries a `max_execution_time` for its class: data 60 s (`CHAINGRAPH_API_QUERY_TIMEOUT_MS`),
    aggregates 30 s (`CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS`). Over budget, the API answers `QUERY_TIMEOUT`.
  - A client-side timeout is no longer retried, and reads cancel on client close.
  - L15/L16 read `spent_by(accepted_by: $node)`, as v1 does.
- **Owner decision:** the DDL change applies to new databases. An existing database (golden) keeps its keys
  (`IF NOT EXISTS`) and the DDL CLI now warns about it. The projection half can be rebuilt in place; the primary keys
  need new tables (§8).

## 2. Mechanism

A MergeTree read analyses its key condition against the part's primary index before it reads anything:

- **Leading key column constrained** (`transaction_hash IN (set)` on a hash-keyed table): binary search, or a
  generic exclusion search whose steps grow with the set size × depth.
- **Only a later key column constrained** (`output_index NOT IN (set)` on `(transaction_hash, output_index)`): no
  range ever has a fixed leading column. The search splits every range down to single marks: steps ≈ marks per part.
  On these tables such a condition also excludes nothing. A transaction's rows share a granule, and rows of every
  node interleave per hash.
- **Projection selection** (`optimizeUseNormalProjections`) repeats this analysis for **each** projection that has
  the needed columns, i.e. all of them (full-column projections), and only then compares marks.
  - `output` has two such projections and `spend` has one.
  - Every hash-set read of `output` paid for both: about 248k steps per part for `p_script`, whose leading column
    almost never repeats, and 28–58k for `p_category`, whose zero category gives long runs where the hash is bounded.
  - Their marks are 1,024-row granules: 421k per projection locally, about 1.1 M on golden.
- **`identity()` does not hide a column from projection analysis.** The compiler's `unindexed()` wrapper hides a set
  from the base table's key. In the projection's re-analysis (`_projection_filter`) the `identity` is gone.
  - Trace, `token_category = X AND identity(transaction_hash) IN (300 hashes)`: base `Key condition: unknown,
    unknown`; `p_script` `(column 1 in 300-element set)`, 247,970 steps per part, 119 ms.
  - `toFixedString(toString(h), 32)` hides it from both, but the key change removes the need (§6).
- `SETTINGS` inside a subquery do not reach projection selection. A subquery with `optimize_use_projections = 0`
  still had `p_category` analysed, so a statement cannot switch projections off for one of its reads.

The counter to watch is **FilteringMarksWithPrimaryKeyMicroseconds**, not FilteringMarksWithPrimaryKeyProcessedMarks.
The latter also counts marks that the "unknown condition" fast path passes for free: it stayed at 9.4 M for L25 while
the CPU fell from 252 ms to 3 ms.

## 3. Reproduction

Scratch database `ch1_floor_g128` on `ch1-local` (26.8.22, Docker VM 14 CPUs). It used the DDL as of `2022166`,
filled by `INSERT … SELECT` from `numbers()` with deterministic UDFs (`floor_*`, dropped afterwards):

| | rows | marks (base) | golden rows | note |
|---|---:|---:|---:|---|
| transactions | 150 M | – | 420 M | tx hash `SHA256('tx'‖t)`; outputs per tx from {1,2,2,2,3,3,4,6} (mean 2.9), inputs mean 2.5, 1 in 300 coinbase |
| output | 431 M | 3.37 M (granularity 128) | 1.16 B (9.07 M) | 60 % P2PKH / 35 % P2SH / 5 % OP_RETURN; addresses: 20 % from 1,000 hot ones, 40 % from a 10 M pool, 40 % unique; 2 % token outputs over 5,000 categories, 30 % of them in 5 hot categories (`cat0` stands in for PUSD); 40 % of token outputs NFTs |
| input | 374 M | 365k, p_outpoint 2.92 M | 1.04 B | outpoint = a random earlier output |
| spend | 374 M | 365k | 1.03 B | the spent output's attributes, derived from the same UDFs |
| tx_acceptance / transaction / block_transaction | 150 M each | 146k | 420 M | node 1, 330 txs per block |

- Built in 31 min, 270 GiB, 2 parts per big table after merges (golden had 6–9). The key-changed variants had 7.
- The statements are the 64 catalogue statements captured on golden (`capture-catalogue.jsonl`), run unchanged.
  Ids are mapped to synthetic ones: PUSD → `cat0`, other categories → tail categories, contract scripts → hot or pool
  P2SH, P2PKH → a hot or pool address, tx hashes → synthetic hashes. Snapshot parameters are set to cover all
  commits.
- Each statement: 4 runs, p50 of runs 2–4, ProfileEvents from `system.query_log`.
- Traces: `clickhouse-client --send_logs_level trace` (the `Key condition`, `generic exclusion search … steps`,
  `PK index has dropped … took` and `optimizeUseNormalProjections` lines).
- Runner and scripts live in `/tmp/floor` (not committed; raw results stay local).

Reproduced, before any change:

| L25 | golden | synthetic (37 % of the marks) |
|---|---:|---:|
| wall | ~470 ms | 145 ms |
| FilteringMarksWithPrimaryKeyMicroseconds | 1,531 ms | 252 ms |
| QueryPlanOptimizeMicroseconds | 490 ms | 156 ms |
| FilteringMarksWithPrimaryKeyProcessedMarks | 25.5 M | 9.4 M |
| SelectedMarksTotal | 3.69 M | 1.35 M |

L25's trace, per part, made the mechanism plain:

| read | analysis |
|---|---|
| base `output` | `(column 1 notIn 66-element set)`, 1,983,754 steps (all marks), 16 ms |
| `p_script` | `(column 1 in 300-element set), ((column 1, column 2) notIn …)`, 247,964 steps, 249 ms |
| `p_category` | 48 steps (category fixed), selected |
| `tx_acceptance` | `(column 0 in 300-element set), (column 1 in [1, 1])`, 5,032 steps, useful |

L8, a relationship-heavy document, spent 133–175 ms per output read in `p_script` and 45–113 ms in `p_category`, and
115 ms in `spend`'s base key. In every case the base table was the path chosen.

## 4. What was measured (synthetic copy, warm p50)

| candidate | result |
|---|---|
| `merge_tree_generic_exclusion_search_max_steps` = 1,000 | key CPU about 0. But capped useful searches read far more: L15 222 → 4,436 ms, L8 709 → 3,926 ms. Rejected |
| same = 10,000 | L25 145 → 32 ms, L1 275 → 100. But L8 709 → 3,926 and L2 reads 7× the marks. Rejected as a default: useful set searches need 15–90k steps per part |
| `preferred_optimize_projection_name = 'p_category'` | L25 30 ms, L1 67. It only compares the preferred projection with the base table, so a statement that also reads by script would read the wrong projection. Statement-wide only. Rejected |
| `optimize_use_projection_filtering = 0`, `use_query_condition_cache = 0`, `optimize_use_implicit_projections = 0`, `use_skip_indexes = 0` | no change |
| subquery `SETTINGS optimize_use_projections = 0` | ignored by projection selection |
| `identity()` → `toFixedString(toString(h), 32)` in the compiler's `unindexed()` | L25 145 → 32, L13 161 → 41, L12 1,367 → 1,058. Untouched where sets are not wrapped (L8, L22, X6) |
| removing `identity()` (sets visible to the base key) | L12 key CPU 0.4 → 3.0 s, L14 0.5 → 1.0 s. The wrappers stay |
| **projection keys trimmed (output)** | L1 270 → 73, L15 223 → 80, L17 → 45, X6 641 → 337, X5 160 → 114 |
| **+ spend PRIMARY KEY `locking_bytecode_prefix`, p_category trimmed** | L3 181 → 65, L5 255 → 83, L10 257 → 77, L21 227 → 50, L22 466 → 106 |
| **+ output PRIMARY KEY `transaction_hash`** | L8 → 346 (key CPU 436 → 176), L25 → 26 |
| **+ tx_acceptance PRIMARY KEY `transaction_hash`** | L2 191 → 125, L13 38 → 27, L22 → 94 |
| keep `transaction_hash` in the `p_category` keys, trim only `p_script` and the spend base | L14 key CPU 462 ms (vs 91), L9 104 (vs 10), L8 283 (vs 160). Avoids the category-plus-hash-set regressions, but the commitment-key change (§6) recovers most of those. Rejected |
| output granularity 1024 (keys trimmed) | planning within noise of 128: L25 24 vs 26 ms, L8 396 vs 346 ms. The PK in memory is 8× smaller (12.9 vs 103 MiB here) |

Outpoint lookups at 128 and 1024 (keys trimmed, 7 parts each, p50 of 5 warm runs):

| lookup | granularity 128 | 1024 |
|---|---:|---:|
| filler-style batch: outputs of the outpoints of 10,000 txs' inputs (≈ 25k outpoints), `IN` | 997 ms, 1.42 GiB read | 2,315 ms, 2.78 GiB |
| the same as the spend-index join | 410 ms | 1,076 ms |
| one outpoint | 2 ms, 897 rows | 2 ms, 7,169 rows |
| 20 random outpoints | 3 ms, 7,956 rows | 5 ms, 62,484 rows |
| spenders of 20 outpoints (`input.p_outpoint`, 128) | 2 ms | 2 ms |

The spent-output read of the commit path is gone, but the spend-index filler (and its backfill) still reads outputs
by outpoint. Granularity 1024 would make it 2.3–2.6 times slower and buy nothing for planning once the keys are
trimmed, so output stays at 128. `input.p_outpoint` stays at 128: its key leads with the outpoint, so its analysis
costs nothing unless a read filters on the outpoint, which is when it wins.

## 5. Statement budgets and timeouts (API)

Golden Q1 (whole-chain UTXO count) hit the API client's fixed 300 s timeout. The client then **retried** the
statement, so two copies ran on the server until the driver gave up. The direct SQL needed 19.5 min.

- Each data statement carries `max_execution_time` from its class (`ApiDb.query`, `db.ts`):
  - `data`: `CHAINGRAPH_API_QUERY_TIMEOUT_MS`, default 60 s.
  - `aggregate` (`*_aggregate` and `*_groups` roots): `CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS`, default the smaller of
    30 s and the data budget.
- Over budget, ClickHouse stops the statement (TIMEOUT_EXCEEDED) and the API answers a GraphQL error with
  `extensions.code = QUERY_TIMEOUT`. The message names the class and the budget, and says to narrow the query.
- The API's ClickHouse client timeout is the larger budget plus 5 s, so the server's error arrives first.
- `ClickHouseClient.query(…, { retryTimeouts: false })`, used by the API, no longer retries a client-side request
  timeout. Transport resets and transient server codes are still retried. Writers are unchanged.
- `cancel_http_readonly_queries_on_client_close = 1` on data statements: a request the client abandons stops on the
  server too.
- Test: `api-ops.spec.ts`. An aggregate with a 1 s budget over `numbers(1e13)` fails with QUERY_TIMEOUT in < 20 s.
  `db.spec.ts` covers the environment parsing and `client.spec.ts` the timeout detection.

## 6. Compiler

- **Commitment keys.**
  - `nonfungible_token_commitment: { _eq | _in }` compared only the nullable column, so `p_category` narrowed to the
    category alone (L18, L11, L19: commitment equality on PUSD).
  - The compiler now adds the implied `nonfungible_token_commitment_key = unhex(…)` / `IN (…)` (not under `_not`).
  - Effect with the trimmed keys: L11 47 → 32 ms, L17 136 → 43, L19 24 → 14, L18 101 → 34.
- **No per-read projection pinning.** The compiler knows each read's access path. But ClickHouse takes projection
  settings per statement, not per subquery (§2), and `preferred_optimize_projection_name` is statement-wide (§4).
  The key change makes the unwanted analyses free instead.
- `unindexed()` (`identity`) stays. It still keeps large acceptance sets off the base key where a projection is the
  path; without it L12 spends 3 s in key analysis. It no longer has a projection to protect from.

## 7. Before / after at synthetic scale

Same data, same statements. "After" = all key changes plus the commitment key, with output at granularity 128.
Columns: wall ms, FilteringMarksWithPrimaryKeyMicroseconds (key CPU, ms), QueryPlanOptimizeMicroseconds (ms, this
includes running MATERIALIZED CTEs), SelectedMarks. All statements with ≥ 50 ms of key CPU before, plus those that
got slower:

| doc | wall before | after | key CPU before | after | plan opt before | after | marks before | after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| L1 | 270 | 76 | 422 | 2 | 346 | 49 | 1,676 | 2,285 |
| L2 | 312 | 147 | 331 | 28 | 478 | 147 | 13,932 | 7,305 |
| L3 | 181 | 62 | 252 | 9 | 298 | 52 | 3,006 | 1,308 |
| L4 | 187 | 69 | 246 | 8 | 295 | 52 | 3,006 | 2,041 |
| L5 | 255 | 73 | 361 | 8 | 421 | 56 | 3,738 | 2,045 |
| L6 | 105 | 52 | 127 | 2 | 154 | 37 | 420 | 392 |
| L8 | 709 | 340 | 848 | 160 | 552 | 113 | 25,313 | 19,689 |
| L9 | 301 | 83 | 455 | 10 | 459 | 71 | 3,377 | 2,102 |
| L10 | 257 | 69 | 390 | 9 | 438 | 56 | 2,897 | 1,613 |
| L11 | 169 | 32 | 249 | 1 | 281 | 22 | 1,529 | 44 |
| L12 | 1,367 | 1,190 | 420 | 100 | 1,632 | 1,408 | 148,911 | 148,728 |
| L13 | 159 | 27 | 261 | 5 | 177 | 33 | 1,316 | 753 |
| L14 | 1,072 | 776 | 527 | 91 | 1,401 | 823 | 143,425 | 123,550 |
| L15 | 223 | 76 | 312 | 28 | 176 | 34 | 3,130 | 2,874 |
| L17 | 201 | 43 | 312 | 5 | 179 | 18 | 2,084 | 667 |
| L19 | 78 | 14 | 110 | 1 | 130 | 11 | 1,067 | 34 |
| L21 | 227 | 52 | 356 | 3 | 386 | 36 | 1,213 | 1,106 |
| L22 | 466 | 90 | 736 | 12 | 695 | 63 | 5,019 | 2,950 |
| **L25** | **145** | **25** | **252** | **3** | **156** | **32** | 610 | 363 |
| L27 | 221 | 75 | 315 | 19 | 280 | 64 | 3,428 | 3,431 |
| L29 | 289 | 67 | 468 | 2 | 366 | 43 | 1,676 | 2,285 |
| X5 | 160 | 106 | 101 | 0 | 57 | 7 | 9,293 | 9,297 |
| X6 | 641 | 271 | 783 | 3 | 566 | 187 | 980 | 1,041 |
| X16 | 1,680 | 1,770 | 110 | 108 | 928 | 914 | 220,174 | 293,416 |
| L18 | 21 | 34 | 0 | 1 | 15 | 31 | 1,692 | 2,451 |
| L23 | 25 | 31 | 0 | 0 | 9 | 16 | 0 | 11 |
| L24 | 17 | 26 | 0 | 0 | 8 | 18 | 0 | 235 |
| L28 | 26 | 32 | 0 | 0 | 10 | 18 | 0 | 11 |

- **Whole catalogue (65 statements):**
  - Median wall 40 → 34 ms.
  - Summed wall 20.7 → 15.8 s.
  - Summed key-analysis CPU 8.8 → 0.6 s.
- **The four slower statements** filter a hot category or script with a candidate set that is empty on the
  synthetic data:
  - The old projection key pruned the hot category to nothing by hash. Now it reads the category's (or
    commitment's) range.
  - On golden these were 57 ms (L18), 1,957 (L23), 1,001 (L24) and 2,075 ms (L28), dominated by other reads.
- **What is left is useful work:**
  - Set searches on leading key columns: L8, L12, the `transaction` hash sets of X16 (4.3 M hashes: 102k steps,
    which drop nothing).
  - Data reads and the MATERIALIZED CTEs: L12, L14, X12/X13.
  - X16's whole-chain hash set could skip the index (`use_index_for_in_with_subqueries_max_values`). That is worth
    ≈ 0.1 s of 1.8 s and was left alone.

## 8. Expected mainnet effect, and the owner decision

- **Expected effect.** Golden has 2.7× the output marks and 6–9 parts per table (here 2–7).
  - The removed analyses scale with marks, so golden lost more than the local copy did. L25's 1.53 s of key CPU was
    mostly `p_script` / `p_category` / base `output_index` searches over 1.1 M + 1.1 M + 9 M marks, per output read.
  - Expected after: L25 and similar point documents **≤ 50 ms**. The L documents that were 0.5–1.5 s should drop to
    roughly 0.1–0.4 s, plus their data reads.
  - L12, L33 and L35, which read data, keep that cost.
  - The soak's saturation below 10 QPS (≈ 0.5–1.5 CPU-s per statement) should move up by the same factor.
- **What it takes.** Existing databases keep their keys. The DDL is `IF NOT EXISTS` and keys cannot be ALTERed.
  `clickhouse-ddl` now logs a WARNING listing every key that differs (`checkKeys`); results are the same either way.
  - **Projections (in place, the bigger half):** for `output.p_script`, `output.p_category` and `spend.p_category`:
    `ALTER TABLE … DROP PROJECTION p`, then the DDL's `ADD PROJECTION`, then `MATERIALIZE PROJECTION p` (a
    mutation). Locally that was ~5 min per projection for 431 M rows. On golden, figure about 3× that.
    - Between DROP and MATERIALIZE the affected reads scan the base table.
    - Under the blue/green practice, build the new database or cluster with the new DDL instead.
  - **Primary keys (`output`, `spend`, `tx_acceptance`): a new table each.**
    - Either re-backfill into a database created with this DDL, or `CREATE` + `INSERT … SELECT` + `EXCHANGE TABLES`.
      Locally that was ~8 min for 431 M output rows and ~5 min for 374 M spend rows.
    - `spend` and the authchain index are derived, so they can also be refilled.
    - **Owner decision:** the next golden or production database should be built with this DDL. A migration of the
      existing golden is optional. The projection rebuild alone gives most of the gain: L1, L15, X5, X6 and the
      relationship levels.

## 9. What the next GKE session should verify

1. On a database built with this DDL (or golden after the projection rebuild), `clickhouse-ddl` logs no key WARNING.
   `system.projections` shows `p_script ['locking_bytecode_prefix']` and
   `p_category ['token_category','nonfungible_token_commitment_key']`, and `system.tables.primary_key` shows
   `transaction_hash` for output and tx_acceptance and `locking_bytecode_prefix` for spend.
2. In-process catalogue run (node 1 and 39), 9 warm runs.
   - **L25 ≤ 50 ms**.
   - From query_log: FilteringMarksWithPrimaryKeyMicroseconds per statement (expected ≲ 50 ms for the L documents,
     was 0.5–1.5 s) and QueryPlanOptimizeMicroseconds.
   - Ignore FilteringMarksWithPrimaryKeyProcessedMarks (§2).
3. Equality: 63/63 against v1 as before; L15/L16 now equal on node 1 (spent_by accepted by the node).
4. Heavy statements no worse: L12, L33, L35, X12, X13, X16, and the L18/L23/L24/L28 group (category or script plus
   candidate set; expected within ±10 % or ±20 ms).
5. The spend-index filler's batch time is unchanged (output granularity unchanged; the spend table's key now has
   only the prefix, and the filler writes it, not reads it).
6. The soak (cache off) at 10 and 20 QPS. Saturation should move well above 10 QPS.
7. Q1 through the API fails in ≈ 30 s with `QUERY_TIMEOUT` (aggregate budget), runs once in `system.query_log` (no
   retry), and the server stops it (no orphan statement in `system.processes`).
8. A trace (`send_logs_level = trace`) of one L document shows no `generic exclusion search` over `p_script`,
   `p_category`, `spend` or a full-mark search of `output`/`tx_acceptance`.

## 10. Commits and suites

| commit | change |
|---|---|
| `2dedb16` | API: per-class statement budgets (`CHAINGRAPH_API_QUERY_TIMEOUT_MS`, `CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS`), QUERY_TIMEOUT, no retry of timed-out reads, cancel on client close |
| `9e74c7a` | L15/L16 documents: `spent_by(accepted_by: $node)` |
| `8badf30` | compiler: commitment `_eq`/`_in` add the `nonfungible_token_commitment_key` condition |
| this document's commit | DDL keys (`010`, `020`, `060`, `070`, README); `ddlKeys` / `checkKeys` and the DDL CLI warning; this document |

Suites (final code; scratch databases dropped afterwards):

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 15 tsdoc warnings as before, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 178 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 169 passed (incl. `checkKeys` e2e and the DDL key test) |
| API specs `build/api/*.spec.js` | 65 passed, 3 skipped (the chipnet spec without its env) |
| chipnet spec (`CHAINGRAPH_API_CHIPNET_DATABASE=ch1_floor_chipcopy`: a copy of `chipnet_ch_pp2` into the new DDL, authchain index backfilled; v1 = Hasura over `chipnet_pg`) | 3 passed: 63/63 documents equal (incl. L15/L16 with `spent_by(accepted_by:)`); R13/R14 EXPLAIN (sort-key projections still chosen); R25/R28 |
| e2e ClickHouse, UTXO on / off | 47 / 47 passed (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed |
