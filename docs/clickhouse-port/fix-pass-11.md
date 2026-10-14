<!-- cspell:ignore chipnet paryon subquery subqueries analysed inlined inlinable arrayJoin PUSD prewhere -->

# Fix pass 11: small-document latency, X5, L35, the small-node totals, cold start

Local only (2026-10-14): ch1-local ClickHouse 26.8.22.13 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` (container `ch1-fix11-hasura`, port 18084) over `chipnet_pg` for v1. Base: `06801a3` (fix
pass 10 + its run log). Input: the fix10 mainnet session (#105, 14 PASS / 5 FAIL), its `qlog-paryon.tsv`, `verdict.md`,
`capture-paryon.jsonl` and the X4 / L17 traces. Scratch databases: `ch1_fix11_syn` (`synthetic/build.sh … golden`:
150 M transactions in golden's part layout, plus a second node accepting the last 2.8 M transactions, golden node 39's
size, and the stored UTXO set of both) and `ch1_fix11_chip` (a copy of `chipnet_ch_pp2` in the current DDL, authchain
index backfilled, UTXO set built). Raw results stay local (`/tmp/fix11/`). Every timing is with `NODE_ENV=production`.

**No data discrepancy.** The chipnet catalogue is 63 / 63 equal against v1 (48 equal, 15 equal-unordered, as before) at
every commit it ran on; every spot check of the synthetic second node is equal to the stored set (3,505 categories,
7,961,478 outputs, digest 885a60ec42c9fb48) in every form and concurrency tried.

## 1. Small-document latency (main item)

### Where a small document's time goes

**Mainnet (fix10 qlog + phases).** For the documents around the catalogue median (L3, L9, L14, L21, L29, X3, X9, X12:
70–180 ms through HTTP), the statement is 90–95 % of the request: HTTP total − statement ≈ 11 ms, of which ~7 ms is the
request snapshot's watermark query (L33 phases: before 5, snapshot 7 ms). Inside the statement, reads are small
(0.05–2.8 M rows, 0.1–0.3 CPU-s) and QueryPlanOptimize alone is 25–70 ms: the statement is mostly fixed cost. The X4
trace shows the shape: ~5 ms of analysis before each level, plus two full scans of `block` (1.05 M rows, ~10 ms each)
for one `block_inclusions { block { … } }`.

**Locally (synthetic, golden layout; real-time profiler on the main query thread, L14).** ~60 % of the thread is inside
the scalar-subquery evaluations (per level: planning, `QueryPlan::optimize` — projection selection alone 45 % of it —
pipeline build, waits for the read); ~20 % is analysis outside them; ~10 % destroying interpreters and trees. Settings
change little: `allow_statistics_optimize = 0`, `use_statistics = 0`, `use_query_condition_cache = 0` within noise;
`max_threads = 8` −3 to −8 ms on most small documents but +10–40 ms on the data-reading ones (L2, L3, L9); `max_threads
= 4` worse still. Not adopted (§6).

What the cost follows is the number of **table reads and joins** per statement, not the number of levels as such
(local: ~4–5 ms per read; mainnet ~1.5–2×). An experiment that inlined the spent outputs of `inputs { outpoint }` into
the inputs level (one level fewer, one join and one more read) was 4–7 ms *slower*; the same move for the block level
(two full block scans and a scalar array fewer) was ~30 ms faster.

Structural findings, per pattern:

| Pattern | Cost (synthetic) | Documents |
|---|---|---|
| `block_inclusions { block { … } }`: the inclusions' scalar array, the order join (three references to the array, a block read) and the block level (a second block read by id: a full scan) | ~40 ms of L1's 98 ms (L1 without the selection: 58 ms) | L1, L4–L7, L10, L11, L14–L17, L21–L23, L26, L28–L30, L33–L36, L39, X3–X6, X9, X12, X13 |
| The page re-checks the conjuncts its candidates come from (`hash` lists, `has_output`, `has_input` via the spend relation) | one output / spend read + 3–5 subqueries each | every `transactions` page with candidates |
| Candidates of several output-side conjuncts as an `INTERSECT` | mainnet L1: every PUSD minting output read (2.8 M rows) to keep 7 | L1, L18, L27, L29 |
| `tupleElement(<scalar>, i)` folds into a constant holding the whole column; the analyzer hashes the constants of a subquery's tree at every resolution | 2 MB hashed per page reference for a 60 k-candidate window | L33, L35, every windowed page |
| The request snapshot's watermark query before the statement | 4.2 ms locally, ~7 ms on mainnet | every request |

### Fixes

| Commit | Change |
|---|---|
| `6897e4e`, `5bb7497`, `d58cd40` | **Inlined to-one rows.** A relationship marked `inline` whose selection is fields of the target (and the target's own relationships) is decoded from the parent level's tuple under a column prefix, with no level of its own; a marker column (`__present`, 1 on a joined row, 0 from a LEFT JOIN miss) makes a missing row null. `BlockInclusion.block` is inline: the inclusions level joins its blocks (every inclusion's, not only multi-inclusion transactions'), so `block_inclusions { block { … } }` is one level and one block read instead of three levels and two block reads. Never at the root (a root's source provides no prefixed columns) |
| `11820f5`, `99b54d2` | **Implied conjuncts.** `candidatesImplied` mirrors `transactionCandidates`; `transactionPredicate(…, implied)` leaves out the conjuncts the candidates come from (output-side: selective `hash` and `has_output`, also in `_and` parts; or the `has_input` spend relation; or the `_and` part that produced the candidates). Same predicates over the same pinned views, so exact |
| `fce1d3e` | **Candidates as a chain.** Several output-side sets are intersected as a chain: each set read among the transactions of the ones before it (`transaction_hash IN …`), so its read can take the table's key instead of a whole category |
| `9b2792f` | **The candidate window as one array of `(hash, id)` tuples** read by `arrayJoin`; `capped` is `length(window) = k` |
| `2838800` | **Speculative request snapshot.** A query runs on its cached snapshot while the watermark check runs beside it; the result stands when the check (read after the request started) finds the cached marks, the same condition the checked path applies before using it. Otherwise the stale cache entry is dropped and the request runs again on a checked snapshot (`stats.speculativeRetries`). `CHAINGRAPH_API_SPECULATIVE_SNAPSHOT=off` restores the checked path. Subscriptions keep their preset snapshots |

### Before / after (local)

Statement wall from `system.query_log` (synthetic, interleaved, median of 5–7 runs after 2 warm-ups; base `06801a3`
captures vs head captures of the same documents):

| Doc | base | head | | Doc | base | head |
|---|---:|---:|---|---|---:|---:|
| L1 | 88 | **53** | | L22 | 100 | **71** |
| L2 | 87 | 71 | | L24 | 174 | **96** |
| L3 | 43 | 38 | | L26 | 47 | **30** |
| L4 / L5 | 148 / 156 | 119 / 124 | | L29 | 76 | **44** |
| L6 / L7 | 88 / 53 | 67 / 35 | | L31 / L36 | 49 / 44 | 36 / 34 |
| L9 | 68 | 58 | | L34 | 89 | 80 |
| L10 | 204 | 172 | | L39 | 55 | **34** |
| L11 | 58 | **38** | | X3 / X4 / X5 | 79 / 29 / 72 | 64 / 20 / 59 |
| L14 | 62 | **41** | | X6 | 474 | **345** |
| L18 / L27 | 31 / 28 | 19 / 17 | | X9 | 77 | **53** |
| L21 | 69 | **51** | | X12 / X13 | 55 / 404 | 42 / 358 |
| L23 / L28 | 344 / 328 | 220 / 225 | | L33 / L35 | 552 / 621 | 468 / 585 |

Unchanged within noise: L12, L13, L19, L20, L25, L38, L40–L47, X1, X2, X7, X8, X10, X14, X15 (no inclusions, no
candidates, or block roots). In process, per request (median of 9): L1 98 → 56 ms, L14 71 → 47, L21 78 → 54, X4 37 → 25,
L40 48 → 40, L31 52 → 38, X9 84 → 57; the snapshot round trip is now beside the statement (total ≈ statement + 2 ms).

**Chipnet catalogue** (`catalogue-run --latency 9`, v1 Hasura, `ch1_fix11_chip`): **63 / 63 equal**, v2 p50 median
**56.0 → 47.0 ms**; no document slower by more than max(20 ms, 20 %); faster by more than that: L1 119 → 67, L14 143 →
71, L29 94 → 58, L6 115 → 76, L22 99 → 68, L11 77 → 50, L21 76 → 49, L5 147 → 97, L4 268 → 227, X6 557 → 447, L33 252 →
209 ms and more.

### Expected on mainnet

Scaling each document's fix10 mainnet statement by its synthetic head / base ratio (documents not measured unchanged),
then taking ~6 ms off every request for the snapshot round trip:

| | fix10 | statement ratio applied | + speculative snapshot | target |
|---|---:|---:|---:|---:|
| node 1 median | 122 | 89 | **~83** | ≤ 80 |
| node 39 median | 89 | 76 | **~70** | ≤ 65 |

Rationale and uncertainty: mainnet's per-read cost is larger than the synthetic copy's (more marks per projection, a
1.3 M-row block table scanned per inclusion level), so the absolute saving per removed read should be larger there, not
smaller; and the candidate chain removes mainnet L1 / L29's 2.8 M-row category reads, which the synthetic mapping does not
reproduce (L1 is projected at 108 ms, likely ~70–80). Node 1 ≤ 80 is therefore plausible but not assured; node 39 ≤ 65
is unlikely without more (§7). The judged median's composition matters as fix10 found (L16 / L17 now answer).

## 2. X5 on node 1

**Cause.** Not fix 9's inputs-level change: X5's statement is the same in #104 and #105 apart from key texts. X5's
`spent_by` selects `unlocking_bytecode`, which `input.p_outpoint` lacks, so the spenders were read from `input` by the
outpoint alone: a bloom-filter read of every part. Its rows depend on the part layout (#104 15 input parts, #105 10;
node 1 2.58 → 7.42 M, node 39 14.06 → 8.24 M: both directions).

**Fix** (`d9aca25`). A `spent_by` level reading a column outside `p_outpoint` first finds the spenders' hashes through
`p_outpoint` (slim, every needed column), then reads the table by its key. Synthetic X5: 0.74 M → **0.00 M** rows (a
handful of granules), 72 → 59 ms. Expected on golden: X5 ≈ 1 M rows (the two block reads left) and ~130–140 ms on node 1.

## 3. L33 / L35

Statements (synthetic, 10,000-row pages): L33 552 → 468 ms, L35 621 → 585 ms (window constants, inlined block,
implied `has_input`); in process per request: L33 713 → 638 ms, L35 856 → 706 ms. Rows read rise for L35 (64.6 → 74.4 M:
every inclusion's block is joined now and the inclusions are read twice) while time falls. Expected on mainnet: L35's
statement ~560–600 ms (from 653) and ~7 ms less before it; with the API side unchanged (343 ms) **L35 ≈ 0.95–1.0 s**
through HTTP: about the 120 ms asked for, not with margin. The API side was not reworked in this pass; a remaining lever
is the outpoint level (§7).

## 4. Small-node totals (S39)

The synthetic second node has golden node 39's size (2.8 M transactions, 7.96 M UTXOs; 8 chunks by the existing
sizing). Each chunk took ~1.8 s, 43.6 M rows, ~13 CPU-s, ~0.5 GiB: mostly the node's outputs read by key, one 128-row
granule per transaction.

- **Tried and reverted** (`367d063`, superseded by `3c7b283`): the acceptance read once per chunk as one tuple scalar.
  `tupleElement` of it folded into a constant holding the node's whole id column, hashed at every resolution: 14.5 →
  23.5 s. Recorded as a design call: big scalars are read by `arrayJoin`, never through `tupleElement`.
- **Fix** (`3c7b283`): the node's ids as one array scalar (read by `arrayJoin`, `arrayMin` / `arrayMax` as bounds: two
  acceptance reads per chunk instead of three), and **two chunks at once** (`smallNodeConcurrency = 2`, option
  `totalsConcurrency`). Synthetic node: 14.4 s → **11.1 s** (4 at once: 10.5 s), CPU-bound on 14 cores; equal.

Expected on golden (48 ClickHouse cores, each chunk far from using them all): 13.7 s → **~8–10 s**.

## 5. Cold start (U3)

**Cause.** The first two AllHolders after the API started took 102 / 110 ms, the third 26 ms; a cold-cache L12 (every
cache dropped) is 93 ms. The first requests of a fresh process find ClickHouse's caches cold for their tables and
projections, the admission model without a cost (unknown = heavy, charged the default), and V8 cold.

**Fix** (`94449d6`, `9edcb1a`): `CHAINGRAPH_API_WARMUP=<file>` runs a list of requests (`{ query | documentId,
variables }`; a catalogue soak-mix file serves) through the in-process server **before listening**, `…_ROUNDS` (1) times,
bounded by `…_TIMEOUT_MS` (120 s); failures are logged, never fatal; a missing file is no warm-up. The startup log gains
`warm-up: <ok> ok, <failed> failed, <skipped> skipped of <n> entries in <ms> ms`. Locally 7 entries in 327 ms; afterwards
`GET /admission` lists their documents with estimates. Persisting cost history was not chosen: a blue/green pod has no
history, and the warm-up also warms the caches and the code.

## 6. Kit / server notes

- **`max_concurrent_queries` ≥ the backstop:** the local Docker server runs the image's `config.xml` value, **1000**
  (`system.server_settings`), above any local backstop; no local override is needed. The kit manifest's 500 (fix10) is
  carried into ch1-fix11.
- Suites at the final code (§8).

## 7. Tried and dropped; what is left

| Option | Result |
|---|---|
| `inputs { outpoint { … } }` inlined (outputs joined into the inputs level) | 4–7 ms slower (a join plus a second input read cost more than a key-read level); the generic join hook removed |
| `block.p_id` projection (point reads of blocks by id) | X4 / X9 rows fell, but L43 28 vs 17 ms and X14 36 vs 27 ms (sets built before reading, as fix-pass-8b found); dropped again |
| `max_threads` 4 / 8 for small statements | mixed (−3 to −8 ms on most, +10–40 ms on data-reading documents) |
| `optimize_use_projections = 0` on key-read levels | no measurable change |
| `allow_statistics_optimize`, `use_statistics`, `use_query_condition_cache`, `enable_scalar_subquery_optimization` = 0 | within noise (the last one slower on large windows) |
| the per-chunk acceptance as one tuple scalar | 14.5 → 23.5 s (constant folding); reverted |

Left: the per-level planning cost itself (projection selection per read is the largest single piece); the block table
scan per inclusion level (a lookup of blocks by id without a full scan, without p_id's side effects); the outpoint level
of L33 / L35 (a key read per spent output, 172 ms of L35's synthetic statement); L35's API side.

## 8. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 17 tsdoc warnings, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 202 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | 181 passed; `g1-fix`'s two lab-conditions tests missed their lease deadline in the parallel run (as in fix 10); `g1-fix.spec` alone 12 / 12 |
| API specs `build/api/*.spec.js` incl. the chipnet catalogue spec (`ch1_fix11_chip`, v1 Hasura) | **97 passed** (fix 10: 93 + 3 warm-up unit tests + the speculative-snapshot test). The snapshot-contract tests count a speculative retry as one more snapshot; the fix-pass-3 cache test and the stored-UTXO-set path test run the checked path explicitly |
| chipnet catalogue (`catalogue-run --latency 9`) | **63 / 63 equal**, median 56.0 → 47.0 ms |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed each (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed, 1 skipped |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`.

## 9. Design calls

| Call | Rationale |
|---|---|
| Inline only to-one rows whose selection the parent can carry, decided per request | the parent's source joins only when the selection asks for it; an unselected block costs nothing |
| Inline `BlockInclusion.block`, not `Input.outpoint` | measured: removing block reads pays, adding a join to a key read does not |
| A marker column for an inlined row's presence | a LEFT JOIN fills defaults (0, empty), never NULL; a key of 0 would read as present |
| Leave out implied conjuncts only where `transactionCandidates` took them | the candidates are exactly those conjuncts' rows over the same views; nothing else is assumed |
| Candidates intersected in document order as a chain | the first set is read in full as before; each later one gets the key path; no worse than the INTERSECT, which read every set in full |
| Speculative snapshot with retry, default on, off switch | keeps "never staler than the watermarks at request start" for every answer delivered; a commit costs one retry burst, after which the entry is dropped and refreshed by the checked path |
| Big scalars read by `arrayJoin`, never through `tupleElement` | `tupleElement` of a scalar folds into a constant of the whole column, hashed at every resolution |
| Two small-form chunks at once | halves the hourly check's wall time for twice the momentary memory (~1 GiB) and CPU; 4 at once gained little more locally |
| Warm-up file before listening, not persisted cost history | a fresh pod has no history; the warm-up also loads ClickHouse's caches and V8's code for the documents that matter |
| No `max_threads` change for light documents | mixed locally; the light class (predicted CPU) does not tell a data-reading small document from a planning-bound one |

## 10. Commits

| Commit | |
|---|---|
| `c6df17e` | fix(measure): synthetic capture / probe create the API with the full disabled admission options |
| `9b2792f` | perf(api): candidate window as one array of (hash, id) tuples read by arrayJoin |
| `11820f5` | perf(api): page leaves out the output-side conjuncts its candidates come from |
| `6897e4e` | perf(api): block inclusions join their blocks; BlockInclusion.block inlined |
| `d9aca25` | fix(api): spent_by reading columns outside input.p_outpoint finds the spenders' hashes first |
| `367d063` | perf(store): small-node totals read the node's acceptance once per chunk (superseded by `3c7b283`) |
| `94449d6` | feat(api): CHAINGRAPH_API_WARMUP runs listed requests before listening |
| `99b54d2` | perf(api): page also leaves out a has_input conjunct (spend relation) its candidates come from |
| `5bb7497` | perf(api): an inlined row may carry relationships |
| `d58cd40` | fix(api): an inlined row is present by a marker column; never at the root |
| `fce1d3e` | perf(api): output-side candidates intersected as a chain |
| `2838800` | perf(api): a query runs on its cached snapshot while the watermark check runs beside it |
| `3c7b283` | perf(store): small-node totals run two chunks at once; the node's ids as an array read by arrayJoin |
| `0251214` | style(api): cspell words |
| `9edcb1a` | fix(api): warm-up sends only the request fields; a missing file is no warm-up |

## 11. Housekeeping

`ch1_fix11_syn`, `ch1_fix11_chip`, the Hasura container `ch1-fix11-hasura` and its metadata database
`ch1_fix11_hasura_meta`, and the worktree `/tmp/fix11/wt-base` removed; every API process started for the measurements
stopped (by PID).

## 12. What the next GKE session should verify

1. **C1 on both nodes** vs v1 and fix10 (122 / 89 ms): targets ≤ 80 / ≤ 65; expected ~83 / ~70. Per document, the
   inclusion documents (L1, L14, L21, L29, L39, X3, X9, X12) and L1 / L18 / L27 / L29 rows read (the candidate chain).
   Equality as before (X11 / X6 known).
2. **X5 node 1**: rows ≤ ~1–2.6 M (fix10 7.42 M), `input.p_outpoint` in its projections; ~130–140 ms.
3. **L33 / L35 through HTTP**: L35 < 1 s (expected ~0.95–1.0 s), phases: the snapshot phase ~0, statements vs fix10
   653 / 672 ms; L35's rows read (more blocks joined) next to its time.
4. **S39**: small form, two chunks at once, equal, < 12 s, ~8–10 s; peak per statement as before, two statements at once.
5. **U3** with the kit's warm-up (`warm-up: … ok` in every pod's log): L12 < 50 ms on the first requests after a start.
6. **Speculation**: `CHAINGRAPH_API_SPECULATIVE_SNAPSHOT=off` as an A/B arm of the catalogue's node-1 pass (median
   difference ≈ the snapshot phase); no retry on the frozen golden store (the agent does not run).
7. **Regressions:** A1 / A3 admission arms, X6 rows (≤ 10 M), U5 whole-set checks, every spot scope equal.
