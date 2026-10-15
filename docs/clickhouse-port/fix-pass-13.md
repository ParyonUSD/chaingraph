<!-- cspell:ignore chipnet paryon qlog txid analysed behaviour subquery prewarm PREWARM coldch syn covariate tsdoc prewhere worktrees noshared oldrec -->

# Fix pass 13: small-lookup gap, L17's key CPU, the random-prefix scope, cold heavy documents, X7

Local only (2026-10-15): ch1-local ClickHouse 26.8.22.13 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` (container `ch1-fix13-hasura`, port 18085) over `chipnet_pg` for v1. Base: `cb2364d` (fix
pass 12; head `d131a98` with the #107 run log). Input: the Phase 2 close-out (ledger #108) and the fix12 mainnet session
(#107): its catalogues (judged and the small-thread A/B), `qlog-*.tsv`, traces, captures and sizes, compared with #106's.
Scratch databases: `ch1_fix13_chip` (a copy of `chipnet_ch_pp2` in the current DDL, authchain index backfilled, UTXO set
built) and `ch1_fix13_syn` (`synthetic/build.sh … golden`, 150 M transactions as node 1, plus `ch1_fix13_chip` merged in as
node 2: the golden-like store of fix pass 12). Raw results stay local (`/tmp/fix13/`). Every timing is with
`NODE_ENV=production`. The catalogue criterion is now the paired in-session ratio v2 / v1 per document (close-out §1).

**No data discrepancy.** The chipnet catalogue is 63 / 63 equal against v1; through HTTP with admission on, the golden-like
store's node-2 catalogue is 63 / 63 equal in every run (48 equal, 15 equal-unordered) and node 1 (synthetic, v2 only) works
62 / 63 as before (X11, synthetic data). The new L17 read returns a byte-identical response.

## 1. Small-lookup gap (item 1)

### 1.1 What made #107's small documents slower than #106's: the store's part layout, not fix 12's code

The suspects were fix 12's shared-values array, its record maker, or node variance. #107 and #106 ran the same backfill of
the same golden snapshot; their raw results separate the three without a local run:

- **The statements of the drifting documents are identical.** Node 39's L9, L40, L42, L11 and L19 send the same SQL in
  #106 and #107 apart from `NULL` → `0` for unread tuple elements (no `__dict`: they select no output token category,
  and no chain-link setting). Yet their direct statement time (qlog, in process) rose 2–7 ms and their CPU 15–40 %.
- **The API side did not change.** Per document, (HTTP p50 − direct statement wall) moved by a median **−0.2 ms** (node 1)
  and **−0.5 ms** (node 39) from #106 to #107: the extra time is all in ClickHouse, so neither the record maker nor the
  decoding of shared values is the cause.
- **The same statements read 1.5–2× the parts.** `SelectedParts` per document, #106 → #107 (node 39): L9 58 → 87,
  L40 / L42 29 → 49, L44 / L46 16 → 33, L38 29 → 53, L24 46 → 79, L19 9 → 16, X1 6 → 11. The backfill's merges left a
  different layout (active parts at the end of the session):

| table | #106 | #107 |
|---|---:|---:|
| input | 7 | **12** |
| transaction | 3 | **8** |
| spend | 8 | **15** |
| output | 13 | 16 |
| block_transaction | 8 | 9 |
| tx_acceptance | 5 | 3 |

Each read analyses the key of every part of its table and of each candidate projection, and opens a read task per part
that has granules; input's parts each span every spender hash (golden's input is chunked by outpoint hash), so a hash
lookup probes every part. More parts → more fixed cost per read → the 2–7 ms. Merge selection depends on timing, so the
layout differs between sessions on unchanged code. The kit now reports it (`PL`, plan §4).

**Local A/B (golden-like store, through HTTP, admission on, 2 workers × 1 lane thread, two interleaved rounds):** fix 12 as
shipped (`cb2364d`) against the same commit without shared values (`shared: false`), with fix 11's per-field record
stores, and this pass's head.

| variant | node 2 median (round 1 / 2) | node 2 per document vs base | node 1 median (1 / 2) | node 1 per document vs base |
|---|---:|---:|---:|---:|
| base `cb2364d` | 67.3 / 75.5 ms | — | 60.0 / 69.7 ms | — |
| no shared values | 64.4 / 69.8 | −4.7 ms | 64.6 / 71.0 | −0.5 ms |
| fix 11's record stores | 66.8 / 76.7 | −2.6 | 59.4 / 54.3 | −3.8 |
| head (this pass) | 63.8 / 60.6 | **−7.7** | 64.8 / 63.8 | −1.9 |

The same variant moved up to 10 ms between its two rounds (the Docker VM): neither revert separates from that, and
neither makes small documents consistently slower. With the mainnet evidence above, fix 12's code is cleared; the drift
was the part layout. Both stay.

### 1.2 Cutting the per-statement fixed cost

- **Server settings, direct, 15 node-2 small statements × 5 interleaved rounds** (golden-like store): against the
  default, `optimize_use_projection_filtering = 0` −2 ms, `use_skip_indexes = 0` 0, `use_concurrency_control = 0` −3,
  `compile_expressions = 0` −2, `use_query_condition_cache = 0` −3, `optimize_move_to_prewhere = 0` −1 (X7 +54 ms) at the
  median: all within the run-to-run spread, none adopted. `max_threads = 8` −4 ms (and −12 CPU-ms) is the only
  consistent one, and it is already the small-request setting.
- **The block level by id** (a scan of `block`, 1.3 M rows on golden): 3 ms vs 2 ms by height locally (782 k blocks);
  not worth `block.p_id`'s side effects (fix-pass-8b.md §2.3).
- **Where the lever is: which requests get the 8 threads.** Fix 12 meant every small request to run with 8, but #107's
  judged catalogue mostly did not (§5): X7 131 ms judged vs 104 ms in the v2-only run at 8 threads, X13 494 vs 413,
  L21 90 vs 77, L22 139 vs 125, L1 104 vs 97, L5 / L4 −8 to −10 ms. Fix 13 makes the class deterministic and
  thread-independent (§5, `7c6158a`).
- Fewer parts would cut every read, but compacting golden's 270 GiB `input` is hours of merges; for a session it is a
  covariate (`PL`), for production a merge-policy question (§8).

### 1.3 Expected paired ratio (mainnet)

From #107's judged catalogues with, for every light document reading ≤ 3 M rows, the session's own 8-thread delta (v2-only
st8 − st0), and L17 −50 ms (§2):

| | #106 | #107 | expected fix 13 |
|---|---:|---:|---:|
| node 1: median per-document v2 / v1 | 2.15 | 2.19 | **~2.0** (p75 16, p90 27) |
| node 39: median per-document v2 / v1 | 2.04 | 1.86 | **~1.9** (unchanged; its 8-thread deltas are small) |
| node 1 / node 39 v2 catalogue medians (reported) | 75.7 / 71.1 | 81.4 / 74.3 | ~78 / ~75 |

Median ratios computed over the documents equal to v1 and timed on both sides (56 / 59). The part layout can move either
node by ±0.15. On the golden-like store the node-2 paired median moved 0.99 → 0.85 (v1 there is chipnet Postgres).

## 2. L17's key CPU (item 2)

**Cause.** L17 is an `inputs` root filtered by the spent output (`outpoint: { token_category, capability, commitment }`).
Its page read was `input WHERE transaction_hash IN (accepted spends) AND (transaction_hash, input_index) IN (spends)`:
on golden 1,471 / 1,573 hashes, analysed against input's base key (1.01 M marks per part, `transaction_hash` first) in
every part — each part spans every spender hash, so nothing prunes a part. #107's trace: `PK index has dropped
993990/1011196 granules, it took 706ms across 12 threads` for that read alone (#106: 347 ms across 7 threads; input had
12 parts instead of 7, §1.1). The same read could take input.p_txid (time-ordered: an id falls in one part) had it an id
condition: the spends carry `transaction_internal_id`.

**Fix** (`b4fc713`). When every part of `input` has p_txid and the page reads no column outside it (`inputsByIdCovers`, as
the inputs level already does), an outpoint-filtered `inputs` root reads
`transaction_internal_id IN (spend ids) AND identity(transaction_hash) IN (accepted) AND identity((transaction_hash,
input_index)) IN (spends)`: the hash sets stay exact but out of key analysis, the ids bound the read through p_txid, and the
acceptance check reads the ids through p_node_tx (`accepted(…, idsOnly)`). `RootQuery.where` may now depend on the page's
columns. A page that selects `unlocking_bytecode` keeps the hash read.

| synthetic L17 shape (22,000 matching spends, 23 input parts), direct, median of 7 | base | head |
|---|---:|---:|
| statement | 4,417 ms | **1,050 ms** |
| key analysis (`FilteringMarksWithPrimaryKeyMicroseconds`) | 1,316 ms | **218 ms** |
| rows read | 535 M | 77 M |
| CPU | 56.6 s | 11.2 s |
| response | byte-identical (763,125 bytes) | |

Node 2's own L17 (5 spends): 76 ms, key 4.8 ms. Spec: `api-inputs-projection` (outpoint-filtered root by spend ids and
p_txid; with `unlocking_bytecode` by hash).

**Expected on mainnet:** the 706 ms read's key analysis goes; L17 key CPU **~150–250 ms** (#107 911, #106 534), HTTP
396 → ~330–350 ms, rows read down.

## 3. Random-prefix spot scope (item 3)

**Cause.** The spot-check drew three prefixes from the outputs at a random transaction hash. A prefix is the first 25
bytes of the locking bytecode: a busy address's whole P2PKH script, or a token protocol's OP_RETURN head, can hold millions
of outputs, and the scope's query-time reference reads all of them. #106 drew small ones (0.3 s), #107 at least one large
one (20.7 s); both equal.

**Fix** (`6eb5697`). A random prefix is used only when at most `prefixMaxOutputs` (default **100,000**) outputs have it:
each candidate is counted with `SELECT count() FROM (SELECT 1 FROM output WHERE locking_bytecode_prefix = … LIMIT
100,001)` (p_script range; the read stops at the limit), up to 8 windows of 64 outputs. Fewer than three scopes when the
windows hold too few small prefixes. Explicit `prefixValues` are not bounded. Spec: `utxo-set` (random prefixes drawn; none
with `prefixMaxOutputs: 0`).

**Expected on mainnet:** each node's prefix scope ≤ a few seconds whatever is drawn (100,000 outputs and their spends per
prefix), typically ~0.3–3 s.

## 4. Cold heavy documents (item 4)

The kit's cold step drops every ClickHouse cache, unloads every primary key **and drops the node's page cache** before each
request: a new node, not a restart. Locally, direct, synthetic statements, median of 3 per state:

| statement | full cold | ClickHouse cold, page cache warm | page cache cold, ClickHouse warm | warm |
|---|---:|---:|---:|---:|
| L35 | 7,182 ms (10.8 GB read from disk) | 2,345 ms | 6,989 ms | 770 ms |
| L33 | 4,492 ms (3.3 GB) | 719 ms | 4,421 ms | 528 ms |
| L30 | 687 ms (0.7 GB) | 318 ms | 528 ms | 175 ms |

- **Full cold is the disk read of the data**: dropping only the page cache costs as much as dropping everything. L35 reads
  ~6 GB of compressed columns per run (warm, from the page cache); cold that comes from NVMe. Mark-cache sizing does not
  touch it: the kit's 10 GiB mark cache holds every mark (synthetic: 134 MiB of marks, 121 MiB of primary keys).
- **A ClickHouse restart** (page cache kept) costs L35 +1.6 s, L33 +0.2 s, L30 +0.14 s: loading 126 MB of primary keys and
  50–62 M marks. The API's warm-up covers API restarts, not ClickHouse restarts.
- **Not changed (not cheap):** fewer bytes per heavy read (L35's full-column projections), or `prewarm_mark_cache` /
  `primary_key_lazy_load = 0` table settings (marks and keys of every part loaded at server start; memory on golden
  unmeasured). The kit gains `coldch`: the S1 set after ClickHouse's caches and keys are dropped with the page cache kept,
  the production-relevant restart number.

## 5. X7: real, not noise (item 5)

**Cause.** A small request (8 threads) was one predicted at ≤ 150 CPU-ms. But a request's CPU depends on the threads it
ran with (X7 on golden: 0.21 CPU-s at the server's 48, below 0.15 at 8) and arrives ~10 s late from `query_log` (until then
the model holds the seed estimate: 250 CPU-ms for a new key). So a document stayed in whichever class it entered: in #107's
judged catalogue X7 ran at 48 threads (131 ms; = the st0 run, 130), in the st8 control at 8 (104 ms). Locally (chipnet
copy, X7 / L21 / L1 / X3 each 1 + 2 + 9 runs back to back): first pass 0 of 53 statements at 8 threads, second pass after
15 s 29 of 73. Fix 12's "10 % slower" for X7 was the synthetic copy at 14 threads, where X7 (0.7 CPU-s) is heavy anyway.

**Fix** (`7c6158a`). Small = light and predicted to read at most **3,000,000 rows** (`CHAINGRAPH_API_SMALL_MAX_ROWS`;
replaces `_SMALL_MAX_CPU_MS`). Rows come from ClickHouse's summary when the statement ends and do not depend on the threads,
so the class is fixed from a document's second request on. 3 M from #107's v2-only A/B: every 8-thread gain is at ≤ 2.8 M
rows (X7 0.08 M −26 ms, L21 0.71 M −13, X13 1.48 M −72, L22 2.36 M −14, X6 2.78 M −6), the losses from 3.5 M (L6 +22,
L7 7.2 M +26). The startup line reads `small<=3000000 rows:max_threads 8`. Locally, the same chipnet runs: 40 of 53
statements at 8 threads in the first pass. Spec: `api-admission` (rows learned at once; small by rows; a later CPU-only
sample keeps them).

**Expected on mainnet:** X7 node 1 ~104 ms (#107 131), node 39 ~103 (114); X13 ~415 (494), L21 ~77, L22 ~125.

## 6. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 17 tsdoc warnings of `cb2364d`) |
| unit (`ava --match='!*[e2e]*'`) | 203 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | **183 passed** |
| API specs `build/api/*.spec.js` incl. the chipnet catalogue spec (`ch1_fix13_chip`, v1 Hasura) | **99 passed** |
| chipnet catalogue (`catalogue-run --latency 9`, `ch1_fix13_chip`) | **63 / 63 equal** (48 + 15) |
| golden-like catalogue through HTTP, admission on (kit `ch1-api-catalogue.mjs`) | **63 / 63 equal** on node 2 in every run (48 + 15); node 1 synthetic v2-only 62 / 63 work (X11, as before) |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed each (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed (123 ClickHouse-only skipped) |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533` and the ch1-pg Postgres (`CHAINGRAPH_E2E_POSTGRES_HOST`/`_PORT`, which the ClickHouse e2e needs as well).

## 7. Design calls

| Call | Rationale |
|---|---|
| Fix 12's shared values and record maker stay | identical SQL and an unchanged API side on mainnet; the drift was the part layout |
| The inputs root by spend ids only with p_txid in every part and covered columns | an id-only read the projection cannot serve scans `input` (fix-pass-9.md §1) |
| `identity()` on the hash sets rather than `use_index_for_in_with_subqueries_max_values` | it hides exactly these sets; the setting would also hide the acceptance subquery's and a client's lists |
| Small by predicted rows, not CPU | rows are thread-independent and known at once; CPU made the class depend on history |
| 3 M rows | the boundary between golden's 8-thread gains (≤ 2.8 M) and losses (≥ 3.5 M) |
| Spot prefixes ≤ 100,000 outputs | bounds a scope at ~0.1 M outputs whatever is drawn; categories and the totals scope still cover large sets |
| No server-setting change | none beat the run-to-run spread on the golden-like store |
| Cold: measure the restart state, change nothing | full cold is disk reads; the restart state is the production case and was never measured |
| C1 paired thresholds (kit): median ratio ≤ 2.30 / ≤ 2.10 and no document regressed | #106 / #107 on near-identical code: 2.15 / 2.19 and 2.04 / 1.86; the per-document rule flags only #106's L27 |

## 8. What the next GKE session should verify

1. **C1 paired** (both nodes): median per-document v2 / v1 ≤ 2.30 (node 1) / ≤ 2.10 (node 39); no document with ratio
   > 1.25 × #107's and v2 > max(20 ms, 20 %) slower; equality as before (X11 / X6 known). Expected ~2.0 / ~1.9. Report
   p75 / p90 / geometric mean and the absolute medians (~78 / ~75), with **PL** (part counts vs #107) beside them.
2. **L17K:** node 1 L17 key CPU < 450 ms (#107 911; expected ~150–250), HTTP vs 396 ms, same rows.
3. **SP:** every node's random-prefix scope < 5 s (#107 node 1 20.7 s), equal.
4. **SM:** every admitting pod's startup line has `small<=3000000 rows:max_threads 8`; X7 node 1 ~104 ms (#107 131),
   X13 ~415 (494); light `query_log` statements at `max_threads` 8 reported.
5. **coldch** (reported): the S1 set after ClickHouse's caches and keys are dropped with the page cache kept, next to the
   full cold medians (#107 L33 3.0 s, L35 4.9 s, L30 1.6 s).
6. **Regressions:** U1–U5, S39, every spot scope, A1–A6 (light p95; more requests now run at 8 threads), L27, L3335, X5R,
   X6R, the soak's CPU per request.

## 9. Commits

| Commit | |
|---|---|
| `b4fc713` | perf(api): outpoint-filtered inputs root reads by spend ids through input.p_txid |
| `6eb5697` | fix(store): spot-check draws only prefixes of at most 100,000 outputs |
| `931767d` | style(api): cspell words for the inputs root by spend ids |
| `7c6158a` | perf(api): small requests are those predicted to read at most 3 M rows |

## 10. Housekeeping

`ch1_fix13_chip`, `ch1_fix13_syn`, the Hasura container `ch1-fix13-hasura` and its metadata database
`ch1_fix13_hasura_meta`, and the worktrees `/tmp/fix13/wt-{base,noshared,oldrec}` removed; every API process started for
the measurements stopped (by PID).
