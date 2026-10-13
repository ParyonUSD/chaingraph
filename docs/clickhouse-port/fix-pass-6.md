<!-- cspell:ignore chipnet paryon backfill backfilled subquery subqueries synth tsyn PUSD sats unhex prewhere txid -->

# Fix pass 6: the G2 S1 statements over 1 s warm on mainnet, and the uncached soak drivers

Local only: ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs, 39 GiB RAM), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` over `chipnet_pg` for v1. Inputs: [floor-run-log.md](floor-run-log.md) (golden, node 1),
[planning-floor.md](planning-floor.md), [fix-pass-4.md](fix-pass-4.md), [fix-pass-5.md](fix-pass-5.md),
[utxo-async.md](utxo-async.md), [v2-documents/](v2-documents/) and the floor session's statement captures and qlog
(`chaingraph-performance/2026-10-12-floor-api/`, read only). Scratch databases `ch1_heavy_syn`, `ch1_heavy_tsyn`,
`ch1_heavy_chip` (dropped afterwards); `chipnet_ch_pp2` read only. Raw results stay local (`/tmp/fix6/`, not committed).

Targets (floor session, node 1, 48-thread ClickHouse): L33 3,260 ms (620 M rows), L35 2,247 ms, X13 4,931 ms,
X12 ~4.4 s, X16 7.9 s; uncached soak drivers L31 21.8 CPU-s and L35 19.7 CPU-s per request.

## 1. Summary

| Statement | What dominated | Fix | Synthetic (golden-like layout), CPU-s per request before → after | Expected on golden |
|---|---|---|---|---|
| L31 PriceContractUtxos | query-time unspent: acceptance of every spend and output of the price contract | **already fixed by the merged stored UTXO set** (no change here) | 13.2 → 0.05 | 21.8 → ~0.1 CPU-s, ~50 ms |
| X12 GetJonathan100CashAccount | `in_block: { height }` filtered the *collapsed* acceptance view: a GROUP BY over every acceptance row of the node | per-node block filters below the collapse (`height` is constant per group) → p_node_height; semi-join on internal id | 58.5 → 0.08 (5,161 → 69 ms) | 4.4 s → ~0.1 s |
| X13 GetBitjson100Authchain | the same as X12, plus the authchain walk | as X12; the walk (≈ 0.5 s on golden) is unchanged (§7) | chipnet 398 → 307 ms | 4.9 s → ~0.6 s |
| X16 TransactionStats2021Q1 | a block set (by timestamp) on the collapsed view: the node's whole acceptance set, then a 4 M-hash set over the whole `transaction` table | the block set below the collapse plus its implied height range → p_node_height; semi-join on internal id → transaction.p_id | 15.5 → 1.0 (1,837 → 363 ms) | 7.9 s → ~0.3–0.8 s |
| L33 ContractUtxoActivity, L35 MonitorLoans, L40/L42 MonitorLoansSignal(Poll) | a `transactions` page in id order checked the node's acceptance and read the transaction row of **every** candidate before `ORDER BY … LIMIT`; acceptance by hash probes every part of `tx_acceptance` (golden: 10 overlapping parts); block inclusions read twice; L33's watermark read every transaction row | candidate window (§2), candidate ids on the acceptance / transaction / inclusion reads (§5, §6), inclusions read once (§4), watermark in p_id order (§7) | L33 45.6 → 5.5, L35 15.2 → 7.5, L40 2.8 → 0.12 | L33 3.3 s → ~0.6–0.9 s, L35 2.2 s → ~0.5–0.8 s (≈ 4 CPU-s), L40/L42 0.5 s → < 0.1 s |

- **Not unspent-type:** L33, L35, L40 and L42 select transactions (`has_output` / `has_input` on a contract script), so
  the stored UTXO set does not apply to them. L31 (and L12 in the soak mix) are unspent reads and already read the
  stored set at e9ce30b: no change needed, numbers in §8.
- **Soak mix (uncached):** the floor's per-request CPU (L31 21.8, L35 19.7, L12 8.3, L40 8.3, L42 8.1 s, the rest
  < 0.25 s) becomes roughly L31 0.1, L35 ~4, L12 ~0.02, L40/L42 ~0.2: about a tenth of the mix's CPU, so 10 QPS cache
  off should fit with room (to verify on GKE, §10).
- **Equality:** chipnet catalogue 63/63 equal after every change; API specs (both nodes, the independent evaluator)
  pass; a new spec forces the window's fallback. **No data discrepancy found.**
- **What remains (production layout):** relationship levels of a 10,000-row page (outputs, inputs, spent outputs)
  read by transaction hash; in a store whose parts are written in time order every hash is probed in every part.
  That is now the dominant cost of L33/L35 on the time-ordered synthetic layout (§9; owner decision §11).

## 2. Setup: the synthetic database, two layouts

The planning-floor generator (`/tmp/floor`) no longer existed; the UTXO-async generator (`/tmp/utxo-async/synth`,
same distributions) was rebuilt with planted mainnet-like activity and is now committed:
[synthetic/](synthetic/) (`udf.sql`, `build.sh`, `build-utxo-set.mjs`, the probes `probe.mjs` / `capture.mjs` /
`bench.mjs` / `trace.sh` and `docs.json`, the documents' synthetic variables).

| | rows | note |
|---|---:|---|
| transactions / tx_acceptance / block_transaction | 150 M | 330 per block, 454,546 blocks, node 1 accepts all |
| output | 431 M | as planning-floor.md §3 (60 % P2PKH, 35 % P2SH, 5 % OP_RETURN, 2 % token outputs) |
| input / spend | 374 M | random earlier outpoints; spend generated directly with the spent output's attributes |
| stored UTXO set | 242.5 M | bulk build 1,150 s |
| price contract | 197,999 transactions | P2SH32, category `SHA256('paryon')`, mutable NFT, 3 per block over the last 66,000 blocks, 5 chains (mainnet: 5 UTXOs; L33's walk of ~60 s on golden suggests 10^5 transactions) |
| loan contract | 22,000 transactions | 300 chains (mainnet L35: 24,970 rows) |
| cash accounts | 50,001 | blocks 150,000–199,999; `Jonathan` and `bitjson` at height 170,000 (X12/X13) |
| 2021Q1 stand-in | 13,000 blocks, 4.29 M transactions | X16 `from`/`to` over blocks 300,000–313,000 |

Two layouts of the same data, because part layout decides what a lookup by hash costs:

- **`ch1_heavy_syn`, golden-like:** output / input / transaction / tx_acceptance inserted in 12 chunks of the hash's
  first byte (like the golden backfill's `20-output` … `23` chunk transforms): each part holds a hash range, so a
  lookup by hash probes one part. block_transaction in 2 block-ordered parts (as golden).
- **`ch1_heavy_tsyn`, production-like:** the same tables in 12 chunks of internal id, i.e. time order: every part spans
  the whole hash range, so a lookup by hash probes all 12 parts. This is how a live-synced store's parts look, and
  how golden's `tx_acceptance` (10 parts, `26-acceptance.sql` is one INSERT in block order) and `block_transaction`
  look. Merges were stopped on the data tables in both (`SYSTEM STOP MERGES`) to keep 12 parts.

Golden mixes both: transaction / output / input are hash-chunked, tx_acceptance and block_transaction are
time-ordered. That is why golden L35 read 287 M rows: 25,000 acceptance lookups × 10 parts × 1,024 rows ≈ 256 M.

Measurements: in-process API (`createApi` + `yoga.fetch`, cache off, admission off), 1 warm-up + 5–7 runs, p50;
per-statement figures from `system.query_log`. **Caveat:** each database is ~300 GiB on a VM with 34 GiB of page cache,
and the base statements read 10–30 GiB of compressed blocks per run, so their wall times here are partly disk-bound
and vary between runs; CPU-s and rows read are the comparable figures (GKE has the data cached).

## 3. Per-node block filters below the acceptance collapse (X12, X13, X16) — `cb7108a`

**What they read.** `in_block` / `block_window` compiled to
`hash IN (SELECT transaction_hash FROM tx_acceptance_at WHERE <block condition>)`, the condition outside the
collapsing `GROUP BY transaction_hash, node, block … HAVING sum(sign) > 0`. ClickHouse pushes filters on GROUP BY keys
below it, but not `height` (selected as `any(height)`). X12's `height = 563720` therefore collapsed every acceptance
row of the node (golden: 420 M rows, 13 s of plan-optimization CPU = the scalar's execution) to keep ~300. X16's
block set (blocks by timestamp) is a key condition and was pushed down, but `block_internal_id IN (set)` matches no
index, so it read the node's whole acceptance set too, then looked up ~4 M hashes in `transaction`, which touches
every granule (golden: 838 M rows).

**Fix.**
- The block condition goes into the view's `keyFilter` (base rows, before the gate and the collapse). Exact:
  `height` is the same in every row of a (transaction, node, block) group (block rows carry their block's height,
  mempool rows 0; checked on chipnet: 2.85 M groups, none with two heights, every block row's height equals its
  block's). The base column is qualified (`tx_acceptance.height`): the select list's alias `any(height)` would win.
- A block-set filter adds the implied range of its blocks' heights (a statement-level CTE of the blocks and a
  scalar `(min(height), max(height))`), so `node = n AND height BETWEEN …` selects p_node_height's range.
- The semi-join from transaction rows is on the internal id (`internal_id IN (SELECT transaction_internal_id …)`),
  which identifies a transaction as its hash does (one id per hash; checked on chipnet: no acceptance row whose id
  differs from its transaction's): a block range's transactions are an id range of transaction.p_id instead of a hash
  set over the whole table.

| synthetic, golden-like | before | after |
|---|---|---|
| X12 | 5,161 ms, 302 M rows, 58.5 CPU-s, 16 GB memory | 69 ms, 1.0 M rows, 0.08 CPU-s |
| X16 | 1,837 ms, 300 M rows, 15.5 CPU-s | 363 ms, 9.5 M rows, 1.0 CPU-s |
| time-ordered: X12 / X16 | 4,890 / 1,428 ms | 87 / 342 ms |
| chipnet X12 / X13 / X16 | 145 / 398 / 132 ms | 60 / 307 / 53 ms |

The same change serves every `block_window` / `in_block` transaction filter (L18, L19, L30's windows).

## 4. Block inclusions read block_transaction once — `54497fe`

Since the merge of utxo-async (block inclusions ordered by height), `block_inclusions { block }` joined the inclusions
(a MATERIALIZED CTE `incl`) to their blocks and read `incl` on both sides of the join. **ClickHouse 26.8 evaluates a
MATERIALIZED CTE once per subquery scope that reads it** (measured: a CTE read by a join's left side and by an IN in its
right side is evaluated twice; two scalar subqueries reading one CTE evaluate it twice). For a 10,000-row page that
was 2 × 19 M rows of block_transaction. `Sql.arrayRows` keeps such a row set as one statement-level scalar array
(evaluated once per statement) and returns it as a subquery; the inclusions use it.

## 5. The candidate window (L33, L35, L40, L42) — `c811429`, `3b78b25`, `2c36b42`, `e066951`, `8332bae`

**What they read.** A `transactions` root with a selective `has_output` / `has_input` filter materializes its
candidates (output rows by script via p_script, spend rows by script), then reads `transaction_at` with
`hash IN candidates AND hash IN accepted(node, candidates) AND <filter>` and only then orders and limits. Every
candidate's acceptance and transaction row was read: mainnet L35 25,000 for a page of 10,000 (and again for every
later page), L33 ~60–200 k, L40 / L42 (`limit: 1`, newest) 25,000 for one row. Golden's acceptance parts overlap in
hash (§2), so each of those lookups read a 1,024-row granule in each of 10 parts: 620 M rows for L33.

**Fix: a window, exact.** Candidates now carry their internal id (output and spend rows have it). For a page in id
order (no `order_by`, no offset, no keyset cursor):

1. the first `limit + 1 + ⌈(limit + 1)/4⌉ + 100` distinct candidates in id order (within the id bounds) are read
   once into a statement-level scalar array: the window, i.e. every candidate up to the last of them;
2. the root reads the transaction rows, the node's acceptance and the rest of the filter for the window's
   candidates only, ordered and limited as before; the statement also returns whether the window was capped
   (`RootQuery.extra`);
3. the page is exact if it is full (`limit + 1` rows: `has_more`) or the window was not capped (it held every
   candidate): no candidate before the window's end was skipped. Otherwise (a capped window short of the page: many
   candidates not accepted by the node or failing other conjuncts) the API runs the previous full statement. For L33 /
   L35 / L40 every candidate matches, so the fallback does not run.

**Ids next to hashes (`3b78b25`, `e066951`).** The window's acceptance read gets the candidates' ids as well as their
hashes (`tx_acceptance.transaction_internal_id IN …` below the collapse) and the transaction read gets `internal_id IN …`;
ClickHouse reads whichever of the hash key and p_node_tx / p_id selects fewer granules. In time-ordered parts ids
cluster in the parts of their time (the window's 12,600 ids: 2 of 12 parts); hash-chunked tables keep the hash path.

**Three versions were measured before the final form.** The first (`c811429`) kept the candidates as a MATERIALIZED
CTE and spelt the fallback inside the statement: per §4 the CTE was evaluated in every scope (chipnet L33: 18 reads of
output / spend), and the scalars were looked up 937 times (each lookup of an array in a subquery scope copies it),
~0.7 s of analysis: **chipnet L33 1.4 s vs 0.25 s before**, caught by the existing S6 API spec (30 concurrent L33
requests overran the light lane). `2c36b42` read the candidates once into the array and moved the fallback to a second
statement; a page scalar and the root's re-read of its rows still cost one serial stage (chipnet L33 418 ms).
`8332bae` reads the window in the root directly and returns the capped flag: chipnet L33 / L35 339 / 279 ms (before the
window 339 / 276), less CPU.

**Spec.** `api-filters.spec.ts` "transactions pages over candidates": seven filters with candidates (incl. two with
conjuncts that fail most candidates: `in_block`, `in_mempool`), both nodes, forced windows of 1, 2, 3 and 5
candidates and the default, limits 1 and 2, ASC and DESC: every page sequence equals the evaluator's ids in order,
`has_more` exact, the window used and its fallback taken. A mutation (fallback disabled) fails it.

| API level, p50 / CPU-s / rows | before (e9ce30b) | after |
|---|---|---|
| L33, golden-like | 10,666 ms / 45.6 / 410 M | 1,145 ms / 5.5 / 20 M |
| L35, golden-like | 7,290 ms / 15.2 / 76 M | 1,241 ms / 7.5 / 50 M |
| L40, golden-like (L42 the same) | 615 ms / 2.8 / 41.5 M | 60 ms / 0.12 / 0.28 M |
| L33, time-ordered | 6,926 ms / 50.2 / 677 M | 2,093 ms / 15.4 / 162 M |
| L35, time-ordered | 7,538 ms / 41.0 / 493 M | 4,735 ms (disk-bound, §2) / 21.2 / 193 M |
| L40, time-ordered | 564 ms / 5.8 / 251 M | 62 ms / 0.10 / 0.19 M |
| chipnet L33 / L35 / L40 / L42 | 339 / 276 / 58 / 62 ms | 339 / 279 / 43 / 44 ms |

Quiet statement-level runs on the golden-like layout, before the page cache was shared with the second database:
L33 3,024 ms before, L35 1,134 ms before (the first window form: 1,664 / 1,087 ms). The before-walls of L33/L35 in
the table include disk reads.

## 6. `block_transaction.p_txid` — `2ecfd24`

block_transaction rows are written in block (time) order, so p_tx's parts overlap in hash and a transaction's
inclusions were probed in every part (golden: 2 parts; time-ordered synthetic: 12). The new projection
`p_txid (ORDER BY transaction_internal_id)` (DDL 060) finds them in the part of their time; `block_inclusions` reads
`transaction_hash IN … AND transaction_internal_id IN …` (the parents' ids: `linkColumns`) and ClickHouse takes the
projection that selects fewer granules. 10,000 inclusions, time-ordered: 84 M → 10 M rows, 196 → 34 ms. Storage: one
more copy of block_transaction (golden p_tx: 17.7 GiB, ~2 % of the database). Parts without it (a database created
before, until `MATERIALIZE PROJECTION p_txid`) are read through p_tx as before; the DDL key check lists it, so
`clickhouse-ddl` warns until it exists.

## 7. Smaller findings

- **L33's `transaction_watermark` (`1f3e8ed`)** was `max(internal_id)` over transaction_at: every row (synthetic 150 M
  rows, 1.8 CPU-s; golden 420 M rows, 84 ms on 48 threads but several CPU-s per request). It now reads p_id in
  descending order to the first visible row: 3 ms, 32 rows.
- **X13's authchain walk** (unchanged): with X12's part gone, X13 is the walk over the authchain index, a recursive CTE
  whose every step re-plans its joins and IN-subqueries (chipnet: 9 steps, 2,307 subqueries, 762 of 1,023 ms in
  `RecursiveCTESource`, ~85 ms per step here). On golden X13 − X12 was ~0.5 s. Expected X13 ≈ 0.6 s; a deeper chain
  costs ~0.1 s per segment end crossed.
- **Query condition cache.** ClickHouse 26.8 caches, per WHERE condition, granules a previous run found empty; repeated
  identical statements (benchmarks, polling documents such as L40/L42/L35) benefit. Benchmarks here run the same
  statement repeatedly, as the GKE catalogue runs do.

## 8. L31 and L12: the stored UTXO set

Both are unspent reads and read the stored set (snapshot mark) since the utxo-async merge; the floor's numbers were
the query-time form (c61dc47). Synthetic, golden-like: L31 query time 1,230 ms / 224.6 M rows / 13.2 CPU-s → stored
40–59 ms / 0.9 M rows / 0.05 CPU-s; L12 74 → 8–15 ms. Expected golden: L31 ~50 ms, < 0.1 CPU-s (floor: 767 ms,
21.8 CPU-s). The relationship levels of its 5 rows are point reads.

## 9. Tried and rejected; what is inherent

| Candidate | Result | Decision |
|---|---|---|
| index_granularity 128 + 4 KiB blocks on transaction / tx_acceptance / input / block_transaction.p_tx | golden-like: rows read ÷3, CPU −22–29 %, wall −6–15 %. Time-ordered: key analysis of the IN sets over 8× the marks in every overlapping part exploded (L35 6.4 s vs 2.7 s, 7 CPU-s of key analysis; base L35 14 s) | rejected |
| 4 KiB compressed blocks at granularity 1024 (same tables) | decompressed bytes −33 %, CPU unchanged, +2–3 % storage | rejected |
| the window's page through a CTE instead of scalar arrays | no gain | not kept |

**Inherent (what remains of L33 / L35).** After the window, a 10,000-row page reads, per row, its acceptance and
transaction row (by id), its inclusions (p_txid), its outputs, its inputs and its inputs' spent outputs: ~5–8 granule
reads per row, ~60 k lookups per page. Time-ordered stage costs (L35, cumulative statements cut after each level,
before p_txid): window page 196 ms, root 134 ms, inclusions 630 ms / 85 M rows (with p_txid: 10 M rows), outputs
1.1 s (12 parts × 10,000 hashes, 15 M rows, 1.3 s of key analysis), inputs 105 M rows, spent outputs 0.6 s (25,000
outpoints × 12 parts, 38 M rows, 2.3 s of key analysis). On golden the outputs / inputs / spent outputs are
hash-chunked (one part per lookup), so these levels cost ~15 M rows. A live store's parts are time-ordered and the
same levels cost what the time-ordered column shows, scaled by its part count.

**Options for the levels (owner decision, §11):**
1. **Part pruning by commit seq, no storage.** A transaction's output and input rows are written by the commit that
   writes its transaction row (block-commit.ts and mempool-commit.ts write output, input and transaction for the
   batch's new transactions under one `commit.seq`; the golden backfill uses one seq), and `commit_seq` is in every
   part's partition min/max index. The outputs and inputs levels could add `commit_seq IN (the page rows' seqs)`,
   pruning the parts of other times. Estimated: time-ordered L35 outputs + inputs ~120 M → ~20 M rows. Risk: an
   exactness invariant of the writers (a future writer breaking it would silently drop rows); it needs a checker
   (`count(output) per (hash, commit_seq) = output_count`) before relying on it. Spent outputs and inclusions do not
   benefit (other commits).
2. **Slim id-ordered projections** (like `input.p_outpoint`): input by `transaction_internal_id` (golden ~50 GiB, +5 %);
   output by id would be ~70 GiB. Exact, no invariant beyond ids.
3. **A stored per-node activity index** (script / category → transactions accepted by the node, maintained off the
   commit path like the UTXO set): it replaces only the selection (candidates + window acceptance, ≤ 25 % of what
   remains) at a cost of every (script prefix, transaction) incidence per node (~2.2 B rows per node on mainnet,
   ~100 GiB per node). Not worth it.

## 10. Expected mainnet effect and what the next GKE session should verify

Expected on golden (node 1), from the mechanism and the golden layout (hash-chunked transaction / output / input,
time-ordered tx_acceptance and block_transaction):

| | floor | expected | why |
|---|---:|---:|---|
| L33 | 3,260 ms, 620 M rows | ~0.6–0.9 s, ~60–80 M rows | window acceptance through p_node_tx (1–2 parts instead of 10), 12,600 instead of all candidates; watermark 420 M rows → ~0 |
| L35 | 2,247 ms, 287 M rows, 19.7 CPU-s | ~0.5–0.8 s, ~55–65 M rows, ~4 CPU-s | as L33; inclusions once, p_txid |
| X12 | 4,402 ms, 837 M rows | ~0.1 s | p_node_height for one height |
| X13 | 4,931 ms | ~0.6 s | X12 part; the walk remains (§7) |
| X16 | 8,024 ms, 838 M rows | ~0.3–0.8 s | p_node_height range + p_id range of the quarter (synthetic 4.3 M transactions: 363 ms on 14 cores) |
| L31 | 767 ms, 21.8 CPU-s | ~50 ms, < 0.1 CPU-s | stored set (merged) |
| L40 / L42 | 505 ms, 190 M rows, 8.3 CPU-s | < 0.1 s, < 1 M rows | window of ~100 candidates |

To verify on GKE (database built with this DDL: `p_txid` added and materialized with the 060 projections; image from
this branch's head, not `d203f9f`):
1. `system.projections` lists `block_transaction.p_txid (transaction_internal_id)`; `clickhouse-ddl` logs no key
   warning.
2. Catalogue node 1 and 39, 9 warm: L33, L35, X12, X13, X16 under 1 s; L31 reads the stored set (one `utxo_set`
   statement); 63 documents equal as before (L15/L16 included).
3. qlog per statement: L35 / L33 read ≲ 80 M rows; their `projections` include `tx_acceptance.p_node_tx`,
   `transaction.p_id` or the base table (whichever was cheaper) and `block_transaction.p_txid`; X12/X13 show
   `tx_acceptance.p_node_height`; L33's `transaction_watermark` reads < 1,000 rows.
4. The window's fallback: count `transactions` statements per request (two = fallback). Expected none for the
   catalogue; a noticeable rate on real traffic means the window size (`candidateWindowRows`: page + 25 % + 100)
   should be revisited.
5. Soak cache off at 10 / 20 / 30 QPS (the F8 mix): per-request CPU from query_log (L35 ≈ 4 CPU-s, L31 / L40 / L42 /
   L12 ≪ 1); errors explicit and fast (admission), throughput well above the floor's.
6. The levels' cost on golden (outputs / inputs / spent outputs of a 10,000-row page) as a baseline for §11 item 1.

## 11. Owner decisions

1. **Level reads on time-ordered parts** (§9): (a) commit-seq part pruning for the outputs / inputs levels with a
   checker of the invariant, (b) slim id-ordered projections on input (and output), or (c) leave it (golden-like
   backfilled data is unaffected; a live store pays it in proportion to its part count). Recommendation: measure the
   golden levels (§10 item 6) and the part count of a live store first; prefer (a) if the invariant check holds.
2. **`p_txid`** costs ~2 % of the database for inclusions by id; keep (decided here) unless storage matters more.

## 12. Commits

| Commit | |
|---|---|
| `cb7108a` | perf(api): per-node block filters below the acceptance collapse, semi-join on internal id (§3) |
| `54497fe` | perf(api): block inclusions read block_transaction once (§4) |
| `c811429` | perf(api): candidate window (§5, first form) |
| `3b78b25` | perf(api): window reads bound by candidate internal ids too (§5) |
| `2c36b42` | fix(api): window without CTE re-evaluation; fallback as a second statement (§5) |
| `2ecfd24` | perf(store,api): block_transaction.p_txid; inclusions by hash and id (§6) |
| `e066951` | perf(api): the window's page rows by hash and id (§5) |
| `46d8646` | style(api): lint and spelling |
| `1f3e8ed` | perf(api): transaction_watermark in p_id order (§7) |
| `8332bae` | perf(api): window in one statement, capped flag, fallback decided by the API (§5); DDL README |
| (docs) | this document, `synthetic/` |

The GKE image should be built from the docs commit (head), not from `c811429` (its chipnet regression, §5).

## 13. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 21 tsdoc warnings of e9ce30b, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 188 passed |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | 180 passed (incl. the DDL key test with p_txid). One earlier run hit the known lease-sensitive flake `clickhouse-store` "event-loop stall past the lease deadline" (fix-pass-4.md §5); the rerun and the final run pass |
| API specs `build/api/*.spec.js` | 77 passed (76 + the window spec), 3 skipped (the chipnet spec without its env) |
| chipnet spec (`ch1_heavy_chip`: a copy of `chipnet_ch_pp2` in the current DDL, authchain index backfilled, UTXO set built; v1 = Hasura over `chipnet_pg`) | 3 passed: **63/63 equal**, 8 statements read the stored set; also with p_txid materialized: 63/63 |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed |
| e2e Postgres (ch1-pg) | 102 passed |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`.

Housekeeping: `ch1_heavy_syn`, `ch1_heavy_tsyn`, `ch1_heavy_chip`, the `h6_*` UDFs, `default.fix6_*` helper tables, the
Hasura container `ch1-fix6-hasura` and its metadata database dropped; temporary worktrees under `/tmp/fix6` removed.
