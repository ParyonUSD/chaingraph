<!-- cspell:ignore chipnet paryon backfill qlog txid synth prewhere analysed analyser analysing serialising serialisation serialised optimise tsdoc worktrees chipb blkb unmaterialized materialized -->

# Fix pass 8b: C1 regressions against the floor, and the API-side gap of L33 / L35

Local only: ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura (`chaingraph/hasura:c1-dev`, built
from `images/hasura`, container `ch1-c1-hasura` on port 18081) over `chipnet_pg` for v1. Inputs: the decision document
`paryon_kubernetes-consolidation/docs/chaingraph/decisions/2026-10-13_clickhouse-fix7-verification.md` (C1 lists),
[fix7-run-log.md](fix7-run-log.md), and the raw captures `chaingraph-performance/2026-10-13-fix7-api/` and
`2026-10-12-floor-api/` (read only: `capture-*.jsonl` statements with parameters, `qlog-*.tsv`, `trace/L14-13.log`).
Scratch databases `ch1_c1_chip` / `ch1_c1_chipb` (copies of `chipnet_ch_pp2` in the current DDL, authchain index
backfilled, UTXO set built), `ch1_c1_blk` / `ch1_c1_blkb` (block table at 4× chipnet); all dropped afterwards. The
mainnet-scale synthetic database (`docs/clickhouse-port/synthetic`, golden layout, 150 M transactions) was the
parallel fix-pass-8 run's `ch1_fix8_syn`, used **read only**; its admission load tests ran at the same time, so every
local wall time here is an interleaved A/B median (A and B alternate per round), and key CPU / rows read (deterministic)
carry the conclusions. Branch `experiment/clickhouse-c1` (from `521fe8e`). The admission code was not touched.

## 1. Summary

| # | Finding (fix7 session) | Cause | Fix | Local before → after |
|---|---|---|---|---|
| 1 | Node 1: L1, L4, L5, L6, L7, L10, L11 key CPU +0.2–0.5 s (L1 14 → 224 ms) | `block_window`: `(block != 0 AND height range) OR block_internal_id = 0` below the collapse. An OR with a non-key atom turned the key analysis of `p_node_height` **and** `p_node_tx` into a generic exclusion search over every mark of the node (node 1: ~410k marks each) | Mempool rows as a second, conjunctive read under `UNION ALL` (`96e827c`) | chipnet trace: 1,117–1,498 steps per part (all marks) → binary search; synthetic block_window 100-row page key CPU 405 → 262 ms |
| 2 | L17 key CPU 0.81 → 1.45 s; L33 / L35 key CPU 1.9 / 1.9 s | inputs and block inclusions read `transaction_hash IN … AND transaction_internal_id IN …`: ClickHouse analysed the hash set against the base key of every part **and** the id set against `p_txid` before choosing | Read by id alone when every active part has `p_txid` (store feature from `system.parts`; else hash and id as before) (`da43124`) | synthetic L33 key CPU 1,889 → 1,007 ms; L35 3,810 → 1,817 ms, 9.3 → 7.9 CPU-s, 3,299 → 2,835 ms |
| 3 | Node 39: +1.25 M rows on almost every document (X4 33 → 51 ms); node 1 X3 / X9 / L29 / L34 +1.1–3.8 M rows | the (height, block hash) default order of `block_inclusions` joined every inclusion to `block`; `block` is ordered by hash, so the read by id scanned the table (~1.2 M rows on golden), twice when two inclusion lists are selected (L34, X6) | Only transactions with two or more inclusions have an order to decide: only their blocks are read (usually none: the empty set prunes every part). A `where` keeps the full join (it may compare the height) (`bffeca8`) | chipnet X4 681k → 354k rows, X3 743k → 416k, L8 730k → 402k (the whole block table, 327k rows, no longer read) |
| 4 | L33 / L35 through HTTP 1,375 / 1,763 ms vs 969 / 962 ms direct | after the statement: result transfer + JSON parse, decoding into rows, GraphQL execution, response JSON; subscriptions also serialised the whole result for change detection | cheaper decoding, link-only tuple elements sent as NULL, change detection by structural comparison (none on the first payload) (`6d6945a`) | API-side CPU of a 10,000-row page: chipnet query 128 → 103 ms, subscription 150 → 103 ms; synthetic L35 181 → 147 ms, L33 211 → 172 ms; ClickHouse result −7 to −10 % bytes |

Chipnet copy, the image's code (`521fe8e`) against this branch, interleaved, 9 runs per statement: **median of the 64
statements 63 → 52 ms, no statement slower** (L1 116 → 105, L14 150 → 135, L22 113 → 95, L30 270 → 237, L33 259 → 224,
L35 281 → 253, X6 547 → 511 ms). **No data discrepancy**: chipnet catalogue 63/63 equal against v1 (§6), and every
catalogue response byte-identical between `521fe8e` and this branch on the same database.

## 2. Catalogue of the C1 regressions (per document)

Method: for every document slower than the floor, the floor's and fix7's captured statements (`capture-paryon.jsonl` /
`capture-chipnet.jsonl`) were diffed, the qlog counters compared (rows, key CPU, plan-optimise time, projections), and
the mechanisms reproduced on the chipnet copy (server traces, `send_logs_level = trace`) and on the synthetic database.

| Doc | Node 1 now (floor) | Node 39 now (floor) | Cause(s) | Fixed by |
|---|---|---|---|---|
| L1 | 216 (106) | 163 (96) | key CPU 14 → 224 ms (node 1): block_window OR walk; +1.2 M rows block join; inputs hash+id | 1, 3, 2 |
| L4 | 248 (152) | 399 (306) | key 69 → 311 / 213 → 340: block_window; block join; inputs hash+id | 1, 3, 2 |
| L5 | 242 (130) | 196 (139) | key 44 → 279 / 46 → 71: block_window; block join | 1, 3, 2 |
| L6 | 229 (125) | 164 (104) | key 119 → 343: block_window; block join | 1, 3, 2 |
| L7 | 475 (334) | 248 (167) | key 285 → 580 / 89 → 135: block_window; block join | 1, 3, 2 |
| L8 | – | 116 (71) | block window (node 1 key 876 → 1,429); +1.25 M rows block join (node 39) | 1, 3 |
| L9 | – | 100 (78) | inputs hash+id, more levels (window) | 2 |
| L10 | 324 (188) | 142 (88) | key 230 → 443: block_window; block join | 1, 3, 2 |
| L11 | 187 (92) | 124 (75) | key 33 → 259: block_window; block join | 1, 3, 2 |
| L14 | 118 (65) | 195 (137) | page empty on node 1: every level pays its fixed planning cost; block join + inclusions level; inputs hash+id | 3, 2 (partly; §5) |
| L17 | – | 106 (71) | node 1 key 812 → 1,450: inputs and inclusions hash+id over 1,000 transactions; block join | 2, 3 |
| L21 | 128 (71) | 127 (74) | window + inclusions levels; block join (+1.3 M rows node 39) | 3 (partly; §5) |
| L22 | 205 (135) | 176 (113) | block join; inputs / inclusions hash+id | 3, 2 |
| L23 | – | 192 (136) | inputs hash+id; block join | 2, 3 |
| L26 | 240 (173) | 210 (147) | key 152 → 215 (inputs hash+id); +1.1 M rows block join | 2, 3 |
| L28 | – | 210 (139) | inputs hash+id; block join | 2, 3 |
| L29 | 160 (99) | 156 (89) | +1.1 M rows block join; inputs hash+id | 3, 2 |
| L30 | – | 446 (334) | key 525 → 816 (node 39): inputs / inclusions hash+id over 1,000 rows | 2 |
| L34 | 147 (89) | 143 (78) | +3.8 M rows: two inclusion lists, each joined to the whole block table | 3 |
| L35 | 1,763 HTTP | 435 (339) | key 297 → 437 (node 39): inputs / inclusions hash+id; HTTP: §4 | 2, 4 |
| L39 | 158 (117) | 136 (90) | block join; window level | 3 (partly) |
| X3 | 123 (82) | 127 (90) | +1.29 M rows block join | 3 |
| X4 | – | 62 (39) | +1.3 M rows block join (the whole doc is a lookup by hash) | 3 |
| X6 | 730 (475) | 931 (672) | two inclusion lists, each joined to the whole block table (+2.6 M rows); window level for a hash lookup | 3 (partly) |
| X9 | 115 (68) | – | +2.6 M rows block join | 3 |

Candidates from the brief that turned out **not** to be causes: fix pass 6's / 7's candidate window width (L1's window
holds a handful of candidates; the window costs one level, as the floor's candidate CTE did), `input.p_txid` being
chosen badly (it is chosen right; the cost was analysing both keys), the UTXO-set read paths (L12 / L13 / L20 / L25 /
L31 / L32 / L36 / L37 are all faster than the floor), and admission (the qlog re-runs statements without it and shows
the same regressions).

### 2.1 The block_window walk (item 1)

The fix7 statements read `tx_acceptance WHERE transaction_hash IN (window) AND ((block_internal_id != 0 AND height >= a
AND height < b) OR block_internal_id = 0) AND node_internal_id = n`. In the floor the height test sat above the collapse
(on `any(height)`), so the key condition was just `node = n` (a binary search). Fix pass 6 (`cb7108a`) moved it below the
collapse to read `p_node_height` ranges. With the OR, the key condition of `p_node_height` is
`node = n AND (height range OR unknown)` and that of `p_node_tx` is `node = n AND (unknown OR unknown)`: neither is a
continuous range, so ClickHouse runs the generic exclusion search, which cannot drop any range of the node and
descends to every mark. Chipnet trace (L1): 1,117 / 1,498 / 1,481 steps per part on both projections, i.e. every mark
of the node; mainnet node 1 has ~410k marks per projection (420 M rows, granularity 1024): 0.2–0.5 s of key CPU per
statement, which is exactly the L1 / L4–L8 / L10 / L11 increase.

Fix: `internal_id IN (block rows UNION ALL mempool rows)`, each a conjunction:
`… AND (block_internal_id != 0 AND height range)` and `… AND block_internal_id = 0 AND height = 0` (mempool rows carry
height 0: `mempool-commit.ts` and the golden backfill's `26-acceptance.sql` both write 0; the first predicate keeps the
read exact, the second bounds it to the node's height-0 range). Trace after: binary search on (node, height) and on
(node) for `p_node_tx`; no exclusion search over the node's marks.

### 2.2 Two keys analysed per relationship read (item 2)

Fix pass 6 / 7 bound the inclusion and input levels by `hash IN (page hashes) AND id IN (page ids)` so that ClickHouse
could pick the base key or `p_txid`. It does pick `p_txid`, but only after analysing both: the hash set against the
base key of every part (golden `input` is chunked by outpoint hash: 15 parts, each spanning every spender hash) and the
id set against `p_txid`. L17 (1,000 transactions per page) doubled its key CPU (0.81 → 1.45 s).

Fix: the levels read `transaction_internal_id IN (page ids)` alone (exact: one id per hash) when **every active part** of
`input` / `block_transaction` carries `p_txid`. `ApiDb.storeFeatures` now also reports
`inputsByIdProjection` / `inclusionsByIdProjection` from `system.parts` (`count() > 0 AND countIf(NOT has(projections,
'p_txid')) = 0`, cached 60 s like the other features); the request's `Sql.readsById` carries them. A part without the
projection (a database between `ADD PROJECTION` and `MATERIALIZE`) keeps the hash-and-id read, since a read by id alone
would scan such a part whole. Spec: `api-acceptance-order` drops `input.p_txid`, re-adds it without materializing
(feature off, same inputs), materializes it (feature on).

Synthetic (golden layout), base `521fe8e` → this branch, interleaved, 7 runs: L33 key CPU 1,889 → 1,007 ms; L35 key
3,810 → 1,817 ms, CPU 9.3 → 7.9 s, wall 3,299 → 2,835 ms (contended host); block_window 10,000-row page key
2,173 → 1,277 ms.

### 2.3 The default order of block inclusions (item 3)

The owner's (height, block hash) order (`5586b9b`, after the floor) joins each inclusion to its block. `block` is
ordered by hash, so `block WHERE internal_id IN (…)` reads the whole table: every node-39 document with a block field
read +1.25 M rows (the golden block table), L34 and X6 twice (two inclusion lists). Only a transaction with two or more
inclusions (a stale fork block) has an order to decide, so the join now reads the blocks of those alone; usually there
are none, the set is empty and ClickHouse prunes every part (`Statistics pruning: 1 parts -> 0 parts`). A single
inclusion gets height 0 and an empty hash, which order nothing. With a `where` (which may compare the height) the full
join stays. `api-structural` (fork and stale blocks, two inclusions per transaction, filters by height, both orders)
passes; the catalogue responses are byte-identical.

Tried and rejected (design calls):
- **`block.p_id` (a projection by internal id)** made the reads by id point reads, but ClickHouse then builds every
  `internal_id IN (subquery)` set on `block` before reading, also where the set is all of a node's blocks: at 4× the
  chipnet block table (1.3 M rows, golden-sized) L43 (the block-tip poll) 36 → 95 ms, X14 48 → 130 ms, even with the
  node-wide set hidden by `identity()` and with a slim projection. Not adopted.
- **A `transform()` lookup over a statement-level array instead of the join**: same time as the join (the cost is the
  extra subqueries, not the join algorithm); `join_algorithm`, `max_threads`, runtime-filter and swap settings changed
  nothing measurable. The remaining cost of the order is ~13 ms per statement on the chipnet copy (one more level and
  its subqueries); removing it needs the order decided in TypeScript or a second statement for multi-inclusion pages
  (§7).

## 3. Per-level fixed cost on mainnet (context for §5)

`trace/L14-13.log` (fix7, node 1, an empty page): every level costs 5–12 ms although it reads nothing (`Selected 0/12
parts`), and the cost grows with depth; the inclusion join level was ~12 ms plus ~22 ms of analysis before the next
level. Each scalar level is analysed and run on its own (the statement is a chain of scalar subqueries), and each
subquery inside a level (an `IN (SELECT … arrayJoin(level))` set) is planned and run too. On the golden tables
(9 M granules for `output`, 1.1 M for `p_category`) each read also scans the query condition cache over every granule.
Fix pass 6 / 7 added levels (the candidate window, the inclusion array, the join), so small documents pay a few more
of these. Items 2 and 3 remove subqueries and reads from each level; the level count itself is unchanged (§7).

## 4. API-side gap of L33 / L35 (item 4)

The fix7 query log shows the gap: L35's statement ends at 19:10:35.887 and the runner's next request starts at
36.689 (0.80 s; L33: 0.42 s). Through HTTP the kit ran L35 as a query (the watermark check of the request snapshot,
then the statement); no round trip is missing and no second statement runs. The time is spent after the statement:
ClickHouse result transfer and `JSON.parse` (one JSONEachRow line of 25–30 MB on the synthetic shapes), decoding the
level arrays into rows, GraphQL execution over 10,000 nodes, and `JSON.stringify` of the response (13–19 MB). Measured
in process on 10,000-row pages with the ClickHouse result replayed from memory (so only the API's own CPU is timed;
`NODE_ENV=production` as on GKE). CPU profile of the 10,000-row L35 shape on the chipnet copy, per request, before:
decode 58 ms, `resultKey` 20 ms (subscriptions), GraphQL execution ~30 ms, response stringify 15 ms, result buffer to
string 15 ms, JSON parse 15 ms, GC 24 ms. Changes:

- **Decoding** (`compile.ts`): tuples are read in place (an offset instead of `slice` per row and level), single-column
  keys without `map` / `join`, relations stored under the symbol directly (`Object.defineProperty` per row was ~10 % of
  decoding), groups of one not sorted.
- **Fewer bytes from ClickHouse:** the final SELECT replaces tuple elements only the SQL reads (the link columns of the
  levels below: raw 32-byte hashes, internal ids) with NULL (`arrayMap(… NULL …) AS __lN_c`). Raw binary hashes cost
  ~50 bytes each in JSON; L35's inputs level carried every outpoint hash raw next to its hex text (25 % of the level).
- **Live change detection** (`live.ts`): the first result of a topic is pushed without serialising it; later results
  are compared with the last delivered one by a structural walk that stops at the first difference (`sameResult`;
  falls back to `resultKey` for anything but plain records and arrays), instead of `JSON.stringify` of both.
- Not changed: response serialisation (yoga's plain `JSON.stringify`; no compression is negotiated), the JSONEachRow
  transport (the result is one row of arrays; a binary format would need a typed decoder for every level, §7).

| API-side CPU per request (median, replayed result) | before (`521fe8e`) | after |
|---|---:|---:|
| chipnet, 10,000-row L35 shape as a query | 128 ms | 103 ms |
| chipnet, the same as a subscription (first payload) | 150 ms | 103 ms |
| synthetic L35 (10,000 loans, 12.9 MB response) | 181 ms | 147 ms |
| synthetic L33 (10,000, 18.9 MB response) | 211 ms | 172 ms |
| ClickHouse result bytes, chipnet 10,000-row page / synthetic L35 / L33 | 18.2 / 26.2 / 31.2 MB | 16.3 / 24.3 / 29.3 MB |

An HTTP request to a local server adds ~70 ms over in process for a 7 MB response (headers at 520 of 533 ms: the
body is built, then sent). Expected on GKE: L33 / L35 direct time falls with item 2 (key CPU about halved), and the API
side by ~20–30 % of its share; whether both get under 1 s through HTTP depends on the c4d core speed and the response
size of golden's loans (not measurable locally). The GKE gap (0.42 / 0.80 s) is 2–4× the local API-side CPU, so the
next session should time the phases there (§8 item 5).

## 5. What is not fixed

- **Fixed cost per level.** L14 / L21 / L39 / X6 still have one or two more levels than in the floor (window,
  inclusion array). On golden each level costs ~5–12 ms of planning even when empty (§3). Locally, against the floor's
  code on the same chipnet copy, this branch's median is equal (52 vs 51 ms over 64 statements) but documents with
  block inclusions remain 10–40 ms slower (L1 68 → 105, L14 85 → 125, L28 91 → 129 under the parallel load tests);
  ~13 ms of that is the inclusion order (§2.3).
- **The block level** (`block { … }` of an inclusion) still reads `block` by id, a full scan (~1.2 M rows on golden),
  as it did in the floor.

## 6. Suites (final code, `378e909`)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 21 tsdoc warnings of `521fe8e`, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 192 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | all passed except 4 timing tests (writer lease held, linger timing, injected 209 timeout, authchain backfill count) that failed while the parallel run's load tests saturated the server; the three files re-run: 20 / 20 passed. Store code is unchanged in this pass |
| API specs `build/api/*.spec.js` incl. e2e and the chipnet spec (`CHAINGRAPH_API_CHIPNET_DATABASE=ch1_c1_chip`, v1 Hasura) | 83 passed |
| chipnet catalogue (`catalogue-run.js`, v1 Hasura, 9 warm) | **63 / 63 equal** (48 equal, 15 equal-unordered as before); v2 p50 median 56 ms on the contended host |
| catalogue responses `521fe8e` vs this branch, same database | 63 / 63 byte-identical (subscriptions: first payload) |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 and 56 / 56 passed (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed, 1 skipped |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3311`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19633`.

## 7. Design calls (autonomous) and open options

- **Mempool rows as a second read** rather than an OR (exact; relies on mempool acceptance rows having height 0 only to
  bound the read, not for the result).
- **Id-only reads gated on materialization in every part**, not on the projection's definition (as `p_node_tx` is): a
  half-materialized `p_txid` would turn an id-only read into a scan of `input` (1 B rows).
- **Inclusion order: blocks of multi-inclusion transactions only**; no `block.p_id` (its side effects on L43 / X14 are
  worse than the scan it saves).
- Open: (a) decide the inclusion order in TypeScript (sort groups of two or more by the block level's height and hash,
  or a second statement when a page has a multi-inclusion transaction) to drop that level's subqueries; (b) fewer
  levels for lookups by hash (X6's candidate window for `hash: { _eq }`); (c) RowBinary transport for the levels.

## 8. What the next GKE session should verify

1. **C1 catalogue** (cache off, 9 warm, admission as configured): node 39 median ≤ 78 ms, node 1 < 118 ms; list the
   documents still slower than the floor by > max(20 ms, 20 %). Expected: the block_window group (L1, L4–L8, L10, L11)
   back near the floor on node 1 (key CPU < 50 ms each); node 39 documents read ~1.25 M rows fewer each.
2. **qlog key CPU** on node 1: L1 / L4 / L5 / L6 / L7 / L10 / L11 key CPU back to the floor's range (14–285 ms, not
   224–580); L17 < 0.9 s; L33 / L35 ~half of fix7's 1.97 / 1.89 s. `projections` for the inputs levels: `input.p_txid`
   only (the statement reads `transaction_internal_id IN` without the hash set).
3. **Store features**: `system.parts` for `input` and `block_transaction` shows `p_txid` in every active part after K1;
   if not (MATERIALIZE still running), the API reads by hash and id (correct, slower) until the next feature refresh
   (60 s).
4. **Rows**: X4 / X3 / L29 / L34 / X9 on both nodes read ≥ 1.2 M rows fewer than in fix7 (the block join reads no block
   unless a transaction of the page has two inclusions).
5. **L3335 through HTTP**: time L33 / L35 direct and through the API, and split the API side (statement end → response
   start → response end) from the runner's own parse, to see whether the remaining gap is CPU in the API pod (GraphQL
   execution and serialisation, ~60 % of it locally) or transfer.
6. **Equality**: catalogue 63/63 on node 39 (as before; X6 differs by design), `checkUtxoSet` untouched by this pass.

## 9. Commits

| Commit | |
|---|---|
| `6d6945a` | perf(api): cheaper result decoding, link-only tuple elements sent as NULL, live change detection without serialising (§4) |
| `96e827c` | perf(api): block_window reads mempool acceptance as a second conjunctive read, not an OR (§2.1) |
| `da43124` | perf(api): inputs and block inclusions read by internal id alone when every part has p_txid (§2.2) |
| `bffeca8` | perf(api): block inclusions read the blocks of multi-inclusion transactions only for their order (§2.3) |
| `378e909` | style(api): spelling |
| (docs) | this document |

Merge note for the admission work on `experiment/clickhouse-store`: this pass does not touch `src/api/admission.ts`.
It touches `db.ts` (`storeFeatures`: two more fields and a `system.parts` subquery; `PinnedSnapshot.statement` sets
`sql.readsById`), `schema.ts` (the subscription runner sets `readsById`), `compile.ts`, `live.ts`, `model.ts`, `sql.ts`
and `api-acceptance-order.spec.ts` (feature object asserted with the two new fields).

Housekeeping: `ch1_c1_chip`, `ch1_c1_chipb`, `ch1_c1_blk`, `ch1_c1_blkb` dropped; the Hasura container `ch1-c1-hasura`,
its image `chaingraph/hasura:c1-dev` and metadata database `ch1_c1_hasura_meta` removed; the temporary worktrees
`/tmp/c1work/wt-floor` (`c61dc47`) and `/tmp/c1work/wt-base` (`521fe8e`) removed. `ch1_fix8_syn` was only read.
