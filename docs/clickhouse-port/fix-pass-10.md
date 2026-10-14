# Fix pass 10: L33 / L35 API-side CPU, the small-node spot check

Local only (2026-10-14): ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` (container `ch1-fix10-hasura`, port 18083) over `chipnet_pg` for v1. Base: `b5474a3` (fix
pass 9). Input: [fix-pass-9.md](fix-pass-9.md) §4, §6 and §9. Scratch: `ch1_fix10_syn` (synthetic, mainnet scale, golden
layout: `synthetic/build.sh … golden`, plus a second node accepting the last 3,030 blocks and the stored UTXO set of
both), `ch1_fix10_chip` (a copy of `chipnet_ch_pp2` in the current DDL, authchain index backfilled, UTXO set built). Raw
results stay local (`/tmp/fix10/`). Every timing below is with `NODE_ENV=production`, as the image runs (graphql-js's
development-mode `instanceOf` checks cost ~4 % of a page otherwise).

**No data discrepancy.** The chipnet catalogue is 63 / 63 equal against v1; every L33 / L35 response is byte-identical
across base, writer-only and head, in process and through HTTP; group numbers on and off answer byte for byte; the
small-node totals form equals the chain-range form and the stored set on every node it ran on (synthetic node 2, the
chipnet copy), and node 1's totals are equal at 256 chunks.

## 1. L33 / L35 API-side CPU

### What changed

- **The compiled writer** (`9f672e3`, fast-execute.ts). The fast executor inspected each value's type
  (`isNonNullType` / `isListType` / `isLeafType`, graphql-js `instanceOf`) per value and pushed ~3 M pieces into an
  array joined at the end. It now builds one writer per field type and selection (non-null, list, leaf, object) on first
  use and appends to one string. Same output, same give-up rules (an error value, a non-null violation, a leaf serialised
  to `undefined` or to `null` under non-null, an abstract type).
- **Group numbers** (`a3ea828`, compile.ts). A relationship level's child rows carried their parent key as text
  (`lower(hex(…))`, 64 characters for a hash), and the client grouped them with string-keyed `Map`s. Now the
  statement's final SELECT numbers each parent level's keys: per parent key one array (`__kN`, a `WITH` alias), each
  parent tuple gets the 1-based position of the first parent row with its key as an extra element
  (`transform(key, __kN, arrayEnumerate(__kN), 0)`), and each child tuple carries that number in place of its key
  texts. The client groups by array index. NULL keys match nothing on either side (`'p'` / `'c'` sentinels, never a hex
  text); a child whose key no parent has gets 0. Pipeline (`recursive`) levels keep key texts (their arrays are not
  constants of the final SELECT).
- **The final SELECT reads the levels as columns of a one-row subquery** (`ef622aa`): `SELECT … FROM (SELECT __page AS
  __page_v, __l1 AS __l1_v, …, __k1 AS __k1_v, …)`. Referenced directly, each `transform` re-resolved its key array's
  alias and the scalar subquery behind it during analysis (the tree hash of the level's whole subquery): a few ms per
  relationship whatever the page's size. The first catalogue run with group numbers showed it: median 56 → 78 ms, X5
  (nine relationships, a one-row answer) 70 → 119 ms, each of its 19 `transform`s +2–5 ms. Wrapped, X5 takes 70 ms
  and small documents are equal to key texts (L1 102 / 105, L5 136 / 137, X3 79 / 79 ms off / on). Not with pipeline
  (recursive) levels, which have no group numbers.
- **The switch** (`443d790`): `CHAINGRAPH_API_GROUP_NUMBERS=off` matches by key texts again (fix 9's statements), for
  the GKE A/B. A spec runs every fast-executor document plus a 2,000-transaction page with inclusions, outputs,
  `spent_by`, inputs, outpoints and their inclusions through two APIs (on / off) and requires byte-identical bodies.

### Measured

Synthetic `ch1_fix10_syn`, the catalogue's L33 / L35 at 10,000 rows (`synthetic/docs.json`). Response bodies are
byte-identical across all builds (`api-side.mjs --out`, HTTP bodies compared with `cmp`).

| | base `b5474a3` | writer `9f672e3` | + group numbers (head) |
|---|---:|---:|---:|
| L33 in process, API side (replayed, median of 9–15) | 169–187 ms | 136 ms | **119–127 ms** (−32 to −36 %) |
| L35 in process, API side | 146–169 ms | 146 ms (min 117) | **95–102 ms** (−35 to −44 %) |
| L33 ClickHouse result | 16.2 MB | 16.2 MB | **10.8 MB** (−33 %) |
| L35 ClickHouse result | 14.4 MB | 14.4 MB | **7.4 MB** (−49 %) |
| L33 statement alone (quiet server, warm, median of 6–12; before the wrapping) | 600 ms | 600 ms | 654 ms (+54) |
| L35 statement alone (before the wrapping) | 724 ms | 724 ms | 715–727 ms (±0) |
| L33 / L35 statement, wrapped (same server later, page cache cold: 1.7 GB read per run) | 2,211 / 2,354 ms | | **2,167 / 2,352 ms** (unwrapped 2,263 / 2,345) |

Through HTTP (`build/api/main.js`, one worker, heavy lane on, cache off, 7 runs, the statement's `query_duration_ms` from
`system.query_log`):

| | base: total / statement / after | head: total / statement / after |
|---|---|---|
| L33 | 712–738 / 554 / ~171 ms | 737–749 / 604 / ~139 ms (−19 %) |
| L35 | 788–793 / 649 / ~141 ms | 777–788 / 678 / ~104 ms (−26 %) |

Chipnet 10,000-row pages of the L35 / L33 shape (`transactions(node:)` with L35's / L33's selection, `ch1_fix10_chip`,
in process, base / head interleaved): 140–156 → **82–88 ms** and 190–205 → **117–138 ms**.

The HTTP rows above were measured before the wrapping (statements +29 / +50 ms there); the wrapping removes that cost
(last statement row; chipnet documents equal on / off), so through HTTP the head saves the API-side difference: ~30–37 ms
per 10,000-row page locally.

**Not met as asked.** In process the API side falls by a third or more; through HTTP locally by 19–26 %, not half. On
mainnet the API side was the larger term (#104: 311 / 581 ms after statements of 653 / 666 ms, results 27.7 / 45.8 MB
on a 4-CPU pod), and both the result bytes and the decoding scale with the page, so the expected gain there is larger;
L35 under 1 s through HTTP is not assured. The GKE session measures it, with the switch as the A/B (§8).

Where the remaining ~100 ms of L35 goes (CPU profile of the API process through HTTP, no lane): GC ~20 %, the client's
`JSON.parse` ~14 %, row decoding ~20 % (scalar fields 10, grouping 8, relations 4 %), leaf writing and its quoting
check ~10 %, Yoga's body encoding (`Buffer.byteLength` + UTF-8 write) ~8 %.

### Tried and dropped

| Option | Result |
|---|---|
| RowBinary (`RowBinaryWithNamesAndTypes`) with a JS decoder | the L35 result is 15 % smaller, but decoding it into the same tuples took 12.5 ms vs `JSON.parse`'s 10.5 ms: the data is strings, and native `JSON.parse` is as fast as a JS byte decoder; not built |
| Rows built by a generated object literal (`new Function`) instead of one store per field | no measurable change; dropped |
| Group numbers computed in the child level's own subquery (parallel) instead of the final SELECT | L33 674 vs 654 ms, L35 727 vs 715 ms: no better; dropped |
| A shared frozen empty relationship map for leaf levels | no measurable change; dropped |
| Group numbers only for pages of ≥ 2,000 root rows (against the per-`transform` analysis cost) | the library documents page by 10,000 whatever they return (L1 still 105 → 161 ms); superseded by the one-row subquery |
| `--max-semi-space-size` 16–128 MB | no change |
| `JSON.stringify` per leaf instead of the regex check + template | equal (~40 ms per 600 k values either way) |

## 2. Small-node spot check

**Cause (fix-pass-9.md §6).** The totals reference reads the node's created outputs by `transaction_hash IN (the
node's acceptance)`. ClickHouse compares projections by marks: `output` (granularity 128) against `p_script` / `p_category`
(1,024). The IN set comes from a subquery, so it is not known when the projection is chosen; the base table looks like
a full read and `p_script` wins with no key condition at all. The spenders side read `input`'s base table with its bloom
filter. Both read in proportion to the chain, at any chunk count.

**Fix** (`8589685`, `be134cd`): `smallNodeTotalsInHashRangeSql`, the same definition by access paths that follow the node.
Per hash-range chunk:

- the node's acceptance read by node (`p_node_tx` / `p_node_height`), its transactions in the range kept as one array
  (`groupUniqArrayIf`);
- the spenders: `input` by the node's transaction ids (`input.p_txid`), bounded by the ids' min and max so the
  projection is chosen at analysis, their outpoints in the range kept as one array;
- the outputs by `output`'s own key in a **scalar subquery with `SETTINGS optimize_use_projections = 0`**: the setting
  applies to that subquery only (checked in `system.query_log.projections`), so the acceptance and spender reads keep
  their projections. Fix 9 tried the setting on an ordinary subquery, where it is ignored;
- the range filters are aggregate conditions (`…If`), not WHERE terms: a hash range on `tx_acceptance` or `input` is a
  key range of their base table / `p_outpoint` and would be chosen over the node's projections.

It does not read the stored set: it is the query-time definition (outputs of transactions the node accepts, minus
outpoints spent by transactions it accepts), so it stays an independent check of `utxo_set` + `utxo_total`.

**When.** `referenceTotalsChunked` takes `form: 'auto' | 'small' | 'large'` (spot check: `totalsForm`). Auto picks the
small form up to `smallNodeMaxTransactions` = 4 M accepted transactions: each chunk reads the node's whole acceptance and
inputs again, so that part grows with the square of the node. Golden node 39 (the chipnet chain, ~2.8 M transactions) is
small; node 1 is not and keeps the chain-range form and its memory bounds (256 chunks, fix 9). Chunks are the larger of
fix 9's sizing and enough to keep each array at `smallNodeChunkElements` = 2 M (a 2.8 M-transaction chipnet node's
arrays, 2.8 M and 7 M elements, ran in one chunk and answered exactly; 250 k gave node 2 sixteen chunks, 9.7 s, where
four take 7.6 s). The spot check reports the form and chunks it used (`totalsReference`).

**Measured** (`ch1_fix10_syn`: node 1 accepts all 150 M transactions, 242,512,925 UTXOs; node 2 the last 3,030 blocks,
999,720 transactions, 2,858,741 UTXOs; quiet server; statements from `system.query_log` by `log_comment`):

| Node 2 totals reference | chunks | time | rows read | CPU | peak per statement | answer |
|---|---:|---:|---:|---:|---:|---|
| chain-range form (fix 9) | 1 | 17.0 s | 977 M | 117 CPU-s | 1.29 GiB | 3,505 categories, 2,858,741 outputs, digest cf7310a9aedc4bca |
| chain-range form (fix 9) | 4 | 18.9 s | 864 M | 93 CPU-s | 0.36 GiB | equal |
| small form | 1 | 9.1 s | 119 M | 67 CPU-s | 1.83 GiB | equal |
| **small form (auto)** | **4** | **6.5–7.7 s** | **135 M** | **53 CPU-s** | **0.47 GiB** | **equal** |
| small form | 16 | 9.7 s | 201 M | 57 CPU-s | 0.22 GiB | equal |

The whole spot check of node 2's totals (stored totals + reference): **7.5 s** small form vs 25.1 s chain-range form, both
equal to the stored set. The outputs read is now by `output`'s key (`EXPLAIN`: 220,597 of 3,369,368 granules per
chunk), the spenders by `input.p_txid` (2,448 of 365,521 granules), the acceptance by node (977 granules).

On the chipnet copy (one node accepting the whole chain, 2.8 M transactions: auto picks the small form, 4 chunks) the
small form takes 5.0 s against the chain-range form's 4.0 s, equal: a node that is the whole database gains nothing,
and the hourly check costs a second more there.

Node 1 beside it (auto picks the chain-range form): the totals spot check **equal** (3,505 categories, 242,512,925 outputs,
digest 2928b78d3573b812), 256 chunks, 455 s, peak 1.74 GiB per statement: fix 9's sizing and memory bound, unchanged.

**Tests** (`utxo-set.spec.ts`): on both nodes of the two-node fixture, the small form with 1 and 4 chunks answers
exactly as the chain-range form; auto picks the small form for a small node; the spot check reports
`{ chunks, form: 'small' }` and its chunk statements as before. The mismatch-detection specs (a planted stored-row error,
a failing totals statement) run on the small form now, since the fixture's nodes are small.

## 3. Suites (final code, `ef622aa`)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: 17 tsdoc warnings, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 199 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | 183 passed (a first run beside a local API rehearsal had `g1-fix`'s lab-conditions test miss its lease deadline; `g1-fix.spec` alone 12 / 12, the rerun 183 / 183) |
| API specs `build/api/*.spec.js` incl. the chipnet spec (`ch1_fix10_chip`, v1 Hasura) | **93 passed** (fix 9: 92 + the group-number spec) |
| API specs with `CHAINGRAPH_API_FAST_EXECUTION=verify` | no answer differs; the 2 statement-count tests (S6, fix pass 3 cache) fail by design (verify runs every query twice), as in fix 9 |
| API specs with `CHAINGRAPH_API_GROUP_NUMBERS=off` | 93 passed (at `865681a`; the group-number spec sets both modes explicitly since `5c558c0`) |
| chipnet catalogue (`catalogue-run --latency 9`, v1 Hasura, `NODE_ENV=production`) | **63 / 63 equal** (48 equal, 15 equal-unordered as before), v2 p50 median **55.4 ms** (fix 9: 56.1); no document slower or faster than fix 9's run by more than max(20 ms, 20 %) |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed each (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg; at `8db4575`, no Postgres path changed since) | 102 passed, 1 skipped |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`.

## 4. Deployment

No change to the defaults: `CHAINGRAPH_API_GROUP_NUMBERS` defaults to on; the kit keeps 3 workers x 1 heavy-lane thread
per 4-CPU pod (fix-pass-9.md §7).

## 5. Design calls

| Call | Rationale |
|---|---|
| Group numbers in the final SELECT, `transform` over a per-parent key array | exact (first parent row with the key; NULLs never match), one hash table per relationship; computing them in the child's own subquery was no faster |
| Group numbers on by default, with an off switch | they halve L35's ClickHouse result and cut the API side by a third in process; with the levels read as columns they cost ClickHouse nothing measurable (catalogue equal to fix 9). The switch lets the GKE session A/B them on mainnet without a new image |
| The final SELECT over a one-row subquery of the levels | an identifier that is a column is resolved once; an alias of a scalar subquery inside a lambda was re-resolved (and its subquery tree hashed) per use |
| Compiled writer per field type, one appended string | graphql-js type checks per value were the largest single cost of the writer; V8's rope beats an array of ~3 M pieces |
| No binary transport | for string-dominated results a JS decoder is not faster than native `JSON.parse` (12.5 vs 10.5 ms on L35's result) |
| Small-node form reads outputs in a scalar subquery with `optimize_use_projections = 0` | the per-subquery setting is honoured for scalar subqueries (not for FROM subqueries); the outputs read by key, the rest keeps its projections |
| Small form from the node's acceptance, never from `utxo_set` | an independent check: the stored set is compared with the query-time definition, not with itself |
| Auto threshold 2 M accepted transactions; arrays ≤ 250 k per chunk | the form re-reads the node per chunk (cost ~ size²); 4x margin under ClickHouse's 1 M elements per value |
| Range filters as `…If` aggregates in the small form | a WHERE hash range is a key range of the base table / `p_outpoint`, which ClickHouse would pick over `p_node_tx` / `p_txid` |
| Measure with `NODE_ENV=production` | the image sets it (images/agent/Dockerfile); development mode adds graphql-js's multi-realm checks |

## 6. Commits

| Commit | |
|---|---|
| `9f672e3` | perf(api): fast executor compiles a writer per field type and appends to one string |
| `a3ea828` | perf(api): relationship levels matched by group numbers instead of key texts |
| `990c818` | chore(measure): api-side.mjs --out writes the response body |
| `8589685` | perf(store): small-node form of the spot-check totals reference |
| `be134cd` | feat(store): spot-check reports the totals reference's form and chunks |
| `8db4575` | fix(measure): api-side.mjs runs subscription documents with header comments as queries |
| `443d790` | feat(api): CHAINGRAPH_API_GROUP_NUMBERS=off matches levels by key texts; test both answer byte for byte |
| `865681a` | style(api): group-number spec: lint |
| `64ef678` | perf(store): small-node totals form up to 4 M transactions, 2 M elements per chunk array |
| `5c558c0` | test(api): group-number spec sets both modes explicitly |
| `ef622aa` | perf(api): group numbers read the levels as columns of a one-row subquery |

## 7. Housekeeping

`ch1_fix10_syn`, `ch1_fix10_chip`, the Hasura container `ch1-fix10-hasura` and its metadata database
`ch1_fix10_hasura_meta`, and the worktrees `/tmp/fix10/wt-{base,g1,w}` removed; every API process started for the
measurements stopped. A note on process hygiene: one cleanup during the pass stopped the local API servers with
`pkill -f "build/api/main.js"` (a pattern any other session's local API would also match); later ones used PIDs.

## 8. What the next GKE session should verify

Fix pass 9's list (fix-pass-9.md §9) still stands; this pass adds to items 4 and 6.

1. **X6** on both nodes: rows read back near fix 7's 5.4 M (fix 8: 1.04 B); X6 ≈ 0.7 / 0.9 s. Every statement whose inputs
   level reads by id alone lists `input.p_txid`; X6's inputs level reads by hash and id.
2. **Idle repeated L15**: a sequential client, admission on: L15 ≈ 1.7 s every run (fix 8: 13.7 s on the second),
   `stats.contended` stays 0, L17 is not refused.
3. **Refusal and light p95 with the deployment default** (3 workers x 1 heavy thread per 4-CPU pod): refusal p95 < 250 ms,
   light p95 < 500 ms at A1 / A3; startup line ends `workers=3 heavy_threads=1`; backstop 442; `GET /admission`
   `loopUtilisation` of the main loops stays low under overload. One worker x 1 thread for comparison with #104.
4. **L33 / L35 through HTTP, by phase** (fix 8: API side 311 / 581 ms, results 27.7 / 45.8 MB): the API side at most
   ~0.6 x fix 8's, L35 under 1 s; the ClickHouse result about half for L35. Then the same with
   `CHAINGRAPH_API_GROUP_NUMBERS=off`: if the statements' extra time exceeds the API side's gain, turn it off.
5. **Probes on golden**: L17 key CPU (fix 8 1.1 s), the L6 block_window group, node-1 X4 (the block level by id): `EXPLAIN
   indexes = 1` per level and the qlog key CPU. Explain each level's scalar subquery on its own, with the WITH items
   before it: an EXPLAIN of the whole statement evaluates the scalars during analysis and shows only the final SELECT
   over constants (found while preparing the kit).
6. **Node 39's totals spot check**: the small form (`totalsReference.form = small`), equal, near 9 s (fix 8: 26.7 s);
   the chain-range form (`--totals-form large`) for comparison, equal to it. Node 1's whole-set check: 256 chunks, peak
   ≤ ~2.6 GiB per statement (fix 8: 128 chunks, 5.20 GiB).
7. **Catalogue** on both nodes vs v1, the floor (125 / 78 ms) and fix 8 (112 / 80 ms).
