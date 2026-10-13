<!-- cspell:ignore chipnet paryon backfill backfilled sats EWMA cgapi tsdoc txid synth unhex OVERLOADED prewhere qlog qlog's uncontended analysed VCMT worktree dafedec -->

# Fix pass 7: the mainnet UTXO / admission session of 2026-10-13

Local only: ch1-local ClickHouse 26.8 (Docker VM, 14 CPUs, 34 GiB server memory), ch1-pg Postgres 14, Hasura
`chaingraph/hasura:dev-build` over `chipnet_pg` for v1. Inputs: [utxo-run-log.md](utxo-run-log.md), the decision
document `paryon_kubernetes-consolidation/docs/chaingraph/decisions/2026-10-13_clickhouse-utxo-verification.md` and the
raw results `chaingraph-performance/2026-10-13-utxo-*/` (read only). Scratch databases `ch1_fix7_syn` (synthetic,
mainnet scale), `ch1_fix7_chip` (chipnet copy), `ch1_fix7_m_*`; all dropped afterwards. Raw results stay local
(`/tmp/fix7/`, not committed).

## 1. Summary

| # | Session finding | Cause | Fix | Before → after (local) |
|---|---|---|---|---|
| 1 | Admission worse than none (A1, A3); refusals after up to 10 s; heavy cap 8 threads | heavy lane 6 × 8 threads, 1 M-row heavy threshold, 10 s queue wait | 250 ms waits + predicted-wait refusals; server-default threads; priority and a thread cap only for *contended* heavy statements; heavy from 10 M rows; light = cores, heavy = cores / 2 | §2: at the loads the server carries, as admission off; refusals p95 10 s → 0.26 s (A3); A3 light p50 / p95 1,205 / 7,961 (off) → 60 / 536 ms |
| 2 | Small documents' key CPU 6 → 93 ms (p_node_tx plans) | the window's id set was a key condition of `p_node_height` (id as its third key column): generic exclusion over every mark of the node | `p_node_height` keyed by (node, height); with p_node_tx, the window's acceptance read by id alone | §3: L14 key CPU 29 → 1 ms, L21 85 → 4 ms, L34 9 → 3 ms |
| 3 | §11 q1: option (b) on `input` | inputs level read ~9 scattered granules per transaction in every part | `input.p_txid` (slim, ORDER BY transaction_internal_id); inputs read by hash and id | §4: L35 145 → 65 M rows, 11.8 → 7.6 CPU-s, 2.47 → 1.23 s; L33 115 → 13 M rows |
| 4 | Window fallbacks L23 / L24 / L28 | page never full (loan spends that are not closures): capped window → second statement | 16× window when the filter has conjuncts beyond its candidates | §5: L23 1,040 → 352 ms, 171 → 42 M rows, 2 → 1 statement |
| 5 | Whole-set checks > 280 GiB; failed scope "skipped", exit 0 | single-statement FULL OUTER JOIN / totals over the whole chain | hash-range chunks (key ranges), one pass per chunk; failed scope = failed check (throws) | §6: synthetic 242.5 M UTXOs: MEMORY_LIMIT after 137 s → equal, 64 chunks, 12.2 min, 4.19 GiB peak |
| 6 | Spend build +37 % | not the fork's projections (060 incl. p_txid was applied after the backfill); `p_node_tx` became a default DDL item and was written during the backfill | documented; kit change | §7 |
| 7 | `--utxo-chunks 64` refused | compare.mjs took 16^k only | any power of two 2–65536 | §8 |

**No data discrepancy was found.** Every equality check of this pass is equal: chipnet catalogue 63/63, every
`checkUtxoSet` run (chipnet whole set; synthetic whole set, 242,512,925 outpoints, totals and digest), the API specs'
independent evaluator.

## 2. Admission (item 1)

### 2.1 What went wrong on mainnet

- **Lane classification.** A statement was heavy from 1 M rows read. On mainnet, point and signal reads of 1–3 M rows
  (L41 / L43 block tips 2.1–2.3 M rows, X4 2.1 M, X2 1.2 M; 0.09–0.29 CPU-s each, from `qlog-paryon.tsv`) shared the
  6 heavy slots with L33 / L35 / L23 (100–150 M rows, 11–13 CPU-s). Light documents such as L43 and X4 waited in the
  heavy queue (overload B: light p95 6.8 s).
- **Thread cap.** Heavy statements ran at `max_threads = 8` whatever the load: idle L15 7.0 s vs 1.6 s direct.
- **Slow refusals.** A full heavy lane queued statements for up to 10 s before refusing them.
- At 10 QPS (~23 % of the 48 cores at 1.1 CPU-s per request) the server carries the mix unaided; the six 8-thread
  slots turned it into 16 refusals and a 13 s p95.

### 2.2 Design (`src/api/admission.ts`)

- **Fast refusals.** Each lane waits at most 250 ms (`CHAINGRAPH_API_{LIGHT,HEAVY}_MAX_WAIT_MS`, was 5 s / 10 s). A
  statement whose *predicted* wait is longer is refused at once. The prediction uses the running statements' ages and
  the lane's recent statement time S (EWMA): a running statement younger than S ends in S − age, an older one in its age
  again; queue position p gets the p-th slot to free.
- **No fixed thread cap.** Heavy statements run with the server's default `max_threads` (every core). ClickHouse 26.8's
  CPU slot scheduler (`concurrent_threads_soft_limit_ratio_to_cores` 2, `max_min_fair`) shares the cores between
  running statements; a fixed cap only slows an uncontended statement.
- **Contended heavy statements only.** A heavy statement that took the heavy lane's last slot, or waited in its queue,
  gets `max_threads` ≈ 2 per heavy slot (4 at 48 cores / 24 slots) and `priority = 2` (light statements run at 1).
  `CHAINGRAPH_API_HEAVY_CONTENDED_AT` (fraction of the slots, default 1),
  `CHAINGRAPH_API_HEAVY_CONTENDED_MAX_THREADS`, `CHAINGRAPH_API_HEAVY_PRIORITY` (0: none).
- **Classification.** Heavy from 10 M rows read (`CHAINGRAPH_API_HEAVY_ROWS`).
- **Lane sizes** (from the server's cores): light = max(8, cores), heavy = max(2, round(cores / 2)), heavy queue 1 per
  slot. At 48 cores: 48 light, 24 heavy. A fixed `CHAINGRAPH_API_HEAVY_MAX_THREADS` keeps the fix-pass-5 rule
  (cores / threads). The API user's backstop default is now `pods × 72 + 10`.
- Startup line, e.g.: `admission: on cores=48 (clickhouse) light=48 heavy=24 heavy_max_threads=server default
  heavy_contended=1:max_threads 4,priority 2 heavy_rows=10000000 max_wait_ms=250/250 max_concurrent_queries_for_user=…`.

### 2.3 Local load test (`scripts/measure/admission-load.mjs`)

The mainnet A1 / A3 shapes against an API process (`build/api/main.js`, response cache off) on `ch1_fix7_syn`, one
process per arm, open loop, 60 s per level, 30 s client timeout. A1: the soak mix L40–L47, L12, L31, L35, X2. A3
(overload B): light L13 L20 L25 L40 L43 L46 X4 X8 at 10 QPS + heavy L23 L30 L33 L35 X12 X13. Rates are scaled to the
14-core server by CPU: A1 6 / 12 / 18 QPS = 31 / 62 / 93 % of the cores (server CPU from query_log; mainnet 10 QPS was
~23 %), A3 heavy 6 QPS ≈ 130 % (mainnet 8 QPS ≈ 110 %). Arms: **before** = `2bad457` (the GKE image's code),
**off** = this branch with `CHAINGRAPH_API_ADMISSION=off`, **on** = this branch's defaults (14 cores: light 14,
heavy 7).

| Shape | Arm | ok / sent | refused (OVERLOADED) | refusal p95 | timeouts | p50 / p95 ms |
|---|---|---:|---:|---:|---:|---:|
| A1 6 QPS | before | 346 / 361 | 15 (4.2 %) | 10,405 ms | 0 | 72 / 702 |
| | off | 360 / 360 | 0 | – | 0 | 43 / 1,111 |
| | **on** | **360 / 360** | **0** | – | 0 | 55 / 1,208 |
| A1 12 QPS | before | 687 / 721 | 34 | 10,247 ms | 0 | 46 / 860 |
| | off | 720 / 720 | 0 | – | 0 | 54 / 1,155 |
| | **on** | **720 / 720** | **0** | – | 0 | 98 / 1,393 |
| A1 18 QPS (93 % CPU) | before | 1,019 / 1,080 | 61 | 10,008 ms | 0 | 35 / 412 |
| | off | 1,080 / 1,080 | 0 | – | 0 | 213 / 2,021 |
| | **on** | 1,044 / 1,080 | **36 (3.3 %)** | 1,285 ms | 0 | 239 / 3,002 |
| A3 light 10 + heavy 6 | before | 608 / 960 | 352 | 10,025 ms | 0 | light 53 / 271 (71 refused) |
| | off | 935 / 962 | 0 | – | **27** | light **1,205 / 7,961** |
| | **on** | 841 / 960 | 119 (heavy only) | **259 ms** | 0 | light **60 / 536** (0 refused) |

- **"before" reproduces the mainnet failure**: at 6 QPS (a third of the cores) 4 % of requests are refused, after a
  10 s wait.
- **At the loads the server carries (A1 6 and 12 QPS) admission on equals off**: no refusal, same p95 ±0.2 s (run to
  run noise on this VM is of that order: the database is ~10× the page cache).
- **At saturation (A1 18 QPS, 93 % of the cores)** admission refuses 3.3 % (fast: p50 213 ms; the p95 of 1.3 s includes
  requests refused at their second statement) and its p95 is 3.0 s vs 2.0 s. This is the cost of protecting light
  statements: contended heavy statements are paused while light ones run. **Flag**: strictly, "never worse than off"
  holds up to ~60 % CPU here, not at 93 %; `CHAINGRAPH_API_HEAVY_PRIORITY=0` removes it (see below) at the price of
  light latency under overload.
- **Overload (A3)**: refusals are fast (p95 259 ms, all heavy); no client timeout (off: 27); light documents p50 60 /
  p95 536 ms (off: 1,205 / 7,961).

Variants tried (A1 12 / 18 QPS and A3, same harness), why the defaults are what they are:

| Variant | A1 18 refused / p95 | A3 light p50 / p95 | Verdict |
|---|---|---|---|
| priority 2 on every heavy statement, adaptive threads (cores / running), light 8 / heavy 4 | 21 / 3.5 s | 45 / 977 | heavy slowed at normal load |
| server threads, light 14 / heavy 7, no priority | 0 / 3.0 s | 300 / 997 | light unprotected |
| + contended max_threads 4, no priority | 0 / 4.4 s | 281 / 787 | light unprotected |
| contended (≥ 75 % of slots) priority + threads 4 | 51 / 3.1 s | 45 / 447 | too many refusals |
| ClickHouse workloads (CPU resource, light priority 0 / heavy 1) instead of `priority` | 5 / 4.8 s | 380 / 1,072 | worse light latency; server-global DDL |
| **contended = last slot or waited; threads 4 + priority 2; 250 ms waits (default)** | 36 / 3.0 s | **60 / 536** | chosen |

- Decision (autonomous): protect light statements under overload (A3) over the last 3 % of throughput at saturation;
  one-line rationale: refusals are now fast and explicit, light documents are the signals the library polls.
- Local caveat: light latency under heavy load is partly disk reads here (the synthetic database is 324 GiB, the VM's
  page cache ~34 GiB); on GKE the data is cached and CPU is the only contended resource.

## 3. Small-document planning regression (item 2)

**Why.** Fix pass 6 bound the candidate window's acceptance read by the candidates' ids
(`tx_acceptance.transaction_internal_id IN (ids)`) next to their hashes, for `p_node_tx`. Server traces
(`send_logs_level = trace`) on the chipnet copy and the synthetic database show three analyses per acceptance read:

| Index | Key condition | Steps per part | Effect |
|---|---|---|---|
| base key (hash) | hash IN set | ~1,100–2,900 (time-ordered parts) | drops few granules |
| `p_node_height` (node, height, **id**) | node = n **and id IN set on its third column** | descends to every mark of the node | drops nothing; the cost |
| `p_node_tx` (node, id) | node = n and id IN set | ~300–600 | chosen |

A set on the third key column excludes no range in which the height varies, so ClickHouse's generic exclusion search
walked every mark of node 1 in every part of `p_node_height` before choosing `p_node_tx`. That is a constant per
statement proportional to the node's acceptance rows (mainnet node 1: 420 M rows, ~90 ms), which is why small documents
(few candidates, little else to do) regressed 2–3× and large ones did not notice.

**Fixes.**
- `p_node_height` is keyed by (node, height) only (`62f6e10`): its readers (X12 / X13 / X16 block filters) filter by node
  and height; the id set is no longer a key condition there (binary search on node).
- With `p_node_tx` present the window's acceptance is read by id alone (`c574886`): the hash set is not analysed against
  the base key of every time-ordered part as well. Exact: one id per hash, and the caller compares hashes.

Synthetic, golden layout (time-ordered `tx_acceptance`, 12 parts, 150 M rows), in-process, p50 of 5, key CPU =
`FilteringMarksWithPrimaryKeyMicroseconds`:

| Document | before (`2bad457`) wall / key CPU | after wall / key CPU |
|---|---|---|
| L14 | 144 ms / 29 ms | 87 ms / 1 ms |
| L21 | 109 / 89 | 89 / 4 |
| L34 | 110 / 9 | 96 / 3 |
| L2 | 75 / 1 | 48 / 1 |
| L1 | 241 / 1 | 108 / 1 |
| L40 | 92 / 12 | 58 / 7 |
| X12 / X13 / X16 | 91 / 427 / 429 ms | 74 / 406 / 370 ms (p_node_height still used) |

`p_node_height` rebuild on the synthetic copy: 14 s. **Existing databases keep the old key** until the projection is
dropped and re-added (`ALTER TABLE tx_acceptance DROP PROJECTION p_node_height`, re-apply the DDL, `MATERIALIZE
PROJECTION p_node_height`); `checkKeys` warns. Every acceptance fact stays per node.

## 4. `input.p_txid` (item 3, owner choice §11 q1 = (b) on input)

- DDL 060: `ALTER TABLE input ADD PROJECTION p_txid (SELECT transaction_hash, input_index, transaction_internal_id,
  outpoint_transaction_hash, outpoint_index, sequence_number, commit_seq ORDER BY transaction_internal_id)`; slim (no
  `unlocking_bytecode`: a level selecting it reads the table).
- The `inputs` relation reads `transaction_hash IN (page hashes) AND transaction_internal_id IN (page ids)`; ClickHouse
  takes whichever selects fewer granules (`640e212`).
- **Layout matters.** Golden's `input` is chunked by *outpoint* hash (the backfill's 21-input transform), so every part
  spans every spender hash and a lookup by transaction hash probes all parts (golden: 15 of 15). The synthetic
  generator's golden layout first chunked input by the spender's hash (one part per lookup), where p_txid gains
  nothing; `build.sh golden` now rebuilds input by outpoint hash.

Synthetic, input chunked by outpoint hash (12 parts, 374 M rows), current code, p50 of 5:

| | without p_txid | with p_txid |
|---|---|---|
| L35 (10,000-row page) | 2,470 ms, 144.9 M rows, 11.8 CPU-s | **1,225 ms, 64.7 M rows, 7.6 CPU-s** |
| L33 | 1,462 ms, 115.3 M rows, 9.0 CPU-s | **1,040 ms, 12.9 M rows, 4.8 CPU-s** |
| L40 / L23 / L14 | 57 / 377 / 84 ms | 75 / 407 / 114 ms (unchanged within noise) |

- Size: 24.6 GiB for 374 M inputs (p_outpoint 19.6 GiB on the same parts; the table 69 GiB with both): **+7.6 % of the
  database**. Mainnet (~1.03 B inputs, p_outpoint 52 GiB): **~65 GiB** by either ratio (above the ~50 GiB expected).
- Build: `MATERIALIZE PROJECTION p_txid` 44 s on 14 cores (32 s on the spender-chunked layout); mainnet ≈ 2.75× the
  rows on 48 cores: ~40–90 s.
- Expected mainnet: the inputs level was 5.58 of L35's 12.9 CPU-s with 96.5 M rows; with p_txid L33 / L35 should drop
  by roughly half (L35 direct 1.2 s → ~0.6–0.8 s). Whether L33 / L35 get under 1 s through the API is for GKE to show
  (§10).

## 5. Window fallbacks (item 4)

- L23 / L24 / L28 select loan spends (`has_input`) that are **not** closures (`_not: { has_output }`), so most
  candidates fail the filter and a page is never full: the capped window (page + 25 % + 100 candidates) could never
  decide, and the API ran the full statement as a second one.
- A filter with more than one top-level conjunct (`transactionConjuncts`, `_and` flattened) now checks a 16× wider
  window (~200 k candidates for a page of 10,000) in the first statement (`52a9dd2`). L33 / L35 / L40 / L42 (one
  conjunct) keep the narrow window. Exact either way; the fallback stays for larger candidate sets.

| synthetic | before | after |
|---|---|---|
| L23 | 1,040 ms, 171 M rows, 8.1 CPU-s, **2 statements** | 352 ms, 42 M rows, 1.9 CPU-s, 1 statement |
| L28 | 944 ms, 171 M, 7.9 CPU-s, **2** | 383 ms, 42 M, 1.9 CPU-s, 1 |
| L24 | 583 ms, 138 M, 5.0 CPU-s, **2** | 193 ms, 22 M, 0.7 CPU-s, 1 |

## 6. Whole-set equality checks (item 5)

- `checkUtxoSet` (`f8aa9ba`, `bc22410`): per hash range of the outpoint's transaction (`hashRangeChunks`, ranges of the
  leading 16 bits, key ranges of `output`, `input.p_outpoint` and `tx_acceptance`), one statement compares the
  query-time reference with the stored read row for row (FULL OUTER JOIN, digest per row) and sums the reference's
  per-category totals; a second checks the stored invariant (sum of signs 0 or 1). Totals are summed in the tool and
  compared with the stored totals. Chunks: ~8 M outputs per chunk by default (`checkChunkCount`), `options.chunks` /
  `concurrency` / `maxExecutionSeconds` to override.
- Spot-check totals scope: the same chunked reference (`referenceTotalsChunked`). **Off by default** (rule U6;
  `CHAINGRAPH_CLICKHOUSE_UTXO_SET_SPOT_CHECK_TOTALS=on` to enable): it still reads the whole chain once.
- **A failed scope fails the check.** Spot-check scopes whose statement fails are `failed` (was `skipped`);
  `spotCheckUtxoSet` throws `UtxoSpotCheckFailedError` after running every scope (`throwOnFailure: false` returns
  them); the maintainer logs `utxo_set_spot_check_failed` at error level and counts
  `chaingraph_utxo_set_spot_check_failed_total`. `checkUtxoSet` throws on any failed chunk.

| | single statement (`2bad457`) | chunked |
|---|---|---|
| chipnet copy (1.21 M UTXOs) | 24.0 s, 5 statements, peak 3.28 GiB | 1 chunk: 4.7 s, peak 3.31 GiB; 16 chunks: 0.46 GiB |
| synthetic (242.5 M UTXOs, 431 M outputs, 34 GiB server) | **MEMORY_LIMIT after 137 s** (would use 36.3 GiB) | **equal**, 64 chunks, 12.2 min, **peak 4.19 GiB**, 3,887 CPU-s |
| synthetic, first chunked form (128 chunks, 3 statements per chunk) | | equal, 19.2 min, 2.36 GiB |

Mainnet estimate (~2.3× the synthetic's outputs, 2.8× its acceptance rows; 48 cores, data cached): ~128 chunks, a few
GiB per statement, roughly 5–15 min. The spot-check totals scope reads the reference once more (synthetic: 3.6 s per
chunk, 1.86 GiB).

## 7. Spend-index backfill build +37 % (item 6)

- Per-chunk durations (`backfill-golden-query-log.tsv`): every golden-28 chunk took 15.3–19.6 s on 10-13 vs 11–17 s on
  10-12 and 6.6–17.7 s on 10-11; same rows, same peak memory (~31 GiB).
- The fork's new projections did **not** run then: the kit applies 060 (incl. `block_transaction.p_txid`) at step 8,
  after the backfill (`skip 060_projections.sql` in `backfill-golden.log`). The `spend` DDL (070) is unchanged since
  10-11, and the frozen 28-spend / 10-join SQL is identical between the two sessions.
- What differed: the backfill DDL job applied **`p_node_tx`** (`items=p_node_tx`, a default DDL item since 10-12 noon;
  the floor session's DDL ran with `optional=(none)` and built p_node_tx afterwards in 48 s), and the kit added the tip
  window to step 26. So step 26's tx_acceptance INSERT (420 M rows, many parts) wrote p_node_tx inline and the merges
  that follow it (VCMT with `deduplicate_merge_projection_mode = 'rebuild'`: every merge re-sorts the projection) ran
  while step 28 built the spend index.
- Local check: a copy of the synthetic `tx_acceptance` (150 M rows, time order) inserted in one INSERT … SELECT, as
  step 26 does, then a spend-like join (input × output over 24 of 256 hash bytes, ~15 s quiet) three times while the
  merges run and three times after they settle:

  | | merges after the insert | merge CPU | join during merges | join quiet |
  |---|---|---|---|---|
  | without p_node_tx | 26 merges, settled after 80 s | 114 CPU-s | 22.7 / 18.9 / 18.5 s | 14.4 / 15.5 / 15.4 s |
  | with p_node_tx | 26 merges, settled after 110 s | **264 CPU-s** (2.3×) | 26.8 / 18.5 / 19.5 s | 14.3 / 15.1 / 14.1 s |

  The merges that follow a bulk acceptance insert slow a concurrent CPU-bound build by 25–85 % while they run, and
  p_node_tx makes them 2.3× the CPU and ~40 % longer. Plausible as the main cause of the mainnet +37 %; not proven
  there (the session exported no `system.part_log`).
- **Not fixed in the fork** (the cost is real and belongs to the projection); **kit recommendation**: run the backfill
  DDL with `CHAINGRAPH_CLICKHOUSE_DDL_SKIP=p_node_tx` and add + materialize it with 060 in step 8 (as the floor session
  did, 48 s), or wait for tx_acceptance merges to settle before step 28.

## 8. compare.mjs chunk counts (item 7, kit bug)

`scripts/parity/compare.mjs` accepts any power of two from 2 to 65536 for `--hash-chunks` / `--utxo-chunks` (ranges by
the leading 16 bits; bounds and labels unchanged for 16, 256, 4096, 65536) (`93c7c73`). The kit's `--utxo-chunks 64`
now runs. The F1g Postgres side stays ~10 h per node (the decision document's §8); prefer `checkUtxoSet` (§6).

## 9. Chipnet catalogue

`ch1_fix7_chip` (a copy of `chipnet_ch_pp2` in the current DDL incl. `input.p_txid` and the new `p_node_height`,
authchain index backfilled, UTXO set built; v1 = Hasura over `chipnet_pg`), 9 warm runs, HTTP cache off:

| | equal | v2 p50 median |
|---|---:|---:|
| code before (`2bad457`) on the same database | 63 / 63 | 74.6 ms |
| this branch | **63 / 63** | 73.6 ms |

Largest moves: X6 712 → 661 ms, L33 387 → 356 ms, L17 93 → 77 ms; X5 / X13 / L7 up 20–50 ms in the full run, within
noise when re-run alone (X5 in process 93 vs 89 ms, X13 389 / 380 vs 372 / 380 ms). Chipnet's `tx_acceptance` is small
(2.8 M rows), so §3's planning cost hardly shows there; the synthetic database (§3) and GKE are the measures.

## 10. What the next GKE session should verify

1. **DDL / build:** fresh database from this branch's head. `system.projections`: `tx_acceptance.p_node_height
   (node_internal_id, height)`, `input.p_txid (transaction_internal_id)`; `clickhouse-ddl` no key warning. Time
   `MATERIALIZE PROJECTION p_txid` on input and record its size (expected ~65 GiB, ~40–90 s). Run the backfill DDL with
   `CHAINGRAPH_CLICKHOUSE_DDL_SKIP=p_node_tx` and materialize p_node_tx with 060 (§7): spend build back to ~200 s.
2. **Startup line:** `admission: on cores=48 … light=48 heavy=24 heavy_max_threads=server default
   heavy_contended=1:max_threads 4,priority 2 heavy_rows=10000000 max_wait_ms=250/250`; backstop ≥ `pods × 72 + 10`
   (the kit's api-user profile must be re-rendered: `CHAINGRAPH_CLICKHOUSE_API_STATEMENTS_PER_POD` default 72).
3. **A1 cache off** 10 / 20 / 30 QPS and the same with `CHAINGRAPH_API_ADMISSION=off` back to back: at 10 QPS no
   refusal and p95 within ~10 % of off; OVERLOADED p95 < 250 ms wherever refusals occur; no client timeout, no
   MEMORY_LIMIT. Idle catalogue: L15 ≈ its direct time (~1.6 s), not 7 s.
4. **A3 overload B:** light p50 < 100 / p95 < 500 ms, heavy refusals p95 < 250 ms. If light misses its bound while
   the A1 numbers hold, try `CHAINGRAPH_API_HEAVY_CONTENDED_AT=0.75`; if A1 at 20–30 QPS refuses more than off times
   out, `CHAINGRAPH_API_HEAVY_PRIORITY=0` (§2.3 table).
5. **F3 restated, node 1:** L14 / L21 / L2 / L9 / L24 / L40 / L42 key CPU back to the floor's range (< 50 ms; floor 6–28
   ms); the qlog's `p_node_height` analysis absent from their traces.
6. **L33 / L35** direct and through the API (P1: < 1 s); their inputs level reads `input.p_txid` (`projections` in
   query_log) and ~10× fewer rows than 96.5 M (levels probe, `ch1-levels.sh`).
7. **P3:** no window fallback on L23 / L24 / L28 (one `transactions` statement per request).
8. **U5:** `checkUtxoSet` node 1 + 39 whole set (chunked; `ch1-utxo.sh check` with the image's tool): equal, peak
   memory per statement in query_log a few GiB, wall time recorded. Spot-checks: the totals scope is off by default;
   `spot-totals` with `CHAINGRAPH_CLICKHOUSE_UTXO_SET_SPOT_CHECK_TOTALS=on` or the tool's `--scopes totals`: equal,
   memory bounded. A failing scope must now exit non-zero (kit: `ch1-utxo-job.mjs` reads `failed`, not `skipped`, and
   catches `UtxoSpotCheckFailedError`).
9. Kit: `ch1-parity.sh golden-utxo --utxo-chunks 64` runs (§8).

## 11. Commits

| Commit | |
|---|---|
| `f8aa9ba` | fix(store): whole-set UTXO checks in hash-range chunks; a failed scope fails the check (§6) |
| `93c7c73` | fix(parity): compare.mjs accepts any power of two as a chunk count (§8) |
| `52a9dd2` | perf(api): wider candidate window when the filter has conjuncts beyond its candidates (§5) |
| `640e212` | perf(store,api): input.p_txid; inputs read by hash and id (§4) |
| `62f6e10` | perf(store): tx_acceptance.p_node_height keyed by (node, height) only (§3) |
| `dafedec` | style(store): tsdoc and spelling |
| `c574886` | perf(api): with p_node_tx, the candidate window reads acceptance by id alone (§3) |
| `ddbeef0` | fix(api): admission no worse than none at loads the server carries; fast refusals; load test; synthetic golden layout (§2) |
| `bc22410` | perf(store): the chunked UTXO check compares rows and sums totals in one pass (§6) |
| (docs) | this document |

The GKE image should be built from the docs commit (head).

## 12. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 21 tsdoc warnings of `2bad457`, no errors) |
| unit (`ava --match='!*[e2e]*'`) | 192 passed, 1 todo |
| ClickHouse store specs `build/store/clickhouse/*.spec.js`, `--concurrency=4` | 181 passed |
| API specs `build/api/*.spec.js` incl. the chipnet spec (`CHAINGRAPH_API_CHIPNET_DATABASE=ch1_fix7_chip`, v1 Hasura) | 83 passed (chipnet catalogue equal) |
| chipnet catalogue (`catalogue-run.js`) | 63 / 63 equal (§9) |
| e2e ClickHouse, UTXO on / off (`e2e.spec` + `mempool-cleanup.spec`) | 56 / 56 passed (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed, 1 skipped |

e2e runs with `CHAINGRAPH_E2E_INTERNAL_API_PORT=3301`, `CHAINGRAPH_E2E_NODE_PORT_BASE=19533`.

Housekeeping: `ch1_fix7_syn`, `ch1_fix7_chip`, `ch1_fix7_m_*` dropped; the Hasura container `ch1-fix7-hasura` and its
metadata database `ch1_fix7_hasura_meta` removed; the ClickHouse workloads / resource of the variant test dropped;
the temporary worktree `/tmp/fix7/wt-before` removed.
