<!-- cspell:ignore chipnet seqs dedup coinbases unhex subquery denormalised debeedb -->

# Self-contained inputs and the asynchronous spend index

Date 2026-10-11. Owner decision 2026-10-11 on the input denormalisation plan
(`paryon_kubernetes-consolidation/docs/chaingraph/plans/clickhouse-input-denormalisation.md`, "Mainnet results"):
option A (drop the spent-output columns from `input`) plus option C (an asynchronous spend index). Local only
(ch1-local ClickHouse 26.8, ch1-pg, local BCHN chipnet). Inputs: [mempool-fill-fix.md](mempool-fill-fix.md),
[fix-pass-3.md](fix-pass-3.md), [g1-fix-pass-2.md](g1-fix-pass-2.md), [utxo-off.md](utxo-off.md),
[wp4-commit-and-visibility.md](wp4-commit-and-visibility.md), [phase2-spike.md](phase2-spike.md),
[chipnet-lab.md](chipnet-lab.md).

| Step | Commit |
|---|---|
| 1 + 2. Drop the spent-output columns, self-contained writes; the spend index and its read rule | `debeedb` |
| 3. Consumers: parity `spend` / `spend_join`, chipnet spend check, golden v1 (kit repo) | `7f3694d` (kit: `62997d3`) |
| 4. Transaction-hash lookup | `7ee8602` |
| Fix: block UTXO deltas server-side (gate: 100k-tx blocks overflowed a query parameter); no empty tail; measurement script | `b24be5a` |
| 5 + 6. Measurements, this document, `ddl/README.md` | `b24be5a` (README) and the docs commit |

## 1. What was removed

- **`input` stores the input's own fields only:** `transaction_hash`, `input_index`, `transaction_internal_id`,
  the outpoint, `sequence_number`, `unlocking_bytecode`, `commit_seq`. Gone: the spent output's `value_satoshis`,
  `locking_bytecode` (+ MATERIALIZED `locking_bytecode_prefix`), `token_category`, `fungible_token_amount`,
  `nonfungible_token_capability`, `nonfungible_token_commitment` (+ MATERIALIZED key).
- **Projections:** `p_spent_script` and `p_spent_category` are gone. `p_outpoint` is slim (outpoint, input key,
  `commit_seq`) at granularity 128 with 4 KiB compressed blocks, like `output`. A projection takes those settings
  (`WITH SETTINGS`, as `output`'s projections already did), so no separate table was needed; `EXPLAIN` shows the
  outpoint join reading `p_outpoint` (chipnet: 586 of 54,353 granules for a 110-spend address).
- **`block.generated_value_satoshis`** is gone: readers compute it from the block's inputs joined to the outputs
  they spend (Postgres computes it too, `block_generated_value_satoshis`). The checker does exactly that.
- **Write path:** `pending_spend`, `input_stand_in`, `input_stand_in_resolution`, `stand-in.ts` (607 lines, spec
  735), the `fill_pending` / `incomplete` commits, the parked save (`SaveBlockResult.committed`), the
  pending-spend wait and timeout (`pendingSpendTimeoutMs`, `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS`), the
  spent-output lookup of block commits, the stand-in step of the snapshot (`void` no longer carries hidden seqs),
  and the mempool orphan pool (`orphanGraceMs`, `maxOrphans`: it existed so a child's input rows could wait for
  the parent's output). Every row a commit writes comes from that commit's own transactions. No block commit looks
  up a spent output or waits for one.
- **UTXO off** (the v2 configuration): nothing anywhere resolves a spent output. The output registry (the
  500k-output cache) is not even created; mempool entries have no outstanding spends.
- **UTXO on** (the later add-on; kept working, every UTXO test is green):
  - Block −1 rows come from the server-side join `utxoDeltaInsertSql` (as header acceptance and re-orgs did
    already), `INSERT … SELECT` per accepting node over the batch's transitioning transactions.
  - A child block whose parent block was called later (child before parent in one node's lane) waits for the
    parent's node-agnostic rows (`StoreOperation.agnosticRowsWritten`), commits after it (`noteWaitsOn`, so the
    parent's commit order skips the child) and gives its lane and in-flight slot up while it waits.
  - A parent saved after its child committed writes the child's missing −1 rows itself: `utxoDeltaInsertSql`'s
    late-parent branch emits −1 for an output of a newly accepted transaction that a transaction the node already
    accepts in a block spends, when the node has no `utxo` row for it yet (net 0 with the +1).
  - The mempool path keeps its in-memory resolution (registry of recent outputs, constant-time `RecentCache`,
    capacity 100k) without persisting outstanding spends: a rebuild takes a spend whose output is not stored as
    outstanding.
- **DDL CLI:** a database created before this change (input with spent-output columns, `pending_spend`, the
  stand-in tables or `p_spent_*`) is refused (`legacySchemaFindings`).

**Upgrade path.** None in place: re-sync into a new database (`ddl/README.md` item 29). `CREATE TABLE IF NOT
EXISTS` would keep the old `input`, and the agent now writes fewer columns.

Fixes found on the way (both pre-existing, exposed by the new timing):
- The commit-order barrier ran a cycle check (`waitsTransitively`) per earlier operation on every wait-graph
  change: 46 s of agent CPU for 300 queued one-block commits under cap 16, which starved the 1.5 s test lease
  (the baseline `7edf914` failed that spec the same way here). It now runs only while some operation waits for
  a later one (`hasForwardWaits`); otherwise no earlier operation can reach a later one.
- Tip-mode block UTXO rows first took the transitioning transactions as a hash-list query parameter; the gate's
  100k-transaction blocks overflowed the server's 128 KiB field limit. They are read server-side from the commit's
  own `tx_acceptance` rows now (`blockDeltasSql`, as header acceptance does; regression spec with 5,000-transaction
  blocks).
- A batch waiting for earlier operations' rows held its lane; with child-before-parent waits that was a
  deadlock (child waits for the parent, the parent is queued behind a batch that waits for the child). The batch
  now gives its lane up while it waits (`waitForEarlier`).

## 2. The spend index

`ddl/070_spend.sql`, `src/store/clickhouse/spend-index.ts`.

**Table.** `spend`: per input whose spent output is stored, the input key (`transaction_hash`, `input_index`,
`transaction_internal_id`), the outpoint, the spent output's `value_satoshis`, `locking_bytecode`,
`token_category`, `fungible_token_amount`, `nonfungible_token_capability`, `nonfungible_token_commitment`,
`commit_seq` (the input row's commit), `spent_commit_seq` (the output row's commit) and `batch` (the fill batch).
Sorted by `(locking_bytecode_prefix, transaction_hash, input_index)` with projection `p_category`
`(token_category, nonfungible_token_commitment_key, transaction_hash, input_index)`: the busy filters (an
address's spends, a category's spends, an NFT commitment prefix) read one key range. Partitioned by
`intDiv(batch, 2^20)`. `spend_orphan` holds inputs a batch found no stored output for; `spend_watermark` has one
row per finished batch. View `spend_at(spend)` shows `batch <= spend`.

**Filler.** A background task of the writer (the lease holder), started with each epoch
(`CHAINGRAPH_CLICKHOUSE_SPEND_INDEX`, default `on`). It never runs on a commit path and never waits for a commit.
Loop: read visible(0) (published) and the spend watermark W; if visible(0) > W, fill the next batch `(W, to]`:
- `to`: the committed commits after W in seq order, as many as fit 400k input + output rows (`commit_log`
  `row_counts`), at least one; visible(0) if all fit or none is committed.
- forward: `INSERT INTO spend SELECT … FROM input (commit_seq in (W, to], not coinbase, valid) JOIN output (by
  primary key, commit_seq <= to, valid)`, `batch = to`;
- orphans: inputs of `(W, to]` with no such output, into `spend_orphan`;
- reverse: orphans of earlier batches whose output a commit in `(W, to]` stored, into `spend`;
- then `spend_watermark` gets `to`.

Every seq at or below visible(0) is terminal, and a valid one is committed (an aborted seq is in `commit_void`
before it becomes terminal). So after batch `to` the index holds exactly the pairs whose input and output commits
are both at or below `to`, each once. It never holds a placeholder: a row is written only when both commits are
committed. Output facts are immutable, so a row never needs an update; re-orgs do not touch node-agnostic rows.

**Restart and lease safety.** A batch's rows carry `batch = to`. Rows of a batch that did not publish its watermark
are invisible (above it) and are deleted at the next start (`ALTER TABLE … DELETE WHERE batch > W`, found by
partition and part min/max pruning; normally a no-op). Dedup tokens carry a per-run id, so a re-run after that delete
is never swallowed by deduplication of the earlier attempt, while a retried request of one attempt is. Only the
lease holder fills: every batch asserts the lease, and a lost lease stops the old epoch's filler and waits for it
before the new epoch's starts.

**Lag.** `spend_index_progress` (info, at most every 10 s): `watermark`, `visible0`, `lagCommits` (committed commits
above the watermark), batch rows, orphans and duration. `ClickHouseStore.spendIndexStatus()` returns the same.
Lag never changes a result (read rule below); it only moves work from the index to the tail join.

## 3. Read rule

Every request takes ONE snapshot (`readSnapshot` / `readSnapshotMulti`, unchanged contract). The snapshot now also
carries `spend` = the spend watermark W, read in the same scalar subquery as the visibility watermarks and capped at
visible(0). A visible input/output pair (input seq i, output seq o) is answered from
- the index (`spend_at(spend = W)`) iff both i and o are at most W;
- else, if i > W: the input joined to its output (any o);
- else (i ≤ W, o > W): the input spending an output above W.

The three parts are disjoint and cover every visible pair, so the union is exact at the snapshot whatever the
index's lag, and per node exactly as before (the parts are node-agnostic; acceptance is applied by the caller as
before). Only the two tail parts use the join, and they cover recent commits only. `spendRelationSql`
(`src/api/sql.ts`) builds it. A cached snapshot (`SnapshotCache`) may carry an older W; it is still exact.

API use (`src/api/sql.ts`):
- An `InputFilter` that requires a spent output (`requiresOutpoint`: an `outpoint` match positively, in a
  conjunction or in every `_or` branch) is answered from the spend relation: `has_input` reads it alone (no
  `input` rows), `inputs(where:)` filters by `(transaction_hash, input_index) IN` it. Any other filter that mentions
  `outpoint` (under `_not`, or in some `_or` branches) is evaluated on input rows with the outpoint semi-join
  (`outpoint IN (outputs matching)`), exact but not index-accelerated.
- Query-time unspent with a filter takes the spends of the matching outputs from the spend relation.
- `inputs { outpoint { … } }` and the root `inputs` read the spent output by a join on the outpoint for the page
  only; `outpoint` is null when no output is stored (a coinbase input), as in Hasura v1.
- Equality on `locking_bytecode` also binds `locking_bytecode_prefix`, and a commitment `_prefix` binds
  `nonfungible_token_commitment_key`, so `output.p_script` / `p_category` and `spend`'s keys are used.

## 4. Transaction-hash lookup

`src/store/clickhouse/tx-lookup.ts`.

- `TransactionLookup`: one queue for every block and mempool save; sorted chunks of at most 750 hashes, at most
  4 queries at once; a request on an idle lookup starts at once (the tip), later requests gather while queries run.
- `NewTransactionProof`: during an epoch whose stored blocks form one chain (one block per height, no committed
  re-org, mempool or expiry commit, one node), a block above the stored tip that extends it in this process
  (directly or through blocks that did) is new. Its non-coinbase transactions cannot be stored (BIP30: a valid
  chain holds a transaction once), and nor can a coinbase that commits to its block's height (BIP34; below
  1,983,702, where Bitcoin Core re-enables BIP30 because early coinbases encode later heights). They get ids
  without a lookup. A second block at a height, a re-org, a mempool operation, mempool tracking or a second node
  ends the proof for the process. In practice it covers initial syncs and catch-ups of a database without mempool
  history.
- The recent-transaction cache (`TransactionRegistry`, constant-time `RecentCache`, 1M) answers first, as before.

## 5. Measurements

Host: MacBook Pro (Apple M-series), Docker VM 14 CPUs / ~42 GB, ClickHouse 26.8.22.13 (ch1-local), Postgres 14
(ch1-pg), local BCHN chipnet. Raw results: `chaingraph-performance/2026-10-11-spend-index/` (not committed).
Baseline = `7edf914` (frozen export, same lab script, same session); after = `7ee8602` (chipnet) and `b24be5a`
(gate). UTXO off on chipnet (v2). Chipnet lab part-cleanup settings on both databases.

### 5.1 Chipnet sync from genesis (`chipnet_ch` recreated; baseline into `chipnet_ch_before`)

| | Baseline 7edf914 | After |
|---|---|---|
| wall to tip (every block accepted) | 484 s | **483 s** |
| tx/s to tip | 5,881 | 5,893 |
| max event-loop delay (10 s windows) | 867 ms | 766 ms |
| errors / warnings | 0 / 0 | 0 / 0 |
| database on disk after sync | 4.64 GiB | **4.35 GiB** |

ClickHouse queries during the sync (`system.query_log`, the agent's database, start → tip):

| Kind | Baseline: queries / CH s / rows / read | After |
|---|---|---|
| **tx-hash lookup** | **324,307 / 998.6 s / 8.58 B / 319.5 GiB** | **38 / 1.8 s / 116 M / 4.1 GiB** |
| spent-output lookup | 616 / 5.5 s / 15.3 M / 804 MiB | none (no block commit looks up a spent output) |
| `input` inserts | 2,373 / 20.2 s / 2.88 GiB read / 2.21 GiB written | 2,431 / 10.5 s / 2.03 GiB / **1.02 GiB written** |
| other inserts | 21,409 / 65.7 s / 1.95 GiB written | 21,874 / 62.1 s / 1.95 GiB written |
| other selects | 2,501 / 13.1 s | 9,149 / 24.4 s (the filler's commit_log and count reads) |
| spend index fill (`INSERT INTO spend…`, off the commit path) | – | 7,036 / 87.7 s / 1.55 B rows / 66.6 GiB read / 995 MiB written |
| **total** | **351,206 / 1,103 s / 8.86 B rows / 334 GiB** | **40,528 / 186.5 s / 1.93 B rows / 83.4 GiB** |

The transaction-hash lookup was most of ClickHouse's work in the sync. Batched, and skipped for proven-new blocks
(`NewTransactionProof` proved every block of the sync: an empty store, one node), it is 38 queries. Wall time is
unchanged because the chipnet sync is bound by the agent and the node, not ClickHouse.

Storage (active parts, on disk, after the sync; the baseline is 8 blocks lower):

| | Baseline | After |
|---|---|---|
| `input` incl. projections | 2.50 GiB (base 1.06, p_outpoint 521 MiB, p_spent_script 537 MiB, p_spent_category 529 MiB) | **1.10 GiB** (base 0.77, slim p_outpoint 338 MiB): −56 % |
| `spend` incl. p_category | – | 1.07 GiB (6.63 M rows; p_category 539 MiB) |
| `output` | 1.35 GiB | 1.39 GiB |
| database | 4.64 GiB | 4.35 GiB |

`spend` costs what the two `p_spent_*` copies cost (both ≈ 1.07 GiB here); the gain is the base table, the slim
`p_outpoint` and the write path. On mainnet the same split predicts `input` ≈ 449 → ≈ 207 GiB (plan, "Sizes") plus a
`spend` of about the `p_spent_*` pair minus their input-only columns (≤ 168 GiB, measured there with unlocking
bytecode excluded already): to be measured on the next mainnet run.

**Spend index lag** (from a commit's `finished_at` to the batch publishing it, 1,830 batches over sync and tip):
p50 172 ms, p90 291 ms, p99 745 ms, max 1.03 s. Every `spend_index_progress` line reported `lagCommits: 0`. 5
orphans (mempool children saved before their parents), all resolved or legitimately unknown.

### 5.2 Follow the tip (07:46–08:46Z, both agents), parity at H = 327,384

| Table | Rows | Result |
|---|---|---|
| block, block_transaction, transaction, output, input | 327,385 / 2,846,636 / 2,846,636 / 7,839,248 / 6,956,611 | match |
| **spend** (index vs Postgres `input ⋈ output`) | 6,629,226 | **match** |
| **spend_join** (ClickHouse join vs Postgres) | 6,629,226 | match |
| tx_acceptance, node_block_history | 2,846,636 / 0 | match |
| node_block | 327,385 | 33 blocks `accepted_at` set on Postgres, NULL on ClickHouse: synced one day apart (the agent's 2 h rule, chipnet-lab.md) |
| node_transaction_history | 561 vs 60 | mempool history since the Postgres sync (2026-10-10) vs since this sync |

Spend index vs join on one ClickHouse snapshot (`ch1-chipnet-spend-check.mjs`, after the tip follow): 6,629,304
index rows = 6,629,304 joined pairs; missing, extra, duplicated and mismatched all empty; watermark = visible(0).

### 5.3 Spent-output filters: join vs spend index (chipnet, warm medians of 5, server ms; cold n = 3, client ms)

Levels: typical = a P2PKH address with 110 spends, a category with 26, an NFT prefix with 147 spends; busy = the
busiest address (378,079 spends), category (1,402,190) and mutable-NFT prefix (443,149 / 278,334 txs).
`join` = outputs matching, then inputs by outpoint (slim `p_outpoint`); `index` = `spend_at` only; `api` = the API's
`spendRelationSql`. Every form returned the same fingerprint.

| Case | join warm / cold (rows) | index warm / cold (rows) | api, caught up | api, lag 3 batches (10 commits) |
|---|---|---|---|---|
| typical spends-addr | 19 / 23 (90k) | 2 / 6 (1k) | 2 | 28 |
| typical txs-addr | 17 / 20 (90k) | 6 / 9 (8k) | 5 | 31 |
| typical spends-cat | 14 / 18 (27k) | 3 / 6 (6k) | 3 | 33 |
| typical txs-nft | 19 / 21 (136k) | 3 / 6 (7k) | 2 | 32 |
| busy spends-addr | 171 / 178 (7.7 M) | 5 / 8 (379k) | 5 | 31 |
| busy txs-addr | 161 / 170 (7.7 M) | 36 / 33 (766k) | 30 | 54 |
| busy spends-cat | **572 / 587** (9.8 M) | **8 / 11** (1.4 M) | 7 | 36 |
| busy txs-nft | 176 / 192 (7.4 M) | 27 / 25 (449k) | 21 | 49 |

- The index form reads about the matching rows, as the denormalised columns did (plan §3.1: 13–32 ms busy). The
  join reads k × parts × 128 rows of `p_outpoint`; on mainnet the plan measured 2.8–9.2 s for busy filters.
- With a small lag the tail join adds ~25 ms. With a large lag (200 batches, 2.1 M spends behind, as during an
  initial sync) the API form took ~2 s for every case: the tail reads the inputs above the watermark, here a
  6 M-row merged part. Lag never changes a result.

### 5.4 Local gate (`--store clickhouse --quick`), baseline 7edf914 vs after b24be5a

UTXO off (v2):

| Scenario | Baseline | After |
|---|---|---|
| max-block wall (100,001 tx) | 3.08 s | **2.47 s** (40.5k tx/s) |
| burst drain (3 × 100,001 tx) | 7.79 s | **5.55 s** (54.0k tx/s) |
| re-org converge | 2.73 s | **1.93 s** |
| concurrent ratio | 0.45 (19.6k vs 33.9k + 10.0k) | 0.43 (19.7k vs 35.4k + 10.0k) |
| catch-up (1,000 blocks) | 670 blocks/s | 641 blocks/s |

UTXO on (gate default; baseline n = 2, after n = 3):

| Scenario | Baseline | After |
|---|---|---|
| max-block wall | 3.40 / 3.70 s | 3.02 / 3.04 / 3.04 s |
| burst drain | 9.24 / 9.32 s | 7.59 / 7.64 / 7.70 s |
| re-org converge | 3.59 / 3.71 s | 3.76 / 3.95 / 3.76 s |
| concurrent ratio | 0.48 / 0.48 (18.9k together) | 0.45 / 0.46 / 0.46 (15.0–15.3k together) |
| catch-up | 661 / 663 blocks/s | 592 / 592 / 550 blocks/s |

- Every scenario but `concurrent` passes on both. `concurrent` fails its 0.6 ratio on the baseline too on this
  host today (0.48; 0.57 in g1-fix-pass-2 on the 8 GB VM): the bound is the agent's one JS thread (WP6b §6).
- UTXO on, `concurrent` and catch-up are 10–20 % slower: block −1 rows are now a server-side join per commit
  instead of an in-agent copy (the add-on's cost; the spend index off made no difference, 0.46 / 592).
- Compared with fix-pass-3's catch-up (725 blocks/s at 10k blocks): same scenario family, this is the `--quick`
  1,000-block run; the baseline measured here is the reference.

## 6. Left for the mainnet re-measure

- **Spend index size and fill rate on mainnet**: `spend` ≈ the two `p_spent_*` copies (≤ 168 GiB), to be measured;
  the filler's batches read their inputs by `commit_seq` range, which part min/max pruning serves well at the tip
  but which reads a whole merged part per batch during a catch-up (chipnet: 1.55 B rows for 6.6 M index rows). If
  the lag grows on mainnet, feed the batch's transaction hashes from the writer (in memory) instead of a range scan.
- **Busy filters by the API form on mainnet** (expected as the denormalised form, 17–82 ms; the join was 2.8–9.2 s)
  and the tail cost under catch-up lag.
- **Tip-mode UTXO-on cost** of the server-side −1 join on mainnet blocks (the add-on).
- **The transaction-hash lookup on a mainnet catch-up** of a database with mempool history (the proof is off there;
  only batching applies): queries and rows per block.
- `input` storage (≈ 207 GiB expected) and `p_outpoint` slim (≈ 56 GiB expected) on the mainnet backfill.
