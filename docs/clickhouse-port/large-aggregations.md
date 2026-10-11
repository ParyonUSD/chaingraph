<!-- cspell:ignore chipnet antijoin signsum denorm spendidx subquery -->

# Large per-node aggregations: ClickHouse vs Postgres (chipnet)

Date 2026-10-11. Local only, read-only on every database. Script: `scripts/measure/large-aggregations.mjs`.
Raw results: `chaingraph-performance/2026-10-11-large-aggregations/` (`run1.jsonl`, `run2-hash.jsonl`; not committed).

**Result.**
- Every form of every query gives the same numbers in all three databases (§3).
- At common height H, the node's UTXO set holds 1,343,320,000,000,000 sats. That is exactly the chipnet subsidy total
  (210,000 × 50 + 117,328 × 25 BCH), which gives an outside check on the result.
- The stored UTXO table answers "total value of the node's UTXOs" in **12 ms** (sign-weighted sum) or **96 ms**
  (`utxo_at`, grouped).
- Computing it at query time, the v1 way, costs **2.0–3.1 s** in ClickHouse with a hash anti-join (7–8.6 s with
  `NOT IN`) and **26–31 s** in Postgres.
- On mainnet the query-time forms take minutes (ClickHouse) to hours (Postgres) (§4). A maintained per-node total
  makes the query instant in both stores (§5).

## 1. Setup

| | ClickHouse `chipnet_ch_utxo` | ClickHouse `chipnet_ch_pp2` | Postgres `chipnet_pg` |
|---|---|---|---|
| Schema | UTXO tables on; older `input` that still carries spent-output columns | current: UTXO off, slim `input`, spend index (caught up: spend watermark = visible(0), no tail) | Chaingraph v1, no per-node array (E19 not deployed) |
| Node | `chipnet-local`, internal id 1 | `chipnet-local`, internal id 1 | `chipnet-local`, internal id 1 |
| Tip (node 1) | 327,327 | 327,396 | 327,396 |
| Mempool | 16 transactions | 16 | 16 |
| Size | output 7.84 M, input 6.95 M, utxo 5.66 M rows | output 7.84 M, input 6.96 M, spend 6.63 M | output 2.2 GB, input 2.7 GB (incl. indexes) |

- **Host.** MacBook Pro, Docker VM with 14 CPUs and 41 GB. ClickHouse 26.8.22.13 (`ch1-local`); Postgres 14.24
  (`ch1-pg`, defaults: `shared_buffers` 128 MB, `work_mem` 4 MB, 2 parallel workers). The host was shared with other
  agents' work (load 4–6).
- **Snapshot.** ClickHouse reads the pinned `*_at` views with parameters from one snapshot.
  - `chipnet_ch_pp2` uses the compiled store's `readSnapshot`.
  - `chipnet_ch_utxo` predates `spend_watermark`, so it runs the compiled `snapshotSql` with an empty watermark
    source, followed by `readSnapshotMulti`'s post-processing.
  - The **base** rows run the same statements on the bare tables, without the gate. Collapsing tables are still
    grouped with `HAVING sum(sign) > 0`. No void or fence rows exist, so base equals gated here.
- **Scopes.**
  - **tip**: every acceptance, mempool included. This is the v1 meaning: node_validations ∪ block_inclusions.
  - **H = 327,327**: the minimum of the three tips, and accepted blocks at height ≤ H only. This is the like-for-like
    comparison across the three databases.
  - Q3 always covers the 10,000 blocks ending at the scope's height.
- **Timing.**
  - Warm: median of 5 runs after one warm-up, client wall time.
  - ClickHouse cold: median of 3 runs, each after `SYSTEM DROP` of the MARK, UNCOMPRESSED, PRIMARY INDEX, QUERY and
    MMAP caches.
  - Postgres has **warm only**. A container restart only empties the 128 MB of shared buffers, not the Docker VM's
    page cache, so it would not give a cold run. Dropping the VM page cache needs a privileged container, which was
    refused in this session. Another client was also connected to `ch1-pg`, so a restart was not attempted.
  - The ClickHouse "cold" runs therefore keep the OS page cache as well. The whole chipnet database (≈ 4.4 GB) fits
    in the VM's cache, so cold ≈ warm.
- **Rows and bytes.**
  - ClickHouse: `system.query_log`, median over the warm runs.
  - Postgres: the top node of `EXPLAIN (ANALYZE, BUFFERS)`, in 8 KiB blocks.

## 2. Results

Times are warm / cold in ms. "Rows" is ClickHouse rows read (MB read, peak memory).

### Q1: count and value of the node's unspent outputs

| Scope, DB | Form | Value (n / sats) | Warm | Cold | Rows read (MB, mem) |
|---|---|---|---|---|---|
| tip 327,327, ch_utxo | `utxo_at` (`sum(sign) > 0` groups) | 1,209,921 / 1,343,324,654,646,479 | 96 | 94 | 5.66 M (579 MB, 896 MB) |
| | same, base | = | 90 | 88 | 5.66 M |
| | `utxo` sign-weighted sum (`sum(sign)`, `sum(sign × value)`, gate in WHERE) | = | **12** | 11 | 5.66 M (119 MB, 63 MB) |
| | v1 anti-join, `NOT IN` | = | 8,616 | 8,732 | 20.5 M (1,202 MB, 2.6 GB) |
| | v1 anti-join, `LEFT ANTI JOIN` (parallel_hash) | = | 3,124 | 3,216 | 20.5 M (1,238 MB, 2.1 GB) |
| | both anti-joins, base | = | 5,938 / 2,309 | 6,130 / 2,115 | 20.5 M |
| tip 327,396, ch_pp2 | v1 anti-join, `NOT IN` / hash | 1,210,030 / 1,343,492,499,959,490 | 7,340 / **2,053** | 7,348 / 2,028 | 20.5 M (1,201–1,238 MB, 2.1–2.6 GB) |
| | spend index anti-join, `NOT IN` / hash | = | 8,521 / 2,186 | 8,175 / 2,173 | 20.2 M |
| | base: anti-join / spend index (`NOT IN`; hash) | = | 5,414 / 5,315; 1,562 / 1,696 | – | 20.2–20.5 M |
| tip 327,396, PG | v1 `NOT EXISTS` | = | 26,140 | n/a | 16.2 M hit + 7.1 M read blocks, 518 k temp blocks written |
| | same, `SET work_mem = '256MB'` | = | 30,880 | n/a | 15.5 M + 7.8 M, 124 k temp |
| H, ch_utxo | anti-join `NOT IN` / hash | 1,209,918 / **1,343,320,000,000,000** | 8,296 / 2,949 | 8,282 / 2,958 | 20.5 M |
| H, ch_pp2 | anti-join `NOT IN` / hash | = | 6,989 / 2,031 | 7,106 / 2,029 | 20.5 M |
| | spend index `NOT IN` / hash | = | 7,559 / 2,275 | 7,918 / 2,060 | 20.2 M |
| H, PG | v1 `NOT EXISTS` (default; 256 MB) | = | 29,716; 28,809 | n/a | 16.1 M + 7.2 M blocks |

- The UTXO-table forms run only at the tip of `chipnet_ch_utxo`. The table holds the current set, mempool included,
  and that tip equals H.
- Postgres plan: the accepted set is a CTE. Then a hash semi-join of `output` with the accepted hashes (a nested loop
  over `transaction_pkey`), and a merge anti-join against the inputs of accepted transactions (a nested loop over
  `input_pkey`, 2.85 M probes). Both sides are sorted and spill to temp. A larger `work_mem` cuts the spill but not
  the time.

### Q2: count and value of every output created in an accepted block

| Scope, DB | Form | Value (n / sats) | Warm | Cold | Rows read (MB, mem) |
|---|---|---|---|---|---|
| tip 327,327, ch_utxo | `output_at` ⋉ `tx_acceptance_at` (block rows) | 7,835,736 / 18,205,637,169,571,614 | 1,430 | 1,441 | 10.7 M (527 MB, 1.1 GB) |
| | base | = | 940 | 932 | 10.7 M |
| tip 327,396, ch_pp2 | same (gated / base) | 7,840,127 / 18,259,120,549,694,418 | 1,573 / 888 | 1,665 / 910 | 10.7 M |
| tip 327,396, PG | `output` ⋉ accepted block transactions | = | 958 | n/a | 51 k hit + 233 k read blocks, 125 k temp |
| H, ch_utxo / ch_pp2 / PG | same | 7,835,736 / 18,205,637,169,571,614 | 1,400 / 1,363 / 954 | 1,396 / 1,343 / n/a | 10.7 M |

### Q3: fees of the last 10,000 blocks

Fees are the spent value of non-coinbase transactions' inputs minus those transactions' outputs.

| Scope, DB | Form | txs / in / out / **fees** (sats) | Warm | Cold | Rows read (MB, mem) |
|---|---|---|---|---|---|
| 317,328–327,327, ch_utxo | input ⋈ output by outpoint | 246,130 / 734,946,526,449,392 / 734,945,805,731,751 / **720,717,641** | 1,365 | 1,451 | 52.3 M (2,708 MB) |
| | denormalised `input.value_satoshis` | = | 523 | 593 | 31.9 M (1,479 MB) |
| | base: join / denormalised | = | 1,051 / 385 | 1,103 / 379 | |
| H (same range), ch_pp2 | join / spend index | = | 1,164 / 441 | 1,251 / 436 | 52.4 M / 31.6 M |
| H, PG | input ⋈ output | = | 7,782 | n/a | 10.2 M hit + 2.3 M read blocks |
| 317,397–327,396, ch_pp2 | join / spend index | 245,888 / 783,075,848,860,930 / 783,075,169,855,710 / **679,005,220** | 1,196 / **429** | 1,261 / 417 | 52.4 M / 31.6 M |
| | base: join / spend index | = | 880 / 334 | 911 / 325 | |
| 317,397–327,396, PG | input ⋈ output | = | 8,873 | n/a | 9.4 M hit + 2.3 M read blocks |

The ClickHouse forms read whole columns: the `IN` sets of about 246 k hashes hit almost every granule of `input`,
`spend` and `output`. They do not read just the range.

### Q4: UTXO count and value by token category, top 20 by count

| Scope, DB | Form | Top row (no token) | Warm | Cold | Rows read |
|---|---|---|---|---|---|
| tip 327,327, ch_utxo | `utxo_at` (gated / base) | 648,264 / 1,341,031,184,344,688 | 110 / 110 | 130 / 106 | 5.66 M |
| | anti-join `NOT IN` / hash | = | 7,970 / 2,989 | 7,975 / 2,996 | 20.5 M |
| tip 327,396, ch_pp2 | anti-join `NOT IN` / hash | 648,295 / 1,341,199,010,188,998 | 8,044 / **1,990** | 7,408 / 1,901 | 20.5 M |
| | spend index `NOT IN` / hash | = | 7,936 / 2,130 | 7,421 / 2,132 | 20.2 M |
| tip 327,396, PG | `NOT EXISTS` (default; 256 MB) | = | 31,612; 32,871 | n/a | 16.2 M + 7.1 M blocks, 619 k temp |
| H, all three | anti-join and spend-index forms; PG | 648,263 / 1,341,026,530,047,316 | CH 1,903–8,731; PG 29,666; 34,695 | | |

The second row in every database is category `9c18d359…54ab`: 113,803 UTXOs, 170,705,000 sats. All 20 rows match
across forms and databases.

## 3. Agreement

The script compares the full result rows of each query and scope across every form, mode and database. At the tip it
compares only databases with the same tip. All 18 groups are equal (`"equal": true`), counting `run1` (12) and `run2`
(6):

- **Q1–Q4 at H.** All of these agree: `ch_utxo` anti-join (`NOT IN` and hash); `ch_pp2` anti-join and spend index
  (both join algorithms); `ch_utxo` input ⋈ output and denormalised; `ch_pp2` spend index; Postgres at both
  `work_mem` settings.
- **Q1–Q4 at tip 327,396.** `ch_pp2` (gated and base, every form) = Postgres, the 16 mempool transactions included.
- **Q1 and Q4 at tip 327,327.** `utxo_at` = sign-weighted sum = anti-join, gated and base. So on this database the
  stored UTXO set equals the v1 definition again, as in [chipnet-lab.md](chipnet-lab.md).
- **Q1 at H, against an outside value.** 1,343,320,000,000,000 sats is exactly the subsidy of heights 0–327,327. The
  fees are inside it (miners claim them in coinbases), and no subsidy went unclaimed on this chain.

The sign-weighted sum is exact only while every key's `sum(sign)` is 0 or 1. The write path keeps that invariant
(one +1 per accept, one −1 per release or spend), and it held here. A violation would show up as a mismatch against
`utxo_at`.

**Gate cost on large scans.**
- The gated views cost +30–60 % over base here: Q1 hash 2,053 vs 1,562 ms, Q2 1,573 vs 888 ms, Q3 spend index 429
  vs 334 ms. They read 12–20 % more bytes (`commit_seq` and the views' extra columns).
- This is more than WP6b's +8 % on an 800 k-row `utxo_at` scan ([wp6b-gate-cost.md](wp6b-gate-cost.md) §3). These
  statements gate four or five views of 7–10 M rows each, and the gate is evaluated per row. Not investigated
  further.

## 4. Mainnet extrapolation (estimates, not measured)

Mainnet base is node 1 at 834,000:
- 108,888,660 UTXOs and 1,964,989,364,665,078 sats ([decision 2026-10-11](../../../paryon_kubernetes-consolidation/docs/chaingraph/decisions/2026-10-11_clickhouse-phase1-g1-rerun.md));
- ≈ 1.05 B outputs, ≈ 0.94 B inputs, ≈ 384 M transactions;
- `utxo` 227 M rows / 10.9 GiB ([ch1-run-log.md](ch1-run-log.md)).

Scale factors from chipnet: outputs ×134, inputs ×135, transactions ×135, UTXOs ×90, `utxo` rows ×40.

Method: each ClickHouse form above reads whole columns, so its time and memory scale with the rows it reads.
- **Times assume this host's 14 threads with the data cached.** Mainnet does not fit in page cache on a normal
  server, so cold runs are bound by disk bandwidth.
- **Memory** of the anti-join scales with the spent set (inputs) and the accepted set (transactions).
- **Mainnet anchor:** the c1′ UTXO check computed both sides on `cg_tip` in **417 s** on a `c4d-highmem-96` with
  48 threads and a 300 GB memory allowance (615 s in the first run) ([chr-run-log.md](chr-run-log.md)). The two
  sides were the full v1-style anti-join plus the full `utxo_at` set with a row digest. The acceptance join was
  omitted (one node, no mempool, no re-orgs).
- **Postgres anchors:**
  - the F1g full-set comparison (per-node v1 unspent, node 1 at 834,000) ran 2 h 01 min with none of its 16 chunks
    finished ([ch1-run-log.md](ch1-run-log.md));
  - E19's set-based backfill (ledger #88) found chain-wide UTXO candidates by merge anti-join at 58.5 s per 1/256
    hash slice, ≈ 4.2 h single stream, with no acceptance check. The per-candidate acceptance check was ≈ 46 h
    single stream.

| Query | Form | Chipnet (warm) | Mainnet estimate | Basis |
|---|---|---|---|---|
| Q1 | CH `utxo` sign-weighted sum | 12 ms | **≈ 0.5 s warm, ≈ 1–3 s cold** | ×40 rows; reads 4 small columns (≈ 4.8 GB uncompressed, ≈ 1.5–2.5 GB on disk) |
| Q1 | CH `utxo_at` grouped | 96 ms | ≈ 4–15 s, 10–40 GB group-by memory (108.9 M groups; spill with `max_bytes_before_external_group_by`) | ×40 rows, ×90 groups |
| Q1 | CH v1 anti-join, hash (either `input` or spend index) | 2.0–2.2 s | **≈ 5–10 min** on a 96-vCPU highmem host (anchor: 417 s for this plus the `utxo_at` digest); ≈ 290 GB hash memory as one statement, so it does not fit a normal server unless split by outpoint-hash slice (16 slices ≈ 18 GB each, time unchanged) | ×134–135, mainnet check |
| Q1 | CH v1 anti-join, `NOT IN` | 7.3–8.6 s | ≈ 15–20 min, same memory problem | ×134 |
| Q1 | PG v1 (Hasura semantics) | 26 s | **hours**: > 2 h measured with no chunk done (F1g); ≈ 4.2 h for the chain-wide anti-join alone, ≈ 46 h for the exact per-node acceptance, single stream (≈ 6 h at 8 parallel slices, per E19) | mainnet anchors |
| Q1 | PG with E19 per-node array (not on chipnet, not measured) | – | **≈ 5–10 min cold.** 108.9 M rows with `1 = ANY(unspent_node_ids)`: the covering category index holds the token rows only, so non-token UTXOs need heap reads spread over most of `output`'s ≈ 300 GB heap (≈ a sequential scan at 0.5–1 GB/s). With `INCLUDE (value_satoshis)` on the rest index, an index-only scan of ≈ 4–5 GB, ≈ 10–30 s cold; job updates leave pages not all-visible, which adds heap fetches | E19 index definitions (`unspent-node-ids.ts`), chipnet heap ≈ 283 B per output ×134 |
| Q2 | CH `output_at` ⋉ accepted block transactions | 1.4–1.6 s | ≈ 3 min; the accepted-hash `IN` set of 384 M × 32 B is ≈ 25–40 GB | ×134 |
| Q2 | PG | 0.96 s | ≈ 10–30 min: sequential scan of the ≈ 300 GB output heap plus a 384 M-row hash that spills | ×134, disk bound |
| Q3 | CH spend index / denormalised | 0.43–0.52 s | ≈ 1 min as written (whole `input`/`spend` columns); a few seconds if the statement also bounds `commit_seq` to the range's commits (parts prune; a sync writes heights in commit order) | ×135; not tried |
| Q3 | CH input ⋈ output | 1.2–1.4 s | ≈ 2.5–3 min as written | ×135 |
| Q3 | PG input ⋈ output | 7.8–8.9 s | The range, not the chain, sets the cost: on mainnet the planner probes `input_pkey`/`output_pkey` per input. For the last 10,000 blocks (≈ 1–3 M transactions, an estimate) that is ≈ 1–2 min warm, ≈ 10–30 min cold at the ≈ 131 MB/s random reads seen on pd-balanced ([ch1-run-log.md](ch1-run-log.md)) | indexed nested loops |
| Q4 | CH `utxo_at` grouped by category | 110 ms | ≈ 5–15 s (as Q1 grouped) | ×40 |
| Q4 | CH / PG anti-join forms | 2.0 s / 32 s | as Q1 (the group-by adds little) | as Q1 |

Summary: on mainnet only the stored UTXO table (ClickHouse) answers Q1 and Q4 interactively. The query-time v1
definition is a batch job in ClickHouse (minutes, large memory) and impractical in Postgres (hours). The E19 array
brings Postgres to minutes cold.

## 5. A maintained per-node running total

Keep, per node, `(count, sats)` (and for Q4, per `(node, category)`) as deltas written with each acceptance change.
Q1 then reads a handful of rows, about 1 ms, in either store.

- **The delta of an accepted block or mempool transaction** is (+ outputs created) − (outputs spent) by the newly
  accepted transactions. A released block or dropped transaction writes the inverse.
- **ClickHouse, UTXO on.** The `utxo` −1/+1 rows of a commit already carry `value_satoshis`. Write one more row per
  node per commit, `(node, commit_seq, Σ sign, Σ sign × value)`, into a small MergeTree in the same commit, gated
  like `utxo_at`. The read sums one row per visible commit: thousands of rows, a few ms, and it can be compacted.
  - Cost: one row per node per commit, derived from rows the commit already builds. Effectively free.
  - The sign-weighted sum over `utxo` (12 ms here, ≈ 0.5 s on mainnet) is already this total, computed on read.
- **ClickHouse, UTXO off (v2).** A commit no longer resolves the outputs it spends, so it cannot compute the spent
  value. Maintain the total in the spend-index filler instead: per batch, + outputs of newly accepted transactions −
  spends from the index, applied to node acceptance. Then use the same read rule as the spend relation: the total up
  to the spend watermark W, plus a join correction for commits above W (mempool and a few blocks, small).
  - Exact at any lag.
  - Cost: one aggregate per batch over rows the filler already reads, plus the per-node acceptance check of those
    rows.
  - Re-orgs and mempool drops after W need the inverse delta from the acceptance tables. This is the delicate part.
- **Postgres.**
  - Without the array, the save transaction must look up the value of each spent output: one indexed probe per
    input, the work the E19 job does. Fold the delta into the E19 deferred job, which already decides per output
    and per node when the output enters or leaves the UTXO set. A per-node, per-category total adds one row upsert
    per job batch, with no new lookups.
  - A single hot row per node serialises concurrent writers. Prefer appending per-batch delta rows and summing them
    on read (a few thousand rows), or one row per partition of the job.
- **Caveats.**
  - A running total covers only fixed aggregates (Q1, Q4 by category, per address only if keyed so).
  - It must match the per-node truth at every re-org, mempool drop and backfill. Each needs a test against the
    anti-join, as in §3.
  - Q2 and Q3 need per-block totals instead: Σ outputs and fees per block, written at block save. Q3 then sums
    10,000 rows. `block.generated_value_satoshis` was dropped from ClickHouse in the spend-index change and is now
    computed on read; a per-block fee total would need the spend index or the join, as Q3 does.

## 6. How to re-run

```sh
node scripts/measure/large-aggregations.mjs . http://localhost:18123 \
  postgres://chaingraph:…@localhost:15432/chipnet_pg chipnet_ch_utxo chipnet_ch_pp2 5 3 [regexp over <query>:<form>]
```

`run1` ran every form except the hash anti-joins. `run2` used the filter `hash`. The script is read-only: it sets
`default_transaction_read_only` on Postgres and runs only `SELECT` and `SYSTEM DROP … CACHE` on ClickHouse.
