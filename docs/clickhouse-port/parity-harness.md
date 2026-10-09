<!-- cspell:ignore bytewise nullness unhex selftest orged denormalised Milli clickhouse inlines normalised trunc substr multiset Concat localised localise overridable TMPDIR localisation -->

# Parity harness: Postgres ↔ ClickHouse, per node

Plan: `paryon_kubernetes/docs/chaingraph/plans/clickhouse-primary-store.md` §5.2 (parity harness) and §1
(block-accepted, tx-accepted, unspent; checklist items 1–6). It is the mechanical check behind **Gate G1 item 1**:

> For nodes 1 and 39, at the replay end and every 100 blocks: md5 of canonical rows for base tables, acceptance
> and history equals Postgres; `utxo` equals Postgres F1g; zero mismatches.

Code: `scripts/parity/` (node ESM `.mjs`, no build step).

| File | Contents |
|---|---|
| `compare.mjs` | CLI: plans chunks, runs both engines, writes TSV + summary, exit code |
| `lib/canonical.mjs` | canonical row definition per table, for both engines; digest and row-stream SQL |
| `lib/engines.mjs` | Postgres pool on one exported snapshot; ClickHouse HTTP; the WP4 snapshot read |
| `lib/digest.mjs` | the `sum` digest (JS reference, mod 2^64 reduction, addition) |
| `selftest.mjs` | scratch databases, identical seed, mutations; `node scripts/parity/selftest.mjs` |
| `sql/f1g-unspent-output.sql` | F1g `unspent_output(node_name)`, verbatim from `origin/perf/unspent-output-root` (6b8e24e), for the self-test's scratch Postgres |

**Why `.mjs` and not TS under `src/`:** the harness is an operator tool that runs against two databases, like
`scripts/ingestion-gate`. It needs no compiled agent. It imports nothing from `src/store/clickhouse`, which is
still changing, and only relies on the view contract of the DDL (`*_at`). It depends only on `pg`, which is already a
dependency. The self-test applies the DDL with `src/store/clickhouse/ddl/apply.sh` and the Postgres migrations from
`images/hasura/…/migrations`, so it always tests the checked-out schema.

## 1. What is compared

| Table | Scope | Chunks | Postgres side | ClickHouse side |
|---|---|---|---|---|
| `block` | node-agnostic | height ranges | `block` | `block_at` |
| `block_transaction` | node-agnostic | height ranges | `block ⋈ block_transaction ⋈ transaction` | `block_transaction_at ⋈ block_at` (uses its own `transaction_hash`) |
| `transaction` | node-agnostic | tx-hash prefixes | `transaction` in scope | `transaction_at` in scope |
| `output` | node-agnostic | tx-hash prefixes | `output` of in-scope txs | `output_at` |
| `input` | node-agnostic | tx-hash prefixes | `input` of in-scope txs | `input_at` |
| `input_spent` *(opt-in)* | node-agnostic | tx-hash prefixes | `input ⋈ output` on the outpoint | ClickHouse's denormalised spent-output columns on `input_at` |
| `node_block` | per node | height ranges | `node_block ⋈ block` | `node_block_at` (its `block_hash`, `height`) |
| `tx_acceptance` | per node | tx-hash prefixes (+ `mempool`) | **ACC predicate** (below) | `tx_acceptance_at ⋈ block_at` |
| `node_transaction` | per node | `mempool` (only with `--include-mempool`) | `node_transaction` | `node_transaction_at` |
| `node_block_history` | per node | `all` | `node_block_history ⋈ block` | `node_block_history_at ⋈ block_at` |
| `node_transaction_history` | per node | `all` | `node_transaction_history ⋈ transaction` | `node_transaction_history_at ⋈ transaction_at` |
| `utxo` | per node | utxo tx-hash prefixes | **F1g** `unspent_output(name)` | `utxo_at` |

**Base tables are node-agnostic** (plan §5.2: "per height window"; checklist item 7). Scoping them per node would
make a wrong acceptance fact also show up as a base mismatch. That would break the isolation property the self-test
checks: a fact gained by node B must mismatch `node-b/<that table>` and nothing else. Base rows are reported with
node `*`.

**Transaction scope.**
- Tx-level tables cover the transactions contained in at least one stored block, of any node or of none (stale
  forks included), with `height ≤ H` when `--at-height H` is given.
- With `--include-mempool`, they also cover the transactions in a compared node's mempool.
- Transactions in no block and no compared mempool are not covered: dropped or replaced txs that only history
  references. Their hash is still compared through `node_transaction_history`.

**ACC predicate on Postgres** (`tx_acceptance` has no Postgres table; plan §1 tx-accepted):
- block part: `transaction ⋈ block_transaction ⋈ node_block(node = n) ⋈ block`, one row per (tx, accepting block);
- mempool part (`mempool` chunk): `node_transaction(node = n)`.

ClickHouse keeps one `tx_acceptance` row per (tx, node, accepting block or 0 = mempool), so the two sides line up
row for row. A BIP30 duplicate coinbase, or a tx shared by two fork blocks, gives one row per block on both sides.

**Unspent on Postgres** is F1g: `SELECT … FROM unspent_output('<node name>') o WHERE o.transaction_hash >= … AND <
…`. The function inlines, so the hash range drives an `output_pkey` range scan. It covers block and mempool
acceptance (F1g semantics). The harness never installs F1g on a target database; the target must have it.

## 2. Canonical rows (exact formats)

One text value `s` per row on both engines: fields joined with `|`.
- Bytes (`bytea` / `FixedString` / `String`): lowercase hex; empty bytes give `''`.
- Integers: decimal. `version`: signed int32 (Postgres `bigint` normalised mod 2^32; ClickHouse `Int32`).
- Booleans: `1`/`0`. SQL NULL: `NULL`.
- No token: `NULL` on both sides (Postgres `token_category IS NULL`; ClickHouse 32 zero bytes).
- Capability: `none` / `mutable` / `minting` / `NULL`.

**Internal ids never appear.** Rows are joined on hashes and indices, and nodes are matched by `node.name`.

| Table | `s` = |
|---|---|
| `block` | `hash\|height\|version\|timestamp\|previous_block_hash\|merkle_root\|bits\|nonce\|size_bytes` |
| `block_transaction` | `block_hash\|transaction_index\|transaction_hash` |
| `transaction` | `hash\|version\|locktime\|size_bytes\|is_coinbase` |
| `output`, `utxo` | `transaction_hash\|output_index\|value_satoshis\|locking_bytecode\|token_category\|fungible_token_amount\|nonfungible_token_capability\|nonfungible_token_commitment` |
| `input` | `transaction_hash\|input_index\|outpoint_transaction_hash\|outpoint_index\|sequence_number\|unlocking_bytecode` |
| `input_spent` | `transaction_hash\|input_index\|value_satoshis\|locking_bytecode\|token_category\|fungible_token_amount\|nonfungible_token_capability\|nonfungible_token_commitment` (coinbase and unresolved outpoints excluded) |
| `node_block` | `block_hash\|height\|accepted_at?` |
| `tx_acceptance` | `transaction_hash\|block_hash\|height`; mempool rows `transaction_hash\|mempool\|-` |
| `node_transaction` | `transaction_hash\|validated_at?` |
| `node_block_history` | `block_hash\|height\|accepted_at?` |
| `node_transaction_history` | `transaction_hash\|validated_at?\|replaced_at?` |

`x?` is the NULL-ness of timestamp `x`: `null` or `set`. It is semantic (accepted before monitoring; `replaced_at`
NULL = confirmed), so it is always compared exactly.

**Columns not compared:**
- ClickHouse-only derived columns: `block.transaction_count`, `block.output_value_satoshis`,
  `block.generated_value_satoshis`, `transaction.input_count`, `transaction.output_count`,
  `transaction.output_value_satoshis`, `utxo.created_height` (always 0, WP5a §3).
- History rows' own `internal_id`.

Postgres computes the derived columns through functions. Comparing them belongs to the plan's API row (§5.2). The
spent-output copies on `input` are covered by the opt-in `input_spent`.

### Timestamps

`accepted_at`, `validated_at`, `removed_at` and `replaced_at` come from the agent's clock, or from the DB clock for
Postgres defaults. Two agents hear blocks and transactions at different times. `--timestamps` picks the policy:

| Mode | md5 | Values |
|---|---|---|
| `tolerance` (default) | NULL-ness only | Separate pass: rows streamed from both sides, sorted by `s`, merged. Every pair whose timestamps are both non-NULL must agree within `--ts-tolerance-ms` (default 120 000). Reported as row `<table>:<columns>` with `max_ms=…;over=…`. |
| `exact` | ISO-8601 UTC with ms appended to `s`: `2026-10-09T12:34:56.789Z` (Postgres `to_char(date_trunc('milliseconds', x), …)`, ClickHouse `toString` of the `DateTime64(3, 'UTC')`) | — |
| `exclude` | NULL-ness only | not compared |

Use `exact` when both stores got their timestamps from the same source, such as a backfill copied from golden.
Use `tolerance` or `exclude` for independently ingested periods. `tx_acceptance.accepted_at` is not compared: on
Postgres it is `node_block.accepted_at` / `node_transaction.validated_at`, which are compared.

## 3. Digests, chunking, consistency

**Chunk digest (`--hash sum`, default).**
- For each row, take `md5(s)`. Its two big-endian 64-bit halves are summed separately over the chunk, mod 2^64.
- The digest prints as 32 hex characters, next to the row count.
- Postgres: `sum(('x' || substr(md5(s), 1, 16))::bit(64)::bigint::numeric)`, reduced mod 2^64.
- ClickHouse: `sum(reinterpretAsUInt64(reverse(substring(MD5(s), 1, 8))))` (UInt64 sum wraps mod 2^64), and the same
  for bytes 9–16.
- It is order-independent, so it needs no sort and no string concatenation, and runs in constant memory: it scales
  to 1B-row tables.
- It is additive, so window digests sum to cumulative digests.
- It is a multiset hash: duplicate rows count. It is not adversarially collision-resistant, but no adversary
  writes either store.

**`--hash ordered`:** `md5` of all `s` sorted bytewise and joined with `\n`:
- Postgres: `string_agg(s, E'\n' ORDER BY s COLLATE "C")`.
- ClickHouse: `arrayStringConcat(arraySort(groupArray(s)), '\n')`.

This is literally "md5 of sorted canonical rows", but it holds a whole chunk in memory, and Postgres caps text at
1 GB. Use it for small chunks or as a cross-check. The self-test proves that both modes give identical values on
JS, Postgres and ClickHouse.

**Roll-up.** Each (node, table) gets an `ALL` row: the md5 of its chunk lines (`chunk\tcount\tdigest\n` in chunk
order), computed client side, plus summed counts. In window mode with `sum`, `cum:<from>-<hi>` rows give the
running digest up to each window ("as of height hi").

**Chunks.**
- Block-level tables (`block`, `block_transaction`, `node_block`): height ranges `b:lo-hi` of `--chunk-blocks`
  (default 10 000).
- Tx-level tables and `tx_acceptance`: transaction-hash prefixes `h:x`. `--hash-chunks` is 16, 256, 4096 or
  65536 (default 16). Each chunk is `hash >= prefix‖00… AND hash < next‖00…`.
  - Postgres: a `transaction_hash_key` / `output_pkey` range scan.
  - ClickHouse: a primary-key range on `transaction`, `output`, `input` and `tx_acceptance`, all ordered by
    transaction hash.
  - On ClickHouse the scope set is read from `block_transaction_at` with the same hash range, so the base tables
    do not depend on any acceptance table.
- `utxo`: `--utxo-chunks` prefixes (default 16). `utxo` is ordered by node and token category, so each chunk
  scans the node's rows; fewer chunks means fewer scans.
- Histories: one chunk `all`. Mempool: one chunk `mempool`.

**Consistency.**
- Postgres: one coordinator opens `REPEATABLE READ READ ONLY` and calls `pg_export_snapshot()`. Every worker
  connection (`--parallel`) runs `SET TRANSACTION SNAPSHOT`, so all chunks see one database state.
- ClickHouse: the WP4 `readSnapshot` order. Read `visible(n)` for every compared node first, then `visible(0)` and
  the committed tail once. Every query passes them to the pinned views: `*_at(node, visible)` and
  `*_at(visible0, tail)`. All chunks see one gated state, and an open commit is never half-visible.

**Output.**
- `<out>/parity.tsv`: `node, table, chunk, pg_count, ch_count, pg_md5, ch_md5, match`. It holds the chunk rows,
  then `cum:` and `ALL` roll-ups, then timestamp rows.
- `<out>/summary.json`: mode, snapshot, nodes and tips, skipped checks, mismatched rows and node/tables, duration.
- `--diff`: for every mismatched chunk, both sides are streamed sorted by `s` and merged. The first 20 rows found
  only in Postgres (`- pg`) and only in ClickHouse (`+ ch`) go to `<out>/diff.txt` and stderr.
- Exit code: 0 if every chunk and timestamp row matches, 1 on any mismatch, 2 on error.

## 4. Modes: "as of height H" and "every 100 blocks"

| Mode | Base tables | Acceptance | Histories | `utxo` |
|---|---|---|---|---|
| default (current) | all blocks; txs in any block | current `node_block`, `tx_acceptance` (block rows) | all rows | current set, both sides |
| `--at-height H` | blocks `height ≤ H`; txs in a block `≤ H` | `node_block` / `tx_acceptance` rows with block `height ≤ H` | `node_block_history` rows of blocks `≤ H`; `node_transaction_history` all rows | only for a node whose tip is exactly H on both sides; otherwise skipped (listed in `summary.skipped`) |
| `--every N --from H0 --to H1` | per window `[lo, hi]`: blocks in it; txs in those blocks | per window: `node_block` and `tx_acceptance` rows with block height in it | once, blocks `≤ H1` | skipped |
| `--include-mempool` (any mode) | adds compared nodes' mempool txs | adds `tx_acceptance` mempool rows and `node_transaction` (chunk `mempool`) | — | — |

**Mempool is excluded by default.** Two agents' mempools differ by timing (plan §5.2 expected noise), and a
mempool "as of height H" is not reconstructible. F1g and `utxo_at` both include mempool effects, so `utxo` is
exact only when the mempools agree. At replay end the replay feeds no mempool, so they agree.

**"As of H" semantics.**
- Postgres: the snapshot plus `height ≤ H` filters.
- ClickHouse: the pinned snapshot plus the same filters.
- For acceptance this is "the facts of node n with block height ≤ H in the current state". That equals the state
  at the moment n reached H only if no later re-org removed blocks ≤ H. On the replay chain no re-org occurs, so
  they coincide.
- `utxo` has no height dimension in either store (`created_height` is 0; F1g has no height argument). It is
  compared only at a tip both sides share.

**"Every 100 blocks"** = `--every 100 --from 831863 --to 834000`, run on the stopped (or both past H1) stores.
- Each window is one chunk per table, so a mismatch is localised to 100 blocks.
- The `cum:` rows give the digest as of every 100th height without re-reading earlier windows.
- Live checkpoints during a replay work too: run `--at-height H` once both stores have passed H.

## 5. How to run (G1 item 1)

```sh
# credentials only via env; node names or Postgres node ids (1, 39)
export CH_USER=… CH_PASSWORD=…
PG=postgres://…/chaingraph CH=http://…:8123

# replay end, both nodes, everything (node 39 on golden: WP7)
node scripts/parity/compare.mjs --pg $PG --ch $CH --ch-db cg --nodes 1,39 \
  --parallel 8 --hash-chunks 16 --timestamps exact --out data/parity/end   # exact: backfilled timestamps come from golden

# every 100 blocks over the replay range (mainnet node 1)
node scripts/parity/compare.mjs --pg $PG --ch $CH --nodes 1 --every 100 --from 831863 --to 834000 \
  --timestamps tolerance --ts-tolerance-ms 600000 --out data/parity/windows

# on a mismatch: localise with --diff (and --tables to restrict)
node scripts/parity/compare.mjs … --tables tx_acceptance --hash-chunks 4096 --diff --out data/parity/debug
```

**G1 item 1 passes** when all of these exit 0:
- the replay-end run for nodes 1 and 39 (all default tables, incl. `utxo` = F1g);
- the window run.

Every `match` column is then `yes`, and `summary.json` has `mismatchedRows: 0` and no unexpected `skipped`
entries. `data/` is gitignored: raw results stay local.

**Timestamps in the lab replay.** The Postgres reference and the ClickHouse replay ingest the replay blocks at
different wall times.
- Rows ≤ 831,862 carry golden's timestamps on both sides (§5.1 backfill), so `exact` holds for them.
- Rows of the replay period differ by the start offset of the two replays. For them, use `--timestamps tolerance`
  with a tolerance above that offset, or `exclude` and report the timestamp pass separately.
- The md5 rows (with NULL-ness) are what G1 counts.

## 6. Self-test (`node scripts/parity/selftest.mjs`)

Local only: Postgres `ch1-pg` localhost:15432 and ClickHouse `ch1-local` http://localhost:18123 (overridable with
`--pg-admin`, `--ch`). It creates `ch1_parity_<hex>` on both and drops them at the end (`--keep` keeps them).
- Postgres gets all 13 migrations' `up.sql`, the agent's 5 indexes (`src/components/db-utils.ts`) and F1g.
- ClickHouse gets `ddl/apply.sh`.
- Output goes to `$TMPDIR/ch1-parity-selftest-*`, or `--out`.

**Seed.** Identical facts on both sides, with different internal ids (blocks 1–5 vs 101–105, txs 1–10 vs
1001–1010, nodes 1/2 vs 5/9):
- blocks B0–B3 and a stale fork block B2x at height 2;
- `node-a` accepts B0 (`accepted_at` NULL), B1, B2, B3, and re-orged B2x away: one `node_block_history` row;
- `node-b` accepts B0, B1 and B2x;
- tx T2 is in both B2 and B2x;
- T1 creates an FT output, a mutable NFT (commitment `0102`), an FT+`none` NFT with an **empty** commitment and a
  plain output; T2, T3 and M each spend one of them;
- mempools: `node-a` has M, `node-b` has T3, which `node-a` confirmed in B3;
- `node_transaction_history`: T3 confirmed for `node-a` (`replaced_at` NULL), R replaced for `node-b`;
- ClickHouse UTXO rows follow the WP5a sign rules: +1 per output of an accepted tx, −1 per spent outpoint;
- all rows are under one committed `commit_log` seq `(1 << 40) | 1`, with `visibility` rows for nodes 0, 5 and 9,
  so the WP4 gate shows them;
- mutations are later commits, each published the same way.

**Result (2026-10-09, local ClickHouse 26.8.22.13, Postgres 14.24; 15/15 passed, 15 s):**

| Run | Rows | Exit | Mismatched |
|---|---|---|---|
| digest primitive: JS = Postgres = ClickHouse (`sum` `3497a7ea…e39f`, `ordered` `1ecab7a0…1c5f`) | — | — | none |
| identical, `sum`, exact timestamps, mempool | 145 | 0 | none |
| identical, `ordered`, exact timestamps, mempool | 145 | 0 | none |
| identical, `sum`, tolerance 0 ms, mempool | 155 | 0 | none |
| identical, no mempool, 256 hash chunks | 1 359 | 0 | none |
| identical, `--at-height 2` (`utxo` runs for node-b only: node-a's tip is 3) | 130 | 0 | none |
| identical, `--every 1 --from 0 --to 3` (windows + `cum:`) | 101 | 0 | none |
| **node-b gains `node_block` B3** (ClickHouse +1 row) | 145 | 1 | **`node-b/node_block` only**; diff `+ ch e19a…d3b0\|3\|set\|2026-10-09T10:03:01.000Z` |
| reverted (−1 row in a later commit) | 145 | 0 | none |
| **node-b gains `tx_acceptance` T3@B3** (ClickHouse) | 155 | 1 | **`node-b/tx_acceptance` only** |
| **Postgres `output` T1:3 value +1** | 155 | 1 | **`*/output` only** (`- pg …\|101\|…`, `+ ch …\|100\|…`); `utxo` unaffected (T1:3 is spent) |
| ClickHouse `accepted_at` +3 s, tolerance 1 s | 155 | 1 | `node-a/node_block:accepted_at` only (md5 rows match) |
| same, tolerance 5 s | 155 | 0 | none |
| same, `--timestamps exact` | 145 | 1 | `node-a/node_block` only |
| all mutations reverted | 145 | 0 | none |

Seeded `ALL` counts (both sides): block 5, block_transaction 9, transaction 9 (with mempool), output 12, input 9;
node_block 4/3, tx_acceptance 8/6, node_transaction 1/1, node_block_history 1/0, node_transaction_history 1/1,
utxo 7/6 (node-a/node-b). The utxo counts equal the hand-derived unspent sets.

Checklist item 2 (isolation): a fact gained by node B, in `node_block` or `tx_acceptance`, mismatches exactly that
node and table. Node A, the other tables and the base tables still match.

## 7. Runtime at mainnet scale, and the knobs

**Row counts.**
- Outputs 1.16 B, inputs 1.04 B, transactions 420 M, block_transaction ≈ 420 M, blocks ≈ 0.83–0.94 M.
- Per node: `tx_acceptance` ≈ 420 M for node 1. F1g reads every output once with two index probes.
- Node 39 (chipnet) is small (minutes).

**Measured throughput (local, 2026-10-09, synthetic 8-field rows, format + md5 + sum):**
- ClickHouse: 50 M rows in 10.5 s (≈ 4.8 M rows/s, 18 cores).
- Postgres: 5 M rows in 12.9 s on one backend (≈ 0.4 M rows/s).

| Side | Work (node 1, full run) | Estimate |
|---|---|---|
| Postgres | 3.04 B base rows + 420 M `tx_acceptance` ≈ 3.5 B rows ≈ 2.4 core-h of hashing. Index-order heap access (scope ⋈ `output_pkey` / `input_pkey` / `block_inclusions_index`) ×2–3. F1g: 1.16 B outputs × 2 probes (`spent_by_index`, then the creator test on survivors) ≈ 3 core-h warm. | **≈ 12–18 core-h → 2–4 h wall at `--parallel 8` warm; 4–8 h cold** (pd-balanced) |
| ClickHouse | 3.5 B rows hashed ≈ 12 min. Per tx-hash chunk, the scope scan reads `block_transaction.transaction_hash` (≈ 13 GB) → 16 chunks ≈ 215 GB read ≈ 30–60 min. `utxo`: `--utxo-chunks` scans of node 1's rows. | **≈ 1–1.5 h**, concurrent with Postgres |
| Window run (replay 831,863–834,000, 22 windows) | ≈ 2 k blocks of txs; on ClickHouse, granule reads for the IN sets | **minutes** |

A full run is Postgres-bound: plan **≈ 3–8 h** for node 1 at replay end, plus minutes for node 39 and the windows.
Run it on the lab restore next to the replay (WP7), not against production.

**Knobs.**
- `--parallel N`: Postgres connections in the shared snapshot, and concurrent ClickHouse queries. The main lever:
  match it to the Postgres host's cores and IO queue depth.
- `--hash-chunks`: more chunks means smaller ClickHouse IN sets (16 chunks ≈ 26 M hashes ≈ 1.5 GB per query ×
  `--parallel`) and finer localisation, but more `block_transaction` scans on ClickHouse. 16 is right for
  mainnet; raise it to 256+ on a small ClickHouse, or when debugging.
- `--utxo-chunks`: fewer chunks means fewer scans of the node's `utxo` part; each chunk's GROUP BY holds 1/n of
  the node's live set.
- `--chunk-blocks`: height-range size for block-level tables (cheap either way).
- `--hash sum` (server-side, constant memory) is required at mainnet scale. `ordered` needs a chunk under
  Postgres's 1 GB text limit: ≥ 4 096 hash chunks for `output`/`input`.
- `--tables`: split a run across invocations or hosts. Each invocation is self-contained; digests of the same
  chunks are comparable across runs at the same snapshot.
- Not built: a ClickHouse scope from `tx_acceptance` instead of `block_transaction` would avoid the 16 scans. It
  would make base parity depend on acceptance parity, so it was not used.

## 8. Ambiguities resolved

1. **Base tables per node or node-agnostic?** Node-agnostic (node `*`). §5.2 says "per height window behind both
   watermarks"; per-node scoping would break the isolation check (a node B fact would also move node B's base
   rows).
2. **Base tx scope** is "in some stored block (≤ H)": not "accepted by n", and not "every stored tx". Mempool txs
   are added with `--include-mempool`. Dropped or replaced txs in no block are not row-compared. Stale-fork blocks
   are included.
3. **"Every 100 blocks"** is implemented as 100-block windows plus cumulative `sum` digests, at a common snapshot,
   not as live pauses. It equals the state at each height when no re-org touched those heights, which holds on the
   replay chain.
4. **`utxo` at a height** is not defined by either store. It is compared only at the stores' common tip (replay
   end), with F1g's mempool semantics.
5. **History at a height**: `node_block_history` is limited by the removed block's height. `node_transaction_history`
   has no height and is compared whole.
6. **`tx_acceptance` mempool rows** are compared as `transaction_hash|mempool|-`. Their `height` (ClickHouse
   writes 0) and timestamp are not compared.
7. **Nodes**: matched by `node.name`. A bare number that is not a name is the Postgres `internal_id` (G1's "nodes 1
   and 39"). ClickHouse node ids differ (WP4 allocator) and never enter a canonical row.
8. **`version`** is normalised to signed int32 on both sides (Postgres `bigint`, ClickHouse `Int32`; WP2 README).
9. **Timestamps**: plan §5.2 "modulo the timestamp source". NULL-ness is exact; values use `exact`, `tolerance`
   (default 120 s) or `exclude` (§2).
10. **ClickHouse-only derived columns** (counts, values, `created_height`) are out of md5 parity; see §2.
