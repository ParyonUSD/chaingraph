<!-- cspell:ignore chipnet paryon authchain authchains authhead bytecode pushdata unhex subquery denormalised setof normalisation pothos materialised unreviewed bitjson optimisation -->

# Phase 2 stage 1: the Chaingraph v2 GraphQL API

Date 2026-10-11. GraphQL Yoga + Pothos over the ClickHouse store (owner decision D-2), local only. Inputs:
`paryon_kubernetes-consolidation/docs/chaingraph/plans/clickhouse-api-requirements-inventory.md` (R1–R40, documents
L1–L47, X1–X16), `clickhouse-api-options.md` (spike criteria), [phase2-spike.md](phase2-spike.md),
[fix-pass-3.md](fix-pass-3.md) §4–5, [spend-index.md](spend-index.md),
[pre-phase2-concurrency-mempool.md](pre-phase2-concurrency-mempool.md).

Owner rules kept throughout: no Hasura-v2 compatibility; every acceptance-scoped root (and relationship) names its
node; exactly one `readSnapshot(Multi)` per request, its parameters never client-supplied; UTXO tables off (unspent at
query time, spent-output attributes from the output join or the spend index).

| Group | Commit |
|---|---|
| Generic one-statement compiler, entity model | `c742d63` |
| G1 roots (R5–R8, R32, R35) | `ed9423d` |
| v2 document catalogue + chipnet equality harness | `bb3ab2d` |
| G2 filters (R9–R14) | `807b03b` |
| G3 shape (R15–R23) | `34d8589` |
| G4 values (R24–R30) | `be7d3a0` |
| G5 live and ops (R31–R34, R37, R38) | `eaef874` |
| Candidates materialised per statement; authchain acceptance once | `d4ecdb0` |
| This document, catalogue headers with latency | docs commit |

**Result.** Every MUST requirement is implemented with a test that compares it with an independent computation. All
**63 catalogue documents (L1–L47, X1–X16) run on chipnet and equal Chaingraph v1** (Hasura over the Postgres reference
`chipnet_pg`) after normalisation; 48 of them are equal row for row in order, the other 15 equal as row sets with the v2
rows in v2's own internal-id order (the two stores were synced independently, so their internal ids differ; §5.2).

## 1. What was built

| Piece | Where |
|---|---|
| Schema (Pothos), SDL committed and checked | `src/api/schema.ts`, [`v2-schema.graphql`](v2-schema.graphql) (`yarn api:schema:update`; `schema.spec.ts` fails on any unreviewed change) |
| Entity model: per object type its row source, scalars, computed fields, relationships | `src/api/model.ts` |
| Selection compiler: one root field = one statement, any depth | `src/api/compile.ts` |
| Roots: sources, acceptance, ordering, keyset/offset paging, caps | `src/api/roots.ts` |
| Filters → SQL (operators, three-valued logic, relationship filters, spend relation) | `src/api/sql.ts` |
| Aggregates and group-by | `src/api/aggregates.ts` |
| Bytecode patterns, P2P encodings (port of v1's Postgres functions) | `src/api/bytecode.ts`, vectors `src/api/bytecode-pattern.vectors.ts` |
| Query limits, trusted documents, response cache | `src/api/plugins.ts` |
| send_transaction (through the agent) | `src/api/send.ts` |
| Request snapshot (multi-node), statement environments | `src/api/db.ts` |
| Live-query hub (unchanged from the spike) | `src/api/live.ts` |
| Catalogue (63 documents), runner, chipnet spec | [`v2-documents/`](v2-documents), `src/api/catalogue.ts`, `catalogue-run.ts`, `catalogue.spec.ts` |
| Specs | `api-roots.spec.ts` (G1), `api-filters.spec.ts` + `spec-oracle.ts` (G2), `api-shape.spec.ts` (G3), `api-values.spec.ts` + `bytecode.spec.ts` (G4), `api-ops.spec.ts` (G5), `catalogue.spec.ts` (chipnet), spike `api.spec.ts`, `sql.spec.ts`, `schema.spec.ts` |

No new npm packages: the plugins are small Yoga/envelop plugins of our own.

### 1.1 The compiler

The spike joined fixed relationships onto the root page. It now handles any selection:

- The root page is a `MATERIALIZED` CTE (`page`). Each selected relationship is a further `MATERIALIZED` CTE (a
  *level*) that reads a pinned view restricted to the parent level's keys (`… WHERE transaction_hash IN (SELECT hash
  FROM page)`), applies the relationship's `where`, and orders and limits per parent with
  `row_number() OVER (PARTITION BY parent key ORDER BY …)`.
- Levels fold back bottom-up: each groups its rows by parent key into `arraySort(x -> x.1, groupArray(tuple(rn, …)))`
  and is `LEFT JOIN`ed onto its parent. One statement returns one tuple per root row with everything nested; resolvers
  only read prefetched data, so a live query result is computed once and fanned out.
- Computed fields (`encoded_hex`, `fee_satoshis`, `locking_bytecode_pattern`, `header`, …) declare hidden selections
  (e.g. `__enc_inputs`) that the compiler adds; their resolvers compute the value in TypeScript.
- A per-node relationship takes `node:` (default: the node of the level above). Under a node-agnostic root it must name
  one; `Sql.nodeView` refuses otherwise. Every `node:` argument of the operation, at any depth, is pinned by the one
  request snapshot; a relationship naming another node reads that node's `visible(n)` (`node_<id>`/`visible_<id>`
  parameters) in the same statement.
- Rows of all levels of one root statement are capped (`maxResultRows`, default 500,000): above it the request fails
  with `RESULT_TOO_LARGE`, never a silent cut.

### 1.2 Acceptance without reading a node's whole acceptance set

`hash IN (SELECT transaction_hash FROM tx_acceptance_at(node))` builds a set of every transaction the node accepts
(2.8 M on chipnet, ~1 B on mainnet). Where a filter has a positive, selective conjunct (a hash list, an EXISTS over
outputs by script/category/transaction, an EXISTS over spends), that conjunct is the root's *candidate* set:

- transactions: the candidates are a statement-level `MATERIALIZED` CTE; the transaction rows (`hash IN candidates`,
  primary key), the acceptance check and every per-transaction semi-join of the filter (`has_output`, `has_input`,
  `block_window`, `in_block`, `in_mempool`) read only the candidates' rows by key. Safe under any negation: a row
  outside the candidates is excluded by the root conjunct anyway;
- outputs / unspent outputs / inputs / spends: the acceptance check reads the acceptance rows of the matching rows'
  transactions only; the unspent anti-join reads the spend relation of the matching outputs once
  (`Sql.acceptedRows`).

Chipnet, busy category (128,695 transactions): unrestricted 420 ms, restricted 95 ms. The block-window documents
(L1, L4–L8, L10, L11) were 500–800 ms before the candidates were materialised and passed into the semi-joins (§6).

### 1.3 Filters

- Operators `_eq _neq _in _nin _gt _gte _lt _lte _is_null` on Int, BigInt, Hex, capability, Boolean and Timestamp;
  Hex adds `_prefix` and `_prefix_in`. Values outside a column's range resolve statically (`internal_id > -1` is true;
  the library sends `-1` as its first keyset cursor).
- **v1 null semantics.** v1 (Postgres) holds "no NFT commitment", "no token category" and "no fungible amount" as
  NULL, and comparisons on NULL are NULL. ClickHouse stores the zero category instead, and `NULL IN (…)` is 0, not
  NULL. Under an even number of `_not`s NULL and false both exclude a row, so positive filters keep plain key
  conditions (`token_category = X`), and only conditions that could be true for an absent value get a guard
  (`_neq`, ranges, `_nin`, prefixes). Under an odd number of `_not`s the exact NULL is produced
  (`if(absent, NULL, …)`). An `outpoint` sub-filter evaluated on a spend row is EXISTS-like (never NULL) under
  negation. The redundant commitment-prefix key condition is emitted only in positive context.
- Relationship filters are semi-joins and nest to any depth: `TransactionFilter.has_output / has_input / in_block /
  in_mempool / block_window`, `OutputFilter.transaction / spent_by`, `InputFilter.outpoint / transaction`,
  `BlockFilter.has_transaction`, `NodeTransactionFilter.transaction`, `NodeBlockFilter.block`. All fields of one
  filter object bind to one row.
- **Index-backed lists and prefixes.** A hex list is bound in chunks of at most 1,000 values per parameter
  (ClickHouse's 128 KiB field limit) as a subquery set: `col IN (SELECT unhex(x) FROM (SELECT arrayJoin({p}) AS x))`.
  The spike's `col IN arrayMap(x -> unhex(x), {p})` is rewritten by ClickHouse into `has()`, which no primary key or
  projection can use: chipnet `spend` read 6,491 of 6,491 granules for a two-address set, 8 with the subquery set.
  Prefixes on `locking_bytecode` add `startsWith(locking_bytecode_prefix, …)` (p_script; prefixes longer than 25
  bytes too), commitment prefixes `startsWith(nonfungible_token_commitment_key, …)` (p_category). The chipnet spec
  asserts the projections in `EXPLAIN`.
- Spent-output filters follow the spend-index read rule unchanged (index up to the snapshot's spend watermark, the
  join above it). Nested `inputs(where: { outpoint … })` reads only the level's own spent outputs by key.

## 2. Schema overview

Full SDL: [`v2-schema.graphql`](v2-schema.graphql) (sorted). Scalars: `Hex` (lowercase hex, no prefix,
in and out), `BigInt` (64-bit, decimal string), `Timestamp` (ISO-8601 UTC, ms). `Int` for indexes and small counts
(`outpoint_index` is BigInt: a coinbase input has 4294967295).

Per-node roots (`node: String!`; all also subscriptions unless marked):

| Root | Rows |
|---|---|
| `transactions(node, where, order, after, before, cursor, order_by, offset, limit)` | transactions the node accepts (block it accepts or its mempool) |
| `outputs(node, where, order_by, after, offset, limit)` | outputs of those transactions (search by script set / prefix, category, commitment prefix) |
| `unspent_outputs(…)` | the node's unspent outputs (query time) |
| `inputs(…)` | inputs of transactions the node accepts |
| `blocks(node, where, order_by, after, offset, limit)` | blocks the node accepts (tip, height range, checked heights, by height) |
| `block_tip(node)` | the node's tip, with `previous_block_hash` |
| `mempool(node, where, order_by, after, offset, limit)` | the node's mempool (`validated_at` order, keyset) |
| `node(node)` | metadata, `unconfirmed_transaction_count`, `unconfirmed_transactions`, `accepted_blocks` |
| `watermark(node)` (query) | the visibility watermark the request reads |
| `*_aggregate(node, where)` (query): transactions, outputs, unspent_outputs, spends, blocks, mempool | count, sum/min/max/avg |
| `output_groups(unspent:)`, `spend_groups`, `transaction_groups`, `block_groups` (query) | group-by, paged |
| `unspent_output_groups` (query; the spike's S3) | group-by list |

Node-agnostic roots: `transactions_by_hash(hashes)`, `blocks_by_hash(hashes)`,
`outputs_by_transaction_hash(transaction_hashes, where)` (up to 10,000 hashes), `transaction_watermark`, `nodes`
(metadata only). Mutation: `send_transaction(node, encoded_hex)`.

Relationship edges (R20):

| # | Edge | Per node |
|---|---|---|
| 1 | `Transaction.outputs(where, order, order_by, limit, offset)` | |
| 2 | `Transaction.inputs(…)`, `Input.outpoint` (null if not stored) | |
| 3 | `Output.spent_by(node, where, order_by, limit)` → `Input.transaction` | yes: spends the node accepts |
| 4 | `Output.transaction`, `Input.transaction` | |
| 5 | `Transaction.block_inclusions(node, where, order_by, limit)` → `block` | yes |
| 6 | `Block.accepted_by(node, where)` → `node` | yes |
| 7 | `Transaction.node_validations(node, where)` → `node` | yes |
| 8 | `Transaction.node_validation_timeline(node, order_by)` | yes |
| 9 | `Node.unconfirmed_transactions(where, order_by, limit, offset)` | yes (the node) |
| 10 | `Node.accepted_blocks(where, order_by, limit, offset)` | yes (the node) |
| 11 | `Block.transactions(where, order_by, limit, offset)` → `transaction`, `block` | |
| 12 | `Transaction.authchains(node)` → `authhead`, `migrations(order_by, limit)` → `transaction` | yes |
| 13 | `Transaction.identity_output`, `signing_output`, `data_carrier_outputs(where, order_by, limit)` | |

Computed fields: `Transaction.input_value_satoshis`, `fee_satoshis`, `encoded_hex`; `Input.value_satoshis`,
`unlocking_bytecode_pattern`, `redeem_bytecode_pattern`; `Output.locking_bytecode_pattern`; `Block.input_count`,
`output_count`, `input_value_satoshis`, `fee_satoshis`, `generated_value_satoshis`, `header`, `encoded_hex`;
`Node.unconfirmed_transaction_count`. Values come from the output join (page only), never from denormalised input
columns.

Changes from v1 to release-note: ACC is the root's `node:`; WIN (`block_window`) and `block_inclusions` are per node
(v1: "any block", "no inclusion anywhere"); `spent_by` is per node (v1: any spend); authchains are per node (v1: any
spend; at most one chain per node); `identity_output`/`signing_output`/migration `transaction` are objects (v1: lists
of at most one, a missing signing output a row of nulls); bytes have no `\x` prefix; indexes are Int; offset is capped
and discouraged; unbounded lists need a `limit` or hit the row cap.

## 3. Requirements: how each is met

| R | Status | Implementation | Test (independent computation) |
|---|---|---|---|
| R1 node argument | done | per-node roots and relationships; refused below node-agnostic roots without `node:` | G1 R8 spec; catalogue |
| R2 one snapshot | done | `PinnedSnapshot` pins every `node:` of the operation; one `readSnapshotMulti` | S6, G1 R5 (two named nodes, 1 snapshot), G5 live (1 per advance) |
| R3 ACC, WIN | done | root `node:`; `block_window` per node | S1; catalogue L1, L4–L11 equal v1 |
| R4 unspent | done | query time, spend relation | S5; L12, L13, L20, L25, L31, L32, L36, L37 equal v1 |
| R5 facts as data | done | edges 5–8 | G1 R5 vs `*_v` views; X3, X5, X6, L34 equal v1 |
| R6 mempool root | done | `mempool`, `validated_at` keyset | G1 R6 vs `node_transaction_v`; L44–L47, X1 equal v1 |
| R7 block roots | done | `blocks`, `block_tip` | G1 R7 vs `node_block_v ⋈ block_v`; L41, L43, X7, X8 equal v1 |
| R8 lookups, watermark | done | `*_by_hash`, `transaction_watermark` | G1 R8 (2,500 hashes in 3 chunks); L33, L34, X4, X5 |
| R9 operators | done | §1.3 | G2: 154 filters × 2 nodes vs a JS evaluator |
| R10 combinators | done | `_and/_or/_not`, NOT EXISTS, OR of EXISTS | G2; L5, L9, L10, L21–L24, L33, L45 |
| R11 relationship depth | done | any depth, one row per object | G2 depth-5 and same-row filters |
| R12 bytes | done | `Hex` scalar, hash length checks | G2 R12 |
| R13 prefixes | done | `_prefix`, `_prefix_in`, index-backed | G2; chipnet EXPLAIN (p_script, p_category); X9, X11–X13 |
| R14 script sets | done | chunked subquery sets | G2 (2,500 scripts); chipnet EXPLAIN (spend key 8/6,492 granules); X3, X10 |
| R15 keyset | done | every root, any order (opaque cursor), transactions after/before | G3 walks vs JS-sorted rows; S2 |
| R16 offset | done (discouraged, cap 100,000) | `offset` on list roots and nested lists | G3 slices; L26 |
| R17 ordering | done | `order_by` lists, through object relationships (stored keys), nested, variable direction | G3; L12–L16, L26, L30, X1, X2 |
| R18 limits | done | root and nested `limit`/`offset`, top-1 | G3; L40, L42, X1, X2 |
| R19 nested args, aliases | done | per-selection args, aliases | G3; L34 (`accepted_block_inclusions`), X2 (`latest_tip`) |
| R20 13 edges | done | §2 | G1, G3 (edge 11), G4 (edge 12), catalogue |
| R21 multi-root | done | one snapshot | S6, G3; L33, X14 |
| R22 operations | done | named operations, variables, fragments, `@include/@skip` | G3 |
| R23 SDL | done | committed, `schema.spec.ts` | unit |
| R24 aggregates | done | `*_aggregate` | G4 vs JS; X11, X14–X16 equal v1 |
| R25 group-by | done | `output_groups`, `spend_groups`, `transaction_groups`, `block_groups` | G4 vs JS; chipnet holders = v1 L12 aggregated |
| R26 computed | done | §2 | G4 vs raw rows; X1, X6, X7 equal v1 |
| R27 patterns | done | TS port, shared vectors | 32 vectors (and the Postgres macros now read them); X5, X6 equal v1 |
| R28 encodings | done | TS encoders | libauth, genesis; chipnet: sha256d(encoded) = hash for 20 tx, 3 blocks; X6–X8, X13 equal v1 |
| R29 authchains | done | recursive CTE per node | G4 (node-two shorter); X6 (776-long chain), X13 equal v1 |
| R30 search | done | `outputs` + `outputs_aggregate` | X3, X9–X13 equal v1 |
| R31 live queries | done | all per-node roots are subscriptions, on the shared hub | G5: the 12 live documents, pushes only on change, 1 run per topic per advance |
| R32 signals | done | `watermark(node)`, signal documents | G5; L40–L47 equal v1 |
| R33 caps | done | per-root page caps, nested cap, statement row cap, `has_more` | G3, G5 |
| R34 types, limits, trusted documents | done | BigInt/Int; depth 16, aliases 64, root fields 32, fields 2,000; trusted documents by sha256 (HTTP and graphql-ws) | G5 |
| R36 latency | measured (chipnet) | §6 | catalogue runner |
| R37 response cache | done (COULD) | keyed on document, variables and the snapshot's watermarks | G5: a hit = 1 watermark read, 0 snapshots, 0 data queries |
| R38 send_transaction | done (COULD) | relayed through the agent's internal API (v1's action endpoint) | G5 with a mock agent |
| R39 `_like` | not done (SHOULD, drop candidate) | `_eq` on node names | — |
| R40 bool-expression variables | replaced (SHOULD) | `_prefix_in` | — |

## 4. The catalogue

[`v2-documents/`](v2-documents): one `.graphql` per document (`L01-…` to `L47-…`, `X01-…` to `X16-…`) with its v1
original under `v1/`, and `catalogue.json` with the chipnet variables of both versions (from the library's benchmark
fixture `chipnet-16`, node `chipnet-local`; the chaingraph.cash examples' mainnet literals replaced by chipnet values).
Each file's header holds its status, chipnet equality outcome and latency. Run:

```sh
CHAINGRAPH_CLICKHOUSE_URL=http://localhost:18123 CHAINGRAPH_CLICKHOUSE_DATABASE=chipnet_ch_pp2 \
CHAINGRAPH_API_V1_URL=http://localhost:18080/v1/graphql CHAINGRAPH_API_V1_ADMIN_SECRET=… \
node build/api/catalogue-run.js [--latency 9] [--update-headers] [L1 X6 …]
```

The v1 side is Hasura (`chaingraph/hasura:dev-build`, container `ch1-api-hasura`, `HASURA_GRAPHQL_STRINGIFY_NUMERIC_TYPES`)
over `chipnet_pg` through a read-only connection (`default_transaction_read_only=on`; metadata in the scratch database
`ch1_api_hasura_meta`). The runner walks every page on both sides (v1 as the library does: keyset, composite keyset,
offset), normalises (no `\x`; integers as strings; internal ids and per-sync timestamps dropped; nested lists as sets;
v1's single-row lists unwrapped) and compares.

| | Documents | Works | Equal in order | Equal as row sets | Differs | Blocked |
|---|---:|---:|---:|---:|---:|---:|
| Library L1–L47 | 47 | 47 | 32 | 15 | 0 | 0 |
| chaingraph.cash X1–X16 | 16 | 16 | 16 | 0 | 0 | 0 |

"Equal as row sets": the v2 rows are ordered by v2's internal ids (checked ascending/descending), v1's by v1's. Both
stores were synced from genesis independently and assign ids in a different order within some blocks (e.g. L3 rows
40–44). With ids carried over from Postgres (plan §5.1) the orders coincide.

## 5. Notes

### 5.1 Per-node semantics on chipnet

Chipnet has one node, so per-node and v1's "any node" coincide for block inclusions, spends and authchains. The
scratch specs cover the two-node differences (node-two lacks a block and the mempool: G1 R5, G2, G4 authchain).

### 5.2 Things v1 does that v2 does not reproduce

- Timestamps (`validated_at`, `accepted_at`, `replaced_at`, node connection times) are those of each store's own agent;
  the runner drops them. The validation timeline compares on chipnet because the chosen documents' transactions have
  no history in either store; the histories differ in length (Postgres: since 2026-10-10, ClickHouse: since its sync).
- X1 shows the five newest mempool transactions by `validated_at`; validation times differ between the two agents, so
  the catalogue compares all 16 mempool transactions (limit 100).

## 6. Latency (chipnet, p50 of 9 warm requests, first page)

Host: MacBook Pro (Apple M5 Pro), Docker VM 14 CPUs, ClickHouse 26.8 (`ch1-local`, database `chipnet_ch_pp2`,
UTXO off, spend index caught up), Postgres 14 (`ch1-pg`, `chipnet_pg`) behind Hasura v2.49.5 (admin role, no row
limits). Both at height 327,396, 2,847,058 transactions, 16 mempool transactions. v2 is in process (`yoga.fetch`,
request snapshot included, response cache off); v1 over HTTP to Hasura. p50 of 9 sequential runs after 2 warm-ups;
"first page" for paged documents (the equality run walks every page). Classes as the inventory: A < 20 ms, B 20–200,
C 200 ms–1 s, D 1–3 s, E > 3 s. Raw results: `/tmp/chaingraph-v2-catalogue.json` (not committed).

Summary: median v2 136 ms, v1 83 ms; v2 classes B 50, C 12, E 1 (X6); v1 classes A 26, B 25, C 10, D 2. v2 is faster
on 20 of 63, mostly the heavy ones (L30 MonitorLoansWindow, L33 ContractUtxoActivity, L12 AllHolders, the block tip
and aggregate documents). On this small dataset Postgres answers point lookups in 2–10 ms where v2 pays a fixed
80–150 ms per multi-level statement (§7). Golden/mainnet numbers are Phase 2 stage 2 work.

| Doc | Name | v2 rows | v2 p50 ms | class | v1 p50 ms (Hasura/Postgres) | equality |
|---|---|---:|---:|---|---:|---|
| L1 | LoanBorrowingTxs | 7 | 206 | C | 10 | equal |
| L2 | LoanCompletedRedemptions | 1 | 108 | B | 86 | equal |
| L3 | LoanInterestPayments | 695 | 184 | B | 120 | equal-unordered |
| L4 | UserLoanInteractions | 707 | 342 | C | 69 | equal-unordered |
| L5 | UserLoanInteractionsExcludingInterest | 12 | 215 | C | 236 | equal |
| L6 | UserStakingInteractions | 28 | 184 | B | 8 | equal |
| L7 | AllStakingInteractions | 192 | 245 | C | 16 | equal-unordered |
| L8 | RedemptionInteractions | 10 | 125 | B | 176 | equal-unordered |
| L9 | UserRedemptionSidecarCommitments | 6 | 120 | B | 160 | equal |
| L10 | UserRedemptionInteractions | 6 | 157 | B | 218 | equal |
| L11 | AllLiquidations | 10 | 139 | B | 84 | equal |
| L12 | AllHolders | 1536 | 136 | B | 368 | equal-unordered |
| L13 | allStakingReceipts | 6 | 70 | B | 4 | equal |
| L14 | finalizedUserRedemptions | 3 | 203 | C | 85 | equal |
| L15 | startedAndSwappedRedemptions | 5 | 118 | B | 3 | equal-unordered |
| L16 | UserStartedAndSwappedRedemptions | 3 | 177 | B | 3 | equal |
| L17 | finalizedRedemptions | 5 | 116 | B | 86 | equal-unordered |
| L18 | BorrowingTxsInPeriod | 1 | 60 | B | 83 | equal |
| L19 | FinalizedRedemptionsInPeriod | 0 | 38 | B | 84 | equal |
| L20 | InterestManager | 2 | 60 | B | 3 | equal |
| L21 | LoanClosure | 1 | 126 | B | 2 | equal |
| L22 | LoanClosures | 7 | 188 | B | 24 | equal |
| L23 | AllLoanClosures | 20 | 184 | B | 109 | equal-unordered |
| L24 | UserInitiatedLoanClosures | 8 | 109 | B | 102 | equal-unordered |
| L25 | LoanKeyOwner | 1 | 34 | B | 2 | equal |
| L26 | AllNewPeriodPoolTxs | 176 | 196 | B | 11 | equal |
| L27 | LoanBorrowingTx | 16 | 146 | B | 104 | equal-unordered |
| L28 | AllLoanClosureSummaries | 20 | 197 | B | 104 | equal-unordered |
| L29 | LoanBorrowingSummaries | 7 | 173 | B | 9 | equal |
| L30 | MonitorLoansWindow | 1000 | 317 | C | 2373 | equal-unordered |
| L31 | PriceContractUtxos | 5 | 189 | B | 111 | equal-unordered |
| L32 | RedemptionContractUtxos | 0 | 117 | B | 3 | equal |
| L33 | ContractUtxoActivity | 2305 | 383 | C | 2659 | equal-unordered |
| L34 | ContractOutputsByHash | 5 | 199 | B | 5 | equal |
| L35 | MonitorLoans | 1040 | 315 | C | 154 | equal |
| L36 | MonitorPriceContractUTXOs | 5 | 192 | B | 111 | equal-unordered |
| L37 | MonitorRedemptionContractUTXOs | 0 | 118 | B | 3 | equal |
| L38 | StabilityPool | 1 | 94 | B | 4 | equal |
| L39 | newPeriodPool | 1 | 143 | B | 5 | equal |
| L40 | MonitorLoansSignal | 1 | 94 | B | 99 | equal |
| L41 | ChaingraphBlockTipSignal | 1 | 29 | B | 350 | equal |
| L42 | MonitorLoansSignalPoll | 1 | 96 | B | 108 | equal |
| L43 | ChaingraphBlockTipSignalPoll | 1 | 24 | B | 349 | equal |
| L44 | MonitorLoansMempoolSignal | 0 | 28 | B | 1 | equal |
| L45 | MonitorContractUtxosMempoolSignal | 5 | 35 | B | 39 | equal |
| L46 | MonitorLoansMempoolSignalPoll | 0 | 27 | B | 1 | equal |
| L47 | MonitorContractUtxosMempoolSignalPoll | 5 | 35 | B | 40 | equal |
| X1 | MonitorMempools | 1 | 26 | B | 91 | equal |
| X2 | MonitorSyncTips | 1 | 79 | B | 2 | equal |
| X3 | MonitorForPayments | 39 | 149 | B | 297 | equal |
| X4 | ShowDuplicateTransactions | 2 | 55 | B | 16 | equal |
| X5 | GetSpendingTransactions | 1 | 259 | C | 9 | equal |
| X6 | GetTransactionDetails | 1 | 7475 | E | 83 | equal |
| X7 | GetBlockDetails | 1 | 200 | B | 20 | equal |
| X8 | GenesisBlockInfo | 1 | 78 | B | 9 | equal |
| X9 | SearchOutputsByLockingBytecodePrefix | 3548 | 333 | C | 54 | equal |
| X10 | SearchOutputsByLockingBytecode | 71 | 88 | B | 8 | equal |
| X11 | CountCashAccountOutputs | 1 | 55 | B | 2 | equal |
| X12 | GetJonathan100CashAccount | 1 | 190 | B | 3 | equal |
| X13 | GetBitjson100Authchain | 1 | 473 | C | 333 | equal |
| X14 | BlockchainSize | 1 | 34 | B | 342 | equal |
| X15 | BlockSizeStatsSince2021 | 1 | 33 | B | 343 | equal |
| X16 | TransactionStats2021Q1 | 1 | 557 | C | 591 | equal |


## 7. Known gaps and open questions

Known gaps (no owner decision needed, Phase 2 stage 2):

1. **Fixed cost per statement.** A multi-level document costs 80–150 ms on chipnet even for 0–5 rows (L15, L21,
   L32, L37 against 2–5 ms on Postgres). `system.query_log` attributes it to analysis and plan optimisation of large
   statements (every level is a `MATERIALIZED` CTE over pinned views with array parameters; L36: 62 ms analysis), not
   to rows read. Candidates: fewer view references per statement (one acceptance CTE), the gate as a row policy,
   lighter levels for 1:1 relationships, the materialised CTE setting (still Experimental in 26.8, spike §4.1).
2. **Long authchains.** The recursive CTE costs ~9 ms per link (X6: a 776-long chain, 7.4 s; v1 83 ms). Options:
   a stored authhead index, or batching several links per iteration.
3. **Unselective per-node roots** (e.g. `transactions(node)` with only an internal-id range, `transaction_groups`
   without a filter, `spends_aggregate` without a filter) read the node's whole acceptance set: 0.4 s on chipnet,
   not viable on mainnet. Selective filters are restricted to their candidates (§1.2).
4. `inputs_aggregate` is not offered: inputs have no numeric field of their own; `spends_aggregate` /
   `spend_groups` aggregate the spent outputs (coinbase inputs excluded).
5. Timestamps of acceptance/validation and node connection metadata are store-local; the response cache never
   caches node connection metadata.
6. R39 `_like` not implemented (drop candidate); R40 replaced by `_prefix_in`.
7. The equality runs are chipnet only (owner: local = chipnet); golden/mainnet parity and latency are not run.

Open questions for the owner:

1. **Unselective per-node roots on mainnet (gap 3):** refuse them (require a selective filter or an internal-id
   window below a cap), or add a per-node acceptance projection ordered by internal id so they page in order?
2. **Per-node semantic changes for release notes:** `block_inclusions`, `spent_by` and authchains are per node (v1:
   any node). Confirm, or keep node-agnostic variants for explorers (e.g. `spent_by(node: null)` = any spend)?
3. **send_transaction** relays through the agent's internal API (as v1's Hasura action did), so the API holds no P2P
   connection. Confirm, or should the API connect to nodes itself?
4. **Response cache default:** on in `api:dev`/`main.ts` (`CHAINGRAPH_API_RESPONSE_CACHE=off` disables), off in
   `createApi`. A hit costs one watermark read instead of the request's snapshot read: confirm this counts as
   compatible with "exactly one readSnapshot per request" (it reads no snapshot at all).


## 8. Suites (final code)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean |
| unit (`ava --match='!*[e2e]*'`) | 171 passed, 1 todo (incl. 36 bytecode/encoding, SDL check) |
| API specs `build/api/*.spec.js` (scratch `ch1_api_*`, dropped) | 61 passed, 3 skipped (the chipnet spec without its env), incl. the spike's S1–S6 and overhead (S5 API p50 51 ms, 6 ms above the same SQL) |
| chipnet spec (`CHAINGRAPH_API_CHIPNET_DATABASE=chipnet_ch_pp2`, Hasura) | 3 passed: 63/63 documents equal; R13/R14 EXPLAIN; R25 holders, R28 hashes |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 162 passed |
| e2e ClickHouse, UTXO on / off | 47 / 47 passed (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg; the bytecode macros now read the shared vectors) | 102 passed |

