<!-- cspell:ignore chipnet paryon authchain authchains authhead bytecode pushdata unhex subquery denormalised setof normalisation pothos materialised unreviewed bitjson optimisation analysed backfill backfilled chipcopy -->

# Phase 2: the Chaingraph v2 GraphQL API (stage 1: requirements; stage 2: performance)

Date 2026-10-11 (stage 2: §6, the same day). GraphQL Yoga + Pothos over the ClickHouse store (owner decision D-2), local only. Inputs:
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
| **Stage 2 (performance, §6):** gate inlined into the compiled SQL | `f52536c` |
| Levels as scalar subqueries, nested in TypeScript | `0147973` |
| Block candidates; one acceptance read for unspent outputs | `8b971f6` |
| block_inclusions joins block_transaction only for transaction_index | `6f34476` |
| Nested limits as `LIMIT n BY` | `b8f1206` |
| Acceptance filters below the collapsing GROUP BY | `1d152f4` |
| Authchain index (080) and the segment walk | `00d6267` |
| Intersected output-side candidates; implied acceptance | `f0f0bd4` |
| Acceptance sets out of output primary-key analysis | `1433bcc` |
| Stage 2 docs, catalogue headers | docs commit |

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
| Inlined pinned-view gate (stage 2) | `src/store/clickhouse/visibility.ts` `inlinePinnedView` |
| Authchain index (stage 2): table, filler step, backfill | `ddl/080_authchain.sql`, `src/store/clickhouse/authchain-index.ts`, `bin/chaingraph-clickhouse-authchain-backfill.js` |
| Catalogue (63 documents), runner, chipnet spec | [`v2-documents/`](v2-documents), `src/api/catalogue.ts`, `catalogue-run.ts`, `catalogue.spec.ts` |
| Specs | `api-roots.spec.ts` (G1), `api-filters.spec.ts` + `spec-oracle.ts` (G2), `api-shape.spec.ts` (G3), `api-values.spec.ts` + `bytecode.spec.ts` (G4), `api-ops.spec.ts` (G5), `catalogue.spec.ts` (chipnet), spike `api.spec.ts`, `sql.spec.ts`, `schema.spec.ts` |

No new npm packages: the plugins are small Yoga/envelop plugins of our own.

### 1.1 The compiler

The spike joined fixed relationships onto the root page. It now handles any selection, in one statement per root
field (stage 2 shape, §6.1):

- The root page and each selected relationship (a *level*) are scalar subqueries of the statement's `WITH` clause,
  each returning all its rows as one array of tuples (`groupArray(tuple(…))`). A level reads its source restricted to
  its parent level's keys (`… IN (SELECT tupleElement(__r, i) FROM (SELECT arrayJoin(<parent array>) AS __r))`),
  applies the relationship's `where`, keeps the top `offset + limit` rows per parent with `ORDER BY … LIMIT n BY
  parent key`, and numbers them (`row_number() OVER (PARTITION BY parent key ORDER BY …)`). The statement returns the
  arrays in one row; `decode` nests the rows in TypeScript (parent and child keys matched as `hex(toString(key))`,
  children in row-number order, the page in its `ORDER BY` order). Resolvers only read prefetched data, so a live
  query result is computed once and fanned out.
- A relationship whose source runs a recursive CTE (authchains) and every level below it are evaluated in the
  statement's pipeline instead: their rows are `MATERIALIZED` CTEs and their arrays cross-joined one-row subqueries
  of the final `SELECT` (a scalar subquery containing a recursion is re-evaluated at every reference).
- `MATERIALIZED` CTEs (candidates, `acceptedRows`, the authchain walk) are always statement-level `WITH` items
  (`Sql.cte`), never nested in a subquery.
- Computed fields (`encoded_hex`, `fee_satoshis`, `locking_bytecode_pattern`, `header`, …) declare hidden selections
  (e.g. `__enc_inputs`) that the compiler adds; their resolvers compute the value in TypeScript. A relationship
  source receives the raw columns its level reads, so it can leave out a join nothing selected needs
  (`block_inclusions` joins `block_transaction` only for `transaction_index`).
- A per-node relationship takes `node:` (default: the node of the level above). Under a node-agnostic root it must name
  one; `Sql.nodeView` refuses otherwise. Every `node:` argument of the operation, at any depth, is pinned by the one
  request snapshot; a relationship naming another node reads that node's `visible(n)` (`node_<id>`/`visible_<id>`
  parameters) in the same statement.
- Rows of all levels of one root statement are capped (`maxResultRows`, default 500,000): above it the request fails
  with `RESULT_TOO_LARGE`, never a silent cut.
- **Snapshot contract (stage 2 change).** The statements no longer name the pinned `*_at` views: they read each
  view's base table with the view's gate predicate inlined (`visibility.ts` `inlinePinnedView`, a mirror of
  `050_views.sql`/`070_spend.sql`/`080_authchain.sql`; `visibility.spec.ts` proves view = inlined form for every
  view with fences, inline and overflow voids and a committed tail). The parameters are unchanged and still come
  only from `readSnapshot(Multi)`; the text leaves out terms that are constant for the bound arrays (an empty tail or
  void, no void overflow). The views stay for ad-hoc readers; a change to a view must be made in both places.

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

Stage 2 additions:
- The candidate filter is applied to the base `tx_acceptance` rows, below the collapsing `GROUP BY`
  (`inlinePinnedView`'s key filter; same rows), so the primary key is used even inside a recursive CTE step, where
  ClickHouse does not push the outer filter down.
- Transactions: every hash-list and `has_output` conjunct is a candidate set and they are intersected (L1: 1,441
  candidates of one conjunct, 7 of both). Spend-relation conjuncts stay first-found only (intersecting a 144k-spend
  category cost more than it saved). A positive `block_window` / `in_block` / `in_mempool` implies acceptance: the
  root's own acceptance check is left out.
- Blocks: a positive conjunct on `hash` or `height` restricts the node's `node_block` read to those blocks.
- Unspent outputs with a selective filter: one acceptance read for the transactions that created a matching output
  and those that spent one (L12: 6.2M → 3.3M rows read).
- Where output rows come through a category or script projection, the acceptance / candidate sets on
  `transaction_hash` are written on `identity(transaction_hash)`: otherwise ClickHouse analyses them against all
  61k marks of output's primary key (granularity 128) before choosing the projection (L12: 40 of 90 ms).

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
| R29 authchains | done | authchain index (080, heavy-path segments) + segment walk per node (§6.3) | G4 (node-two shorter); `authchain-index.spec.ts` (vs a link-by-link walk: merges, a conflict, a lagging node, tail above the spend watermark, every spend watermark, backfill, no index); X6 (776-long chain), X13 equal v1 |
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
`ch1_api_hasura_meta`). Recreate it (it was removed after the run):

```sh
psql … -c "CREATE DATABASE ch1_api_hasura_meta"
docker run -d --name ch1-api-hasura -p 18080:8080 --entrypoint graphql-engine \
  -e HASURA_GRAPHQL_METADATA_DATABASE_URL=postgres://chaingraph:…@host.docker.internal:15432/ch1_api_hasura_meta \
  -e 'HASURA_GRAPHQL_DATABASE_URL=postgres://chaingraph:…@host.docker.internal:15432/chipnet_pg?options=-c%20default_transaction_read_only%3Don' \
  -e HASURA_GRAPHQL_ADMIN_SECRET=… -e WEBHOOK_HANDLER_BASE_URL=http://127.0.0.1:1 -e HASURA_GRAPHQL_STRINGIFY_NUMERIC_TYPES=true \
  chaingraph/hasura:dev-build serve
docker exec ch1-api-hasura hasura-cli metadata apply --endpoint http://localhost:8080 --admin-secret … --project /hasura-data --skip-update-check
```

The ClickHouse database needs the current DDL (`bin/chaingraph-clickhouse-ddl.js`, incl. `080_authchain.sql`) and,
if its spend index predates the authchain index, the backfill (`bin/chaingraph-clickhouse-authchain-backfill.js`);
the stage-2 run used `ch1_api_chipcopy`, a scratch copy of the read-only `chipnet_ch_pp2` prepared that way (§6),
and `CHAINGRAPH_API_CATALOGUE_LABEL` names the database in the headers.

The runner walks every page on both sides (v1 as the library does: keyset, composite keyset,
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

Host: MacBook Pro (Apple M5 Pro), Docker VM 14 CPUs, ClickHouse 26.8 (`ch1-local`, UTXO off, spend index caught up),
Postgres 14 (`ch1-pg`, `chipnet_pg`) behind Hasura v2.49.5 (admin role, no row limits). Both at height 327,396,
2,847,058 transactions, 16 mempool transactions. v2 is in process (`yoga.fetch`, request snapshot included,
response cache off); v1 over HTTP to Hasura. p50 of 9 sequential runs after 2 warm-ups; "first page" for paged
documents (the equality run walks every page). Classes as the inventory: A < 20 ms, B 20–200, C 200 ms–1 s,
D 1–3 s, E > 3 s. Raw results stay local (`/tmp/chaingraph-v2-catalogue.json`, not committed).

- **Before** = stage 1 (`2182f5f`), re-measured on `chipnet_ch_pp2` the same day (median 136 ms, as in stage 1).
- **After** = stage 2 (`1433bcc`) on `ch1_api_chipcopy`: a scratch copy of `chipnet_ch_pp2` (same rows, `INSERT …
  SELECT` of every table) with `080_authchain.sql` applied and the authchain index backfilled, because
  `chipnet_ch_pp2` is read-only and the API now needs `authchain_link`. The 61 documents without authchains measured
  on `chipnet_ch_pp2` itself are within run-to-run noise of the copy (median 68 vs 69 ms in one pair of v2-only runs).
  Single runs vary by ±5–10 % (L12 was 137 ms in the full run and 71 ms re-run alone; the table has the re-run).

| | median | classes | faster than v1 | worst 5 |
|---|---:|---|---:|---|
| v2 before | 136 ms | B 49, C 13, E 1 | 19 / 63 | X6 7,768, X16 519, X13 465, L33 376, L4 335 |
| **v2 after** | **63 ms** | **A 4, B 53, C 6** | **29 / 63** | X6 393, X13 392, L33 365, L35 315, L30 278 |
| v1 (Hasura/Postgres) | 82 ms | A 26, B 25, C 10, D 2 | | L33 2,912, L30 2,502, X16 722 |

Equality after: 63 / 63 (48 in order, 15 as row sets, as before). Point lookups (by hash, height, node; no
relationship fan-out): X1 18, L43 17, L44 16, L46 17 ms (class A); X4 25, L25 22, L41 21, X2 28, X8 35, X11 27,
X14/X15 27–28 ms. Gate items on chipnet: every library document < 1 s (max L35 315 ms); AllHolders (L12) 71 ms
(G2 wants < 50 ms warm on golden, §6.4).

| Doc | Name | v2 rows | before p50 ms | after p50 ms | class after | v1 p50 ms | equality |
|---|---|---:|---:|---:|---|---:|---|
| L1 | LoanBorrowingTxs | 7 | 209 | 79 | B | 12 | equal |
| L2 | LoanCompletedRedemptions | 1 | 111 | 63 | B | 86 | equal |
| L3 | LoanInterestPayments | 695 | 182 | 146 | B | 124 | equal-unordered |
| L4 | UserLoanInteractions | 707 | 335 | 272 | C | 68 | equal-unordered |
| L5 | UserLoanInteractionsExcludingInterest | 12 | 209 | 105 | B | 220 | equal |
| L6 | UserStakingInteractions | 28 | 187 | 87 | B | 8 | equal |
| L7 | AllStakingInteractions | 192 | 241 | 153 | B | 16 | equal-unordered |
| L8 | RedemptionInteractions | 10 | 123 | 59 | B | 176 | equal-unordered |
| L9 | UserRedemptionSidecarCommitments | 6 | 117 | 62 | B | 154 | equal |
| L10 | UserRedemptionInteractions | 6 | 154 | 70 | B | 216 | equal |
| L11 | AllLiquidations | 10 | 137 | 67 | B | 88 | equal |
| L12 | AllHolders | 1536 | 136 | 71 | B | 374 | equal-unordered |
| L13 | allStakingReceipts | 6 | 69 | 33 | B | 5 | equal |
| L14 | finalizedUserRedemptions | 3 | 201 | 113 | B | 93 | equal |
| L15 | startedAndSwappedRedemptions | 5 | 118 | 60 | B | 4 | equal-unordered |
| L16 | UserStartedAndSwappedRedemptions | 3 | 172 | 74 | B | 3 | equal |
| L17 | finalizedRedemptions | 5 | 117 | 63 | B | 86 | equal-unordered |
| L18 | BorrowingTxsInPeriod | 1 | 60 | 50 | B | 100 | equal |
| L19 | FinalizedRedemptionsInPeriod | 0 | 38 | 26 | B | 85 | equal |
| L20 | InterestManager | 2 | 59 | 30 | B | 3 | equal |
| L21 | LoanClosure | 1 | 125 | 66 | B | 2 | equal |
| L22 | LoanClosures | 7 | 187 | 98 | B | 22 | equal |
| L23 | AllLoanClosures | 20 | 178 | 99 | B | 103 | equal-unordered |
| L24 | UserInitiatedLoanClosures | 8 | 106 | 63 | B | 104 | equal-unordered |
| L25 | LoanKeyOwner | 1 | 33 | 22 | B | 3 | equal |
| L26 | AllNewPeriodPoolTxs | 176 | 189 | 127 | B | 11 | equal |
| L27 | LoanBorrowingTx | 16 | 144 | 44 | B | 114 | equal-unordered |
| L28 | AllLoanClosureSummaries | 20 | 195 | 106 | B | 108 | equal-unordered |
| L29 | LoanBorrowingSummaries | 7 | 171 | 76 | B | 8 | equal |
| L30 | MonitorLoansWindow | 1000 | 314 | 278 | C | 2502 | equal-unordered |
| L31 | PriceContractUtxos | 5 | 189 | 76 | B | 126 | equal-unordered |
| L32 | RedemptionContractUtxos | 0 | 115 | 49 | B | 4 | equal |
| L33 | ContractUtxoActivity | 2305 | 376 | 364 | C | 2912 | equal-unordered |
| L34 | ContractOutputsByHash | 5 | 211 | 77 | B | 5 | equal |
| L35 | MonitorLoans | 1040 | 326 | 315 | C | 170 | equal |
| L36 | MonitorPriceContractUTXOs | 5 | 192 | 76 | B | 115 | equal-unordered |
| L37 | MonitorRedemptionContractUTXOs | 0 | 116 | 48 | B | 3 | equal |
| L38 | StabilityPool | 1 | 93 | 56 | B | 4 | equal |
| L39 | newPeriodPool | 1 | 143 | 73 | B | 6 | equal |
| L40 | MonitorLoansSignal | 1 | 93 | 59 | B | 98 | equal |
| L41 | ChaingraphBlockTipSignal | 1 | 27 | 21 | B | 354 | equal |
| L42 | MonitorLoansSignalPoll | 1 | 95 | 60 | B | 95 | equal |
| L43 | ChaingraphBlockTipSignalPoll | 1 | 23 | 17 | A | 342 | equal |
| L44 | MonitorLoansMempoolSignal | 0 | 26 | 16 | A | 2 | equal |
| L45 | MonitorContractUtxosMempoolSignal | 5 | 34 | 24 | B | 39 | equal |
| L46 | MonitorLoansMempoolSignalPoll | 0 | 28 | 17 | A | 2 | equal |
| L47 | MonitorContractUtxosMempoolSignalPoll | 5 | 35 | 24 | B | 41 | equal |
| X1 | MonitorMempools | 1 | 25 | 18 | A | 89 | equal |
| X2 | MonitorSyncTips | 1 | 79 | 28 | B | 2 | equal |
| X3 | MonitorForPayments | 39 | 152 | 56 | B | 295 | equal |
| X4 | ShowDuplicateTransactions | 2 | 54 | 25 | B | 16 | equal |
| X5 | GetSpendingTransactions | 1 | 257 | 80 | B | 9 | equal |
| X6 | GetTransactionDetails | 1 | 7768 | 392 | C | 82 | equal |
| X7 | GetBlockDetails | 1 | 200 | 133 | B | 19 | equal |
| X8 | GenesisBlockInfo | 1 | 73 | 35 | B | 8 | equal |
| X9 | SearchOutputsByLockingBytecodePrefix | 3548 | 324 | 194 | B | 59 | equal |
| X10 | SearchOutputsByLockingBytecode | 71 | 84 | 58 | B | 7 | equal |
| X11 | CountCashAccountOutputs | 1 | 54 | 27 | B | 3 | equal |
| X12 | GetJonathan100CashAccount | 1 | 183 | 116 | B | 3 | equal |
| X13 | GetBitjson100Authchain | 1 | 465 | 392 | C | 359 | equal |
| X14 | BlockchainSize | 1 | 36 | 28 | B | 367 | equal |
| X15 | BlockSizeStatsSince2021 | 1 | 34 | 27 | B | 370 | equal |
| X16 | TransactionStats2021Q1 | 1 | 519 | 77 | B | 620 | equal |

### 6.1 Where the fixed cost went (profile)

Method: every catalogue statement captured from the API and replayed with a `log_comment`; `system.query_log`
(`query_duration_ms`, ProfileEvents `QueryParseMicroseconds`, `QueryAnalysisMicroseconds`,
`QueryPlanBuildMicroseconds`, `QueryPlanOptimizeMicroseconds`, `read_rows`), `system.processors_profile_log` per
step, and server trace timelines (`send_logs_level`) for single statements. Scripts and raw numbers stayed in
`/tmp` (not committed).

Stage-1 statements: analysis was 45–60 % of a multi-level statement (L21 59 of 122 ms, X5 188 of 260, X13 238 of
476, L34 81 of 188); execution was small (L21: ~25 ms of processor time in total, the page 20 ms). Two causes:

1. **Every pinned view reference costs ~3–4 ms of analysis** (the parameterised view's query is analysed again per
   reference). A 12–20 reference statement paid 40–80 ms. Inlining the gate (§1.1): L21 analysis 57 → 40 ms, plan
   build 19 → 6 ms, 121 → 92 ms; catalogue median 136 → 117 ms.
2. **A `MATERIALIZED` CTE's definition is analysed again at every reference, recursively.** `IN (SELECT hash FROM
   page)` costs the page's own analysis (+2 ms per reference: 1 → 8 references = +14 ms), and a level referencing a
   level referencing the page pays for both. Scalar subqueries are analysed and evaluated once; their references are
   constants (8 references of one scalar: +0.6 ms). Levels as scalars (§1.1): L21 92 → 58 ms on the same SQL;
   catalogue median 117 → 88 ms.

Found on the way (each fixed, §1.1–1.2):
- `has(constant array, key)` uses the primary key but is linear per row: 28 s for 10k keys against 10M rows (`IN
  (SELECT arrayJoin(array))`: 46 ms). Links stay `IN` subqueries.
- A scalar subquery containing a `MATERIALIZED` CTE or a recursive CTE is re-evaluated at every reference; a
  `MATERIALIZED` CTE referenced from two scalars is evaluated once per scalar. Hence statement-level CTEs only, and
  pipeline levels for the authchain walk (X6: 6 evaluations of the walk → 1).
- The window function numbering 327k blocks for `Node.accepted_blocks(limit: 1)`: 65 → 27 ms with `LIMIT n BY`.
- The `block_transaction` join under every `block_inclusions`: ~5 ms per statement, now only for `transaction_index`.
- Primary-key analysis of `transaction_hash IN (6,674 hashes)` against output's 61k marks: 74 ms of CPU (~40 ms wall)
  before the category projection was chosen anyway (L12); `identity(transaction_hash)` keeps such sets out.

Tried and not taken:
- (c) Settings: `enable_analyzer = 0` is 25–150× slower (materialized CTEs are re-executed); `max_threads` 1/2/4,
  `optimize_use_projections = 0`, `use_query_condition_cache = 0`, `optimize_move_to_prewhere = 0`,
  `use_skip_indexes = 0`: within noise or worse; `query_plan_enable_optimizations = 0`: 4× slower.
- (d) Cached statements: ClickHouse 26.8 has no plan or analysis cache (every statement is analysed); the query
  result cache refuses these statements (window functions are non-deterministic) and, forced, would only duplicate
  the R37 response cache.
- (e) Node lookups: node names → ids are cached per process (unchanged). Node metadata (`node { name user_agent … }`)
  stays a level (2–5 ms): an in-process copy would serve stale connection times, which R37 deliberately never caches.
- Overlapping the cached snapshot's watermark check with the data query would save its round trip (2.7 ms of the
  ~4.5 ms API overhead here; Docker Desktop HTTP costs ~2.3 ms per round trip) but re-runs every in-flight request
  after each watermark advance (every mempool commit): not done.

What remains per statement: each level is one serial pipeline of ~1–1.5 ms when trivial and 3–9 ms typically
(collapsing `GROUP BY` of a membership table, a join, numbering); X4 (5 levels) 22 ms of SQL, X5 (9 levels) ~70 ms.
Acceptance checks over thousands of candidates read most of `tx_acceptance`'s hash-ordered granules (L4: 707 rows
with nested inputs/outputs, 8.3M rows read, 272 ms; v1 68 ms). On chipnet's small data Postgres' index nested loops
still win most 1–10-row documents (v1 2–10 ms).

### 6.2 Point lookups

Target ≤ 20 ms p50: met by X1, L43, L44, L46 (16–18 ms); X4 25, L25 22, L41 21, X2 28, X11 27, X8 35 ms. The floor
here is the two HTTP round trips (snapshot check, data statement: ~2.3 ms each on Docker Desktop) plus one serial
pipeline per level; X5 (output → spends → transaction → validations, timeline, inclusions → block → accepted_by →
node: 9 levels) is 80 ms and X7 (a block's 264 transactions' inputs for fees and `encoded_hex`) 133 ms.

### 6.3 Authchains (X6)

Chipnet's identity-output chains (spends of output 0): 2,378,928 such spends by 1,407,324 transactions; chains up to
**56,904 links**, median 3,999 from a random transaction, 811,120 transactions with a chain over 10,000. A per-link
walk (stage 1: ~9 ms per link, X6's 776 links 7.5 s) or iterative rounds that resolve one link per round per chain
cannot answer these in < 1 s, so the chain structure is precomputed (`080_authchain.sql`, `authchain-index.ts`):

- **Index.** Every transaction spending an output 0 gets one row placing it on the segment of its heaviest output-0
  parent (size = transactions whose chain passes through it) at the parent's position + 1; a transaction without a
  row starts its own segment. Heavy-path placement bounds the segment ends on any chain by log2 of the transactions:
  chipnet **at most 6 over every chain** (p99 3; X6: 2; the longest chain: 0). Following the first output-0 input
  instead would give up to 245 (p99 44; X6: 72). Filled by the spend index filler with each batch (same `batch` tag,
  same restart cleanup, visible up to the snapshot's spend watermark); existing databases run
  `bin/chaingraph-clickhouse-authchain-backfill.js` (chipnet: 1,407,324 rows from 1,181 batches in 51 s).
  Size on chipnet: 115 MiB (91 MiB uncompressed), of which the `p_tx` projection 65 MiB; ~11 % of `spend` (1.06 GiB).
- **Walk** (`model.ts`). One recursive CTE alternating per base: a keyed read of the current segment (members the node
  accepts, in position order while contiguous) and the accepted spender of the last member's output 0 from the input
  table. The rows only place transactions; what the node accepts is decided at read time, so conflicts (two spenders
  of one output in two nodes' views), stale placement and the tail above the spend watermark cost extra steps, never
  a wrong chain. `authchain-index.spec.ts` checks the walk against a link-by-link TypeScript walk per node at every
  spend watermark, after a backfill and with an empty index.
- **Result** (copy of chipnet, backfilled): X6 7,768 → **393 ms** (equal to v1), X13 465 → 392 ms; the longest chain
  (56,904 links) with X6's whole selection 452 ms of SQL; the walk alone 157 ms for X6's chain.

### 6.4 Unselective per-node roots (owner question 1): measured

`transactions(node)` with only an internal-id range (`after`/`before`) or only `after`, chipnet copy, SQL p50 of 9:

| Plan | id window (1,000 ids) | keyset after (100 rows) | rows read |
|---|---:|---:|---|
| Today (acceptance collapsed in full, then filtered) | 365 ms | 300–370 ms | 2.85M |
| Id range applied to the base `tx_acceptance` rows (no store change) | 9 ms | 14 ms (bound the window by the 202nd raw row after the cursor) | 0.69M / 1.55M |
| Same with a projection `p_node_tx ORDER BY (node_internal_id, transaction_internal_id)` | 5 ms | 8 ms | 7k / 80k |
| Projection size (chipnet) | 102.7 MiB, +43 % of `tx_acceptance` (as `p_node_height`); materialised in seconds | | |

Without the projection the id-range plan relies on part-level pruning (chipnet's parts are id-ordered by insertion);
on mainnet's merged parts it would read far more. The keyset-after plan returns, exactly, the accepted transactions
in (cursor, bound], so a page can be shorter than `limit` while `has_more` is true (a contract change). The same
projection would also serve acceptance checks by internal id: AllHolders' acceptance read 3.2M → 1.7M rows, but its
latency only 96 → 90 ms (the rest is the category reads and the primary-key analysis fixed above). Not decided here.


## 7. Known gaps and open questions

Known gaps (no owner decision needed):

1. **Fixed cost per statement** (stage 2: reduced, §6.1). Analysis no longer dominates; each level is one serial
   pipeline of ~1–9 ms, and acceptance checks over thousands of candidates read most of `tx_acceptance`'s
   hash-ordered granules. Documents of 1–10 rows cost 16–80 ms on chipnet (Postgres 2–10 ms); X5 (9 levels) 80 ms,
   L4 (707 rows with nested lists) 272 ms against v1 68 ms. Next candidates: fusing many-to-one levels into their
   parent, input/output reads of many random transaction hashes (X7, L4).
2. **Authchains** (stage 2: done, §6.3). Existing databases need the backfill once
   (`bin/chaingraph-clickhouse-authchain-backfill.js`, after the DDL); without it the walk is still exact but goes
   link by link (`max_recursive_cte_evaluation_depth` covers v1's 100,000 links).
3. **Unselective per-node roots** (e.g. `transactions(node)` with only an internal-id range, `transaction_groups`
   without a filter, `spends_aggregate` without a filter) read the node's whole acceptance set: 0.3–0.4 s on chipnet,
   not viable on mainnet. Measured options in §6.4 (owner question 1).
4. `inputs_aggregate` is not offered: inputs have no numeric field of their own; `spends_aggregate` /
   `spend_groups` aggregate the spent outputs (coinbase inputs excluded).
5. Timestamps of acceptance/validation and node connection metadata are store-local; the response cache never
   caches node connection metadata.
6. R39 `_like` not implemented (drop candidate); R40 replaced by `_prefix_in`.
7. The equality runs are chipnet only (owner: local = chipnet); golden/mainnet parity and latency are not run.

Open questions for the owner:

1. **Unselective per-node roots on mainnet (gap 3):** refuse them (require a selective filter or an internal-id
   window below a cap), or add a per-node acceptance projection ordered by internal id so they page in order?
   Numbers in §6.4: an id window costs 365 ms today, 9 ms with the id range applied below the acceptance collapse
   (no store change, relies on part pruning), 5 ms with `p_node_tx` (+102.7 MiB on chipnet, +43 % of
   `tx_acceptance`); keyset-after pages 8–14 ms if pages may be shorter than `limit` (`has_more` still exact).
2. **Per-node semantic changes for release notes:** `block_inclusions`, `spent_by` and authchains are per node (v1:
   any node). Confirm, or keep node-agnostic variants for explorers (e.g. `spent_by(node: null)` = any spend)?
3. **send_transaction** relays through the agent's internal API (as v1's Hasura action did), so the API holds no P2P
   connection. Confirm, or should the API connect to nodes itself?
4. **Response cache default:** on in `api:dev`/`main.ts` (`CHAINGRAPH_API_RESPONSE_CACHE=off` disables), off in
   `createApi`. A hit costs one watermark read instead of the request's snapshot read: confirm this counts as
   compatible with "exactly one readSnapshot per request" (it reads no snapshot at all).
5. **Authchain index on mainnet** (§6.3): one row per transaction that spends an output 0 (chipnet: 49 % of
   transactions, ~86 bytes per row with its `p_tx` projection, ~11 % of `spend`). Accept the storage and a one-off
   backfill per database (chipnet 51 s; mainnet proportionally hours, run alongside the agent)? Keying the rows by
   transaction internal id instead of hash would roughly halve it at the cost of id lookups in the walk.
6. **Inlined gate (contract change, §1.1):** the API reads base tables with the pinned views' predicate inlined
   (mirrored in `visibility.ts`, equality-tested) instead of naming the views. Confirm this reading of "every read
   through the pinned views"; the views remain the definition for ad-hoc readers.


## 8. Suites (final code, stage 2)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean |
| unit (`ava --match='!*[e2e]*'`) | 171 passed, 1 todo |
| API specs `build/api/*.spec.js` (scratch `ch1_api_*`, dropped) | 61 passed, 3 skipped (the chipnet spec without its env) |
| chipnet spec (`CHAINGRAPH_API_CHIPNET_DATABASE=ch1_api_chipcopy`, Hasura) | 3 passed: 63/63 documents equal; R13/R14 EXPLAIN; R25 holders, R28 hashes |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 163 passed (incl. `authchain-index.spec.ts`; `visibility.spec.ts` view = inlined gate) |
| e2e ClickHouse, UTXO on / off | 47 / 47 passed (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed |

The scratch databases (`ch1_api_chipcopy`, `ch1_api_authscan`, `ch1_api_hasura_meta`) and the v1 oracle container
were removed after the runs.
