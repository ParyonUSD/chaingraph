<!-- cspell:ignore chipnet paryon authchain authchains authhead bytecode pushdata unhex subquery denormalised setof normalisation pothos materialised unreviewed bitjson optimisation analysed backfill backfilled chipcopy agnostically tsdoc worktree specialised sats psql parameterised prewhere -->

# Phase 2: the Chaingraph v2 GraphQL API (stage 1: requirements; stage 2: performance; stage 3: owner decisions)

Date 2026-10-11 (stage 2: §6; stage 3, the owner's decisions of the same day: §9). GraphQL Yoga + Pothos over the
ClickHouse store (owner decision D-2), local only. Inputs:
`paryon_kubernetes-consolidation/docs/chaingraph/plans/clickhouse-api-requirements-inventory.md` (R1–R40, documents
L1–L47, X1–X16), `clickhouse-api-options.md` (spike criteria), [phase2-spike.md](phase2-spike.md),
[fix-pass-3.md](fix-pass-3.md) §4–5, [spend-index.md](spend-index.md),
[pre-phase2-concurrency-mempool.md](pre-phase2-concurrency-mempool.md).

Owner rules kept throughout: no Hasura-v2 compatibility; every acceptance-scoped root (and relationship) names its
node; structural facts (block inclusions, spends, authchain branches) are node-agnostic and complete, with
acceptance opt-in and exact per node (stage 3); every answer corresponds to exactly one consistent snapshot (one
`readSnapshot(Multi)` per computed request, its parameters never client-supplied; a response-cache hit is an answer
computed at one earlier snapshot with the same watermarks); UTXO tables off (unspent at query time, spent-output
attributes from the output join or the spend index).

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
| **Stage 3 (owner decisions 2026-10-11, §9):** gate views generated from the inlined definition (item 2) | `61ae8d3` |
| Structural facts node-agnostic; opt-in per-node acceptance; authchain branches (item 1) | `80aacfe` |
| Optional `p_node_tx`; unselective `transactions(node:)` pages (item 3) | `d4ca10a` |
| Unspent at query time by a hash anti-join (item 4) | `1205763` |
| Response cache on by default; the restated snapshot rule (item 5) | `c71d1e8` |
| `block_inclusions` in storage order; heights joined once when asked (+ test) | `12d91f2`, `b49f51c` |
| Authchain walks: no path copied per member (branching and per-node) | `a8d2f43`, `cae64ba` |
| Stage 3 docs, catalogue headers | docs commit |

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
| Inlined pinned-view gate (stage 2); the one gate definition (stage 3) | `src/store/clickhouse/visibility.ts` `pinnedViewDefinitions`, `pinnedGateTerms`, `inlinePinnedView`; `050_views.sql` generated by `view-ddl.ts` (`yarn clickhouse:views:generate`, checked by `view-ddl.spec.ts`) |
| Optional DDL items (stage 3) | `ddl/optional/p_node_tx.sql`; `ddl-apply.ts` `optionalDdlItems` (`CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL`, `--optional`) |
| Authchain index (stage 2): table, filler step, backfill | `ddl/080_authchain.sql`, `src/store/clickhouse/authchain-index.ts`, `bin/chaingraph-clickhouse-authchain-backfill.js` |
| Catalogue (63 documents), runner, chipnet spec | [`v2-documents/`](v2-documents), `src/api/catalogue.ts`, `catalogue-run.ts`, `catalogue.spec.ts` |
| Specs | `api-roots.spec.ts` (G1), `api-filters.spec.ts` + `spec-oracle.ts` (G2), `api-shape.spec.ts` (G3), `api-values.spec.ts` + `bytecode.spec.ts` (G4), `api-ops.spec.ts` (G5), `catalogue.spec.ts` (chipnet), spike `api.spec.ts`, `sql.spec.ts`, `schema.spec.ts`; stage 3: `api-structural.spec.ts`, `api-acceptance-order.spec.ts`, `view-ddl.spec.ts` |

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
- A per-node relationship (an acceptance fact: `node_validations`, `node_validation_timeline`, `Block.accepted_by`,
  `authchains`, the `Node` lists) takes `node:` (default: the node of the level above). Under a node-agnostic root it
  must name one; `Sql.nodeView` refuses otherwise. Structural relationships (`block_inclusions`, `spent_by`,
  `authchain_branches`) are node-agnostic; `accepted_by: node` and `accepted(node:)` opt into one node's acceptance
  (§9.1). Every `node:` and `accepted_by:` argument of the operation, at any depth, is pinned by the one request
  snapshot; a relationship naming another node reads that node's `visible(n)` (`node_<id>`/`visible_<id>`
  parameters) in the same statement.
- Rows of all levels of one root statement are capped (`maxResultRows`, default 500,000): above it the request fails
  with `RESULT_TOO_LARGE`, never a silent cut.
- **Snapshot contract (stage 2 change).** The statements no longer name the pinned `*_at` views: they read each
  view's base table with the view's gate predicate inlined (`visibility.ts` `inlinePinnedView`, a mirror of
  `050_views.sql`/`070_spend.sql`/`080_authchain.sql`; `visibility.spec.ts` proves view = inlined form for every
  view with fences, inline and overflow voids and a committed tail). The parameters are unchanged and still come
  only from `readSnapshot(Multi)`; the text leaves out terms that are constant for the bound arrays (an empty tail or
  void, no void overflow). The views stay for ad-hoc readers. Stage 3: both come from one definition
  (`pinnedViewDefinitions`, `pinnedGateTerms`): `050_views.sql` (and `spend_at` / `authchain_link_at` in 070/080) is
  generated from it, and `view-ddl.spec.ts` fails when the committed DDL differs from the generated text.

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
| 3 | `Output.spent_by(accepted_by, where, order_by, limit)` → `Input.transaction` | no: every stored spend (stage 3); `accepted_by: node` opts in |
| 4 | `Output.transaction`, `Input.transaction` | |
| 5 | `Transaction.block_inclusions(accepted_by, where, order_by, limit, offset)` → `block` | no: every stored block (stage 3); `accepted_by: node` opts in |
| 6 | `Block.accepted_by(node, where)` → `node` | yes |
| 7 | `Transaction.node_validations(node, where)` → `node` | yes |
| 8 | `Transaction.node_validation_timeline(node, order_by)` | yes |
| 9 | `Node.unconfirmed_transactions(where, order_by, limit, offset)` | yes (the node) |
| 10 | `Node.accepted_blocks(where, order_by, limit, offset)` | yes (the node) |
| 11 | `Block.transactions(where, order_by, limit, offset)` → `transaction`, `block` | |
| 12 | `Transaction.authchains(node)` → `authhead`, `migrations(order_by, limit)` → `transaction`; `Transaction.authchain_branches(limit, offset)` (stage 3) | `authchains` yes; `authchain_branches` no |
| 13 | `Transaction.identity_output`, `signing_output`, `data_carrier_outputs(where, order_by, limit)` | |

Computed fields: `Transaction.input_value_satoshis`, `fee_satoshis`, `encoded_hex`; `Input.value_satoshis`,
`unlocking_bytecode_pattern`, `redeem_bytecode_pattern`; `Output.locking_bytecode_pattern`; `Block.input_count`,
`output_count`, `input_value_satoshis`, `fee_satoshis`, `generated_value_satoshis`, `header`, `encoded_hex`;
`Node.unconfirmed_transaction_count`. Values come from the output join (page only), never from denormalised input
columns. Per-node acceptance fields (stage 3, node required): `Transaction.accepted(node:)`, `Input.accepted(node:)`,
`BlockInclusion.accepted(node:)`.

Changes from v1 to release-note: ACC is the root's `node:`; WIN (`block_window`) is per node (v1: "any block", "no
inclusion anywhere"); `block_inclusions`, `spent_by` (relationship and filter) are node-agnostic as in v1 (stage 3;
stage 1 had narrowed them to the node), with `accepted_by: node` / `accepted(node:)` where v1 filtered on
`accepted_by { node { name } }`; `BlockInclusion.accepted_at` is gone (per node: `block { accepted_by(node:) {
accepted_at } }`); authchains are per node (v1: any spend; at most one chain per node), every branch in
`authchain_branches`; `identity_output`/`signing_output`/migration `transaction` are objects (v1: lists
of at most one, a missing signing output a row of nulls); bytes have no `\x` prefix; indexes are Int; offset is capped
and discouraged; unbounded lists need a `limit` or hit the row cap.

## 3. Requirements: how each is met

| R | Status | Implementation | Test (independent computation) |
|---|---|---|---|
| R1 node argument | done | per-node roots and acceptance relationships; refused below node-agnostic roots without `node:`; structural relationships node-agnostic with opt-in `accepted_by` / `accepted(node:)` (stage 3) | G1 R8 spec; `api-structural.spec.ts`; catalogue |
| R2 one snapshot | done | `PinnedSnapshot` pins every `node:` of the operation; one `readSnapshotMulti` | S6, G1 R5 (two named nodes, 1 snapshot), G5 live (1 per advance) |
| R3 ACC, WIN | done | root `node:`; `block_window` per node | S1; catalogue L1, L4–L11 equal v1 |
| R4 unspent | done | query time, spend relation | S5; L12, L13, L20, L25, L31, L32, L36, L37 equal v1 |
| R5 facts as data | done | edges 5–8; stage 3: inclusions and spends complete (stale/fork blocks, conflicting spends), acceptance per node | G1 R5 vs `*_v` views; `api-structural.spec.ts` (fork, stale block, conflicting block and mempool spends); X3, X5, X6, L34 equal v1 |
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
| R29 authchains | done | authchain index (080, heavy-path segments) + segment walk per node (§6.3); stage 3: node-agnostic `authchain_branches` (§9.1) | G4 (node-two shorter); `authchain-index.spec.ts` (vs a link-by-link walk: merges, a conflict, a lagging node, tail above the spend watermark, every spend watermark, backfill, no index; branches vs an all-paths walk, same cases); `api-structural.spec.ts`; X6 (776-long chain), X13 equal v1 |
| R30 search | done | `outputs` + `outputs_aggregate` | X3, X9–X13 equal v1 |
| R31 live queries | done | all per-node roots are subscriptions, on the shared hub | G5: the 12 live documents, pushes only on change, 1 run per topic per advance |
| R32 signals | done | `watermark(node)`, signal documents | G5; L40–L47 equal v1 |
| R33 caps | done | per-root page caps, nested cap, statement row cap, `has_more` | G3, G5 |
| R34 types, limits, trusted documents | done | BigInt/Int; depth 16, aliases 64, root fields 32, fields 2,000; trusted documents by sha256 (HTTP and graphql-ws) | G5 |
| R36 latency | measured (chipnet) | §6 | catalogue runner |
| R37 response cache | done (COULD); on by default (`main.ts`, stage 3) | keyed on document, variables and the snapshot's watermarks; every answer corresponds to exactly one consistent snapshot (§9.5) | G5: a hit = 1 watermark read, 0 snapshots, 0 data queries |
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

Chipnet has one node, so per-node and v1's "any node" coincide for authchains (and, before stage 3, for block
inclusions and spends). Stage 3 made inclusions and spends node-agnostic again, which is v1's form; both stores hold
the same chipnet blocks, so the documents compare either way. The scratch specs cover the two-node differences
(node-two lacks a block and the mempool: G1 R5, G2, G4 authchain) and, stage 3, forks, stale blocks and conflicting
spends (`api-structural.spec.ts`).

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
3. **Unselective per-node roots** (stage 3: `transactions(node:)` pages in id order use `p_node_tx` when present,
   §9.3). Still reading the node's whole acceptance set: `transaction_groups` / `spends_aggregate` /
   `transactions_aggregate` without a filter, `transactions(node:)` with a non-selective filter or an `order_by`
   other than the internal id, and `outputs(node:)` / `inputs(node:)` without a selective filter.
4. `inputs_aggregate` is not offered: inputs have no numeric field of their own; `spends_aggregate` /
   `spend_groups` aggregate the spent outputs (coinbase inputs excluded).
5. Timestamps of acceptance/validation and node connection metadata are store-local; the response cache never
   caches node connection metadata.
6. R39 `_like` not implemented (drop candidate); R40 replaced by `_prefix_in`.
7. The equality runs are chipnet only (owner: local = chipnet); golden/mainnet parity and latency are not run.

Open questions for the owner (stage 2 numbering; 1, 2, 4 and 6 decided 2026-10-11, §9):

1. *Decided (§9.3):* `p_node_tx` as an optional DDL item, default off until the mainnet numbers decide; the
   compiler uses it when present, the id range on the base rows otherwise.
2. *Decided (§9.1):* structural facts node-agnostic and complete, acceptance opt-in per node.
3. **send_transaction** relays through the agent's internal API (as v1's Hasura action did), so the API holds no P2P
   connection. Confirm, or should the API connect to nodes itself?
4. *Decided (§9.5):* response cache on by default; "every answer corresponds to exactly one consistent snapshot".
5. **Authchain index on mainnet** (§6.3): one row per transaction that spends an output 0 (chipnet: 49 % of
   transactions, ~86 bytes per row with its `p_tx` projection, ~11 % of `spend`). Accept the storage and a one-off
   backfill per database (chipnet 51 s; mainnet proportionally hours, run alongside the agent)? Keying the rows by
   transaction internal id instead of hash would roughly halve it at the cost of id lookups in the walk.
6. *Decided (§9.2):* the inlined gate stays; the views are generated from the same definition.

New questions from stage 3 are in §9.7.


## 8. Suites (final code, stage 3)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell (`src`) | clean (eslint: the 15 tsdoc warnings of stage 2, no errors) |
| unit (`ava --match='!*[e2e]*'`, incl. `view-ddl.spec.ts`) | 174 passed, 1 todo |
| API specs `build/api/*.spec.js` (scratch `ch1_api_*`, dropped) | 63 passed, 3 skipped (the chipnet spec without its env) |
| chipnet spec (`CHAINGRAPH_API_CHIPNET_DATABASE=ch1_api_chipcopy`, Hasura) | 3 passed: 63/63 documents equal; R13/R14 EXPLAIN; R25 holders, R28 hashes |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 166 passed (incl. `authchain-index.spec.ts` with branches, `visibility.spec.ts` view = inlined gate, `view-ddl.spec.ts`); one earlier run of the same code had 1 failure in `g1-fix.spec.ts` (lab conditions, utxo off: "a prefix of the chain, no hole", sees 32 of 160), not reproduced in 3 runs of the file and a full re-run |
| e2e ClickHouse, UTXO on / off | 47 / 47 passed (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 102 passed |

Stage 2's results were: unit 171 + 1 todo, API 61 + 3 skipped, store 163, e2e as above. The scratch databases
(`ch1_api_chipcopy`, `ch1_api_hasura_meta`), the stage-2 worktree and the v1 oracle container were removed after the
runs.

## 9. Stage 3: the owner's decisions of 2026-10-11

Five decisions, implemented on `experiment/clickhouse-store` (commits in the table at the top). Chipnet numbers are
on `ch1_api_chipcopy` (a scratch copy of the read-only `chipnet_ch_pp2` with the authchain index backfilled, as in
§6), host as in §6 but shared with other sessions (load 2.6–4.5 during the runs), p50 of 9 warm requests.

### 9.1 Structural facts vs acceptance (decision 1)

A structural fact is something the store holds whatever any node thinks: a block contains a transaction, an input
spends an outpoint, a transaction continues an identity output. Acceptance is per node. Stage 1 had narrowed the
structural relationships to the level's node; they are now node-agnostic and complete, and acceptance is opt-in and
exact per node (no field answers "accepted by any node"):

```graphql
type Transaction {
  # every stored block containing the transaction (stale and fork blocks included), in storage order
  block_inclusions(accepted_by: String, where: BlockInclusionFilter, order_by: [BlockInclusionOrderBy!],
                   limit: Int, offset: Int): [BlockInclusion!]!
  accepted(node: String!): Boolean!            # in a block the node accepts or in its mempool
  authchains(node: String): [Authchain!]!      # unchanged: the chain the node accepts (default: level's node)
  authchain_branches(limit: Int, offset: Int): [Authchain!]!   # every branch, node-agnostic
}
type BlockInclusion {
  accepted(node: String!): Boolean!            # the node accepts this block (replaces the per-node accepted_at)
  block: Block!
  block_internal_id: BigInt!
  transaction_index: Int
}
type Output {
  # every stored input spending the output (conflicting spends included)
  spent_by(accepted_by: String, where: InputFilter, order_by: [InputOrderBy!], limit: Int): [Input!]!
}
type Input { accepted(node: String!): Boolean! }   # the node accepts the spending transaction
input OutputFilter { spent_by: InputFilter }       # node-agnostic EXISTS, as the relationship
```

- `accepted_by: node` keeps the rows that node accepts (inclusions: blocks it accepts, read from its acceptance
  rows; spends: spending transactions it accepts). Without it, the rows come from `block_transaction` / `input`
  only (gated by visible(0) like every node-agnostic read).
- `accepted(node:)` is a per-node relation of one key column (`tx_acceptance_at` / `node_block_at` of that node,
  restricted to the parent keys); the resolver answers "has a row". `node` is required.
- `accepted_by:` values are pinned by the request snapshot exactly like `node:` (`operationNodeNames`); an unnamed
  node is refused.
- The `OutputFilter.spent_by` filter mirrors the relationship (any stored spend). A filter that needs a spend the
  level's node accepts adds `transaction: { block_window: {} }` (the per-node WIN predicate, mempool included).
- Acceptance-scoped roots (`transactions`, `outputs`, `unspent_outputs`, `inputs`, `blocks`, `block_tip`,
  `mempool`, `node`, aggregates and groups) and acceptance relationships (`node_validations`,
  `node_validation_timeline`, `Block.accepted_by`, the `Node` lists, `authchains`) keep their node. Unspent at
  query time stays per node: a spend in a stale block no node accepts does not spend an output.
- **2026-10-12 (owner decision, item 3):** `block_inclusions` is ordered by block height, then block hash, by default
  (`order_by: { block_internal_id: ASC }` gives storage order). The paragraph below describes the stage-3 default.
- `block_inclusions` is in storage order (`block_internal_id`): ordering by height joins every inclusion to its
  block (~10–20 ms on the library documents: L26 121 → 144 ms) for a list that is almost always one block.
  `order_by: { block: { height: ASC } }` or a height filter still gets it (the inclusions are read once, a
  statement-level CTE).

**Authchain branches.** `authchain_branches` walks every stored spend of output 0 from the base: one `Authchain` per
path to an output 0 no stored transaction spends (or to the depth limit), longest first; a transaction merging two
members of the tree is on two paths. It uses the authchain index like the per-node walk (§6.3) but trusts it less:
along a segment it takes only positions holding exactly one member (two members at a position are a conflict), and
every member it adds is verified against the input table (all spenders of its output 0): spenders other than the
next member start branches; a next member that is not a spender cuts the path (a stale placement costs a step,
never a wrong edge). Walk rows are identified by (base, sipHash64(path)) in the joins; the path never enters a join
with segment members (see below).

- Chipnet copy: X6's base (776 links) 172 ms (per-node 146 ms); the longest chain (56,903 links) 268 ms (per-node
  101 ms); 200 random bases with an output-0 spend: every result equals `authchains(node:)` (chipnet has no stored
  conflict on those chains), 1.7 s for all 200 against 1.4 s.
- Found on the way, also in the per-node walk: ClickHouse 26.8 may evaluate an expression over `walk`'s columns after
  a later join, so a path still selected from `walk AS w` was carried into the join with every member of the next
  segment. A 5,870-link path crossing into a 23,027-member segment (a chipnet base found by query): per-node walk
  566 ms / 6 GiB → 146 ms / 18 MiB; branching walk 2.3 s / 7.2 GiB → 0.25 s / 0.28 GiB. Both walks now drop the path
  in a subquery of `walk` before any join (`cae64ba`). The catalogue's X6/X13 never crossed such a boundary.

**Tests.** `api-structural.spec.ts` (new): blocks 0–1 on both nodes, a fork at height 2 (2a on node-one, 2b on
node-two) where both blocks include one transaction and spend one coin differently, a block 3s on 2a that node-one
accepts and then releases (stored, accepted by no node), conflicting mempool spends of one output 0 (one per node).
Checked against the fixture itself: every inclusion and spend listed once with `accepted(node:)` per node;
`accepted_by` per node; `Transaction.accepted`; heights ordered/filtered; below a per-node root the structure stays
complete; unspent at query time per node (the stale block's spend spends nothing); the `spent_by` filter
node-agnostic and its opt-in form; `authchain_branches` = both mempool branches, `authchains(node:)` one per node.
G1 R5/R8 compare node-agnostic inclusions and spends with `block_transaction_v ⋈ block_v` / `input_v` and the
per-node flags with `node_block_v` / `tx_acceptance_v`; G2's evaluator treats `spent_by` node-agnostically (252
filters × 2 nodes); `authchain-index.spec.ts` checks `authchain_branches` against an all-paths walk in TypeScript
(merges, the k1/k2 conflict, transactions above the spend watermark) at every spend watermark, with no index and
after a backfill.

**Catalogue.** v1's `block_inclusions` and `spent_by` were node-agnostic, so the node-agnostic form is v1's.
Rewritten: X4 (`block_inclusions` without a node; `accepted_by(node: $node)` on the block, v1 listed every
accepting node), X5 (`spent_by` without a node; `node_validations`, `node_validation_timeline` and `accepted_by`
name `$node`, as they no longer inherit it from `spent_by`), L34 (`spent_by` without a node; v1's
`block_inclusions(where: { block: { accepted_by: { node: … } } })` is `block_inclusions(accepted_by: $node)`; plain
`block_inclusions` node-agnostic). The other documents' plain `block_inclusions { … }` / `spent_by { … }` now read
the v1 form without change. All 63 equal v1 (§9.6).

### 9.2 One source of truth for the gate (decision 2)

`visibility.ts` holds the one definition of every pinned view (`pinnedViewDefinitions`: base table, select list,
collapsing key) and its gate (`pinnedGateTerms`: the general form for the DDL views, the snapshot-specialised form
for the inlined API reads). `view-ddl.ts` generates `050_views.sql` from it (every `*_at` view, its `*_v` twin, and
`node_v`) and the `spend_at` / `authchain_link_at` statements of 070/080; `yarn clickhouse:views:generate` rewrites
050 (`--check` compares). `view-ddl.spec.ts` (unit) fails when a committed file differs from the generated text;
`visibility.spec.ts` still proves view = inlined form on a scratch database (fences, inline and overflow voids, a
committed tail). The regenerated 050 differs from the hand-written one in whitespace only.

### 9.3 Per-node acceptance projection `p_node_tx` (decision 3)

> **2026-10-12 (owner decision, item 1):** `p_node_tx` is now a **default** DDL item; opt out with
> `CHAINGRAPH_CLICKHOUSE_DDL_SKIP=p_node_tx` (or `--skip p_node_tx`). See [utxo-async.md](utxo-async.md) §1.

- **DDL.** `ddl/optional/p_node_tx.sql`: `tx_acceptance` projection `ORDER BY (node_internal_id,
  transaction_internal_id)`, off by default. Applied when named: `CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL=p_node_tx` (DDL
  CLI, `applyClickHouseDdl`, `apply.sh`) or `bin/chaingraph-clickhouse-ddl.js --optional p_node_tx`. The DDL CLI
  materializes it (an asynchronous mutation) once, when it adds it to a table with rows; a re-apply does not.
- **Compiler.** The API notices the projection (`system.projections`, re-checked every 60 s, `ApiDb.storeFeatures`;
  it changes plans, never results). An unselective `transactions(node:)` page in internal-id order (no `where`, the
  default order, `after` / `before` / `offset`) then reads the first `8·n + 1,000` raw acceptance rows after the
  cursor in (node, id) order (a few granules of the projection, n = rows needed), collapses only the transactions
  before the last id read, and checks that it found n of them; if not (a run of released or cancelled rows), it
  takes the whole range instead, a constant branch, so the page and `has_more` are exact either way. Without the
  projection: the id range applied to the base acceptance rows below the collapse (the no-store-change fallback of
  §6.4). The id predicates are qualified (`tx_acceptance.transaction_internal_id`): the collapsed rows alias
  `any(transaction_internal_id)`.

| `transactions(node:)` page (chipnet copy, API p50 of 9) | stage 2 | without `p_node_tx` | with `p_node_tx` |
|---|---:|---:|---:|
| first 100 | ~365 | 355 | 32 |
| 100 after id 1,000,000 | ~365 | 214 | 28 |
| 100 after id 2,800,000 (near the tip) | ~365 | 18 | 15 |
| last 100 (`order: DESC`) | ~365 | 346 | 31 |
| 10,000 after id 1,000,000 | – | 237 | 52 |
| id window of 1,000 (`after` + `before`) | 365 | 19 | 20 |

  Projection size on chipnet: 102.7 MiB (+43 % of `tx_acceptance`, as `p_node_height`); materialized in seconds.
  `api-acceptance-order.spec.ts`: page walks (both orders, limits 1–100, id windows, offset) equal the accepted
  transactions in id order (`*_v` views) with the projection, with its fallback forced (windows of 1–5 raw rows),
  and without it; the DDL tool materializes once.
- Still reading a node's whole acceptance set: aggregates and groups without a filter, `transactions(node:)` with
  a non-selective filter or another order, `outputs` / `inputs` / `unspent_outputs` without a selective filter.

### 9.4 Unspent at query time: hash anti-join (decision 4)

> **2026-10-12 (owner decision, item 4):** the unspent roots now read the stored per-node UTXO set maintained off the
> commit path when the store keeps one (`CHAINGRAPH_API_UTXO_SET`, default on); the query-time forms below remain the
> fallback. See [utxo-async.md](utxo-async.md).

The forms that subtract a large spent set (no filter, or a filter no index narrows) are now a `LEFT ANTI JOIN` row
source (`unspentOutputRows`; the server's default `join_algorithm` `direct,parallel_hash,hash,ie_join` takes
`parallel_hash`) instead of a tuple `NOT IN`; same results (count and value checked against the `NOT IN` form;
G2 runs `unspent_outputs` for every output filter, and no filter, on both nodes against the evaluator).

| Chipnet copy, node `chipnet-local` | `NOT IN` | anti-join |
|---|---:|---:|
| whole UTXO set: count and value (1,210,030 / 1,343,492,499,959,490 sats) | 5,122 ms | 1,585 ms |
| first page of 100 (default order) | 4,820 ms | 1,505 ms |
| value > 1 BCH: count (266,371) | 2,408 ms | 1,813 ms |
| selective (L12, L13, L20, L25, L31, L32, L36, L37) | 22–86 ms | within noise (±10 %): kept `NOT IN` |

On mainnet the whole-set forms remain batch work either way (large-aggregations.md §4; the Cloud run of the v1
form timed out at 600 s).

### 9.5 Response cache and the snapshot rule (decision 5)

On by default in `main.ts` (`CHAINGRAPH_API_RESPONSE_CACHE=off` disables it; `createApi` leaves it off unless asked).
The rule, as restated: **every answer corresponds to exactly one consistent snapshot.** A computed request reads one
snapshot (`readSnapshot(Multi)`) for all its roots; a cache hit returns an answer computed entirely at one earlier
snapshot whose watermarks equal those the request just read (visible(0) and visible(n) of every pinned node). Commits
that land without moving a watermark (the committed tail above visible(0); the spend watermark) can be newer than
a hit's snapshot, exactly as with the request-snapshot cache (fix-pass-3.md §4); the spend watermark never changes
an answer (the spend relation and the authchain walks are exact at any index lag).

### 9.6 Catalogue: equality and latency

63/63 documents equal v1 (48 row for row in order, 15 as row sets, as in stage 2). Latency against stage 2
(`b6ffb65`, built in a scratch worktree) on the same database, run back to back (stage 2, then stage 3), p50 of 9
warm requests, first page; `p_node_tx` absent (the default):

| | median | classes | faster than v1 | worst 5 |
|---|---:|---|---:|---|
| stage 2 (re-run) | 61.8 ms | A 5, B 52, C 6 | 28 / 63 | X6 379, X13 354, L33 294, L35 260, L4 254 |
| **stage 3** | **59.3 ms** | **A 4, B 53, C 6** | **28 / 63** | X13 369, X6 352, L33 299, L35 267, L4 254 |
| v1 (Hasura/Postgres) | 82.6 ms | | | |

Beyond run-to-run noise (±10–15 %): X10 55 → 30 ms, X5 77 → 52, L34 74 → 62 (`spent_by` no longer checks
acceptance), L15 56 → 44. An earlier stage-3 run with a height-ordered `block_inclusions` had L26 121 → 144 ms
and X4 26 → 32 (now 117 and 24). Headers of the document files hold the stage-3 numbers.

| Doc | Name | v2 rows | stage 2 p50 ms | stage 3 p50 ms | class | v1 p50 ms | equality |
|---|---|---:|---:|---:|---|---:|---|
| L1 | LoanBorrowingTxs | 7 | 78 | 76 | B | 9 | equal |
| L2 | LoanCompletedRedemptions | 1 | 64 | 62 | B | 85 | equal |
| L3 | LoanInterestPayments | 695 | 138 | 139 | B | 118 | equal-unordered |
| L4 | UserLoanInteractions | 707 | 254 | 254 | C | 69 | equal-unordered |
| L5 | UserLoanInteractionsExcludingInterest | 12 | 100 | 100 | B | 229 | equal |
| L6 | UserStakingInteractions | 28 | 84 | 86 | B | 7 | equal |
| L7 | AllStakingInteractions | 192 | 144 | 143 | B | 14 | equal-unordered |
| L8 | RedemptionInteractions | 10 | 56 | 55 | B | 178 | equal-unordered |
| L9 | UserRedemptionSidecarCommitments | 6 | 60 | 61 | B | 157 | equal |
| L10 | UserRedemptionInteractions | 6 | 68 | 67 | B | 215 | equal |
| L11 | AllLiquidations | 10 | 63 | 63 | B | 83 | equal |
| L12 | AllHolders | 1536 | 62 | 61 | B | 352 | equal-unordered |
| L13 | allStakingReceipts | 6 | 30 | 31 | B | 4 | equal |
| L14 | finalizedUserRedemptions | 3 | 100 | 98 | B | 85 | equal |
| L15 | startedAndSwappedRedemptions | 5 | 56 | 44 | B | 3 | equal-unordered |
| L16 | UserStartedAndSwappedRedemptions | 3 | 69 | 59 | B | 3 | equal |
| L17 | finalizedRedemptions | 5 | 54 | 55 | B | 83 | equal-unordered |
| L18 | BorrowingTxsInPeriod | 1 | 46 | 47 | B | 83 | equal |
| L19 | FinalizedRedemptionsInPeriod | 0 | 23 | 24 | B | 84 | equal |
| L20 | InterestManager | 2 | 28 | 28 | B | 3 | equal |
| L21 | LoanClosure | 1 | 56 | 56 | B | 3 | equal |
| L22 | LoanClosures | 7 | 90 | 90 | B | 21 | equal |
| L23 | AllLoanClosures | 20 | 96 | 95 | B | 104 | equal-unordered |
| L24 | UserInitiatedLoanClosures | 8 | 63 | 63 | B | 99 | equal-unordered |
| L25 | LoanKeyOwner | 1 | 20 | 20 | B | 3 | equal |
| L26 | AllNewPeriodPoolTxs | 176 | 116 | 117 | B | 10 | equal |
| L27 | LoanBorrowingTx | 16 | 43 | 43 | B | 101 | equal-unordered |
| L28 | AllLoanClosureSummaries | 20 | 102 | 102 | B | 101 | equal-unordered |
| L29 | LoanBorrowingSummaries | 7 | 74 | 72 | B | 7 | equal |
| L30 | MonitorLoansWindow | 1000 | 247 | 247 | C | 2329 | equal-unordered |
| L31 | PriceContractUtxos | 5 | 72 | 70 | B | 106 | equal-unordered |
| L32 | RedemptionContractUtxos | 0 | 45 | 43 | B | 3 | equal |
| L33 | ContractUtxoActivity | 2305 | 294 | 299 | C | 2603 | equal-unordered |
| L34 | ContractOutputsByHash | 5 | 74 | 62 | B | 5 | equal |
| L35 | MonitorLoans | 1040 | 260 | 267 | C | 156 | equal |
| L36 | MonitorPriceContractUTXOs | 5 | 74 | 72 | B | 112 | equal-unordered |
| L37 | MonitorRedemptionContractUTXOs | 0 | 45 | 43 | B | 3 | equal |
| L38 | StabilityPool | 1 | 52 | 53 | B | 4 | equal |
| L39 | newPeriodPool | 1 | 69 | 70 | B | 5 | equal |
| L40 | MonitorLoansSignal | 1 | 57 | 58 | B | 95 | equal |
| L41 | ChaingraphBlockTipSignal | 1 | 21 | 22 | B | 359 | equal |
| L42 | MonitorLoansSignalPoll | 1 | 58 | 59 | B | 100 | equal |
| L43 | ChaingraphBlockTipSignalPoll | 1 | 16 | 18 | A | 347 | equal |
| L44 | MonitorLoansMempoolSignal | 0 | 17 | 18 | A | 2 | equal |
| L45 | MonitorContractUtxosMempoolSignal | 5 | 23 | 24 | B | 39 | equal |
| L46 | MonitorLoansMempoolSignalPoll | 0 | 17 | 18 | A | 1 | equal |
| L47 | MonitorContractUtxosMempoolSignalPoll | 5 | 22 | 24 | B | 41 | equal |
| X1 | MonitorMempools | 1 | 16 | 17 | A | 91 | equal |
| X2 | MonitorSyncTips | 1 | 27 | 27 | B | 2 | equal |
| X3 | MonitorForPayments | 39 | 53 | 54 | B | 301 | equal |
| X4 | ShowDuplicateTransactions | 2 | 24 | 24 | B | 16 | equal |
| X5 | GetSpendingTransactions | 1 | 77 | 52 | B | 8 | equal |
| X6 | GetTransactionDetails | 1 | 379 | 352 | C | 86 | equal |
| X7 | GetBlockDetails | 1 | 129 | 131 | B | 20 | equal |
| X8 | GenesisBlockInfo | 1 | 33 | 35 | B | 8 | equal |
| X9 | SearchOutputsByLockingBytecodePrefix | 3548 | 173 | 163 | B | 53 | equal |
| X10 | SearchOutputsByLockingBytecode | 71 | 55 | 30 | B | 7 | equal |
| X11 | CountCashAccountOutputs | 1 | 26 | 27 | B | 2 | equal |
| X12 | GetJonathan100CashAccount | 1 | 112 | 124 | B | 3 | equal |
| X13 | GetBitjson100Authchain | 1 | 354 | 369 | C | 330 | equal |
| X14 | BlockchainSize | 1 | 24 | 25 | B | 333 | equal |
| X15 | BlockSizeStatsSince2021 | 1 | 24 | 26 | B | 336 | equal |
| X16 | TransactionStats2021Q1 | 1 | 62 | 61 | B | 581 | equal |

### 9.7 Questions for the owner

1. **`block_inclusions` order:** storage order (`block_internal_id`) by default, height order only when asked
   (saves 10–20 ms on ~25 library documents). Confirm, or keep height order as the default?
   **Owner decision 2026-10-12:** the default order of `block_inclusions` is height, then block hash (not storage
   order).
2. **`BlockInclusion.accepted_at`** is gone (it was the level node's acceptance time); per node it is
   `block { accepted_by(node:) { accepted_at } }`. Confirm, or add `accepted_at(node: String!)`?
   **Owner decision 2026-10-12: no shortcut.** v2 keeps `accepted_at` on blocks and `validated_at` on transactions
   and reaches a block by traversal only: no `transaction.accepted_at(node:)`, no `BlockInclusion.accepted_at`
   shortcut and no `transaction.block(node:)` field. Clients use
   `block_inclusions(accepted_by: node) { block { accepted_by(node:) { accepted_at } } }`.
3. **The `spent_by` filter** is node-agnostic like the relationship; acceptance through `transaction: { block_window:
   {} }` at the level's node. Confirm, or add an explicit filter-level `accepted_by`?
   **Owner decision 2026-10-12:** add it. `spent_by: { accepted_by: node, … }` (input type `SpentByFilter`: the
   `InputFilter` fields plus `accepted_by`) keeps only spends whose spending transaction the node accepts (a block it
   accepts or its mempool) at the request snapshot, per node exactly, as the `spent_by(accepted_by:)` field; the node
   is pinned by the request like any `accepted_by:` argument (also when named only inside a variable's filter).
   Without it the filter stays node-agnostic and structural. Equal to `transaction: { block_window: {} }` at the
   node's own root (`api-structural.spec.ts`: fork, stale and conflicting mempool spends). See
   [fix-pass-4.md](fix-pass-4.md) §2.
4. **`p_node_tx` on mainnet:** the GKE session can measure it by applying the DDL with and without
   `CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL=p_node_tx` (or `--optional p_node_tx`; the API notices it within 60 s) and
   paging `transactions(node:, limit: 100, after: …)` from a few cursors. The plan relies on
   `tx_acceptance.transaction_internal_id` being the transaction row's `internal_id` (true for every store write
   path; equal pages on chipnet and in the specs).
   **Owner decision 2026-10-12:** `p_node_tx` is ON by default (floor session 2026-10-12: build 48 s / 14.76 GiB on
   golden, U1 4.0 s and U3 105 ms with it; catalogue key CPU unchanged).
5. **`authchain_branches` size:** branches are paths, so a tree with many conflicting spends on a long chain repeats
   the shared prefix per branch. The depth limit (100,000) bounds each path and the row cap (500,000) the rows
   returned, but nothing bounds the number of branches the walk builds. Acceptable for an explorer feature, or cap
   the branches per base (e.g. 1,000, with an error above it)?
6. Still open from stage 2: send_transaction through the agent (§7 question 3), the authchain index on mainnet
   (§7 question 5); unspent at query time on mainnet (§9.4) depends on the UTXO-table / maintained-total decision.
