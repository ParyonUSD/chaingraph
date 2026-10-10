# Phase 2 spike: GraphQL Yoga + Pothos over the ClickHouse store

Decision being tested: `paryon_kubernetes/docs/chaingraph/plans/clickhouse-api-options.md` (A: Yoga + Pothos; spike
cases S1–S6, gate G2a pass criteria). Requirements: `clickhouse-api-requirements-inventory.md` (R1–R40, documents
L10/L12/L17/L24/L30/L33/L36). Local only (ch1-local ClickHouse 26.8.22.13, ch1-pg), 2026-10-10.

**Result.** Criteria 2 (snapshot contract) and 3 (subscriptions) pass. Criterion 1 passes against independent
computations on a local two-node dataset; the golden/Postgres parity part cannot run locally (see §3). Criterion 4
passes against "readSnapshot + the same SQL" (1.8–2.4 ms p50) but not against the bare SQL (7.9–9.2 ms p50), because
the one `readSnapshot` per request costs about 6.5 ms locally. Criterion 5 (Studio) was not attempted. The hard
part was not GraphQL: it was making one root field into one ClickHouse statement without re-running the root (§4.2).

## 1. What was built

| Piece | Where |
|---|---|
| TypeScript 4.9.4 → **5.9.3** (latest 5.x), `@types/node` 22, `module: nodenext`, `types: ["node"]`; the `Symbol.asyncDispose` shim in `client.ts` is gone | commit `build: TypeScript 5` |
| Pothos schema (S1–S6 roots only, v2 shape), `Hex` and `BigInt` scalars | `src/api/schema.ts`, SDL in [`phase2-spike.graphql`](phase2-spike.graphql) (`yarn api:schema`) |
| Typed filter → SQL compiler: every client value bound as `{pN:Type}`, every read through a pinned `*_at` view | `src/api/sql.ts` |
| Selection-driven statement builder: one root field = one statement; relationships prefetched by joins | `src/api/compile.ts` |
| Request snapshot: one `readSnapshot` per request, pinned for every resolver; query counters | `src/api/db.ts` (`ApiDb`, `PinnedSnapshot`), plugin in `src/api/server.ts` |
| Live-query hub: watermark poll, one snapshot + one data query per advance per topic, fan-out | `src/api/live.ts` |
| Yoga server (HTTP, SSE) + `graphql-ws` on the same path | `src/api/server.ts`, `src/api/main.ts` |
| Proofs | `src/api/api.spec.ts` (6 `[e2e]`), `src/api/sql.spec.ts` (3 unit), dataset + model in `src/api/spec-dataset.ts` |

New dependencies (all in the `.yarn` offline cache, submodule commit `a9d1c68` on `chaingraph-dependencies`
`clickhouse-store`; TS 5 was `bfd14a1`): `graphql` 16.14.2, `graphql-yoga` 5.24.4, `@pothos/core` 4.15.1,
`graphql-ws` 6.3.0, `ws` 8.22.0, `@types/ws`. How to add one: `local-dev.md` "Adding a package".

`skipLibCheck` is now on: Yoga's and `@whatwg-node`'s typings reference `URLPattern` and `DisposableStack`
(lib.dom / esnext), which this Node-only build does not load.

### 1.1 Request flow

1. Yoga parses and validates. The `useRequestSnapshot` plugin (`onExecute`) reads the root fields' `node:` arguments
   (literals, variables, fragments) and puts a `PinnedSnapshot` for that node in the context.
2. The first resolver that needs data calls `snapshot.get()`: node name → id (cached; node rows are few and
   permanent), then **one** `readSnapshot(client, nodeId)`. Every other root of the operation awaits the same promise.
   A node-agnostic-only operation (`{ transaction_watermark }`) snapshots node 0.
3. Each root resolver builds one statement with `new Sql(snapshot)`: the six snapshot parameters come from the
   snapshot, client values are bound as `p0, p1, …` (`Sql.bind`), views are `pinnedView(name)` from `visibility.ts`.
4. Nested fields read prefetched data only (`prefetched(parent, responseKey)`), so they issue no SQL.

Rules enforced by construction and tested (`sql.spec.ts`, S6): no schema argument or input field can carry a
snapshot value (`visible`, `visible0`, `tail`, `fence`, `void`, `node_internal_id`, `snapshot`, `watermark`,
`commit_seq`, `visible_seq`); a hostile value (`'); DROP TABLE output; --{visible:UInt64}`) never appears in SQL
text; every `{name:Type}` placeholder is either a snapshot parameter or a bound client value; a `Hex` that is not
even-length hex is rejected by the scalar.

Spike limit: all per-node roots of one operation must name the same node (one snapshot = one node). An operation
naming two nodes gets a clear error. Full Phase 2 can read a multi-node snapshot in the same single statement
(`visible(n)` for each named node; tail, void and fence are node-independent).

## 2. Schema shapes (v2, no Hasura compatibility)

Full SDL: [`phase2-spike.graphql`](phase2-spike.graphql). Operators keep Hasura's names (`_eq _neq _in _is_null _gt
_gte _lt _lte`) plus `_prefix` on bytes. Relationship filters are explicit EXISTS fields, so "same row" is a type
rule, not a convention.

```graphql
type Query {
  transactions(node: String!, where: TransactionFilter, order: Order = ASC,
               after: BigInt, before: BigInt, limit: Int!): TransactionPage!        # S1, S2 (L30), S6 (L33)
  inputs(node: String!, where: InputFilter, after: String, limit: Int!): InputPage!     # S2 (L17)
  unspent_outputs(node: String!, where: OutputFilter, after: String, limit: Int!): OutputPage!  # S5 (L12), S4 (L36)
  unspent_output_groups(node: String!, where: OutputFilter, group_by: [OutputGroupColumn!]!,
                        order_by: OutputGroupOrder, limit: Int!): [OutputGroup!]!    # S3
  transaction_watermark: BigInt                                                      # S6 (node-agnostic)
}
type Subscription {            # live queries over SSE and graphql-ws (S4)
  unspent_outputs(...same args...): OutputPage!
  transactions(...same args...): TransactionPage!
}
input TransactionFilter {
  _and: [TransactionFilter!] _or: [TransactionFilter!] _not: TransactionFilter
  internal_id: BigIntComparison  hash: HexComparison
  has_output: OutputFilter       # EXISTS one output matching all of it (same row)
  has_input: InputFilter         # EXISTS one input matching all of it (same row, incl. outpoint)
  block_window: BlockWindow      # WIN per node: { from_height, to_height, include_mempool = true }
}
input InputFilter  { _and _or _not  input_index: IntComparison  outpoint_transaction_hash: HexComparison
                     outpoint: OutputFilter }   # outpoint binds to the same input row (input carries it)
input OutputFilter { _and _or _not  output_index value_satoshis locking_bytecode token_category
                     fungible_token_amount nonfungible_token_capability nonfungible_token_commitment }
type TransactionPage { nodes: [Transaction!]!  has_more: Boolean!  end_cursor: String }   # same for Input/Output
type Transaction { hash internal_id … outputs(where, order, limit) inputs(where, order, limit)
                   block_inclusions { transaction_index block { hash height timestamp internal_id } } }
type Output { … transaction: Transaction! }   type Input { … outpoint: SpentOutput!  transaction: Transaction! }
type OutputGroup { key { locking_bytecode token_category } aggregate { count sum { fungible_token_amount value_satoshis } } }
```

Shape decisions worth noting:
- **ACC is implicit** in every per-node root (`node:`): `transactions` = tx accepted by the node (block it accepts or
  its mempool); `inputs` = inputs of such txs; `unspent_outputs` = v1's unspent(n, o). There is no way to forget it.
- **`block_inclusions` is per node** (blocks the root node accepts), and **WIN is per node** (`block_window`: a block
  the node accepts in the window, or the node's mempool). v1's WIN was "any block" / "no inclusion anywhere"; this is
  the semantic change the inventory (R3) says to release-note.
- **Keyset:** `transactions` uses `after`/`before` internal ids (L30's two-sided window; DESC pages continue with
  `before: end_cursor`). `inputs` and `unspent_outputs` use an opaque server-built cursor over
  `(transaction_internal_id, index)` (base64url of `id:index`), so clients no longer spell L17's `_or` composite.
  `has_more` is computed from `limit + 1` rows; per-root caps (R33): transactions 10,000, outputs 5,000, inputs 1,000,
  groups 1,000.
- **Integers:** indexes are `Int`, 64-bit values `BigInt` (decimal string). Exception found by the tests:
  `Input.outpoint_index` is `BigInt`, because a coinbase input's index is 4294967295 (not an Int32).
- **Capability** is an enum (`NONE MUTABLE MINTING`); `token_category` is null for no token (the store's zero
  category); commitment is null for non-NFT outputs and `""` for an empty commitment.
- Fields are non-null unless they can be null (Pothos `defaultFieldNullability: false`).

### 2.1 The documents in v2 form

L24 (S1), same-row conjunction and NOT EXISTS:
```graphql
transactions(node: $node, after: $after, limit: $limit, where: {
  has_input: { outpoint: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $loan } } }
  _and: [{ has_input: { input_index: { _eq: 4 }, outpoint: { locking_bytecode: { _in: $users } } } }]
  _not: { has_output: { locking_bytecode: { _eq: $loan } } }
}) { nodes { internal_id hash inputs(where: { input_index: { _eq: 4 } }) { outpoint { token_category } } } }
```
L10 (S1), OR of EXISTS and the per-node window:
```graphql
transactions(node: $node, limit: $limit, where: {
  has_input: { outpoint: { token_category: { _eq: $redeemer } } }
  block_window: { from_height: $from, to_height: $to }
  _or: [
    { has_output: { token_category: { _eq: $redeemer }, nonfungible_token_capability: { _eq: NONE }, nonfungible_token_commitment: { _in: $commitments } } }
    { has_input: { outpoint: { token_category: { _eq: $redeemer }, nonfungible_token_capability: { _eq: NONE }, nonfungible_token_commitment: { _in: $commitments } } } }
  ]
}) { … }
```
L17 (S2): `inputs(node:, after: $cursor, limit:, where: { outpoint: { token_category, nonfungible_token_capability: { _eq: NONE }, nonfungible_token_commitment: { _eq: "03" } } }) { has_more end_cursor nodes { transaction_internal_id input_index transaction { hash outputs(where: { output_index: { _in: [0, 1, 3] } }) {…} inputs(where: { input_index: { _in: [0, 3, 4, 5] } }) {…} block_inclusions {…} } } }`.
L30 (S2): `transactions(node:, after: $after, before: $before, order: $order, limit:, where: { has_input: { outpoint: {…loan…} } }) { … outputs(where: {…loan…}) inputs(where: { outpoint: {…loan…} }) }`.
L12 (S5): `unspent_outputs(node:, after:, limit:, where: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _is_null: true }, locking_bytecode: { _neq: "6a" } })`.
S3: `unspent_output_groups(node:, group_by: [LOCKING_BYTECODE], order_by: { by: SUM_FUNGIBLE_TOKEN_AMOUNT, direction: DESC }, limit: 1000, where: {…L12…}) { key { locking_bytecode } aggregate { count sum { fungible_token_amount value_satoshis } } }`.
L36 (S4): `subscription { unspent_outputs(node:, limit: 5000, where: { token_category, nonfungible_token_capability: { _eq: MUTABLE }, locking_bytecode: { _eq: $price } }) { nodes { output_index transaction_hash transaction { block_inclusions {…} outputs {…} inputs { outpoint {…} } } } } }`.
L33 (S6): `transactions(node:, where: { _or: [{ has_output: {…} }, { has_input: { outpoint: {…} } }] }, …) { … }  transactionWatermark: transaction_watermark`.

## 3. Results against the pass criteria

Dataset (`spec-dataset.ts`): a Paryon-shaped chain written through `ClickHouseStore` (spec-fixtures scratch
databases `ch1_api_*`, dropped on teardown): a genesis coinbase with loan, price, redeemer-sidecar, FT-holder and
28 other outputs; block 1 (loan closure, interest payment, redemption finalize, redeemer move); block 2 on node-one
only (finalize, price update, FT transfer); one mempool tx on node-one (new sidecar `c2`); node-two stops at block 1
with an empty mempool. "Model" = an in-memory evaluation of each document's predicate over that dataset (Chaingraph
v1's definitions, per node), independent of the API and of SQL.

| # | Criterion | Result | Evidence |
|---|---|---|---|
| 1 | Correctness | **Pass locally; golden parity not run (owner: local = chipnet only)** | S1: L24 for 4 node/user combinations and L10 for 5 node/commitment/window combinations equal the model, incl. nested `block_inclusions` and `outputs`. S2: L17 nested filters equal the model. S5: AllHolders equals the model for both nodes at page sizes 1, 2 and 1,000, and agrees with `checker.unspent` (the store checker's own query-time SQL) on every outpoint, with `utxo` empty (UTXO off). S3 equals both the model's aggregation and the client-side aggregation of S5. S6's watermark equals direct SQL at the same snapshot parameters. S2 walks L17 with page size 1 while 3 blocks are committed between pages: 10 rows in 10 pages, no duplicate, no skip, in key order, equal to the final model. L30 both directions and with bounds equals the model |
| 2 | Snapshot contract | **Pass** | One `readSnapshot` per HTTP request (S6: 1 for the two-root document; 30 concurrent requests during a block commit → exactly 30). Both roots' statements carry identical gate parameters. One snapshot per subscription re-run (S4). Schema check and hostile-input checks as in §1.1. S4 delivers only at strictly increasing watermarks (asserted) |
| 3 | Subscriptions | **Pass** | 100 subscribers (50 SSE, 50 `graphql-ws`) on L36: one topic; initial result = **1 data query**. Per watermark advance: **1 data query + 1 readSnapshot**, both shared by all 100 (asserted on three advances, one of them with an unchanged result → re-run, no push). Every subscriber gets the new result after that one advance, about 100–120 ms after the watermark is published (20 ms poll + ~70–80 ms statement + fan-out). A late subscriber gets the current result with no query. The poll is one extra query per tick for all nodes (`watermarks`, 20 ms in the test; it runs no data query while the watermark stands still) |
| 4 | Cost ≤ 5 ms p50 | **Pass vs readSnapshot + SQL; not vs bare SQL** | Table below |
| 5 | Studio check (optional) | Not attempted | — |

**Overhead** (`[e2e] API overhead …`, 200 sequential warm requests over HTTP on localhost after 20 warm-ups; the
"SQL" is the exact statement and parameters the API sent, replayed with the same client; local Docker, tiny dataset;
two runs):

| Document | API p50 | readSnapshot + SQL p50 | SQL alone p50 | API − (snapshot + SQL) | API − SQL | API p95 |
|---|---|---|---|---|---|---|
| S5 AllHolders | 25.6–27.6 ms | 23.4–25.1 ms | 17.4–18.4 ms | **2.2–2.4 ms** | 8.2–9.2 ms | 29–33 ms |
| S3 HolderBalances | 23.7–25.8 ms | 21.8–24.0 ms | 15.9–17.4 ms | **1.8–2.3 ms** | 7.9–8.8 ms | 25–31 ms |

The API's own work (HTTP, parse, validate, plugin, compile, decode, serialise) is about 2 ms. The rest of the gap to
bare SQL is the snapshot read (about 6.5 ms here), which any reader of the pinned views pays once per request; it is
store-side fixed cost (`wp6b-gate-cost.md`: 1.3–6.2 ms) and matters only for sub-20 ms documents. The absolute
numbers are not the G1 lab's (S5 v1-style was 3,666 ms warm on golden): 18 ms of "SQL alone" here is mostly the
fixed cost of four parameterised views on a near-empty database.

**Equality on real data.** S5's mainnet equality was already proven on golden on 2026-10-10 (v1-style query-time
unspent = `utxo_at` = the expected md5; `paryon_kubernetes` G1 report). Rows-equal parity of S1–S6 against Postgres
on golden is not possible locally (owner decision 2026-10-10: local = chipnet only); chipnet equality will come from
the chipnet lab (`chipnet-lab.md`), which can run these documents against its synced Postgres and ClickHouse once
the API points at its database.

## 4. What turned out hard

1. **One statement per root without re-running the root.** Relationships are `LEFT JOIN`s onto grouped subqueries
   restricted to the page's transaction hashes. ClickHouse expands a plain CTE at every use, so the root (with its
   ACC semi-join and anti-join) ran once per relationship: L36 took 160 ms where its root alone takes 17 ms.
   `WITH page AS MATERIALIZED (…)` (setting `enable_materialized_cte = 1`, sent with every data query) evaluates it
   once: 65 ms. **That setting is Experimental in ClickHouse 26.8.** The alternative is two statements per root
   (root, then the relationships with the page's hashes as an `Array` parameter): about 40 ms for L36 here, but 2
   queries per request and per live re-run. Phase 2 should measure both on golden and decide; the materialized CTE
   keeps criterion 3's "one query per advance" literal.
2. **Fixed cost per pinned view.** Each `*_at` reference costs about 0.5 ms to resolve plus array-gate analysis
   (`wp6b-gate-cost.md` §4). A relationship-heavy document references 15–25 views, so L36 costs ~65 ms on an almost
   empty database. Fixes are store-side (fewer view references per statement, e.g. one `tx_acceptance_at` CTE per
   statement; or the gate as a row policy).
3. **The snapshot read is most of the overhead** (≈ 6.5 ms locally per request). Options: fold the snapshot into the
   data statement, or coalesce concurrent requests onto one in-flight snapshot read (a few ms staler, still
   consistent). Not done.
4. **Multi-node operations.** One `readSnapshot` = one node. The spike rejects operations naming two nodes; a
   multi-node snapshot query is a small store change.
5. Smaller: a coinbase input's `outpoint_index` (4294967295) does not fit GraphQL `Int`; Yoga's typings need
   `skipLibCheck` (or DOM/esnext libs); TypeScript 5 requires `module: nodenext` with `moduleResolution: nodenext`.

The GraphQL layer itself was not hard: Pothos + Yoga gave typed inputs, SSE and `graphql-ws` with no custom
transport code; the live-query hub is ~300 lines.

## 5. What is left for full Phase 2

Covered by the spike (at least the parts the documents need): R1, R2, R3 (ACC, WIN per node), R4 (query-time), R9
(subset), R10, R11, R12, R13 (`_prefix` operator exists, not index-backed, untested), R15, R18 (nested `limit`),
R19 (nested args; aliases are supported by the compiler but not covered by a test), R21, R22 (`@include/@skip` are
honoured by the compiler), R23 (`yarn api:schema`), R25, R31 (live queries for two roots), R33 (caps + `has_more`),
R34.

Left:
- **Roots and relations:** R5 acceptance facts as data (`node_validations`, history timeline, `accepted_by`), R6
  per-node mempool root, R7 block roots (tip with previous hash, height range, by height), R8 node-agnostic lookups by
  hash and `_in` chunking above ~1,000 hashes (R14), R20's remaining edges (`spent_by`, node → blocks/mempool,
  block → transactions, authchains), deeper nesting than root → transaction → outputs/inputs/inclusions (the compiler
  rejects it today), R32 tip/watermark roots per node, R35 node metadata.
- **Filters and ordering:** index-backed `_prefix`/`_prefix_in` on `locking_bytecode_prefix` and commitments (R13,
  R40), ordering other than the keyset orders (R17: multi-key, through relationships, mempool `validated_at`), R16
  offset where still wanted.
- **Aggregates and computed data:** R24 count/sum/min/max/avg roots, R26 computed counts and values, R27 bytecode
  patterns, R28 `encoded_hex`/`header`, R29 authchains, R30 output search.
- **Operations:** R36 latency on golden for the 82 benchmark cases, R37 watermark-keyed response cache (the snapshot
  is the natural key), R38 `send_transaction`, complexity limits and persisted operations (Yoga plugins), Redis or
  equivalent if live queries run on more than one pod, the Studio/Apollo check (criterion 5), multi-node snapshots,
  the view-count and snapshot-cost work of §4, and a parity harness run on chipnet (and golden, when allowed).

## 6. How to run

```sh
yarn build
# the API against a store database (default cg), GraphiQL at the printed URL
CHAINGRAPH_CLICKHOUSE_URL=http://localhost:18123 CHAINGRAPH_CLICKHOUSE_DATABASE=cg yarn api:dev
#   CHAINGRAPH_API_HOST (default 127.0.0.1), CHAINGRAPH_API_PORT (default 4000)
yarn api:schema > schema.graphql       # SDL for typed clients

# proofs (scratch databases ch1_api_*, dropped on teardown; ~45 s)
CHAINGRAPH_E2E_CLICKHOUSE_URL=http://localhost:18123 npx ava --timeout=120s 'build/api/*.spec.js'
```

Subscriptions: SSE with `POST /graphql`, `Accept: text/event-stream`; WebSocket with the `graphql-transport-ws`
protocol (`graphql-ws` client) on `ws://…/graphql`.

## 7. Suites after the change (2026-10-10)

| Suite | Result |
|---|---|
| `yarn build`, eslint, prettier, cspell | clean |
| `yarn test:unit` | 117 passed, 1 todo (+3 `sql.spec.ts`) |
| ClickHouse store specs `build/store/clickhouse/*.spec.js` | 143/143 |
| API specs `build/api/*.spec.js` | 9/9 (6 `[e2e]` + 3 unit), two consecutive runs |
| e2e ClickHouse, UTXO on / off | 45/45 / 45/45 (47 `[postgres]` skipped) |
| e2e Postgres (ch1-pg) | 92/92 |
