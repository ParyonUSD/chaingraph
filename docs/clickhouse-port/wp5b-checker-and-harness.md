# WP5b: ClickHouse checker, e2e harness and ingestion-gate adapter

Code: `src/store/clickhouse/checker.ts` (+ `checker.spec.ts`), `src/store/clickhouse/ddl-apply.ts`,
`src/store/checker-factory.ts`, `src/e2e/e2e.spec.store.helper.ts`, `src/e2e/e2e.spec.ts` (harness only),
`scripts/ingestion-gate/{run.mjs,lib/clickhouse.mjs,lib/postgres.mjs,lib/scenarios.mjs,lib/agent.mjs,thresholds.clickhouse.json}`,
`docs/ingestion-gate.md` (section "ClickHouse backend").

## 1. The checker

`ClickHouseChecker implements StoreChecker` (`new ClickHouseChecker(client, database)` or
`createClickHouseChecker(client)`). The interface (`src/store/checker.ts`) is unchanged.

**Reads.** Each read method takes exactly one `readSnapshot` and passes `nodeViewParams` /
`agnosticViewParams` of that snapshot to every view it reads. Per-node methods take the snapshot of
that node; node-agnostic methods take the snapshot of node 0 (`visible(0)` plus the committed tail).
Base tables are never read, except by `schemaReport` (system tables) and the two fault-injection
methods. `validatingNodes` is the only method that reads several nodes: one snapshot per node,
each node's mempool read through its own pinned view.

Output conventions follow the Postgres checker: lower-case big-endian hex (`lower(hex(…))`), the
same orderings (hex order equals the bytewise `FixedString` order, as for `bytea`), absent token
fields omitted, timestamps read as `toUnixTimestamp64Milli(…)` and turned into `Date`s (UTC by
construction), `null` for missing `accepted_at` / `validated_at` / `replaced_at`, `undefined` /
`[]` / `0` / `false` for an unknown node.

| Method | Views (pinned) | Notes |
| --- | --- | --- |
| `nodeInternalId`, `nodeNamesOrdered` | `node_v` | the node registry is not gated (`node FINAL`) |
| `transactionExists`, `transactionRowCount` | `transaction_at` | |
| `encodedTransactionHex` | `transaction_at`, `input_at`, `output_at` | libauth `encodeTransaction` over the stored rows |
| `encodedBlockHeaderHex` | `block_at` | 80-byte header built from the row (hashes reversed) |
| `encodedBlockHex` | `block_at`, `block_transaction_at`, `transaction_at`, `input_at`, `output_at` | count = linked rows (as `encode_block`), transactions in `transaction_index` order |
| `allBlockHashes` | `block_at` | |
| `blockTransactionCount`, `blockTransactionAt` | `block_at`, `block_transaction_at` (+ `transaction_at` for `At`) | |
| `blockValueAggregates` | `block_at`, `block_transaction_at`, `transaction_at`, `input_at` | as the Postgres functions: input = spent value stored on `input` of non-coinbase txs; fee = input − non-coinbase output; generated = output − input |
| `outputsOfTx`, `inputsOfTx`, `inputsSpending` | `output_at`, `input_at` | |
| `acceptedBlocks`, `acceptedBlockCount` | `node_block_at` | sorted by height, hash |
| `mempool`, `mempoolMembership` | `node_transaction_at` | |
| `validatingNodes` | `node_v`, then `node_transaction_at` per node | one snapshot per node |
| `transactionHistory` | `node_transaction_history_at` ⋈ `transaction_at` | sorted by `validated_at`, `replaced_at` (NULLS LAST, as Postgres), hash |
| `blockHistory` | `node_block_history_at` ⋈ `block_at` | sorted by `removed_at`, hash |
| `txAccepted` | `tx_acceptance_at` | ACC: any live row (block 0 = mempool, or an accepting block of the node) |
| `unspent` (scope `lockingBytecode`) | `utxo_by_script_at` | `locking_bytecode_prefix = substring(lb, 1, 25) AND locking_bytecode = lb` (+ category) |
| `unspent` (scope `category` / none) | `utxo_at` | primary key `(node, token_category, …)` |
| `confirmedButInMempool` | `node_transaction_at`, `tx_acceptance_at` (`block_internal_id != 0`) | |
| `orphanMempoolDescendants` | `node_transaction_history_at` ⋈ `transaction_at`, `input_at` of the node's mempool | closure computed in TS over one snapshot's rows |
| `schemaReport` | `system.tables`, `system.projections`, `system.data_skipping_indices` | `indexes`: `table:<t>`, `projection:<t>.<p>`, `index:<t>.<i>`; `triggers`: materialized views (none) |

**`unspent` vs Postgres.** Postgres derives unspent from acceptance (outputs of accepted txs not
spent by accepted txs); ClickHouse reads the per-node UTXO set the agent maintains. They agree only
if the agent's state machine is right, which is exactly what the e2e and gate parity checks test.

### Fault injection

Both methods write base tables on purpose; neither is used outside tests.

- **`dropBlockTransactionLink(blockHash, index)`**: `ALTER TABLE block_transaction DELETE WHERE
  block_internal_id = <id of the block> AND transaction_index = index`, `mutations_sync = 2` (a
  classic mutation; lightweight DELETE is not allowed on tables with projections; the `p_tx`
  projection is rebuilt by the mutation). This is a lost row, as the Postgres `DELETE` is.
  *Not* "void" or "incomplete", as the plan sketched: a void hides every row of the block's commit
  (the block row, all its links, every node's `node_block` / `tx_acceptance`), so the block would
  vanish instead of showing one link fewer (`count − 1`, which the e2e asserts before the restart),
  and `commit_log` is `ReplacingMergeTree(state_rank)`, so an `incomplete` row (rank 2) can never
  override `committed` (rank 3). After the mutation the agent's incomplete-block scan sees a block
  whose visible links do not add up to `transaction_count`, as on Postgres, and repairs it under a
  new commit.
- **`forgetNodeValidation(node, hash)`**: finds the node's live `+1` rows for the transaction in
  `node_transaction` and in `tx_acceptance` (`block_internal_id = 0`) through the gate predicate
  (`gateSql.visibleAt` at the node's watermark, not void, not fenced; `sum(sign) > 0`), and inserts a
  `−1` row for each with the same version and **the same `commit_seq` as the `+1` it cancels**.
  It does not open "a tiny commit": only the writer-lease holder may allocate seqs (the running agent
  holds the lease; a checker seq in the agent's epoch would collide with a later agent seq and its
  dedup tokens, and a new epoch would fence the agent's future commits). The `+1`'s commit is
  committed and published, so the cancellation is visible immediately and the pair collapses on
  merge (same version). Tokens are `seq:table:forget-<node>-<version>-<hash>` (idempotent; never
  equal to the agent's `seq:table:N`). Caveats: the two inserts are separate (a reader between them
  can see the mempool and acceptance disagree), the commit's `row_counts` no longer match its rows
  (a row-count verifier will flag the seq: that is the fault), and `utxo` is not touched.

## 2. `checker.spec.ts` (`[e2e]`, CHAINGRAPH_E2E_CLICKHOUSE_URL)

Each test creates `ch1_wp5b_<random>` with `applyClickHouseDdl`, registers `nodeA` (1) and
`nodeB` (2), seeds rows through the real commit protocol (`CommitLog` + `VisibilityPublisher`, the
row encoders and test-support's `testSaveSteps`) and drops the database on teardown.

| Test | Checks |
| --- | --- |
| per-node mempool, acceptance and UTXOs never leak across nodes | nodeA's mempool/ACC/UTXOs (hal tx + chipnet CashTokens tx) never in nodeB's answers and vice versa; `unspent` by category (incl. a real token category) and by locking bytecode, combined scopes; `validatingNodes` |
| encodedTransactionHex round-trips libauth fixtures | hal tx and the chipnet CashTokens tx: decoded with libauth, inserted via the row encoders, re-encoded from rows = the fixture hex; `outputsOfTx` token fields, `inputsOfTx`, `inputsSpending` |
| rows of open, aborted and unpublished commits are invisible | open (never committed) and aborted commits invisible before and after the watermark passes them; committed-but-unpublished: node facts hidden, node-agnostic rows visible via the committed tail |
| blocks, encodings, aggregates and per-node acceptance | genesis block hex/header/aggregates; a 3-tx block hex; `acceptedBlocks` per node with UTC `acceptedAt`; `confirmedButInMempool`; `dropBlockTransactionLink` (count − 1); `schemaReport` |
| history, orphans and forgetNodeValidation per node | `transactionHistory` / `blockHistory` per node; orphan closure (child + grandchild) for the node that replaced the parent only; `forgetNodeValidation` removes only that node's validation, idempotent, no-op for unknown node/tx |

Result on local 26.8.22.13: 5/5 pass.

## 3. e2e harness

| Env var | Meaning |
| --- | --- |
| `CHAINGRAPH_E2E_STORE` | `postgres` (default) or `clickhouse` |
| `CHAINGRAPH_E2E_POSTGRES_HOST`, `_PORT` | Postgres path (unchanged) |
| `CHAINGRAPH_E2E_CLICKHOUSE_URL` | required for `clickhouse` (e.g. `http://localhost:18123`) |
| `CHAINGRAPH_E2E_CLICKHOUSE_USER`, `_PASSWORD` | optional |

ClickHouse path (`src/e2e/e2e.spec.store.helper.ts`):

- `test.before`: drop `cg_e2e_<pid>` databases of dead runs (a run that dies on an uncaught
  exception never reaches `after`), recreate `cg_e2e_<pid>` and apply the DDL with
  `applyClickHouseDdl` (TypeScript, same parsing as `ddl/apply.sh`; 57 statements today), build the
  checker with `createChecker({ backend: 'clickhouse', client })`.
- The agent is spawned with `CHAINGRAPH_STORE=clickhouse`, `CHAINGRAPH_CLICKHOUSE_URL/_DATABASE/_USER/_PASSWORD`
  (and the Postgres connection string, which `src/config.ts` still requires).
- `[postgres]`-tagged tests are registered with `postgresTest.serial` / `postgresTest.concurrent`,
  which are `test.serial.skip` / `test.skip` on ClickHouse (47 tests: 10 serial, 5 `encode_*`, 32
  macro uses). Equivalent CLI filter: `--match '*[e2e]*' --match '!*[postgres]*'`.
- `getAllKnownBlockHashes returns hex hashes…` calls the store's own `getAllKnownBlockHashes`: the
  `../db.js` shim on Postgres, a never-`init()`ed `createStore({ backend: 'clickhouse' })` on ClickHouse.
- `test.after.always`: kill any agent still running, close the client, drop `cg_e2e_<pid>`.
- Fixed sleeps: the double-spend test's two 1 s sleeps now wait (via `eventually`) for each
  transaction to reach node1's mempool. The remaining `sleep(1000)` in "ignores inbound transactions
  before initial sync is complete" is a negative check (the tx must stay absent) and stays.
- Timing (WP6b, `wp6b-e2e-stability.md`). Waits scale with the backend:

  | Wait | Postgres | ClickHouse |
  |---|---|---|
  | one log line | 10 s | 30 s |
  | multi-block feeds, shutdown drain | 10 s | 60 s |
  | initial sync / catch-up | 60 s | 120 s |
  | read after a save | 3 s | 10 s |
  | sync-scale read | 10 s | 60 s |

  On ClickHouse each agent-driving test gets a 180 s AVA timeout. A `waitForStdout` timeout is a
  test failure, not an uncaught exception. Every read after a log line or agent event goes through
  `eventually` / `eventuallyEqual`. The negative check asserts absence across two reads 1 s apart
  (`readTwice`). Shutdown is asserted by exit code 0. Startup also drops `cg_e2e_*` databases older
  than 1 h.

  Run time: Postgres 6–7 s. ClickHouse 23–24 s from `5c5413f` (multi-block commits); it was 57–66 s
  before, when initial sync alone took 22–29 s.

Run:

```sh
yarn build
# Postgres (default)
CHAINGRAPH_E2E_POSTGRES_HOST=localhost CHAINGRAPH_E2E_POSTGRES_PORT=15432 \
  npx ava --match='*[e2e]*' --timeout=60s build/e2e/e2e.spec.js
# ClickHouse
CHAINGRAPH_E2E_STORE=clickhouse CHAINGRAPH_E2E_CLICKHOUSE_URL=http://localhost:18123 \
  npx ava --match='*[e2e]*' --timeout=60s build/e2e/e2e.spec.js
# checker spec only
CHAINGRAPH_E2E_CLICKHOUSE_URL=http://localhost:18123 npx ava build/store/clickhouse/checker.spec.js
```

Results (2026-10-09): Postgres 92/92 (before and after the harness change). ClickHouse: the database
is created with the DDL, 47 `[postgres]` tests are skipped, the agent starts (`[e2e] spawn
chaingraph` passes) and exits with `Error: CHAINGRAPH_STORE=clickhouse is not implemented yet (WP5)`
from `createStore`, so `[e2e] api /health-check is alive` fails with `ECONNREFUSED` and the run
stops at the first `waitForStdout` timeout. That is the expected state until the WP5 store lands.

## 4. Ingestion-gate adapter

See `docs/ingestion-gate.md`, section "ClickHouse backend". `lib/postgres.mjs` and
`lib/clickhouse.mjs` now share one surface (`recreateDatabase`, `dropDatabase`, `openSession`,
`closeSession`, `agentEnvironment`, `writeMetricsStart` / `writeMetricsSince`, `countRows`,
`acceptedBlockCount`, `blockTransactionCount`, `acceptedChain`, `nodeBlockCount`,
`mempoolRowCount`, `historyNodeCount`, `confirmedButInMempoolCount`); `scenarios.mjs` uses
`context.backend` only. The Postgres gate still passes (`--quick --scenarios catch-up,reorg`:
905.8 blocks/s, converge 1.45 s, PASS / PASS*). Metric queries were verified on local 26.8 with a
throwaway database; no ClickHouse scenario can run until the store exists.

## 5. Blocked on the WP5 store

- Every ClickHouse e2e test after `spawn chaingraph`, and every `--store clickhouse` gate scenario.
- Whether `createStore({ backend: 'clickhouse' }).getAllKnownBlockHashes()` works without `init()`
  (the harness never calls `init()` on that instance, so it cannot take the writer lease).
- `forgetNodeValidation` assumes the store writes mempool membership to both `node_transaction` and
  `tx_acceptance` (block 0), as test-support and the views document.
- `dropBlockTransactionLink` assumes the store's incomplete-block scan compares visible
  `block_transaction_at` links with `block.transaction_count`.
