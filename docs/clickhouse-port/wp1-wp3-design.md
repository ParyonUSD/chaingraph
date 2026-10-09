# Chaingraph ClickHouse port, Phase 1: WP1 (test ports) and WP3 (store abstraction) design

## Orchestrator decisions

1. **In-flight Postgres PRs #5, #8, #12 and #13 are not merged or reconciled into this branch.** They belong to the Postgres session. The Postgres backend stays at the `paryon` head e561d0c, and PR #13's COPY path is not included. Where this document mentions those PRs (sections E, F and G), this decision takes precedence: leave out the "reconcile in-flight PRs" step and its days.
2. **Only `origin/feat/ingestion-gate` (PR #4, scripts and docs) is merged into `experiment/clickhouse-store`, as the proof instrument.** The implementer does that merge as step 0 of WP1a.

---

Base for the port: branch `paryon` at e561d0c.

**The finding that changes the plan:** of the e2e suite's 102 `client.query` calls, only 20 are plain data reads. About 50 write fixtures straight into the tables, delete rows or control transactions, mostly to fire Postgres triggers. Those tests cannot be pointed at a checker as they are. They have to be re-expressed through a store API, because on ClickHouse the single-writer lease and the in-memory mempool graph make outside writes either invisible or not allowed.

---

## A. The e2e suite

**Files:** `src/e2e/e2e.spec.ts` (3,311 lines), `src/e2e/e2e.spec.mockchain.helper.ts` (mockchain, `generateMockDoubleSpend`, hal/chipnet tx fixtures), `src/e2e/e2e.spec.logging.helper.ts`. The suite runs through `yarn test:e2e` (`ava --match='*[e2e]*'`).

**Counts:**
- 92 tests:
  - 55 `test.serial`. Three of these are the `[sql] search_output*` tests at the end.
  - 37 concurrent `test(...)`: 32 `bytecodeFunction` / `bytecodeFunctionReturnsNull` macro uses and 5 `encode_*` tests. The plan says 36; it is 37.
- 102 `client.query` calls, plus 4 `competingClient.query`. "~102 SQL assertions" really means these 102 calls.
- 160 `t.*` assertions (71 in the serial section).

**What the 102 calls do:**

| Class | # | Where |
|---|---|---|
| Schema setup (migrations) | 1 | `test.before`, line 229 |
| Transaction control (BEGIN/ROLLBACK) | 8 | cascade, backfill and search fixtures |
| Raw fixture INSERTs (transaction/output/input/node_transaction/node_transaction_history/block/block_transaction/node_block) | 24 | cascade, expiry, confirmed-archive, backfill, data-carrier, search |
| Migration replay | 2 | backfill test runs `1778158619747_backfill_orphan_mempool_descendants/up.sql` twice |
| Cleanup DELETEs | 22 | concurrent-conflict, expiry, confirmed-archive |
| Fault injection / state reset | 2 | delete `block_transaction` row (incomplete-block repair); delete node2's `node_transaction` for halTx |
| Postgres catalog / planner | 6 | `pg_stat_activity` (`waitForTransactionSaveConflict`), `pg_indexes`, `pg_trigger`, `CREATE INDEX test_*`, `SET LOCAL enable_seqscan`, `EXPLAIN` |
| SQL-function reads | 17 | `encode_transaction` ×3, `block_header_encoded_hex`, `transaction_encoded_hex`, `block_encoded_hex` ×2, `block_*_value_satoshis`, `transaction_data_carrier_outputs`, `search_output*`, macro and `encode_*` |
| Plain data reads | 20 | listed below |

**The 20 plain reads, by what they ask:**
- **Transaction present by hash:** line 775. The pre-sync ignore test expects 0 rows.
- **All block hashes:** line 868 (`getAllKnownBlockHashes` parity).
- **Node id by name:** lines 918 and 2644. Line 2644 takes the first node by name for `/send-transaction`.
- **`node_transaction` for (tx, node, validated_at):** line 979, a count.
- **Mempool membership of a hash set, per node:** lines 1137 (node1/node2), 1668 (node1), 657 and 718 (helpers, node1, joined with history).
- **`node_transaction_history` rows per node with `replaced_at`:**
  - lines 1162 and 1691;
  - helpers 657/718: `historyRowCount` and `MIN(replaced_at)`.
- **Which nodes validated a tx:** line 1782.
- **`node_block` for a node at a height:** line 2030 (node3 at 3001).
- **`node_block` count and top for a node:** line 2373 (node4 = 3163 rows).
- **History rows for two txs with no node filter:** line 2212 (double-spend). It uses `SELECT *` and so reads `internal_id`.
- **Whole `node_transaction` with no node filter:** lines 2250 and 2272 (confirmed-by-block).
- **`block_transaction` count per block:** lines 626 and 2441.
- **`block_transaction` at an index:** line 2304.

**Assertions that read across nodes (they break the per-node rule):**
- line 2212, history without a node;
- lines 2250 and 2272, every node's mempool in one list;
- the gate's `confirmedButInMempoolCount`.

Each should be rewritten per node, for example `mempool('node1')`, and node2/node3 asserted separately. This makes them stricter.

**Postgres-only artefacts and what to do with each:**

| Artefact | Test | Action |
|---|---|---|
| `pg_stat_activity` row-lock wait; competing `BEGIN` + `INSERT transaction` | `records node validation after concurrent transaction insert conflict` | Keep as `[postgres]`-only. Add a neutral twin: two concurrent `store.saveMempoolTransaction` calls for the same tx on two nodes, then assert one transaction row and both validations. On ClickHouse this exercises the in-flight map (§3.3). |
| `pg_indexes`, `pg_trigger` (`tgenabled = 'O'`) | `creates expected indexes after initial sync` | Replace with `checker.schemaReport()`, compared against an expected constant for each backend. ClickHouse lists tables and projections, and must show no triggers. |
| Trigger side effects fired by raw INSERTs into `node_transaction_history` (cascade), `node_transaction` and `node_block` | cascade, expiry, confirmed-archive | Rewrite through the store API; see "Fixtures" below. Keep the raw versions as `[postgres]` until cutover. |
| Migration replay | `backfills existing orphan mempool descendants` | `[postgres]`-only. Turn its intent into a per-node invariant, `checker.orphanMempoolDescendants(node) === []`, and assert it at the end of every mempool scenario on both backends. |
| `SELECT *` including the `node_transaction_history.internal_id` sequence | double-spend | Read only the stable columns: hash, node, `validated_at`, `replaced_at`. |
| `removed_at DEFAULT timezone('UTC', now())`, which is the DB server clock | implicit | ClickHouse uses the agent clock. Compare with a tolerance (§5.2 already says "modulo timestamp source"). |
| `EXPLAIN` / `enable_seqscan` / `indexDefinitions` | `search functions use the 25-byte … index` | `[postgres]`-only. The ClickHouse equivalent (`EXPLAIN indexes=1` showing the projection) is deferred to Phase 2. |
| Bytea `ORDER BY hash` | mempool lists | Portable. `FixedString` sorts bytewise, as bytea does, and lower-case hex sorts the same way. |

**Setup and teardown today:** `test.before` runs `DROP DATABASE chaingraph_e2e_test WITH (FORCE)` and `CREATE DATABASE`, then applies 13 `up.sql` migrations. After that, one long scenario carries state from test to test: initial sync, re-orgs, a restart as node4, and so on. Fixture tests isolate themselves by:
- distinct hash bytes per test (c1, f1/f3/f4, d1–d6, e1–e3, f0);
- wrapping in `BEGIN`/`ROLLBACK`;
- or explicit DELETE cleanup.

The macro tests run concurrently on the same DB after the serial tests.

**Mapping to ClickHouse:**
- There are no transactions, and lightweight DELETE is not allowed on tables with projections, so ROLLBACK and DELETE cleanup go away.
- For the agent scenario, use one database per run, `cg_e2e_<pid>`: `DROP DATABASE … SYNC` then `CREATE DATABASE`, then the schema DDL, which takes about 100–300 ms.
- For in-process store tests, use a fresh database per test (cheap) or `TRUNCATE` every table. These tests must never share the agent's database, because the writer lease (§2.6) forbids a second writer.
- Leftover fixtures do not leak, provided fixtures use a dedicated fixture node (`node_fixture`) and assertions are per node. That isolation itself exercises checklist item 2.
- On Postgres, the same store tests use `CREATE DATABASE … TEMPLATE chaingraph_e2e_template` per test file.

**Fixtures:** rewrite cascade, expiry, confirmed-archive and the concurrent-conflict test as in-process store tests, in a new `src/e2e/store-mempool.spec.ts`. They seed through the store with synthetic `ChaingraphTransaction` and `ChaingraphBlock` objects; neither backend checks hashes. For example, the cascade test puts parent_a in node1's mempool, then saves a conflicting tx with `validatedAt` 00:10 for node1, and expects child_b and child_c archived for node1 only.

The expiry test currently relies on the agent's 100 ms scan picking up rows injected into the DB. On ClickHouse the agent's in-memory graph never sees injected rows. Split it into:
1. a store-level test that calls `store.getMempoolTransactionsExpiringBefore` and `archiveMempoolTransaction` with fixed timestamps;
2. an agent-level test that sets `CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_MS` to about 2 s and feeds txs over P2P.

**Proposed checker** (`src/store/checker.ts`; implemented by `src/store/postgres/checker.ts` and `src/store/clickhouse/checker.ts`). Every acceptance method takes a node *name*. The ClickHouse version reads **only through the gated views**.

```ts
interface StoreChecker {
  // nodes
  nodeInternalId(node: string): Promise<number | undefined>;
  nodeNamesOrdered(): Promise<{ name: string; internalId: number }[]>;

  // node-agnostic base facts (by hash)
  transactionExists(hash: string): Promise<boolean>;
  transactionRowCount(hash: string): Promise<number>; // duplicate detection
  encodedTransactionHex(hash: string): Promise<string | undefined>;
  // Postgres: encode_transaction(); ClickHouse: libauth encodeTransaction over fetched rows
  encodedBlockHex(by: { hash?: string; height?: number }): Promise<string | undefined>;
  encodedBlockHeaderHex(by: { hash?: string; height?: number }): Promise<string | undefined>;
  allBlockHashes(): Promise<string[]>; // sorted
  blockTransactionCount(blockHash: string): Promise<number>;
  blockTransactionAt(blockHash: string, index: number): Promise<string | undefined>;
  blockValueAggregates(blockHash: string): Promise<{ fee: bigint; generated: bigint; input: bigint; output: bigint }>;
  outputsOfTx(hash: string): Promise<Output[]>;
  inputsOfTx(hash: string): Promise<Input[]>;
  inputsSpending(outpointHash: string, index: number): Promise<{ txHash: string; inputIndex: number }[]>;

  // per-node acceptance (checklist items 1, 6)
  acceptedBlocks(node: string, filter?: { height?: number }): Promise<{ hash: string; height: number; acceptedAt: Date | null }[]>;
  acceptedBlockCount(node: string, hashes: string[]): Promise<number>;
  mempool(node: string): Promise<{ hash: string; validatedAt: Date }[]>;
  mempoolMembership(node: string, hashes: string[]): Promise<Set<string>>;
  validatingNodes(hash: string): Promise<string[]>; // node names, sorted
  transactionHistory(node: string, hashes?: string[]): Promise<{ hash: string; validatedAt: Date | null; replacedAt: Date | null }[]>;
  blockHistory(node: string): Promise<{ hash: string; acceptedAt: Date | null; removedAt: Date }[]>;
  txAccepted(node: string, hash: string): Promise<boolean>; // ACC predicate
  unspent(node: string, scope: { category?: string; lockingBytecode?: string }): Promise<Outpoint[]>; // UTXO parity

  // per-node invariants, asserted after every scenario
  confirmedButInMempool(node: string): Promise<string[]>;
  orphanMempoolDescendants(node: string): Promise<string[]>;

  // fault injection
  dropBlockTransactionLink(blockHash: string, index: number): Promise<void>;
  // Postgres: DELETE; ClickHouse: mark that block's commit incomplete in commit_log
  forgetNodeValidation(node: string, hash: string): Promise<void>; // ClickHouse: write a −1 row
  schemaReport(): Promise<{ indexes: string[]; triggers: Record<string, string> }>;
}
```

Also add a shared `eventually(fn, {timeoutMs: 3000})` helper. Every read that follows a stdout event, or follows one of today's fixed 100 ms / 1,000 ms `sleep`s, must poll through it, because of the ClickHouse 250 ms mempool batch and 100 ms watermark publish.

## B. `mempool-cleanup.spec.ts` (243 lines, 5 tests)

**What it tests:** `db.archiveMempoolTransactionsAcceptedByBlocks`, the repair sweep for mempool rows whose inclusion arrived after the `node_block` trigger had fired.

**How it drives the SQL:**
- `CHAINGRAPH_POSTGRES_MAX_CONNECTIONS=1`, with a `BEGIN`, `SET LOCAL search_path = pg_temp`, and cut-down TEMP tables (`node`, `transaction` 1..20 with hash = byte repeated, `output`, `input`, `node_transaction`, `node_transaction_history`, `block_transaction`, `node_block`).
- It extracts the cascade function from `1778151011521_cascade_invalidate_mempool_descendants/up.sql`, installs it as `pg_temp.trigger_node_transaction_history_insert`, and adds the `cascade_history` trigger.
- `afterEach` rolls back.

**What the five cases pin down:**
1. A confirmed parent is archived with `replaced_at NULL` and its descendants are *not* invalidated. Other nodes are untouched. A tx included in two accepted blocks still gives one history row. The second run is idempotent (`[]`).
2. A conflict with an accepted block's tx archives at `MIN(accepted_at)` over every block of that node that contains it. The cascade gives descendants the same `replaced_at`. Node 2 is untouched.
3. In a mixed batch, confirmation wins (`bool_or(replaced_at IS NULL)`). A conflict at 01-04 cascades through 5→6→7, while tx2 (child of the confirmed tx1) stays.
4. These are ignored:
   - confirmations seen only by another node;
   - the null-hash coinbase outpoint;
   - conflicts where neither side is in a block;
   - a different `outpoint_index` on the same hash.
5. An empty mempool gives `[]`.

**TS rewrite:** a pure planner in `src/store/mempool-graph.ts`, which the ClickHouse backend uses inside its commits:

```ts
type Outpoint = `${string}:${number}`;
interface NodeMempool { txs: Map<TxKey, { validatedAt: Date; spends: Outpoint[] }> }
interface AcceptedInclusion { tx: TxKey; spends: Outpoint[]; acceptedAt: Date | null }
export const planAcceptedBlockCleanup = (
  mempool: NodeMempool,             // one node: the per-node invariant by construction
  inclusions: AcceptedInclusion[],  // txs in blocks this node accepts
  spenderIndex: Map<Outpoint, TxKey[]>, // mempool graph: outpoint → spending txs
): { tx: TxKey; replacedAt: Date | null }[]
// 1. confirmed = mempool ∩ inclusions                    → replacedAt null
// 2. conflicts  = spenders of inclusion outpoints (skip zero hash), ≠ the including tx,
//                 in mempool, not confirmed               → MIN(acceptedAt)
// 3. cascade    = BFS over children of the conflicts only, MIN over parents
```

The same planner covers `trigger_node_block_insert`, where the inclusions are a newly accepted block. With one inclusion and `acceptedAt = validatedAt`, it also covers `trigger_node_transaction_insert` (the replacement case).

Put the five cases into a shared vectors file, `src/store/mempool-cleanup.vectors.ts`, as plain objects (inputs, node_transaction, block_transaction, node_block, expected archive, membership and history). Then:
- `src/store/mempool-graph.spec.ts` runs them as a unit test with no DB;
- `src/e2e/mempool-cleanup.spec.ts` loads the same vectors into the Postgres temp tables and runs the SQL.

That makes it a differential test.

**A trap to copy exactly or fix deliberately:** when `accepted_at` is NULL (blocks not accepted live, and header-accepted blocks), the trigger archives *conflicts* with `replaced_at NULL`, the same as confirmations. The sweep's `MIN(NULL)` does the same. Add a vector for it.

## C. The SQL-function macro tests (37)

| Tests | # | Recommendation |
|---|---|---|
| `parse_bytecode_pattern` | 10 | Pull the vectors out now (0.25 day). Defer the TS implementation and runner to Phase 2 (§2.5: `*_bytecode_pattern` moves to the API layer). |
| `parse_bytecode_pattern_with_pushdata_lengths` | 13 | Same. |
| `parse_bytecode_pattern_redeem` (4 values, 5 NULL) | 9 | Same. |
| `encode_uint16le`, `encode_uint32le`, `encode_int32le`, `encode_uint64le`, `encode_compact_uint` | 5 | **TS unit test now** against libauth's `numberToBinUint16LE`, `numberToBinUint32LE`, `numberToBinInt32LE`, `bigIntToBinUint64LE`, `bigIntToCompactUint`. These underpin the ClickHouse checker's `encodedTransactionHex`. |

Keep the Postgres macro runner as-is, tagged `[postgres]`, and feed it from the shared vectors.

Related serial tests that use SQL functions:
- The `encode_transaction` and `block_encoded_hex` group (7 assertions) moves to the checker **now**. Re-encoding byte-for-byte is the strongest check that the ClickHouse write path stored base facts exactly.
- `block_*_value_satoshis` also moves to the checker now. ClickHouse stores these at write time (§2.1).
- `transaction_data_carrier_outputs` and the 3 `search_output*` tests wait for Phase 2.

## D. The ingestion gate (`origin/feat/ingestion-gate`, PR #4)

**Files:** `scripts/ingestion-gate/run.mjs`, `lib/{agent,fixtures,mock-node,postgres,scenarios,heap-sampler}.mjs`, `thresholds.json`, `docs/ingestion-gate.md`.

**How it runs:**
- The real `bin/chaingraph.js` runs against mock P2P nodes.
- Each scenario gets a fresh DB with every `up.sql`, then waits for `initial sync is complete` and `enabled mempool tracking`.
- Agent settings are pinned (`BLOCK_BUFFER_TARGET_SIZE_MB=128`, `POSTGRES_MAX_CONNECTIONS=8`).

| Scenario | What it measures | Thresholds |
|---|---|---|
| `max-block` | One 31.8 MB block (100,001 tx): wall time, tx/s, WAL, peak heap; correctness = `block_transaction` count | ≤ 10 s, ≤ 340 MB WAL, ≤ 1.7 GB heap |
| `burst` | Three chained blocks of that size: drain time and WAL | ≤ 30 s, ≤ 1 GB |
| `reorg` | Two nodes, 100-block branch A, 40 mempool txs, switch to 101-block branch B | Converge ≤ 6 s; per-node checks (below) |
| `concurrent` | A mainnet-like and a chipnet-like sequential writer, alone versus together | Ratio ≥ 0.6 |
| `catch-up` | 10k small blocks via `inv` | ≥ 300 blocks/s |

The reorg per-node checks: B accepted and A not; 30 confirmed txs out of the mempool and in history; 10 still in the mempool; nothing confirmed but still in the mempool. "Below fork still accepted" is allow-listed as a known failure (the deep-reorg race, PR #7).

**How it talks to Postgres** (`lib/postgres.mjs`):
- `recreateDatabase` (DROP/CREATE plus migrations);
- `currentWalLsn` / `walBytesSince` (`pg_wal_lsn_diff`, cluster-wide on a private server);
- `countRows`, `acceptedBlockCount` (polled every 5–100 ms to stop the clock), `blockTransactionCount`;
- in `scenarios.mjs`: `acceptedChain`, `mempoolRowCount`, `historyNodeCount`, `confirmedButInMempoolCount`, the last of which reads across nodes.

**ClickHouse backend:** add `lib/clickhouse.mjs` with the same surface, a `--store pg|clickhouse` option, a `--ch-url` option (Cloud), and docker `clickhouse/clickhouse-server:26.8`. Do the correctness reads through the compiled checker (`build/main/store/clickhouse/checker.js`), so the gate and e2e share one implementation. `acceptedBlockCount` **must read through the visibility gate**; otherwise the clock stops before commit.

**What replaces WAL bytes** (take a snapshot or delta around the window; run `SYSTEM FLUSH LOGS` before reading the log tables; use `clusterAllReplicas('default', …)` on Cloud):
- **Bytes written:** `sum(size_in_bytes)` from `system.part_log` where `event_type = 'NewPart'` (compressed bytes inserted), plus the same for `'MergeParts'` (merge rewrite), reported separately as write amplification. Cross-check with the `system.events` deltas `InsertedBytes` and `InsertedRows`.
- **Parts created:** `count()` of `NewPart` per table. Also sample `max(count())` of active parts per table/partition from `system.parts` during the window, against `parts_to_delay_insert`.
- **Merge time:** `sum(duration_ms)` of `MergeParts` in `part_log`. Add the deltas of `MergedRows`, `MergedUncompressedBytes` and `MergesTimeMilliseconds`. Add a "quiesce" metric: time until `system.merges` is empty after the window.
- **Back-pressure:** the `DelayedInserts` and `RejectedInserts` deltas (both should be 0), and `system.asynchronous_insert_log` flush latency.
- **Server memory** (the Postgres "~3 GB per block" note): `max(memory_usage)` in `system.query_log` where `type = 'QueryFinish' AND query_kind = 'Insert'`.

Check the exact `system.events` names on 26.8 before relying on them.

**Thresholds:**
- Wall, drain, converge, ratio and catch-up keep the Postgres limits. G1 needs ClickHouse to be no worse than Postgres on the same host.
- The byte, part and merge limits go in `thresholds.clickhouse.json` (`maxBytesWritten`, `maxPartsCreated`, `maxMergeSeconds`), calibrated as the doc describes: about 1.5–2× the first three reference runs.

## E. Write path today

Only `src/agent.ts` imports `src/db.ts`: 18 names (lines 40–60).

| Entry point (`db.ts`) | Called from (`agent.ts`) | Tables touched, in order | Triggers that fire |
|---|---|---|---|
| `registerTrustedNodeWithDb` | node `nodeRegistered` (≈551) | UPSERT `node`; SELECT `block` ⋈ `node_block` for that node (restores the chain) | — |
| `saveTransactionForNodes` | `handleTransactionFromNode` | One DB transaction: `transaction` (ON CONFLICT DO NOTHING) → `output`, `input` (only if new) → SELECT id → `node_transaction` | `trigger_public_node_transaction_insert` → history (`replaced_at = validated_at`) → `trigger_public_node_transaction_history_insert` cascade (enabled after sync) |
| `recordNodeValidation` | `recordTransactionAnnouncementFromNode` | `node_transaction` (by hash lookup) | Same chain |
| `saveBlock` | `Agent.saveBlock` (≈1795) | One DB transaction: CTE `transaction` → `output` → `input`; then a CTE `block` (ON CONFLICT) → `block_transaction` → `node_block`; then a linked-count check, rolling back on mismatch | `trigger_public_node_block_insert` (enabled after sync) → `node_transaction` DELETE + history → cascade |
| `acceptBlocksViaHeaders` | `catchUpViaHeaders` (≈1449) | `node_block` INSERT…SELECT by hash; `accepted_at` NULL for blocks older than 2 h | `trigger_public_node_block_insert` |
| `removeStaleBlocksForNode` | `handleStaleBlocks` | DELETE `node_block` | `trigger_public_node_block_delete` (row-level, BEFORE) → `node_block_history` (`removed_at = now()`) |
| `archiveMempoolTransactionsAcceptedByBlocks` | `archiveAcceptedMempoolTransactions` (each expiry scan) | DELETE `node_transaction` + INSERT history | Cascade |
| `archiveMempoolTransaction` | `expireMempoolTransaction` | Same, for a single row | Cascade |
| `reenableMempoolCleaning` | after `buildIndexes` | `ALTER TABLE … ENABLE TRIGGER` on both statement triggers | — |
| `optionallyDisableSynchronousCommit` / `optionallyEnableSynchronousCommit`, `listExistingIndexes` / `createIndexes` / `getIndexCreationProgress` | sync start and end | DDL / settings | — |

The read paths are `getAllKnownBlockHashes`, `getIncompleteBlocks` and `getMempoolTransactionsExpiringBefore`. `pool` is used for the heartbeat stats (≈2128) and in `shutdown`.

**The four triggers** (`1616195337538_init/up.sql:243–322`; `1778151011521…/up.sql`):
1. **`trigger_node_transaction_insert`** (AFTER INSERT on `node_transaction`, per statement). It finds `input`s of the new rows, then other `input`s spending the same outpoint, then DELETEs that node's conflicting `node_transaction` rows and INSERTs them into history with `replaced_at = new.validated_at`. It is always enabled.
2. **`trigger_node_block_insert`** (AFTER INSERT on `node_block`). It takes the block's txs from `block_transaction`, their inputs, and every `input` on the same outpoints (skipping the zero hash). It deletes that node's mempool rows into history with `replaced_at = NULL` for the tx itself and `accepted_at` for a conflict. It is disabled until sync completes.
3. **`trigger_node_transaction_history_insert`** (AFTER INSERT on history, per statement). Guarded by the `chaingraph.suppress_mempool_descendant_cascade` setting, it runs a recursive CTE over `output` ⋈ `input` ⋈ `node_transaction`, same node only, takes `MIN(replaced_at)`, and deletes and archives the descendants. It only seeds from `replaced_at IS NOT NULL`. It is disabled until sync completes.
4. **`trigger_node_block_delete`** (BEFORE DELETE, per row). It INSERTs into `node_block_history` (`accepted_at`, and `removed_at` from the DB default).

**COPY encoder** (`origin/perf/agent-binary-copy`, PR #13; switch `CHAINGRAPH_WRITE_PATH=sql|copy`, default `sql`). This is reference only; see Orchestrator decision 1.
- **`src/components/pg-binary-copy.ts`:**
  - `BinaryCopyWriter`: `startRow(n)`, `null()`, `int8()`, `boolean()`, `hexBytea()`, `bytea()`, `text()`, `finish()`. `finish()` returns CopyData + CopyDone messages.
  - `CopyFromBuffersQuery` (a `pg.Submittable`) and `copyFromBuffers(client, CopyFromStdin[])`, where each entry is `{statement, messages}`. They pipeline all four COPYs in one round trip.
  - `decodeBinaryCopy` and `copyDataPayload` (tests).
- **`src/components/block-copy-rows.ts`:**
  - `stageTableColumns` for the four `pg_temp.chaingraph_stage_*` tables: transaction(hash, version, locktime, size_bytes, is_coinbase); input(transaction_hash, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode); output(… token columns, capability as text); block_transaction(hash, transaction_index). These are created `ON COMMIT DELETE ROWS`.
  - The encoders `encodeStageTransactions`, `encodeStageInputs`, `encodeStageOutputs` and `encodeStageBlockTransactions`.
- **`db.ts`:** `saveBlockViaCopy` stages rows, then runs the same CTE inserts and `verifyBlockTransactionsLinked`.

The RowBinary encoder should copy this shape: a `RowBinaryWriter` with a pre-sized buffer, typed column writers (`fixedString32`, `uint32`, `int64`, `nullable`, `string` with LEB128 length, `enum8`, `dateTime64`), and per-table encode functions from `ChaingraphTransaction` / `ChaingraphBlock`. It should stream through `@clickhouse/client` `exec()`. PR #13 stays a Postgres-only option inside the Postgres backend.

## F. The proposed `src/store/`

**Layout:**
- `src/store/types.ts` (interfaces)
- `src/store/index.ts` (`createStore()`)
- `src/store/checker.ts`
- `src/store/mempool-graph.ts` (pure planner, shared)
- `src/store/postgres/{postgres-store.ts, checker.ts}`, where `postgres-store.ts` is today's `db.ts` moved verbatim and `src/db.ts` becomes a re-export shim so in-flight PRs still merge
- `src/store/clickhouse/{client.ts, schema/*.sql, row-binary.ts, commit-log.ts, visibility.ts, id-allocator.ts, mempool-state.ts, utxo.ts, clickhouse-store.ts, checker.ts}`

**Backend selection:**
- `CHAINGRAPH_STORE=postgres|clickhouse` (default `postgres`), validated in `src/config.ts` the same way as `CHAINGRAPH_WRITE_PATH`.
- ClickHouse settings: `CHAINGRAPH_CLICKHOUSE_URL`, `CHAINGRAPH_CLICKHOUSE_DATABASE` (default `cg`), `CHAINGRAPH_CLICKHOUSE_USER`, `CHAINGRAPH_CLICKHOUSE_PASSWORD`.
- e2e uses `CHAINGRAPH_E2E_STORE` and passes the matching values to the spawned agent.

**`ChaingraphStore` interface.** Each method keeps today's signature, so the agent only changes `fn(...)` to `this.store.fn(...)`. The ClickHouse backend owns the mempool graph, so the agent stays backend-agnostic. ClickHouse §2 table names are used below.

| Method | Purpose and inputs | ClickHouse tables written | Checklist |
|---|---|---|---|
| `init()` / `close()` / `poolStats()` | ClickHouse: take the writer lease; recover `intent`s without `committed` and mark them `aborted`; rebuild the mempool graph | `writer_lease`, `commit_log` | 4 (crash), 5 |
| `prepareForInitialSync()` / `finishInitialSync(onIndexProgress)` | Wraps the synchronous-commit and index functions; ClickHouse: the bulk-horizon switch and projection materialisation | `commit_log` | — |
| `enableMempoolTracking(): {schemaIsCurrent}` | Replaces `reenableMempoolCleaning` | — | 4 |
| `registerNode(node): {internalId, syncedHeaderHashChain}` | Upsert the node; restore its chain | `node` | 1, 7 |
| `getAllKnownBlockHashes()` | Agent read, node-agnostic | — | (read; gated) |
| `saveBlock({block, nodeAcceptances, isSavedTransaction})` | Block plus acceptance by n nodes | `output`, `input`, `transaction`, `block`, `block_transaction`, `node_block` +1, `tx_acceptance` (mempool −1 / block +1), `node_transaction` −1 (confirms, conflicts and cascade), `node_transaction_history`, `utxo` / `utxo_by_script`, `pending_spend` | 1, 2, 4, 5, 6 |
| `saveMempoolTransaction(tx, validations[])` | Replaces `saveTransactionForNodes` | base facts, `node_transaction` +1, `tx_acceptance` +1, `utxo` ±, plus replacement and cascade rows | 1–6 |
| `recordNodeValidation(hash, {node, validatedAt})` | A known tx seen by another node | `node_transaction`, `tx_acceptance`, `utxo`, replacement | 1–6 |
| `acceptBlocksViaHeaders(node, blocks, acceptedAt)` | Header-sync acceptance | `node_block` +1, `tx_acceptance` (INSERT…SELECT), `node_transaction` confirms, `utxo` | 1, 2, 4 |
| `removeStaleBlocksForNode(node, staleHashes, removedAt?)` | Re-org release; `removedAt` is ignored by Postgres | `node_block` −1, `node_block_history`, `tx_acceptance` −1, `utxo` inverse | 1, 2, 4, 6 |
| `archiveMempoolTransactionsAcceptedByBlocks()` | Repair sweep (uses the planner) | `node_transaction` −1, history, `tx_acceptance`, `utxo` | 2, 4, 6 |
| `getMempoolTransactionsExpiringBefore(...)` / `archiveMempoolTransaction(...)` | Expiry | as above | 4, 6 |
| `getIncompleteBlocks(...)` | Repair scan; ClickHouse: `intent` commits for the scope plus the counts check | — | 4 |

Item 7 (the API names the node) is Phase 2. Every read method on the checker takes a node name already.

**Change to `saveBlock`'s signature:** replace `transactionCache: Agent['transactionCache']` with `isSavedTransaction: (hash) => boolean`. Today's `db.ts` imports a type from `agent.ts`; this removes that circular type dependency.

**Order of work, so the Postgres backend is a pure refactor:**
1. **Pre-step (superseded by the Orchestrator decisions).** The original recommendation was to merge or freeze the in-flight PRs that touch `db.ts` or `e2e.spec.ts` (#5, #8, #12, #13) and to merge #4 first. Instead, only #4 is merged, as step 0 of WP1a.
2. **WP1a.** Add `StoreChecker` and `PostgresChecker` (the existing SQL, moved). Rewrite the e2e reads to use it. Make the three cross-node assertions per node.
   - **Exit:** the same 92 tests, green on Postgres.
3. **WP1b.** Write the store-level mempool tests and vectors (B, C), plus the `[postgres]` tag for the legacy raw-SQL, catalog and migration tests.
   - **Exit:** green on Postgres.
4. **WP3a.** `git mv src/db.ts src/store/postgres/postgres-store.ts` with no SQL text changes, a `db.ts` shim, `createStore()`, and agent wiring. **Proof:**
   - e2e green;
   - the gate on the same host before and after. WAL bytes must match **exactly**, because they are deterministic (224.5 MB / 675 MB). Wall times must be within run-to-run noise.
   - optionally, a query-text capture through a `pg.Pool` `query` wrapper on one e2e run before and after, diffed as identical multisets;
   - `git diff -M` showing a rename with only import edits.
5. **WP1c.** Gate store adapter (D), Postgres first.
   - **Exit:** numbers equal to step 4.
6. **WP3b.** The ClickHouse skeleton: schema DDL, `RowBinaryWriter`, `ClickHouseChecker` over gated views, `CHAINGRAPH_STORE=clickhouse` wiring. The e2e then runs on ClickHouse and is expected red. That red list becomes the backlog for the write-path and mempool work.

## G. Sizing (eng-days) and risks

| Piece | Days |
|---|---|
| Reconcile in-flight PRs #5, #8, #12, #13 (dropped per Orchestrator decision 1) | 1–2 |
| WP1a: checker interface, Postgres checker, e2e reads ported | 3–4 |
| WP1b: store-level mempool tests, vectors, differential `mempool-cleanup` | 2–2.5 |
| WP1: macro vectors plus 5 encoder TS unit tests | 0.5 |
| WP1c: gate store adapter (Postgres, then ClickHouse metrics and lifecycle) | 2–3 |
| WP3a: `ChaingraphStore` interface, Postgres move, agent wiring, proof runs | 2–3 |
| WP3b: ClickHouse skeleton (client, DDL, RowBinary encoder with unit tests, ClickHouse checker over gated views) | 4–6 |
| **Total WP1 + WP3** | **14.5–21** (13.5–19 without the PR reconciliation) |

The test work alone (WP1a–c) is 7.5–10 days, against the plan's "tests 4–6". The difference is the trigger-fixture rewrites and the gate adapter.

**Recommended order:** merge #4 → WP1a → WP1b → WP3a (with proof) → WP1c → WP3b.

**Top 3 risks in porting the tests:**

1. **Losing trigger coverage while re-expressing the fixture tests.**
   - About 50 calls seed state with raw SQL and depend on triggers firing, or on the agent's expiry scan reading injected rows.
   - On ClickHouse, outside writes are forbidden by the lease and invisible to the in-memory graph, so these tests have to go through the store API. On Postgres that tests a different path from today.
   - **Mitigation:** keep the raw versions as `[postgres]` through dual-run. Run the differential vectors (B). Assert the `orphanMempoolDescendants` and `confirmedButInMempool` invariants per node after every scenario.
2. **Visibility timing, both false greens and flakes.**
   - Today's tests read tables right after a stdout line or a fixed `sleep(100|1000)`. ClickHouse adds the 250 ms mempool batch, the 100 ms watermark publish, async inserts, and collapsing rows that need `sum(sign) > 0`.
   - A checker reading base tables would pass while the API shows nothing. Fixed sleeps will flake.
   - **Mitigation:** the ClickHouse checker reads only the gated views; use `eventually()` everywhere; add a crash-injection test asserting that rows of an aborted commit are invisible.
3. **Assertions that are not per node and not portable.**
   - Three read every node at once (lines 2212, 2250, 2272, plus the gate's `confirmedButInMempoolCount`).
   - One reads the `internal_id` sequence through `SELECT *`. One takes `node1InternalId` as the first node by name.
   - `removed_at` comes from the DB clock, and the NULL-`accepted_at` conflict-as-confirmation quirk exists.
   - Any of these can hide a node A→B leak or produce false mismatches.
   - **Mitigation:** make every acceptance read take a node name; project only stable columns; compare timestamps with a tolerance; and either copy the NULL quirk deliberately with a vector or fix it in both backends.

## Sources read

- `ParyonUSD/chaingraph`, branch `paryon` (e561d0c): `src/e2e/e2e.spec.ts`, `src/e2e/mempool-cleanup.spec.ts`, `src/db.ts`, `src/agent.ts`, `src/config.ts`, `src/components/db-utils.ts`, `images/hasura/hasura-data/migrations/default/{1616195337538_init,1778151011521_cascade_invalidate_mempool_descendants,1778158619747_backfill_orphan_mempool_descendants}/up.sql`
- `origin/feat/ingestion-gate`: `docs/ingestion-gate.md`, `scripts/ingestion-gate/lib/{postgres,scenarios}.mjs`, `thresholds.json`
- `origin/perf/agent-binary-copy`: `src/components/{pg-binary-copy,block-copy-rows}.ts`, `src/db.ts`
- `paryon_kubernetes-consolidation/docs/chaingraph/plans/clickhouse-primary-store.md` (§1, §2, §3, §5.2, §8 Phase 1)
