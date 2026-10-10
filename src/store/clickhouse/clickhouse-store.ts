/* eslint-disable max-classes-per-file, camelcase, @typescript-eslint/naming-convention, functional/no-mixed-type, @typescript-eslint/no-magic-numbers, complexity, max-lines, functional/no-try-statement, functional/no-throw-statement, functional/no-loop-statement, no-await-in-loop, @typescript-eslint/member-ordering, max-params, @typescript-eslint/parameter-properties, functional/no-let, @typescript-eslint/init-declarations, prefer-destructuring, @typescript-eslint/no-invalid-void-type */
// cspell:ignore clickhouse dedup unhex seqs milli varint
/**
 * The ClickHouse `ChaingraphStore` (WP5a-core): nodes, blocks, header
 * acceptance, re-org release, the per-node UTXO set and incomplete-block
 * repair (WP5a-core), and the per-node mempools (WP5a-mempool:
 * mempool-commit.ts, docs/clickhouse-port/wp5a-mempool.md).
 *
 * Every per-node fact carries the node in its key; a call for node A never
 * writes a fact for node B; every call that changes facts is one commit
 * (WP4), so a reader of node n sees all of its facts for n or none.
 * Design, invariants and the plan §1 checklist review:
 * docs/clickhouse-port/wp5a-core.md.
 */
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';
import type {
  ArchivedMempoolTransaction,
  ChaingraphStore,
  ExpiringMempoolTransaction,
  FinishInitialSyncHooks,
  IncompleteBlock,
  IncompleteBlockScan,
  NodeAcceptance,
  NodeValidation,
  StorePoolStats,
} from '../types.js';

import type { FaultInjector, SaveBlockResult } from './block-commit.js';
import {
  appendAll,
  BlockCommitter,
  chunked,
  freshen,
  minMax,
  SimulatedCrash,
  TransactionRegistry,
} from './block-commit.js';
import type { ClickHouseConnectionConfig } from './client.js';
import { ClickHouseClient } from './client.js';
import type { OpenCommit } from './commit-log.js';
import { CommitLog } from './commit-log.js';
import { ClickHouseReservationStore, IdAllocator } from './id-allocator.js';
import { changeRows, MempoolCommitter } from './mempool-commit.js';
import type { NodeMempoolChange } from './mempool-state.js';
import { isEmptyChange, MempoolState } from './mempool-state.js';
import type {
  NodeBlockHistoryRow,
  NodeBlockRow,
  OperationKind,
  StoreMode,
  StoreOperation,
} from './node-state.js';
import {
  AbandonedError,
  AbandonSignal,
  acceptanceColumns,
  awaitDependencies,
  deferred,
  encodeNodeBlockHistoryRows,
  encodeNodeBlockRows,
  InFlightLimiter,
  NodeRegistry,
  OperationRegistry,
  SaveSlot,
  waitForPredecessorRows,
} from './node-state.js';
import { RowBinaryWriter } from './row-binary.js';
import {
  committedSql,
  heightBatches,
  horizonDeltasSql,
  OutputRegistry,
  utxoDeltaInsertSql,
  validCommitSql,
} from './utxo.js';
import type { PublisherOptions } from './visibility.js';
import { VisibilityPublisher } from './visibility.js';
import type { WriterLeaseOptions } from './writer-lease.js';
import { WriterLease } from './writer-lease.js';

export interface ClickHouseStoreOptions {
  connection: ClickHouseConnectionConfig;
  lease?: WriterLeaseOptions;
  publisher?: PublisherOptions;
  /** Hashes/outpoints per lookup query (query parameters travel in the URL). */
  lookupChunkSize?: number;
  /** How long a child block waits for the outputs it spends (WP4 §3: bounded). */
  pendingSpendTimeoutMs?: number;
  /** Heights per statement of the horizon UTXO build. */
  horizonBatchHeights?: number;
  recentOutputCapacity?: number;
  recentTransactionCapacity?: number;
  /** How long a mempool tx spending an unknown output waits in the orphan pool. */
  orphanGraceMs?: number;
  /** Orphan pool bound (the oldest is released early when full). */
  maxOrphans?: number;
  /**
   * In-flight cap: at most this many `saveBlock` / `acceptBlocksViaHeaders`
   * calls work at once (`CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES`). 0 or
   * undefined: unbounded (no slots, the WP5a behavior). A call gives its
   * slot up while it waits on another call (wp5c-hardening.md §1).
   */
  maxInFlightSaves?: number;
  /** Test hook: called between the steps of every commit. */
  fault?: FaultInjector;
  /** Background errors (watermark publishing, lease loss). */
  onError?: (error: unknown) => void;
}

/*
 * Hex hashes per array query parameter: 1,000 × ~67 bytes stays under the
 * server's `http_max_field_value_size` (128 KiB); 2,000 did not
 * ("HTML Form Exception: Field value too long" on a 2,000-block header
 * acceptance).
 */
const defaultLookupChunkSize = 1_000;
const defaultPendingSpendTimeoutMs = 60_000;
const defaultHorizonBatchHeights = 10_000;
const defaultOrphanGraceMs = 1_000;
const closeDrainTimeoutMs = 5_000;
const defaultMaxOrphans = 10_000;
const twoHoursSeconds = 7_200;
const msPerSecond = 1_000;
const headerVarintThresholds = [252, 65_535, 4_294_967_295];

export class StoreClosedError extends Error {}

/** Postgres's `blockArrayToHashChain`: hashes at their heights, `null` gaps. */
export const hashChainFromBlocks = (
  blocks: readonly { hash: string; height: number }[]
): (string | null)[] => {
  if (blocks.length === 0) {
    return [];
  }
  const best = blocks.reduce(
    (highest, block) => Math.max(highest, block.height),
    0
  );
  const chain = Array.from({ length: best + 1 }, (): string | null => null);
  blocks.forEach((block) => {
    chain[block.height] = block.hash;
  });
  return chain;
};

/** Postgres getIncompleteBlocks' linked size: header + varint(count) + tx sizes. */
export const linkedBlockSize = (
  transactionCount: number,
  transactionBytes: number
) => {
  const [oneByte, threeBytes, fiveBytes] = headerVarintThresholds as [
    number,
    number,
    number
  ];
  const varint =
    transactionCount <= oneByte
      ? 1
      : transactionCount <= threeBytes
      ? 3
      : transactionCount <= fiveBytes
      ? 5
      : 9;
  return 80 + varint + transactionBytes;
};

const nodeColumns = [
  'internal_id',
  'name',
  'protocol_version',
  'user_agent',
  'first_connected_at',
  'latest_connection_began_at',
  'updated_at',
  'commit_seq',
];

export class ClickHouseStore implements ChaingraphStore {
  private client: ClickHouseClient | undefined;

  private lease: WriterLease | undefined;

  private commitLogInstance: CommitLog | undefined;

  private ids: IdAllocator | undefined;

  private publisher: VisibilityPublisher | undefined;

  private fenceArray: bigint[] = [];

  private mode: StoreMode = 'tip';

  private bulkStartSeq = 0n;

  private exclusive: Promise<void> | undefined;

  private fatal: unknown;

  private closed = false;

  private lastNodeUpdateMs = 0;

  readonly nodes = new NodeRegistry();

  readonly operations = new OperationRegistry();

  readonly mempool = new MempoolState();

  /** Shutdown: abandon in-flight work (`abandonInFlightWork`). */
  private readonly abandonSignal = new AbandonSignal();

  readonly outputs: OutputRegistry<StoreOperation>;

  readonly transactions: TransactionRegistry;

  private committerInstance: BlockCommitter | undefined;

  private mempoolCommitter: MempoolCommitter | undefined;

  private readonly lookupChunkSize: number;

  /** The in-flight cap; `undefined` = unbounded. */
  private readonly slots: InFlightLimiter | undefined;

  constructor(private readonly options: ClickHouseStoreOptions) {
    this.lookupChunkSize = options.lookupChunkSize ?? defaultLookupChunkSize;
    const cap = options.maxInFlightSaves ?? 0;
    if (!Number.isInteger(cap) || cap < 0) {
      throw new RangeError(
        `maxInFlightSaves must be an integer >= 0 (got ${cap}).`
      );
    }
    this.slots = cap === 0 ? undefined : new InFlightLimiter(cap);
    this.outputs = new OutputRegistry<StoreOperation>(
      options.recentOutputCapacity
    );
    this.transactions = new TransactionRegistry(
      options.recentTransactionCapacity
    );
  }

  /* ------------------------------------------------------------------ */
  /* lifecycle                                                           */
  /* ------------------------------------------------------------------ */

  /** The commit log (tests and the checker read it). */
  get commitLog(): CommitLog {
    if (this.commitLogInstance === undefined) {
      throw new StoreClosedError('ClickHouseStore.init() has not run.');
    }
    return this.commitLogInstance;
  }

  get storeMode(): StoreMode {
    return this.mode;
  }

  async init(): Promise<void> {
    const client = new ClickHouseClient(this.options.connection);
    this.client = client;
    const lease = new WriterLease(client, this.options.lease);
    await lease.acquire();
    this.lease = lease;
    lease.startHeartbeat((error) => {
      this.fatal = error;
      this.options.onError?.(error);
    });
    const commitLog = new CommitLog(client, lease);
    await commitLog.init();
    this.commitLogInstance = commitLog;
    await this.loadFence();
    this.ids = new IdAllocator(
      new ClickHouseReservationStore(client, () => lease.epoch),
      {
        assertHeld: () => {
          lease.assertHeld();
        },
      }
    );
    const publisher = new VisibilityPublisher(client, commitLog, {
      ...this.options.publisher,
      onError: (error) => {
        this.options.publisher?.onError?.(error);
        this.options.onError?.(error);
      },
    });
    await publisher.init();
    publisher.start();
    await publisher.publishWatermark();
    this.publisher = publisher;
    const { ids } = this;
    this.committerInstance = new BlockCommitter({
      abandon: this.abandonSignal,
      client,
      commitLog,
      fault: async (step, context) => this.fault(step, context),
      fence: () => this.fenceArray,
      ids,
      lookupChunkSize: this.lookupChunkSize,
      mempool: this.mempool,
      mempoolHooks: () => this.mempoolCommitter,
      mode: () => this.mode,
      onCommitted: () => undefined,
      operationOfSeq: (seq) => this.operationOfSeq(seq),
      operations: this.operations,
      outputs: this.outputs,
      pendingSpendTimeoutMs:
        this.options.pendingSpendTimeoutMs ?? defaultPendingSpendTimeoutMs,
      savesQueued: () => this.slots?.waiting ?? 0,
      transactions: this.transactions,
    });
    this.mempoolCommitter = new MempoolCommitter({
      abandon: this.abandonSignal,
      beginOperation: async (kind, operationNodes) =>
        this.beginOperation(kind, operationNodes),
      client,
      commitLog,
      endOperation: (operation) => {
        this.operations.end(operation);
      },
      fault: async (step, context) => this.fault(step, context),
      fence: () => this.fenceArray,
      ids,
      lookupChunkSize: this.lookupChunkSize,
      maxOrphans: this.options.maxOrphans ?? defaultMaxOrphans,
      mempool: this.mempool,
      mode: () => this.mode,
      nodes: this.nodes,
      onCommitted: () => undefined,
      operationOfSeq: (seq) => this.operationOfSeq(seq),
      operations: this.operations,
      orphanGraceMs: this.options.orphanGraceMs ?? defaultOrphanGraceMs,
      outputs: this.outputs,
      pendingSpendTimeoutMs:
        this.options.pendingSpendTimeoutMs ?? defaultPendingSpendTimeoutMs,
      transactions: this.transactions,
    });
    const nodes = await client.query<{ internal_id: number; name: string }>(
      'SELECT internal_id, name FROM node FINAL'
    );
    nodes.forEach((node) => {
      this.nodes.set(node.name, Number(node.internal_id));
      publisher.registerNode(Number(node.internal_id));
    });
    const horizon = await client.query<{ commit_seq: string; kind: string }>(
      `SELECT commit_seq, kind FROM (
         SELECT * FROM commit_log FINAL WHERE kind IN ('horizon_switch', 'utxo_build'))
       WHERE state = 'committed' ORDER BY commit_seq DESC LIMIT 1`
    );
    const last = horizon[0];
    if (last?.kind === 'horizon_switch') {
      this.mode = 'bulk';
      this.bulkStartSeq = BigInt(last.commit_seq);
    }
    await this.mempoolCommitter.rebuild();
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.mempoolCommitter?.dropOrphans(
      new StoreClosedError('The ClickHouse store is closed.')
    );
    if (this.operations.activeCount > 0) {
      // background work (parked child blocks): abandon it and let it abort
      this.abandonSignal.abandon('store closed');
      await Promise.race([
        this.operations.drain(),
        new Promise((resolve) => {
          setTimeout(resolve, closeDrainTimeoutMs);
        }),
      ]);
    }
    this.publisher?.stop();
    await this.publisher?.publishWatermark().catch(() => undefined);
    this.lease?.stopHeartbeat();
    await this.lease?.release().catch(() => undefined);
    await this.client?.close();
  }

  /**
   * Test hook: stop as if the process died (no abort, no lease release, no
   * final publish). Recovery happens in the next store's `init()`.
   */
  async simulateCrash(): Promise<void> {
    this.closed = true;
    this.fatal = new SimulatedCrash('crashed');
    this.publisher?.stop();
    this.lease?.stopHeartbeat();
    await this.client?.close();
  }

  /**
   * Shutdown: stop waiting and abandon every operation that has not
   * committed yet (pending-spend waits, dependency waits, later steps). An
   * abandoned block save is aborted and resolves as handled (so the agent's
   * block buffer drains); abandoned mempool saves reject; orphans are
   * dropped. Nothing abandoned is committed, so the next start restores the
   * chain without it and the agent downloads it again. Wired to SIGINT /
   * SIGTERM by `createStore` (the agent's shutdown drains the block buffer
   * before it closes the store, and a block waiting for a parent that will
   * never be downloaded would otherwise hold the drain for
   * `pendingSpendTimeoutMs`).
   */
  abandonInFlightWork(reason = 'store shutdown') {
    this.abandonSignal.abandon(reason);
    this.mempoolCommitter?.dropOrphans(
      new StoreClosedError(`Orphan dropped: ${reason}.`)
    );
  }

  /** Publish watermarks now (tests; the publisher also runs on its own). */
  async publishWatermarks(): Promise<Map<number, bigint>> {
    return this.requirePublisher().publishWatermark();
  }

  /**
   * Heartbeat stats. With the in-flight cap: `active` = saves / header
   * acceptances holding a slot, `max` = the cap, `waitingRequests` = calls
   * queued for a slot, `total` = every live store operation (including
   * parked children and calls waiting on other calls). Unbounded (cap 0):
   * live operations, `max` 0, as in WP5a.
   */
  poolStats(): StorePoolStats {
    const live = this.operations.activeCount;
    if (this.slots === undefined) {
      return {
        clients: { active: live, max: 0, total: live },
        waitingRequests: 0,
      };
    }
    return {
      clients: { active: this.slots.active, max: this.slots.max, total: live },
      waitingRequests: this.slots.waiting,
    };
  }

  /* ------------------------------------------------------------------ */
  /* nodes                                                               */
  /* ------------------------------------------------------------------ */

  async registerNode(node: {
    latestConnectionBeganAt: Date;
    nodeName: string;
    protocolVersion: number;
    userAgent: string;
  }): Promise<{
    internalId: number;
    syncedHeaderHashChain: (string | null)[];
  }> {
    this.assertOpen();
    const client = this.requireClient();
    const existing = await client.query<{
      internal_id: number;
      first_ms: string;
    }>(
      `SELECT internal_id, toUnixTimestamp64Milli(first_connected_at) AS first_ms
       FROM node FINAL WHERE name = {name:String} ORDER BY internal_id LIMIT 1`,
      { name: node.nodeName }
    );
    const found = existing[0];
    const internalId =
      found === undefined
        ? Number(await this.requireIds().allocateOne('node'))
        : Number(found.internal_id);
    const firstConnectedMs =
      found === undefined
        ? node.latestConnectionBeganAt.getTime()
        : Number(found.first_ms);
    const updatedMs = Math.max(Date.now(), this.lastNodeUpdateMs + 1);
    this.lastNodeUpdateMs = updatedMs;
    const row = new RowBinaryWriter()
      .uint32(internalId)
      .string(node.nodeName)
      .int32(node.protocolVersion)
      .string(node.userAgent)
      .dateTime64(firstConnectedMs)
      .dateTime64(node.latestConnectionBeganAt)
      .dateTime64(updatedMs)
      .uint64(0)
      .endRow()
      .finish();
    await client.insertRowBinary('node', nodeColumns, row, {
      deduplicationToken: `node:${internalId}:${updatedMs}`,
    });
    this.nodes.set(node.nodeName, internalId);
    this.requirePublisher().registerNode(internalId);
    const accepted = await client.query<{ hash: string; block_height: number }>(
      `SELECT lower(hex(any(block_hash))) AS hash, any(height) AS block_height
       FROM node_block
       WHERE node_internal_id = {node:UInt32} AND ${committedSql()}
       GROUP BY block_internal_id
       HAVING sum(sign) > 0`,
      { fence: this.fenceArray, node: internalId, open: this.openSeqs() }
    );
    return {
      internalId,
      syncedHeaderHashChain: hashChainFromBlocks(
        accepted.map((block) => ({
          hash: block.hash,
          height: Number(block.block_height),
        }))
      ),
    };
  }

  async getAllKnownBlockHashes(): Promise<string[]> {
    this.assertOpen();
    if (this.client === undefined) {
      /*
       * A reader that never ran init() (it holds no lease and knows no open
       * commits): read through the gated node-agnostic view.
       */
      this.client = new ClickHouseClient(this.options.connection);
      const viewRows = await this.client.query<{ hash_hex: string }>(
        'SELECT DISTINCT lower(hex(hash)) AS hash_hex FROM block_v'
      );
      return viewRows.map((row) => row.hash_hex);
    }
    const rows = await this.requireClient().query<{ hash_hex: string }>(
      `SELECT lower(hex(hash)) AS hash_hex FROM block WHERE ${committedSql()}`,
      { fence: this.fenceArray, open: this.openSeqs() }
    );
    return [...new Set(rows.map((row) => row.hash_hex))];
  }

  /* ------------------------------------------------------------------ */
  /* blocks                                                              */
  /* ------------------------------------------------------------------ */

  async saveBlock(args: {
    block: ChaingraphBlock;
    nodeAcceptances: NodeAcceptance[];
    isSavedTransaction: (hash: string) => boolean;
  }): Promise<SaveBlockResult> {
    this.assertOpen();
    if (this.abandonSignal.abandoned) {
      return { attemptedSavedTransactions: [], transactionCacheMisses: 0 };
    }
    const nodes = [
      ...new Set(args.nodeAcceptances.map((item) => item.nodeInternalId)),
    ];
    const operation = await this.beginOperation('block', nodes);
    const parked = deferred<SaveBlockResult>();
    parked.promise.catch(() => undefined);
    let reportedEarly = false;
    const full = (async () => {
      try {
        try {
          await this.takeSlot(operation);
        } catch (error) {
          operation.markFailed(error);
          if (error instanceof AbandonedError) {
            // shutdown while queued: nothing written (as an abandoned save)
            return {
              attemptedSavedTransactions: [],
              transactionCacheMisses: 0,
            };
          }
          throw error;
        }
        if (this.committerInstance === undefined) {
          throw new StoreClosedError('ClickHouseStore.init() has not run.');
        }
        return await this.committerInstance.save(operation, args, (result) => {
          reportedEarly = true;
          parked.resolve(result);
        });
      } finally {
        operation.slot?.release();
        this.mempool.removeModifier(operation);
        this.operations.end(operation);
      }
    })();
    full.catch((error: unknown) => {
      if (reportedEarly && !(error instanceof SimulatedCrash)) {
        this.options.onError?.(error);
      }
    });
    /*
     * A child block waiting for its parent's outputs resolves once its
     * commit is `incomplete` (block-commit.ts); everything else resolves on
     * commit.
     */
    return Promise.race([full, parked.promise]);
  }

  /**
   * Header-sync acceptance of stored blocks by one node (plan §2.2): one
   * `header_accept` commit with `node_block` +1, `tx_acceptance` +1 (server
   * side, from `block_transaction`) and, above the horizon, the node's UTXO
   * transitions. Resolves the number of acceptances added (Postgres
   * `rowCount`; blocks the node already accepts are skipped).
   */
  async acceptBlocksViaHeaders(
    nodeInternalId: number,
    acceptedBlocks: { height: number; hash: string }[],
    acceptedAt: Date
  ): Promise<number | null> {
    this.assertOpen();
    const operation = await this.beginOperation('header_accept', [
      nodeInternalId,
    ]);
    let commit: OpenCommit | undefined;
    try {
      await this.takeSlot(operation);
      await waitForPredecessorRows(operation);
      const dependencies = new Set<StoreOperation>(operation.predecessors);
      const client = this.requireClient();
      const hashes = [...new Set(acceptedBlocks.map((block) => block.hash))];
      const stored: {
        hash_hex: string;
        internal_id: string;
        height: number;
        timestamp: number;
        commit_seq: string;
      }[] = [];
      for (const chunk of chunked(hashes, this.lookupChunkSize)) {
        appendAll(
          stored,
          await client.query<{
            hash_hex: string;
            internal_id: string;
            height: number;
            timestamp: number;
            commit_seq: string;
          }>(
            `SELECT lower(hex(hash)) AS hash_hex, internal_id, height, timestamp, commit_seq FROM block
             WHERE hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
               AND ${validCommitSql()}
             ORDER BY commit_seq`,
            { fence: this.fenceArray, hashes: chunk }
          )
        );
      }
      const blocks = new Map<string, (typeof stored)[number]>();
      stored.forEach((row) => {
        if (!blocks.has(row.hash_hex)) {
          blocks.set(row.hash_hex, row);
          const owner = this.operationOfSeq(BigInt(row.commit_seq));
          if (owner !== undefined && owner !== operation) {
            dependencies.add(owner);
          }
        }
      });
      const live = new Set<string>();
      for (const chunk of chunked(
        [...blocks.values()].map((row) => BigInt(row.internal_id)),
        this.lookupChunkSize
      )) {
        const rows = await client.query<{ block: string }>(
          `SELECT block_internal_id AS block FROM node_block
           WHERE node_internal_id = {node:UInt32} AND has({blocks:Array(UInt64)}, block_internal_id)
             AND ${validCommitSql()}
           GROUP BY block_internal_id HAVING sum(sign) > 0`,
          { blocks: chunk, fence: this.fenceArray, node: nodeInternalId }
        );
        rows.forEach((row) => live.add(row.block));
      }
      const toAccept = [...blocks.values()].filter(
        (row) => !live.has(row.internal_id)
      );
      if (toAccept.length === 0) {
        operation.markDone();
        return 0;
      }
      const nullifyBefore = Math.round(
        acceptedAt.getTime() / msPerSecond - twoHoursSeconds
      );
      /* the node's mempool cleanup for the accepted blocks, in this commit */
      const hooks = this.requireMempoolCommitter();
      await freshen(hooks, this.mempool, operation, [nodeInternalId]);
      const mempoolChanges: NodeMempoolChange[] = [];
      if (!this.mempool.isEmpty(nodeInternalId)) {
        const blocksAccepted = new Map<string, Date | null>(
          toAccept.map((row) => [
            row.internal_id,
            Number(row.timestamp) < nullifyBefore ? null : acceptedAt,
          ])
        );
        const { inclusions, outputsOf } = await hooks.loadInclusions(
          nodeInternalId,
          blocksAccepted,
          operation,
          dependencies
        );
        const known = await hooks.knownOutputsForConfirmed(
          nodeInternalId,
          inclusions,
          operation,
          dependencies
        );
        const change = this.mempool.planBlockAcceptance(
          nodeInternalId,
          inclusions,
          outputsOf,
          (spent) => known.get(spent)
        );
        if (!isEmptyChange(change)) {
          mempoolChanges.push(change);
        }
      }
      hooks.applyChanges(operation, mempoolChanges);
      const historyIds = await hooks.historyIds(mempoolChanges);
      commit = await this.requireCommitLog().beginCommit({
        kind: 'header_accept',
        nodeScope: [nodeInternalId],
      });
      // eslint-disable-next-line require-atomic-updates
      operation.seq = commit.seq;
      await this.fault('intent', { kind: 'header_accept', seq: commit.seq });
      const nodeBlockRows: NodeBlockRow[] = toAccept.map((row) => ({
        acceptedAt: Number(row.timestamp) < nullifyBefore ? null : acceptedAt,
        blockHash: row.hash_hex,
        blockInternalId: BigInt(row.internal_id),
        height: Number(row.height),
        nodeInternalId,
        sign: 1,
        version: commit!.seq,
      }));
      const rowCounts: { [table: string]: number } = {};
      const encoded = encodeNodeBlockRows(nodeBlockRows, commit.seq);
      await client.insertRowBinary(
        'node_block',
        acceptanceColumns.node_block,
        encoded.data,
        { deduplicationToken: commit.token('node_block') }
      );
      rowCounts.node_block = encoded.rowCount;
      await this.fault('node_block', {
        kind: 'header_accept',
        seq: commit.seq,
      });
      const params = {
        fence: this.fenceArray,
        node: nodeInternalId,
        seq: commit.seq,
      };
      const acceptedBlocksSql = `SELECT block_internal_id FROM node_block
        WHERE node_internal_id = {node:UInt32} AND commit_seq = {seq:UInt64}`;
      const acceptedTxsSql = `SELECT transaction_hash FROM block_transaction
        WHERE block_internal_id IN (${acceptedBlocksSql}) AND ${validCommitSql()}`;
      if (this.mode === 'tip') {
        const deltas = `SELECT transaction_hash, toInt8(1) AS d
          FROM (SELECT DISTINCT transaction_hash FROM (${acceptedTxsSql}))
          WHERE transaction_hash NOT IN (
            SELECT transaction_hash FROM tx_acceptance
            WHERE node_internal_id = {node:UInt32} AND commit_seq != {seq:UInt64}
              AND transaction_hash IN (${acceptedTxsSql})
              AND ${validCommitSql()}
            GROUP BY transaction_hash, block_internal_id
            HAVING sum(sign) > 0)`;
        await client.insertSelect(utxoDeltaInsertSql('utxo', deltas), params, {
          deduplicationToken: commit.token('utxo'),
        });
        await client.insertSelect(
          utxoDeltaInsertSql('utxo_by_script', deltas),
          params,
          { deduplicationToken: commit.token('utxo_by_script') }
        );
        await this.fault('utxo', { kind: 'header_accept', seq: commit.seq });
      }
      await client.insertSelect(
        `INSERT INTO tx_acceptance (${acceptanceColumns.tx_acceptance.join(
          ', '
        )})
         SELECT bt.transaction_hash, {node:UInt32}, bt.block_internal_id, bt.transaction_internal_id,
           nb.height, nb.accepted_at, toInt8(1), {seq:UInt64}, {seq:UInt64}
         FROM block_transaction AS bt
         INNER JOIN (
           SELECT block_internal_id, height, accepted_at FROM node_block
           WHERE node_internal_id = {node:UInt32} AND commit_seq = {seq:UInt64}) AS nb
           ON bt.block_internal_id = nb.block_internal_id
         WHERE bt.block_internal_id IN (${acceptedBlocksSql})
           AND ${validCommitSql('bt.commit_seq')}`,
        params,
        { deduplicationToken: commit.token('tx_acceptance') }
      );
      await this.fault('tx_acceptance', {
        kind: 'header_accept',
        seq: commit.seq,
      });
      if (mempoolChanges.length > 0) {
        await hooks.insertChangeRows(
          commit,
          'header_accept',
          changeRows(mempoolChanges, historyIds),
          'm',
          rowCounts
        );
      }
      operation.markRowsWritten();
      const settled = this.abandonSignal.race(awaitDependencies(dependencies));
      await (dependencies.size > 0 ? operation.whileWaiting(settled) : settled);
      await this.requireCommitLog().markCommitted(commit.seq, rowCounts);
      operation.markCommitted();
      await this.fault('committed', { kind: 'header_accept', seq: commit.seq });
      return toAccept.length;
    } catch (error) {
      if (!(error instanceof SimulatedCrash)) {
        this.mempool.markStale(operation);
      }
      await this.failOperation(
        operation,
        commit,
        error,
        'acceptBlocksViaHeaders'
      );
      throw error;
    } finally {
      operation.slot?.release();
      this.mempool.removeModifier(operation);
      this.operations.end(operation);
    }
  }

  /**
   * Re-org release for one node (plan §3.6, `trigger_node_block_delete`): one
   * `reorg` commit with `node_block` −1 (version = its own seq),
   * `node_block_history`, `tx_acceptance` −1 and, above the horizon, the UTXO
   * inverse rows for the node's transactions no longer accepted. Other nodes
   * are untouched. Transactions are not returned to the mempool (Postgres).
   */
  async removeStaleBlocksForNode(
    nodeInternalId: number,
    staleChain: string[],
    removedAt?: Date
  ): Promise<void> {
    this.assertOpen();
    const operation = await this.beginOperation('reorg', [nodeInternalId]);
    let commit: OpenCommit | undefined;
    try {
      await waitForPredecessorRows(operation);
      const dependencies = new Set<StoreOperation>(operation.predecessors);
      const client = this.requireClient();
      const liveRows: {
        block: string;
        hash: string;
        block_height: number;
        accepted_ms: string | null;
        seqs: string[];
      }[] = [];
      for (const chunk of chunked(
        [...new Set(staleChain)],
        this.lookupChunkSize
      )) {
        appendAll(
          liveRows,
          await client.query<(typeof liveRows)[number]>(
            `SELECT block_internal_id AS block, lower(hex(any(block_hash))) AS hash,
               any(height) AS block_height,
               toUnixTimestamp64Milli(argMaxIf(accepted_at, version, sign > 0)) AS accepted_ms,
               groupArray(commit_seq) AS seqs
             FROM node_block
             WHERE node_internal_id = {node:UInt32}
               AND block_hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
               AND ${validCommitSql()}
             GROUP BY block_internal_id
             HAVING sum(sign) > 0`,
            { fence: this.fenceArray, hashes: chunk, node: nodeInternalId }
          )
        );
      }
      if (liveRows.length === 0) {
        operation.markDone();
        return;
      }
      liveRows.forEach((row) => {
        row.seqs.forEach((seq) => {
          const owner = this.operationOfSeq(BigInt(seq));
          if (owner !== undefined && owner !== operation) {
            dependencies.add(owner);
          }
        });
      });
      const historyIds = await this.requireIds().allocate(
        'node_block_history',
        liveRows.length
      );
      const historyIdList = historyIds.flatMap((segment) => {
        const list: bigint[] = [];
        for (let id = segment.start; id < segment.end; id += 1n) {
          list.push(id);
        }
        return list;
      });
      commit = await this.requireCommitLog().beginCommit({
        kind: 'reorg',
        nodeScope: [nodeInternalId],
      });
      // eslint-disable-next-line require-atomic-updates
      operation.seq = commit.seq;
      await this.fault('intent', { kind: 'reorg', seq: commit.seq });
      const removed = removedAt ?? new Date();
      const nodeBlockRows: NodeBlockRow[] = liveRows.map((row) => ({
        acceptedAt:
          row.accepted_ms === null ? null : new Date(Number(row.accepted_ms)),
        blockHash: row.hash,
        blockInternalId: BigInt(row.block),
        height: Number(row.block_height),
        nodeInternalId,
        sign: -1,
        version: commit!.seq,
      }));
      const historyRows: NodeBlockHistoryRow[] = liveRows.map((row, index) => ({
        acceptedAt:
          row.accepted_ms === null ? null : new Date(Number(row.accepted_ms)),
        blockInternalId: BigInt(row.block),
        internalId: historyIdList[index]!,
        nodeInternalId,
        removedAt: removed,
      }));
      const rowCounts: { [table: string]: number } = {};
      const nodeBlocks = encodeNodeBlockRows(nodeBlockRows, commit.seq);
      await client.insertRowBinary(
        'node_block',
        acceptanceColumns.node_block,
        nodeBlocks.data,
        { deduplicationToken: commit.token('node_block') }
      );
      rowCounts.node_block = nodeBlocks.rowCount;
      await this.fault('node_block', { kind: 'reorg', seq: commit.seq });
      const history = encodeNodeBlockHistoryRows(historyRows, commit.seq);
      await client.insertRowBinary(
        'node_block_history',
        acceptanceColumns.node_block_history,
        history.data,
        { deduplicationToken: commit.token('node_block_history') }
      );
      rowCounts.node_block_history = history.rowCount;
      await this.fault('node_block_history', {
        kind: 'reorg',
        seq: commit.seq,
      });
      const [minHeight, maxHeight] = minMax(
        liveRows.map((row) => Number(row.block_height))
      );
      const params = {
        fence: this.fenceArray,
        maxHeight,
        minHeight,
        node: nodeInternalId,
        seq: commit.seq,
      };
      const staleBlocksSql = `SELECT block_internal_id FROM node_block
        WHERE node_internal_id = {node:UInt32} AND commit_seq = {seq:UInt64} AND sign = -1`;
      const staleTxsSql = `SELECT transaction_hash FROM block_transaction
        WHERE block_internal_id IN (${staleBlocksSql}) AND ${validCommitSql()}`;
      if (this.mode === 'tip') {
        const deltas = `SELECT transaction_hash, toInt8(-1) AS d
          FROM (SELECT DISTINCT transaction_hash FROM (${staleTxsSql}))
          WHERE transaction_hash NOT IN (
            SELECT transaction_hash FROM tx_acceptance
            WHERE node_internal_id = {node:UInt32} AND commit_seq != {seq:UInt64}
              AND transaction_hash IN (${staleTxsSql})
              AND block_internal_id NOT IN (${staleBlocksSql})
              AND ${validCommitSql()}
            GROUP BY transaction_hash, block_internal_id
            HAVING sum(sign) > 0)`;
        await client.insertSelect(utxoDeltaInsertSql('utxo', deltas), params, {
          deduplicationToken: commit.token('utxo'),
        });
        await client.insertSelect(
          utxoDeltaInsertSql('utxo_by_script', deltas),
          params,
          { deduplicationToken: commit.token('utxo_by_script') }
        );
        await this.fault('utxo', { kind: 'reorg', seq: commit.seq });
      }
      await client.insertSelect(
        `INSERT INTO tx_acceptance (${acceptanceColumns.tx_acceptance.join(
          ', '
        )})
         SELECT transaction_hash, node_internal_id, block_internal_id, any(transaction_internal_id),
           any(height), argMaxIf(accepted_at, version, sign > 0), toInt8(-1), {seq:UInt64}, {seq:UInt64}
         FROM tx_acceptance
         WHERE node_internal_id = {node:UInt32}
           AND transaction_hash IN (${staleTxsSql})
           AND height >= {minHeight:UInt32} AND height <= {maxHeight:UInt32}
           AND block_internal_id IN (${staleBlocksSql})
           AND commit_seq != {seq:UInt64}
           AND ${validCommitSql()}
         GROUP BY transaction_hash, node_internal_id, block_internal_id
         HAVING sum(sign) > 0`,
        params,
        { deduplicationToken: commit.token('tx_acceptance') }
      );
      await this.fault('tx_acceptance', { kind: 'reorg', seq: commit.seq });
      operation.markRowsWritten();
      await this.abandonSignal.race(awaitDependencies(dependencies));
      await this.requireCommitLog().markCommitted(commit.seq, rowCounts);
      operation.markCommitted();
      await this.fault('committed', { kind: 'reorg', seq: commit.seq });
    } catch (error) {
      await this.failOperation(
        operation,
        commit,
        error,
        'removeStaleBlocksForNode'
      );
      throw error;
    } finally {
      this.operations.end(operation);
    }
  }

  /**
   * Repair scan (plan §3.7). With commits, a stored block is complete iff
   * its commit is committed, so this reports (a) committed blocks accepted by
   * the nodes whose linked transactions do not add up to the block size (the
   * Postgres check, kept as a verifier) and (b) blocks whose block commit was
   * aborted (rows on disk but void) and not stored since, with linked size 0.
   */
  async getIncompleteBlocks({
    excludedBlockHashes,
    heightLowerBound,
    heightUpperBound,
    limit,
    nodeInternalIds,
  }: {
    excludedBlockHashes: string[];
    heightLowerBound: number;
    heightUpperBound: number;
    limit: number;
    nodeInternalIds: number[];
  }): Promise<IncompleteBlockScan> {
    this.assertOpen();
    if (nodeInternalIds.length === 0) {
      return { incompleteBlocks: [], scannedBlockCount: 0 };
    }
    const client = this.requireClient();
    const params = {
      excluded: excludedBlockHashes.map((hash) => hash.toLowerCase()),
      fence: this.fenceArray,
      high: heightUpperBound,
      low: heightLowerBound,
      nodes: nodeInternalIds,
      open: this.openSeqs(),
    };
    const committedBlocks = await client.query<{
      hash: string;
      height: number;
      size_bytes: number;
      transaction_count: string;
      transaction_bytes: string;
    }>(
      `WITH
         accepted AS (
           SELECT block_internal_id FROM node_block
           WHERE has({nodes:Array(UInt32)}, node_internal_id)
             AND height >= {low:UInt32} AND height < {high:UInt32}
             AND ${committedSql()}
           GROUP BY node_internal_id, block_internal_id HAVING sum(sign) > 0),
         blocks AS (
           SELECT internal_id, hash, height, size_bytes FROM block
           WHERE height >= {low:UInt32} AND height < {high:UInt32}
             AND internal_id IN (SELECT block_internal_id FROM accepted)
             AND NOT has({excluded:Array(String)}, lower(hex(hash)))
             AND ${committedSql()}),
         links AS (
           SELECT block_internal_id, transaction_hash FROM block_transaction
           WHERE block_internal_id IN (SELECT internal_id FROM blocks) AND ${committedSql()}),
         sizes AS (
           SELECT hash, any(size_bytes) AS tx_size FROM transaction
           WHERE hash IN (SELECT transaction_hash FROM links) AND ${committedSql()}
           GROUP BY hash),
         linked AS (
           SELECT l.block_internal_id AS id, count() AS transaction_count, sum(s.tx_size) AS transaction_bytes
           FROM links AS l INNER JOIN sizes AS s ON s.hash = l.transaction_hash
           GROUP BY l.block_internal_id)
       SELECT lower(hex(b.hash)) AS hash, b.height AS height, b.size_bytes AS size_bytes,
         toString(k.transaction_count) AS transaction_count, toString(k.transaction_bytes) AS transaction_bytes
       FROM blocks AS b LEFT JOIN linked AS k ON k.id = b.internal_id`,
      params
    );
    const abortedBlocks = await client.query<{
      hash_hex: string;
      block_height: number;
      block_size: number;
    }>(
      `SELECT lower(hex(hash)) AS hash_hex, any(height) AS block_height, any(size_bytes) AS block_size FROM block
       WHERE height >= {low:UInt32} AND height < {high:UInt32}
         AND commit_seq IN (SELECT commit_seq FROM commit_void)
         AND commit_seq IN (SELECT commit_seq FROM commit_log
                            WHERE kind = 'block' AND hasAny(node_scope, {nodes:Array(UInt32)}))
         AND hash NOT IN (SELECT hash FROM block WHERE ${committedSql()})
         AND NOT has({excluded:Array(String)}, lower(hex(hash)))
       GROUP BY hash`,
      params
    );
    const incomplete: IncompleteBlock[] = [
      ...committedBlocks
        .map((row) => {
          const transactionCount = Number(row.transaction_count);
          return {
            hash: row.hash,
            height: Number(row.height),
            linkedSizeBytes: linkedBlockSize(
              transactionCount,
              Number(row.transaction_bytes)
            ),
            sizeBytes: Number(row.size_bytes),
            transactionCount,
          };
        })
        .filter((block) => block.linkedSizeBytes !== block.sizeBytes),
      ...abortedBlocks.map((row) => ({
        hash: row.hash_hex,
        height: Number(row.block_height),
        linkedSizeBytes: 0,
        sizeBytes: Number(row.block_size),
        transactionCount: 0,
      })),
    ];
    incomplete.sort((a, b) =>
      a.height === b.height
        ? a.hash < b.hash
          ? -1
          : a.hash > b.hash
          ? 1
          : 0
        : a.height - b.height
    );
    return {
      incompleteBlocks: incomplete.slice(0, limit),
      scannedBlockCount: committedBlocks.length + abortedBlocks.length,
    };
  }

  /* ------------------------------------------------------------------ */
  /* initial sync: the bulk horizon (plan §3.8, §5.1)                    */
  /* ------------------------------------------------------------------ */

  /**
   * Enter bulk mode: from here until `finishInitialSync`, commits write no
   * inline UTXO rows. Recorded as a `horizon_switch` commit (its seq is the
   * bulk start), so a restart mid-sync resumes bulk mode. Resolves `false`:
   * no Postgres-style sync-only setting is applied.
   */
  async prepareForInitialSync(): Promise<boolean> {
    this.assertOpen();
    if (this.mode === 'bulk') {
      return false;
    }
    await this.runExclusive(async () => {
      const commitLog = this.requireCommitLog();
      const commit = await commitLog.beginCommit({
        kind: 'horizon_switch',
        nodeScope: [],
      });
      await commitLog.markCommitted(commit.seq, { bulk_enter: 1 });
      this.bulkStartSeq = commit.seq;
      this.mode = 'bulk';
    });
    return false;
  }

  /**
   * Leave bulk mode: build the UTXO rows every bulk-mode commit skipped, as
   * ONE `utxo_build` commit (per node, per height batch, server side), then
   * switch to inline emission. Exact: for every (node, tx) with acceptance
   * rows written in bulk mode it emits acc(now) − acc(before bulk), and the
   * rows before bulk were emitted inline (wp5a-core.md §5). Projections are
   * defined on the (initially empty) tables and maintained on insert, so
   * there is nothing to materialize.
   */
  async finishInitialSync(hooks: FinishInitialSyncHooks): Promise<void> {
    this.assertOpen();
    if (this.mode !== 'bulk') {
      return;
    }
    await this.runExclusive(async () => {
      const client = this.requireClient();
      const commitLog = this.requireCommitLog();
      const bulkStart = this.bulkStartSeq;
      const nodeRows = await client.query<{ node: number }>(
        `SELECT DISTINCT node_internal_id AS node FROM node_block
         WHERE commit_seq >= {bulkStart:UInt64} AND ${validCommitSql()} ORDER BY node`,
        { bulkStart, fence: this.fenceArray }
      );
      const nodes = nodeRows.map((row) => Number(row.node));
      const commit = await commitLog.beginCommit({
        kind: 'utxo_build',
        nodeScope: nodes,
      });
      try {
        await this.fault('intent', { kind: 'utxo_build', seq: commit.seq });
        const batchSize =
          this.options.horizonBatchHeights ?? defaultHorizonBatchHeights;
        for (const node of nodes) {
          const range = await client.query<{ low: number; high: number }>(
            `SELECT min(height) AS low, max(height) AS high FROM node_block
             WHERE node_internal_id = {node:UInt32} AND commit_seq >= {bulkStart:UInt64}
               AND commit_seq != {seq:UInt64} AND ${validCommitSql()}`,
            { bulkStart, fence: this.fenceArray, node, seq: commit.seq }
          );
          const batches = heightBatches(
            Number(range[0]?.low ?? 0),
            Number(range[0]?.high ?? -1),
            batchSize
          );
          for (const [index, [fromHeight, toHeight]] of batches.entries()) {
            const params = {
              bulkStart,
              fence: this.fenceArray,
              fromHeight,
              node,
              seq: commit.seq,
              toHeight,
            };
            await client.insertSelect(
              utxoDeltaInsertSql('utxo', horizonDeltasSql),
              params,
              { deduplicationToken: commit.token('utxo', `n${node}b${index}`) }
            );
            await client.insertSelect(
              utxoDeltaInsertSql('utxo_by_script', horizonDeltasSql),
              params,
              {
                deduplicationToken: commit.token(
                  'utxo_by_script',
                  `n${node}b${index}`
                ),
              }
            );
            await this.fault('utxo', { kind: 'utxo_build', seq: commit.seq });
            hooks.onIndexProgress([
              [
                `utxo (node ${node})`,
                Math.round(((index + 1) / batches.length) * 100).toString(),
              ],
            ]);
          }
        }
        await commitLog.markCommitted(commit.seq, {
          bulk_start: bulkStart,
        });
        this.mode = 'tip';
      } catch (error) {
        if (!(error instanceof SimulatedCrash)) {
          await commitLog
            .markAborted(commit.seq, `utxo build failed: ${String(error)}`)
            .catch(() => undefined);
        }
        throw error;
      }
    });
    await this.requirePublisher()
      .publishWatermark()
      .catch((error: unknown) => {
        hooks.onNonFatalError(error);
      });
  }

  async enableMempoolTracking(): Promise<{ schemaIsCurrent: boolean }> {
    this.assertOpen();
    this.mempool.tracking = true;
    return Promise.resolve({ schemaIsCurrent: true });
  }

  /* ------------------------------------------------------------------ */
  /* mempool: WP5a-mempool                                               */
  /* ------------------------------------------------------------------ */

  async saveMempoolTransaction(
    transaction: ChaingraphTransaction,
    nodeValidations: NodeValidation[]
  ): Promise<void> {
    this.assertOpen();
    return this.requireMempoolCommitter().saveTransaction(
      transaction,
      nodeValidations
    );
  }

  async recordNodeValidation(
    transactionHash: string,
    validation: NodeValidation
  ): Promise<void> {
    this.assertOpen();
    return this.requireMempoolCommitter().recordValidation(
      transactionHash,
      validation
    );
  }

  async archiveMempoolTransactionsAcceptedByBlocks(): Promise<
    ArchivedMempoolTransaction[]
  > {
    this.assertOpen();
    return this.requireMempoolCommitter().sweep();
  }

  async getMempoolTransactionsExpiringBefore(args: {
    expirationMs: number;
    expiresBefore: Date;
  }): Promise<ExpiringMempoolTransaction[]> {
    this.assertOpen();
    return Promise.resolve(this.requireMempoolCommitter().expiringBefore(args));
  }

  async archiveMempoolTransaction(args: {
    nodeInternalId: number;
    replacedAt: Date;
    transactionInternalId: number;
  }): Promise<number> {
    this.assertOpen();
    return this.requireMempoolCommitter().expire(args);
  }

  /* ------------------------------------------------------------------ */
  /* internals                                                           */
  /* ------------------------------------------------------------------ */

  private async fault(
    step: string,
    context: { kind: string; seq: bigint | undefined }
  ) {
    if (this.fatal instanceof SimulatedCrash) {
      throw this.fatal;
    }
    this.abandonSignal.assertNotAbandoned();
    await this.options.fault?.(step, context);
  }

  private operationOfSeq(seq: bigint): StoreOperation | undefined {
    return this.operations.operationOfSeq(seq);
  }

  private openSeqs(): bigint[] {
    return this.requireCommitLog()
      .openCommits()
      .map((commit) => commit.seq);
  }

  /**
   * Register an operation now (call order = agent order), unless a mode
   * switch is draining: then wait for it first.
   */
  private async beginOperation(
    kind: OperationKind,
    nodes: readonly number[]
  ): Promise<StoreOperation> {
    while (this.exclusive !== undefined) {
      await this.exclusive;
    }
    this.assertOpen();
    return this.operations.begin(kind, nodes);
  }

  /**
   * Take an in-flight slot for `operation` (no-op when unbounded). Called
   * after registration, so the operation is already a predecessor of later
   * calls and its ticket (id) is its call order.
   */
  private async takeSlot(operation: StoreOperation) {
    if (this.slots === undefined) return;
    const slot = new SaveSlot(this.slots, operation.id, this.abandonSignal);
    operation.slot = slot;
    await slot.acquire();
  }

  /** Run `work` with no other operation live (mode switches). */
  private async runExclusive(work: () => Promise<void>): Promise<void> {
    while (this.exclusive !== undefined) {
      await this.exclusive;
    }
    const gate = deferred<void>();
    this.exclusive = gate.promise;
    try {
      await this.operations.drain();
      await work();
    } finally {
      this.exclusive = undefined;
      gate.resolve();
    }
  }

  private async failOperation(
    operation: StoreOperation,
    commit: OpenCommit | undefined,
    error: unknown,
    method: string
  ) {
    if (!(error instanceof SimulatedCrash) && commit !== undefined) {
      await this.requireCommitLog()
        .markAborted(commit.seq, `${method} failed: ${String(error)}`)
        .catch(() => undefined);
    }
    operation.markFailed(error);
  }

  private async loadFence() {
    const rows = await this.requireClient().query<{ fence: string[] }>(
      `SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq)))) AS fence
       FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM epoch_fence GROUP BY epoch)`
    );
    this.fenceArray = (rows[0]?.fence ?? []).map(BigInt);
  }

  private assertOpen() {
    if (this.fatal !== undefined) {
      throw this.fatal instanceof Error
        ? this.fatal
        : new StoreClosedError(String(this.fatal));
    }
    if (this.closed) {
      throw new StoreClosedError('The ClickHouse store is closed.');
    }
  }

  private requireClient(): ClickHouseClient {
    if (this.client === undefined) {
      throw new StoreClosedError('ClickHouseStore.init() has not run.');
    }
    return this.client;
  }

  private requireCommitLog(): CommitLog {
    return this.commitLog;
  }

  private requireIds(): IdAllocator {
    if (this.ids === undefined) {
      throw new StoreClosedError('ClickHouseStore.init() has not run.');
    }
    return this.ids;
  }

  private requireMempoolCommitter(): MempoolCommitter {
    if (this.mempoolCommitter === undefined) {
      throw new StoreClosedError('ClickHouseStore.init() has not run.');
    }
    return this.mempoolCommitter;
  }

  private requirePublisher(): VisibilityPublisher {
    if (this.publisher === undefined) {
      throw new StoreClosedError('ClickHouseStore.init() has not run.');
    }
    return this.publisher;
  }
}

export const createClickHouseStore = (options: ClickHouseStoreOptions) =>
  new ClickHouseStore(options);
