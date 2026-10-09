// cspell:ignore clickhouse
/**
 * Backend-neutral write interface used by the agent (`src/agent.ts`).
 *
 * Each method keeps the signature of the `src/db.ts` function it replaces (see
 * docs/clickhouse-port/wp1-wp3-design.md §E/§F), so the Postgres backend is a
 * pure refactor. The "ClickHouse writes" note on each method lists the §2
 * ClickHouse tables the WP5 backend is expected to write for it.
 *
 * The ClickHouse backend owns the mempool graph (conflicts, cascades, expiry);
 * on Postgres the same work is done by triggers. Either way the agent stays
 * backend-agnostic.
 */
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

export interface IncompleteBlock {
  hash: string;
  height: number;
  linkedSizeBytes: number;
  sizeBytes: number;
  transactionCount: number;
}

export interface IncompleteBlockScan {
  incompleteBlocks: IncompleteBlock[];
  scannedBlockCount: number;
}

export interface ExpiringMempoolTransaction {
  expiresAt: Date;
  hash: string;
  nodeInternalId: number;
  nodeName: string;
  transactionInternalId: number;
  validatedAt: Date;
}

export interface ArchivedMempoolTransaction {
  hash: string;
  nodeName: string;
  replacedAt: Date | null;
}

export interface NodeValidation {
  nodeInternalId: number;
  validatedAt: Date;
}

export interface NodeAcceptance {
  nodeInternalId: number;
  acceptedAt: Date | null;
  nodeName: string;
}

/**
 * Connection-pool statistics for the agent heartbeat log (logged as `pgPool`).
 */
export interface StorePoolStats {
  clients: {
    active: number;
    max: number;
    total: number;
  };
  waitingRequests: number;
}

/**
 * Callbacks for `finishInitialSync`. They mirror the log lines the agent wrote
 * when it drove the Postgres-specific steps itself.
 */
export interface FinishInitialSyncHooks {
  /**
   * Called (repeatedly) with the progress of any index/projection builds.
   */
  onIndexProgress: (
    progress: [indexName: string, percentage: string][]
  ) => void;
  /**
   * Called for errors which must not abort the sync finish (failing to restore
   * a sync-only setting, failing to poll build progress).
   */
  onNonFatalError: (err: unknown) => void;
  /**
   * Called if a setting applied by `prepareForInitialSync` was restored.
   */
  onSyncSettingsRestored: () => void;
}

export interface ChaingraphStore {
  /**
   * Prepare the backend before the agent starts.
   * Postgres: no-op. ClickHouse writes: `writer_lease` (take the lease),
   * `commit_log` (mark `intent`s without `committed` as `aborted`); also
   * rebuilds the in-memory mempool graph.
   */
  init: () => Promise<void>;

  /**
   * Release all connections (and, on ClickHouse, the writer lease).
   * ClickHouse writes: `writer_lease`.
   */
  close: () => Promise<void>;

  /**
   * Connection-pool statistics for the heartbeat log. No writes.
   */
  poolStats: () => StorePoolStats;

  /**
   * Called once before initial sync begins; resolves `true` if a sync-only
   * setting was applied (Postgres: `synchronous_commit = off`, if configured).
   * ClickHouse writes: `commit_log` (bulk-horizon switch).
   */
  prepareForInitialSync: () => Promise<boolean>;

  /**
   * Called once when initial sync completes: undo `prepareForInitialSync`,
   * then build any missing indexes (Postgres) / materialize projections
   * (ClickHouse). Rejects only if the index/projection build fails.
   * ClickHouse writes: `commit_log`.
   */
  finishInitialSync: (hooks: FinishInitialSyncHooks) => Promise<void>;

  /**
   * Start maintaining per-node mempools (Postgres: re-enable the mempool
   * cleaning triggers). `schemaIsCurrent` is `false` if the database schema is
   * missing migrations. Replaces `reenableMempoolCleaning`. No ClickHouse table
   * writes.
   */
  enableMempoolTracking: () => Promise<{ schemaIsCurrent: boolean }>;

  /**
   * Upsert a trusted node and restore the chain of blocks it has accepted.
   * Replaces `registerTrustedNodeWithDb`. ClickHouse writes: `node`.
   */
  registerNode: (node: {
    latestConnectionBeganAt: Date;
    nodeName: string;
    protocolVersion: number;
    userAgent: string;
  }) => Promise<{
    internalId: number;
    syncedHeaderHashChain: (string | null)[];
  }>;

  /**
   * All known block hashes (hex), node-agnostic. No writes.
   */
  getAllKnownBlockHashes: () => Promise<string[]>;

  /**
   * Save a block (and every transaction not already saved), accepted by the
   * listed nodes. `isSavedTransaction` reports transactions already known to
   * be saved, which are skipped.
   * ClickHouse writes: `output`, `input`, `transaction`, `block`,
   * `block_transaction`, `node_block` +1, `tx_acceptance` (mempool −1 /
   * block +1), `node_transaction` −1 (confirms, conflicts and cascade),
   * `node_transaction_history`, `utxo` / `utxo_by_script`, `pending_spend`.
   */
  saveBlock: (args: {
    block: ChaingraphBlock;
    nodeAcceptances: NodeAcceptance[];
    isSavedTransaction: (hash: string) => boolean;
  }) => Promise<{
    attemptedSavedTransactions: ChaingraphTransaction[];
    transactionCacheMisses: number;
  }>;

  /**
   * Save a mempool transaction (if new) and record its validation by each
   * listed node. Replaces `saveTransactionForNodes`.
   * ClickHouse writes: base facts (`transaction`, `input`, `output`),
   * `node_transaction` +1, `tx_acceptance` +1, `utxo` ±, plus replacement and
   * cascade rows (`node_transaction` −1, `node_transaction_history`).
   */
  saveMempoolTransaction: (
    transaction: ChaingraphTransaction,
    nodeValidations: NodeValidation[]
  ) => Promise<void>;

  /**
   * Record that another node validated an already-saved transaction.
   * ClickHouse writes: `node_transaction`, `tx_acceptance`, `utxo`,
   * replacement rows (`node_transaction` −1, `node_transaction_history`).
   */
  recordNodeValidation: (
    transactionHash: string,
    validation: NodeValidation
  ) => Promise<void>;

  /**
   * Header-sync acceptance of already-saved blocks by a node; resolves the
   * number of acceptances inserted. ClickHouse writes: `node_block` +1,
   * `tx_acceptance` (INSERT…SELECT), `node_transaction` confirms, `utxo`.
   */
  acceptBlocksViaHeaders: (
    nodeInternalId: number,
    acceptedBlocks: { height: number; hash: string }[],
    acceptedAt: Date
  ) => Promise<number | null>;

  /**
   * Re-org release: remove a node's acceptance of stale blocks. `removedAt` is
   * ignored by Postgres (the history trigger uses `now()`).
   * ClickHouse writes: `node_block` −1, `node_block_history`,
   * `tx_acceptance` −1, `utxo` inverse.
   */
  removeStaleBlocksForNode: (
    nodeInternalId: number,
    staleChain: string[],
    removedAt?: Date
  ) => Promise<void>;

  /**
   * Repair sweep: archive mempool rows already accepted (or replaced) by a
   * block the same node accepted. ClickHouse writes: `node_transaction` −1,
   * `node_transaction_history`, `tx_acceptance`, `utxo`.
   */
  archiveMempoolTransactionsAcceptedByBlocks: () => Promise<
    ArchivedMempoolTransaction[]
  >;

  /**
   * Expiry scan: mempool rows whose `validated_at + expirationMs` is at or
   * before `expiresBefore`. No writes.
   */
  getMempoolTransactionsExpiringBefore: (args: {
    expirationMs: number;
    expiresBefore: Date;
  }) => Promise<ExpiringMempoolTransaction[]>;

  /**
   * Expire one mempool row (plus same-node descendants); resolves the number of
   * rows archived directly (0 if it already left the mempool).
   * ClickHouse writes: `node_transaction` −1, `node_transaction_history`,
   * `tx_acceptance`, `utxo`.
   */
  archiveMempoolTransaction: (args: {
    nodeInternalId: number;
    replacedAt: Date;
    transactionInternalId: number;
  }) => Promise<number>;

  /**
   * Repair scan: blocks whose linked transactions don't add up to the block's
   * size. ClickHouse: `intent` commits for the scope plus the counts check.
   * No writes.
   */
  getIncompleteBlocks: (args: {
    excludedBlockHashes: string[];
    heightLowerBound: number;
    heightUpperBound: number;
    limit: number;
    nodeInternalIds: number[];
  }) => Promise<IncompleteBlockScan>;
}

export type ChaingraphStoreBackend = 'clickhouse' | 'postgres';
