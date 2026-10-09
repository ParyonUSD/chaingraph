// cspell:ignore tgenabled
/**
 * Backend-neutral read interface used by the e2e suite (and later the
 * ingestion gate) to check what a store has persisted.
 *
 * Conventions:
 * - every hash is lower-case, big-endian hex (as displayed by explorers);
 * - every acceptance method (mempool, accepted blocks, history) takes a node
 *   NAME and only ever returns rows for that node – no method reads across
 *   nodes, so a leak from node A to node B cannot hide in an aggregate;
 * - base facts (transactions, blocks, inputs, outputs) are node-agnostic and
 *   addressed by hash;
 * - only stable columns are exposed (no `internal_id` of history rows);
 * - timestamps are UTC `Date`s; lists are sorted deterministically (by hash
 *   unless stated otherwise).
 */

export interface CheckerOutput {
  outputIndex: number;
  valueSatoshis: bigint;
  lockingBytecode: string;
  tokenCategory?: string;
  fungibleTokenAmount?: bigint;
  nonfungibleTokenCapability?: 'minting' | 'mutable' | 'none';
  nonfungibleTokenCommitment?: string;
}

export interface CheckerInput {
  inputIndex: number;
  outpointTransactionHash: string;
  outpointIndex: number;
  sequenceNumber: number;
  unlockingBytecode: string;
}

export interface CheckerOutpoint {
  transactionHash: string;
  outputIndex: number;
}

export interface BlockSelector {
  hash?: string;
  height?: number;
}

export interface BlockValueAggregates {
  fee: bigint;
  generated: bigint;
  input: bigint;
  output: bigint;
}

export interface AcceptedBlock {
  hash: string;
  height: number;
  acceptedAt: Date | null;
}

export interface MempoolEntry {
  hash: string;
  validatedAt: Date | null;
}

export interface TransactionHistoryEntry {
  hash: string;
  validatedAt: Date | null;
  replacedAt: Date | null;
}

export interface BlockHistoryEntry {
  hash: string;
  acceptedAt: Date | null;
  removedAt: Date;
}

export interface SchemaReport {
  indexes: string[];
  /**
   * Trigger name → backend-specific state (Postgres: `pg_trigger.tgenabled`).
   * ClickHouse must report none.
   */
  triggers: { [name: string]: string };
}

export interface StoreChecker {
  // nodes
  nodeInternalId: (node: string) => Promise<number | undefined>;
  /**
   * Sorted by name.
   */
  nodeNamesOrdered: () => Promise<{ name: string; internalId: number }[]>;

  // node-agnostic base facts (by hash)
  transactionExists: (hash: string) => Promise<boolean>;
  /**
   * Number of stored rows for this hash (duplicate detection).
   */
  transactionRowCount: (hash: string) => Promise<number>;
  /**
   * The P2P-encoded transaction as reconstructed from stored base facts.
   */
  encodedTransactionHex: (hash: string) => Promise<string | undefined>;
  encodedBlockHex: (by: BlockSelector) => Promise<string | undefined>;
  encodedBlockHeaderHex: (by: BlockSelector) => Promise<string | undefined>;
  /**
   * Every known block hash, sorted.
   */
  allBlockHashes: () => Promise<string[]>;
  blockTransactionCount: (blockHash: string) => Promise<number>;
  blockTransactionAt: (
    blockHash: string,
    index: number
  ) => Promise<string | undefined>;
  blockValueAggregates: (
    by: BlockSelector
  ) => Promise<BlockValueAggregates | undefined>;
  outputsOfTx: (hash: string) => Promise<CheckerOutput[]>;
  inputsOfTx: (hash: string) => Promise<CheckerInput[]>;
  inputsSpending: (
    outpointHash: string,
    index: number
  ) => Promise<{ txHash: string; inputIndex: number }[]>;

  // per-node acceptance
  /**
   * Sorted by height, then hash.
   */
  acceptedBlocks: (
    node: string,
    filter?: { height?: number }
  ) => Promise<AcceptedBlock[]>;
  acceptedBlockCount: (node: string, hashes: string[]) => Promise<number>;
  mempool: (node: string) => Promise<MempoolEntry[]>;
  mempoolMembership: (node: string, hashes: string[]) => Promise<Set<string>>;
  /**
   * Names of nodes with the transaction in their mempool, sorted.
   */
  validatingNodes: (hash: string) => Promise<string[]>;
  /**
   * Sorted by validatedAt, then replacedAt, then hash.
   */
  transactionHistory: (
    node: string,
    hashes?: string[]
  ) => Promise<TransactionHistoryEntry[]>;
  /**
   * Sorted by removedAt, then hash.
   */
  blockHistory: (node: string) => Promise<BlockHistoryEntry[]>;
  /**
   * In the node's mempool or in a block the node accepts.
   */
  txAccepted: (node: string, hash: string) => Promise<boolean>;
  /**
   * Outputs of transactions accepted by the node which no transaction
   * accepted by the node spends. Sorted by hash, then index.
   */
  unspent: (
    node: string,
    scope: { category?: string; lockingBytecode?: string }
  ) => Promise<CheckerOutpoint[]>;

  // per-node invariants (expected to be empty after every scenario)
  confirmedButInMempool: (node: string) => Promise<string[]>;
  orphanMempoolDescendants: (node: string) => Promise<string[]>;

  // fault injection
  dropBlockTransactionLink: (blockHash: string, index: number) => Promise<void>;
  forgetNodeValidation: (node: string, hash: string) => Promise<void>;
  schemaReport: () => Promise<SchemaReport>;
}
