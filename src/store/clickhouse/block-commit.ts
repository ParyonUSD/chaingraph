/* eslint-disable max-classes-per-file, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, complexity, max-lines, functional/no-try-statement, functional/no-throw-statement, @typescript-eslint/parameter-properties, no-await-in-loop, functional/no-loop-statement, max-params, functional/no-let, @typescript-eslint/init-declarations, class-methods-use-this, @typescript-eslint/no-loop-func, prefer-destructuring, require-atomic-updates, functional/no-mixed-type, no-continue */
// cspell:ignore clickhouse dedup unhex seqs varint
/**
 * `saveBlock` for the ClickHouse store (WP5a-core): one block = one commit
 * (plan §3.1), written in dependency order, per accepting node:
 * `node_block` +1, `tx_acceptance` +1 and (above the bulk horizon) the UTXO
 * transition rows. Child-before-parent spends go through the pending-spend
 * path of WP4 (`incomplete`, fill rows under the child's seq).
 * Design and invariants: docs/clickhouse-port/wp5a-core.md.
 */
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';
import type { AcceptedInclusion } from '../mempool-graph.js';
import { outpoint } from '../mempool-graph.js';
import type { NodeAcceptance } from '../types.js';

import type { ClickHouseClient } from './client.js';
import type { CommitLog, OpenCommit } from './commit-log.js';
import type { IdAllocator } from './id-allocator.js';
import type { MempoolCommitter } from './mempool-commit.js';
import { changeRows } from './mempool-commit.js';
import type { MempoolState, NodeMempoolChange } from './mempool-state.js';
import { isEmptyChange } from './mempool-state.js';
import type {
  AbandonSignal,
  Deferred,
  NodeBlockRow,
  OperationRegistry,
  PendingSpendRow,
  StoreMode,
  StoreOperation,
  TxAcceptanceRow,
} from './node-state.js';
import {
  AbandonedError,
  acceptanceColumns,
  awaitDependencies,
  deferred,
  encodeNodeBlockRows,
  encodePendingSpendRows,
  encodeTxAcceptanceRows,
  waitForPredecessorRows,
} from './node-state.js';
import { RowBinaryWriter } from './row-binary.js';
import type { ResolvedInput, SpentOutput } from './row-encoders.js';
import {
  encodeBlockRows,
  encodeBlockTransactionRows,
  encodeInputRows,
  encodeOutputRows,
  encodeResolvedInputRows,
  encodeTransactionRows,
  rowBinaryTableColumns,
} from './row-encoders.js';
import type {
  OutputRegistry,
  RegisteredOutput,
  UtxoOutput,
  UtxoRow,
} from './utxo.js';
import {
  encodeUtxoRows,
  outpointKey,
  utxoByScriptColumns,
  utxoColumns,
  utxoFromChaingraphOutput,
  utxoRowsForTransition,
  validCommitSql,
} from './utxo.js';

/** A test-only crash: the store stops as if the process died (no abort). */
export class SimulatedCrash extends Error {}

export class PendingSpendTimeoutError extends Error {}

/**
 * Called between the steps of a commit (tests inject crashes here). Step
 * names: `intent`, then the table just written (`output`, `input`, …), then
 * `incomplete`, `fill`, `rows-written`, `committed`.
 */
export type FaultInjector = (
  step: string,
  context: { kind: string; seq: bigint | undefined }
) => Promise<void> | void;

const coinbaseHash = '00'.repeat(32);

/** Written on `input` rows whose spent output never became known. */
const unknownSpentOutput: SpentOutput = {
  lockingBytecode: '',
  valueSatoshis: 0n,
};

export const chunked = <T>(items: readonly T[], size: number): T[][] => {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
};

/**
 * Transactions known to this writer: pinned while the operation writing them
 * is live (so concurrent saves reuse one id), then remembered (bounded).
 */
export class TransactionRegistry {
  private readonly pinned = new Map<
    string,
    { internalId: Promise<bigint>; owner: StoreOperation }
  >();

  private readonly recent = new Map<string, bigint>();

  private readonly hashesByOwner = new Map<StoreOperation, string[]>();

  constructor(private readonly recentCapacity = 1_000_000) {}

  lookup(hash: string):
    | {
        internalId: Promise<bigint> | bigint;
        owner: StoreOperation | undefined;
      }
    | undefined {
    const pinned = this.pinned.get(hash);
    if (pinned !== undefined) {
      return pinned;
    }
    const known = this.recent.get(hash);
    return known === undefined
      ? undefined
      : { internalId: known, owner: undefined };
  }

  pin(hash: string, owner: StoreOperation, internalId: Promise<bigint>) {
    this.pinned.set(hash, { internalId, owner });
    const hashes = this.hashesByOwner.get(owner) ?? [];
    hashes.push(hash);
    this.hashesByOwner.set(owner, hashes);
  }

  remember(hash: string, internalId: bigint) {
    this.recent.delete(hash);
    this.recent.set(hash, internalId);
    while (this.recent.size > this.recentCapacity) {
      this.recent.delete(this.recent.keys().next().value as string);
    }
  }

  /** Unpin; ids of a committed owner are remembered. */
  async release(owner: StoreOperation, committed: boolean) {
    const hashes = this.hashesByOwner.get(owner) ?? [];
    this.hashesByOwner.delete(owner);
    await Promise.all(
      hashes.map(async (hash) => {
        const entry = this.pinned.get(hash);
        if (entry?.owner !== owner) {
          return;
        }
        this.pinned.delete(hash);
        if (committed) {
          this.remember(hash, await entry.internalId);
        }
      })
    );
  }
}

/**
 * `ensureFresh` (rebuild a stale node mempool after its earlier operations
 * settle) without holding an in-flight slot when it has to wait.
 */
export const freshen = async (
  hooks: Pick<MempoolCommitter, 'ensureFresh'>,
  mempool: Pick<MempoolState, 'stale'>,
  operation: StoreOperation,
  nodes: readonly number[]
) =>
  nodes.some((node) => mempool.stale.has(node))
    ? operation.whileWaiting(hooks.ensureFresh(operation, nodes))
    : hooks.ensureFresh(operation, nodes);

/** What the block committer needs from the store. */
export interface WriterContext {
  client: ClickHouseClient;
  commitLog: CommitLog;
  ids: IdAllocator;
  outputs: OutputRegistry<StoreOperation>;
  transactions: TransactionRegistry;
  operations: OperationRegistry;
  mempool: MempoolState;
  mode: () => StoreMode;
  /** The dense epoch-fence array (validCommitSql's `{fence}`). */
  fence: () => bigint[];
  /** The owner of an open commit of this writer. */
  operationOfSeq: (seq: bigint) => StoreOperation | undefined;
  onCommitted: (operation: StoreOperation) => void;
  fault: FaultInjector;
  lookupChunkSize: number;
  pendingSpendTimeoutMs: number;
  /** The mempool committer (block acceptance cleans the node's mempool). */
  mempoolHooks?: () => MempoolCommitter | undefined;
  /** Shutdown: abandon waits and uncommitted work. */
  abandon?: AbandonSignal;
  /** Saves queued for an in-flight slot (0 when unbounded). */
  savesQueued?: () => number;
}

/** Postgres's saveBlock result semantics. */
export interface SaveBlockResult {
  attemptedSavedTransactions: ChaingraphTransaction[];
  transactionCacheMisses: number;
}

interface StoredOutputRow {
  hash: string;
  output_index: number;
  transaction_internal_id: string;
  value_satoshis: string;
  locking_bytecode: string;
  token_category: string;
  fungible_token_amount: string | null;
  nonfungible_token_capability: 'minting' | 'mutable' | 'none' | null;
  nonfungible_token_commitment: string | null;
  commit_seq: string;
}

const zeroCategory = '00'.repeat(32);

export const storedOutputToUtxo = (row: StoredOutputRow): UtxoOutput => ({
  fungibleTokenAmount:
    row.fungible_token_amount === null
      ? undefined
      : BigInt(row.fungible_token_amount),
  lockingBytecode: row.locking_bytecode,
  nonfungibleTokenCapability: row.nonfungible_token_capability ?? undefined,
  nonfungibleTokenCommitment: row.nonfungible_token_commitment ?? undefined,
  outputIndex: Number(row.output_index),
  tokenCategory:
    row.token_category === zeroCategory ? undefined : row.token_category,
  transactionHash: row.hash,
  transactionInternalId: BigInt(row.transaction_internal_id),
  valueSatoshis: BigInt(row.value_satoshis),
});

/**
 * Read stored (valid-commit) outputs by outpoint. Includes rows of this
 * writer's open commits (the caller takes a dependency on them).
 */
export const lookupStoredOutputs = async (
  context: Pick<WriterContext, 'client' | 'fence' | 'lookupChunkSize'>,
  outpoints: readonly { hash: string; index: number }[]
): Promise<{ output: UtxoOutput; seq: bigint }[]> => {
  const results: { output: UtxoOutput; seq: bigint }[] = [];
  for (const chunk of chunked(outpoints, context.lookupChunkSize)) {
    const rows = await context.client.query<StoredOutputRow>(
      `SELECT lower(hex(transaction_hash)) AS hash, output_index, transaction_internal_id, value_satoshis,
         lower(hex(locking_bytecode)) AS locking_bytecode, lower(hex(token_category)) AS token_category,
         fungible_token_amount, nonfungible_token_capability,
         lower(hex(nonfungible_token_commitment)) AS nonfungible_token_commitment, commit_seq
       FROM output
       WHERE transaction_hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
         AND (transaction_hash, output_index) IN (
           SELECT toFixedString(unhex(p.1), 32), p.2
           FROM (SELECT arrayJoin(arrayZip({hashes:Array(String)}, {indexes:Array(UInt32)})) AS p))
         AND ${validCommitSql()}`,
      {
        fence: context.fence(),
        hashes: chunk.map((item) => item.hash),
        indexes: chunk.map((item) => item.index),
      }
    );
    rows.forEach((row) => {
      results.push({
        output: storedOutputToUtxo(row),
        seq: BigInt(row.commit_seq),
      });
    });
  }
  return results;
};

/** Stored (valid-commit) transactions by hash: id and the commit that wrote it. */
export const lookupStoredTransactions = async (
  context: Pick<WriterContext, 'client' | 'fence' | 'lookupChunkSize'>,
  hashes: readonly string[]
): Promise<Map<string, { internalId: bigint; seq: bigint }>> => {
  const found = new Map<string, { internalId: bigint; seq: bigint }>();
  for (const chunk of chunked(hashes, context.lookupChunkSize)) {
    const rows = await context.client.query<{
      hash_hex: string;
      internal_id: string;
      commit_seq: string;
    }>(
      `SELECT lower(hex(hash)) AS hash_hex, internal_id, commit_seq FROM transaction
       WHERE hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
         AND ${validCommitSql()}
       ORDER BY commit_seq`,
      { fence: context.fence(), hashes: chunk }
    );
    rows.forEach((row) => {
      if (!found.has(row.hash_hex)) {
        found.set(row.hash_hex, {
          internalId: BigInt(row.internal_id),
          seq: BigInt(row.commit_seq),
        });
      }
    });
  }
  return found;
};

/**
 * Pure: split a block's transactions into those this save inserts and those
 * already stored, and assign ids: `known` ids win; the rest take `newIds` in
 * block order. Returns the ids in block order.
 */
export const assignTransactionIds = (
  hashes: readonly string[],
  known: ReadonlyMap<string, bigint>,
  newIds: readonly bigint[]
): { internalIds: bigint[]; inserted: Set<string> } => {
  const inserted = new Set<string>();
  const assigned = new Map<string, bigint>();
  let next = 0;
  const internalIds = hashes.map((hash) => {
    const existing = known.get(hash) ?? assigned.get(hash);
    if (existing !== undefined) {
      return existing;
    }
    const id = newIds[next];
    if (id === undefined) {
      throw new RangeError(`Not enough new ids for ${hash}.`);
    }
    next += 1;
    assigned.set(hash, id);
    inserted.add(hash);
    return id;
  });
  if (next !== newIds.length) {
    throw new RangeError(`Assigned ${next} of ${newIds.length} new ids.`);
  }
  return { inserted, internalIds };
};

/**
 * Pure: the UTXO rows of one block for one accepting node (plan §2.3): every
 * transaction not already accepted by the node becomes accepted (+1 outputs,
 * −1 spent outputs). Spends whose output is not yet known are returned in
 * `pending` (their −1 rows are written by the fill step).
 */
export const blockUtxoDelta = ({
  nodeInternalId,
  transactions,
  acceptedBefore,
  resolveSpent,
}: {
  nodeInternalId: number;
  transactions: readonly {
    transaction: ChaingraphTransaction;
    internalId: bigint;
  }[];
  acceptedBefore: ReadonlySet<string>;
  resolveSpent: (hash: string, index: number) => UtxoOutput | undefined;
}): {
  rows: UtxoRow[];
  pending: {
    spender: string;
    inputIndex: number;
    hash: string;
    index: number;
  }[];
  transitions: string[];
} => {
  const rows: UtxoRow[] = [];
  const pending: {
    spender: string;
    inputIndex: number;
    hash: string;
    index: number;
  }[] = [];
  const transitions: string[] = [];
  const seen = new Set<string>();
  transactions.forEach(({ transaction, internalId }) => {
    if (acceptedBefore.has(transaction.hash) || seen.has(transaction.hash)) {
      return;
    }
    seen.add(transaction.hash);
    transitions.push(transaction.hash);
    const spentOutputs: UtxoOutput[] = [];
    if (!transaction.isCoinbase) {
      transaction.inputs.forEach((input, inputIndex) => {
        const spent = resolveSpent(
          input.outpointTransactionHash,
          input.outpointIndex
        );
        if (spent === undefined) {
          pending.push({
            hash: input.outpointTransactionHash,
            index: input.outpointIndex,
            inputIndex,
            spender: transaction.hash,
          });
        } else {
          spentOutputs.push(spent);
        }
      });
    }
    rows.push(
      ...utxoRowsForTransition({
        delta: 1,
        nodeInternalId,
        outputs: transaction.outputs.map((output, outputIndex) => ({
          fungibleTokenAmount: output.fungibleTokenAmount,
          lockingBytecode: output.lockingBytecode,
          nonfungibleTokenCapability: output.nonfungibleTokenCapability,
          nonfungibleTokenCommitment: output.nonfungibleTokenCommitment,
          outputIndex,
          tokenCategory: output.tokenCategory,
          transactionHash: transaction.hash,
          transactionInternalId: internalId,
          valueSatoshis: output.valueSatoshis,
        })),
        spentOutputs,
      })
    );
  });
  return { pending, rows, transitions };
};

interface ResolvedSpend {
  output: UtxoOutput;
  owner: StoreOperation | undefined;
}

const spentOutputOf = (output: UtxoOutput): SpentOutput => ({
  fungibleTokenAmount: output.fungibleTokenAmount,
  lockingBytecode: output.lockingBytecode,
  nonfungibleTokenCapability: output.nonfungibleTokenCapability,
  nonfungibleTokenCommitment: output.nonfungibleTokenCommitment,
  tokenCategory: output.tokenCategory,
  valueSatoshis: output.valueSatoshis,
});

const inclusionsOf = (
  block: ChaingraphBlock,
  acceptedAt: Date | null
): AcceptedInclusion[] =>
  block.transactions.map((transaction) => ({
    acceptedAt,
    spends: transaction.inputs.map((input) =>
      outpoint(input.outpointTransactionHash, input.outpointIndex)
    ),
    tx: transaction.hash,
  }));

export class BlockCommitter {
  /**
   * Block hash → the live operation saving it. A concurrent save of the same
   * block (the agent saves the genesis block once per node) waits for it,
   * then finds the stored block (one `block` row, as Postgres's ON CONFLICT).
   */
  private readonly inFlight = new Map<string, StoreOperation>();

  constructor(private readonly context: WriterContext) {}

  /**
   * Save `block` for `nodeAcceptances` under `operation` (registered by the
   * caller at call time for the accepting nodes).
   */
  async save(
    operation: StoreOperation,
    {
      block,
      nodeAcceptances,
      isSavedTransaction,
    }: {
      block: ChaingraphBlock;
      nodeAcceptances: readonly NodeAcceptance[];
      isSavedTransaction: (hash: string) => boolean;
    },
    onParked?: (result: SaveBlockResult) => void
  ): Promise<SaveBlockResult> {
    const { context } = this;
    let commit: OpenCommit | undefined;
    const pendingIds = new Map<string, Deferred<bigint>>();
    for (;;) {
      const other = this.inFlight.get(block.hash);
      if (other === undefined || other === operation || other.finished) break;
      await operation.whileWaiting(other.committed.catch(() => undefined));
    }
    this.inFlight.set(block.hash, operation);
    try {
      const result = await this.run(
        operation,
        { block, isSavedTransaction, nodeAcceptances },
        pendingIds,
        (opened) => {
          commit = opened;
        },
        onParked
      );
      context.outputs.release(operation, true);
      await context.transactions.release(operation, true);
      return result;
    } catch (error) {
      if (error instanceof SimulatedCrash) {
        operation.markFailed(error);
        pendingIds.forEach((id) => {
          id.reject(error);
        });
        throw error;
      }
      if (error instanceof AbandonedError) {
        /*
         * Shutdown: abort, and report the block as handled so the agent's
         * block buffer drains; nothing of it is committed, so the next start
         * restores the chain without it and downloads it again.
         */
        if (commit !== undefined) {
          await context.commitLog
            .markAborted(commit.seq, String(error))
            .catch(() => undefined);
        }
        operation.markFailed(error);
        pendingIds.forEach((id) => {
          id.reject(error);
        });
        context.outputs.release(operation, false);
        await context.transactions.release(operation, false);
        return { attemptedSavedTransactions: [], transactionCacheMisses: 0 };
      }
      context.mempool.markStale(operation);
      if (commit !== undefined) {
        await context.commitLog
          .markAborted(commit.seq, `saveBlock failed: ${String(error)}`)
          .catch(() => undefined);
      }
      operation.markFailed(error);
      pendingIds.forEach((id) => {
        id.reject(error);
      });
      context.outputs.release(operation, false);
      await context.transactions.release(operation, false);
      throw error;
    } finally {
      if (this.inFlight.get(block.hash) === operation) {
        this.inFlight.delete(block.hash);
      }
    }
  }

  private async run(
    operation: StoreOperation,
    {
      block,
      nodeAcceptances,
      isSavedTransaction,
    }: {
      block: ChaingraphBlock;
      nodeAcceptances: readonly NodeAcceptance[];
      isSavedTransaction: (hash: string) => boolean;
    },
    pendingIds: Map<string, Deferred<bigint>>,
    onCommit: (commit: OpenCommit) => void,
    onParked?: (result: SaveBlockResult) => void
  ): Promise<SaveBlockResult> {
    const { context } = this;
    const tipMode = context.mode() === 'tip';
    const dependencies = new Set<StoreOperation>();
    const acceptanceByNode = new Map<number, NodeAcceptance>();
    nodeAcceptances.forEach((acceptance) => {
      acceptanceByNode.set(acceptance.nodeInternalId, acceptance);
    });

    /*
     * 1. Transaction ids. Pin every transaction not known to this writer
     * (synchronously, so a concurrent save of the same tx reuses the id),
     * and pin their outputs so concurrent children can resolve spends.
     */
    const idOf = new Map<string, Promise<bigint> | bigint>();
    const ownedHashes = new Set<string>();
    block.transactions.forEach((transaction) => {
      if (idOf.has(transaction.hash)) {
        return;
      }
      const known = context.transactions.lookup(transaction.hash);
      if (known !== undefined) {
        idOf.set(transaction.hash, known.internalId);
        if (known.owner !== undefined && known.owner !== operation) {
          dependencies.add(known.owner);
        }
        return;
      }
      const id = deferred<bigint>();
      id.promise.catch(() => undefined);
      pendingIds.set(transaction.hash, id);
      idOf.set(transaction.hash, id.promise);
      context.transactions.pin(transaction.hash, operation, id.promise);
      ownedHashes.add(transaction.hash);
    });
    context.outputs.register(
      operation,
      block.transactions
        .filter((transaction) => ownedHashes.has(transaction.hash))
        .map((transaction) => ({
          hash: transaction.hash,
          internalId: idOf.get(transaction.hash) as Promise<bigint>,
          outputs: transaction.outputs,
        }))
    );

    const blockLookup = context.client.query<{
      internal_id: string;
      commit_seq: string;
    }>(
      `SELECT internal_id, commit_seq FROM block
       WHERE hash = toFixedString(unhex({hash:String}), 32) AND ${validCommitSql()}
       ORDER BY commit_seq LIMIT 1`,
      { fence: context.fence(), hash: block.hash }
    );
    const stored = await lookupStoredTransactions(context, [
      ...pendingIds.keys(),
    ]);
    const storedBlock = (await blockLookup)[0];
    const newHashes = [...pendingIds.keys()].filter(
      (hash) => !stored.has(hash)
    );
    const newIds =
      newHashes.length === 0
        ? []
        : (await context.ids.allocate('transaction', newHashes.length)).flatMap(
            (segment) => {
              const ids: bigint[] = [];
              for (let id = segment.start; id < segment.end; id += 1n) {
                ids.push(id);
              }
              return ids;
            }
          );
    const knownIds = new Map<string, bigint>();
    stored.forEach((row, hash) => {
      knownIds.set(hash, row.internalId);
      const owner = context.operationOfSeq(row.seq);
      if (owner !== undefined && owner !== operation) {
        dependencies.add(owner);
      }
    });
    const { inserted } = assignTransactionIds(newHashes, knownIds, newIds);
    newHashes.forEach((hash, index) => {
      pendingIds.get(hash)!.resolve(newIds[index]!);
    });
    stored.forEach((row, hash) => {
      pendingIds.get(hash)?.resolve(row.internalId);
    });
    const internalIds = await Promise.all(
      block.transactions.map(async (transaction) => idOf.get(transaction.hash)!)
    );
    const idByHash = new Map(
      block.transactions.map((transaction, index) => [
        transaction.hash,
        internalIds[index]!,
      ])
    );
    const newTransactions = block.transactions.filter((transaction) =>
      inserted.has(transaction.hash)
    );
    const attemptedSavedTransactions = block.transactions.filter(
      (transaction) => !isSavedTransaction(transaction.hash)
    );
    const transactionCacheMisses = attemptedSavedTransactions.filter(
      (transaction) => !inserted.has(transaction.hash)
    ).length;
    const blockExists = storedBlock !== undefined;
    if (storedBlock !== undefined) {
      const owner = context.operationOfSeq(BigInt(storedBlock.commit_seq));
      if (owner !== undefined && owner !== operation) {
        dependencies.add(owner);
      }
    }

    /*
     * 2. Per-node decisions need every earlier operation's rows for these
     * nodes (tip mode: UTXO transitions; any mode: re-accepting a stored block).
     */
    const requestedNodes = [...acceptanceByNode.keys()].sort((a, b) => a - b);
    const hooks = context.mempoolHooks?.();
    if (
      tipMode ||
      blockExists ||
      requestedNodes.some((node) => context.mempool.mayHaveMempool(node))
    ) {
      await waitForPredecessorRows(operation);
      if (hooks !== undefined) {
        await freshen(hooks, context.mempool, operation, requestedNodes);
      }
    }
    const blockInternalId = blockExists
      ? BigInt(storedBlock.internal_id)
      : await context.ids.allocateOne('block');
    const liveNodes = blockExists
      ? await this.nodesAccepting(
          blockInternalId,
          requestedNodes,
          dependencies,
          operation
        )
      : new Set<number>();
    const acceptingNodes = requestedNodes.filter(
      (node) => !liveNodes.has(node)
    );
    /*
     * Re-saving a stored block (incomplete-block repair) re-inserts the
     * `block_transaction` links that are missing, as Postgres's
     * ON CONFLICT DO NOTHING insert does.
     */
    const missingLinks = blockExists
      ? await this.missingLinks(blockInternalId, block, operation, dependencies)
      : [];
    if (
      blockExists &&
      acceptingNodes.length === 0 &&
      missingLinks.length === 0
    ) {
      // Postgres: every insert hits ON CONFLICT DO NOTHING
      operation.markDone();
      return { attemptedSavedTransactions, transactionCacheMisses };
    }

    const acceptedBefore = new Map<number, Set<string>>(
      acceptingNodes.map((node) => [node, new Set<string>()])
    );
    if (tipMode && acceptingNodes.length > 0) {
      const preExisting = block.transactions
        .map((transaction) => transaction.hash)
        .filter((hash) => !inserted.has(hash));
      await this.loadAcceptedBefore(
        preExisting,
        acceptingNodes,
        acceptedBefore,
        dependencies,
        operation
      );
    }
    /*
     * 2b. Each accepting node's mempool cleanup, in this commit: confirmed,
     * conflicting and cascading entries, and outstanding spends of entries
     * whose creator this block makes accepted (wp5a-mempool.md).
     */
    const blockTxByHash = new Map(
      block.transactions.map((transaction) => [transaction.hash, transaction])
    );
    const creatorOutputs = (hash: string) => {
      const transaction = blockTxByHash.get(hash);
      return transaction === undefined
        ? undefined
        : transaction.outputs.map((output, index) =>
            utxoFromChaingraphOutput(hash, index, idByHash.get(hash)!, output)
          );
    };
    const mempoolChanges: NodeMempoolChange[] = [];
    for (const node of acceptingNodes) {
      context.mempool.modifiersOf(node, operation).forEach((modifier) => {
        dependencies.add(modifier);
      });
      if (hooks === undefined || context.mempool.isEmpty(node)) continue;
      const inclusions = inclusionsOf(
        block,
        acceptanceByNode.get(node)!.acceptedAt
      );
      const known = await hooks.knownOutputsForConfirmed(
        node,
        inclusions,
        operation,
        dependencies
      );
      const change = context.mempool.planBlockAcceptance(
        node,
        inclusions,
        creatorOutputs,
        (spent) => known.get(spent)
      );
      if (!isEmptyChange(change)) {
        mempoolChanges.push(change);
      }
    }
    hooks?.applyChanges(operation, mempoolChanges);
    const historyIds =
      hooks === undefined ? [] : await hooks.historyIds(mempoolChanges);

    /*
     * 3. Resolve spent outputs: for new transactions (input rows), for the
     * block row of a new block (generated value), and in tip mode for every
     * transaction that becomes accepted (UTXO −1 rows).
     */
    const transitionTxs = new Set<string>();
    if (tipMode) {
      acceptingNodes.forEach((node) => {
        block.transactions.forEach((transaction) => {
          if (!acceptedBefore.get(node)!.has(transaction.hash)) {
            transitionTxs.add(transaction.hash);
          }
        });
      });
    }
    const needsSpends = (transaction: ChaingraphTransaction) =>
      !transaction.isCoinbase &&
      (!blockExists ||
        inserted.has(transaction.hash) ||
        transitionTxs.has(transaction.hash));
    const wanted = new Map<string, { hash: string; index: number }>();
    block.transactions.filter(needsSpends).forEach((transaction) => {
      transaction.inputs.forEach((input) => {
        if (input.outpointTransactionHash === coinbaseHash) {
          return;
        }
        wanted.set(
          outpointKey(input.outpointTransactionHash, input.outpointIndex),
          { hash: input.outpointTransactionHash, index: input.outpointIndex }
        );
      });
    });
    const resolved = await this.resolveSpends(wanted, operation);
    const unresolved = [...wanted.keys()].filter((key) => !resolved.has(key));
    resolved.forEach((spend) => {
      if (spend.owner !== undefined && spend.owner !== operation) {
        dependencies.add(spend.owner);
      }
    });
    const resolveSpent = (hash: string, index: number) =>
      resolved.get(outpointKey(hash, index))?.output;

    /* 4. The commit. */
    const nodeScope = acceptingNodes;
    const dependsOn = [...dependencies]
      .filter((dependency) => dependency.state === 'committed')
      .map((dependency) => dependency.seq)
      .filter((seq): seq is bigint => seq !== undefined);
    const commit = await context.commitLog.beginCommit({
      blockHashHex: block.hash,
      dependsOn,
      kind: 'block',
      nodeScope,
    });
    onCommit(commit);
    operation.seq = commit.seq;
    await context.fault('intent', { kind: 'block', seq: commit.seq });
    const rowCounts: { [table: string]: number } = {};
    const insert = async (
      table: string,
      columns: readonly string[],
      encoded: { data: Uint8Array; rowCount: number },
      chunk: number | string = 0
    ) => {
      if (encoded.rowCount === 0) {
        return;
      }
      await context.client.insertRowBinary(table, columns, encoded.data, {
        deduplicationToken: commit.token(table, chunk),
      });
      rowCounts[table] = (rowCounts[table] ?? 0) + encoded.rowCount;
      await context.fault(String(table), { kind: 'block', seq: commit.seq });
    };
    const columnsOf = (table: keyof typeof rowBinaryTableColumns) =>
      rowBinaryTableColumns[table].map(([name]) => name);

    const newContext = {
      commitSeq: commit.seq,
      transactionInternalIds: newTransactions.map(
        (transaction) => idByHash.get(transaction.hash)!
      ),
    };
    await insert(
      'output',
      columnsOf('output'),
      encodeOutputRows(newTransactions, newContext)
    );
    const inputRows = encodeInputRows(
      newTransactions,
      newContext,
      resolveSpent
    );
    await insert('input', columnsOf('input'), inputRows);
    await insert(
      'transaction',
      columnsOf('transaction'),
      encodeTransactionRows(newTransactions, newContext)
    );
    const allContext = {
      commitSeq: commit.seq,
      transactionInternalIds: internalIds,
    };
    const blockRowNow = !blockExists && unresolved.length === 0;
    if (blockRowNow) {
      await insert(
        'block',
        columnsOf('block'),
        encodeBlockRows(
          [
            {
              block,
              generatedValueSatoshis: this.generatedValue(block, resolveSpent),
              internalId: blockInternalId,
            },
          ],
          commit.seq
        )
      );
    }
    if (!blockExists) {
      await insert(
        'block_transaction',
        columnsOf('block_transaction'),
        encodeBlockTransactionRows(block, blockInternalId, allContext)
      );
    } else if (missingLinks.length > 0) {
      const writer = new RowBinaryWriter(missingLinks.length * 64);
      missingLinks.forEach((index) => {
        writer
          .uint64(blockInternalId)
          .uint32(index)
          .uint64(internalIds[index]!)
          .fixedString32(block.transactions[index]!.hash)
          .uint64(commit.seq)
          .endRow();
      });
      await insert('block_transaction', columnsOf('block_transaction'), {
        data: writer.finish(),
        rowCount: writer.rowCount,
      });
    }

    const nodeBlockRows: NodeBlockRow[] = acceptingNodes.map((node) => ({
      acceptedAt: acceptanceByNode.get(node)!.acceptedAt,
      blockHash: block.hash,
      blockInternalId,
      height: block.height,
      nodeInternalId: node,
      sign: 1,
      version: commit.seq,
    }));
    await insert(
      'node_block',
      acceptanceColumns.node_block,
      encodeNodeBlockRows(nodeBlockRows, commit.seq)
    );
    const txAcceptanceRows: TxAcceptanceRow[] = acceptingNodes.flatMap((node) =>
      block.transactions.map((transaction) => ({
        acceptedAt: acceptanceByNode.get(node)!.acceptedAt,
        blockInternalId,
        height: block.height,
        nodeInternalId: node,
        sign: 1 as const,
        transactionHash: transaction.hash,
        transactionInternalId: idByHash.get(transaction.hash)!,
        version: commit.seq,
      }))
    );
    await insert(
      'tx_acceptance',
      acceptanceColumns.tx_acceptance,
      encodeTxAcceptanceRows(txAcceptanceRows, commit.seq)
    );

    const utxoRows: UtxoRow[] = [];
    if (hooks !== undefined && mempoolChanges.length > 0) {
      const mempoolRows = changeRows(mempoolChanges, historyIds);
      /*
       * mempool-originated UTXO rows are written in every mode (the bulk
       * horizon build only covers transactions in bulk-period blocks)
       */
      utxoRows.push(...mempoolRows.utxo);
      await hooks.insertChangeRows(
        commit,
        'block',
        { ...mempoolRows, utxo: [] },
        'm',
        rowCounts
      );
    }
    const pendingUtxo: {
      node: number;
      spender: string;
      inputIndex: number;
      key: string;
    }[] = [];
    if (tipMode) {
      acceptingNodes.forEach((node) => {
        const delta = blockUtxoDelta({
          acceptedBefore: acceptedBefore.get(node)!,
          nodeInternalId: node,
          resolveSpent,
          transactions: block.transactions.map((transaction) => ({
            internalId: idByHash.get(transaction.hash)!,
            transaction,
          })),
        });
        utxoRows.push(...delta.rows);
        delta.pending.forEach((item) => {
          pendingUtxo.push({
            inputIndex: item.inputIndex,
            key: outpointKey(item.hash, item.index),
            node,
            spender: item.spender,
          });
        });
      });
    }
    const encodedUtxo = encodeUtxoRows(utxoRows, commit.seq);
    await insert('utxo', utxoColumns, {
      data: encodedUtxo.utxo,
      rowCount: encodedUtxo.rowCount,
    });
    await insert('utxo_by_script', utxoByScriptColumns, {
      data: encodedUtxo.utxoByScript,
      rowCount: encodedUtxo.rowCount,
    });

    /* 5. Child-before-parent: pending spends, filled under this seq. */
    if (unresolved.length > 0) {
      const pendingInputs = newTransactions.flatMap((transaction) =>
        transaction.inputs
          .map((input, inputIndex) => ({ input, inputIndex, transaction }))
          .filter(
            ({ input }) =>
              !transaction.isCoinbase &&
              !resolved.has(
                outpointKey(input.outpointTransactionHash, input.outpointIndex)
              )
          )
      );
      const pendingRows = (sign: -1 | 1): PendingSpendRow[] =>
        pendingInputs.flatMap(({ input, inputIndex, transaction }) => {
          const nodes = pendingUtxo
            .filter(
              (item) =>
                item.spender === transaction.hash &&
                item.inputIndex === inputIndex
            )
            .map((item) => item.node);
          return (nodes.length === 0 ? [0] : nodes).map((node) => ({
            nodeInternalId: node,
            outpointIndex: input.outpointIndex,
            outpointTransactionHash: input.outpointTransactionHash,
            sign,
            spenderInputIndex: inputIndex,
            spenderTransactionHash: transaction.hash,
          }));
        });
      await insert(
        'pending_spend',
        acceptanceColumns.pending_spend,
        encodePendingSpendRows(pendingRows(1), commit.seq)
      );
      await context.commitLog.markIncomplete(commit.seq);
      await context.fault('incomplete', { kind: 'block', seq: commit.seq });
      /*
       * Every row but the fill is written and the commit is `incomplete`
       * (it holds its nodes' watermarks): the caller may report the block as
       * handled now, so the agent's bounded block buffer keeps downloading
       * (the parent may still be queued behind this block). The commit
       * completes in the background; if the process stops first, recovery
       * aborts it and the next start downloads the block again.
       */
      onParked?.({ attemptedSavedTransactions, transactionCacheMisses });
      const filled = await operation.whileWaiting(
        this.waitForPending(unresolved)
      );
      filled.forEach((spend, key) => {
        resolved.set(key, spend);
        if (spend.owner !== undefined && spend.owner !== operation) {
          dependencies.add(spend.owner);
        }
      });
      const fillInputs: ResolvedInput[] = pendingInputs.map(
        ({ input, inputIndex, transaction }) => {
          const spent = resolveSpent(
            input.outpointTransactionHash,
            input.outpointIndex
          );
          return {
            input,
            inputIndex,
            spent:
              spent === undefined ? unknownSpentOutput : spentOutputOf(spent),
            transactionHash: transaction.hash,
            transactionInternalId: idByHash.get(transaction.hash)!,
          };
        }
      );
      await insert(
        'input',
        columnsOf('input'),
        encodeResolvedInputRows(fillInputs, commit.seq),
        'f0'
      );
      const fillUtxo = pendingUtxo
        .filter((item) => resolved.has(item.key))
        .map(
          (item): UtxoRow => ({
            nodeInternalId: item.node,
            output: resolved.get(item.key)!.output,
            sign: -1,
          })
        );
      const encodedFill = encodeUtxoRows(fillUtxo, commit.seq);
      await insert(
        'utxo',
        utxoColumns,
        { data: encodedFill.utxo, rowCount: encodedFill.rowCount },
        'f0'
      );
      await insert(
        'utxo_by_script',
        utxoByScriptColumns,
        { data: encodedFill.utxoByScript, rowCount: encodedFill.rowCount },
        'f0'
      );
      if (!blockExists) {
        await insert(
          'block',
          columnsOf('block'),
          encodeBlockRows(
            [
              {
                block,
                generatedValueSatoshis: this.generatedValue(
                  block,
                  resolveSpent,
                  true
                ),
                internalId: blockInternalId,
              },
            ],
            commit.seq
          ),
          'f0'
        );
      }
      await insert(
        'pending_spend',
        acceptanceColumns.pending_spend,
        encodePendingSpendRows(pendingRows(-1), commit.seq),
        'f0'
      );
      await context.fault('fill', { kind: 'block', seq: commit.seq });
    }
    operation.markRowsWritten();
    await context.fault('rows-written', { kind: 'block', seq: commit.seq });

    /* 6. Commit once every commit this one read from is committed. */
    const settled =
      context.abandon?.race(awaitDependencies(dependencies)) ??
      awaitDependencies(dependencies);
    await (dependencies.size > 0 ? operation.whileWaiting(settled) : settled);
    await context.commitLog.markCommitted(commit.seq, rowCounts);
    operation.markCommitted();
    context.onCommitted(operation);
    await context.fault('committed', { kind: 'block', seq: commit.seq });
    return { attemptedSavedTransactions, transactionCacheMisses };
  }

  /** Σ outputs − Σ spent outputs (coinbase inputs spend nothing). */
  private generatedValue(
    block: ChaingraphBlock,
    resolveSpent: (hash: string, index: number) => UtxoOutput | undefined,
    unknownAsZero = false
  ) {
    return block.transactions.reduce((total, transaction) => {
      const outputs = transaction.outputs.reduce(
        (sum, output) => sum + output.valueSatoshis,
        0n
      );
      const spent = transaction.isCoinbase
        ? 0n
        : transaction.inputs.reduce((sum, input) => {
            const output = resolveSpent(
              input.outpointTransactionHash,
              input.outpointIndex
            );
            if (output === undefined && unknownAsZero) {
              return sum;
            }
            if (output === undefined) {
              throw new Error(
                `Spent output ${input.outpointTransactionHash}:${input.outpointIndex} is unknown.`
              );
            }
            return sum + output.valueSatoshis;
          }, 0n);
      return total + outputs - spent;
    }, 0n);
  }

  /** Indexes of `block`'s transactions with no valid `block_transaction` link. */
  private async missingLinks(
    blockInternalId: bigint,
    block: ChaingraphBlock,
    operation: StoreOperation,
    dependencies: Set<StoreOperation>
  ): Promise<number[]> {
    const rows = await this.context.client.query<{
      idx: number;
      seq: string;
    }>(
      `SELECT transaction_index AS idx, commit_seq AS seq FROM block_transaction
       WHERE block_internal_id = {block:UInt64} AND ${validCommitSql()}`,
      { block: blockInternalId, fence: this.context.fence() }
    );
    const linked = new Set<number>();
    rows.forEach((row) => {
      linked.add(Number(row.idx));
      const owner = this.context.operationOfSeq(BigInt(row.seq));
      if (owner !== undefined && owner !== operation) dependencies.add(owner);
    });
    return block.transactions
      .map((_, index) => index)
      .filter((index) => !linked.has(index));
  }

  /** Nodes already accepting a stored block (live `node_block` rows). */
  private async nodesAccepting(
    blockInternalId: bigint,
    nodes: readonly number[],
    dependencies: Set<StoreOperation>,
    operation: StoreOperation
  ) {
    const rows = await this.context.client.query<{
      node: number;
      seqs: string[];
    }>(
      `SELECT node_internal_id AS node, groupArray(commit_seq) AS seqs FROM node_block
       WHERE has({nodes:Array(UInt32)}, node_internal_id) AND block_internal_id = {block:UInt64}
         AND ${validCommitSql()}
       GROUP BY node_internal_id HAVING sum(sign) > 0`,
      { block: blockInternalId, fence: this.context.fence(), nodes }
    );
    rows.forEach((row) => {
      row.seqs.forEach((seq) => {
        const owner = this.context.operationOfSeq(BigInt(seq));
        if (owner !== undefined && owner !== operation) {
          dependencies.add(owner);
        }
      });
    });
    return new Set(rows.map((row) => Number(row.node)));
  }

  /**
   * Transactions already tx-accepted by each node (a live `tx_acceptance` row
   * for another block or the mempool): no UTXO transition for them.
   */
  private async loadAcceptedBefore(
    hashes: readonly string[],
    nodes: readonly number[],
    acceptedBefore: Map<number, Set<string>>,
    dependencies: Set<StoreOperation>,
    operation: StoreOperation
  ) {
    for (const chunk of chunked(hashes, this.context.lookupChunkSize)) {
      const rows = await this.context.client.query<{
        hash: string;
        node: number;
        seqs: string[];
      }>(
        `SELECT lower(hex(transaction_hash)) AS hash, node_internal_id AS node, arrayFlatten(groupArray(block_seqs)) AS seqs
         FROM (
           SELECT transaction_hash, node_internal_id, block_internal_id, groupArray(commit_seq) AS block_seqs
           FROM tx_acceptance
           WHERE transaction_hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
             AND has({nodes:Array(UInt32)}, node_internal_id)
             AND ${validCommitSql()}
           GROUP BY transaction_hash, node_internal_id, block_internal_id
           HAVING sum(sign) > 0)
         GROUP BY transaction_hash, node_internal_id`,
        { fence: this.context.fence(), hashes: chunk, nodes }
      );
      rows.forEach((row) => {
        acceptedBefore.get(Number(row.node))?.add(row.hash);
        row.seqs.forEach((seq) => {
          const owner = this.context.operationOfSeq(BigInt(seq));
          if (owner !== undefined && owner !== operation) {
            dependencies.add(owner);
          }
        });
      });
    }
  }

  /**
   * Resolve spent outputs: (a) the registry (recent and in-flight outputs),
   * (b) the store, (c) the registry again (a save may have registered while
   * the query ran). Unresolved outpoints are pending.
   */
  private async resolveSpends(
    wanted: ReadonlyMap<string, { hash: string; index: number }>,
    operation: StoreOperation
  ): Promise<Map<string, ResolvedSpend>> {
    const { context } = this;
    const resolved = new Map<string, ResolvedSpend>();
    const fromRegistry = async (keys: Iterable<string>) => {
      const misses: string[] = [];
      for (const key of keys) {
        const entry = context.outputs.lookup(key);
        if (entry === undefined) {
          misses.push(key);
        } else {
          resolved.set(key, await this.toResolved(entry, operation));
        }
      }
      return misses;
    };
    const misses = await fromRegistry(wanted.keys());
    if (misses.length > 0) {
      const stored = await lookupStoredOutputs(
        context,
        misses.map((key) => wanted.get(key)!)
      );
      stored.forEach(({ output, seq }) => {
        const key = outpointKey(output.transactionHash, output.outputIndex);
        if (resolved.has(key)) {
          return;
        }
        const owner = context.operationOfSeq(seq);
        resolved.set(key, { output, owner });
        if (owner === undefined) {
          context.outputs.remember(key, {
            internalId: Promise.resolve(output.transactionInternalId),
            output,
            owner: undefined,
          });
        }
      });
      await fromRegistry(misses.filter((key) => !resolved.has(key)));
    }
    return resolved;
  }

  private async toResolved(
    entry: RegisteredOutput<StoreOperation>,
    operation: StoreOperation
  ): Promise<ResolvedSpend> {
    const owner =
      entry.owner !== undefined &&
      entry.owner !== operation &&
      !entry.owner.finished
        ? entry.owner
        : undefined;
    return {
      output: {
        ...entry.output,
        transactionInternalId: await entry.internalId,
      },
      owner,
    };
  }

  /** Wait (bounded) until every pending outpoint is registered by a save. */
  private async waitForPending(
    keys: readonly string[]
  ): Promise<Map<string, ResolvedSpend>> {
    const { context } = this;
    /*
     * check and subscribe in one synchronous pass: a save that registered
     * the outpoint since `resolveSpends` is found here, a later one wakes us
     */
    const result = new Map<string, ResolvedSpend>();
    const waits = keys.map((key) => {
      const known = context.outputs.lookup(key);
      const wait =
        known === undefined
          ? { key, ...context.outputs.waitFor(key) }
          : { cancel: () => undefined, key, promise: Promise.resolve(known) };
      return {
        ...wait,
        promise: wait.promise.then(async (entry) => {
          result.set(key, {
            output: {
              ...entry.output,
              transactionInternalId: await entry.internalId,
            },
            owner: entry.owner?.finished === true ? undefined : entry.owner,
          });
        }),
      };
    });
    const all = Promise.all(waits.map(async ({ promise }) => promise)).then(
      () => true
    );
    try {
      /*
       * After the timeout the outputs still missing are taken as unknown
       * (a block spending outputs Chaingraph never sees: test chains, or a
       * parent that never arrives): their inputs are written with a
       * coinbase-like stand-in (value 0, no token, empty bytecode) and no
       * UTXO row, as Postgres stores such inputs (no output to join).
       * While saves are queued for an in-flight slot (one may be the
       * parent: it registers its outputs only once it holds a slot), the
       * timeout is re-armed, so the cap never turns a late parent into a
       * stand-in. Unbounded stores never queue: one timeout, as before.
       */
      for (;;) {
        let timer: ReturnType<typeof setTimeout> | undefined;
        const timeout = new Promise<boolean>((resolve) => {
          timer = setTimeout(() => {
            resolve(false);
          }, context.pendingSpendTimeoutMs);
        });
        const done = await Promise.race([
          all,
          timeout,
          ...(context.abandon === undefined ? [] : [context.abandon.promise]),
        ]).finally(() => {
          clearTimeout(timer);
        });
        if (done || (context.savesQueued?.() ?? 0) === 0) break;
      }
      return new Map(result);
    } finally {
      waits.forEach(({ cancel }) => {
        cancel();
      });
    }
  }
}
