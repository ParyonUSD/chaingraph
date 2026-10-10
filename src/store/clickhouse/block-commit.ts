/* eslint-disable max-classes-per-file, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, complexity, max-lines, functional/no-try-statement, functional/no-throw-statement, @typescript-eslint/parameter-properties, no-await-in-loop, functional/no-loop-statement, max-params, functional/no-let, @typescript-eslint/init-declarations, class-methods-use-this, @typescript-eslint/no-loop-func, prefer-destructuring, require-atomic-updates, functional/no-mixed-type, no-continue, @typescript-eslint/member-ordering, max-depth, camelcase */
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

/**
 * `target.push(...items)` without spreading into a call: a spread passes
 * every element as an argument, which overflows the stack for arrays of
 * ~100k+ elements (a 100k-tx block has 300k UTXO rows).
 */
export const appendAll = <T>(target: T[], items: readonly T[]): T[] => {
  for (const item of items) {
    target.push(item);
  }
  return target;
};

/** Smallest and largest of a non-empty list, without spreading into a call. */
export const minMax = (values: readonly number[]): [number, number] => {
  if (values.length === 0) {
    throw new RangeError('minMax of an empty list.');
  }
  let low = Infinity;
  let high = -Infinity;
  for (const value of values) {
    if (value < low) low = value;
    if (value > high) high = value;
  }
  return [low, high];
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
}

/** Postgres's saveBlock result semantics. */
export interface SaveBlockResult {
  attemptedSavedTransactions: ChaingraphTransaction[];
  transactionCacheMisses: number;
  /**
   * Only on a parked save (child-before-parent: answered while its commit is
   * `incomplete`): resolves once the block is committed, rejects if the
   * commit fails.
   */
  committed?: Promise<void>;
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
    appendAll(
      rows,
      utxoRowsForTransition({
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

/**
 * Pure: the `pending_spend` rows of a commit's unresolved inputs: one per
 * node whose UTXO −1 waits on the input (`pendingUtxo`), or one for node 0
 * when none does (bulk mode, or a tx no accepting node transitions). The
 * nodes are indexed by `spender:inputIndex`, so this is linear in the
 * inputs plus the pending UTXO items (it was a scan of `pendingUtxo` per
 * input: 100k unresolved spends blocked the event loop for about a minute).
 */
export const pendingSpendRows = (
  pendingInputs: readonly {
    input: { outpointTransactionHash: string; outpointIndex: number };
    inputIndex: number;
    transaction: { hash: string };
  }[],
  pendingUtxo: readonly { node: number; spender: string; inputIndex: number }[],
  sign: -1 | 1
): PendingSpendRow[] => {
  const nodesBySpend = new Map<string, number[]>();
  for (const item of pendingUtxo) {
    const key = `${item.spender}:${item.inputIndex}`;
    const nodes = nodesBySpend.get(key);
    if (nodes === undefined) {
      nodesBySpend.set(key, [item.node]);
    } else {
      nodes.push(item.node);
    }
  }
  const rows: PendingSpendRow[] = [];
  for (const { input, inputIndex, transaction } of pendingInputs) {
    const nodes = nodesBySpend.get(`${transaction.hash}:${inputIndex}`) ?? [0];
    for (const node of nodes) {
      rows.push({
        nodeInternalId: node,
        outpointIndex: input.outpointIndex,
        outpointTransactionHash: input.outpointTransactionHash,
        sign,
        spenderInputIndex: inputIndex,
        spenderTransactionHash: transaction.hash,
      });
    }
  }
  return rows;
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

/** One `saveBlock` call. */
export interface BlockSaveRequest {
  block: ChaingraphBlock;
  nodeAcceptances: readonly NodeAcceptance[];
  isSavedTransaction: (hash: string) => boolean;
}

/** One request of a batch and how its call is answered. */
interface BatchItem {
  request: BlockSaveRequest;
  /** Resolves the call early when the batch parks (`incomplete`). */
  onParked: (result: SaveBlockResult) => void;
}

/**
 * Consecutive `saveBlock` calls for the same node set, saved as ONE commit
 * (docs/clickhouse-port/wp6b-write-path.md): one `commit_log` intent and
 * committed row and one insert per table for all of its blocks. The batch
 * has one store operation, registered when the batch was created; a block
 * is appended only while no other operation has been registered on these
 * nodes since (so the call order the agent sees is unchanged), and only
 * until the batch starts.
 *
 * Transaction ids and outputs are pinned when a block is appended (as a
 * single save pinned them when it started), and ids are resolved by an
 * "id phase" scheduled at once (I/O only, no wait on any operation), so
 * other operations never wait on a batch that has not started.
 */
export class BlockBatch {
  readonly items: BatchItem[] = [];

  /** Sum of the blocks' `sizeBytes`. */
  bytes = 0;

  /** `open` (accepts appends), then `started`. */
  state: 'open' | 'started' = 'open';

  /** Ids of every transaction of the batch (own promises or other owners'). */
  readonly idOf = new Map<string, Promise<bigint> | bigint>();

  /** Transactions this batch pinned, with their pending ids. */
  readonly pendingIds = new Map<string, Deferred<bigint>>();

  /** Owned hashes found stored by an id phase, with the writing commit. */
  readonly stored = new Map<string, { internalId: bigint; seq: bigint }>();

  /** Operations whose rows this batch reads. */
  readonly dependencies = new Set<StoreOperation>();

  /** Block hashes in the batch. */
  readonly blockHashes = new Set<string>();

  /** Owned hashes waiting for an id phase. */
  unresolved: string[] = [];

  private phases: Promise<void>[] = [];

  private phaseScheduled = false;

  idError: unknown;

  constructor(
    readonly operation: StoreOperation,
    /** Prepare (pin) only when the batch runs: its block is saved elsewhere now. */
    readonly deferPrepare = false
  ) {}

  /** Schedule an id phase for the hashes pinned so far (one per event-loop turn). */
  scheduleIdPhase(run: (hashes: string[]) => Promise<void>) {
    if (this.phaseScheduled) return;
    this.phaseScheduled = true;
    const phase = new Promise<void>((resolve) => {
      setImmediate(resolve);
    }).then(async () => {
      this.phaseScheduled = false;
      const hashes = this.unresolved;
      this.unresolved = [];
      if (hashes.length > 0) await run(hashes);
    });
    this.phases.push(
      phase.catch((error: unknown) => {
        this.idError ??= error;
      })
    );
  }

  /** Resolves once every id phase scheduled so far has finished. */
  async idsSettled(): Promise<void> {
    while (this.phases.length > 0) {
      const phases = this.phases;
      this.phases = [];
      await Promise.all(phases);
    }
  }
}

export class BlockCommitter {
  /**
   * Block hash → the live operation saving it. A concurrent save of the same
   * block (the agent saves the genesis block once per node) waits for it,
   * then finds the stored block (one `block` row, as Postgres's ON CONFLICT).
   */
  private readonly inFlight = new Map<string, StoreOperation>();

  constructor(private readonly context: WriterContext) {}

  /** Whether another live operation is saving `hash` now. */
  isInFlight(hash: string) {
    const other = this.inFlight.get(hash);
    return other !== undefined && !other.finished;
  }

  /**
   * Add a request to a batch: pin its transactions and register its outputs
   * now (synchronously, in call order), and schedule an id phase.
   */
  append(batch: BlockBatch, item: BatchItem) {
    batch.items.push(item);
    batch.bytes += item.request.block.sizeBytes;
    batch.blockHashes.add(item.request.block.hash);
    if (!batch.deferPrepare) this.prepare(batch, item.request.block);
  }

  /** Pin `block`'s transactions and outputs for the batch (wp5a-core §4). */
  private prepare(batch: BlockBatch, block: ChaingraphBlock) {
    const { context } = this;
    const { operation } = batch;
    this.inFlight.set(block.hash, operation);
    const owned: ChaingraphTransaction[] = [];
    block.transactions.forEach((transaction) => {
      if (batch.idOf.has(transaction.hash)) return;
      const known = context.transactions.lookup(transaction.hash);
      if (known !== undefined) {
        batch.idOf.set(transaction.hash, known.internalId);
        if (known.owner !== undefined && known.owner !== operation) {
          batch.dependencies.add(known.owner);
        }
        return;
      }
      const id = deferred<bigint>();
      id.promise.catch(() => undefined);
      batch.pendingIds.set(transaction.hash, id);
      batch.idOf.set(transaction.hash, id.promise);
      context.transactions.pin(transaction.hash, operation, id.promise);
      batch.unresolved.push(transaction.hash);
      owned.push(transaction);
    });
    context.outputs.register(
      operation,
      owned.map((transaction) => ({
        hash: transaction.hash,
        internalId: batch.idOf.get(transaction.hash) as Promise<bigint>,
        outputs: transaction.outputs,
      }))
    );
    batch.scheduleIdPhase(async (hashes) => this.idPhase(batch, hashes));
  }

  /**
   * Resolve the ids of `hashes` (pinned by the batch): stored ones keep
   * their id (and the batch depends on their commit if it is open), new ones
   * get fresh ids. I/O only: never waits on another operation.
   */
  private async idPhase(batch: BlockBatch, hashes: readonly string[]) {
    const { context } = this;
    try {
      const stored = await lookupStoredTransactions(context, hashes);
      const newHashes = hashes.filter((hash) => !stored.has(hash));
      const newIds: bigint[] = [];
      if (newHashes.length > 0) {
        for (const segment of await context.ids.allocate(
          'transaction',
          newHashes.length
        )) {
          for (let id = segment.start; id < segment.end; id += 1n) {
            newIds.push(id);
          }
        }
      }
      stored.forEach((row, hash) => {
        batch.stored.set(hash, row);
        const owner = context.operationOfSeq(row.seq);
        if (owner !== undefined && owner !== batch.operation) {
          batch.dependencies.add(owner);
        }
        batch.pendingIds.get(hash)?.resolve(row.internalId);
      });
      newHashes.forEach((hash, index) => {
        batch.pendingIds.get(hash)!.resolve(newIds[index]!);
      });
    } catch (error) {
      hashes.forEach((hash) => {
        batch.pendingIds.get(hash)?.reject(error);
      });
      throw error;
    }
  }

  /**
   * Save every request of `batch` as one commit; resolves the per-request
   * results in request order. If the commit parks (child-before-parent),
   * every request is answered early through its `onParked`.
   */
  async save(batch: BlockBatch): Promise<SaveBlockResult[]> {
    const { context } = this;
    const { operation } = batch;
    let commit: OpenCommit | undefined;
    const empty = () =>
      batch.items.map(() => ({
        attemptedSavedTransactions: [],
        transactionCacheMisses: 0,
      }));
    try {
      if (batch.deferPrepare) {
        for (const item of batch.items) {
          const { hash } = item.request.block;
          for (;;) {
            const other = this.inFlight.get(hash);
            if (other === undefined || other === operation || other.finished)
              break;
            await operation.whileWaiting(
              other.committed.catch(() => undefined)
            );
          }
          this.prepare(batch, item.request.block);
        }
      }
      const results = await this.run(batch, (opened) => {
        commit = opened;
      });
      context.outputs.release(operation, true);
      await context.transactions.release(operation, true);
      return results;
    } catch (error) {
      if (error instanceof SimulatedCrash) {
        operation.markFailed(error);
        batch.pendingIds.forEach((id) => {
          id.reject(error);
        });
        throw error;
      }
      if (error instanceof AbandonedError) {
        /*
         * Shutdown: abort, and report the blocks as handled so the agent's
         * block buffer drains; nothing of them is committed, so the next
         * start restores the chain without them and downloads them again.
         */
        if (commit !== undefined) {
          await context.commitLog
            .markAborted(commit.seq, String(error))
            .catch(() => undefined);
        }
        operation.markFailed(error);
        batch.pendingIds.forEach((id) => {
          id.reject(error);
        });
        context.outputs.release(operation, false);
        await context.transactions.release(operation, false);
        return empty();
      }
      context.mempool.markStale(operation);
      if (commit !== undefined) {
        await context.commitLog
          .markAborted(commit.seq, `saveBlock failed: ${String(error)}`)
          .catch(() => undefined);
      }
      operation.markFailed(error);
      batch.pendingIds.forEach((id) => {
        id.reject(error);
      });
      context.outputs.release(operation, false);
      await context.transactions.release(operation, false);
      throw error;
    } finally {
      batch.blockHashes.forEach((hash) => {
        if (this.inFlight.get(hash) === operation) this.inFlight.delete(hash);
      });
    }
  }

  private async run(
    batch: BlockBatch,
    onCommit: (commit: OpenCommit) => void
  ): Promise<SaveBlockResult[]> {
    const { context } = this;
    const { operation, dependencies } = batch;
    const tipMode = context.mode() === 'tip';
    const blocks = batch.items.map((item) => item.request.block);

    /* 1. Ids (pinned at append; id phases resolve them). */
    batch.scheduleIdPhase(async (hashes) => this.idPhase(batch, hashes));
    const storedBlocksQuery = this.lookupStoredBlocks(
      blocks.map((block) => block.hash)
    );
    storedBlocksQuery.catch(() => undefined);
    await batch.idsSettled();
    // eslint-disable-next-line @typescript-eslint/no-throw-literal
    if (batch.idError !== undefined) throw batch.idError;
    const storedBlocks = await storedBlocksQuery;
    const uniqueHashes = [...batch.idOf.keys()];
    const resolvedIds = await Promise.all(
      uniqueHashes.map(async (hash) => batch.idOf.get(hash)!)
    );
    const idByHash = new Map(
      uniqueHashes.map((hash, index) => [hash, resolvedIds[index]!])
    );
    /** Hashes this batch inserts (owned and not stored), with the first item. */
    const insertedBy = new Map<string, number>();
    blocks.forEach((block, itemIndex) => {
      block.transactions.forEach((transaction) => {
        if (
          batch.pendingIds.has(transaction.hash) &&
          !batch.stored.has(transaction.hash) &&
          !insertedBy.has(transaction.hash)
        ) {
          insertedBy.set(transaction.hash, itemIndex);
        }
      });
    });
    const results: SaveBlockResult[] = batch.items.map(
      ({ request }, itemIndex) => {
        const attemptedSavedTransactions = request.block.transactions.filter(
          (transaction) => !request.isSavedTransaction(transaction.hash)
        );
        return {
          attemptedSavedTransactions,
          transactionCacheMisses: attemptedSavedTransactions.filter(
            (transaction) => insertedBy.get(transaction.hash) !== itemIndex
          ).length,
        };
      }
    );
    const newTransactions: ChaingraphTransaction[] = [];
    {
      const added = new Set<string>();
      blocks.forEach((block) => {
        block.transactions.forEach((transaction) => {
          if (
            insertedBy.has(transaction.hash) &&
            !added.has(transaction.hash)
          ) {
            added.add(transaction.hash);
            newTransactions.push(transaction);
          }
        });
      });
    }
    storedBlocks.forEach((row) => {
      const owner = context.operationOfSeq(BigInt(row.commit_seq));
      if (owner !== undefined && owner !== operation) dependencies.add(owner);
    });

    /*
     * 2. Per-node decisions need every earlier operation's rows for these
     * nodes (tip mode: UTXO transitions; any mode: re-accepting a stored block).
     */
    const requestedNodes = [
      ...new Set(
        batch.items.flatMap(({ request }) =>
          request.nodeAcceptances.map((item) => item.nodeInternalId)
        )
      ),
    ].sort((a, b) => a - b);
    const anyBlockExists = blocks.some((block) => storedBlocks.has(block.hash));
    const hooks = context.mempoolHooks?.();
    const needsEarlierRows =
      tipMode ||
      anyBlockExists ||
      requestedNodes.some((node) => context.mempool.mayHaveMempool(node));
    let waitedForEarlier = false;
    const waitForEarlier = async () => {
      if (waitedForEarlier || !needsEarlierRows) return;
      waitedForEarlier = true;
      await waitForPredecessorRows(operation);
      if (hooks !== undefined) {
        await freshen(hooks, context.mempool, operation, requestedNodes);
      }
    };
    /*
     * A batch of new blocks only writes its node-agnostic rows (output,
     * input, transaction, block, block_transaction) before it waits for the
     * earlier operations of its nodes: those rows decide nothing from
     * per-node state, and the commit is open (holding its nodes'
     * watermarks) until the per-node rows follow. This overlaps the batch
     * with e.g. a re-org of its nodes (wp6b-write-path.md §3). A batch
     * re-saving a stored block decides its node scope from stored state, so
     * it waits first, as before.
     */
    const writeAgnosticFirst = !anyBlockExists;
    if (!writeAgnosticFirst) await waitForEarlier();
    const newBlockCount = blocks.filter(
      (block) => !storedBlocks.has(block.hash)
    ).length;
    const newBlockIds: bigint[] = [];
    if (newBlockCount > 0) {
      for (const segment of await context.ids.allocate(
        'block',
        newBlockCount
      )) {
        for (let id = segment.start; id < segment.end; id += 1n) {
          newBlockIds.push(id);
        }
      }
    }
    interface BlockPlan {
      block: ChaingraphBlock;
      acceptanceByNode: Map<number, NodeAcceptance>;
      blockExists: boolean;
      blockInternalId: bigint;
      acceptingNodes: number[];
      missingLinks: number[];
    }
    const plans: BlockPlan[] = [];
    let nextNewBlock = 0;
    for (const { request } of batch.items) {
      const { block } = request;
      const acceptanceByNode = new Map<number, NodeAcceptance>();
      request.nodeAcceptances.forEach((acceptance) => {
        acceptanceByNode.set(acceptance.nodeInternalId, acceptance);
      });
      const nodes = [...acceptanceByNode.keys()].sort((a, b) => a - b);
      const storedBlock = storedBlocks.get(block.hash);
      const blockExists = storedBlock !== undefined;
      let blockInternalId: bigint;
      if (storedBlock === undefined) {
        blockInternalId = newBlockIds[nextNewBlock]!;
        nextNewBlock += 1;
      } else {
        blockInternalId = BigInt(storedBlock.internal_id);
      }
      const liveNodes = blockExists
        ? await this.nodesAccepting(
            blockInternalId,
            nodes,
            dependencies,
            operation
          )
        : new Set<number>();
      plans.push({
        acceptanceByNode,
        acceptingNodes: nodes.filter((node) => !liveNodes.has(node)),
        block,
        blockExists,
        blockInternalId,
        /*
         * Re-saving a stored block (incomplete-block repair) re-inserts the
         * `block_transaction` links that are missing, as Postgres's
         * ON CONFLICT DO NOTHING insert does.
         */
        missingLinks: blockExists
          ? await this.missingLinks(
              blockInternalId,
              block,
              operation,
              dependencies
            )
          : [],
      });
    }
    if (
      plans.every(
        (plan) =>
          plan.blockExists &&
          plan.acceptingNodes.length === 0 &&
          plan.missingLinks.length === 0
      )
    ) {
      // Postgres: every insert hits ON CONFLICT DO NOTHING
      operation.markDone();
      return results;
    }
    const acceptingNodes = [
      ...new Set(plans.flatMap((plan) => plan.acceptingNodes)),
    ].sort((a, b) => a - b);
    /** Per node, the batch's blocks it accepts now, in batch order. */
    const plansOfNode = new Map<number, BlockPlan[]>(
      acceptingNodes.map((node) => [
        node,
        plans.filter((plan) => plan.acceptingNodes.includes(node)),
      ])
    );

    /*
     * 3. Resolve spent outputs the node-agnostic rows need: every input of a
     * new transaction (input rows) and of a new block (generated value).
     */
    const resolved = new Map<string, ResolvedSpend>();
    const wanted = new Map<string, { hash: string; index: number }>();
    /** Per block: outpoints its block row needs (generated value). */
    const wantedOfPlan = new Map<BlockPlan, string[]>();
    const want = (
      transaction: ChaingraphTransaction,
      keys: string[] | undefined
    ) => {
      if (transaction.isCoinbase) return;
      transaction.inputs.forEach((input) => {
        if (input.outpointTransactionHash === coinbaseHash) return;
        const key = outpointKey(
          input.outpointTransactionHash,
          input.outpointIndex
        );
        keys?.push(key);
        if (!wanted.has(key)) {
          wanted.set(key, {
            hash: input.outpointTransactionHash,
            index: input.outpointIndex,
          });
        }
      });
    };
    plans.forEach((plan) => {
      const keys: string[] = [];
      plan.block.transactions.forEach((transaction) => {
        if (!plan.blockExists) want(transaction, keys);
        else if (insertedBy.has(transaction.hash)) want(transaction, undefined);
      });
      wantedOfPlan.set(plan, keys);
    });
    const resolveWanted = async () => {
      const missing = new Map(
        [...wanted].filter(([key]) => !resolved.has(key))
      );
      (await this.resolveSpends(missing, operation)).forEach((spend, key) => {
        resolved.set(key, spend);
        if (spend.owner !== undefined && spend.owner !== operation) {
          dependencies.add(spend.owner);
        }
      });
    };
    await resolveWanted();
    const resolveSpent = (hash: string, index: number) =>
      resolved.get(outpointKey(hash, index))?.output;
    const agnosticUnresolved = new Set(
      [...wanted.keys()].filter((key) => !resolved.has(key))
    );

    /* 4. The commit. */
    const nodeScope = acceptingNodes;
    const dependsOn = [...dependencies]
      .filter((dependency) => dependency.state === 'committed')
      .map((dependency) => dependency.seq)
      .filter((seq): seq is bigint => seq !== undefined);
    const commit = await context.commitLog.beginCommit({
      blockHashHex: blocks[blocks.length - 1]!.hash,
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
    /*
     * Inserts of one phase go out concurrently: independent rows of the same
     * open commit (none is visible before it commits), so their order does
     * not matter to readers or to recovery; only the phases are ordered
     * (intent, node-agnostic rows, per-node rows, fill, committed).
     */
    const agnosticWrites: Promise<void>[] = [];
    agnosticWrites.push(
      insert(
        'output',
        columnsOf('output'),
        encodeOutputRows(newTransactions, newContext)
      )
    );
    agnosticWrites.push(
      insert(
        'input',
        columnsOf('input'),
        encodeInputRows(newTransactions, newContext, resolveSpent)
      )
    );
    agnosticWrites.push(
      insert(
        'transaction',
        columnsOf('transaction'),
        encodeTransactionRows(newTransactions, newContext)
      )
    );
    const blockRowNow = (plan: BlockPlan) =>
      !plan.blockExists &&
      !wantedOfPlan.get(plan)!.some((key) => agnosticUnresolved.has(key));
    agnosticWrites.push(
      insert(
        'block',
        columnsOf('block'),
        encodeBlockRows(
          plans.filter(blockRowNow).map((plan) => ({
            block: plan.block,
            generatedValueSatoshis: this.generatedValue(
              plan.block,
              resolveSpent
            ),
            internalId: plan.blockInternalId,
          })),
          commit.seq
        )
      )
    );
    {
      const writer = new RowBinaryWriter();
      plans.forEach((plan) => {
        const indexes = plan.blockExists
          ? plan.missingLinks
          : plan.block.transactions.map((_, index) => index);
        indexes.forEach((index) => {
          const transaction = plan.block.transactions[index]!;
          writer
            .uint64(plan.blockInternalId)
            .uint32(index)
            .uint64(idByHash.get(transaction.hash)!)
            .fixedString32(transaction.hash)
            .uint64(commit.seq)
            .endRow();
        });
      });
      agnosticWrites.push(
        insert('block_transaction', columnsOf('block_transaction'), {
          data: writer.finish(),
          rowCount: writer.rowCount,
        })
      );
    }

    await Promise.all(agnosticWrites);

    /* 5. Per-node decisions: after every earlier operation of the nodes. */
    await waitForEarlier();
    const acceptedBefore = new Map<number, Set<string>>(
      acceptingNodes.map((node) => [node, new Set<string>()])
    );
    if (tipMode && acceptingNodes.length > 0) {
      const preExisting = uniqueHashes.filter((hash) => !insertedBy.has(hash));
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
     * whose creator this batch makes accepted (wp5a-mempool.md).
     */
    const batchTxByHash = new Map<string, ChaingraphTransaction>();
    blocks.forEach((block) => {
      block.transactions.forEach((transaction) => {
        if (!batchTxByHash.has(transaction.hash)) {
          batchTxByHash.set(transaction.hash, transaction);
        }
      });
    });
    const creatorOutputs = (hash: string) => {
      const transaction = batchTxByHash.get(hash);
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
      const inclusions = plansOfNode
        .get(node)!
        .flatMap((plan) =>
          inclusionsOf(plan.block, plan.acceptanceByNode.get(node)!.acceptedAt)
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
     * In tip mode every transaction that becomes accepted needs its spent
     * outputs (UTXO −1 rows), also in stored blocks.
     */
    if (tipMode) {
      const transitionTxs = new Set<string>();
      acceptingNodes.forEach((node) => {
        plansOfNode.get(node)!.forEach((plan) => {
          plan.block.transactions.forEach((transaction) => {
            if (!acceptedBefore.get(node)!.has(transaction.hash)) {
              transitionTxs.add(transaction.hash);
            }
          });
        });
      });
      plans.forEach((plan) => {
        if (!plan.blockExists) return;
        plan.block.transactions.forEach((transaction) => {
          if (transitionTxs.has(transaction.hash)) want(transaction, undefined);
        });
      });
      await resolveWanted();
    }
    const unresolved = [...wanted.keys()].filter((key) => !resolved.has(key));

    const nodeBlockRows: NodeBlockRow[] = [];
    const txAcceptanceRows: TxAcceptanceRow[] = [];
    plans.forEach((plan) => {
      plan.acceptingNodes.forEach((node) => {
        const { acceptedAt } = plan.acceptanceByNode.get(node)!;
        nodeBlockRows.push({
          acceptedAt,
          blockHash: plan.block.hash,
          blockInternalId: plan.blockInternalId,
          height: plan.block.height,
          nodeInternalId: node,
          sign: 1,
          version: commit.seq,
        });
        plan.block.transactions.forEach((transaction) => {
          txAcceptanceRows.push({
            acceptedAt,
            blockInternalId: plan.blockInternalId,
            height: plan.block.height,
            nodeInternalId: node,
            sign: 1,
            transactionHash: transaction.hash,
            transactionInternalId: idByHash.get(transaction.hash)!,
            version: commit.seq,
          });
        });
      });
    });
    const nodeWrites: Promise<void>[] = [];
    nodeWrites.push(
      insert(
        'node_block',
        acceptanceColumns.node_block,
        encodeNodeBlockRows(nodeBlockRows, commit.seq)
      )
    );
    nodeWrites.push(
      insert(
        'tx_acceptance',
        acceptanceColumns.tx_acceptance,
        encodeTxAcceptanceRows(txAcceptanceRows, commit.seq)
      )
    );

    const utxoRows: UtxoRow[] = [];
    if (hooks !== undefined && mempoolChanges.length > 0) {
      const mempoolRows = changeRows(mempoolChanges, historyIds);
      /*
       * mempool-originated UTXO rows are written in every mode (the bulk
       * horizon build only covers transactions in bulk-period blocks)
       */
      appendAll(utxoRows, mempoolRows.utxo);
      nodeWrites.push(
        hooks.insertChangeRows(
          commit,
          'block',
          { ...mempoolRows, utxo: [] },
          'm',
          rowCounts
        )
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
        /*
         * One transition per (node, tx) over all the node's blocks in the
         * batch: a tx in two of them (or stored before) counts once.
         */
        const transactions: {
          transaction: ChaingraphTransaction;
          internalId: bigint;
        }[] = [];
        plansOfNode.get(node)!.forEach((plan) => {
          plan.block.transactions.forEach((transaction) => {
            transactions.push({
              internalId: idByHash.get(transaction.hash)!,
              transaction,
            });
          });
        });
        const delta = blockUtxoDelta({
          acceptedBefore: acceptedBefore.get(node)!,
          nodeInternalId: node,
          resolveSpent,
          transactions,
        });
        appendAll(utxoRows, delta.rows);
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
    nodeWrites.push(
      insert('utxo', utxoColumns, {
        data: encodedUtxo.utxo,
        rowCount: encodedUtxo.rowCount,
      })
    );
    nodeWrites.push(
      insert('utxo_by_script', utxoByScriptColumns, {
        data: encodedUtxo.utxoByScript,
        rowCount: encodedUtxo.rowCount,
      })
    );

    await Promise.all(nodeWrites);

    /* 6. Child-before-parent: pending spends, filled under this seq. */
    if (unresolved.length > 0) {
      const pendingInputs = newTransactions.flatMap((transaction) =>
        transaction.isCoinbase
          ? []
          : transaction.inputs
              .map((input, inputIndex) => ({ input, inputIndex, transaction }))
              .filter(
                ({ input }) =>
                  !resolved.has(
                    outpointKey(
                      input.outpointTransactionHash,
                      input.outpointIndex
                    )
                  )
              )
      );
      const pendingRows = (sign: -1 | 1): PendingSpendRow[] =>
        pendingSpendRows(pendingInputs, pendingUtxo, sign);
      await insert(
        'pending_spend',
        acceptanceColumns.pending_spend,
        encodePendingSpendRows(pendingRows(1), commit.seq)
      );
      await context.commitLog.markIncomplete(commit.seq);
      await context.fault('incomplete', { kind: 'block', seq: commit.seq });
      /*
       * Every row but the fill is written and the commit is `incomplete`
       * (it holds its nodes' watermarks): the callers may report the blocks
       * as parked now, so the agent's bounded block buffer keeps downloading
       * (the parent may still be queued behind them). The commit completes
       * in the background; if the process stops first, recovery aborts it
       * and the next start downloads the blocks again.
       */
      operation.yieldLane();
      batch.items.forEach((item, index) => {
        item.onParked(results[index]!);
      });
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
      await insert(
        'block',
        columnsOf('block'),
        encodeBlockRows(
          plans
            .filter((plan) => !plan.blockExists && !blockRowNow(plan))
            .map((plan) => ({
              block: plan.block,
              generatedValueSatoshis: this.generatedValue(
                plan.block,
                resolveSpent,
                true
              ),
              internalId: plan.blockInternalId,
            })),
          commit.seq
        ),
        'f0'
      );
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
    return results;
  }

  /** Stored (valid-commit) blocks by hash: the first row per hash. */
  private async lookupStoredBlocks(
    hashes: readonly string[]
  ): Promise<Map<string, { internal_id: string; commit_seq: string }>> {
    const found = new Map<
      string,
      { internal_id: string; commit_seq: string }
    >();
    for (const chunk of chunked(
      [...new Set(hashes)],
      this.context.lookupChunkSize
    )) {
      const rows = await this.context.client.query<{
        hash_hex: string;
        internal_id: string;
        commit_seq: string;
      }>(
        `SELECT lower(hex(hash)) AS hash_hex, internal_id, commit_seq FROM block
         WHERE hash IN (SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({hashes:Array(String)}) AS h))
           AND ${validCommitSql()}
         ORDER BY commit_seq`,
        { fence: this.context.fence(), hashes: chunk }
      );
      rows.forEach((row) => {
        if (!found.has(row.hash_hex)) {
          found.set(row.hash_hex, {
            commit_seq: row.commit_seq,
            internal_id: row.internal_id,
          });
        }
      });
    }
    return found;
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
       * There is no re-arm: saves register their outputs when they are
       * called (WP6b), before any slot or batch-lane wait, and a save that
       * waits for another save of the same block registers nothing new (that
       * save registered the block's outputs). So a parent queued for a slot
       * or behind a running batch is already visible here. (WP5c re-armed
       * the timeout while ANY call was queued for a slot, which includes
       * children resuming after their own wait: under a cap they completed
       * one at a time and the agent's initial sync did not finish; re-arming
       * on waits that themselves wait behind this child is a deadlock.)
       */
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<boolean>((resolve) => {
        timer = setTimeout(() => {
          resolve(false);
        }, context.pendingSpendTimeoutMs);
      });
      await Promise.race([
        all,
        timeout,
        ...(context.abandon === undefined ? [] : [context.abandon.promise]),
      ]).finally(() => {
        clearTimeout(timer);
      });
      return new Map(result);
    } finally {
      waits.forEach(({ cancel }) => {
        cancel();
      });
    }
  }
}
