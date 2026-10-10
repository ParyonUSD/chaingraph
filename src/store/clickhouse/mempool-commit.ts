/* eslint-disable max-lines, complexity, functional/no-try-statement, functional/no-throw-statement, functional/no-loop-statement, no-await-in-loop, functional/no-let, @typescript-eslint/init-declarations, max-params, @typescript-eslint/naming-convention, camelcase, @typescript-eslint/no-magic-numbers, @typescript-eslint/parameter-properties, @typescript-eslint/member-ordering, no-continue, no-negated-condition, prefer-destructuring, @typescript-eslint/no-invalid-void-type, prefer-const */
// cspell:ignore clickhouse unhex seqs milli
/**
 * Mempool commits of the ClickHouse store (WP5a-mempool).
 *
 * Every call that changes a node's mempool is ONE commit whose `node_scope`
 * is exactly the nodes whose facts it writes; every per-node row carries the
 * node in its key and `version` = its own `commit_seq`:
 * - `saveTransaction` / `recordValidation`: base facts of a new transaction
 *   (`output`, `input`, `transaction`), then per validating node either the
 *   addition (`node_transaction` +1, `tx_acceptance` block 0 +1, `utxo` +1
 *   per output / −1 per known spent output, `pending_spend` +1 per unknown
 *   one) with the replacement archives, or (already confirmed / conflicting
 *   with a confirmed transaction) a history row only;
 * - `sweep`: the repair scan (`archiveMempoolTransactionsAcceptedByBlocks`);
 * - `expire`: `archiveMempoolTransaction`.
 * Block and header acceptance write their node's cleanup inside their own
 * commit (`changeRows`, used by block-commit.ts and clickhouse-store.ts).
 *
 * A new transaction spending an unknown output is parked in the orphan pool
 * (no commit) until a save registers the output or the grace period ends;
 * mempool commits are never `incomplete` (WP4 §3).
 *
 * Design, end-state tables and the Postgres parity notes:
 * docs/clickhouse-port/wp5a-mempool.md.
 */
import type { ChaingraphTransaction } from '../../types/chaingraph.js';
import type {
  AcceptedInclusion,
  Outpoint,
  PlannedArchive,
  TxKey,
} from '../mempool-graph.js';
import { outpoint } from '../mempool-graph.js';
import type {
  ArchivedMempoolTransaction,
  ExpiringMempoolTransaction,
  NodeValidation,
} from '../types.js';

import type { WriterContext } from './block-commit.js';
import {
  chunked,
  lookupStoredOutputs,
  lookupStoredTransactions,
  SimulatedCrash,
  storedOutputToUtxo,
  utxoTables,
} from './block-commit.js';
import type { OpenCommit } from './commit-log.js';
import { segmentIds } from './id-allocator.js';
import type {
  LiveMempoolEntry,
  MempoolTxFacts,
  NodeMempoolChange,
  OrphanEntry,
} from './mempool-state.js';
import { isEmptyChange } from './mempool-state.js';
import type {
  Deferred,
  NodeRegistry,
  OperationKind,
  PendingSpendRow,
  StoreOperation,
  TxAcceptanceRow,
} from './node-state.js';
import {
  acceptanceColumns,
  awaitDependencies,
  deferred,
  encodePendingSpendRows,
  encodeTxAcceptanceRows,
  waitForPredecessorRows,
} from './node-state.js';
import { RowBinaryWriter } from './row-binary.js';
import type { SpentOutput } from './row-encoders.js';
import {
  encodeInputRows,
  encodeOutputRows,
  encodeTransactionRows,
  rowBinaryTableColumns,
} from './row-encoders.js';
import type { UtxoOutput, UtxoRow } from './utxo.js';
import {
  encodeUtxoRows,
  outpointKey,
  utxoByScriptColumns,
  utxoColumns,
  utxoFromChaingraphOutput,
  validCommitSql,
} from './utxo.js';

/* ---------------------------------------------------------------- rows */

export const mempoolColumns = {
  node_transaction: [
    'node_internal_id',
    'transaction_internal_id',
    'transaction_hash',
    'validated_at',
    'sign',
    'version',
    'commit_seq',
  ],
  node_transaction_history: [
    'node_internal_id',
    'transaction_internal_id',
    'internal_id',
    'validated_at',
    'replaced_at',
    'commit_seq',
  ],
} as const;

export interface NodeTransactionRow {
  nodeInternalId: number;
  transactionInternalId: bigint;
  transactionHash: string;
  validatedAt: Date | null;
  sign: -1 | 1;
}

export const encodeNodeTransactionRows = (
  rows: readonly NodeTransactionRow[],
  commitSeq: bigint
) => {
  const writer = new RowBinaryWriter(rows.length * 72);
  rows.forEach((row) => {
    writer
      .uint32(row.nodeInternalId)
      .uint64(row.transactionInternalId)
      .fixedString32(row.transactionHash)
      .nullable(row.validatedAt, (w, at) => w.dateTime64(at))
      .int8Sign(row.sign)
      .uint64(commitSeq)
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

export interface NodeTransactionHistoryRow {
  nodeInternalId: number;
  transactionInternalId: bigint;
  internalId: bigint;
  validatedAt: Date | null;
  replacedAt: Date | null;
}

export const encodeNodeTransactionHistoryRows = (
  rows: readonly NodeTransactionHistoryRow[],
  commitSeq: bigint
) => {
  const writer = new RowBinaryWriter(rows.length * 56);
  rows.forEach((row) => {
    writer
      .uint32(row.nodeInternalId)
      .uint64(row.transactionInternalId)
      .uint64(row.internalId)
      .nullable(row.validatedAt, (w, at) => w.dateTime64(at))
      .nullable(row.replacedAt, (w, at) => w.dateTime64(at))
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

/** The rows one commit writes for a set of node mempool changes. */
export interface MempoolRows {
  nodeTransaction: NodeTransactionRow[];
  history: NodeTransactionHistoryRow[];
  txAcceptance: TxAcceptanceRow[];
  pendingSpend: PendingSpendRow[];
  utxo: UtxoRow[];
}

/** History ids a change needs (one per archive, plus an immediate row). */
export const historyRowCount = (changes: readonly NodeMempoolChange[]) =>
  changes.reduce(
    (total, change) =>
      total + change.archives.length + (change.immediate === undefined ? 0 : 1),
    0
  );

const outpointParts = (spent: string) => {
  const at = spent.lastIndexOf(':');
  return { hash: spent.slice(0, at), index: Number(spent.slice(at + 1)) };
};

/**
 * Pure: the rows of `changes` (each for one node) under `version = seq`
 * (the encoders write the commit's seq into `version`); `historyIds` holds
 * one id per history row.
 *
 * | transition | node_transaction | history | tx_acceptance (block 0) | utxo | pending_spend |
 * | addition | +1 | – | +1 | +1 per output, −1 per known spend | +1 per unknown spend |
 * | confirmed (archive) | −1 | (validated_at, NULL) | −1 | – | −1 per unknown spend |
 * | replaced / conflict / descendant / expired | −1 | (validated_at, replaced_at) | −1 | −1 per output, +1 per written spend | −1 per unknown spend |
 * | resolution | – | – | – | −1 for the spent output | −1 |
 * | immediate (confirmed or conflicting on arrival) | – | (validated_at, replaced_at) | – | – | – |
 */
export const changeRows = (
  changes: readonly NodeMempoolChange[],
  historyIds: readonly bigint[],
  { utxo = true }: { utxo?: boolean } = {}
): MempoolRows => {
  const rows: MempoolRows = {
    history: [],
    nodeTransaction: [],
    pendingSpend: [],
    txAcceptance: [],
    utxo: [],
  };
  /** `utxo: false` (`CHAINGRAPH_CLICKHOUSE_UTXO=off`): `rows.utxo` stays empty. */
  const utxoRows = {
    push: (row: UtxoRow) => {
      if (utxo) rows.utxo.push(row);
    },
  };
  let nextId = 0;
  const takeId = () => {
    const id = historyIds[nextId];
    if (id === undefined) {
      throw new RangeError('Not enough node_transaction_history ids.');
    }
    nextId += 1;
    return id;
  };
  const pending = (
    node: number,
    spender: TxKey,
    spent: Outpoint,
    inputIndex: number,
    sign: -1 | 1
  ): PendingSpendRow => {
    const { hash, index } = outpointParts(spent);
    return {
      nodeInternalId: node,
      outpointIndex: index,
      outpointTransactionHash: hash,
      sign,
      spenderInputIndex: Math.max(inputIndex, 0),
      spenderTransactionHash: spender,
    };
  };
  changes.forEach((change) => {
    const { node } = change;
    const resolvedNow = new Set(
      change.resolutions.map(
        (resolution) => `${resolution.spender}|${resolution.outpoint}`
      )
    );
    change.resolutions.forEach((resolution) => {
      utxoRows.push({
        nodeInternalId: node,
        output: resolution.output,
        sign: -1,
      });
      rows.pendingSpend.push(
        pending(
          node,
          resolution.spender,
          resolution.outpoint,
          resolution.inputIndex,
          -1
        )
      );
    });
    if (change.immediate !== undefined) {
      const { facts, replacedAt, validatedAt } = change.immediate;
      rows.history.push({
        internalId: takeId(),
        nodeInternalId: node,
        replacedAt,
        transactionInternalId: facts.internalId,
        validatedAt,
      });
    }
    change.archives.forEach((archive) => {
      const { entry } = archive;
      rows.nodeTransaction.push({
        nodeInternalId: node,
        sign: -1,
        transactionHash: archive.tx,
        transactionInternalId: entry.internalId,
        validatedAt: entry.validatedAt,
      });
      rows.history.push({
        internalId: takeId(),
        nodeInternalId: node,
        replacedAt: archive.replacedAt,
        transactionInternalId: entry.internalId,
        validatedAt: entry.validatedAt,
      });
      rows.txAcceptance.push({
        acceptedAt: entry.validatedAt,
        blockInternalId: 0n,
        height: 0,
        nodeInternalId: node,
        sign: -1,
        transactionHash: archive.tx,
        transactionInternalId: entry.internalId,
        version: 0n,
      });
      entry.unresolved.forEach((spent) => {
        if (resolvedNow.has(`${archive.tx}|${spent}`)) return;
        rows.pendingSpend.push(
          pending(node, archive.tx, spent, entry.spends.indexOf(spent), -1)
        );
      });
      if (archive.cause === 'confirmed') {
        // still accepted (by the block): no UTXO transition
        return;
      }
      const { facts } = archive;
      facts.outputs.forEach((output) => {
        utxoRows.push({ nodeInternalId: node, output, sign: -1 });
      });
      entry.spends.forEach((spent) => {
        if (entry.unresolved.has(spent)) return;
        const output = facts.spent.get(spent);
        if (output === undefined) {
          throw new Error(
            `No facts for ${spent}, spent by archived mempool tx ${archive.tx}.`
          );
        }
        utxoRows.push({ nodeInternalId: node, output, sign: 1 });
      });
    });
    if (change.addition !== undefined) {
      const { facts, validatedAt } = change.addition;
      rows.nodeTransaction.push({
        nodeInternalId: node,
        sign: 1,
        transactionHash: facts.hash,
        transactionInternalId: facts.internalId,
        validatedAt,
      });
      rows.txAcceptance.push({
        acceptedAt: validatedAt,
        blockInternalId: 0n,
        height: 0,
        nodeInternalId: node,
        sign: 1,
        transactionHash: facts.hash,
        transactionInternalId: facts.internalId,
        version: 0n,
      });
      facts.outputs.forEach((output) => {
        utxoRows.push({ nodeInternalId: node, output, sign: 1 });
      });
      facts.spends.forEach((spent, inputIndex) => {
        const output = facts.spent.get(spent);
        if (output === undefined) {
          rows.pendingSpend.push(
            pending(node, facts.hash, spent, inputIndex, 1)
          );
        } else {
          utxoRows.push({ nodeInternalId: node, output, sign: -1 });
        }
      });
    }
  });
  if (nextId !== historyIds.length) {
    throw new RangeError(
      `Used ${nextId} of ${historyIds.length} node_transaction_history ids.`
    );
  }
  return rows;
};

/** Facts of a transaction from its Chaingraph form and resolved spends. */
export const factsFromTransaction = (
  transaction: ChaingraphTransaction,
  internalId: bigint,
  spent: ReadonlyMap<string, UtxoOutput>
): MempoolTxFacts => {
  const spends = transaction.isCoinbase
    ? []
    : transaction.inputs.map((input) =>
        outpoint(input.outpointTransactionHash, input.outpointIndex)
      );
  const resolved = new Map<Outpoint, UtxoOutput>();
  spends.forEach((key) => {
    const output = spent.get(key);
    if (output !== undefined) resolved.set(key, output);
  });
  return {
    hash: transaction.hash,
    internalId,
    outputs: transaction.outputs.map((output, index) =>
      utxoFromChaingraphOutput(transaction.hash, index, internalId, output)
    ),
    spends,
    spent: resolved,
  };
};

/** Coinbase-like stand-in written on `input` rows whose spent output is unknown. */
const unknownSpentOutput: SpentOutput = {
  lockingBytecode: '',
  valueSatoshis: 0n,
};

/* ------------------------------------------------------------ context */

export interface MempoolContext extends WriterContext {
  nodes: NodeRegistry;
  orphanGraceMs: number;
  maxOrphans: number;
  /** Register an operation (call order) once no mode switch is draining. */
  beginOperation: (
    kind: OperationKind,
    nodes: readonly number[]
  ) => Promise<StoreOperation>;
  endOperation: (operation: StoreOperation) => void;
}

interface ResolvedSpend {
  output: UtxoOutput;
  owner: StoreOperation | undefined;
}

/** Resolve outpoints: the output registry, then the store, then the registry again. */
export const resolveOutpoints = async (
  context: WriterContext,
  keys: readonly string[],
  operation: StoreOperation | undefined
): Promise<Map<string, ResolvedSpend>> => {
  const resolved = new Map<string, ResolvedSpend>();
  const fromRegistry = async (wanted: readonly string[]) => {
    const misses: string[] = [];
    for (const key of wanted) {
      const entry = context.outputs.lookup(key);
      if (entry === undefined) {
        misses.push(key);
      } else {
        const owner =
          entry.owner !== undefined &&
          entry.owner !== operation &&
          !entry.owner.finished
            ? entry.owner
            : undefined;
        resolved.set(key, {
          output: {
            ...entry.output,
            transactionInternalId: await entry.internalId,
          },
          owner,
        });
      }
    }
    return misses;
  };
  const misses = await fromRegistry([...new Set(keys)]);
  if (misses.length > 0) {
    const stored = await lookupStoredOutputs(
      context,
      misses.map(outpointParts)
    );
    stored.forEach(({ output, seq }) => {
      const key = outpointKey(output.transactionHash, output.outputIndex);
      if (resolved.has(key)) return;
      const owner = context.operationOfSeq(seq);
      resolved.set(key, {
        output,
        owner: owner === operation ? undefined : owner,
      });
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
};

const hashArraySql = (name: string) =>
  `(SELECT toFixedString(unhex(h), 32) FROM (SELECT arrayJoin({${name}:Array(String)}) AS h))`;

const pairArraySql = (hashes: string, indexes: string) =>
  `(SELECT toFixedString(unhex(p.1), 32), p.2
    FROM (SELECT arrayJoin(arrayZip({${hashes}:Array(String)}, {${indexes}:Array(UInt32)})) AS p))`;

/* ----------------------------------------------------------- committer */

export class MempoolCommitter {
  constructor(private readonly context: MempoolContext) {}

  private get mempool() {
    return this.context.mempool;
  }

  /* ------------------------------------------------ public entry points */

  /** `saveMempoolTransaction`: save if new, record each node's validation. */
  async saveTransaction(
    transaction: ChaingraphTransaction,
    validations: readonly NodeValidation[]
  ): Promise<void> {
    const parked = this.mempool.orphans.get(transaction.hash);
    if (parked !== undefined) {
      validations.forEach((validation) => {
        if (!parked.validations.has(validation.nodeInternalId)) {
          parked.validations.set(
            validation.nodeInternalId,
            validation.validatedAt
          );
        }
      });
      await parked.done.promise;
      return;
    }
    const byNode = new Map<number, Date>();
    validations.forEach((validation) => {
      if (!byNode.has(validation.nodeInternalId)) {
        byNode.set(validation.nodeInternalId, validation.validatedAt);
      }
    });
    await this.saveOrPark(transaction, byNode, false, undefined);
  }

  /** `recordNodeValidation`: a known transaction validated by one more node. */
  async recordValidation(
    transactionHash: string,
    validation: NodeValidation
  ): Promise<void> {
    const parked = this.mempool.orphans.get(transactionHash);
    if (parked !== undefined) {
      if (!parked.validations.has(validation.nodeInternalId)) {
        parked.validations.set(
          validation.nodeInternalId,
          validation.validatedAt
        );
      }
      await parked.done.promise;
      return;
    }
    const nodes = [validation.nodeInternalId];
    const operation = await this.begin(nodes);
    let commit: OpenCommit | undefined;
    let waitFor: Promise<void> | undefined;
    try {
      await this.awaitPinnedMempoolOwner(transactionHash, operation);
      const orphan = this.mempool.orphans.get(transactionHash);
      if (orphan !== undefined) {
        MempoolCommitter.mergeValidations(
          orphan,
          new Map([[validation.nodeInternalId, validation.validatedAt]])
        );
        operation.markDone();
        waitFor = orphan.done.promise;
        return;
      }
      const dependencies = new Set<StoreOperation>();
      const facts = await this.loadFacts(
        transactionHash,
        operation,
        dependencies
      );
      if (facts === undefined) {
        // Postgres: the CROSS JOIN with the unknown transaction inserts nothing
        operation.markDone();
        return;
      }
      commit = await this.commitValidations(
        operation,
        facts,
        undefined,
        new Map([[validation.nodeInternalId, validation.validatedAt]]),
        dependencies,
        (opened) => {
          commit = opened;
        }
      );
    } catch (error) {
      await this.fail(operation, commit, error, 'recordNodeValidation');
      throw error;
    } finally {
      this.end(operation);
      if (waitFor !== undefined) {
        await waitFor;
      }
    }
  }

  /**
   * The repair sweep: per node, archive mempool rows already confirmed or
   * conflicted by blocks the node accepts (planner over the node's mempool
   * and its accepted inclusions read from the store). One commit per node
   * with something to archive. Returns the direct rows (Postgres shape and
   * order: node name, hash).
   */
  async sweep(): Promise<ArchivedMempoolTransaction[]> {
    const archived: ArchivedMempoolTransaction[] = [];
    for (const node of this.mempool.nodeIds) {
      if (this.mempool.isEmpty(node)) continue;
      const operation = await this.begin([node]);
      let commit: OpenCommit | undefined;
      try {
        await waitForPredecessorRows(operation);
        const dependencies = new Set<StoreOperation>(operation.predecessors);
        await this.ensureFresh(operation, [node]);
        const mempool = this.mempool.node(node);
        if (mempool.txs.size === 0) {
          operation.markDone();
          continue;
        }
        const { inclusions, outputsOf } = await this.loadInclusions(
          node,
          undefined,
          operation,
          dependencies
        );
        const known = await this.knownOutputsForConfirmed(
          node,
          inclusions,
          operation,
          dependencies
        );
        const change = this.mempool.planBlockAcceptance(
          node,
          inclusions,
          outputsOf,
          (spent) => known.get(spent)
        );
        if (isEmptyChange(change)) {
          operation.markDone();
          continue;
        }
        this.applyChanges(operation, [change]);
        commit = await this.writeCommit(
          operation,
          'mempool_batch',
          [change],
          undefined,
          dependencies,
          (opened) => {
            commit = opened;
          }
        );
        const name = this.context.nodes.nameOf(node) ?? String(node);
        change.archives
          .filter(
            (archive) =>
              archive.cause === 'confirmed' || archive.cause === 'conflict'
          )
          .forEach((archive) => {
            archived.push({
              hash: archive.tx,
              nodeName: name,
              replacedAt: archive.replacedAt,
            });
          });
      } catch (error) {
        await this.fail(
          operation,
          commit,
          error,
          'archiveMempoolTransactionsAcceptedByBlocks'
        );
        throw error;
      } finally {
        this.end(operation);
      }
    }
    return archived.sort((a, b) =>
      a.nodeName === b.nodeName
        ? a.hash < b.hash
          ? -1
          : Number(a.hash > b.hash)
        : a.nodeName < b.nodeName
        ? -1
        : 1
    );
  }

  /** `getMempoolTransactionsExpiringBefore`, from the in-memory mempools. */
  expiringBefore({
    expirationMs,
    expiresBefore,
  }: {
    expirationMs: number;
    expiresBefore: Date;
  }): ExpiringMempoolTransaction[] {
    const limit = expiresBefore.getTime();
    const rows: ExpiringMempoolTransaction[] = [];
    this.mempool.nodeIds.forEach((node) => {
      const nodeName = this.context.nodes.nameOf(node) ?? String(node);
      this.mempool.node(node).txs.forEach((entry, hash) => {
        const expiresAtMs = entry.validatedAt.getTime() + expirationMs;
        if (expiresAtMs <= limit) {
          rows.push({
            expiresAt: new Date(expiresAtMs),
            hash,
            nodeInternalId: node,
            nodeName,
            transactionInternalId: Number(entry.internalId),
            validatedAt: entry.validatedAt,
          });
        }
      });
    });
    return rows.sort(
      (a, b) =>
        a.expiresAt.getTime() - b.expiresAt.getTime() ||
        (a.nodeName < b.nodeName ? -1 : Number(a.nodeName > b.nodeName)) ||
        (a.hash < b.hash ? -1 : Number(a.hash > b.hash))
    );
  }

  /** `archiveMempoolTransaction`: expire one entry (+ cascade); 1 or 0. */
  async expire({
    nodeInternalId,
    replacedAt,
    transactionInternalId,
  }: {
    nodeInternalId: number;
    replacedAt: Date;
    transactionInternalId: number;
  }): Promise<number> {
    const operation = await this.begin([nodeInternalId]);
    let commit: OpenCommit | undefined;
    try {
      await waitForPredecessorRows(operation);
      const dependencies = new Set<StoreOperation>(operation.predecessors);
      await this.ensureFresh(operation, [nodeInternalId]);
      const mempool = this.mempool.node(nodeInternalId);
      const hash = [...mempool.txs.entries()].find(
        ([, entry]) => entry.internalId === BigInt(transactionInternalId)
      )?.[0];
      if (hash === undefined) {
        operation.markDone();
        return 0;
      }
      const change = this.mempool.planExpiry(nodeInternalId, hash, replacedAt);
      this.applyChanges(operation, [change]);
      commit = await this.writeCommit(
        operation,
        'expiry',
        [change],
        undefined,
        dependencies,
        (opened) => {
          commit = opened;
        }
      );
      return 1;
    } catch (error) {
      await this.fail(operation, commit, error, 'archiveMempoolTransaction');
      throw error;
    } finally {
      this.end(operation);
    }
  }

  /* --------------------------------------------- block / header helpers */

  /**
   * Outputs (by outpoint) for the unresolved spends of mempool entries the
   * inclusions confirm (their −1 rows are written at confirmation when the
   * spent output is known by then).
   */
  async knownOutputsForConfirmed(
    node: number,
    inclusions: readonly AcceptedInclusion[],
    operation: StoreOperation,
    dependencies: Set<StoreOperation>
  ): Promise<Map<string, UtxoOutput>> {
    const mempool = this.mempool.node(node);
    const wanted = new Set<Outpoint>();
    inclusions.forEach((inclusion) => {
      mempool.txs.get(inclusion.tx)?.unresolved.forEach((spent) => {
        wanted.add(spent);
      });
    });
    const known = new Map<string, UtxoOutput>();
    if (wanted.size === 0) return known;
    const resolved = await resolveOutpoints(
      this.context,
      [...wanted],
      operation
    );
    resolved.forEach((spend, key) => {
      known.set(key, spend.output);
      if (spend.owner !== undefined) dependencies.add(spend.owner);
    });
    return known;
  }

  /**
   * The inclusions relevant to `node`'s mempool, read from the store:
   * mempool txs included in, and other txs spending a mempool outpoint
   * included in, either `blocks` (header acceptance: block id → the node's
   * accepted_at) or every block the node accepts (`blocks` undefined: the
   * sweep, accepted_at from `tx_acceptance`). Also the outputs of creators of
   * the node's outstanding spends included in `blocks`.
   */
  async loadInclusions(
    node: number,
    blocks: ReadonlyMap<string, Date | null> | undefined,
    operation: StoreOperation,
    dependencies: Set<StoreOperation>
  ): Promise<{
    inclusions: AcceptedInclusion[];
    outputsOf: (tx: TxKey) => readonly UtxoOutput[] | undefined;
  }> {
    const { client, lookupChunkSize } = this.context;
    const mempool = this.mempool.node(node);
    const hashes = [...mempool.txs.keys()];
    const outpoints = [
      ...new Set([...mempool.txs.values()].flatMap((entry) => entry.spends)),
    ];
    const fence = this.context.fence();
    const addSeqs = (seqs: readonly string[]) => {
      seqs.forEach((seq) => {
        const owner = this.context.operationOfSeq(BigInt(seq));
        if (owner !== undefined && owner !== operation) dependencies.add(owner);
      });
    };
    /* spenders of mempool outpoints (other than the mempool tx itself) */
    const spendsBy = new Map<TxKey, Outpoint[]>();
    for (const chunk of chunked(outpoints, lookupChunkSize)) {
      const parts = chunk.map(outpointParts);
      const rows = await client.query<{
        spender: string;
        hash: string;
        idx: number;
        seq: string;
      }>(
        `SELECT lower(hex(transaction_hash)) AS spender, lower(hex(outpoint_transaction_hash)) AS hash,
           outpoint_index AS idx, commit_seq AS seq
         FROM input
         WHERE (outpoint_transaction_hash, outpoint_index) IN ${pairArraySql(
           'hashes',
           'indexes'
         )}
           AND ${validCommitSql()}`,
        {
          fence,
          hashes: parts.map((part) => part.hash),
          indexes: parts.map((part) => part.index),
        }
      );
      rows.forEach((row) => {
        addSeqs([row.seq]);
        const list = spendsBy.get(row.spender) ?? [];
        const key = outpoint(row.hash, Number(row.idx));
        if (!list.includes(key)) list.push(key);
        spendsBy.set(row.spender, list);
      });
    }
    const candidates = [...new Set([...hashes, ...spendsBy.keys()])];
    const creators = [...mempool.waitingOnCreator.keys()];
    const inclusions: AcceptedInclusion[] = [];
    const includedCreators = new Set<TxKey>();
    const spendsOf = (tx: TxKey) =>
      mempool.txs.get(tx)?.spends ?? spendsBy.get(tx) ?? [];
    if (blocks === undefined) {
      for (const chunk of chunked(candidates, lookupChunkSize)) {
        const rows = await client.query<{
          hash: string;
          accepted_ms: string | null;
          seqs: string[];
        }>(
          `SELECT lower(hex(transaction_hash)) AS hash,
             toUnixTimestamp64Milli(argMaxIf(accepted_at, version, sign > 0)) AS accepted_ms,
             groupArray(commit_seq) AS seqs
           FROM tx_acceptance
           WHERE transaction_hash IN ${hashArraySql('hashes')}
             AND node_internal_id = {node:UInt32} AND block_internal_id != 0
             AND ${validCommitSql()}
           GROUP BY transaction_hash, block_internal_id
           HAVING sum(sign) > 0`,
          { fence, hashes: chunk, node }
        );
        rows.forEach((row) => {
          addSeqs(row.seqs);
          inclusions.push({
            acceptedAt:
              row.accepted_ms === null
                ? null
                : new Date(Number(row.accepted_ms)),
            spends: spendsOf(row.hash),
            tx: row.hash,
          });
        });
      }
    } else {
      const blockIds = [...blocks.keys()].map((id) => BigInt(id));
      const wanted = [...new Set([...candidates, ...creators])];
      for (const blockChunk of chunked(blockIds, lookupChunkSize)) {
        for (const chunk of chunked(wanted, lookupChunkSize)) {
          const rows = await client.query<{
            hash: string;
            block: string;
            seq: string;
          }>(
            `SELECT lower(hex(transaction_hash)) AS hash, toString(block_internal_id) AS block,
               commit_seq AS seq
             FROM block_transaction
             WHERE has({blocks:Array(UInt64)}, block_internal_id)
               AND transaction_hash IN ${hashArraySql('hashes')}
               AND ${validCommitSql()}`,
            { blocks: blockChunk, fence, hashes: chunk }
          );
          rows.forEach((row) => {
            addSeqs([row.seq]);
            if (mempool.waitingOnCreator.has(row.hash)) {
              includedCreators.add(row.hash);
            }
            if (candidates.includes(row.hash)) {
              inclusions.push({
                acceptedAt: blocks.get(row.block) ?? null,
                spends: spendsOf(row.hash),
                tx: row.hash,
              });
            }
          });
        }
      }
    }
    /* outputs of included creators of outstanding spends */
    const creatorOutputs = new Map<TxKey, UtxoOutput[]>();
    if (includedCreators.size > 0) {
      const wanted: string[] = [];
      mempool.txs.forEach((entry) => {
        entry.unresolved.forEach((spent) => {
          if (includedCreators.has(outpointParts(spent).hash)) {
            wanted.push(spent);
          }
        });
      });
      const resolved = await resolveOutpoints(this.context, wanted, operation);
      resolved.forEach((spend) => {
        if (spend.owner !== undefined) dependencies.add(spend.owner);
        const list = creatorOutputs.get(spend.output.transactionHash) ?? [];
        list[spend.output.outputIndex] = spend.output;
        creatorOutputs.set(spend.output.transactionHash, list);
      });
    }
    return {
      inclusions,
      outputsOf: (tx) => creatorOutputs.get(tx),
    };
  }

  /** Apply planned changes in memory and register `operation` as their writer. */
  applyChanges(
    operation: StoreOperation,
    changes: readonly NodeMempoolChange[]
  ) {
    const nonEmpty = changes.filter((change) => !isEmptyChange(change));
    if (nonEmpty.length === 0) return;
    this.mempool.addModifier(
      operation,
      nonEmpty.map((change) => change.node)
    );
    nonEmpty.forEach((change) => {
      change.resolutions.forEach((resolution) => {
        const facts = this.mempool.factsOf(resolution.spender);
        if (facts !== undefined && !facts.spent.has(resolution.outpoint)) {
          (facts.spent as Map<Outpoint, UtxoOutput>).set(
            resolution.outpoint,
            resolution.output
          );
        }
      });
      this.mempool.apply(change);
    });
  }

  /** Allocate the history ids for `changes` (before the commit opens). */
  async historyIds(changes: readonly NodeMempoolChange[]): Promise<bigint[]> {
    const count = historyRowCount(changes);
    return count === 0
      ? []
      : segmentIds(
          await this.context.ids.allocate('node_transaction_history', count)
        );
  }

  /**
   * Insert the mempool rows of `changes` under `commit` (block, header or
   * mempool commit). `extraUtxo` rows (the caller's own) go in the same
   * `utxo` insert. Returns the row counts.
   */
  async insertChangeRows(
    commit: OpenCommit,
    kind: string,
    rows: MempoolRows,
    chunk: string,
    rowCounts: { [table: string]: number }
  ) {
    const { client } = this.context;
    const insert = async (
      table: string,
      columns: readonly string[],
      encoded: { data: Uint8Array; rowCount: number }
    ) => {
      if (encoded.rowCount === 0) return;
      if (this.context.utxo === false && utxoTables.has(table)) return;
      await client.insertRowBinary(table, columns, encoded.data, {
        deduplicationToken: commit.token(table, chunk),
      });
      rowCounts[table] = (rowCounts[table] ?? 0) + encoded.rowCount;
      await this.context.fault(table, { kind, seq: commit.seq });
    };
    await insert(
      'node_transaction',
      mempoolColumns.node_transaction,
      encodeNodeTransactionRows(rows.nodeTransaction, commit.seq)
    );
    await insert(
      'tx_acceptance',
      acceptanceColumns.tx_acceptance,
      encodeTxAcceptanceRows(
        rows.txAcceptance.map((row) => ({ ...row, version: commit.seq })),
        commit.seq
      )
    );
    await insert(
      'node_transaction_history',
      mempoolColumns.node_transaction_history,
      encodeNodeTransactionHistoryRows(rows.history, commit.seq)
    );
    await insert(
      'pending_spend',
      acceptanceColumns.pending_spend,
      encodePendingSpendRows(rows.pendingSpend, commit.seq)
    );
    if (rows.utxo.length > 0) {
      const encoded = encodeUtxoRows(rows.utxo, commit.seq);
      await insert('utxo', utxoColumns, {
        data: encoded.utxo,
        rowCount: encoded.rowCount,
      });
      await insert('utxo_by_script', utxoByScriptColumns, {
        data: encoded.utxoByScript,
        rowCount: encoded.rowCount,
      });
    }
  }

  /**
   * Rebuild the in-memory mempools (all nodes, or `only`) from the store's
   * valid rows: live `node_transaction` rows, the transactions' outputs and
   * inputs, the spent outputs, and live `pending_spend` rows (outstanding
   * spends). Run at init (no open commits) and after a failure (once the
   * node's earlier operations have settled).
   */
  async rebuild(only?: readonly number[]) {
    const { client, lookupChunkSize } = this.context;
    const fence = this.context.fence();
    const members = await client.query<{
      node: number;
      id: string;
      hash: string;
      validated_ms: string | null;
    }>(
      `SELECT node_internal_id AS node, toString(transaction_internal_id) AS id,
         lower(hex(any(transaction_hash))) AS hash,
         toUnixTimestamp64Milli(argMaxIf(validated_at, version, sign > 0)) AS validated_ms
       FROM node_transaction
       WHERE ({all:UInt8} = 1 OR has({nodes:Array(UInt32)}, node_internal_id))
         AND ${validCommitSql()}
       GROUP BY node_internal_id, transaction_internal_id
       HAVING sum(sign) > 0`,
      { all: only === undefined ? 1 : 0, fence, nodes: only ?? [] }
    );
    const pendingRows = await client.query<{
      node: number;
      spender: string;
      hash: string;
      idx: number;
    }>(
      `SELECT node_internal_id AS node, lower(hex(spender_transaction_hash)) AS spender,
         lower(hex(outpoint_transaction_hash)) AS hash, outpoint_index AS idx
       FROM pending_spend
       WHERE node_internal_id != 0
         AND ({all:UInt8} = 1 OR has({nodes:Array(UInt32)}, node_internal_id))
         AND ${validCommitSql()}
       GROUP BY outpoint_transaction_hash, outpoint_index, node_internal_id,
         spender_transaction_hash, spender_input_index
       HAVING sum(sign) > 0`,
      { all: only === undefined ? 1 : 0, fence, nodes: only ?? [] }
    );
    const hashes = [...new Set(members.map((row) => row.hash))];
    const outputsByTx = new Map<string, UtxoOutput[]>();
    const spendsByTx = new Map<string, Outpoint[]>();
    for (const chunk of chunked(hashes, lookupChunkSize)) {
      const outputs = await client.query<{
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
      }>(
        `SELECT lower(hex(transaction_hash)) AS hash, output_index, transaction_internal_id, value_satoshis,
           lower(hex(locking_bytecode)) AS locking_bytecode, lower(hex(token_category)) AS token_category,
           fungible_token_amount, nonfungible_token_capability,
           lower(hex(nonfungible_token_commitment)) AS nonfungible_token_commitment, commit_seq
         FROM output
         WHERE transaction_hash IN ${hashArraySql(
           'hashes'
         )} AND ${validCommitSql()}
         ORDER BY hash, output_index, commit_seq`,
        { fence, hashes: chunk }
      );
      outputs.forEach((row) => {
        const list = outputsByTx.get(row.hash) ?? [];
        const index = Number(row.output_index);
        if (list[index] === undefined) list[index] = storedOutputToUtxo(row);
        outputsByTx.set(row.hash, list);
      });
      const inputs = await client.query<{
        hash: string;
        input_index: number;
        outpoint_hash: string;
        outpoint_index: number;
      }>(
        `SELECT lower(hex(transaction_hash)) AS hash, input_index,
           lower(hex(outpoint_transaction_hash)) AS outpoint_hash, outpoint_index
         FROM input
         WHERE transaction_hash IN ${hashArraySql(
           'hashes'
         )} AND ${validCommitSql()}
         ORDER BY hash, input_index, commit_seq`,
        { fence, hashes: chunk }
      );
      inputs.forEach((row) => {
        const list = spendsByTx.get(row.hash) ?? [];
        const index = Number(row.input_index);
        if (list[index] === undefined) {
          list[index] = outpoint(row.outpoint_hash, Number(row.outpoint_index));
        }
        spendsByTx.set(row.hash, list);
      });
    }
    const allSpends = [...new Set([...spendsByTx.values()].flat())].filter(
      (spent) => !spent.startsWith('00'.repeat(32))
    );
    const spentOutputs = new Map<string, UtxoOutput>();
    (
      await lookupStoredOutputs(this.context, allSpends.map(outpointParts))
    ).forEach(({ output }) => {
      spentOutputs.set(
        outpointKey(output.transactionHash, output.outputIndex),
        output
      );
    });
    const factsByHash = new Map<string, MempoolTxFacts>();
    const membersById = new Map<string, string>();
    members.forEach((row) => {
      membersById.set(row.id, row.hash);
      if (factsByHash.has(row.hash)) return;
      const spends = spendsByTx.get(row.hash) ?? [];
      const spent = new Map<Outpoint, UtxoOutput>();
      spends.forEach((key) => {
        const output = spentOutputs.get(key);
        if (output !== undefined) spent.set(key, output);
      });
      factsByHash.set(row.hash, {
        hash: row.hash,
        internalId: BigInt(row.id),
        outputs: outputsByTx.get(row.hash) ?? [],
        spends,
        spent,
      });
    });
    const unresolvedOf = new Map<string, Set<Outpoint>>();
    pendingRows.forEach((row) => {
      const key = `${row.node}|${row.spender}`;
      const set = unresolvedOf.get(key) ?? new Set<Outpoint>();
      set.add(outpoint(row.hash, Number(row.idx)));
      unresolvedOf.set(key, set);
    });
    const nodes = only ?? [
      ...new Set([
        ...members.map((row) => Number(row.node)),
        ...this.mempool.nodeIds,
      ]),
    ];
    nodes.forEach((node) => {
      const entries: [MempoolTxFacts, LiveMempoolEntry][] = members
        .filter((row) => Number(row.node) === node)
        .map((row) => {
          const facts = factsByHash.get(row.hash)!;
          return [
            facts,
            {
              internalId: facts.internalId,
              spends: facts.spends,
              unresolved:
                unresolvedOf.get(`${node}|${row.hash}`) ?? new Set<Outpoint>(),
              validatedAt: new Date(Number(row.validated_ms ?? 0)),
            },
          ];
        });
      this.mempool.reset(node, entries);
    });
  }

  /* ------------------------------------------------------- internals */

  private async begin(nodes: readonly number[]) {
    const operation = await this.context.beginOperation('mempool', nodes);
    this.mempool.addModifier(operation, nodes);
    return operation;
  }

  private end(operation: StoreOperation) {
    this.mempool.removeModifier(operation);
    this.context.endOperation(operation);
  }

  /**
   * If a node's in-memory mempool is stale (an operation that changed it
   * failed), wait until its earlier operations settle, then rebuild it from
   * the store.
   */
  async ensureFresh(operation: StoreOperation, nodes: readonly number[]) {
    const stale = nodes.filter((node) => this.mempool.stale.has(node));
    if (stale.length === 0) return;
    await Promise.all(
      this.context.operations.liveOperations
        .filter(
          (other) =>
            other !== operation &&
            other.id < operation.id &&
            other.nodes.some((node) => stale.includes(node))
        )
        .map(async (other) => other.committed.catch(() => undefined))
    );
    await this.rebuild(stale);
  }

  private async fail(
    operation: StoreOperation,
    commit: OpenCommit | undefined,
    error: unknown,
    method: string
  ) {
    if (!(error instanceof SimulatedCrash)) {
      this.mempool.markStale(operation);
      if (commit !== undefined) {
        await this.context.commitLog
          .markAborted(commit.seq, `${method} failed: ${String(error)}`)
          .catch(() => undefined);
      }
    }
    operation.markFailed(error);
  }

  /**
   * Load the facts of a stored transaction (for `recordNodeValidation`):
   * the shared mempool facts if some node holds it, else `transaction`,
   * `output` and `input` rows plus the spent outputs.
   */
  private async loadFacts(
    hash: string,
    operation: StoreOperation,
    dependencies: Set<StoreOperation>
  ): Promise<MempoolTxFacts | undefined> {
    const cached = this.mempool.factsOf(hash);
    if (cached !== undefined) return cached;
    const { client } = this.context;
    const fence = this.context.fence();
    const pinned = this.context.transactions.lookup(hash);
    let internalId: bigint | undefined;
    if (pinned !== undefined) {
      internalId = await pinned.internalId;
      if (pinned.owner !== undefined && pinned.owner !== operation) {
        dependencies.add(pinned.owner);
      }
    } else {
      const stored = await lookupStoredTransactions(this.context, [hash]);
      const row = stored.get(hash);
      if (row === undefined) return undefined;
      internalId = row.internalId;
      const owner = this.context.operationOfSeq(row.seq);
      if (owner !== undefined && owner !== operation) dependencies.add(owner);
    }
    const [outputs, inputs] = await Promise.all([
      client.query<{
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
      }>(
        `SELECT lower(hex(transaction_hash)) AS hash, output_index, transaction_internal_id, value_satoshis,
           lower(hex(locking_bytecode)) AS locking_bytecode, lower(hex(token_category)) AS token_category,
           fungible_token_amount, nonfungible_token_capability,
           lower(hex(nonfungible_token_commitment)) AS nonfungible_token_commitment, commit_seq
         FROM output
         WHERE transaction_hash = toFixedString(unhex({hash:String}), 32) AND ${validCommitSql()}
         ORDER BY output_index, commit_seq`,
        { fence, hash }
      ),
      client.query<{
        input_index: number;
        outpoint_hash: string;
        outpoint_index: number;
        commit_seq: string;
      }>(
        `SELECT input_index, lower(hex(outpoint_transaction_hash)) AS outpoint_hash, outpoint_index, commit_seq
         FROM input
         WHERE transaction_hash = toFixedString(unhex({hash:String}), 32) AND ${validCommitSql()}
         ORDER BY input_index, commit_seq`,
        { fence, hash }
      ),
    ]);
    const outputList: UtxoOutput[] = [];
    outputs.forEach((row) => {
      const index = Number(row.output_index);
      if (outputList[index] === undefined) {
        outputList[index] = storedOutputToUtxo(row);
      }
    });
    const spends: Outpoint[] = [];
    inputs.forEach((row) => {
      const index = Number(row.input_index);
      if (spends[index] === undefined) {
        spends[index] = outpoint(row.outpoint_hash, Number(row.outpoint_index));
      }
    });
    const realSpends = spends.filter(
      (spent) => !spent.startsWith(`${'00'.repeat(32)}:`)
    );
    const resolved = await resolveOutpoints(
      this.context,
      realSpends,
      operation
    );
    const spent = new Map<Outpoint, UtxoOutput>();
    resolved.forEach((spend, key) => {
      spent.set(key as Outpoint, spend.output);
      if (spend.owner !== undefined) dependencies.add(spend.owner);
    });
    return {
      hash,
      internalId,
      outputs: outputList,
      spends: realSpends,
      spent,
    };
  }

  /**
   * If another live mempool operation has pinned `hash` (it is saving the
   * same transaction), wait until it settles: it either commits the
   * transaction or parks it in the orphan pool.
   */
  private async awaitPinnedMempoolOwner(
    hash: string,
    operation: StoreOperation
  ) {
    for (;;) {
      const owner = this.context.transactions.lookup(hash)?.owner;
      if (
        owner === undefined ||
        owner === operation ||
        owner.finished ||
        owner.kind !== 'mempool'
      ) {
        return;
      }
      await owner.committed.catch(() => undefined);
    }
  }

  /** Merge validations into a parked orphan. */
  private static mergeValidations(
    entry: OrphanEntry,
    byNode: ReadonlyMap<number, Date>
  ) {
    byNode.forEach((at, node) => {
      if (!entry.validations.has(node)) entry.validations.set(node, at);
    });
  }

  /**
   * Save a new or known transaction for `byNode`; park it in the orphan pool
   * if it is new and spends an unknown output (unless `force`). Resolves
   * once it is saved (after parking, when the parked attempt is saved).
   */
  private async saveOrPark(
    transaction: ChaingraphTransaction,
    byNode: Map<number, Date>,
    force: boolean,
    parkedDone: Deferred<void> | undefined,
    orphanParents: readonly TxKey[] = []
  ): Promise<void> {
    const nodes = [...byNode.keys()];
    const operation = await this.begin(nodes);
    let commit: OpenCommit | undefined;
    let waitFor: Promise<void> | undefined;
    const { context } = this;
    try {
      await this.awaitPinnedMempoolOwner(transaction.hash, operation);
      const parked = this.mempool.orphans.get(transaction.hash);
      if (parked !== undefined) {
        MempoolCommitter.mergeValidations(parked, byNode);
        operation.markDone();
        waitFor = parked.done.promise;
        return;
      }
      const dependencies = new Set<StoreOperation>();
      /* 1. the transaction id (pinned, so concurrent saves share it) */
      let internalId: bigint;
      let isNew = false;
      const known = context.transactions.lookup(transaction.hash);
      let ownId: Deferred<bigint> | undefined;
      if (known !== undefined) {
        internalId = await known.internalId;
        if (known.owner !== undefined && known.owner !== operation) {
          dependencies.add(known.owner);
        }
      } else {
        ownId = deferred<bigint>();
        ownId.promise.catch(() => undefined);
        context.transactions.pin(transaction.hash, operation, ownId.promise);
        context.outputs.register(operation, [
          {
            hash: transaction.hash,
            internalId: ownId.promise,
            outputs: transaction.outputs,
          },
        ]);
        const stored = await lookupStoredTransactions(context, [
          transaction.hash,
        ]);
        const row = stored.get(transaction.hash);
        if (row === undefined) {
          isNew = true;
          internalId = await context.ids.allocateOne('transaction');
        } else {
          internalId = row.internalId;
          const owner = context.operationOfSeq(row.seq);
          if (owner !== undefined && owner !== operation) {
            dependencies.add(owner);
          }
        }
        ownId.resolve(internalId);
      }
      /* 2. spent outputs */
      const spendKeys = transaction.isCoinbase
        ? []
        : transaction.inputs.map((input) =>
            outpointKey(input.outpointTransactionHash, input.outpointIndex)
          );
      const resolved = await resolveOutpoints(context, spendKeys, operation);
      resolved.forEach((spend) => {
        if (spend.owner !== undefined) dependencies.add(spend.owner);
      });
      const missing = spendKeys.filter((key) => !resolved.has(key));
      if (isNew && missing.length > 0 && !force) {
        /*
         * Orphan: no commit. Unpin (synchronously) and enter the pool before
         * this operation finishes, so a concurrent save of the same tx that
         * waits for this operation finds the pool entry.
         */
        context.outputs.release(operation, false);
        await context.transactions.release(operation, false);
        const done = parkedDone ?? deferred<void>();
        done.promise.catch(() => undefined);
        this.registerOrphan(transaction, byNode, missing, done, orphanParents);
        operation.markDone();
        waitFor = done.promise;
        return;
      }
      const spentMap = new Map<string, UtxoOutput>();
      resolved.forEach((spend, key) => {
        spentMap.set(key, spend.output);
      });
      const facts =
        this.mempool.factsOf(transaction.hash) ??
        factsFromTransaction(transaction, internalId, spentMap);
      commit = await this.commitValidations(
        operation,
        facts,
        isNew
          ? {
              internalId,
              resolvedSpend: (key) => resolved.get(key)?.output,
              transaction,
            }
          : undefined,
        byNode,
        dependencies,
        (opened) => {
          commit = opened;
        },
        orphanParents
      );
      context.outputs.release(operation, true);
      await context.transactions.release(operation, true);
      parkedDone?.resolve();
    } catch (error) {
      await this.fail(operation, commit, error, 'saveMempoolTransaction');
      context.outputs.release(operation, false);
      await context.transactions.release(operation, false);
      parkedDone?.reject(error);
      throw error;
    } finally {
      this.end(operation);
      /*
       * Parked (an orphan, or merged into one): resolve once the parked
       * attempt is saved. In `finally`, because the parked paths return.
       */
      if (waitFor !== undefined) {
        await waitFor;
      }
    }
  }

  /**
   * Hold an orphan until one of its missing outputs is registered by a save
   * (a block or another mempool transaction), or the grace period since it
   * was first received ends; then it is saved again (after the grace period
   * with its unknown spends outstanding, as Postgres saves such a
   * transaction at once). The pool is bounded: when it is full, the oldest
   * orphan is released early.
   */
  private registerOrphan(
    transaction: ChaingraphTransaction,
    byNode: Map<number, Date>,
    missing: readonly string[],
    done: Deferred<void>,
    earlierParents: readonly TxKey[]
  ) {
    const { orphans } = this.mempool;
    const parents = [
      ...new Set([
        ...earlierParents,
        ...missing.map((key) => outpointParts(key).hash),
      ]),
    ];
    const waits = missing.map((key) => this.context.outputs.waitFor(key));
    let timer: ReturnType<typeof setTimeout> | undefined;
    let settled = false;
    const firstReceived =
      this.orphanReceivedAt.get(transaction.hash) ?? new Date();
    this.orphanReceivedAt.set(transaction.hash, firstReceived);
    const remainingGrace = Math.max(
      0,
      firstReceived.getTime() + this.context.orphanGraceMs - Date.now()
    );
    const run = (force: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      waits.forEach(({ cancel }) => {
        cancel();
      });
      const entry = orphans.get(transaction.hash);
      orphans.delete(transaction.hash);
      const validations = entry?.validations ?? byNode;
      this.saveOrPark(transaction, validations, force, done, parents)
        .catch(() => undefined)
        .finally(() => {
          if (!orphans.has(transaction.hash)) {
            this.orphanReceivedAt.delete(transaction.hash);
          }
        });
    };
    if (orphans.size >= this.context.maxOrphans) {
      const [oldest] = orphans.values();
      oldest?.release();
    }
    orphans.set(transaction.hash, {
      done,
      missingParents: [
        ...new Set(missing.map((key) => outpointParts(key).hash)),
      ],
      receivedAt: firstReceived,
      release: () => {
        run(true);
      },
      validations: new Map(byNode),
    });
    waits.forEach(({ promise }) => {
      promise
        .then(() => {
          run(false);
        })
        .catch(() => undefined);
    });
    timer = setTimeout(() => {
      run(true);
    }, remainingGrace);
  }

  private readonly orphanReceivedAt = new Map<string, Date>();

  /** Abandon every parked orphan (store shutdown). */
  dropOrphans(error: Error) {
    [...this.mempool.orphans.values()].forEach((entry) => {
      entry.done.reject(error);
    });
    this.mempool.orphans.clear();
  }

  /**
   * The per-node phase of a save or validation, then the commit: wait for
   * the nodes' earlier operations to write their rows (and depend on them),
   * read which nodes already accept the transaction in a block or accept a
   * block spending one of its outpoints, plan each node's change against its
   * in-memory mempool, and write everything as one commit.
   */
  private async commitValidations(
    operation: StoreOperation,
    facts: MempoolTxFacts,
    base:
      | {
          transaction: ChaingraphTransaction;
          internalId: bigint;
          resolvedSpend: (key: string) => UtxoOutput | undefined;
        }
      | undefined,
    byNode: ReadonlyMap<number, Date>,
    dependencies: Set<StoreOperation>,
    onCommit: (commit: OpenCommit) => void,
    orphanParents: readonly TxKey[] = []
  ): Promise<OpenCommit | undefined> {
    await waitForPredecessorRows(operation);
    operation.predecessors.forEach((predecessor) => {
      dependencies.add(predecessor);
    });
    const nodes = [...byNode.keys()].sort((a, b) => a - b);
    await this.ensureFresh(operation, nodes);
    const pendingNodes = nodes.filter(
      (node) => !this.mempool.node(node).txs.has(facts.hash)
    );
    const confirmed =
      pendingNodes.length > 0
        ? await this.confirmedState(
            facts,
            pendingNodes,
            operation,
            dependencies
          )
        : new Map<number, { confirmedFor: boolean; conflict?: Date | null }>();
    const changes: NodeMempoolChange[] = [];
    pendingNodes.forEach((node) => {
      const state = confirmed.get(node);
      const change = this.mempool.planValidation(
        node,
        facts,
        byNode.get(node)!,
        {
          confirmedFor: state?.confirmedFor ?? false,
          conflictReplacedAt: state?.conflict,
          inheritedReplacedAt: this.mempool.inheritedReplacedAt(
            node,
            orphanParents
          ),
        }
      );
      if (change !== undefined && !isEmptyChange(change)) {
        changes.push(change);
        // later nodes plan against the shared facts as updated by this one
        this.applyChanges(operation, [change]);
      }
    });
    if (changes.length === 0 && base === undefined) {
      operation.markDone();
      return undefined;
    }
    return this.writeCommit(
      operation,
      'mempool_batch',
      changes,
      base,
      dependencies,
      onCommit
    );
  }

  /**
   * Per node: does it accept the transaction in a block (Postgres would
   * archive the new row with replaced_at NULL in the next sweep), or accept a
   * block spending one of its outpoints (archived with MIN(accepted_at),
   * NULL if every such block has a NULL accepted_at)?
   */
  private async confirmedState(
    facts: MempoolTxFacts,
    nodes: readonly number[],
    operation: StoreOperation,
    dependencies: Set<StoreOperation>
  ): Promise<Map<number, { confirmedFor: boolean; conflict?: Date | null }>> {
    const { client } = this.context;
    const fence = this.context.fence();
    const result = new Map<
      number,
      { confirmedFor: boolean; conflict?: Date | null }
    >();
    const addSeqs = (seqs: readonly string[]) => {
      seqs.forEach((seq) => {
        const owner = this.context.operationOfSeq(BigInt(seq));
        if (owner !== undefined && owner !== operation) dependencies.add(owner);
      });
    };
    const own = await client.query<{ node: number; seqs: string[] }>(
      `SELECT node_internal_id AS node, groupArray(commit_seq) AS seqs
       FROM tx_acceptance
       WHERE transaction_hash = toFixedString(unhex({hash:String}), 32)
         AND has({nodes:Array(UInt32)}, node_internal_id) AND block_internal_id != 0
         AND ${validCommitSql()}
       GROUP BY node_internal_id, block_internal_id
       HAVING sum(sign) > 0`,
      { fence, hash: facts.hash, nodes }
    );
    own.forEach((row) => {
      addSeqs(row.seqs);
      result.set(Number(row.node), { confirmedFor: true });
    });
    if (facts.spends.length === 0) return result;
    const parts = facts.spends.map(outpointParts);
    const conflicts = await client.query<{
      node: number;
      min_ms: string | null;
      seqs: string[];
    }>(
      `SELECT node, toUnixTimestamp64Milli(min(accepted)) AS min_ms, arrayFlatten(groupArray(seqs)) AS seqs
       FROM (
         SELECT node_internal_id AS node, argMaxIf(accepted_at, version, sign > 0) AS accepted,
           groupArray(commit_seq) AS seqs
         FROM tx_acceptance
         WHERE transaction_hash IN (
             SELECT transaction_hash FROM input
             WHERE (outpoint_transaction_hash, outpoint_index) IN ${pairArraySql(
               'hashes',
               'indexes'
             )}
               AND transaction_hash != toFixedString(unhex({hash:String}), 32)
               AND ${validCommitSql()})
           AND has({nodes:Array(UInt32)}, node_internal_id) AND block_internal_id != 0
           AND ${validCommitSql()}
         GROUP BY transaction_hash, node_internal_id, block_internal_id
         HAVING sum(sign) > 0)
       GROUP BY node`,
      {
        fence,
        hash: facts.hash,
        hashes: parts.map((part) => part.hash),
        indexes: parts.map((part) => part.index),
        nodes,
      }
    );
    conflicts.forEach((row) => {
      addSeqs(row.seqs);
      const node = Number(row.node);
      const current = result.get(node);
      if (current?.confirmedFor === true) return;
      result.set(node, {
        confirmedFor: false,
        conflict: row.min_ms === null ? null : new Date(Number(row.min_ms)),
      });
    });
    return result;
  }

  /** One mempool commit: base rows (if new), the changes' rows, commit. */
  private async writeCommit(
    operation: StoreOperation,
    kind: 'expiry' | 'mempool_batch',
    changes: readonly NodeMempoolChange[],
    base:
      | {
          transaction: ChaingraphTransaction;
          internalId: bigint;
          resolvedSpend: (key: string) => UtxoOutput | undefined;
        }
      | undefined,
    dependencies: Set<StoreOperation>,
    onCommit: (commit: OpenCommit) => void
  ): Promise<OpenCommit> {
    const { context } = this;
    const historyIds = await this.historyIds(changes);
    const rows = changeRows(changes, historyIds, {
      utxo: context.utxo !== false,
    });
    const nodeScope = [...new Set(changes.map((change) => change.node))].sort(
      (a, b) => a - b
    );
    const dependsOn = [...dependencies]
      .filter((dependency) => dependency.state === 'committed')
      .map((dependency) => dependency.seq)
      .filter((seq): seq is bigint => seq !== undefined);
    const commit = await context.commitLog.beginCommit({
      dependsOn,
      kind,
      nodeScope,
    });
    onCommit(commit);
    operation.seq = commit.seq;
    await context.fault('intent', { kind, seq: commit.seq });
    const rowCounts: { [table: string]: number } = {};
    if (base !== undefined) {
      const columnsOf = (table: keyof typeof rowBinaryTableColumns) =>
        rowBinaryTableColumns[table].map(([name]) => name);
      const transactionContext = {
        commitSeq: commit.seq,
        transactionInternalIds: [base.internalId],
      };
      const insert = async (
        table: string,
        encoded: { data: Uint8Array; rowCount: number }
      ) => {
        if (encoded.rowCount === 0) return;
        if (context.utxo === false && utxoTables.has(table)) return;
        await context.client.insertRowBinary(
          table,
          columnsOf(table as keyof typeof rowBinaryTableColumns),
          encoded.data,
          { deduplicationToken: commit.token(table) }
        );
        rowCounts[table] = (rowCounts[table] ?? 0) + encoded.rowCount;
        await context.fault(table, { kind, seq: commit.seq });
      };
      await insert(
        'output',
        encodeOutputRows([base.transaction], transactionContext)
      );
      await insert(
        'input',
        encodeInputRows(
          [base.transaction],
          transactionContext,
          (hash, index) =>
            base.resolvedSpend(outpointKey(hash, index)) ?? unknownSpentOutput
        )
      );
      await insert(
        'transaction',
        encodeTransactionRows([base.transaction], transactionContext)
      );
    }
    await this.insertChangeRows(commit, kind, rows, '0', rowCounts);
    operation.markRowsWritten();
    await context.fault('rows-written', { kind, seq: commit.seq });
    await (context.abandon?.race(awaitDependencies(dependencies, operation)) ??
      awaitDependencies(dependencies, operation));
    await context.commitLog.markCommitted(commit.seq, rowCounts);
    operation.markCommitted();
    await context.fault('committed', { kind, seq: commit.seq });
    return commit;
  }
}

export type { PlannedArchive };
