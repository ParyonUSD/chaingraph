/* eslint-disable max-classes-per-file, camelcase, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, max-params, @typescript-eslint/parameter-properties, @typescript-eslint/member-ordering, @typescript-eslint/no-invalid-void-type, class-methods-use-this */
// cspell:ignore clickhouse seqs
/**
 * Per-node in-memory state of the ClickHouse writer (WP5a-core):
 * - the node registry (name ↔ internal id);
 * - the operation registry: every store call that changes node facts is an
 *   operation registered (synchronously, at call time) for its nodes, so
 *   later operations of a node can wait until earlier ones have written
 *   their rows and depend on their commits (wp5a-core.md §4);
 * - RowBinary encoders for the per-node acceptance tables.
 */
import { RowBinaryWriter } from './row-binary.js';

export class NodeRegistry {
  private readonly idsByName = new Map<string, number>();

  private readonly namesById = new Map<number, string>();

  set(name: string, internalId: number) {
    this.idsByName.set(name, internalId);
    this.namesById.set(internalId, name);
  }

  idOf(name: string) {
    return this.idsByName.get(name);
  }

  nameOf(internalId: number) {
    return this.namesById.get(internalId);
  }

  get ids(): number[] {
    return [...this.namesById.keys()].sort((a, b) => a - b);
  }
}

export interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
}

export const deferred = <T>(): Deferred<T> => {
  // eslint-disable-next-line functional/no-let
  let resolve: (value: T) => void = () => undefined;
  // eslint-disable-next-line functional/no-let
  let reject: (error: unknown) => void = () => undefined;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, reject, resolve };
};

export type OperationKind =
  | 'block'
  | 'header_accept'
  | 'horizon'
  | 'mempool'
  | 'reorg';

export type OperationState = 'committed' | 'done' | 'failed' | 'running';

/**
 * One store call that writes node facts. `rowsWritten` resolves once every
 * row it will ever write is acknowledged (including pending-spend fill rows),
 * or once it fails; `committed` resolves when its commit is committed and
 * rejects if it failed.
 */
export class StoreOperation {
  state: OperationState = 'running';

  seq: bigint | undefined;

  /**
   * The operation's in-flight slot (`InFlightLimiter`), set for block saves
   * and header acceptances when the cap is on; `undefined` when unbounded.
   */
  slot: SaveSlot | undefined;

  private readonly rows = deferred<void>();

  private readonly commit = deferred<void>();

  private rowsDone = false;

  constructor(
    readonly id: number,
    readonly kind: OperationKind,
    readonly nodes: readonly number[],
    /**
     * Live operations registered earlier that share a node with this one
     * (cleared when this one finishes, so finished chains can be collected).
     */
    public predecessors: StoreOperation[]
  ) {
    // never an unhandled rejection: callers that care await these explicitly
    this.commit.promise.catch(() => undefined);
  }

  get rowsWritten(): Promise<void> {
    return this.rows.promise;
  }

  get committed(): Promise<void> {
    return this.commit.promise;
  }

  get finished() {
    return this.state !== 'running';
  }

  markRowsWritten() {
    this.predecessors = [];
    if (!this.rowsDone) {
      this.rowsDone = true;
      this.rows.resolve();
    }
  }

  markCommitted() {
    this.markRowsWritten();
    this.state = 'committed';
    this.commit.resolve();
  }

  /** Finished without a commit (nothing to write). */
  markDone() {
    this.markRowsWritten();
    this.state = 'done';
    this.commit.resolve();
  }

  markFailed(error: unknown) {
    this.markRowsWritten();
    this.state = 'failed';
    this.commit.reject(error);
  }

  /**
   * Await `work`, a wait on other operations (their rows, commits or
   * registrations), without holding an in-flight slot: the slot is released
   * for the wait and taken again (by this operation's ticket) afterwards.
   * This is the cap's deadlock rule: a slot holder never waits on another
   * operation (docs/clickhouse-port/wp5c-hardening.md §1).
   */
  async whileWaiting<T>(work: Promise<T>): Promise<T> {
    return this.slot === undefined ? work : this.slot.idle(work);
  }
}

export class DependencyFailedError extends Error {}

/** In-flight work was abandoned (store shutdown): nothing was committed. */
export class AbandonedError extends Error {}

/**
 * Shutdown signal: once `abandon` is called, every wait raced against it
 * (pending spends, commit dependencies) and every commit step rejects with
 * `AbandonedError`; the commit is aborted and recovery redoes nothing (the
 * agent re-downloads what was not committed).
 */
export class AbandonSignal {
  abandoned = false;

  readonly promise: Promise<never>;

  private rejectWith: (error: AbandonedError) => void = () => undefined;

  constructor() {
    this.promise = new Promise<never>((_, reject) => {
      this.rejectWith = reject;
    });
    this.promise.catch(() => undefined);
  }

  abandon(reason = 'store shutdown') {
    if (this.abandoned) return;
    this.abandoned = true;
    this.rejectWith(new AbandonedError(`Abandoned: ${reason}.`));
  }

  assertNotAbandoned() {
    if (this.abandoned) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new AbandonedError('Abandoned: store shutdown.');
    }
  }

  async race<T>(work: Promise<T>): Promise<T> {
    this.assertNotAbandoned();
    return Promise.race([work, this.promise]);
  }
}

/**
 * The in-flight cap (`CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES`): at most
 * `max` block saves / header acceptances hold a slot at once. Waiters are
 * granted in ticket order (ticket = operation id = store call order), so new
 * operations are served FIFO and an operation that gave its slot up for a
 * wait (`StoreOperation.whileWaiting`) is served before every newer one.
 * Invariant: the queue is empty whenever fewer than `max` slots are held (a
 * release hands the slot straight to the head of the queue).
 */
export class InFlightLimiter {
  private holders = 0;

  private readonly queue: { ticket: number; grant: () => void }[] = [];

  constructor(readonly max: number) {
    if (!Number.isInteger(max) || max < 1) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new RangeError(
        `In-flight cap must be an integer >= 1 (got ${max}).`
      );
    }
  }

  /** Slots held now. */
  get active() {
    return this.holders;
  }

  /** Operations queued for a slot. */
  get waiting() {
    return this.queue.length;
  }

  /** Resolves once a slot is held; rejects (and leaves the queue) on abandon. */
  async acquire(ticket: number, abandon?: AbandonSignal): Promise<void> {
    abandon?.assertNotAbandoned();
    if (this.holders < this.max) {
      this.holders += 1;
      return;
    }
    const { granted, waiter } = this.enqueue(ticket);
    if (abandon === undefined) {
      await granted;
      return;
    }
    // eslint-disable-next-line functional/no-try-statement
    try {
      await Promise.race([granted, abandon.promise]);
    } catch (error) {
      const index = this.queue.indexOf(waiter);
      if (index === -1) {
        // granted in the same turn: hand the slot on
        this.release();
      } else {
        this.queue.splice(index, 1);
      }
      // eslint-disable-next-line functional/no-throw-statement
      throw error;
    }
  }

  /** Queue a waiter in ticket order (new tickets are the largest: FIFO). */
  private enqueue(ticket: number) {
    const waiter = { grant: () => undefined as void, ticket };
    const granted = new Promise<void>((resolve) => {
      waiter.grant = resolve;
    });
    const position = this.queue.findIndex((other) => other.ticket > ticket);
    this.queue.splice(
      position === -1 ? this.queue.length : position,
      0,
      waiter
    );
    return { granted, waiter };
  }

  release() {
    const next = this.queue.shift();
    if (next === undefined) {
      this.holders -= 1;
      return;
    }
    next.grant();
  }
}

/** One operation's hold on the in-flight cap (idempotent acquire/release). */
export class SaveSlot {
  private held = false;

  constructor(
    private readonly limiter: InFlightLimiter,
    private readonly ticket: number,
    private readonly abandon?: AbandonSignal
  ) {}

  get isHeld() {
    return this.held;
  }

  async acquire() {
    if (this.held) return;
    await this.limiter.acquire(this.ticket, this.abandon);
    this.held = true;
  }

  release() {
    if (!this.held) return;
    this.held = false;
    this.limiter.release();
  }

  /** Release the slot while `work` is pending, then take it again. */
  async idle<T>(work: Promise<T>): Promise<T> {
    if (!this.held) return work;
    this.release();
    // eslint-disable-next-line functional/no-try-statement
    try {
      return await work;
    } finally {
      await this.acquire();
    }
  }
}

/**
 * The registry of live operations. Registration is synchronous, so the order
 * of registration is the order of the store calls (the agent's order).
 */
export class OperationRegistry {
  private readonly live = new Set<StoreOperation>();

  private nextId = 1;

  private readonly idle: (() => void)[] = [];

  get activeCount() {
    return this.live.size;
  }

  get liveOperations(): readonly StoreOperation[] {
    return [...this.live];
  }

  begin(kind: OperationKind, nodes: readonly number[]): StoreOperation {
    const nodeSet = new Set(nodes);
    const predecessors = [...this.live].filter((operation) =>
      operation.nodes.some((node) => nodeSet.has(node))
    );
    const operation = new StoreOperation(
      this.nextId,
      kind,
      [...nodeSet].sort((a, b) => a - b),
      predecessors
    );
    this.nextId += 1;
    this.live.add(operation);
    return operation;
  }

  /** Remove a finished operation. */
  end(operation: StoreOperation) {
    this.live.delete(operation);
    if (this.live.size === 0) {
      this.idle.splice(0).forEach((resolve) => {
        resolve();
      });
    }
  }

  /** Resolve once no operation is live. */
  async drain(): Promise<void> {
    if (this.live.size === 0) {
      return;
    }
    await new Promise<void>((resolve) => {
      this.idle.push(resolve);
    });
  }
}

/**
 * Wait until every predecessor of `operation` has written all its rows
 * (without holding an in-flight slot while waiting).
 */
export const waitForPredecessorRows = async (operation: StoreOperation) => {
  const rows = Promise.all(
    operation.predecessors.map(async (predecessor) => predecessor.rowsWritten)
  );
  await (operation.predecessors.length > 0
    ? operation.whileWaiting(rows)
    : rows);
};

/**
 * Wait until every dependency is committed (or finished without writing);
 * throws `DependencyFailedError` if one failed. Returns the seqs to pass as
 * `dependsOn`.
 */
export const awaitDependencies = async (
  dependencies: Iterable<StoreOperation>
): Promise<bigint[]> => {
  const list = [...new Set(dependencies)];
  await Promise.all(
    list.map(async (dependency) =>
      dependency.committed.catch((error: unknown) => {
        // eslint-disable-next-line functional/no-throw-statement
        throw new DependencyFailedError(
          `Operation ${dependency.id} (${
            dependency.kind
          }) this commit depends on failed: ${String(error)}`
        );
      })
    )
  );
  return list
    .filter((dependency) => dependency.state === 'committed')
    .map((dependency) => dependency.seq)
    .filter((seq): seq is bigint => seq !== undefined);
};

/** Column lists (DDL order, ddl/020_acceptance.sql and 040_bookkeeping.sql). */
export const acceptanceColumns = {
  node_block: [
    'node_internal_id',
    'block_internal_id',
    'block_hash',
    'height',
    'accepted_at',
    'sign',
    'version',
    'commit_seq',
  ],
  node_block_history: [
    'node_internal_id',
    'removed_at',
    'block_internal_id',
    'internal_id',
    'accepted_at',
    'commit_seq',
  ],
  pending_spend: [
    'outpoint_transaction_hash',
    'outpoint_index',
    'node_internal_id',
    'spender_transaction_hash',
    'spender_input_index',
    'spender_commit_seq',
    'sign',
    'version',
    'commit_seq',
  ],
  tx_acceptance: [
    'transaction_hash',
    'node_internal_id',
    'block_internal_id',
    'transaction_internal_id',
    'height',
    'accepted_at',
    'sign',
    'version',
    'commit_seq',
  ],
} as const;

export interface NodeBlockRow {
  nodeInternalId: number;
  blockInternalId: bigint;
  blockHash: string;
  height: number;
  acceptedAt: Date | null;
  sign: -1 | 1;
  /** The row's own commit_seq (collapse only within one commit; wp5a-core.md §2). */
  version: bigint;
}

export const encodeNodeBlockRows = (
  rows: readonly NodeBlockRow[],
  commitSeq: bigint
) => {
  const writer = new RowBinaryWriter(rows.length * 72);
  rows.forEach((row) => {
    writer
      .uint32(row.nodeInternalId)
      .uint64(row.blockInternalId)
      .fixedString32(row.blockHash)
      .uint32(row.height)
      .nullable(row.acceptedAt, (w, at) => w.dateTime64(at))
      .int8Sign(row.sign)
      .uint64(row.version)
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

export interface NodeBlockHistoryRow {
  nodeInternalId: number;
  removedAt: Date;
  blockInternalId: bigint;
  internalId: bigint;
  acceptedAt: Date | null;
}

export const encodeNodeBlockHistoryRows = (
  rows: readonly NodeBlockHistoryRow[],
  commitSeq: bigint
) => {
  const writer = new RowBinaryWriter(rows.length * 48);
  rows.forEach((row) => {
    writer
      .uint32(row.nodeInternalId)
      .dateTime64(row.removedAt)
      .uint64(row.blockInternalId)
      .uint64(row.internalId)
      .nullable(row.acceptedAt, (w, at) => w.dateTime64(at))
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

export interface TxAcceptanceRow {
  transactionHash: string;
  nodeInternalId: number;
  /** 0 = mempool */
  blockInternalId: bigint;
  transactionInternalId: bigint;
  /** 0 = mempool */
  height: number;
  acceptedAt: Date | null;
  sign: -1 | 1;
  version: bigint;
}

export const encodeTxAcceptanceRows = (
  rows: readonly TxAcceptanceRow[],
  commitSeq: bigint
) => {
  const writer = new RowBinaryWriter(rows.length * 80);
  rows.forEach((row) => {
    writer
      .fixedString32(row.transactionHash)
      .uint32(row.nodeInternalId)
      .uint64(row.blockInternalId)
      .uint64(row.transactionInternalId)
      .uint32(row.height)
      .nullable(row.acceptedAt, (w, at) => w.dateTime64(at))
      .int8Sign(row.sign)
      .uint64(row.version)
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

export interface PendingSpendRow {
  outpointTransactionHash: string;
  outpointIndex: number;
  /** 0 when no node's UTXO rows wait on it (bulk mode or no accepting node). */
  nodeInternalId: number;
  spenderTransactionHash: string;
  spenderInputIndex: number;
  sign: -1 | 1;
}

/** `pending_spend` rows; `version` and `spender_commit_seq` are the spender's seq. */
export const encodePendingSpendRows = (
  rows: readonly PendingSpendRow[],
  commitSeq: bigint
) => {
  const writer = new RowBinaryWriter(rows.length * 100);
  rows.forEach((row) => {
    writer
      .fixedString32(row.outpointTransactionHash)
      .uint32(row.outpointIndex)
      .uint32(row.nodeInternalId)
      .fixedString32(row.spenderTransactionHash)
      .uint32(row.spenderInputIndex)
      .uint64(commitSeq)
      .int8Sign(row.sign)
      .uint64(commitSeq)
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

/** Store modes: `bulk` below the horizon (no inline UTXO rows), `tip` above. */
export type StoreMode = 'bulk' | 'tip';
