/* eslint-disable max-classes-per-file, camelcase, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, @typescript-eslint/parameter-properties, @typescript-eslint/member-ordering, @typescript-eslint/no-invalid-void-type, class-methods-use-this, functional/no-let, functional/no-loop-statement, no-bitwise, complexity, functional/no-throw-statement, @typescript-eslint/no-use-before-define */
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
  let resolve: (value: T) => void = () => undefined;
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
  | 'predecessors'
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

  /**
   * The registry that ordered this operation (`OperationRegistry.begin`);
   * `undefined` for stand-alone operations (unit tests, barriers).
   */
  registry: OperationRegistry | undefined;

  /** Set by `OperationRegistry` when a predecessor of this one fails. */
  poisoned: unknown;

  /** Set by `OperationRegistry.end`: no longer orders later operations. */
  ended = false;

  /**
   * Called once when the operation stops needing its batch lane
   * (`yieldLane`): its rows are written, it parked, or it finished.
   */
  onYield: (() => void) | undefined;

  private seqValue: bigint | undefined;

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
    readonly nodes: readonly number[]
  ) {
    // never an unhandled rejection: callers that care await these explicitly
    this.commit.promise.catch(() => undefined);
  }

  get seq(): bigint | undefined {
    return this.seqValue;
  }

  /** The commit seq; the registry indexes live operations by it. */
  set seq(seq: bigint | undefined) {
    this.seqValue = seq;
    if (seq !== undefined) this.registry?.indexSeq(this, seq);
  }

  /**
   * The operations registered earlier that share a node with this one and
   * have not settled (committed, done or failed), as ONE dependency: a
   * barrier whose `committed` resolves once all of them have settled and
   * rejects if one of them failed. Empty when there are none, or once this
   * operation's rows are written (as before: predecessors are only
   * consulted while an operation decides its rows).
   */
  get predecessors(): StoreOperation[] {
    if (this.rowsDone || this.registry === undefined) return [];
    return this.registry.hasUnsettledEarlier(this)
      ? [new PredecessorBarrier(this, this.registry.earlierSettled(this))]
      : [];
  }

  /** Whether `markRowsWritten` (or a terminal mark) has run. */
  get hasWrittenRows() {
    return this.rowsDone;
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

  /** Give up the block-batch lane (idempotent; see clickhouse-store.ts). */
  yieldLane() {
    const { onYield } = this;
    this.onYield = undefined;
    onYield?.();
  }

  markRowsWritten() {
    this.yieldLane();
    if (!this.rowsDone) {
      this.rowsDone = true;
      this.rows.resolve();
      this.registry?.rowsWrittenBy(this);
    }
  }

  markCommitted() {
    this.markRowsWritten();
    if (this.state !== 'running') return;
    this.state = 'committed';
    this.commit.resolve();
    this.registry?.settled(this);
  }

  /** Finished without a commit (nothing to write). */
  markDone() {
    this.markRowsWritten();
    if (this.state !== 'running') return;
    this.state = 'done';
    this.commit.resolve();
    this.registry?.settled(this);
  }

  markFailed(error: unknown) {
    this.markRowsWritten();
    if (this.state !== 'running') return;
    this.state = 'failed';
    this.commit.reject(error);
    this.registry?.settled(this, error);
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

/**
 * One dependency standing for "every earlier unsettled operation sharing a
 * node with `owner`" (see `StoreOperation.predecessors`). Its state is
 * `done`, so it never contributes a `dependsOn` seq: what it waits for is
 * ordering, and each of those commits is terminal before `owner` commits.
 */
class PredecessorBarrier extends StoreOperation {
  constructor(
    owner: StoreOperation,
    private readonly settledAll: Promise<void>
  ) {
    super(-owner.id, 'predecessors', owner.nodes);
    this.state = 'done';
    this.settledAll.catch(() => undefined);
  }

  override get committed(): Promise<void> {
    return this.settledAll;
  }
}

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

  /**
   * Abandoned because the writer lease was lost: the work is re-run under
   * the next epoch, so an abandoned block save rejects instead of resolving
   * as handled.
   */
  retryable = false;

  readonly promise: Promise<never>;

  private rejectWith: (error: AbandonedError) => void = () => undefined;

  constructor() {
    this.promise = new Promise<never>((_, reject) => {
      this.rejectWith = reject;
    });
    this.promise.catch(() => undefined);
  }

  abandon(reason = 'store shutdown', retryable = false) {
    if (this.abandoned) return;
    this.abandoned = true;
    this.retryable = retryable;
    this.rejectWith(new AbandonedError(`Abandoned: ${reason}.`));
  }

  assertNotAbandoned() {
    if (this.abandoned) {
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

/** A min-heap of waiters by threshold (operation id). */
class WaiterHeap {
  private readonly items: { threshold: number; wake: () => void }[] = [];

  get size() {
    return this.items.length;
  }

  push(threshold: number, wake: () => void) {
    const { items } = this;
    items.push({ threshold, wake });
    let index = items.length - 1;
    while (index > 0) {
      const parent = (index - 1) >> 1;
      if (items[parent]!.threshold <= items[index]!.threshold) break;
      [items[parent], items[index]] = [items[index]!, items[parent]!];
      index = parent;
    }
  }

  /** Wake (and remove) every waiter whose threshold is at most `upTo`. */
  wakeUpTo(upTo: number) {
    const { items } = this;
    while (items.length > 0 && items[0]!.threshold <= upTo) {
      const top = items[0]!;
      const last = items.pop()!;
      if (items.length > 0) {
        items[0] = last;
        let index = 0;
        for (;;) {
          const left = 2 * index + 1;
          const right = left + 1;
          let smallest = index;
          if (
            left < items.length &&
            items[left]!.threshold < items[smallest]!.threshold
          ) {
            smallest = left;
          }
          if (
            right < items.length &&
            items[right]!.threshold < items[smallest]!.threshold
          ) {
            smallest = right;
          }
          if (smallest === index) break;
          [items[smallest], items[index]] = [items[index]!, items[smallest]!];
          index = smallest;
        }
      }
      top.wake();
    }
  }
}

/**
 * Operations of one node in registration (= id) order, with a lazily
 * advanced head: the oldest one for which `isOpen` still holds. Push and
 * amortized head advance are O(1).
 */
class OrderedQueue {
  private items: StoreOperation[] = [];

  private head = 0;

  readonly waiters = new WaiterHeap();

  constructor(
    private readonly isOpen: (operation: StoreOperation) => boolean
  ) {}

  push(operation: StoreOperation) {
    this.items.push(operation);
  }

  /** The id of the oldest open operation, or Infinity if none is open. */
  oldestOpenId(): number {
    while (
      this.head < this.items.length &&
      !this.isOpen(this.items[this.head]!)
    ) {
      this.head += 1;
    }
    const compactAt = 1_024;
    if (this.head >= compactAt && this.head * 2 >= this.items.length) {
      this.items = this.items.slice(this.head);
      this.head = 0;
    }
    return this.head < this.items.length ? this.items[this.head]!.id : Infinity;
  }

  /** Re-check after an operation closed: wake waiters it was holding. */
  advance() {
    if (this.waiters.size === 0) {
      this.oldestOpenId();
      return;
    }
    this.waiters.wakeUpTo(this.oldestOpenId());
  }

  get isEmpty() {
    return this.oldestOpenId() === Infinity;
  }
}

/** Per-node ordering state. */
class NodeLane {
  /** The operation registered last on this node. */
  last: StoreOperation | undefined;

  /** Registered and not yet ended (for failure propagation). */
  readonly live = new Set<StoreOperation>();

  /** Operations whose rows are not all written yet. */
  readonly rows = new OrderedQueue(
    (operation) => !operation.ended && !operation.hasWrittenRows
  );

  /** Operations not yet settled (committed, done or failed). */
  readonly unsettled = new OrderedQueue(
    (operation) => !operation.ended && !operation.finished
  );
}

/**
 * The registry of live operations. Registration is synchronous, so the order
 * of registration is the order of the store calls (the agent's order), and
 * operation ids increase in that order.
 *
 * Ordering (wp5a-core.md §4, unchanged): an operation's predecessors are the
 * operations registered earlier that share a node with it and were live at
 * its registration. Rather than copying them into every operation (O(live)
 * per operation, quadratic in queued operations: the WP6 catch-up OOM), each
 * node keeps its operations in id order with a head pointer to the oldest
 * one whose rows are not written (and one to the oldest unsettled one).
 * "Every predecessor on node n has written its rows" is then "the oldest
 * operation of n with unwritten rows is this one or newer"; waiters sit in
 * a min-heap by id and are woken when the head passes them. A failure marks
 * every live later operation on its nodes (`poisoned`), which is exactly the
 * set that had it as a predecessor. Memory: O(1) per operation per node.
 */
export class OperationRegistry {
  private readonly live = new Set<StoreOperation>();

  private readonly lanes = new Map<number, NodeLane>();

  private readonly bySeq = new Map<bigint, StoreOperation>();

  private nextId = 1;

  private readonly idle: (() => void)[] = [];

  get activeCount() {
    return this.live.size;
  }

  get liveOperations(): readonly StoreOperation[] {
    return [...this.live];
  }

  begin(kind: OperationKind, nodes: readonly number[]): StoreOperation {
    const sorted = [...new Set(nodes)].sort((a, b) => a - b);
    const operation = new StoreOperation(this.nextId, kind, sorted);
    operation.registry = this;
    this.nextId += 1;
    this.live.add(operation);
    sorted.forEach((node) => {
      const lane = this.lane(node);
      lane.live.add(operation);
      lane.last = operation;
      lane.rows.push(operation);
      lane.unsettled.push(operation);
    });
    return operation;
  }

  /** Remove a finished operation. */
  end(operation: StoreOperation) {
    if (!this.live.delete(operation)) return;
    operation.ended = true;
    operation.nodes.forEach((node) => {
      const lane = this.lanes.get(node);
      lane?.live.delete(operation);
      lane?.rows.advance();
      lane?.unsettled.advance();
    });
    if (
      operation.seq !== undefined &&
      this.bySeq.get(operation.seq) === operation
    ) {
      this.bySeq.delete(operation.seq);
    }
    if (this.live.size === 0) {
      this.idle.splice(0).forEach((resolve) => {
        resolve();
      });
    }
  }

  /** The live, unfinished operation that owns `seq`, if any. O(1). */
  operationOfSeq(seq: bigint): StoreOperation | undefined {
    const operation = this.bySeq.get(seq);
    return operation !== undefined && !operation.finished
      ? operation
      : undefined;
  }

  /** @internal called by `StoreOperation` when its seq is assigned. */
  indexSeq(operation: StoreOperation, seq: bigint) {
    if (this.live.has(operation)) this.bySeq.set(seq, operation);
  }

  /** @internal called by `StoreOperation.markRowsWritten`. */
  rowsWrittenBy(operation: StoreOperation) {
    operation.nodes.forEach((node) => {
      this.lanes.get(node)?.rows.advance();
    });
  }

  /** @internal called once when an operation settles. */
  settled(operation: StoreOperation, error?: unknown) {
    operation.nodes.forEach((node) => {
      const lane = this.lanes.get(node);
      if (lane === undefined) return;
      if (error !== undefined) {
        // rare (failures only): every live later operation had it as a predecessor
        lane.live.forEach((other) => {
          if (other.id > operation.id && other.poisoned === undefined) {
            other.poisoned = error;
          }
        });
      }
      lane.unsettled.advance();
    });
  }

  /** Whether no operation was registered on `operation`'s nodes after it. */
  isLatestOnItsNodes(operation: StoreOperation): boolean {
    return operation.nodes.every(
      (node) => this.lanes.get(node)?.last === operation
    );
  }

  /** Whether an earlier operation on a shared node is still unsettled. */
  hasUnsettledEarlier(operation: StoreOperation): boolean {
    return operation.nodes.some(
      (node) =>
        (this.lanes.get(node)?.unsettled.oldestOpenId() ?? Infinity) <
        operation.id
    );
  }

  /** Whether an earlier operation on a shared node has rows to write. */
  hasUnwrittenEarlier(operation: StoreOperation): boolean {
    return operation.nodes.some(
      (node) =>
        (this.lanes.get(node)?.rows.oldestOpenId() ?? Infinity) < operation.id
    );
  }

  /** Resolves once every earlier operation on a shared node has written its rows. */
  async earlierRowsWritten(operation: StoreOperation): Promise<void> {
    await this.waitForHeads(operation, (lane) => lane.rows);
  }

  /**
   * Resolves once every earlier operation on a shared node has settled;
   * rejects (`DependencyFailedError`) if one of them failed.
   */
  async earlierSettled(operation: StoreOperation): Promise<void> {
    await this.waitForHeads(operation, (lane) => lane.unsettled);
    if (operation.poisoned !== undefined) {
      throw new DependencyFailedError(
        `An earlier operation on nodes ${operation.nodes.join(
          ', '
        )} failed: ${String(operation.poisoned)}`
      );
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

  private lane(node: number): NodeLane {
    let lane = this.lanes.get(node);
    if (lane === undefined) {
      lane = new NodeLane();
      this.lanes.set(node, lane);
    }
    return lane;
  }

  private async waitForHeads(
    operation: StoreOperation,
    queueOf: (lane: NodeLane) => OrderedQueue
  ): Promise<void> {
    await Promise.all(
      operation.nodes.map(async (node) => {
        const lane = this.lanes.get(node);
        if (lane === undefined) return;
        const queue = queueOf(lane);
        if (queue.oldestOpenId() >= operation.id) return;
        await new Promise<void>((resolve) => {
          queue.waiters.push(operation.id, resolve);
        });
      })
    );
  }
}

/**
 * Wait until every predecessor of `operation` has written all its rows
 * (without holding an in-flight slot while waiting).
 */
export const waitForPredecessorRows = async (operation: StoreOperation) => {
  const { registry } = operation;
  if (registry === undefined || !registry.hasUnwrittenEarlier(operation)) {
    return;
  }
  await operation.whileWaiting(registry.earlierRowsWritten(operation));
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
