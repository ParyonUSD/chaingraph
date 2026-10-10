/* eslint-disable no-bitwise, max-classes-per-file, @typescript-eslint/naming-convention, functional/no-mixed-type, functional/no-try-statement, @typescript-eslint/parameter-properties, @typescript-eslint/no-magic-numbers, complexity, max-params */
// cspell:ignore clickhouse dedup milli seqs unhex
/**
 * The commit protocol (plan §3.1, WP4): every unit of work is a commit with a
 * `commit_seq`. Its rows are invisible until the commit is `committed` and the
 * per-node watermark (visibility.ts) passes it; an `aborted` commit's rows stay
 * invisible forever (`commit_void`).
 *
 * `commit_seq = (writer_epoch << 40) | counter` (`counter >= 1`): each lease epoch
 * owns a disjoint range, so a stale writer can never reuse a live writer's seq
 * or dedup token, and its later commits are fenced by range (`epoch_fence`).
 *
 * Design, watermark semantics and the crash-consistency table:
 * docs/clickhouse-port/wp4-commit-and-visibility.md.
 */
import type { ClickHouseClient } from './client.js';

/** Bits of `commit_seq` below the writer epoch. */
export const epochShift = 40n;
const counterMask = (1n << epochShift) - 1n;

export const seqForEpoch = (epoch: bigint, counter: bigint) => {
  if (epoch < 1n || counter < 1n || counter > counterMask) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new RangeError(`Invalid commit_seq parts: ${epoch}/${counter}`);
  }
  return (epoch << epochShift) | counter;
};
export const epochOfSeq = (seq: bigint) => seq >> epochShift;
export const counterOfSeq = (seq: bigint) => seq & counterMask;
/** The highest seq epoch `epoch` can ever use. */
export const lastSeqOfEpoch = (epoch: bigint) =>
  ((epoch + 1n) << epochShift) - 1n;

/**
 * The insert deduplication token of one step of one commit: `seq:table:chunk`.
 * `chunk` is deterministic for the commit's content (0, 1, … per table, or a
 * named late step such as `f0` for a pending-spend fill).
 */
export const dedupToken = (
  seq: bigint,
  table: string,
  chunk: number | string = 0
) => `${seq}:${table}:${chunk}`;

/**
 * Values of the `commit_log.kind` enum (ddl/040_bookkeeping.sql). `backfill`
 * is the bulk data commit of an offline backfill (all-zero `block_hash`); the
 * agent never writes it.
 */
export const commitKinds = [
  'backfill',
  'block',
  'expiry',
  'fill_pending',
  'header_accept',
  'horizon_switch',
  'mempool_batch',
  'reorg',
  'utxo_build',
] as const;

export type CommitKind = (typeof commitKinds)[number];

export type CommitState = 'aborted' | 'committed' | 'incomplete' | 'intent';

const terminalStates: ReadonlySet<CommitState> = new Set([
  'aborted',
  'committed',
]);

/** What the commit log needs from the writer lease (writer-lease.ts). */
export interface CommitLease {
  /** The held epoch; throws if no lease is held. */
  readonly epoch: bigint;
  /** Throws if the lease is lost or its local deadline has passed. */
  assertHeld: () => void;
}

/** The subset of `ClickHouseClient` the commit log uses (stubbed in unit tests). */
export type CommitLogClient = Pick<ClickHouseClient, 'insertSelect' | 'query'>;

export interface BeginCommitOptions {
  kind: CommitKind;
  /** Nodes whose facts this commit changes; empty = node-agnostic data only. */
  nodeScope: readonly number[];
  /** 32-byte block hash (hex) for block commits. */
  blockHashHex?: string;
  /**
   * Commits whose rows this commit's facts reference (e.g. a block commit
   * accepting a tx whose rows were written by an in-flight mempool commit).
   * `markCommitted` refuses until every one of them is committed.
   */
  dependsOn?: readonly bigint[];
}

export interface OpenCommit {
  readonly seq: bigint;
  readonly epoch: bigint;
  readonly kind: CommitKind;
  readonly nodeScope: readonly number[];
  /** `seq:table:chunk`, the dedup token of one insert of this commit. */
  token: (table: string, chunk?: number | string) => string;
}

export interface CommitRecord {
  seq: bigint;
  state: CommitState;
  kind: CommitKind;
  nodeScope: number[];
  blockHashHex: string;
  rowCounts: { [key: string]: bigint };
  writerEpoch: bigint;
  startedAt: string;
  startedAtMs: number;
  finishedAt: string | null;
  abortReason: string;
}

export interface EpochFence {
  epoch: bigint;
  maxValidSeq: bigint;
}

export interface RecoveryResult {
  /** Commits found non-terminal and aborted now (re-queue their work). */
  aborted: CommitRecord[];
  /** Fences written now (older epochs not fenced before). */
  fences: EpochFence[];
  /** The highest seq in the log before this epoch; every seq up to it is resolved. */
  lastSeq: bigint;
}

interface TrackedCommit {
  seq: bigint;
  state: CommitState;
  kind: CommitKind;
  nodeScope: number[];
  blockHashHex: string;
  dependsOn: bigint[];
  startedAtMs: number;
}

export class CommitDependencyError extends Error {}

/** The commit log of a lost lease epoch: it never writes again. */
export class CommitLogRetiredError extends Error {}
export class CommitStateError extends Error {}

const zeroHash = '';

const commitLogInsert = `INSERT INTO commit_log
  (commit_seq, state, node_scope, kind, block_hash, row_counts, writer_epoch, started_at, finished_at, abort_reason)
SELECT
  {seq:UInt64}, {state:String}, {scope:Array(UInt32)}, {kind:String},
  toFixedString(unhex({blockHash:String}), 32),
  mapFromArrays({countKeys:Array(String)}, {countValues:Array(UInt64)}),
  {epoch:UInt64},
  fromUnixTimestamp64Milli({startedAtMs:Int64}, 'UTC'),
  if({finished:UInt8} = 1, fromUnixTimestamp64Milli({finishedAtMs:Int64}, 'UTC'), NULL),
  {reason:String}`;

const commitVoidInsert = `INSERT INTO commit_void (commit_seq, reason, writer_epoch, voided_at)
SELECT {seq:UInt64}, {reason:String}, {epoch:UInt64}, now64(3, 'UTC')`;

interface CommitLogRow {
  commit_seq: string;
  state: CommitState;
  kind: CommitKind;
  node_scope: number[];
  block_hash_hex: string;
  row_counts: { [key: string]: string };
  writer_epoch: string;
  started_at_text: string;
  started_at_ms: string;
  finished_at_text: string | null;
  abort_reason: string;
}

const commitLogColumns = `commit_seq, state, kind, node_scope, lower(hex(block_hash)) AS block_hash_hex,
  row_counts, writer_epoch, toUnixTimestamp64Milli(started_at) AS started_at_ms,
  toString(started_at) AS started_at_text, toString(finished_at) AS finished_at_text, abort_reason`;

const toRecord = (row: CommitLogRow): CommitRecord => ({
  abortReason: row.abort_reason,
  blockHashHex: /^0+$/u.test(row.block_hash_hex) ? '' : row.block_hash_hex,
  finishedAt: row.finished_at_text,
  kind: row.kind,
  nodeScope: row.node_scope.map(Number),
  rowCounts: Object.fromEntries(
    Object.entries(row.row_counts).map(([table, count]) => [
      table,
      BigInt(count),
    ])
  ),
  seq: BigInt(row.commit_seq),
  startedAt: row.started_at_text,
  startedAtMs: Number(row.started_at_ms),
  state: row.state,
  writerEpoch: BigInt(row.writer_epoch),
});

export class CommitLog {
  /** Non-terminal commits of this writer (and, during recovery, none else). */
  private readonly open = new Map<bigint, TrackedCommit>();

  /** Aborted seqs seen by this process (tiny: crashes and failures only). */
  private readonly aborted = new Set<bigint>();

  private readonly terminalListeners: ((seq: bigint) => void)[] = [];

  private nextCounter = 1n;

  private lastAllocated = 0n;

  private initialized = false;

  private retiredReason: string | undefined;

  constructor(
    private readonly client: CommitLogClient,
    private readonly lease: CommitLease,
    private readonly now: () => number = Date.now
  ) {}

  /** Whether `retire` was called. */
  get isRetired() {
    return this.retiredReason !== undefined;
  }

  /** The highest seq allocated so far (or, before any, the last recovered seq). */
  get lastAllocatedSeq() {
    return this.lastAllocated;
  }

  /**
   * Call once after the lease is acquired: recover (abort non-terminal commits
   * of older epochs), fence older epochs, and position the seq counter.
   */
  async init(): Promise<RecoveryResult> {
    const result = await this.recoverIncomplete(this.lease.epoch);
    this.lastAllocated = result.lastSeq;
    this.nextCounter = 1n;
    this.initialized = true;
    return result;
  }

  /**
   * The writer lease of this log's epoch is lost: from now on every write
   * (begin, incomplete, committed, aborted) throws `CommitLogRetiredError`
   * without touching ClickHouse. The next holder's `init()` aborts and fences
   * whatever this epoch left open (wp6b-gate-cost.md §6.2).
   */
  retire(reason: string) {
    this.retiredReason ??= reason;
  }

  /** Non-terminal commits, for the watermark publisher. */
  openCommits(): readonly { seq: bigint; nodeScope: readonly number[] }[] {
    return [...this.open.values()];
  }

  /** In-process view of a commit's state; `undefined` if unknown here. */
  stateOf(seq: bigint): CommitState | undefined {
    const tracked = this.open.get(seq);
    if (tracked !== undefined) {
      return tracked.state;
    }
    if (this.aborted.has(seq)) {
      return 'aborted';
    }
    return seq <= this.lastAllocated ? 'committed' : undefined;
  }

  /** Called after every transition to a terminal state (committed or aborted). */
  onTerminal(listener: (seq: bigint) => void) {
    this.terminalListeners.push(listener);
  }

  /**
   * Step 1: allocate a seq and write its `intent` row. The seq is tracked as
   * non-terminal before the insert is sent, so the watermark never passes it.
   */
  async beginCommit(options: BeginCommitOptions): Promise<OpenCommit> {
    if (!this.initialized) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new CommitStateError('CommitLog.init() has not run.');
    }
    this.assertNotRetired();
    this.lease.assertHeld();
    const { epoch } = this.lease;
    const seq = seqForEpoch(epoch, this.nextCounter);
    this.nextCounter += 1n;
    this.lastAllocated = seq;
    const tracked: TrackedCommit = {
      blockHashHex: options.blockHashHex ?? zeroHash,
      dependsOn: [...(options.dependsOn ?? [])],
      kind: options.kind,
      nodeScope: [...options.nodeScope],
      seq,
      startedAtMs: this.now(),
      state: 'intent',
    };
    this.open.set(seq, tracked);
    try {
      await this.writeState(tracked, 'intent', {});
    } catch (error) {
      await this.markAborted(seq, 'intent insert failed').catch(() => {
        // the commit stays non-terminal in memory: the watermark holds until restart recovers it
      });
      // eslint-disable-next-line functional/no-throw-statement
      throw error;
    }
    return {
      epoch,
      kind: tracked.kind,
      nodeScope: tracked.nodeScope,
      seq,
      token: (table: string, chunk?: number | string) =>
        dedupToken(seq, table, chunk),
    };
  }

  /**
   * The commit has unresolved pending spends (plan §3.5); its late rows will be
   * written under the same seq. Still non-terminal: it holds the watermark.
   */
  async markIncomplete(seq: bigint): Promise<void> {
    const tracked = this.requireOpen(seq);
    await this.writeState(tracked, 'incomplete', {});
    tracked.state = 'incomplete';
  }

  /**
   * Step 3: all data rows are acknowledged; write `committed` with the row
   * counts that were sent. Refuses if a dependency is not committed.
   */
  async markCommitted(
    seq: bigint,
    rowCounts: { [key: string]: bigint | number }
  ): Promise<void> {
    const tracked = this.requireOpen(seq);
    tracked.dependsOn.forEach((dependency) => {
      const state = this.stateOf(dependency);
      if (state !== 'committed') {
        // eslint-disable-next-line functional/no-throw-statement
        throw new CommitDependencyError(
          `Commit ${seq} depends on ${dependency}, which is ${
            state ?? 'unknown'
          }.`
        );
      }
    });
    this.lease.assertHeld();
    await this.writeState(tracked, 'committed', rowCounts);
    this.open.delete(seq);
    this.emitTerminal(seq);
  }

  /**
   * Abort: write `commit_void` first (the gate hides the seq from then on),
   * then commit_log's `aborted` row. Idempotent.
   */
  async markAborted(
    seq: bigint,
    reason: string,
    record?: CommitRecord
  ): Promise<void> {
    const tracked: TrackedCommit = this.open.get(seq) ??
      (record === undefined
        ? undefined
        : {
            blockHashHex: record.blockHashHex,
            dependsOn: [],
            kind: record.kind,
            nodeScope: record.nodeScope,
            seq,
            startedAtMs: record.startedAtMs,
            state: record.state,
          }) ?? {
        blockHashHex: zeroHash,
        dependsOn: [],
        kind: 'block',
        nodeScope: [],
        seq,
        startedAtMs: this.now(),
        state: 'intent',
      };
    this.assertNotRetired();
    await this.client.insertSelect(
      commitVoidInsert,
      { epoch: this.lease.epoch, reason, seq },
      { deduplicationToken: dedupToken(seq, 'commit_void') }
    );
    await this.writeState(tracked, 'aborted', {}, reason);
    this.aborted.add(seq);
    this.open.delete(seq);
    this.emitTerminal(seq);
  }

  /** Incomplete commits older than `maxAgeMs` (to abort and re-queue). */
  staleIncomplete(maxAgeMs: number): bigint[] {
    const cutoff = this.now() - maxAgeMs;
    return [...this.open.values()]
      .filter(
        (commit) => commit.state === 'incomplete' && commit.startedAtMs < cutoff
      )
      .map((commit) => commit.seq);
  }

  /**
   * Startup recovery for `epoch` (the newly acquired lease epoch):
   * 1. read, per older epoch not yet fenced, its highest seq in the log;
   * 2. abort every non-terminal commit above the node-agnostic watermark;
   * 3. fence every older epoch not yet fenced at that highest seq (dense).
   * A stale writer's commits after step 1 are fenced; see the doc for the race.
   */
  async recoverIncomplete(epoch: bigint): Promise<RecoveryResult> {
    const fencedRows = await this.client.query<{ max_epoch: string }>(
      'SELECT max(epoch) AS max_epoch FROM epoch_fence'
    );
    const maxFencedEpoch = BigInt(fencedRows[0]?.max_epoch ?? '0');
    const perEpoch = await this.client.query<{
      epoch: string;
      max_seq: string;
    }>(
      `SELECT bitShiftRight(commit_seq, 40) AS epoch, max(commit_seq) AS max_seq
       FROM commit_log
       WHERE commit_seq >= {low:UInt64} AND commit_seq < {high:UInt64}
       GROUP BY epoch`,
      {
        high: epoch << epochShift,
        low: (maxFencedEpoch + 1n) << epochShift,
      }
    );
    const maxSeqByEpoch = new Map(
      perEpoch.map((row) => [BigInt(row.epoch), BigInt(row.max_seq)])
    );
    const lastRows = await this.client.query<{ last_seq: string }>(
      'SELECT max(commit_seq) AS last_seq FROM commit_log'
    );
    const lastSeq = BigInt(lastRows[0]?.last_seq ?? '0');
    if (lastSeq >= epoch << epochShift) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new CommitStateError(
        `commit_log already has seq ${lastSeq} in epoch >= ${epoch}: another writer holds this epoch.`
      );
    }

    const nonTerminal = await this.client.query<CommitLogRow>(
      `SELECT ${commitLogColumns}
       FROM (SELECT * FROM commit_log FINAL
             WHERE commit_seq > (SELECT max(visible_seq) FROM visibility WHERE node_internal_id = 0)
               AND commit_seq < {high:UInt64})
       WHERE state IN ('intent', 'incomplete')
       ORDER BY commit_seq`,
      { high: epoch << epochShift }
    );
    const aborted = nonTerminal.map(toRecord);
    // eslint-disable-next-line functional/no-loop-statement
    for (const record of aborted) {
      // eslint-disable-next-line no-await-in-loop
      await this.markAborted(
        record.seq,
        `recovered at startup by epoch ${epoch} (was ${record.state})`,
        record
      );
    }

    const fences: EpochFence[] = [];
    // eslint-disable-next-line functional/no-loop-statement, functional/no-let
    for (let older = maxFencedEpoch + 1n; older < epoch; older += 1n) {
      fences.push({
        epoch: older,
        maxValidSeq: maxSeqByEpoch.get(older) ?? older << epochShift,
      });
    }
    if (fences.length > 0) {
      await this.client.insertSelect(
        `INSERT INTO epoch_fence (epoch, max_valid_seq, fenced_by_epoch, fenced_at)
         SELECT fence.1, fence.2, {epoch:UInt64}, now64(3, 'UTC')
         FROM (SELECT arrayJoin(arrayZip({epochs:Array(UInt64)}, {maxSeqs:Array(UInt64)})) AS fence)`,
        {
          epoch,
          epochs: fences.map((fence) => fence.epoch),
          maxSeqs: fences.map((fence) => fence.maxValidSeq),
        },
        { deduplicationToken: `${epoch}:epoch_fence:0` }
      );
    }
    return { aborted, fences, lastSeq };
  }

  /** Commits in a seq range, final state, for the checker. */
  async listCommits(
    options: {
      fromSeq?: bigint;
      toSeq?: bigint;
      states?: readonly CommitState[];
      limit?: number;
    } = {}
  ): Promise<CommitRecord[]> {
    const defaultLimit = 10_000;
    const rows = await this.client.query<CommitLogRow>(
      `SELECT ${commitLogColumns}
       FROM (SELECT * FROM commit_log FINAL
             WHERE commit_seq >= {fromSeq:UInt64} AND commit_seq <= {toSeq:UInt64})
       WHERE length({states:Array(String)}) = 0 OR has({states:Array(String)}, toString(state))
       ORDER BY commit_seq
       LIMIT {limit:UInt64}`,
      {
        fromSeq: options.fromSeq ?? 0n,
        limit: options.limit ?? defaultLimit,
        states: [...(options.states ?? [])],
        toSeq: options.toSeq ?? (1n << 64n) - 1n,
      }
    );
    return rows.map(toRecord);
  }

  private requireOpen(seq: bigint): TrackedCommit {
    const tracked = this.open.get(seq);
    if (tracked === undefined || terminalStates.has(tracked.state)) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new CommitStateError(
        `Commit ${seq} is not open (${this.stateOf(seq) ?? 'unknown'}).`
      );
    }
    return tracked;
  }

  private emitTerminal(seq: bigint) {
    this.terminalListeners.forEach((listener) => {
      listener(seq);
    });
  }

  private assertNotRetired() {
    if (this.retiredReason !== undefined) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new CommitLogRetiredError(
        `Commit log of a lost lease epoch: ${this.retiredReason}`
      );
    }
  }

  private async writeState(
    tracked: TrackedCommit,
    state: CommitState,
    rowCounts: { [key: string]: bigint | number },
    reason = ''
  ) {
    this.assertNotRetired();
    const entries = Object.entries(rowCounts);
    const finished = terminalStates.has(state);
    await this.client.insertSelect(
      commitLogInsert,
      {
        blockHash: tracked.blockHashHex,
        countKeys: entries.map(([table]) => table),
        countValues: entries.map(([, count]) => BigInt(count)),
        epoch: epochOfSeq(tracked.seq),
        finished: finished ? 1 : 0,
        finishedAtMs: finished ? this.now() : 0,
        kind: tracked.kind,
        reason,
        scope: tracked.nodeScope,
        seq: tracked.seq,
        startedAtMs: tracked.startedAtMs,
        state,
      },
      { deduplicationToken: dedupToken(tracked.seq, 'commit_log', state) }
    );
  }
}
