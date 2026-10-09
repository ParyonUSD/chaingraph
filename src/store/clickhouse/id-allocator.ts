/* eslint-disable max-classes-per-file, camelcase, @typescript-eslint/naming-convention, @typescript-eslint/parameter-properties, complexity */
// cspell:ignore clickhouse serialises
/**
 * Internal id allocation, hi/lo (plan §3.3, WP4).
 *
 * The writer keeps a counter per id kind and reserves ranges in
 * `id_reservation` before handing out any id from them (durable before use).
 * On start it resumes above the highest reservation, so a crash skips at most
 * the unused rest of one range per kind and never reuses an id.
 *
 * Ids from a fenced (stale) writer can overlap a new writer's ranges only if
 * the stale writer reserved after the takeover; such rows are invisible
 * (epoch fence), and the hash-to-id lookup must ignore them
 * (`gateSql.validCommit`). See docs/clickhouse-port/wp4-commit-and-visibility.md.
 */
import type { ClickHouseClient } from './client.js';

export type IdKind =
  | 'block'
  | 'node_block_history'
  | 'node_transaction_history'
  | 'node'
  | 'transaction';

/** A half-open run of ids `[start, end)`. */
export interface IdSegment {
  start: bigint;
  end: bigint;
}

/** Durable storage of reservations. */
export interface ReservationStore {
  /** The highest `range_end` reserved for `kind`, or 1n if none (ids start at 1). */
  highestReservedEnd: (kind: IdKind) => Promise<bigint>;
  /** Durably record `[start, end)`; resolves only once it is stored. */
  reserve: (kind: IdKind, start: bigint, end: bigint) => Promise<void>;
}

export const defaultRangeSizes: Readonly<{ [kind in IdKind]: bigint }> = {
  block: 100_000n,
  // node ids are UInt32 and nodes are few: small ranges
  node: 16n,
  node_block_history: 100_000n,
  node_transaction_history: 100_000n,
  transaction: 100_000n,
};

/** Expand segments into individual ids (for small allocations and tests). */
export const segmentIds = (segments: readonly IdSegment[]): bigint[] =>
  segments.flatMap((segment) => {
    const ids: bigint[] = [];
    // eslint-disable-next-line functional/no-loop-statement, functional/no-let
    for (let id = segment.start; id < segment.end; id += 1n) {
      ids.push(id);
    }
    return ids;
  });

export const segmentsLength = (segments: readonly IdSegment[]) =>
  segments.reduce(
    (total, segment) => total + (segment.end - segment.start),
    0n
  );

interface KindState {
  next: bigint;
  end: bigint;
  /** Serialises allocations of this kind (reservations are async). */
  chain: Promise<unknown>;
  loaded: boolean;
}

export class IdAllocator {
  private readonly states = new Map<IdKind, KindState>();

  private readonly rangeSizes: { [kind in IdKind]: bigint };

  private readonly assertHeld: () => void;

  constructor(
    private readonly store: ReservationStore,
    options: {
      rangeSizes?: Partial<{ [kind in IdKind]: bigint }>;
      /** Checked before each reservation (writer-lease.ts). */
      assertHeld?: () => void;
    } = {}
  ) {
    this.rangeSizes = { ...defaultRangeSizes, ...options.rangeSizes };
    this.assertHeld = options.assertHeld ?? (() => undefined);
  }

  /**
   * Allocate `count` ids of `kind`, in increasing order. Concurrent calls get
   * disjoint ids; each call's ids are greater than those of earlier calls.
   */
  async allocate(kind: IdKind, count: bigint | number): Promise<IdSegment[]> {
    const wanted = BigInt(count);
    if (wanted < 0n) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new RangeError(`Cannot allocate ${count} ids.`);
    }
    const state = this.stateOf(kind);
    const run = state.chain.then(async () => this.allocateNow(kind, wanted));
    state.chain = run.catch(() => undefined);
    return run;
  }

  async allocateOne(kind: IdKind): Promise<bigint> {
    const [segment] = await this.allocate(kind, 1);
    return segment!.start;
  }

  private stateOf(kind: IdKind): KindState {
    const existing = this.states.get(kind);
    if (existing !== undefined) {
      return existing;
    }
    const created: KindState = {
      chain: Promise.resolve(),
      end: 0n,
      loaded: false,
      next: 0n,
    };
    this.states.set(kind, created);
    return created;
  }

  private async allocateNow(kind: IdKind, wanted: bigint) {
    const state = this.stateOf(kind);
    if (!state.loaded) {
      const resumeAt = await this.store.highestReservedEnd(kind);
      state.next = resumeAt;
      state.end = resumeAt;
      state.loaded = true;
    }
    const segments: IdSegment[] = [];
    // eslint-disable-next-line functional/no-let
    let remaining = wanted;
    // eslint-disable-next-line functional/no-loop-statement
    while (remaining > 0n) {
      if (state.next === state.end) {
        const rangeSize: bigint = this.rangeSizes[kind];
        const size = remaining > rangeSize ? remaining : rangeSize;
        this.assertHeld();
        // eslint-disable-next-line no-await-in-loop
        await this.store.reserve(kind, state.end, state.end + size);
        state.end += size;
      }
      const available = state.end - state.next;
      const take = remaining < available ? remaining : available;
      segments.push({ end: state.next + take, start: state.next });
      state.next += take;
      remaining -= take;
    }
    return segments;
  }
}

/** `id_reservation`-backed store. */
export class ClickHouseReservationStore implements ReservationStore {
  constructor(
    private readonly client: Pick<ClickHouseClient, 'insertSelect' | 'query'>,
    private readonly writerEpoch: () => bigint
  ) {}

  async highestReservedEnd(kind: IdKind): Promise<bigint> {
    const rows = await this.client.query<{ highest: string }>(
      'SELECT greatest(toUInt64(1), max(range_end)) AS highest FROM id_reservation WHERE id_kind = {kind:String}',
      { kind }
    );
    return BigInt(rows[0]?.highest ?? '1');
  }

  async reserve(kind: IdKind, start: bigint, end: bigint): Promise<void> {
    const epoch = this.writerEpoch();
    await this.client.insertSelect(
      `INSERT INTO id_reservation (id_kind, range_start, range_end, writer_epoch, reserved_at)
       SELECT {kind:String}, {start:UInt64}, {end:UInt64}, {epoch:UInt64}, now64(3, 'UTC')`,
      { end, epoch, kind, start },
      { deduplicationToken: `${epoch}:id_reservation:${kind}:${start}` }
    );
  }
}
