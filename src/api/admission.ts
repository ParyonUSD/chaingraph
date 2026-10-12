/* eslint-disable functional/no-throw-statement, @typescript-eslint/no-magic-numbers, max-classes-per-file, camelcase, @typescript-eslint/naming-convention, complexity, functional/no-mixed-type, @typescript-eslint/parameter-properties, max-params, functional/no-try-statement, @typescript-eslint/member-ordering */
// cspell:ignore clickhouse EWMA
/**
 * Admission control of the API's ClickHouse statements (fix pass 4,
 * docs/clickhouse-port/fix-pass-4.md §3b).
 *
 * Every data statement passes a gate before it is sent. Two lanes:
 * - `light`: statements whose shape has been cheap (or is new);
 * - `heavy`: every shape whose recent cost (an EWMA per statement shape:
 *   label + SQL text, parameters excluded) is at least `heavyRows`, and
 *   aggregates until their shape has been measured. The cost is the rows a
 *   statement reads as ClickHouse reports them (`X-ClickHouse-Summary`:
 *   the larger of `read_rows` and `total_rows_to_read`), which does not
 *   grow with load as wall time does (a wall-time rule moved every shape to
 *   the heavy lane under load in the local measurement, fix-pass-4.md); a
 *   statement that times out or runs out of memory counts as heavy.
 * Each lane has a concurrency limit, a bounded queue and a bounded wait.
 * A statement that finds its lane's queue full, or waits longer than the
 * lane allows, fails at once with OVERLOADED (HTTP 503, Retry-After): a fast,
 * explicit error instead of a slow timeout, and no unbounded backlog. A
 * request that goes away while its statement waits leaves the queue
 * (`signal`). Heavy statements can never take the light lane's slots, so
 * cheap lookups keep their latency while heavy ones queue or are refused.
 * On the ClickHouse side each lane has its own settings (`max_threads`,
 * `priority`, optionally a `workload`).
 */
import { createHash } from 'node:crypto';

import { GraphQLError } from 'graphql';

export type Lane = 'heavy' | 'light';

export interface LaneLimits {
  /** Statements of the lane running at once (per API process). */
  maxConcurrent: number;
  /** Statements waiting for a slot; one more is refused (OVERLOADED). */
  maxQueued: number;
  /** Longest wait for a slot, ms; longer is refused (OVERLOADED). */
  maxWaitMs: number;
  /** ClickHouse settings of the lane's statements (`max_threads`, `priority`, `workload` …). */
  settings: { [name: string]: number | string };
}

export interface AdmissionOptions {
  /** Off: no gate (every statement is sent at once, as before fix pass 4). */
  enabled: boolean;
  light: LaneLimits;
  heavy: LaneLimits;
  /** A shape whose cost EWMA (rows read) reaches this is heavy. */
  heavyRows: number;
  /** Seconds a client is told to wait (Retry-After) when refused. */
  retryAfterSeconds: number;
  /** Shapes remembered (least recently used dropped). */
  maxShapes: number;
}

/**
 * Defaults (per API process): 24 light and 6 heavy statements at once;
 * queues of 256 / 24 waiting at most 5 s / 10 s. Heavy statements run with
 * at most 8 threads and a lower priority than light ones. A shape is heavy
 * from 1 M rows read on average (the F3 restatement of 2026-10-12: key
 * analysis is judged only below about 1 M rows read).
 */
export const defaultAdmissionOptions: AdmissionOptions = {
  enabled: true,
  heavy: {
    maxConcurrent: 6,
    maxQueued: 24,
    maxWaitMs: 10_000,
    settings: { max_threads: 8, priority: 2 },
  },
  heavyRows: 1_000_000,
  light: {
    maxConcurrent: 24,
    maxQueued: 256,
    maxWaitMs: 5_000,
    settings: { priority: 1 },
  },
  maxShapes: 10_000,
  retryAfterSeconds: 2,
};

const nonNegativeInteger = (
  env: NodeJS.ProcessEnv,
  name: string,
  fallback: number
) => {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be an integer >= 0.`);
  }
  return value;
};

const laneSettingsFromEnv = (
  env: NodeJS.ProcessEnv,
  prefix: string,
  fallback: LaneLimits['settings']
) => {
  const settings: LaneLimits['settings'] = { ...fallback };
  const threads = env[`${prefix}_MAX_THREADS`];
  if (threads !== undefined && threads !== '') {
    const value = nonNegativeInteger(env, `${prefix}_MAX_THREADS`, 0);
    if (value === 0) delete settings.max_threads;
    else settings.max_threads = value;
  }
  const priority = env[`${prefix}_PRIORITY`];
  if (priority !== undefined && priority !== '') {
    const value = nonNegativeInteger(env, `${prefix}_PRIORITY`, 0);
    if (value === 0) delete settings.priority;
    else settings.priority = value;
  }
  const workload = env[`${prefix}_WORKLOAD`];
  if (workload !== undefined && workload !== '') {
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(workload)) {
      throw new Error(`${prefix}_WORKLOAD must be a workload name.`);
    }
    settings.workload = workload;
  }
  return settings;
};

/**
 * From the environment (all optional):
 * CHAINGRAPH_API_ADMISSION (`on` | `off`, default on);
 * CHAINGRAPH_API_LIGHT_… / CHAINGRAPH_API_HEAVY_…: MAX_CONCURRENT, MAX_QUEUED, MAX_WAIT_MS;
 * MAX_THREADS (0: the server's default),
 * _PRIORITY (0: none), _WORKLOAD (a ClickHouse workload name);
 * CHAINGRAPH_API_HEAVY_ROWS; CHAINGRAPH_API_RETRY_AFTER_SECONDS.
 */
export const admissionOptionsFromEnv = (
  env: NodeJS.ProcessEnv = process.env
): AdmissionOptions => {
  const flag = env.CHAINGRAPH_API_ADMISSION ?? 'on';
  if (flag !== 'on' && flag !== 'off' && flag !== '') {
    throw new Error(`CHAINGRAPH_API_ADMISSION must be 'on' or 'off'.`);
  }
  const lane = (name: 'HEAVY' | 'LIGHT', fallback: LaneLimits) => {
    const prefix = `CHAINGRAPH_API_${name}`;
    const limits: LaneLimits = {
      maxConcurrent: nonNegativeInteger(
        env,
        `${prefix}_MAX_CONCURRENT`,
        fallback.maxConcurrent
      ),
      maxQueued: nonNegativeInteger(
        env,
        `${prefix}_MAX_QUEUED`,
        fallback.maxQueued
      ),
      maxWaitMs: nonNegativeInteger(
        env,
        `${prefix}_MAX_WAIT_MS`,
        fallback.maxWaitMs
      ),
      settings: laneSettingsFromEnv(env, prefix, fallback.settings),
    };
    if (limits.maxConcurrent < 1) {
      throw new Error(`${prefix}_MAX_CONCURRENT must be at least 1.`);
    }
    return limits;
  };
  return {
    enabled: flag !== 'off',
    heavy: lane('HEAVY', defaultAdmissionOptions.heavy),
    heavyRows: nonNegativeInteger(
      env,
      'CHAINGRAPH_API_HEAVY_ROWS',
      defaultAdmissionOptions.heavyRows
    ),
    light: lane('LIGHT', defaultAdmissionOptions.light),
    maxShapes: defaultAdmissionOptions.maxShapes,
    retryAfterSeconds: Math.max(
      1,
      nonNegativeInteger(
        env,
        'CHAINGRAPH_API_RETRY_AFTER_SECONDS',
        defaultAdmissionOptions.retryAfterSeconds
      )
    ),
  };
};

/** The error code of a refused statement. */
export const overloadedCode = 'OVERLOADED';

/** The error code of a statement of a request whose client went away. */
export const requestAbortedCode = 'REQUEST_ABORTED';

export const overloadedError = (
  lane: Lane,
  reason: string,
  retryAfterSeconds: number
) =>
  new GraphQLError(
    `The API is overloaded (${lane} statements: ${reason}); retry after ${retryAfterSeconds} s.`,
    {
      extensions: {
        code: overloadedCode,
        http: {
          headers: { 'Retry-After': String(retryAfterSeconds) },
          status: 503,
        },
        lane,
        retryAfterSeconds,
      },
    }
  );

export const requestAbortedError = () =>
  new GraphQLError(
    'The request was abandoned by its client; its statements were dropped or cancelled.',
    { extensions: { code: requestAbortedCode } }
  );

export interface LaneStats {
  running: number;
  queued: number;
  admitted: number;
  /** Refused: queue full. */
  rejectedFull: number;
  /** Refused: waited longer than allowed. */
  rejectedWait: number;
  /** Left the queue because the request went away. */
  abandoned: number;
  /** Highest `running` seen. */
  peakRunning: number;
  /** Highest `queued` seen. */
  peakQueued: number;
}

const emptyStats = (): LaneStats => ({
  abandoned: 0,
  admitted: 0,
  peakQueued: 0,
  peakRunning: 0,
  queued: 0,
  rejectedFull: 0,
  rejectedWait: 0,
  running: 0,
});

interface Waiter {
  resolve: () => void;
  reject: (error: unknown) => void;
  timer: NodeJS.Timeout | undefined;
  onAbort: () => void;
  signal: AbortSignal | undefined;
}

/** One lane: a counting semaphore with a bounded FIFO queue. */
class LaneGate {
  readonly stats = emptyStats();

  private readonly queue: Waiter[] = [];

  constructor(
    readonly lane: Lane,
    private readonly limits: LaneLimits,
    private readonly retryAfterSeconds: number
  ) {}

  /** Wait for a slot (or fail fast); the caller must `release()` once done. */
  async acquire(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted === true) throw requestAbortedError();
    if (
      this.stats.running < this.limits.maxConcurrent &&
      this.queue.length === 0
    ) {
      this.take();
      return;
    }
    if (this.queue.length >= this.limits.maxQueued) {
      this.stats.rejectedFull += 1;
      throw overloadedError(
        this.lane,
        `${this.stats.running} running, ${this.queue.length} queued`,
        this.retryAfterSeconds
      );
    }
    await new Promise<void>((resolve, reject) => {
      const waiter: Waiter = {
        onAbort: () => {
          this.remove(waiter);
          this.stats.abandoned += 1;
          reject(requestAbortedError());
        },
        reject,
        resolve,
        signal,
        timer: undefined,
      };
      waiter.timer = setTimeout(() => {
        this.remove(waiter);
        this.stats.rejectedWait += 1;
        reject(
          overloadedError(
            this.lane,
            `no slot within ${this.limits.maxWaitMs} ms`,
            this.retryAfterSeconds
          )
        );
      }, this.limits.maxWaitMs);
      signal?.addEventListener('abort', waiter.onAbort, { once: true });
      this.queue.push(waiter);
      this.stats.queued = this.queue.length;
      this.stats.peakQueued = Math.max(
        this.stats.peakQueued,
        this.queue.length
      );
    });
  }

  release() {
    this.stats.running -= 1;
    const next = this.queue.shift();
    this.stats.queued = this.queue.length;
    if (next === undefined) return;
    clearTimeout(next.timer);
    next.signal?.removeEventListener('abort', next.onAbort);
    this.take();
    next.resolve();
  }

  private take() {
    this.stats.running += 1;
    this.stats.admitted += 1;
    this.stats.peakRunning = Math.max(
      this.stats.peakRunning,
      this.stats.running
    );
  }

  private remove(waiter: Waiter) {
    clearTimeout(waiter.timer);
    waiter.signal?.removeEventListener('abort', waiter.onAbort);
    const index = this.queue.indexOf(waiter);
    if (index >= 0) this.queue.splice(index, 1);
    this.stats.queued = this.queue.length;
  }
}

/** The statement shape a cost is remembered by: label + SQL text (no parameter values). */
export const statementShape = (label: string, sql: string) =>
  createHash('sha256').update(label).update('\0').update(sql).digest('hex');

/** The gate of one API process. */
export class Admission {
  readonly lanes: { [lane in Lane]: LaneGate };

  /** Cost EWMA per statement shape, rows read (insertion order = recency). */
  private readonly costs = new Map<string, number>();

  constructor(readonly options: AdmissionOptions = defaultAdmissionOptions) {
    this.lanes = {
      heavy: new LaneGate('heavy', options.heavy, options.retryAfterSeconds),
      light: new LaneGate('light', options.light, options.retryAfterSeconds),
    };
  }

  /** The lane of a statement: heavy for shapes with a heavy history and for unmeasured aggregates. */
  laneOf(shape: string, aggregate: boolean): Lane {
    const cost = this.costs.get(shape);
    if (cost === undefined) return aggregate ? 'heavy' : 'light';
    return cost >= this.options.heavyRows ? 'heavy' : 'light';
  }

  /** The cost recorded for a statement that timed out or ran out of memory. */
  get failedCost() {
    return Math.max(1, this.options.heavyRows) * 10;
  }

  /** Record a statement's cost (rows read). */
  record(shape: string, rows: number) {
    const before = this.costs.get(shape);
    this.costs.delete(shape);
    /* a costly run counts at once; cheap runs bring a heavy shape back slowly */
    this.costs.set(
      shape,
      before === undefined || rows > before ? rows : before * 0.7 + rows * 0.3
    );
    if (this.costs.size > this.options.maxShapes) {
      const oldest = this.costs.keys().next().value;
      if (oldest !== undefined) this.costs.delete(oldest);
    }
  }

  /**
   * Run `send` once the statement is admitted to its lane, with the lane's
   * ClickHouse settings; `send` reports the rows the statement read
   * (`report`), and a statement that failed with `costly(error)` true
   * counts as heavy. Without admission (`enabled` false) `send` runs at
   * once with no extra settings.
   */
  async run<T>(
    shape: string,
    aggregate: boolean,
    signal: AbortSignal | undefined,
    send: (
      settings: LaneLimits['settings'],
      report: (rows: number) => void
    ) => Promise<T>,
    costly: (error: unknown) => boolean = () => false
  ): Promise<T> {
    const report = (rows: number) => {
      this.record(shape, rows);
    };
    if (!this.options.enabled) {
      if (signal?.aborted === true) throw requestAbortedError();
      return send({}, report);
    }
    const lane = this.laneOf(shape, aggregate);
    const gate = this.lanes[lane];
    await gate.acquire(signal);
    try {
      return await send(this.options[lane].settings, report);
    } catch (error: unknown) {
      /* an abandoned statement's failure says nothing about its cost */
      if (signal?.aborted !== true && costly(error)) {
        this.record(shape, this.failedCost);
      }
      throw error;
    } finally {
      gate.release();
    }
  }
}
