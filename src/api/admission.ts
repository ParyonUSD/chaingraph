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
 * On the ClickHouse side each lane has its own settings (`priority`,
 * optionally a `workload`).
 *
 * Fix pass 7 (docs/clickhouse-port/fix-pass-7.md §1; the mainnet soak of
 * 2026-10-13 was worse with admission than without):
 * - refusals are fast: a lane's wait is bounded at 250 ms, and a statement
 *   whose predicted wait (from the running statements' ages and the lane's
 *   recent statement time) is over the bound is refused at once instead
 *   of after the wait;
 * - heavy statements are not capped at a fixed `max_threads` (the server's
 *   default: every core; ClickHouse's CPU slot scheduler shares the cores
 *   between running statements), and they get the lower `priority` only
 *   under contention (`contendedAt`): a heavy statement admitted while the
 *   heavy lane is nearly full, or after waiting in its queue. Priority
 *   always on protected light statements but slowed heavy ones at loads
 *   the server carries (local load test: worse than admission off);
 *   priority never on left light statements at p95 ~1 s under overload;
 * - a statement is heavy from 10 M rows read (was 1 M: mainnet point reads
 *   of 1–3 M rows, ~0.1 CPU-s, queued behind 12 CPU-s statements);
 * - lanes are wider: light = cores, heavy = cores / 2.
 * Lane sizes follow the ClickHouse
 * server's cores (`laneConcurrencyForCores`); a cluster-wide backstop across
 * API pods is the API user's `max_concurrent_queries_for_user`
 * (store/clickhouse/api-user.ts). Trusted documents get no lane of their
 * own: a trusted document's statement that reads ≥ `heavyRows` shares the
 * heavy lane (owner decision 2026-10-12, fix-pass-5.md §4).
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
  /**
   * Settings added to a heavy statement admitted under contention (default
   * `{ max_threads: 4, priority: 2 }`, threads derived from the cores: about
   * 2 cores per heavy slot; ClickHouse pauses it while light statements, at
   * priority 1, run).
   */
  contendedSettings: { [name: string]: number | string };
  /**
   * Heavy statements running (itself included) from which a newly admitted
   * one is contended, as a fraction of the heavy slots (default 1: it took
   * the last slot); a statement that waited in the queue is always
   * contended.
   */
  contendedAt: number;
  /**
   * The cores the lane sizes were derived from and where that number came
   * from (`env`: CHAINGRAPH_API_CLICKHOUSE_CORES, `clickhouse`: the
   * server's `max_threads` for the API user, `default`: neither, the
   * static defaults); for the startup log.
   */
  sizing?: {
    cores: number | undefined;
    source: 'clickhouse' | 'default' | 'env';
  };
}

/** Light statements at once per core of the ClickHouse server. */
export const lightConcurrentPerCore = 1;

/** The light lane never gets fewer slots than this (small servers). */
export const minLightConcurrent = 8;

/** Heavy queue length per heavy slot when the sizes are derived (24 at 24 slots). */
export const heavyQueuedPerSlot = 1;

/**
 * Heavy slots per core with the server's default threads: one heavy
 * statement per 2 cores (24 at 48 cores). Not a thread budget: under
 * contention ClickHouse shares the cores between them.
 */
export const heavyConcurrentPerCore = 0.5;

/** Longest wait for a slot in either lane by default, ms (fix-pass-7.md §1: refusals must be fast, under 250 ms). */
export const defaultMaxWaitMs = 250;

/**
 * Lane sizes from the ClickHouse server's cores (owner decision 2026-10-12,
 * fix-pass-4.md §8; revised in fix-pass-7.md §1). Heavy: with the server's
 * default threads (`heavyMaxThreads` 0, the default) max(2, round(cores /
 * 2)): 24 at 48 cores, 7 at 14; with a fixed `heavyMaxThreads` max(1,
 * round(cores / heavyMaxThreads)) as before. Light = max(8, cores): light
 * statements are point and range lookups of ~10–60 ms, mostly one thread;
 * under load they slow down, and half a slot per core (fix pass 5) refused
 * them in the local overload test. At 48 cores: 48 light, 24 heavy. The
 * heavy queue is 1 per heavy slot (24 at 24).
 */
export const laneConcurrencyForCores = (
  cores: number,
  heavyMaxThreads: number
) => {
  const usableCores = Math.max(1, Math.floor(cores));
  return {
    heavy:
      heavyMaxThreads > 0
        ? Math.max(1, Math.round(usableCores / heavyMaxThreads))
        : Math.max(2, Math.round(usableCores * heavyConcurrentPerCore)),
    light: Math.max(
      minLightConcurrent,
      Math.floor(usableCores * lightConcurrentPerCore)
    ),
  };
};

/**
 * Defaults (per API process) when the server's cores are unknown: the
 * lane sizes of a 48-core server (`laneConcurrencyForCores(48, 0)`): 48
 * light and 24 heavy statements at once; queues of 256 / 24, waits of at
 * most 250 ms (refused at once when the predicted wait is longer). Light
 * statements run at priority 1; heavy ones with the server's default
 * threads, at priority 2 only when contended. A shape is heavy from 10 M
 * rows read on average (fix-pass-7.md §1; 1 M before).
 */
export const defaultAdmissionOptions: AdmissionOptions = {
  contendedAt: 1,
  contendedSettings: { max_threads: 4, priority: 2 },
  enabled: true,
  heavy: {
    maxConcurrent: 24,
    maxQueued: 24,
    maxWaitMs: defaultMaxWaitMs,
    settings: {},
  },
  heavyRows: 10_000_000,
  light: {
    maxConcurrent: 48,
    maxQueued: 256,
    maxWaitMs: defaultMaxWaitMs,
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
 * CHAINGRAPH_API_CLICKHOUSE_CORES: the cores the lane sizes are derived
 * from (default: `serverCores`, the ClickHouse server's `max_threads` for
 * the API user, read at startup; without either the static defaults);
 * CHAINGRAPH_API_LIGHT_… / CHAINGRAPH_API_HEAVY_…: MAX_CONCURRENT (default
 * derived from the cores, `laneConcurrencyForCores`), MAX_QUEUED, MAX_WAIT_MS;
 * MAX_THREADS (0 or unset: the server's default),
 * _PRIORITY (0: none; for the heavy lane the priority of CONTENDED heavy
 * statements, default 2), _WORKLOAD (a ClickHouse workload name);
 * CHAINGRAPH_API_HEAVY_CONTENDED_AT (heavy statements running as a
 * fraction of the heavy slots from which a newly admitted one is contended,
 * default 1; 0: always), CHAINGRAPH_API_HEAVY_ROWS;
 * CHAINGRAPH_API_RETRY_AFTER_SECONDS.
 */
export const admissionOptionsFromEnv = (
  env: NodeJS.ProcessEnv = process.env,
  serverCores?: number
): AdmissionOptions => {
  const flag = env.CHAINGRAPH_API_ADMISSION ?? 'on';
  if (flag !== 'on' && flag !== 'off' && flag !== '') {
    throw new Error(`CHAINGRAPH_API_ADMISSION must be 'on' or 'off'.`);
  }
  const envCores = nonNegativeInteger(
    env,
    'CHAINGRAPH_API_CLICKHOUSE_CORES',
    0
  );
  const knownServerCores =
    serverCores !== undefined &&
    Number.isInteger(serverCores) &&
    serverCores > 0
      ? serverCores
      : undefined;
  const cores = envCores > 0 ? envCores : knownServerCores;
  const sizing: NonNullable<AdmissionOptions['sizing']> = {
    cores,
    source:
      envCores > 0
        ? 'env'
        : knownServerCores === undefined
        ? 'default'
        : 'clickhouse',
  };
  const heavySettings = laneSettingsFromEnv(
    env,
    'CHAINGRAPH_API_HEAVY',
    defaultAdmissionOptions.heavy.settings
  );
  /* the heavy lane's priority applies under contention only (fix-pass-7.md §1) */
  const contendedPriority = nonNegativeInteger(
    env,
    'CHAINGRAPH_API_HEAVY_PRIORITY',
    Number(defaultAdmissionOptions.contendedSettings.priority ?? 0)
  );
  const contendedAtRaw = env.CHAINGRAPH_API_HEAVY_CONTENDED_AT;
  const contendedAt =
    contendedAtRaw === undefined || contendedAtRaw === ''
      ? defaultAdmissionOptions.contendedAt
      : Number(contendedAtRaw);
  if (!Number.isFinite(contendedAt) || contendedAt < 0) {
    throw new Error('CHAINGRAPH_API_HEAVY_CONTENDED_AT must be a number >= 0.');
  }
  const derived =
    cores === undefined
      ? undefined
      : laneConcurrencyForCores(
          cores,
          typeof heavySettings.max_threads === 'number'
            ? heavySettings.max_threads
            : 0
        );
  const lane = (name: 'HEAVY' | 'LIGHT', defaults: LaneLimits) => {
    const prefix = `CHAINGRAPH_API_${name}`;
    const fallback: LaneLimits =
      derived === undefined
        ? defaults
        : {
            ...defaults,
            maxConcurrent: name === 'HEAVY' ? derived.heavy : derived.light,
            /*
             * the heavy queue holds about 2 statements per slot (24 at 12):
             * a deeper queue only turns fast refusals into long waits
             */
            maxQueued:
              name === 'HEAVY'
                ? derived.heavy * heavyQueuedPerSlot
                : defaults.maxQueued,
          };
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
    if (name === 'HEAVY') delete limits.settings.priority;
    if (limits.maxConcurrent < 1) {
      throw new Error(`${prefix}_MAX_CONCURRENT must be at least 1.`);
    }
    return limits;
  };
  const heavy = lane('HEAVY', defaultAdmissionOptions.heavy);
  /*
   * contended heavy statements share the cores: about 2 cores' worth of
   * threads per heavy slot (4 at 48 cores / 24 slots), unless set (0: none)
   */
  const contendedThreads = nonNegativeInteger(
    env,
    'CHAINGRAPH_API_HEAVY_CONTENDED_MAX_THREADS',
    cores === undefined
      ? Number(defaultAdmissionOptions.contendedSettings.max_threads ?? 0)
      : Math.min(
          Math.max(2, Math.round((2 * cores) / heavy.maxConcurrent)),
          typeof heavy.settings.max_threads === 'number'
            ? heavy.settings.max_threads
            : Number.POSITIVE_INFINITY
        )
  );
  return {
    contendedAt,
    contendedSettings: {
      ...(contendedThreads === 0 ? {} : { max_threads: contendedThreads }),
      ...(contendedPriority === 0 ? {} : { priority: contendedPriority }),
    },
    enabled: flag !== 'off',
    heavy,
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
    sizing,
  };
};

/**
 * What the API user's ClickHouse session reports at startup: `max_threads`
 * (its `auto` value is the server's usable cores: physical cores on hosts
 * with 64 or more logical CPUs, limited by the container's cgroup) and the
 * `max_concurrent_queries_for_user` backstop of the user's profile (0: none).
 * Both undefined when the server cannot be reached.
 */
export const readClickHouseServerLimits = async (client: {
  query: <T>(sql: string) => Promise<T[]>;
}): Promise<{
  maxThreads: number | undefined;
  maxConcurrentQueriesForUser: number | undefined;
}> => {
  try {
    const [row] = await client.query<{
      max_threads: number | string;
      max_concurrent_queries_for_user: number | string;
    }>(
      `SELECT toUInt64(getSetting('max_threads')) AS max_threads,
              toUInt64(getSetting('max_concurrent_queries_for_user')) AS max_concurrent_queries_for_user`
    );
    const asNumber = (value: number | string | undefined) => {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : undefined;
    };
    return {
      maxConcurrentQueriesForUser: asNumber(
        row?.max_concurrent_queries_for_user
      ),
      maxThreads: asNumber(row?.max_threads),
    };
  } catch {
    return { maxConcurrentQueriesForUser: undefined, maxThreads: undefined };
  }
};

/** One line describing the admission sizing, for the API's startup log. */
export const admissionSummary = (
  options: AdmissionOptions,
  backstop: number | undefined
) =>
  `admission: ${options.enabled ? 'on' : 'off'} cores=${
    options.sizing?.cores ?? 'unknown'
  } (${options.sizing?.source ?? 'default'}) light=${
    options.light.maxConcurrent
  } heavy=${options.heavy.maxConcurrent} heavy_max_threads=${String(
    options.heavy.settings.max_threads ?? 'server default'
  )} heavy_contended=${options.contendedAt}:${
    Object.entries(options.contendedSettings)
      .map(([name, value]) => `${name} ${String(value)}`)
      .join(',') || 'none'
  } heavy_rows=${options.heavyRows} max_wait_ms=${options.light.maxWaitMs}/${
    options.heavy.maxWaitMs
  } max_concurrent_queries_for_user=${
    backstop === undefined ? 'unknown' : backstop === 0 ? '0 (none)' : backstop
  }`;

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
  /** Refused at once: the predicted wait was longer than allowed. */
  rejectedPredicted: number;
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
  rejectedPredicted: 0,
  rejectedWait: 0,
  running: 0,
});

/** A slot taken: when, the lane's running statements then (itself included), and whether it waited. */
interface Slot {
  at: number;
  running: number;
  queued: boolean;
}

interface Waiter {
  resolve: (slot: Slot) => void;
  reject: (error: unknown) => void;
  timer: NodeJS.Timeout | undefined;
  onAbort: () => void;
  signal: AbortSignal | undefined;
}

/** One lane: a counting semaphore with a bounded FIFO queue. */
class LaneGate {
  readonly stats = emptyStats();

  /** Recent statement time of the lane (EWMA, ms; undefined until one finished). */
  serviceMs: number | undefined;

  private readonly queue: Waiter[] = [];

  /** Start times of the running statements. */
  private readonly running: Slot[] = [];

  /**
   * The wait a statement joining the queue now can expect (0 until the lane
   * has a measured statement time S): a running statement younger than S is
   * expected to end in S − its age, an older one in its age again (a
   * statement that has run long is likely a long one: heavy statements'
   * times spread over 10×); position p in the queue gets the p-th slot to
   * free (sorted), and every further round of slots adds S.
   */
  predictedWaitMs(now = Date.now()) {
    const service = this.serviceMs;
    if (service === undefined || this.running.length === 0) return 0;
    const remaining = this.running
      .map((slot) => {
        const age = now - slot.at;
        return age < service ? service - age : age;
      })
      .sort((a, b) => a - b);
    const position = this.queue.length;
    return (
      remaining[position % remaining.length]! +
      Math.floor(position / remaining.length) * service
    );
  }

  constructor(
    readonly lane: Lane,
    private readonly limits: LaneLimits,
    private readonly retryAfterSeconds: number
  ) {}

  /**
   * Wait for a slot (or fail fast); the caller must `release()` once done.
   * Resolves with the lane's running statements at the moment it took the
   * slot (itself included).
   */
  async acquire(signal?: AbortSignal): Promise<Slot> {
    if (signal?.aborted === true) throw requestAbortedError();
    if (
      this.stats.running < this.limits.maxConcurrent &&
      this.queue.length === 0
    ) {
      return this.take(false);
    }
    if (this.queue.length >= this.limits.maxQueued) {
      this.stats.rejectedFull += 1;
      throw overloadedError(
        this.lane,
        `${this.stats.running} running, ${this.queue.length} queued`,
        this.retryAfterSeconds
      );
    }
    const predicted = this.predictedWaitMs();
    if (predicted > this.limits.maxWaitMs) {
      /* refuse now rather than after the wait (fix-pass-7.md §1) */
      this.stats.rejectedPredicted += 1;
      throw overloadedError(
        this.lane,
        `${this.stats.running} running, ${
          this.queue.length
        } queued, predicted wait ${Math.round(predicted)} ms > ${
          this.limits.maxWaitMs
        } ms`,
        this.retryAfterSeconds
      );
    }
    return new Promise<Slot>((resolve, reject) => {
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

  /** Free `slot`; `measured`: its time counts as the lane's service time (not a failed or abandoned statement). */
  release(slot: Slot, measured = true) {
    const ms = Date.now() - slot.at;
    if (measured) {
      this.serviceMs =
        this.serviceMs === undefined ? ms : this.serviceMs * 0.8 + ms * 0.2;
    }
    const index = this.running.indexOf(slot);
    if (index >= 0) this.running.splice(index, 1);
    this.stats.running -= 1;
    const next = this.queue.shift();
    this.stats.queued = this.queue.length;
    if (next === undefined) return;
    clearTimeout(next.timer);
    next.signal?.removeEventListener('abort', next.onAbort);
    next.resolve(this.take(true));
  }

  private take(queued: boolean): Slot {
    this.stats.running += 1;
    this.stats.admitted += 1;
    this.stats.peakRunning = Math.max(
      this.stats.peakRunning,
      this.stats.running
    );
    const slot = { at: Date.now(), queued, running: this.stats.running };
    this.running.push(slot);
    return slot;
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

  /**
   * The ClickHouse settings of a statement admitted to `lane` (`slot`: the
   * lane's running statements then, itself included, and whether it
   * waited in the queue): the lane's settings, plus `contendedSettings`
   * for a contended heavy statement (it waited, or the heavy lane holds at
   * least `contendedAt` × its slots).
   */
  laneSettings(
    lane: Lane,
    slot: Pick<Slot, 'queued' | 'running'>
  ): LaneLimits['settings'] {
    const { settings } = this.options[lane];
    const contended =
      slot.queued ||
      slot.running >=
        Math.max(
          1,
          Math.ceil(this.options.contendedAt * this.options.heavy.maxConcurrent)
        );
    if (lane !== 'heavy' || !contended) return settings;
    return { ...settings, ...this.options.contendedSettings };
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
    const slot = await gate.acquire(signal);
    const outcome = { failed: false };
    try {
      return await send(this.laneSettings(lane, slot), report);
    } catch (error: unknown) {
      outcome.failed = true;
      /* an abandoned statement's failure says nothing about its cost */
      if (signal?.aborted !== true && costly(error)) {
        this.record(shape, this.failedCost);
      }
      throw error;
    } finally {
      /* a failed or abandoned statement's time says nothing about the lane's */
      gate.release(slot, !outcome.failed);
    }
  }
}
