/* eslint-disable functional/no-throw-statement, @typescript-eslint/no-magic-numbers, max-classes-per-file, camelcase, @typescript-eslint/naming-convention, complexity, @typescript-eslint/parameter-properties, max-params, functional/no-try-statement, @typescript-eslint/member-ordering, functional/no-let, functional/no-loop-statement, max-lines */
// cspell:ignore clickhouse EWMA utilisation misprediction mispredicted unticketed uncontended
/**
 * Admission control of API requests (fix pass 8,
 * docs/clickhouse-port/fix-pass-8.md; replaces the per-statement lanes of
 * fix passes 4–7).
 *
 * The mainnet session of 2026-10-13 showed per-statement admission failing
 * at scale: a refusal landed only after a request's earlier statements had
 * run (OVERLOADED after seconds), statement counts said nothing about CPU,
 * and the binding resource at 20 QPS was not ClickHouse (≤ 16 of 48 cores
 * busy) but the API process's one event loop (light documents whose
 * statements took ~10 ms waited 15 s). So:
 *
 * - **Request level.** A request is admitted or refused once, before any
 *   statement (or snapshot read) runs, in ~1 ms; an admitted request is
 *   never refused part-way. Response-cache hits are not admitted at all
 *   (they cost neither resource).
 * - **Predicted cost.** Each request's cost is predicted from its plan key
 *   (document hash + operation + nodes + the magnitude of its numeric and
 *   list variables), falling back to the document, then to its root fields,
 *   then to a default: an EWMA of what such requests cost, in two
 *   resources: ClickHouse CPU-seconds (from `system.query_log`, polled for
 *   the API's own statements; rows read × a learned CPU per row until then
 *   or when the log is not readable) and event-loop milliseconds of this
 *   process (the loop's busy time, attributed to the requests not waiting
 *   for ClickHouse).
 * - **CPU budgets, not counts.** Per resource a token budget refills at a
 *   rate (ClickHouse: server cores × target utilisation / API processes;
 *   event loop: target utilisation × 1 s/s) up to a short burst. An
 *   admitted request is charged its prediction; the charge is reconciled
 *   with the measured cost when it is known. A budget may go into debt
 *   (a heavy request is admitted while its budget is positive), so a single
 *   request larger than the burst still runs, and the debt refuses the next
 *   ones until repaid.
 * - **Light reserve.** Each budget is split: light requests (predicted
 *   below `lightMaxCpuMs` and `lightMaxLoopMs`) have their own share
 *   (`lightShare`); heavy requests can never spend it, but every unused
 *   token of either share overflows into the other, so heavy requests use
 *   the whole budget when light ones are idle, and light ones borrow idle
 *   heavy tokens.
 * - **Fast refusals.** A request whose budget is exhausted is refused at
 *   once: OVERLOADED, HTTP 503, Retry-After = the budget's time to repay.
 * - **Idle never refuses.** With no request in flight (and the loop not
 *   busy) a request is admitted whatever the levels, and any debt is
 *   forgiven: debt only rations concurrent load (fix pass 9).
 * - **Backstops kept.** Heavy requests in flight per process are capped
 *   (`heavyMaxInFlight`), an event loop busier than `loopGuard` refuses heavy
 *   requests whatever the budget says (a misprediction must not stall the
 *   loop), the API user's `max_concurrent_queries_for_user` bounds the
 *   statements of every pod, and a request whose client goes away has its
 *   statements cancelled (db.ts).
 *
 * Statements outside a request (live-query re-runs, snapshot and watermark
 * reads) are never refused; their measured cost is charged to the budgets.
 */
import { createHash } from 'node:crypto';
import { performance } from 'node:perf_hooks';

import type { DocumentNode, OperationDefinitionNode } from 'graphql';
import { getOperationAST, GraphQLError, Kind, print } from 'graphql';

export type RequestClass = 'heavy' | 'light';

/** A request's cost in the two budgeted resources. */
export interface RequestCost {
  /** ClickHouse CPU (user + system), seconds. */
  cpuSeconds: number;
  /** This process's event loop, milliseconds. */
  loopMs: number;
  /**
   * Rows the statements read (ClickHouse's summary): known when they end
   * and the same at any thread count, unlike the CPU (`smallMaxRows`).
   */
  rows?: number;
  /**
   * Data parts the statements selected (`query_log` SelectedParts, summed):
   * known when the CPU is, and the same at any thread count. With `rows`,
   * the rows per part (`smallMaxRowsPerPart`).
   */
  parts?: number;
}

export interface BudgetOptions {
  /** Tokens per second (CPU-seconds per second, or loop milliseconds per second). */
  ratePerSecond: number;
  /** Capacity, seconds of `ratePerSecond`. */
  burstSeconds: number;
  /** The light requests' share of rate and capacity (0–1). */
  lightShare: number;
}

export interface AdmissionOptions {
  /** Off: every request runs at once (no prediction, no charge). */
  enabled: boolean;
  /** ClickHouse CPU budget of this process. */
  clickhouse: BudgetOptions;
  /** Event-loop budget of this process. */
  loop: BudgetOptions;
  /** A request predicted at most this (CPU ms) and `lightMaxLoopMs` is light. */
  lightMaxCpuMs: number;
  /** A request predicted at most this (loop ms) and `lightMaxCpuMs` is light. */
  lightMaxLoopMs: number;
  /** Heavy requests in flight at most (this process). */
  heavyMaxInFlight: number;
  /** Loop utilisation (last ~0.5 s) from which heavy requests are refused regardless of the budget (0–1; 1: never). */
  loopGuard: number;
  /** The prediction for a request nothing is known about (counted heavy unless under both light bounds). */
  unknownCost: RequestCost;
  /** ClickHouse CPU per row read until measured, seconds. */
  defaultCpuPerRow: number;
  /** Where the measured ClickHouse CPU comes from: `query_log` (exact, lagging) or `rows` (estimate). */
  cpuSource: 'query_log' | 'rows';
  /** Seconds a client is told to wait at least (Retry-After). */
  minRetryAfterSeconds: number;
  /** Plan keys remembered (least recently used dropped). */
  maxKeys: number;
  /** ClickHouse settings of light and heavy requests' statements. */
  settings: { [cls in RequestClass]: { [name: string]: number | string } };
  /** Settings of a small request (light, predicted to read at most `smallMaxRows`), under `settings.light`. */
  smallSettings: { [name: string]: number | string };
  /** Predicted rows read up to which a light request is small. */
  smallMaxRows: number;
  /**
   * Predicted rows per selected part above which a light request is not
   * small (once its parts are measured); 0: no such bound.
   */
  smallMaxRowsPerPart: number;
  /**
   * Settings added to a heavy request admitted under contention (another
   * heavy request in flight, and its budget share below `contendedFill`
   * of its capacity or half the heavy in-flight cap taken): about 2 cores'
   * worth of threads per heavy slot, so light statements find cores. An
   * uncontended heavy request (one at a time, whatever the past debt)
   * runs with the server's threads.
   */
  contendedSettings: { [name: string]: number | string };
  /** Heavy-share fill (0–1) under which a newly admitted heavy request is contended. */
  contendedFill: number;
  /** The sizing inputs (startup log). */
  sizing?: {
    cores: number | undefined;
    source: 'clickhouse' | 'default' | 'env';
    processes: number;
    cpuTarget: number;
    loopTarget: number;
    /** Heavy-lane threads per process (heavy-lane.ts). */
    heavyThreads?: number;
  };
}

/** Fraction of the ClickHouse server's cores the API's requests may use on average. */
export const defaultCpuTarget = 0.9;

/** Fraction of the event loop the requests may use on average. */
export const defaultLoopTarget = 0.85;

/** Cores assumed when the server's are unknown. */
export const defaultCores = 48;

/**
 * Settings of a small request: light and predicted to read at most
 * `smallMaxRows` rows (fix-pass-12.md §2, fix-pass-13.md §5). Such a
 * document is a handful of small reads, each split over the server's
 * default thread count (48 on golden); 8 threads made them faster for less
 * CPU (golden #107, v2-only A/B: X7 -26, X13 -72, L21 -13, L22 -14 ms; all
 * read at most 2.8 M rows), while light documents reading more were slower
 * (L6 +22 ms at 3.5 M rows, L7 +26 at 7.2 M).
 *
 * By rows, not CPU (fix 12 used a bound of 150 predicted CPU-ms): a request's CPU
 * depends on the threads it ran with and arrives late (`query_log`), so the
 * class depended on the model's history (X7: 0.21 CPU-s at the server's
 * threads, below 0.15 at 8, so it stayed in whichever class it entered;
 * 131 vs 104 ms in the same session). Rows are known when the statements
 * end and do not depend on the threads.
 */
export const defaultSmallSettings = { max_threads: 8 };
export const defaultSmallMaxRows = 3_000_000;

/**
 * Rows per selected part above which a light request is not small
 * (fix-pass-14.md §1). The 8 threads help a document of many small reads
 * (each read a few granules from each of many parts: X13 8.7 k rows per
 * part, L34 29.6 k); they slow one that scans a few large parts, where the
 * server's threads split real work (golden #110: X2, the node tips, 1.22 M
 * rows in 4 parts, 305 k per part, 67 ms at 8 threads vs 42 at the default;
 * L41 / L43 / X14 / X15, the block tips and counts, 526–568 k per part, all
 * faster at the default). Golden's light documents leave a gap from 37.5 k
 * (L6) to 305 k rows per part; 100 k is near its geometric middle. Parts
 * come from `query_log` with the CPU (a few seconds late) and, like rows,
 * do not depend on the threads, so a document's class changes at most once
 * (when its parts arrive) and never flips back.
 */
export const defaultSmallMaxRowsPerPart = 100_000;

/**
 * Whether a light request's predicted cost makes it small: rows known and
 * at most `smallMaxRows`, and, once its parts are measured, at most
 * `smallMaxRowsPerPart` rows per part (fix-pass-14.md §1).
 */
export const isSmall = (
  cost: RequestCost,
  options: Pick<AdmissionOptions, 'smallMaxRows' | 'smallMaxRowsPerPart'>
) =>
  cost.rows !== undefined &&
  cost.rows <= options.smallMaxRows &&
  (options.smallMaxRowsPerPart <= 0 ||
    cost.parts === undefined ||
    cost.rows <= options.smallMaxRowsPerPart * Math.max(1, cost.parts));

/**
 * Defaults (one process, 48 cores): ClickHouse 43.2 CPU-s/s (90 % of the
 * cores) with a 4 s burst (a burst of ~173 CPU-s: mainnet's heaviest
 * catalogue request, L15, is 59 CPU-s; a 2 s burst and a 75 % target refused
 * at loads admission off carried, fix-pass-8.md §4), the loop 850 ms/s with a
 * 1 s burst, a quarter of each for light requests;
 * light = predicted ≤ 500 CPU-ms and ≤ 25 loop-ms (mainnet's light
 * documents cost 0.09–0.29 CPU-s); a request never seen
 * 250 CPU-ms and 20 loop-ms (heavy: over neither bound but not proven light);
 * heavy requests in flight ≤ 24; heavy refused while the loop is ≥ 95 % busy.
 */
export const defaultAdmissionOptions: AdmissionOptions = {
  clickhouse: {
    burstSeconds: 4,
    lightShare: 0.25,
    ratePerSecond: defaultCores * defaultCpuTarget,
  },
  contendedFill: 0.5,
  contendedSettings: { max_threads: 4 },
  cpuSource: 'query_log',
  defaultCpuPerRow: 0.3e-6,
  enabled: true,
  heavyMaxInFlight: 24,
  lightMaxCpuMs: 500,
  lightMaxLoopMs: 25,
  loop: {
    burstSeconds: 1,
    lightShare: 0.25,
    ratePerSecond: 1000 * defaultLoopTarget,
  },
  loopGuard: 0.95,
  maxKeys: 10_000,
  minRetryAfterSeconds: 1,
  settings: { heavy: {}, light: {} },
  smallMaxRows: defaultSmallMaxRows,
  smallMaxRowsPerPart: defaultSmallMaxRowsPerPart,
  smallSettings: defaultSmallSettings,
  unknownCost: { cpuSeconds: 0.25, loopMs: 20 },
};

/** Options for an embedded API (tests, tools): no admission. */
export const disabledAdmissionOptions: AdmissionOptions = {
  ...defaultAdmissionOptions,
  enabled: false,
};

/* ------------------------------------------------------------------ */
/* errors                                                               */
/* ------------------------------------------------------------------ */

/** The error code of a refused request. */
export const overloadedCode = 'OVERLOADED';

/** The error code of a statement of a request whose client went away. */
export const requestAbortedCode = 'REQUEST_ABORTED';

export type RefusalReason = 'clickhouse' | 'in_flight' | 'loop_guard' | 'loop';

export const overloadedError = (
  cls: RequestClass,
  reason: RefusalReason,
  detail: string,
  retryAfterSeconds: number
) =>
  new GraphQLError(
    `The API is overloaded (${cls} request: ${detail}); retry after ${retryAfterSeconds} s.`,
    {
      extensions: {
        code: overloadedCode,
        http: {
          headers: { 'Retry-After': String(retryAfterSeconds) },
          status: 503,
        },
        lane: cls,
        reason,
        retryAfterSeconds,
      },
    }
  );

export const requestAbortedError = () =>
  new GraphQLError(
    'The request was abandoned by its client; its statements were dropped or cancelled.',
    { extensions: { code: requestAbortedCode } }
  );

/* ------------------------------------------------------------------ */
/* the budget of one resource                                           */
/* ------------------------------------------------------------------ */

/**
 * A token budget split into a light and a heavy share. Each share refills
 * at its part of the rate up to its part of the capacity; what a full share
 * would gain overflows into the other. Levels may go negative (debt).
 */
export class Budget {
  readonly level: { [cls in RequestClass]: number };

  private readonly rate: { [cls in RequestClass]: number };

  private readonly capacity: { [cls in RequestClass]: number };

  private at: number;

  constructor(options: BudgetOptions, now = performance.now()) {
    const share = Math.min(1, Math.max(0, options.lightShare));
    const capacity = options.ratePerSecond * options.burstSeconds;
    this.rate = {
      heavy: options.ratePerSecond * (1 - share),
      light: options.ratePerSecond * share,
    };
    this.capacity = { heavy: capacity * (1 - share), light: capacity * share };
    this.level = { ...this.capacity };
    this.at = now;
  }

  /** Add the tokens earned since the last call (overflow between the shares). */
  refill(now = performance.now()) {
    const seconds = Math.max(0, (now - this.at) / 1000);
    this.at = Math.max(this.at, now);
    if (seconds === 0) return;
    const gained = {
      heavy: this.rate.heavy * seconds,
      light: this.rate.light * seconds,
    };
    const room = (cls: RequestClass) =>
      Math.max(0, this.capacity[cls] - this.level[cls]);
    const spill = {
      heavy: Math.max(0, gained.heavy - room('heavy')),
      light: Math.max(0, gained.light - room('light')),
    };
    this.level.heavy = Math.min(
      this.capacity.heavy,
      this.level.heavy + gained.heavy + spill.light
    );
    this.level.light = Math.min(
      this.capacity.light,
      this.level.light + gained.light + spill.heavy
    );
  }

  /**
   * Charge `cost` for a request of class `cls`, if allowed: a light request
   * from its share while that is positive, else from the heavy share if
   * that holds the whole cost; a heavy request from its share while that is
   * positive. Returns the share charged, or undefined (refused).
   */
  take(cls: RequestClass, cost: number): RequestClass | undefined {
    if (this.level[cls] > 0) {
      this.level[cls] -= cost;
      return cls;
    }
    if (cls === 'light' && this.level.heavy >= cost) {
      this.level.heavy -= cost;
      return 'heavy';
    }
    return undefined;
  }

  /**
   * Charge `cost` to a share unconditionally (an idle process admits
   * whatever its levels: `Admission.admit`).
   */
  charge(cls: RequestClass, cost: number): RequestClass {
    this.level[cls] -= cost;
    return cls;
  }

  /** Forgive debt: every share below zero back to zero (an idle process). */
  forgive() {
    this.level.heavy = Math.max(0, this.level.heavy);
    this.level.light = Math.max(0, this.level.light);
  }

  /** Give back (a positive `amount`) or charge more (a negative one) to a share. */
  adjust(cls: RequestClass, amount: number) {
    this.level[cls] = Math.min(this.capacity[cls], this.level[cls] + amount);
  }

  /** Seconds until a refused request of `cls` would be admitted (at least 0). */
  secondsToPositive(cls: RequestClass) {
    const deficit = -this.level[cls];
    return deficit <= 0 ? 0 : deficit / Math.max(1e-9, this.rate[cls]);
  }

  /** Levels as fractions of the capacities (stats). */
  fill() {
    return {
      heavy: this.level.heavy / Math.max(1e-9, this.capacity.heavy),
      light: this.level.light / Math.max(1e-9, this.capacity.light),
    };
  }
}

/* ------------------------------------------------------------------ */
/* the cost model                                                       */
/* ------------------------------------------------------------------ */

/** The keys a request's cost is predicted by, most specific first. */
export interface PlanKeys {
  /** Document + operation + nodes + variable magnitudes. */
  plan: string;
  /** Document + operation. */
  document: string;
  /** The operation's root fields. */
  roots: string;
}

const sha = (text: string) =>
  createHash('sha256').update(text).digest('hex').slice(0, 32);

const documentHashes = new WeakMap<DocumentNode, string>();

/** sha256 of a parsed document's printed text (cached per parsed document). */
export const documentHash = (document: DocumentNode) => {
  let hash = documentHashes.get(document);
  if (hash === undefined) {
    hash = sha(print(document));
    documentHashes.set(document, hash);
  }
  return hash;
};

const byText = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** Magnitude class of a number (0, then log2 buckets). */
const magnitude = (value: number) =>
  value <= 0 ? 0 : Math.ceil(Math.log2(value + 1));

/**
 * What of a variable value changes a request's cost: numbers by magnitude
 * (a `limit` of 10 vs 10,000), lists by length magnitude (a list of hashes),
 * objects by their fields; strings not at all (a hash is a hash).
 */
const variableShape = (value: unknown, depth = 0): unknown => {
  if (depth > 4) return '…';
  if (typeof value === 'number') return magnitude(value);
  if (typeof value === 'bigint') return magnitude(Number(value));
  if (Array.isArray(value)) return `[${magnitude(value.length)}]`;
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as { [key: string]: unknown })
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([key, item]) => [key, variableShape(item, depth + 1)])
    );
  }
  if (typeof value === 'boolean') return value;
  return '';
};

/** The plan keys of one request. */
export const planKeys = (
  document: DocumentNode,
  operationName: string | null | undefined,
  variables: { [name: string]: unknown } | null | undefined,
  nodeNames: readonly string[]
): PlanKeys => {
  const operation: OperationDefinitionNode | null | undefined = getOperationAST(
    document,
    operationName ?? undefined
  );
  const roots = (operation?.selectionSet.selections ?? [])
    .map((selection) =>
      selection.kind === Kind.FIELD ? selection.name.value : '…'
    )
    .sort(byText)
    .join(',');
  const documentKey = `${documentHash(document)}:${operationName ?? ''}`;
  return {
    document: `d:${documentKey}`,
    plan: `p:${documentKey}:${[...nodeNames]
      .sort(byText)
      .join(',')}:${JSON.stringify(variableShape(variables ?? {}))}`,
    roots: `r:${operation?.operation ?? 'query'}:${roots}`,
  };
};

interface Estimate {
  cpuSeconds: number;
  loopMs: number;
  rows?: number;
  parts?: number;
  samples: number;
}

/** Weight of a new observation in the cost EWMAs. */
const costAlpha = 0.3;

/**
 * Cost EWMAs per key (insertion order = recency). The budgets are
 * reconciled with measured costs, so the model predicts the mean, not a
 * bound.
 */
export class CostModel {
  private readonly estimates = new Map<string, Estimate>();

  constructor(private readonly maxKeys: number) {}

  /** The prediction for `keys`: the most specific key with a sample, and which. */
  predict(
    keys: PlanKeys
  ): { cost: RequestCost; source: keyof PlanKeys } | undefined {
    for (const source of ['plan', 'document', 'roots'] as const) {
      const estimate = this.estimates.get(keys[source]);
      if (estimate !== undefined) {
        return {
          cost: {
            cpuSeconds: estimate.cpuSeconds,
            loopMs: estimate.loopMs,
            ...(estimate.rows === undefined ? {} : { rows: estimate.rows }),
            ...(estimate.parts === undefined ? {} : { parts: estimate.parts }),
          },
          source,
        };
      }
    }
    return undefined;
  }

  /**
   * Record a measured cost under every key of the request. `cpuSeed`: the
   * CPU is an estimate awaiting its measurement: it only seeds a key that
   * has none (the measurement is observed when it arrives).
   */
  observe(keys: PlanKeys, cost: RequestCost, cpuSeed = false) {
    for (const key of [keys.plan, keys.document, keys.roots]) {
      const before = this.estimates.get(key);
      this.estimates.delete(key);
      const blend = (old: number | undefined, value: number) =>
        old === undefined ? value : old * (1 - costAlpha) + value * costAlpha;
      this.estimates.set(key, {
        cpuSeconds:
          cpuSeed && before !== undefined
            ? before.cpuSeconds
            : blend(before?.cpuSeconds, cost.cpuSeconds),
        loopMs: blend(before?.loopMs, cost.loopMs),
        ...(cost.rows === undefined
          ? before?.rows === undefined
            ? {}
            : { rows: before.rows }
          : { rows: blend(before?.rows, cost.rows) }),
        ...(cost.parts === undefined
          ? before?.parts === undefined
            ? {}
            : { parts: before.parts }
          : { parts: blend(before?.parts, cost.parts) }),
        samples: (before?.samples ?? 0) + 1,
      });
    }
    while (this.estimates.size > this.maxKeys) {
      const oldest = this.estimates.keys().next().value;
      if (oldest === undefined) break;
      this.estimates.delete(oldest);
    }
  }

  get size() {
    return this.estimates.size;
  }

  /** Document keys with their estimates (`GET /admission`), most recent last, at most 200. */
  documents() {
    return Object.fromEntries(
      [...this.estimates]
        .filter(([key]) => key.startsWith('d:'))
        .slice(-200)
        .map(([key, estimate]) => [
          key.slice(2, 10) + key.slice(key.lastIndexOf(':')),
          {
            cpuSeconds: Number(estimate.cpuSeconds.toFixed(3)),
            loopMs: Number(estimate.loopMs.toFixed(1)),
            samples: estimate.samples,
          },
        ])
    );
  }
}

/* ------------------------------------------------------------------ */
/* tickets                                                              */
/* ------------------------------------------------------------------ */

/** One statement of a request (or a statement outside one). */
export interface StatementRecord {
  queryId: string;
  shape: string;
  rows: number;
  /** Measured CPU, s (`query_log`); undefined until known. */
  cpuSeconds?: number;
  /** Selected data parts (`query_log` SelectedParts); undefined until known. */
  parts?: number;
  /** When the statement finished (performance.now()), for the ledger's patience. */
  finishedAt?: number;
  /** Outside a request: the CPU estimate it was charged when it finished. */
  chargedCpu?: number;
}

/** An admitted request: what it was charged, and what it costs. */
export class Ticket {
  /** Statements sent, finished or not. */
  readonly statements: StatementRecord[] = [];

  /** Statements waiting for ClickHouse (sent, response not started). */
  waiting = 0;

  /** Loop milliseconds attributed so far. */
  loopMs = 0;

  /** Finished (response written or execution done). */
  finished = false;

  /** ClickHouse settings of the request's statements (its class's, plus the contended ones). */
  settings: { [name: string]: number | string } = {};

  /**
   * Admitted small (light, few rows read in small parts: `isSmall`): its
   * statements also match relationship levels by key texts, not group
   * numbers (db.ts `statement`; fix-pass-14.md §2).
   */
  small = false;

  /** CPU charged to the ClickHouse budget so far (prediction, then estimates). */
  chargedCpu: number;

  /** Loop ms charged to the loop budget so far. */
  chargedLoop: number;

  constructor(
    readonly keys: PlanKeys | undefined,
    readonly cls: RequestClass,
    readonly predicted: RequestCost,
    /** The share each budget was charged from (a light request may borrow heavy tokens). */
    readonly shares: { clickhouse: RequestClass; loop: RequestClass },
    readonly startedAt: number
  ) {
    this.chargedCpu = predicted.cpuSeconds;
    this.chargedLoop = predicted.loopMs;
  }
}

/* ------------------------------------------------------------------ */
/* stats                                                                */
/* ------------------------------------------------------------------ */

export interface AdmissionStats {
  admitted: { [cls in RequestClass]: number };
  refused: { [reason in RefusalReason]: number };
  /** Requests whose cost had no sample of their own plan key. */
  unknownPlans: number;
  inFlight: { [cls in RequestClass]: number };
  peakInFlight: { [cls in RequestClass]: number };
  /** Statements with a measured CPU from `query_log`. */
  ledgerResolved: number;
  /** Statements whose `query_log` row never came (estimate kept). */
  ledgerExpired: number;
  /** The ledger's last error, if it stopped. */
  ledgerError?: string;
  /** Statements outside requests (live re-runs) charged. */
  unticketed: number;
  /** Heavy requests admitted under contention (`contendedSettings`). */
  contended: number;
}

/* ------------------------------------------------------------------ */
/* the gate                                                             */
/* ------------------------------------------------------------------ */

/** What `query_log` tells of a finished statement: CPU (user + system, µs) and the data parts it selected. */
export interface StatementMeasurement {
  cpuMicros: number;
  /** SelectedParts; undefined: not reported. */
  parts?: number;
}

/** Reads the measurements of statements by query id; a missing id: not logged yet. */
export type CpuReader = (
  queryIds: readonly string[]
) => Promise<Map<string, StatementMeasurement>>;

/** Loop busy time since the previous call, ms (default: `performance.eventLoopUtilization`). */
export type LoopMeter = () => { activeMs: number; elapsedMs: number };

const defaultLoopMeter = (): LoopMeter => {
  let last = performance.eventLoopUtilization();
  return () => {
    const now = performance.eventLoopUtilization();
    const delta = performance.eventLoopUtilization(now, last);
    last = now;
    return { activeMs: delta.active, elapsedMs: delta.active + delta.idle };
  };
};

/** How long the ledger waits for a statement's `query_log` row before keeping the estimate, ms. */
export const ledgerPatienceMs = 60_000;

/** Ledger poll period, ms. */
export const ledgerPollMs = 2_000;

/** Loop attribution period, ms. */
export const loopTickMs = 100;

/** The admission gate of one API process. */
export class Admission {
  readonly stats: AdmissionStats = {
    admitted: { heavy: 0, light: 0 },
    contended: 0,
    inFlight: { heavy: 0, light: 0 },
    ledgerExpired: 0,
    ledgerResolved: 0,
    peakInFlight: { heavy: 0, light: 0 },
    refused: { clickhouse: 0, in_flight: 0, loop: 0, loop_guard: 0 },
    unknownPlans: 0,
    unticketed: 0,
  };

  readonly budgets: { clickhouse: Budget; loop: Budget };

  readonly model: CostModel;

  /** CPU per row read, seconds: global and per statement shape (learned from the ledger). */
  private cpuPerRow: number;

  private readonly cpuPerRowByShape = new Map<string, number>();

  /** Requests in flight (loop attribution). */
  private readonly active = new Set<Ticket>();

  /** Statements awaiting their measured CPU, with their ticket (undefined: outside a request). */
  private readonly pending = new Map<
    string,
    { statement: StatementRecord; ticket: Ticket | undefined }
  >();

  /** Loop utilisation of the last ~0.5 s (EWMA of the ticks). */
  loopUtilisation = 0;

  private tickTimer: NodeJS.Timeout | undefined;

  private pollTimer: NodeJS.Timeout | undefined;

  private ledgerBusy = false;

  private cpuReader: CpuReader | undefined;

  private readonly meter: LoopMeter;

  constructor(
    readonly options: AdmissionOptions = defaultAdmissionOptions,
    meter?: LoopMeter
  ) {
    this.budgets = {
      clickhouse: new Budget(options.clickhouse),
      loop: new Budget(options.loop),
    };
    this.model = new CostModel(options.maxKeys);
    this.cpuPerRow = options.defaultCpuPerRow;
    this.meter = meter ?? defaultLoopMeter();
  }

  /** Start the loop attribution tick and (with a reader and `cpuSource` query_log) the ledger. Timers do not hold the process. */
  start(cpuReader?: CpuReader) {
    if (!this.options.enabled || this.tickTimer !== undefined) return;
    this.cpuReader =
      this.options.cpuSource === 'query_log' ? cpuReader : undefined;
    this.meter();
    this.tickTimer = setInterval(() => {
      this.tick();
    }, loopTickMs);
    this.tickTimer.unref();
    if (this.cpuReader !== undefined) {
      this.pollTimer = setInterval(() => {
        // eslint-disable-next-line @typescript-eslint/no-floating-promises
        this.pollLedger();
      }, ledgerPollMs);
      this.pollTimer.unref();
    }
  }

  stop() {
    clearInterval(this.tickTimer);
    clearInterval(this.pollTimer);
    this.tickTimer = undefined;
    this.pollTimer = undefined;
  }

  /** Where measured CPU comes from now (`rows` after the ledger stopped). */
  get cpuSource() {
    return this.cpuReader === undefined ? 'rows' : 'query_log';
  }

  /** The class of a predicted cost. */
  classOf(cost: RequestCost): RequestClass {
    return cost.cpuSeconds * 1000 <= this.options.lightMaxCpuMs &&
      cost.loopMs <= this.options.lightMaxLoopMs
      ? 'light'
      : 'heavy';
  }

  /**
   * The prediction for a request and its class: the model's, or the
   * default for a request nothing is known about (always heavy: it may be
   * anything, and only the heavy share may go deep into debt).
   */
  predict(keys: PlanKeys | undefined) {
    const predicted = keys === undefined ? undefined : this.model.predict(keys);
    const cost = predicted?.cost ?? this.options.unknownCost;
    return {
      cls: predicted === undefined ? ('heavy' as const) : this.classOf(cost),
      cost,
      source: predicted?.source ?? ('default' as const),
    };
  }

  /**
   * Admit a request (or throw OVERLOADED at once). `keys` undefined: a
   * request the model cannot key (charged the default).
   */
  admit(keys: PlanKeys | undefined, now = performance.now()): Ticket {
    const { cls, cost, source } = this.predict(keys);
    /*
     * Idle: no request of this process in flight and the loop not busy
     * (live re-runs run outside requests). Nothing competes for the
     * budgets, so a request is never refused and a leftover debt (a heavy
     * request larger than the burst, a late measurement) is forgiven
     * (fix-pass-9.md §2: debt from two sequential L15s refused L17 on an
     * idle server).
     */
    const idle =
      this.active.size === 0 && this.loopUtilisation < this.options.loopGuard;
    const refuse = (reason: RefusalReason, detail: string, wait: number) => {
      this.stats.refused[reason] += 1;
      const retryAfter = Math.max(
        this.options.minRetryAfterSeconds,
        Math.min(60, Math.ceil(wait))
      );
      return overloadedError(cls, reason, detail, retryAfter);
    };
    if (
      !idle &&
      cls === 'heavy' &&
      this.stats.inFlight.heavy >= this.options.heavyMaxInFlight
    ) {
      throw refuse(
        'in_flight',
        `${this.stats.inFlight.heavy} heavy requests in flight`,
        1
      );
    }
    if (cls === 'heavy' && this.loopUtilisation >= this.options.loopGuard) {
      throw refuse(
        'loop_guard',
        `event loop ${Math.round(this.loopUtilisation * 100)} % busy`,
        1
      );
    }
    const { clickhouse, loop } = this.budgets;
    clickhouse.refill(now);
    loop.refill(now);
    if (idle) {
      clickhouse.forgive();
      loop.forgive();
    }
    const chShare = idle
      ? clickhouse.charge(cls, cost.cpuSeconds)
      : clickhouse.take(cls, cost.cpuSeconds);
    if (chShare === undefined) {
      throw refuse(
        'clickhouse',
        `ClickHouse CPU budget spent, predicted ${cost.cpuSeconds.toFixed(
          2
        )} CPU-s`,
        clickhouse.secondsToPositive(cls)
      );
    }
    const loopShare = idle
      ? loop.charge(cls, cost.loopMs)
      : loop.take(cls, cost.loopMs);
    if (loopShare === undefined) {
      clickhouse.adjust(chShare, cost.cpuSeconds);
      throw refuse(
        'loop',
        `event loop budget spent, predicted ${Math.round(cost.loopMs)} ms`,
        loop.secondsToPositive(cls)
      );
    }
    if (source !== 'plan') this.stats.unknownPlans += 1;
    this.account();
    const ticket = new Ticket(
      keys,
      cls,
      cost,
      { clickhouse: chShare, loop: loopShare },
      now
    );
    /*
     * Contended only under concurrent load: another heavy request of this
     * process in flight, and the heavy share below `contendedFill` or half
     * the in-flight cap taken. Past debt alone never throttles: a
     * sequential run of one heavy document keeps the server's threads
     * (fix-pass-9.md §2: back-to-back L15s were throttled to max_threads 4,
     * 1.65 -> 13.6 s, on an otherwise idle server).
     */
    const contended =
      cls === 'heavy' &&
      this.stats.inFlight.heavy > 0 &&
      (clickhouse.fill().heavy < this.options.contendedFill ||
        2 * this.stats.inFlight.heavy >= this.options.heavyMaxInFlight);
    const small = cls === 'light' && isSmall(cost, this.options);
    ticket.settings = contended
      ? { ...this.options.settings[cls], ...this.options.contendedSettings }
      : small
      ? { ...this.options.smallSettings, ...this.options.settings.light }
      : this.options.settings[cls];
    ticket.small = small && !contended;
    if (contended) this.stats.contended += 1;
    this.stats.admitted[cls] += 1;
    this.stats.inFlight[cls] += 1;
    this.stats.peakInFlight[cls] = Math.max(
      this.stats.peakInFlight[cls],
      this.stats.inFlight[cls]
    );
    this.active.add(ticket);
    return ticket;
  }

  /** CPU estimate of a finished statement from its rows (until measured). */
  estimateCpu(shape: string, rows: number) {
    return rows * (this.cpuPerRowByShape.get(shape) ?? this.cpuPerRow);
  }

  /**
   * A statement finished (its rows known). Inside a request it is part of
   * the ticket; outside one (a live re-run) it is charged now.
   */
  statementFinished(
    ticket: Ticket | undefined,
    statement: StatementRecord,
    now = performance.now()
  ) {
    if (!this.options.enabled) return;
    statement.finishedAt = now;
    if (this.cpuReader !== undefined) {
      this.pending.set(statement.queryId, { statement, ticket });
    }
    if (ticket === undefined) {
      this.stats.unticketed += 1;
      const estimate = this.estimateCpu(statement.shape, statement.rows);
      statement.chargedCpu = estimate;
      this.budgets.clickhouse.refill(now);
      this.budgets.clickhouse.adjust(
        estimate * 1000 <= this.options.lightMaxCpuMs ? 'light' : 'heavy',
        -estimate
      );
    }
  }

  /**
   * The request ended (response written, or execution done without one):
   * the loop charge is reconciled with the attributed time, the CPU charge
   * with the statements' estimates (measured CPU follows from the ledger),
   * and the model learns what is known.
   */
  finish(ticket: Ticket, now = performance.now()) {
    if (ticket.finished) return;
    this.account();
    ticket.finished = true;
    this.active.delete(ticket);
    this.stats.inFlight[ticket.cls] -= 1;
    const { clickhouse, loop } = this.budgets;
    clickhouse.refill(now);
    loop.refill(now);
    this.recharge(ticket, 'loop', ticket.loopMs);
    const cpu = this.ticketCpu(ticket);
    this.recharge(ticket, 'clickhouse', cpu);
    if (ticket.keys !== undefined) {
      /* the CPU is learned when every statement is measured (an estimate only seeds a new key) */
      this.model.observe(
        ticket.keys,
        {
          cpuSeconds: cpu,
          loopMs: ticket.loopMs,
          rows: ticket.statements.reduce(
            (sum, statement) => sum + statement.rows,
            0
          ),
        },
        !this.allMeasured(ticket)
      );
    }
  }

  /**
   * Replace what a ticket was charged in one budget by `actual`: the
   * charge is given back to the share it came from, and the actual cost
   * taken from that share, or from the heavy one when the request turned
   * out heavy (a mispredicted light request must not put the light share
   * into debt).
   */
  private recharge(
    ticket: Ticket,
    budget: 'clickhouse' | 'loop',
    actual: number
  ) {
    const charged =
      budget === 'clickhouse' ? ticket.chargedCpu : ticket.chargedLoop;
    const heavyNow =
      budget === 'clickhouse'
        ? actual * 1000 > this.options.lightMaxCpuMs
        : actual > this.options.lightMaxLoopMs;
    const from = ticket.shares[budget];
    const to = heavyNow ? 'heavy' : from;
    if (from === to) {
      /*
       * One net adjustment: giving the charge back first was capped at the
       * capacity on a refilled share, so the actual cost was charged a
       * second time (fix-pass-9.md §2: the debt behind contended L15s).
       */
      this.budgets[budget].adjust(from, charged - actual);
    } else {
      this.budgets[budget].adjust(from, charged);
      this.budgets[budget].adjust(to, -actual);
    }
    ticket.shares[budget] = to;
    if (budget === 'clickhouse') ticket.chargedCpu = actual;
    else ticket.chargedLoop = actual;
  }

  /**
   * The request's CPU: the measured sum once every statement is measured
   * (or given up on); before that, with a ledger, the charge stands (a
   * known plan's prediction is a better guess than rows × CPU per row);
   * without one, the rows estimate.
   */
  private ticketCpu(ticket: Ticket) {
    if (this.cpuReader !== undefined && !this.allMeasured(ticket)) {
      return ticket.chargedCpu;
    }
    return ticket.statements.reduce(
      (sum, statement) =>
        sum +
        (statement.cpuSeconds ??
          this.estimateCpu(statement.shape, statement.rows)),
      0
    );
  }

  private allMeasured(ticket: Ticket) {
    return (
      this.cpuReader === undefined ||
      ticket.statements.every(
        (statement) =>
          statement.cpuSeconds !== undefined ||
          !this.pending.has(statement.queryId)
      )
    );
  }

  /** Loop time measured since the last utilisation tick. */
  private readonly window = { activeMs: 0, elapsedMs: 0 };

  /**
   * Attribute the loop's busy time since the last call: to the requests
   * not waiting for ClickHouse, in proportion to their predicted loop time
   * (a light request queued behind a heavy one's decoding is not charged
   * for it; the sum is exact). Called at every transition (admitted, a
   * statement sent, its response started, finished) and every tick, so a
   * request shorter than a tick gets its share too.
   */
  account() {
    const { activeMs, elapsedMs } = this.meter();
    this.window.activeMs += activeMs;
    this.window.elapsedMs += elapsedMs;
    if (activeMs <= 0) return;
    const working = [...this.active].filter((ticket) => ticket.waiting === 0);
    if (working.length === 0) return;
    const weight = (ticket: Ticket) => Math.max(1, ticket.predicted.loopMs);
    const total = working.reduce((sum, ticket) => sum + weight(ticket), 0);
    working.forEach((ticket) => {
      ticket.loopMs += (activeMs * weight(ticket)) / total;
    });
  }

  /** A request starts (1) or stops (-1) waiting for ClickHouse (db.ts). */
  waiting(ticket: Ticket, delta: number) {
    if (this.options.enabled) this.account();
    ticket.waiting += delta;
  }

  /** One period: attribution, and the loop utilisation (EWMA, ~0.5 s). */
  tick() {
    this.account();
    const { activeMs, elapsedMs } = this.window;
    this.window.activeMs = 0;
    this.window.elapsedMs = 0;
    if (elapsedMs > 0) {
      const utilisation = Math.min(1, activeMs / elapsedMs);
      this.loopUtilisation = this.loopUtilisation * 0.8 + utilisation * 0.2;
    }
  }

  /** Read measured CPU of pending statements; reconcile budgets and the model. */
  async pollLedger(now = performance.now()) {
    if (this.cpuReader === undefined || this.ledgerBusy) return;
    if (this.pending.size === 0) return;
    this.ledgerBusy = true;
    try {
      const ids = [...this.pending.keys()].slice(0, 2_000);
      const measured = await this.cpuReader(ids);
      this.budgets.clickhouse.refill(now);
      const touched = new Set<Ticket>();
      for (const id of ids) {
        const entry = this.pending.get(id);
        const measurement = measured.get(id);
        if (entry !== undefined && measurement === undefined) {
          this.expireIfStale(id, entry, now, touched);
        }
        if (entry !== undefined && measurement !== undefined) {
          this.resolve(id, entry, measurement, touched);
        }
      }
      for (const ticket of touched) this.reconcileMeasured(ticket);
    } catch (error: unknown) {
      /* no grant on system.query_log (an older API user profile): estimates from rows from now on */
      this.stats.ledgerError = String(error).slice(0, 200);
      this.cpuReader = undefined;
      this.pending.clear();
      clearInterval(this.pollTimer);
      this.pollTimer = undefined;
    } finally {
      this.ledgerBusy = false;
    }
  }

  /** A statement's measured CPU (and parts): the estimate replaced, the CPU per row learned. */
  private resolve(
    id: string,
    entry: { statement: StatementRecord; ticket: Ticket | undefined },
    measurement: StatementMeasurement,
    touched: Set<Ticket>
  ) {
    this.pending.delete(id);
    this.stats.ledgerResolved += 1;
    const { statement, ticket } = entry;
    const cpu = measurement.cpuMicros / 1e6;
    if (measurement.parts !== undefined) statement.parts = measurement.parts;
    const estimate =
      statement.chargedCpu ?? this.estimateCpu(statement.shape, statement.rows);
    statement.cpuSeconds = cpu;
    this.learnCpuPerRow(statement.shape, statement.rows, cpu);
    if (ticket === undefined) {
      /* a statement outside a request was charged its estimate */
      this.budgets.clickhouse.adjust(
        estimate * 1000 <= this.options.lightMaxCpuMs ? 'light' : 'heavy',
        estimate - cpu
      );
    } else {
      touched.add(ticket);
    }
  }

  /** Give up on a statement whose `query_log` row did not come within `ledgerPatienceMs` (its estimate stays). */
  private expireIfStale(
    id: string,
    entry: { statement: StatementRecord; ticket: Ticket | undefined },
    now: number,
    touched: Set<Ticket>
  ) {
    if (now - (entry.statement.finishedAt ?? now) <= ledgerPatienceMs) return;
    this.pending.delete(id);
    this.stats.ledgerExpired += 1;
    if (entry.ticket !== undefined) touched.add(entry.ticket);
  }

  /** A finished request whose statements' CPU became known: reconcile and learn. */
  private reconcileMeasured(ticket: Ticket) {
    if (!ticket.finished) return;
    const cpu = this.ticketCpu(ticket);
    this.recharge(ticket, 'clickhouse', cpu);
    if (ticket.keys !== undefined && this.allMeasured(ticket)) {
      /* parts only when every statement reported them (a part count of some statements would overstate rows per part) */
      const parts = ticket.statements.every(
        (statement) => statement.parts !== undefined
      )
        ? ticket.statements.reduce(
            (sum, statement) => sum + (statement.parts ?? 0),
            0
          )
        : undefined;
      this.model.observe(ticket.keys, {
        cpuSeconds: cpu,
        loopMs: ticket.loopMs,
        ...(parts === undefined ? {} : { parts }),
      });
    }
  }

  private learnCpuPerRow(shape: string, rows: number, cpu: number) {
    if (rows < 1_000) return;
    const ratio = cpu / rows;
    this.cpuPerRow = this.cpuPerRow * 0.9 + ratio * 0.1;
    const before = this.cpuPerRowByShape.get(shape);
    this.cpuPerRowByShape.delete(shape);
    this.cpuPerRowByShape.set(
      shape,
      before === undefined
        ? ratio
        : before * (1 - costAlpha) + ratio * costAlpha
    );
    while (this.cpuPerRowByShape.size > this.options.maxKeys) {
      const oldest = this.cpuPerRowByShape.keys().next().value;
      if (oldest === undefined) break;
      this.cpuPerRowByShape.delete(oldest);
    }
  }

  /** The gate's state for `GET /admission` (server.ts). */
  snapshot() {
    const now = performance.now();
    this.budgets.clickhouse.refill(now);
    this.budgets.loop.refill(now);
    return {
      budgets: {
        clickhouse: this.budgets.clickhouse.fill(),
        loop: this.budgets.loop.fill(),
      },
      cpuPerRow: this.cpuPerRow,
      cpuSource: this.cpuSource,
      /* per document (hash prefix : operation): predicted CPU-s and loop ms */
      documents: this.model.documents(),
      enabled: this.options.enabled,
      loopUtilisation: this.loopUtilisation,
      modelKeys: this.model.size,
      pendingStatements: this.pending.size,
      stats: this.stats,
    };
  }

  /** Statements awaiting their measured CPU (tests, stats). */
  get pendingStatements() {
    return this.pending.size;
  }
}

/* ------------------------------------------------------------------ */
/* configuration                                                        */
/* ------------------------------------------------------------------ */

const nonNegativeNumber = (
  env: NodeJS.ProcessEnv,
  name: string,
  fallback: number,
  { integer = false, max = Number.POSITIVE_INFINITY } = {}
) => {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (
    !Number.isFinite(value) ||
    value < 0 ||
    value > max ||
    (integer && !Number.isInteger(value))
  ) {
    throw new Error(
      `${name} must be ${integer ? 'an integer' : 'a number'} >= 0${
        Number.isFinite(max) ? ` and <= ${max}` : ''
      }.`
    );
  }
  return value;
};

const classSettingsFromEnv = (
  env: NodeJS.ProcessEnv,
  prefix: string,
  defaultThreads = 0
) => {
  const settings: { [name: string]: number | string } = {};
  const threads = nonNegativeNumber(
    env,
    `${prefix}_MAX_THREADS`,
    defaultThreads,
    {
      integer: true,
    }
  );
  if (threads > 0) settings.max_threads = threads;
  const priority = nonNegativeNumber(env, `${prefix}_PRIORITY`, 0, {
    integer: true,
  });
  if (priority > 0) settings.priority = priority;
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
 * - CHAINGRAPH_API_ADMISSION `on` | `off` (default on);
 * - CHAINGRAPH_API_CLICKHOUSE_CORES: the server's cores (default
 *   `serverCores`, its `max_threads` for the API user; else 48);
 * - CHAINGRAPH_API_PROCESSES: API processes sharing the server (pods ×
 *   workers; default 1 × `workers`): the CPU budget is divided by it;
 * - CHAINGRAPH_API_CPU_TARGET (0.9), CHAINGRAPH_API_LOOP_TARGET (0.85):
 *   the budgets' rates as fractions of the cores and of the loop;
 * - CHAINGRAPH_API_LIGHT_SHARE (0.25), CHAINGRAPH_API_BURST_SECONDS (4: of
 *   the whole server's rate, whatever the processes),
 *   CHAINGRAPH_API_LOOP_BURST_SECONDS (1);
 * - CHAINGRAPH_API_LIGHT_MAX_CPU_MS (500), CHAINGRAPH_API_LIGHT_MAX_LOOP_MS
 *   (25): the light class;
 * - CHAINGRAPH_API_HEAVY_MAX_IN_FLIGHT (default cores / 2 / processes, at
 *   least 2), CHAINGRAPH_API_LOOP_GUARD (0.95);
 * - CHAINGRAPH_API_HEAVY_CONTENDED_FILL (0.5),
 *   CHAINGRAPH_API_HEAVY_CONTENDED_MAX_THREADS (default 2 × cores / heavy
 *   slots of the server, at least 2; 0: no cap): a heavy request admitted
 *   while its share is below that fill, or half its in-flight cap is
 *   taken, runs with that many threads;
 * - CHAINGRAPH_API_UNKNOWN_CPU_MS (250), CHAINGRAPH_API_UNKNOWN_LOOP_MS (20);
 * - CHAINGRAPH_API_CPU_SOURCE `query_log` | `rows` (default query_log; the
 *   API user needs SELECT on system.query_log, else rows are used);
 * - CHAINGRAPH_API_LIGHT_… / CHAINGRAPH_API_HEAVY_… MAX_THREADS, PRIORITY, WORKLOAD: ClickHouse
 *   settings of each class's statements (default none: the server's);
 * - CHAINGRAPH_API_RETRY_AFTER_SECONDS: the least Retry-After (1).
 */
export const admissionOptionsFromEnv = (
  env: NodeJS.ProcessEnv = process.env,
  serverCores?: number,
  workers = 1,
  heavyThreads = 0
): AdmissionOptions => {
  const flag = env.CHAINGRAPH_API_ADMISSION ?? 'on';
  if (flag !== 'on' && flag !== 'off' && flag !== '') {
    throw new Error(`CHAINGRAPH_API_ADMISSION must be 'on' or 'off'.`);
  }
  const envCores = nonNegativeNumber(
    env,
    'CHAINGRAPH_API_CLICKHOUSE_CORES',
    0,
    {
      integer: true,
    }
  );
  const knownServerCores =
    serverCores !== undefined &&
    Number.isInteger(serverCores) &&
    serverCores > 0
      ? serverCores
      : undefined;
  const cores = envCores > 0 ? envCores : knownServerCores;
  const processes = Math.max(
    1,
    nonNegativeNumber(env, 'CHAINGRAPH_API_PROCESSES', Math.max(1, workers), {
      integer: true,
    })
  );
  const cpuTarget = nonNegativeNumber(
    env,
    'CHAINGRAPH_API_CPU_TARGET',
    defaultCpuTarget,
    { max: 4 }
  );
  const loopTarget = nonNegativeNumber(
    env,
    'CHAINGRAPH_API_LOOP_TARGET',
    defaultLoopTarget,
    { max: 1 }
  );
  const lightShare = nonNegativeNumber(
    env,
    'CHAINGRAPH_API_LIGHT_SHARE',
    defaultAdmissionOptions.clickhouse.lightShare,
    { max: 1 }
  );
  const usableCores = cores ?? defaultCores;
  const cpuSource = env.CHAINGRAPH_API_CPU_SOURCE ?? 'query_log';
  if (cpuSource !== 'query_log' && cpuSource !== 'rows') {
    throw new Error(`CHAINGRAPH_API_CPU_SOURCE must be 'query_log' or 'rows'.`);
  }
  const heavyMaxInFlight = Math.max(
    1,
    nonNegativeNumber(
      env,
      'CHAINGRAPH_API_HEAVY_MAX_IN_FLIGHT',
      Math.max(2, Math.round(usableCores / 2 / processes)),
      { integer: true }
    )
  );
  /* about 2 cores' worth of threads per heavy slot of the server (4 at 48 cores / 24 slots) */
  const contendedThreads = nonNegativeNumber(
    env,
    'CHAINGRAPH_API_HEAVY_CONTENDED_MAX_THREADS',
    Math.max(2, Math.round((2 * usableCores) / (heavyMaxInFlight * processes))),
    { integer: true }
  );
  return {
    clickhouse: {
      /*
       * the rate is this process's share, the burst the server's: each of n
       * processes may burst like one (a per-process slice of the burst
       * refused heavy requests two workers could have run, fix-pass-8.md §4)
       */
      burstSeconds:
        nonNegativeNumber(
          env,
          'CHAINGRAPH_API_BURST_SECONDS',
          defaultAdmissionOptions.clickhouse.burstSeconds
        ) * processes,
      lightShare,
      ratePerSecond: (usableCores * cpuTarget) / processes,
    },
    contendedFill: nonNegativeNumber(
      env,
      'CHAINGRAPH_API_HEAVY_CONTENDED_FILL',
      defaultAdmissionOptions.contendedFill,
      { max: 1 }
    ),
    contendedSettings:
      contendedThreads === 0 ? {} : { max_threads: contendedThreads },
    cpuSource,
    defaultCpuPerRow: defaultAdmissionOptions.defaultCpuPerRow,
    enabled: flag !== 'off',
    heavyMaxInFlight,
    lightMaxCpuMs: nonNegativeNumber(
      env,
      'CHAINGRAPH_API_LIGHT_MAX_CPU_MS',
      defaultAdmissionOptions.lightMaxCpuMs
    ),
    lightMaxLoopMs: nonNegativeNumber(
      env,
      'CHAINGRAPH_API_LIGHT_MAX_LOOP_MS',
      defaultAdmissionOptions.lightMaxLoopMs
    ),
    /*
     * With heavy-lane threads (heavy-lane.ts) heavy requests run on their
     * loops: the light share is this thread's loop, the heavy share the
     * lane's threads.
     */
    loop: {
      burstSeconds: nonNegativeNumber(
        env,
        'CHAINGRAPH_API_LOOP_BURST_SECONDS',
        defaultAdmissionOptions.loop.burstSeconds
      ),
      lightShare: heavyThreads > 0 ? 1 / (1 + heavyThreads) : lightShare,
      ratePerSecond: 1000 * loopTarget * (1 + Math.max(0, heavyThreads)),
    },
    loopGuard: nonNegativeNumber(
      env,
      'CHAINGRAPH_API_LOOP_GUARD',
      defaultAdmissionOptions.loopGuard,
      { max: 1 }
    ),
    maxKeys: defaultAdmissionOptions.maxKeys,
    minRetryAfterSeconds: Math.max(
      1,
      nonNegativeNumber(
        env,
        'CHAINGRAPH_API_RETRY_AFTER_SECONDS',
        defaultAdmissionOptions.minRetryAfterSeconds,
        { integer: true }
      )
    ),
    settings: {
      heavy: classSettingsFromEnv(env, 'CHAINGRAPH_API_HEAVY'),
      light: classSettingsFromEnv(env, 'CHAINGRAPH_API_LIGHT'),
    },
    sizing: {
      cores,
      cpuTarget,
      heavyThreads: Math.max(0, heavyThreads),
      loopTarget,
      processes,
      source:
        envCores > 0
          ? 'env'
          : knownServerCores === undefined
          ? 'default'
          : 'clickhouse',
    },
    smallMaxRows: nonNegativeNumber(
      env,
      'CHAINGRAPH_API_SMALL_MAX_ROWS',
      defaultSmallMaxRows
    ),
    smallMaxRowsPerPart: nonNegativeNumber(
      env,
      'CHAINGRAPH_API_SMALL_MAX_ROWS_PER_PART',
      defaultSmallMaxRowsPerPart
    ),
    smallSettings: classSettingsFromEnv(
      env,
      'CHAINGRAPH_API_SMALL',
      defaultSmallSettings.max_threads
    ),
    unknownCost: {
      cpuSeconds:
        nonNegativeNumber(
          env,
          'CHAINGRAPH_API_UNKNOWN_CPU_MS',
          defaultAdmissionOptions.unknownCost.cpuSeconds * 1000
        ) / 1000,
      loopMs: nonNegativeNumber(
        env,
        'CHAINGRAPH_API_UNKNOWN_LOOP_MS',
        defaultAdmissionOptions.unknownCost.loopMs
      ),
    },
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

const settingsText = (settings: { [name: string]: number | string }) =>
  Object.entries(settings)
    .map(([name, value]) => `${name} ${String(value)}`)
    .join(',') || 'server default';

/** One line describing the admission sizing, for the API's startup log. */
export const admissionSummary = (
  options: AdmissionOptions,
  backstop: number | undefined,
  cpuSource?: string
) =>
  `admission: ${options.enabled ? 'on' : 'off'} (request level) cores=${
    options.sizing?.cores ?? 'unknown'
  } (${options.sizing?.source ?? 'default'}) processes=${
    options.sizing?.processes ?? 1
  } cpu_budget=${options.clickhouse.ratePerSecond.toFixed(1)} CPU-s/s burst ${
    options.clickhouse.burstSeconds
  } s loop_budget=${Math.round(options.loop.ratePerSecond)} ms/s burst ${
    options.loop.burstSeconds
  } s light_share=${options.clickhouse.lightShare} light<=${
    options.lightMaxCpuMs
  } CPU-ms,${options.lightMaxLoopMs} loop-ms heavy_in_flight=${
    options.heavyMaxInFlight
  } heavy_contended=${options.contendedFill}:${settingsText(
    options.contendedSettings
  )} loop_guard=${options.loopGuard} unknown=${Math.round(
    options.unknownCost.cpuSeconds * 1000
  )} CPU-ms,${options.unknownCost.loopMs} loop-ms cpu_source=${
    cpuSource ?? options.cpuSource
  } light_settings=${settingsText(options.settings.light)} small<=${
    options.smallMaxRows
  } rows,${
    options.smallMaxRowsPerPart > 0
      ? `<=${options.smallMaxRowsPerPart} rows/part`
      : 'any rows/part'
  }:${settingsText(options.smallSettings)} heavy_settings=${settingsText(
    options.settings.heavy
  )} max_concurrent_queries_for_user=${
    backstop === undefined ? 'unknown' : backstop === 0 ? '0 (none)' : backstop
  }`;

/** The statement shape a cost is remembered by: label + SQL text (no parameter values). */
export const statementShape = (label: string, sql: string) =>
  createHash('sha256').update(label).update('\0').update(sql).digest('hex');
