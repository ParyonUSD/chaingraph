/* eslint-disable functional/no-let, functional/no-loop-statement, functional/no-throw-statement, functional/no-mixed-type, @typescript-eslint/sort-type-constituents, @typescript-eslint/member-ordering, @typescript-eslint/parameter-properties */
// cspell:ignore clickhouse serialisation serialised recognise
/**
 * The heavy lane (fix-pass-9.md §3): heavy requests run on worker threads,
 * each with its own event loop and its own API instance (Yoga, ClickHouse
 * client, no admission of its own, no response cache), so the main loop
 * stays free for light requests, refusals and response-cache hits.
 *
 * Mainnet (#104): one API process was loop-bound at 20 QPS; light documents
 * (statements of ~10 ms) and refusals waited behind L33 / L35's 0.3–0.6 s
 * of decoding, GraphQL execution and serialisation (light p95 ~1.2 s,
 * refusal p95 ~550 ms with one worker; 0.6–0.7 s light p95 with three).
 *
 * The main thread admits a request (admission.ts) before it is sent here;
 * the worker answers with the response bytes (transferred, not copied),
 * the statements it ran (query ids and rows, for the CPU ledger), the
 * loop time it spent on the request and the snapshot its result reads (for
 * the main thread's response cache).
 */
import { Worker } from 'node:worker_threads';

import type { ClickHouseConnectionConfig } from '../store/clickhouse/client.js';

import type { StatementTimeouts } from './db.js';
import type { QueryLimits, TrustedDocuments } from './plugins.js';
import type { UtxoSetPolicy } from './sql.js';

/** What a lane worker needs to build its API instance (structured-cloneable). */
export interface HeavyLaneWorkerData {
  clickhouse: ClickHouseConnectionConfig;
  maxResultRows?: number;
  authchainMaxBranches?: number;
  statementTimeouts?: Partial<StatementTimeouts>;
  utxoSet?: Partial<UtxoSetPolicy>;
  limits?: Partial<QueryLimits> | false;
  trustedDocuments?: TrustedDocuments;
}

export interface HeavyLaneOptions extends HeavyLaneWorkerData {
  /** Worker threads (at least 1). */
  threads: number;
}

/** A request for a lane worker. */
export interface LaneRequest {
  /** The HTTP body (JSON: query, variables, operationName, extensions). */
  body: string;
  /** The request's Accept header. */
  accept?: string;
  /** ClickHouse settings of its statements (the ticket's class and contention). */
  settings: { [name: string]: number | string };
}

/** A lane worker's answer. */
export interface LaneReply {
  status: number;
  headers: [string, string][];
  body: Uint8Array;
  /** The statements sent: query id, rows read, shape (the CPU ledger). */
  statements: { queryId: string; rows: number; shape: string }[];
  /** The worker loop's busy time attributed to the request, ms. */
  loopMs: number;
  /** The snapshot the result reads (node → visible seq), when one was read. */
  marks?: [number, string][];
}

export type LaneMessage =
  | ({ type: 'done'; id: number } & LaneReply)
  | { type: 'error'; id: number; message: string }
  | { type: 'ready' };

export type LaneCommand =
  | ({ type: 'run'; id: number } & LaneRequest)
  | { type: 'cancel'; id: number };

interface Pending {
  resolve: (reply: LaneReply) => void;
  reject: (error: Error) => void;
  slot: Slot;
}

interface Slot {
  worker: Worker;
  inFlight: number;
}

const workerUrl = new URL('./heavy-worker.js', import.meta.url);

/** The lane's worker threads (main-thread side). */
export class HeavyLane {
  private readonly slots: Slot[] = [];

  private readonly pending = new Map<number, Pending>();

  private next = 1;

  private closed = false;

  /** Requests answered / failed (stats). */
  readonly stats = { failed: 0, restarted: 0, served: 0 };

  constructor(private readonly options: HeavyLaneOptions) {
    const threads = Math.max(1, Math.floor(options.threads));
    for (let index = 0; index < threads; index += 1) {
      this.slots.push(this.spawn());
    }
  }

  get threads() {
    return this.slots.length;
  }

  /** Requests in flight on the lane. */
  get inFlight() {
    return this.pending.size;
  }

  private spawn(): Slot {
    const data: HeavyLaneWorkerData = Object.fromEntries(
      Object.entries(this.options).filter(([name]) => name !== 'threads')
    ) as unknown as HeavyLaneWorkerData;
    const worker = new Worker(workerUrl, { workerData: data });
    const slot: Slot = { inFlight: 0, worker };
    worker.on('message', (message: LaneMessage) => {
      if (message.type === 'ready') return;
      const entry = this.pending.get(message.id);
      if (entry === undefined) return;
      this.pending.delete(message.id);
      entry.slot.inFlight -= 1;
      if (message.type === 'error') {
        this.stats.failed += 1;
        entry.reject(new Error(message.message));
        return;
      }
      this.stats.served += 1;
      entry.resolve({
        body: message.body,
        headers: message.headers,
        loopMs: message.loopMs,
        ...(message.marks === undefined ? {} : { marks: message.marks }),
        statements: message.statements,
        status: message.status,
      });
    });
    const fail = (error: Error) => {
      for (const [id, entry] of this.pending) {
        if (entry.slot === slot) {
          this.pending.delete(id);
          this.stats.failed += 1;
          entry.reject(error);
        }
      }
      slot.inFlight = 0;
      if (this.closed) return;
      /* a worker that died is replaced */
      const index = this.slots.indexOf(slot);
      if (index !== -1) {
        this.stats.restarted += 1;
        this.slots[index] = this.spawn();
      }
    };
    worker.on('error', fail);
    worker.on('exit', (code) => {
      if (!this.closed) fail(new Error(`heavy lane worker exited (${code})`));
    });
    worker.unref();
    return slot;
  }

  /** Run a request on the least busy worker; `signal` cancels it there. */
  async run(request: LaneRequest, signal?: AbortSignal): Promise<LaneReply> {
    if (this.closed) throw new Error('The heavy lane is closed.');
    const slot = this.slots.reduce((best, candidate) =>
      candidate.inFlight < best.inFlight ? candidate : best
    );
    const id = this.next;
    this.next += 1;
    slot.inFlight += 1;
    return new Promise<LaneReply>((resolve, reject) => {
      this.pending.set(id, { reject, resolve, slot });
      const cancel = () => {
        if (this.pending.has(id)) {
          slot.worker.postMessage({ id, type: 'cancel' } satisfies LaneCommand);
        }
      };
      signal?.addEventListener('abort', cancel, { once: true });
      slot.worker.postMessage({
        ...request,
        id,
        type: 'run',
      } satisfies LaneCommand);
    });
  }

  async close() {
    this.closed = true;
    for (const [id, entry] of this.pending) {
      this.pending.delete(id);
      entry.reject(new Error('The heavy lane closed.'));
    }
    await Promise.all(this.slots.map(async (slot) => slot.worker.terminate()));
  }
}
