/* eslint-disable complexity, functional/no-loop-statement, functional/no-let, @typescript-eslint/parameter-properties, max-classes-per-file, @typescript-eslint/member-ordering, @typescript-eslint/no-magic-numbers, functional/no-mixed-type, functional/no-try-statement, no-negated-condition */
// cspell:ignore clickhouse serialised serialising Pothos
/**
 * Live queries driven by the visibility watermark (S4).
 *
 * - A topic is one subscription operation + its variables (+ node). All
 *   subscribers of a topic share its result: per watermark advance the hub
 *   reads ONE snapshot of the node (shared by every topic of that node) and
 *   runs ONE data query per topic, then pushes the result to every
 *   subscriber whose result changed.
 * - A single poll per tick (`ApiDb.watermarks`, one query for every watched
 *   node) detects advances; nothing runs while the watermark stands still.
 * - Runs of a topic are serialised and only a result computed at a newer
 *   watermark than the last one delivered is ever pushed, so a subscriber
 *   never receives a result computed at an older snapshot.
 */
import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';

import { relations } from './compile.js';
import type { ApiDb } from './db.js';

/** JSON of a result including prefetched relationships (change detection). */
export const resultKey = (value: unknown): string =>
  JSON.stringify(value, (_key, item: unknown) => {
    if (typeof item === 'bigint') return item.toString();
    const related =
      item !== null && typeof item === 'object'
        ? (item as { [relations]?: unknown })[relations]
        : undefined;
    return related === undefined
      ? item
      : { ...(item as object), relationships: related };
  });

const relatedOf = (value: object): unknown =>
  (value as { [relations]?: unknown })[relations];

/**
 * Whether two results are equal, prefetched relationships included (what
 * `resultKey` of each would compare), without serialising either: a walk
 * that stops at the first difference. A 10,000-row page's `resultKey` took
 * ~20 ms locally and was on the path of every first payload
 * (fix-pass-8b.md §4).
 */
export const sameResult = (a: unknown, b: unknown): boolean => {
  if (a === b) return true;
  if (typeof a === 'bigint' || typeof b === 'bigint') {
    return String(a) === String(b);
  }
  if (
    a === null ||
    b === null ||
    typeof a !== 'object' ||
    typeof b !== 'object'
  ) {
    return false;
  }
  if (Array.isArray(a) || Array.isArray(b)) {
    if (!Array.isArray(a) || !Array.isArray(b) || a.length !== b.length) {
      return false;
    }
    for (let index = 0; index < a.length; index += 1) {
      if (!sameResult(a[index], b[index])) return false;
    }
    return true;
  }
  const plain = (value: object) => {
    const prototype: unknown = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
  };
  /* anything but plain records (none expected): as resultKey compares it */
  if (!plain(a) || !plain(b)) return resultKey(a) === resultKey(b);
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  for (let index = 0; index < keysA.length; index += 1) {
    const key = keysA[index]!;
    if (key !== keysB[index]) return false;
    if (
      !sameResult(
        (a as { [key: string]: unknown })[key],
        (b as { [key: string]: unknown })[key]
      )
    ) {
      return false;
    }
  }
  return sameResult(relatedOf(a) ?? null, relatedOf(b) ?? null);
};

type Runner = (snapshot: VisibilitySnapshot) => Promise<unknown>;

class Sink {
  private readonly queue: unknown[] = [];

  private waiting: ((result: IteratorResult<unknown>) => void) | undefined;

  private done = false;

  constructor(private readonly onClose: () => void) {}

  push(value: unknown) {
    if (this.done) return;
    if (this.waiting !== undefined) {
      const resolve = this.waiting;
      this.waiting = undefined;
      resolve({ done: false, value });
    } else {
      this.queue.push(value);
    }
  }

  close() {
    if (this.done) return;
    this.done = true;
    this.waiting?.({ done: true, value: undefined });
    this.waiting = undefined;
    this.onClose();
  }

  iterator(): AsyncIterableIterator<unknown> {
    const next = async (): Promise<IteratorResult<unknown>> => {
      if (this.queue.length > 0) {
        return { done: false, value: this.queue.shift() };
      }
      if (this.done) return { done: true, value: undefined };
      return new Promise((resolve) => {
        this.waiting = resolve;
      });
    };
    const iterator: AsyncIterableIterator<unknown> = {
      [Symbol.asyncIterator]: () => iterator,
      next,
      return: async () => {
        this.close();
        return { done: true, value: undefined };
      },
      throw: async (error: unknown) => {
        this.close();
        // eslint-disable-next-line functional/no-throw-statement
        throw error;
      },
    };
    return iterator;
  }
}

interface Topic {
  key: string;
  nodeId: number;
  run: Runner;
  sinks: Set<Sink>;
  /** Highest watermark a run was started at. */
  computedVisible: bigint;
  /** Watermark of the result last pushed (or -1). */
  deliveredVisible: bigint;
  /** A result was pushed (`latest` holds it). */
  delivered: boolean;
  latest: unknown;
  running: boolean;
  next: VisibilitySnapshot | undefined;
  /** Watermarks of every delivered result, in order (tests). */
  deliveries: bigint[];
}

export interface LiveHubOptions {
  /** Watermark poll interval. Default 100 ms. */
  pollMs?: number;
  onError?: (error: unknown) => void;
}

export interface LiveHubStats {
  /** Data-query runs (one per topic per advance). */
  runs: number;
  /** Results pushed to subscribers (one per subscriber per change). */
  pushes: number;
  /** Duration of the last run (snapshot to result), ms. */
  lastRunMs: number;
  /** Duration of the last fan-out (result to every subscriber's queue), ms. */
  lastFanOutMs: number;
}

export class LiveHub {
  readonly stats: LiveHubStats = {
    lastFanOutMs: 0,
    lastRunMs: 0,
    pushes: 0,
    runs: 0,
  };

  private readonly topics = new Map<string, Topic>();

  private readonly lastSeen = new Map<number, bigint>();

  private timer: ReturnType<typeof setInterval> | undefined;

  private polling = false;

  private readonly pollMs: number;

  constructor(
    private readonly db: ApiDb,
    private readonly options: LiveHubOptions = {}
  ) {
    this.pollMs = options.pollMs ?? 100;
  }

  /** Number of live topics and their subscriber counts (tests). */
  topicStats() {
    return [...this.topics.values()].map((topic) => ({
      deliveries: [...topic.deliveries],
      key: topic.key,
      subscribers: topic.sinks.size,
    }));
  }

  subscribe(key: string, nodeId: number, run: Runner) {
    let topic = this.topics.get(key);
    const created = topic === undefined;
    if (topic === undefined) {
      topic = {
        computedVisible: -1n,
        delivered: false,
        deliveredVisible: -1n,
        deliveries: [],
        key,
        latest: undefined,
        next: undefined,
        nodeId,
        run,
        running: false,
        sinks: new Set(),
      };
      this.topics.set(key, topic);
    }
    const current = topic;
    const sink = new Sink(() => {
      current.sinks.delete(sink);
      if (current.sinks.size === 0 && this.topics.get(key) === current) {
        this.topics.delete(key);
        if (this.topics.size === 0) this.stop();
      }
    });
    current.sinks.add(sink);
    if (created) {
      this.db
        .readSnapshot(nodeId)
        .then((snapshot) => {
          this.advance(nodeId, snapshot);
        })
        .catch((error: unknown) => this.options.onError?.(error));
    } else if (current.delivered) {
      sink.push(current.latest);
    }
    this.start();
    return sink.iterator();
  }

  stop() {
    if (this.timer !== undefined) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  /** End every subscription. */
  close() {
    this.stop();
    [...this.topics.values()].forEach((topic) => {
      [...topic.sinks].forEach((sink) => {
        sink.close();
      });
    });
    this.topics.clear();
  }

  private start() {
    if (this.timer !== undefined) return;
    this.timer = setInterval(() => {
      this.poll().catch((error: unknown) => this.options.onError?.(error));
    }, this.pollMs);
    this.timer.unref();
  }

  /** One watermark query for every watched node; one snapshot per advanced node. */
  async poll() {
    if (this.polling || this.topics.size === 0) return;
    this.polling = true;
    try {
      const nodes = [
        ...new Set([...this.topics.values()].map((t) => t.nodeId)),
      ];
      const marks = await this.db.watermarks(nodes);
      await Promise.all(
        nodes.map(async (nodeId) => {
          const mark = marks.get(nodeId) ?? 0n;
          if (mark <= (this.lastSeen.get(nodeId) ?? -1n)) return;
          this.advance(nodeId, await this.db.readSnapshot(nodeId));
        })
      );
    } finally {
      this.polling = false;
    }
  }

  /** A fresh snapshot of a node: every topic of the node may re-run on it. */
  private advance(nodeId: number, snapshot: VisibilitySnapshot) {
    if (snapshot.visible > (this.lastSeen.get(nodeId) ?? -1n)) {
      this.lastSeen.set(nodeId, snapshot.visible);
    }
    [...this.topics.values()]
      .filter((topic) => topic.nodeId === nodeId)
      .forEach((topic) => {
        this.schedule(topic, snapshot);
      });
  }

  private schedule(topic: Topic, snapshot: VisibilitySnapshot) {
    if (snapshot.visible <= topic.computedVisible) return;
    if (topic.running) {
      if (topic.next === undefined || snapshot.visible > topic.next.visible) {
        topic.next = snapshot;
      }
      return;
    }
    topic.computedVisible = snapshot.visible;
    topic.running = true;
    this.stats.runs += 1;
    const started = performance.now();
    topic
      .run(snapshot)
      .then((result) => {
        this.stats.lastRunMs = performance.now() - started;
        if (snapshot.visible <= topic.deliveredVisible) return;
        topic.deliveredVisible = snapshot.visible;
        /* the first result is pushed as is; later ones when they changed */
        if (topic.delivered && sameResult(result, topic.latest)) return;
        topic.delivered = true;
        topic.latest = result;
        topic.deliveries.push(snapshot.visible);
        const fanOut = performance.now();
        topic.sinks.forEach((sink) => {
          this.stats.pushes += 1;
          sink.push(result);
        });
        this.stats.lastFanOutMs = performance.now() - fanOut;
      })
      .catch((error: unknown) => this.options.onError?.(error))
      .finally(() => {
        topic.running = false;
        const { next } = topic;
        topic.next = undefined;
        if (next !== undefined) this.schedule(topic, next);
      });
  }
}
