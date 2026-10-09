/* eslint-disable camelcase, @typescript-eslint/naming-convention, functional/no-mixed-type, functional/no-try-statement, @typescript-eslint/parameter-properties */
// cspell:ignore clickhouse seqs
/**
 * Per-node visibility watermarks (plan §3.2, WP4).
 *
 * Semantics: `visible(n)` is the highest W such that every commit with
 * `seq <= W` whose node scope contains n is terminal (committed or aborted);
 * `visible(0)` is the same over every commit. Readers see a row iff its
 * `commit_seq` is at most the watermark, not void (aborted) and not fenced (a stale
 * epoch's). `incomplete` is not terminal; its lifetime is bounded by the writer
 * (see the doc). Full argument: docs/clickhouse-port/wp4-commit-and-visibility.md.
 */
import type { ClickHouseClient } from './client.js';

/** Node id 0 is the node-agnostic data watermark. */
export const nodeAgnosticId = 0;

export interface OpenCommitScope {
  seq: bigint;
  nodeScope: readonly number[];
}

/**
 * Watermarks for `nodes` (node 0 is always included) given the writer's
 * non-terminal commits and the highest allocated seq. Pure.
 *
 * W(n) = (lowest non-terminal seq touching n) - 1, or `lastAllocatedSeq` if
 * none. Seqs are allocated in increasing order and every allocated seq is
 * registered as non-terminal before its first row is sent, so every seq up to
 * W(n) touching n is terminal, and W(n) never decreases.
 */
export const computeWatermarks = ({
  lastAllocatedSeq,
  nodes,
  openCommits,
}: {
  lastAllocatedSeq: bigint;
  nodes: Iterable<number>;
  openCommits: readonly OpenCommitScope[];
}): Map<number, bigint> => {
  const lowestOpen = new Map<number, bigint>();
  const lowestAny = openCommits.reduce<bigint | undefined>(
    (lowest, { seq }) => (lowest === undefined || seq < lowest ? seq : lowest),
    undefined
  );
  openCommits.forEach(({ seq, nodeScope }) => {
    nodeScope.forEach((node) => {
      const current = lowestOpen.get(node);
      if (current === undefined || seq < current) {
        lowestOpen.set(node, seq);
      }
    });
  });
  const result = new Map<number, bigint>();
  result.set(
    nodeAgnosticId,
    lowestAny === undefined ? lastAllocatedSeq : lowestAny - 1n
  );
  [...nodes].forEach((node) => {
    if (node === nodeAgnosticId) {
      return;
    }
    const lowest = lowestOpen.get(node);
    result.set(node, lowest === undefined ? lastAllocatedSeq : lowest - 1n);
  });
  return result;
};

export type VisibilityClient = Pick<ClickHouseClient, 'command' | 'query'>;

/** What the publisher needs from the commit log. */
export interface WatermarkSource {
  readonly lastAllocatedSeq: bigint;
  openCommits: () => readonly OpenCommitScope[];
  onTerminal: (listener: (seq: bigint) => void) => void;
}

/** `visible(n)` as published (0 if no row: nothing visible). */
export const readWatermark = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number
): Promise<bigint> => {
  const rows = await client.query<{ visible: string }>(
    'SELECT max(visible_seq) AS visible FROM visibility WHERE node_internal_id = {node:UInt32}',
    { node: nodeId }
  );
  return BigInt(rows[0]?.visible ?? '0');
};

/**
 * One reader's pinned view of the store. Pass `nodeViewParams` to every
 * node-scoped `*_at` view and `agnosticViewParams` to every node-agnostic
 * `*_at` view of one request: all of them then see the same commits.
 */
export interface VisibilitySnapshot {
  nodeId: number;
  /** visible(n) */
  visible: bigint;
  /** visible(0) */
  visible0: bigint;
  /** Committed seqs above visible0 at snapshot time. */
  committedTail: bigint[];
}

/**
 * Read a snapshot. Order matters: visible(n) first, then visible(0) with the
 * committed tail in one statement. Any commit up to visible(n) (and every
 * commit it depends on) was committed before visible(n) was read, so it is at
 * most the later visible(0) or in the later tail: node-agnostic rows of a visible
 * node-n fact are always visible in the same snapshot.
 */
export const readSnapshot = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number
): Promise<VisibilitySnapshot> => {
  const visible = await readWatermark(client, nodeId);
  const rows = await client.query<{ visible0: string; tail: string[] }>(
    `WITH (SELECT max(visible_seq) FROM visibility WHERE node_internal_id = 0) AS v0
     SELECT v0 AS visible0, arraySort(groupArray(commit_seq)) AS tail
     FROM commit_log
     WHERE state = 'committed' AND commit_seq > v0`
  );
  const [row] = rows;
  return {
    committedTail: (row?.tail ?? []).map(BigInt),
    nodeId,
    visible,
    visible0: BigInt(row?.visible0 ?? '0'),
  };
};

/** Parameters for node-scoped pinned views: `utxo_at(node = …, visible = …)`. */
export const nodeViewParams = (snapshot: VisibilitySnapshot) => ({
  node: snapshot.nodeId,
  visible: snapshot.visible,
});

/** Parameters for node-agnostic pinned views: `output_at(visible0 = …, tail = …)`. */
export const agnosticViewParams = (snapshot: VisibilitySnapshot) => ({
  tail: snapshot.committedTail,
  visible0: snapshot.visible0,
});

/**
 * SQL fragments of the gate for ad-hoc readers (the checker, verifiers) that
 * read base tables. They mirror ddl/050_views.sql. Bind `{visible:UInt64}`.
 */
export const gateSql = {
  /** Prepend as a `WITH` item: `WITH ${gateSql.fenceWith} SELECT …`. */
  fenceWith: `(SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM epoch_fence GROUP BY epoch)) AS fence_max_seq`,
  /** Rows of a valid commit (not void, not fenced); needs `fenceWith`. */
  validCommit: (column = 'commit_seq') =>
    `${column} NOT IN (SELECT commit_seq FROM commit_void)
  AND (bitShiftRight(${column}, 40) > length(fence_max_seq)
       OR ${column} <= arrayElement(fence_max_seq, bitShiftRight(${column}, 40)))`,
  /** Rows visible at a pinned node watermark; needs `fenceWith`. */
  visibleAt: (column = 'commit_seq') =>
    `${column} <= {visible:UInt64} AND ${gateSql.validCommit(column)}`,
};

export interface PublisherOptions {
  /** Publish at most this often (plan §3.1 step 4: 100 ms). */
  minIntervalMs?: number;
  /** Called with errors from scheduled publishes. */
  onError?: (error: unknown) => void;
}

const defaultMinIntervalMs = 100;

/**
 * Computes watermarks from the writer's in-memory commit state and writes the
 * ones that advanced. Visibility rows only ever increase (the table keeps the
 * max), so publishing is idempotent and order-independent.
 */
export class VisibilityPublisher {
  private readonly nodes = new Set<number>([nodeAgnosticId]);

  private readonly published = new Map<number, bigint>();

  private timer: ReturnType<typeof setTimeout> | undefined;

  private lastPublishMs = 0;

  private inFlight: Promise<Map<number, bigint>> | undefined;

  private readonly minIntervalMs: number;

  constructor(
    private readonly client: VisibilityClient,
    private readonly source: WatermarkSource,
    private readonly options: PublisherOptions = {}
  ) {
    this.minIntervalMs = options.minIntervalMs ?? defaultMinIntervalMs;
  }

  /** Load the nodes and watermarks already published. */
  async init(): Promise<void> {
    const rows = await this.client.query<{ node: number; visible: string }>(
      'SELECT node_internal_id AS node, max(visible_seq) AS visible FROM visibility GROUP BY node'
    );
    rows.forEach((row) => {
      this.nodes.add(Number(row.node));
      this.published.set(Number(row.node), BigInt(row.visible));
    });
  }

  /** Track a node (e.g. on registerNode) so it gets a watermark row. */
  registerNode(nodeId: number) {
    this.nodes.add(nodeId);
  }

  /** Start publishing (batched) whenever a commit becomes terminal. */
  start() {
    this.source.onTerminal(() => {
      this.schedule();
    });
  }

  stop() {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
  }

  /** Compute watermarks now and write those that advanced; returns all of them. */
  async publishWatermark(): Promise<Map<number, bigint>> {
    if (this.inFlight !== undefined) {
      await this.inFlight.catch(() => undefined);
    }
    const run = this.publishNow();
    this.inFlight = run;
    try {
      return await run;
    } finally {
      if (this.inFlight === run) {
        this.inFlight = undefined;
      }
    }
  }

  /** The last published value per node (this process's view). */
  publishedWatermark(nodeId: number): bigint {
    return this.published.get(nodeId) ?? 0n;
  }

  private schedule() {
    if (this.timer !== undefined) {
      return;
    }
    const wait = Math.max(
      0,
      this.lastPublishMs + this.minIntervalMs - Date.now()
    );
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.publishWatermark().catch((error: unknown) => {
        this.options.onError?.(error);
      });
    }, wait);
  }

  private async publishNow(): Promise<Map<number, bigint>> {
    this.source.openCommits().forEach(({ nodeScope }) => {
      nodeScope.forEach((node) => this.nodes.add(node));
    });
    const watermarks = computeWatermarks({
      lastAllocatedSeq: this.source.lastAllocatedSeq,
      nodes: this.nodes,
      openCommits: this.source.openCommits(),
    });
    const advanced = [...watermarks]
      .filter(([node, visible]) => visible > (this.published.get(node) ?? 0n))
      .sort(([a], [b]) => a - b);
    this.lastPublishMs = Date.now();
    if (advanced.length > 0) {
      await this.client.command(
        `INSERT INTO visibility (node_internal_id, visible_seq, updated_at)
         SELECT watermark.1, watermark.2, now64(3, 'UTC')
         FROM (SELECT arrayJoin(arrayZip({nodes:Array(UInt32)}, {seqs:Array(UInt64)})) AS watermark)`,
        {
          nodes: advanced.map(([node]) => node),
          seqs: advanced.map(([, visible]) => visible),
        },
        { async_insert: 0 }
      );
      advanced.forEach(([node, visible]) => this.published.set(node, visible));
    }
    return watermarks;
  }
}
