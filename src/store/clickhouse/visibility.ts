/* eslint-disable camelcase, @typescript-eslint/naming-convention, functional/no-mixed-type, functional/no-try-statement, @typescript-eslint/parameter-properties, max-classes-per-file, complexity */
// cspell:ignore clickhouse seqs subquery subqueries
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

/** Low 40 bits of a `commit_seq`: the counter within its epoch. */
export const counterMaskSeq = 1_099_511_627_775n;

/**
 * At most this many aborted seqs are passed inline as the `void` parameter
 * (about 17 bytes each; ClickHouse rejects a single HTTP parameter over
 * `http_max_field_value_size`, 128 KiB by default). Above it the snapshot
 * passes `[voidOverflowSentinel]` and the pinned views fall back to the
 * `commit_void` subquery. Compaction/recovery may later truncate void rows to
 * keep the set small (see docs/clickhouse-port/wp6b-gate-cost.md).
 */
export const voidInlineLimit = 4096;

/** `void = [voidOverflowSentinel]`: the views read `commit_void` themselves. Never a real seq (epoch 2^24 - 1). */
export const voidOverflowSentinel = 18_446_744_073_709_551_615n;

/**
 * Budget for the encoded `fence` parameter (one counter per epoch). An epoch
 * that never committed costs 2 bytes ("0,"), so this is tens of thousands of
 * lease epochs; beyond it `readSnapshot` throws `GateParameterOverflowError`.
 */
export const fenceParamMaxBytes = 120_000;

export class GateParameterOverflowError extends Error {}

/**
 * One reader's pinned view of the store. Pass `nodeViewParams` to every
 * node-scoped `*_at` view and `agnosticViewParams` to every node-agnostic
 * `*_at` view of one request: all of them then see the same commits.
 * Everything the gate needs is in here, so the views run no subqueries.
 */
export interface VisibilitySnapshot {
  nodeId: number;
  /** visible(n) */
  visible: bigint;
  /** visible(0) */
  visible0: bigint;
  /** Committed seqs above visible0 at snapshot time. */
  committedTail: bigint[];
  /**
   * Per epoch e (index e - 1, dense from epoch 1 to the snapshot's highest
   * epoch): the highest valid counter of e, or `counterMaskSeq` if e is not
   * fenced.
   */
  fence: bigint[];
  /** Aborted seqs up to the snapshot's highest seq, or `[voidOverflowSentinel]`. */
  void: bigint[];
  /** True when `void` is the overflow sentinel. */
  voidOverflow: boolean;
}

/**
 * The snapshot query. One statement; the scalar subqueries run in data
 * dependency order (each references the previous result, so ClickHouse must
 * evaluate it first):
 * 1. visible(n) and visible(0), from one read of `visibility`;
 * 2. the committed tail above visible(0);
 * 3. `bound` = the highest seq any view of this snapshot can show; the void
 *    set up to `bound` (read after 1-2, so every aborted seq at or below a
 *    watermark is in it: abort writes `commit_void` before the commit
 *    becomes terminal, and watermarks only pass terminal commits);
 * 4. the fences of epochs up to `bound`'s epoch (read after 1-2: a new
 *    holder writes its fences before its first commit).
 * Any commit up to visible(n) (and every commit it depends on) was committed
 * before visible(n) was read, so it is at most visible(0) or in the tail:
 * node-agnostic rows of a visible node-n fact are always visible in the same
 * snapshot.
 */
export const snapshotSql = (tables: {
  visibility: string;
  commitLog: string;
  commitVoid: string;
  epochFence: string;
}) => `WITH
  (SELECT (maxIf(visible_seq, node_internal_id = {node:UInt32}), maxIf(visible_seq, node_internal_id = 0))
   FROM ${
     tables.visibility
   } WHERE node_internal_id IN (0, {node:UInt32})) AS marks,
  (SELECT arraySort(groupArray(commit_seq)) FROM ${tables.commitLog}
   WHERE state = 'committed' AND commit_seq > marks.2) AS tail_seqs,
  greatest(marks.1, marks.2, arrayMax(arrayPushBack(tail_seqs, toUInt64(0)))) AS bound,
  (SELECT groupArray(commit_seq) FROM
     (SELECT DISTINCT commit_seq FROM ${
       tables.commitVoid
     } WHERE commit_seq <= bound
      ORDER BY commit_seq LIMIT {voidLimit:UInt32})) AS void_seqs,
  (SELECT (groupArray(epoch), groupArray(max_valid_seq)) FROM
     (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM ${
       tables.epochFence
     }
      WHERE epoch <= bitShiftRight(bound, 40) GROUP BY epoch)) AS fences
SELECT
  toString(marks.1) AS visible,
  toString(marks.2) AS visible0,
  arrayMap(x -> toString(x), tail_seqs) AS tail,
  arrayMap(x -> toString(x), void_seqs) AS void,
  arrayMap(e -> toString(if(indexOf(fences.1, e) = 0, ${counterMaskSeq.toString()},
                            bitAnd(fences.2[indexOf(fences.1, e)], ${counterMaskSeq.toString()}))),
           range(1, toUInt64(bitShiftRight(bound, 40)) + 1)) AS fence`;

const snapshotQuery = snapshotSql({
  commitLog: 'commit_log',
  commitVoid: 'commit_void',
  epochFence: 'epoch_fence',
  visibility: 'visibility',
});

/** Bytes of an array parameter as the HTTP interface encodes it: `[a,b,…]`. */
const encodedLength = (values: readonly bigint[]) =>
  values.reduce((total, value) => total + value.toString().length + 1, 1);

/**
 * Read a snapshot in one query (see `snapshotSql` for the order argument).
 * Throws `GateParameterOverflowError` if the fence would not fit in one HTTP
 * parameter.
 */
export const readSnapshot = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number
): Promise<VisibilitySnapshot> => {
  const rows = await client.query<{
    visible: string;
    visible0: string;
    tail: string[];
    void: string[];
    fence: string[];
  }>(snapshotQuery, { node: nodeId, voidLimit: voidInlineLimit + 1 });
  const [row] = rows;
  const voidSeqs = (row?.void ?? []).map(BigInt);
  const voidOverflow = voidSeqs.length > voidInlineLimit;
  const fence = (row?.fence ?? []).map(BigInt);
  if (encodedLength(fence) > fenceParamMaxBytes) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new GateParameterOverflowError(
      `The epoch fence has ${fence.length} epochs (${encodedLength(
        fence
      )} bytes as a parameter, limit ${fenceParamMaxBytes}); see docs/clickhouse-port/wp6b-gate-cost.md.`
    );
  }
  return {
    committedTail: (row?.tail ?? []).map(BigInt),
    fence,
    nodeId,
    visible: BigInt(row?.visible ?? '0'),
    visible0: BigInt(row?.visible0 ?? '0'),
    void: voidOverflow ? [voidOverflowSentinel] : voidSeqs,
    voidOverflow,
  };
};

/** `node = …, visible = …, fence = …, void = …` for a node-scoped `*_at` view. */
export const nodeViewArgs =
  'node = {node:UInt32}, visible = {visible:UInt64}, fence = {fence:Array(UInt64)}, void = {void:Array(UInt64)}';

/** `visible0 = …, tail = …, fence = …, void = …` for a node-agnostic `*_at` view. */
export const agnosticViewArgs =
  'visible0 = {visible0:UInt64}, tail = {tail:Array(UInt64)}, fence = {fence:Array(UInt64)}, void = {void:Array(UInt64)}';

/** The node-agnostic pinned views; every other `*_at` view is node-scoped. */
export const agnosticPinnedViews: readonly string[] = [
  'block_at',
  'block_transaction_at',
  'input_at',
  'output_at',
  'transaction_at',
];

/**
 * A pinned view call with its query-parameter placeholders, e.g.
 * `pinnedView('utxo_at')` = `utxo_at(node = {node:UInt32}, …)`. Bind
 * `snapshotParams(snapshot)` (or the node/agnostic subset).
 */
export const pinnedView = (name: string, qualifier = '') =>
  `${qualifier === '' ? '' : `${qualifier}.`}${name}(${
    agnosticPinnedViews.includes(name) ? agnosticViewArgs : nodeViewArgs
  })`;

/** Parameters for node-scoped pinned views: `utxo_at(${nodeViewArgs})`. */
export const nodeViewParams = (snapshot: VisibilitySnapshot) => ({
  fence: snapshot.fence,
  node: snapshot.nodeId,
  visible: snapshot.visible,
  void: snapshot.void,
});

/** Parameters for node-agnostic pinned views: `output_at(${agnosticViewArgs})`. */
export const agnosticViewParams = (snapshot: VisibilitySnapshot) => ({
  fence: snapshot.fence,
  tail: snapshot.committedTail,
  visible0: snapshot.visible0,
  void: snapshot.void,
});

/** Every parameter of one snapshot (for requests that use both view kinds). */
export const snapshotParams = (snapshot: VisibilitySnapshot) => ({
  ...agnosticViewParams(snapshot),
  ...nodeViewParams(snapshot),
});

/**
 * SQL fragments of the gate for ad-hoc readers that read base tables. They
 * mirror ddl/050_views.sql.
 * - `fenceWith` / `validCommit`: the subquery form of the `*_v` views
 *   (reads `epoch_fence` and `commit_void`).
 * - `pinnedValid` / `visibleAt`: the parameter form of the `*_at` views; bind
 *   `nodeViewParams(snapshot)` (`visible`, `fence`, `void`).
 */
export const gateSql = {
  /** Prepend as a `WITH` item: `WITH ${gateSql.fenceWith} SELECT …`. */
  fenceWith: `(SELECT arrayMap(t -> t.2, arraySort(groupArray((epoch, max_valid_seq))))
     FROM (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM epoch_fence GROUP BY epoch)) AS fence_max_seq`,
  /** Rows of a valid commit per the snapshot's `fence` and `void` parameters. */
  pinnedValid: (column = 'commit_seq', commitVoid = 'commit_void') =>
    `bitAnd(${column}, ${counterMaskSeq.toString()}) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(${column}, 40))
  AND NOT has({void:Array(UInt64)}, ${column})
  AND (NOT has({void:Array(UInt64)}, ${voidOverflowSentinel.toString()}) OR ${column} NOT IN (SELECT commit_seq FROM ${commitVoid}))`,
  /** Rows of a valid commit (not void, not fenced); needs `fenceWith`. */
  validCommit: (column = 'commit_seq') =>
    `${column} NOT IN (SELECT commit_seq FROM commit_void)
  AND (bitShiftRight(${column}, 40) > length(fence_max_seq)
       OR ${column} <= arrayElement(fence_max_seq, bitShiftRight(${column}, 40)))`,
  /** Rows visible in a snapshot at node watermark `visible`. */
  visibleAt: (column = 'commit_seq', commitVoid = 'commit_void') =>
    `${column} <= {visible:UInt64} AND ${gateSql.pinnedValid(
      column,
      commitVoid
    )}`,
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
