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

/** A watermark would pass an open (non-terminal) commit of its scope. */
export class WatermarkInvariantError extends Error {}

/**
 * The publisher never passes an open or incomplete commit (WP4): for every
 * open commit, `W(0)` and `W(n)` of every node n in its scope are below its
 * seq. True by construction of `computeWatermarks`; checked before every
 * publish so a regression throws instead of exposing a partial commit.
 */
export const assertWatermarksBelowOpen = (
  watermarks: ReadonlyMap<number, bigint>,
  openCommits: readonly OpenCommitScope[]
) => {
  openCommits.forEach(({ seq, nodeScope }) => {
    [nodeAgnosticId, ...nodeScope].forEach((node) => {
      const watermark = watermarks.get(node);
      if (watermark !== undefined && watermark >= seq) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new WatermarkInvariantError(
          `Watermark ${watermark} of node ${node} would pass open commit ${seq}.`
        );
      }
    });
  });
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
 * At most this many aborted seqs are passed inline as the `void` parameter.
 * Each element costs about 1.1 us per view per query (WP6b: +0.3 ms at 256,
 * +5 ms at 4096 per view), so past this size the `commit_void` subquery
 * fallback is cheaper: the snapshot passes `[voidOverflowSentinel]` and the
 * pinned views read `commit_void` themselves. (The hard cap would be
 * `http_max_field_value_size`, 128 KiB, about 7,500 seqs.) Compaction or
 * recovery should truncate void rows once their data rows are gone, to keep
 * the set small (docs/clickhouse-port/wp6b-gate-cost.md).
 */
export const voidInlineLimit = 512;

/** `void = [voidOverflowSentinel]`: the views read `commit_void` themselves. Never a real seq (epoch 2^24 - 1). */
export const voidOverflowSentinel = 18_446_744_073_709_551_615n;

/**
 * Budget for the encoded `fence` parameter (one counter per epoch). An epoch
 * that never committed costs 2 bytes ("0,"), so this is tens of thousands of
 * lease epochs; beyond it `readSnapshot` throws `GateParameterOverflowError`.
 */
export const fenceParamMaxBytes = 120_000;

/**
 * Budget for the hidden stand-in seqs appended to `void` (about 5,000 seqs).
 * Each resolved stand-in seq stays hidden until its rows are removed, so
 * the set grows with every stand-in ever resolved (rare: mempool orphans
 * released without their parents); see docs/clickhouse-port/mempool-fill-fix.md.
 */
export const hiddenParamMaxBytes = 100_000;

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
  /**
   * Aborted seqs up to the snapshot's highest seq (or `[voidOverflowSentinel]`),
   * then the hidden stand-in seqs (`snapshotSql` step 5).
   */
  void: bigint[];
  /** True when `void` starts with the overflow sentinel. */
  voidOverflow: boolean;
}

/**
 * A snapshot pinning several nodes at once (`readSnapshotMulti`): one
 * `visible(n)` per node, one node-agnostic part (visible(0), tail, void,
 * fence) shared by all of them, all from one statement. `snapshotForNode`
 * gives the per-node `VisibilitySnapshot` (the per-node rule is unchanged:
 * node n's views see `visible(n)`).
 */
export interface MultiNodeSnapshot {
  nodeIds: number[];
  /** visible(n) per pinned node (0 if the node has no watermark row). */
  visibleByNode: ReadonlyMap<number, bigint>;
  visible0: bigint;
  committedTail: bigint[];
  fence: bigint[];
  void: bigint[];
  voidOverflow: boolean;
}

/**
 * The snapshot query. One statement; the scalar subqueries run in data
 * dependency order (each references the previous result, so ClickHouse must
 * evaluate it first):
 * 1. visible(n) of every pinned node and visible(0), from one read of
 *    `visibility`;
 * 2. the committed tail above visible(0);
 * 3. `bound` = the highest seq any view of this snapshot can show (over
 *    every pinned node); the void set up to `bound` (read after 1-2, so
 *    every aborted seq at or below a watermark is in it: abort writes
 *    `commit_void` before the commit becomes terminal, and watermarks only
 *    pass terminal commits);
 * 4. the fences of epochs up to `bound`'s epoch (read after 1-2: a new
 *    holder writes its fences before its first commit);
 * 5. the hidden stand-in seqs (read after 1-4): a stand-in seq P
 *    (`input_stand_in`, docs/clickhouse-port/mempool-fill-fix.md) is hidden
 *    while its owner C is not visible in this snapshot, and once a commit R
 *    resolving it (`input_stand_in_resolution`) is. "Visible" is the
 *    node-agnostic rule of the views: at most visible(0) and not void, or in
 *    the tail; and not fenced. P's and R's rows in these tables are written
 *    before P / R commit, so a P or R this snapshot shows has its rows read
 *    here. The caller appends the hidden seqs to `void`.
 * Any commit up to visible(n) (and every commit it depends on) was committed
 * before visible(n) was read, so it is at most visible(0) or in the tail:
 * node-agnostic rows of a visible node-n fact are always visible in the same
 * snapshot, for every pinned node.
 */
export const snapshotSql = (tables: {
  visibility: string;
  commitLog: string;
  commitVoid: string;
  epochFence: string;
  standIn?: string;
  standInResolution?: string;
}) => `WITH
  (SELECT maxMap([node_internal_id], [visible_seq]) FROM ${tables.visibility}
   WHERE node_internal_id = 0 OR has({nodes:Array(UInt32)}, node_internal_id)) AS max_by_node,
  (arrayMap(n -> if(indexOf(max_by_node.1, n) = 0, toUInt64(0), max_by_node.2[indexOf(max_by_node.1, n)]),
            {nodes:Array(UInt32)}),
   if(indexOf(max_by_node.1, 0) = 0, toUInt64(0), max_by_node.2[indexOf(max_by_node.1, 0)])) AS marks,
  (SELECT arraySort(groupArray(commit_seq)) FROM ${tables.commitLog}
   WHERE state = 'committed' AND commit_seq > marks.2) AS tail_seqs,
  greatest(arrayMax(arrayPushBack(marks.1, marks.2)),
           arrayMax(arrayPushBack(tail_seqs, toUInt64(0)))) AS bound,
  (SELECT groupArray(commit_seq) FROM
     (SELECT DISTINCT commit_seq FROM ${
       tables.commitVoid
     } WHERE commit_seq <= bound
      ORDER BY commit_seq LIMIT {voidLimit:UInt32})) AS void_seqs,
  (SELECT (groupArray(epoch), groupArray(max_valid_seq)) FROM
     (SELECT epoch, min(max_valid_seq) AS max_valid_seq FROM ${
       tables.epochFence
     }
      WHERE epoch <= bitShiftRight(bound, 40) GROUP BY epoch)) AS fences,
  (SELECT groupUniqArray(seq) FROM
     (SELECT stand_in_seq AS seq, owner_seq AS by_seq, toUInt8(0) AS resolves
      FROM ${tables.standIn ?? 'input_stand_in'} WHERE stand_in_seq <= bound
      UNION ALL
      SELECT stand_in_seq, commit_seq, toUInt8(1)
      FROM ${
        tables.standInResolution ?? 'input_stand_in_resolution'
      } WHERE stand_in_seq <= bound)
   WHERE resolves = toUInt8(
     ((by_seq <= marks.2 AND by_seq NOT IN (SELECT commit_seq FROM ${
       tables.commitVoid
     })) OR has(tail_seqs, by_seq))
     AND (indexOf(fences.1, bitShiftRight(by_seq, 40)) = 0
          OR by_seq <= fences.2[indexOf(fences.1, bitShiftRight(by_seq, 40))]))) AS stand_in_hidden
SELECT
  arrayMap(x -> toString(x), marks.1) AS visible,
  toString(marks.2) AS visible0,
  arrayMap(x -> toString(x), tail_seqs) AS tail,
  arrayMap(x -> toString(x), void_seqs) AS void,
  arrayMap(x -> toString(x), arraySort(stand_in_hidden)) AS hidden,
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
 * Read one snapshot pinning every node of `nodeIds` (see `snapshotSql` for
 * the order argument). Throws `GateParameterOverflowError` if the fence
 * would not fit in one HTTP parameter.
 */
export const readSnapshotMulti = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeIds: readonly number[]
): Promise<MultiNodeSnapshot> => {
  const nodes = [...new Set(nodeIds)].sort((a, b) => a - b);
  const rows = await client.query<{
    visible: string[];
    visible0: string;
    tail: string[];
    void: string[];
    hidden: string[];
    fence: string[];
  }>(snapshotQuery, { nodes, voidLimit: voidInlineLimit + 1 });
  const [row] = rows;
  const voidSeqs = (row?.void ?? []).map(BigInt);
  const voidOverflow = voidSeqs.length > voidInlineLimit;
  const hidden = (row?.hidden ?? []).map(BigInt);
  const fence = (row?.fence ?? []).map(BigInt);
  if (encodedLength(fence) > fenceParamMaxBytes) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new GateParameterOverflowError(
      `The epoch fence has ${fence.length} epochs (${encodedLength(
        fence
      )} bytes as a parameter, limit ${fenceParamMaxBytes}); see docs/clickhouse-port/wp6b-gate-cost.md.`
    );
  }
  if (encodedLength(hidden) > hiddenParamMaxBytes) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new GateParameterOverflowError(
      `${hidden.length} hidden stand-in seqs (${encodedLength(
        hidden
      )} bytes as a parameter, limit ${hiddenParamMaxBytes}); see docs/clickhouse-port/mempool-fill-fix.md.`
    );
  }
  const visible = row?.visible ?? [];
  return {
    committedTail: (row?.tail ?? []).map(BigInt),
    fence,
    nodeIds: nodes,
    visible0: BigInt(row?.visible0 ?? '0'),
    visibleByNode: new Map(
      nodes.map((node, index) => [node, BigInt(visible[index] ?? '0')])
    ),
    void: [...(voidOverflow ? [voidOverflowSentinel] : voidSeqs), ...hidden],
    voidOverflow,
  };
};

/** The per-node snapshot of one node pinned by `snapshot`. */
export const snapshotForNode = (
  snapshot: MultiNodeSnapshot,
  nodeId: number
): VisibilitySnapshot => {
  const visible =
    nodeId === nodeAgnosticId
      ? snapshot.visible0
      : snapshot.visibleByNode.get(nodeId);
  if (visible === undefined) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new RangeError(`Node ${nodeId} is not pinned by this snapshot.`);
  }
  return {
    committedTail: snapshot.committedTail,
    fence: snapshot.fence,
    nodeId,
    visible,
    visible0: snapshot.visible0,
    void: snapshot.void,
    voidOverflow: snapshot.voidOverflow,
  };
};

/** Read a snapshot of one node in one query (`readSnapshotMulti` of `[nodeId]`). */
export const readSnapshot = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number
): Promise<VisibilitySnapshot> =>
  snapshotForNode(await readSnapshotMulti(client, [nodeId]), nodeId);

/** The cheap watermark read of `SnapshotCache`: visibility only, no subquery. */
export const watermarksSql = `SELECT node_internal_id AS node, toString(max(visible_seq)) AS visible
  FROM visibility WHERE node_internal_id IN {nodes:Array(UInt32)} GROUP BY node_internal_id`;

/**
 * Snapshots reused while the watermarks stand still (fix-pass-3.md §4).
 * `read(nodes)` first reads only the published watermarks of the nodes and
 * node 0 (one small query without subqueries). If they equal those of the
 * cached snapshot of the same node set, that snapshot is returned: it is
 * exactly what `readSnapshotMulti` returned when these watermarks were
 * current, and it is still a consistent snapshot of them (a seq at or below
 * a watermark is terminal, so its void row was already read; fences never
 * cover a seq a watermark passed; the tail only lacks commits above every
 * watermark). Its watermarks equal the ones read after the request started,
 * so it is never staler than the published watermarks at request start.
 * Otherwise a full `readSnapshotMulti` replaces the entry.
 */
const defaultSnapshotCacheEntries = 256;

export class SnapshotCache {
  hits = 0;

  misses = 0;

  private readonly entries = new Map<string, MultiNodeSnapshot>();

  constructor(
    private readonly client: Pick<ClickHouseClient, 'query'>,
    private readonly maxEntries = defaultSnapshotCacheEntries
  ) {}

  async read(nodeIds: readonly number[]): Promise<MultiNodeSnapshot> {
    const nodes = [...new Set(nodeIds)].sort((a, b) => a - b);
    const key = nodes.join(',');
    const cached = this.entries.get(key);
    if (cached !== undefined) {
      const rows = await this.client.query<{ node: number; visible: string }>(
        watermarksSql,
        { nodes: [nodeAgnosticId, ...nodes] }
      );
      const marks = new Map(
        rows.map((row) => [Number(row.node), BigInt(row.visible)])
      );
      const same =
        (marks.get(nodeAgnosticId) ?? 0n) === cached.visible0 &&
        nodes.every(
          (node) => (marks.get(node) ?? 0n) === cached.visibleByNode.get(node)
        );
      if (same) {
        this.hits += 1;
        return cached;
      }
    }
    this.misses += 1;
    const fresh = await readSnapshotMulti(this.client, nodes);
    this.entries.delete(key);
    this.entries.set(key, fresh);
    if (this.entries.size > this.maxEntries) {
      this.entries.delete(this.entries.keys().next().value!);
    }
    return fresh;
  }
}

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

  /** Every published watermark (node → visible seq), a copy. */
  publishedWatermarks(): ReadonlyMap<number, bigint> {
    return new Map(this.published);
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
    const openCommits = this.source.openCommits();
    const watermarks = computeWatermarks({
      lastAllocatedSeq: this.source.lastAllocatedSeq,
      nodes: this.nodes,
      openCommits,
    });
    assertWatermarksBelowOpen(watermarks, openCommits);
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
