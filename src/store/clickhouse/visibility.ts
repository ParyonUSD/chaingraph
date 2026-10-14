/* eslint-disable camelcase, @typescript-eslint/naming-convention, functional/no-try-statement, @typescript-eslint/parameter-properties, max-classes-per-file, complexity, max-lines */
// cspell:ignore clickhouse seqs subquery subqueries inlinable analysed
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

/**
 * The spend index's watermark travels with the visibility watermarks under
 * this pseudo node id in the snapshot reads (it is stored in
 * `spend_watermark`, not in `visibility`).
 */
export const spendWatermarkId = 4_294_967_295;

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
  /** Aborted seqs up to the snapshot's highest seq (or `[voidOverflowSentinel]`). */
  void: bigint[];
  /** True when `void` starts with the overflow sentinel. */
  voidOverflow: boolean;
  /**
   * The spend index's watermark at snapshot time, at most `visible0`:
   * `spend_at` shows the index rows up to it; readers take every pair above
   * it from the join (docs/clickhouse-port/spend-index.md).
   */
  spend: bigint;
  /**
   * The stored UTXO set of this node (`utxo_set`, utxo-set.ts): the newest
   * published batch end U at or below `visible` and its generation. Readers
   * take the stored rows up to U and the change rows of (U, visible] from
   * the acceptance tables (docs/clickhouse-port/utxo-async.md). Undefined
   * when the snapshot was read without the UTXO set, or the node has no
   * stored set yet.
   */
  utxo?: UtxoSetMark;
}

/** A node's stored UTXO set as of one snapshot (utxo-set.ts). */
export interface UtxoSetMark {
  /** The batch end U (a commit seq, at most visible(n)). */
  watermark: bigint;
  /** The generation (rebase) whose rows hold the set at U. */
  generation: number;
  /**
   * Committed commits of the node (node scope) in (U, visible(n)]. Zero:
   * no acceptance fact of the node changed after U, so the stored set at U
   * is the set at visible(n) and readers add no correction.
   */
  lagCommits: number;
  /**
   * Their recorded acceptance rows (`tx_acceptance` + `node_transaction` +
   * `node_block` row counts; server-side inserts of header acceptance and
   * re-orgs are not counted, their `node_block` rows are). Readers use it
   * to choose a plan (the correction's cost grows with it), never for a
   * result.
   */
  lagRows: number;
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
  /** The spend watermark (at most visible0). */
  spend: bigint;
  /** The stored UTXO set per pinned node (read with `{ utxo: true }`; nodes without one are absent). */
  utxoByNode?: ReadonlyMap<number, UtxoSetMark>;
}

/**
 * The snapshot query. One statement; the scalar subqueries run in data
 * dependency order (each references the previous result, so ClickHouse must
 * evaluate it first):
 * 1. visible(n) of every pinned node and visible(0), from one read of
 *    `visibility`, and the spend watermark (`spend_watermark`, pseudo node
 *    `spendWatermarkId`) in the same scalar subquery, capped at visible(0);
 * 2. the committed tail above visible(0);
 * 3. `bound` = the highest seq any view of this snapshot can show (over
 *    every pinned node); the void set up to `bound` (read after 1-2, so
 *    every aborted seq at or below a watermark is in it: abort writes
 *    `commit_void` before the commit becomes terminal, and watermarks only
 *    pass terminal commits);
 * 4. the fences of epochs up to `bound`'s epoch (read after 1-2: a new
 *    holder writes its fences before its first commit).
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
  spendWatermark: string;
  /** With the stored UTXO set (utxo-set.ts): `utxo_watermark`. */
  utxoWatermark?: string;
}) => `WITH
  (SELECT maxMap([n], [v]) FROM
     (SELECT node_internal_id AS n, visible_seq AS v FROM ${tables.visibility}
      WHERE node_internal_id = 0 OR has({nodes:Array(UInt32)}, node_internal_id)
      UNION ALL
      SELECT toUInt32(${spendWatermarkId}), watermark FROM ${
  tables.spendWatermark
})) AS max_by_node,
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
      WHERE epoch <= bitShiftRight(bound, 40) GROUP BY epoch)) AS fences${
        tables.utxoWatermark === undefined
          ? ''
          : `,
  (SELECT (groupArray(n), groupArray(wg)) FROM
     (SELECT node_internal_id AS n, max((watermark, generation)) AS wg FROM ${tables.utxoWatermark}
      WHERE has({nodes:Array(UInt32)}, node_internal_id)
        AND watermark <= marks.1[indexOf({nodes:Array(UInt32)}, node_internal_id)]
      GROUP BY node_internal_id)) AS utxo_marks,
  (SELECT (groupArray(n), groupArray((c, l))) FROM
     (SELECT n, count() AS c,
        sum(row_counts['tx_acceptance'] + row_counts['node_transaction'] + row_counts['node_block']) AS l
      FROM ${tables.commitLog} ARRAY JOIN node_scope AS n
      WHERE state = 'committed'
        AND commit_seq > arrayMin(arrayPushBack(arrayMap(m -> m.1, utxo_marks.2), bound))
        AND commit_seq <= bound
        AND has(utxo_marks.1, n)
        AND commit_seq > utxo_marks.2[indexOf(utxo_marks.1, n)].1
        AND commit_seq <= marks.1[indexOf({nodes:Array(UInt32)}, n)]
      GROUP BY n)) AS utxo_lags`
      }
SELECT
  arrayMap(x -> toString(x), marks.1) AS visible,
  toString(marks.2) AS visible0,
  toString(least(marks.2, if(indexOf(max_by_node.1, ${spendWatermarkId}) = 0, toUInt64(0),
                             max_by_node.2[indexOf(max_by_node.1, ${spendWatermarkId})]))) AS spend,
  arrayMap(x -> toString(x), tail_seqs) AS tail,
  arrayMap(x -> toString(x), void_seqs) AS void,
  arrayMap(e -> toString(if(indexOf(fences.1, e) = 0, ${counterMaskSeq.toString()},
                            bitAnd(fences.2[indexOf(fences.1, e)], ${counterMaskSeq.toString()}))),
           range(1, toUInt64(bitShiftRight(bound, 40)) + 1)) AS fence${
             tables.utxoWatermark === undefined
               ? ''
               : `,
  arrayMap(n -> if(indexOf(utxo_marks.1, n) = 0, '',
                   concat(toString(utxo_marks.2[indexOf(utxo_marks.1, n)].1), ':',
                          toString(utxo_marks.2[indexOf(utxo_marks.1, n)].2), ':',
                          if(indexOf(utxo_lags.1, n) = 0, '0:0',
                             concat(toString(utxo_lags.2[indexOf(utxo_lags.1, n)].1), ':',
                                    toString(utxo_lags.2[indexOf(utxo_lags.1, n)].2))))),
           {nodes:Array(UInt32)}) AS utxo`
           }`;

const snapshotTables = {
  commitLog: 'commit_log',
  commitVoid: 'commit_void',
  epochFence: 'epoch_fence',
  spendWatermark: 'spend_watermark',
  visibility: 'visibility',
};

const snapshotQuery = snapshotSql(snapshotTables);

const snapshotQueryWithUtxo = snapshotSql({
  ...snapshotTables,
  utxoWatermark: 'utxo_watermark',
});

/** Options of a snapshot read. */
export interface SnapshotReadOptions {
  /**
   * Also read each pinned node's stored UTXO set mark (`utxo_watermark`;
   * the table must exist: 090_utxo_set.sql).
   */
  utxo?: boolean;
}

/** `U:generation:lagCommits:lagRows` (snapshot column `utxo`) → mark; '' → undefined. */
const parseUtxoMark = (value: string | undefined): UtxoSetMark | undefined => {
  if (value === undefined || value === '') return undefined;
  const [watermark, generation, commits, rows] = value.split(':');
  return {
    generation: Number(generation),
    lagCommits: Number(commits ?? '0'),
    lagRows: Number(rows ?? '0'),
    watermark: BigInt(watermark!),
  };
};

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
  nodeIds: readonly number[],
  options: SnapshotReadOptions = {}
): Promise<MultiNodeSnapshot> => {
  const nodes = [...new Set(nodeIds)].sort((a, b) => a - b);
  const rows = await client.query<{
    visible: string[];
    visible0: string;
    spend: string;
    tail: string[];
    void: string[];
    fence: string[];
    utxo?: string[];
  }>(options.utxo === true ? snapshotQueryWithUtxo : snapshotQuery, {
    nodes,
    voidLimit: voidInlineLimit + 1,
  });
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
  const visible = row?.visible ?? [];
  const utxoMarks = row?.utxo;
  return {
    committedTail: (row?.tail ?? []).map(BigInt),
    fence,
    nodeIds: nodes,
    spend: BigInt(row?.spend ?? '0'),
    ...(utxoMarks === undefined
      ? {}
      : {
          utxoByNode: new Map(
            nodes.flatMap((node, index) => {
              const mark = parseUtxoMark(utxoMarks[index]);
              return mark === undefined ? [] : [[node, mark] as const];
            })
          ),
        }),
    visible0: BigInt(row?.visible0 ?? '0'),
    visibleByNode: new Map(
      nodes.map((node, index) => [node, BigInt(visible[index] ?? '0')])
    ),
    void: voidOverflow ? [voidOverflowSentinel] : voidSeqs,
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
  const utxo = snapshot.utxoByNode?.get(nodeId);
  return {
    committedTail: snapshot.committedTail,
    fence: snapshot.fence,
    nodeId,
    spend: snapshot.spend,
    ...(utxo === undefined ? {} : { utxo }),
    visible,
    visible0: snapshot.visible0,
    void: snapshot.void,
    voidOverflow: snapshot.voidOverflow,
  };
};

/** Read a snapshot of one node in one query (`readSnapshotMulti` of `[nodeId]`). */
export const readSnapshot = async (
  client: Pick<ClickHouseClient, 'query'>,
  nodeId: number,
  options: SnapshotReadOptions = {}
): Promise<VisibilitySnapshot> =>
  snapshotForNode(await readSnapshotMulti(client, [nodeId], options), nodeId);

/** The cheap watermark read of `SnapshotCache`: visibility only, no subquery. */
export const watermarksSql = `SELECT node_internal_id AS node, toString(max(visible_seq)) AS visible
  FROM visibility WHERE node_internal_id IN {nodes:Array(UInt32)} GROUP BY node_internal_id`;

/**
 * The same with each node's newest stored UTXO set mark `U:generation`
 * (`utxo` '' on visibility rows; `visible` '' on mark rows).
 */
export const watermarksWithUtxoSql = `SELECT node_internal_id AS node, toString(max(visible_seq)) AS visible, '' AS utxo
  FROM visibility WHERE node_internal_id IN {nodes:Array(UInt32)} GROUP BY node_internal_id
  UNION ALL
  SELECT node_internal_id AS node, '' AS visible,
    concat(toString(max((watermark, generation)).1), ':', toString(max((watermark, generation)).2)) AS utxo
  FROM utxo_watermark WHERE node_internal_id IN {nodes:Array(UInt32)} GROUP BY node_internal_id`;

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

  /** The cache key of a node set (and whether it reads the UTXO marks). */
  private static key(nodeIds: readonly number[], options: SnapshotReadOptions) {
    const nodes = [...new Set(nodeIds)].sort((a, b) => a - b);
    return {
      key: `${options.utxo === true ? 'u' : ''}${nodes.join(',')}`,
      nodes,
    };
  }

  /** The cached snapshot of a node set, unverified (`verify`), or undefined. */
  peek(nodeIds: readonly number[], options: SnapshotReadOptions = {}) {
    return this.entries.get(SnapshotCache.key(nodeIds, options).key);
  }

  /**
   * Drop a node set's cached snapshot if it is still `stale` (a watermark
   * check found newer marks): until a checked read caches the new one, no
   * request runs on it speculatively again.
   */
  forget(
    nodeIds: readonly number[],
    options: SnapshotReadOptions,
    stale: MultiNodeSnapshot
  ) {
    const { key } = SnapshotCache.key(nodeIds, options);
    if (this.entries.get(key) === stale) this.entries.delete(key);
  }

  /**
   * Whether `cached` is still the snapshot of the node set: one watermark
   * query (read now) whose marks equal the cached snapshot's.
   */
  async verify(
    nodeIds: readonly number[],
    options: SnapshotReadOptions,
    cached: MultiNodeSnapshot
  ) {
    const { nodes } = SnapshotCache.key(nodeIds, options);
    const withUtxo = options.utxo === true;
    const rows = await this.client.query<{
      node: number;
      visible: string;
      utxo?: string;
    }>(withUtxo ? watermarksWithUtxoSql : watermarksSql, {
      nodes: [nodeAgnosticId, ...nodes],
    });
    const marks = new Map(
      rows
        .filter((row) => row.visible !== '')
        .map((row) => [Number(row.node), BigInt(row.visible)])
    );
    const utxoMarks = new Map(
      rows
        .filter((row) => row.utxo !== undefined && row.utxo !== '')
        .map((row) => [Number(row.node), row.utxo!])
    );
    /*
     * The spend watermark is not compared: a cached (lower) one is still
     * exact, the tail join just covers more (spend-index.md, read rule).
     * The UTXO set mark is: a cached lower one would still be exact, but
     * every request would pay the correction until the next commit, and
     * an older generation's rows are dropped some time after a rebase
     * (utxo-async.md). While visible(n) stands still, the newest mark is
     * at most visible(n), so it is the mark a fresh snapshot would take.
     */
    return (
      (marks.get(nodeAgnosticId) ?? 0n) === cached.visible0 &&
      nodes.every(
        (node) => (marks.get(node) ?? 0n) === cached.visibleByNode.get(node)
      ) &&
      (!withUtxo ||
        nodes.every((node) => {
          const mark = cached.utxoByNode?.get(node);
          return (
            utxoMarks.get(node) ===
            (mark === undefined
              ? undefined
              : `${mark.watermark}:${mark.generation}`)
          );
        }))
    );
  }

  async read(
    nodeIds: readonly number[],
    options: SnapshotReadOptions = {}
  ): Promise<MultiNodeSnapshot> {
    const { key, nodes } = SnapshotCache.key(nodeIds, options);
    const cached = this.entries.get(key);
    if (cached !== undefined && (await this.verify(nodes, options, cached))) {
      this.hits += 1;
      return cached;
    }
    this.misses += 1;
    const fresh = await readSnapshotMulti(this.client, nodes, options);
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

/** `spend = …` for the spend index's pinned view (`spend_at`). */
export const spendViewArgs = 'spend = {spend:UInt64}';

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
    name === 'spend_at' || name === 'authchain_link_at'
      ? spendViewArgs
      : agnosticPinnedViews.includes(name)
      ? agnosticViewArgs
      : nodeViewArgs
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

/**
 * Every parameter of one snapshot (for requests that use both view kinds and
 * `spend_at`), and with a stored UTXO set `utxo` (U) and `utxo_generation`.
 */
export const snapshotParams = (snapshot: VisibilitySnapshot) => ({
  ...agnosticViewParams(snapshot),
  ...nodeViewParams(snapshot),
  spend: snapshot.spend,
  ...(snapshot.utxo === undefined
    ? {}
    : {
        utxo: snapshot.utxo.watermark,

        utxo_generation: snapshot.utxo.generation,
      }),
});

/**
 * The pinned `*_at` views (and their `*_v` convenience twins): base table,
 * select list, and for the membership views the collapsing GROUP BY. This is
 * the one definition of the gate: `inlinePinnedView` builds the API's inlined
 * form from it, and `view-ddl.ts` generates `ddl/050_views.sql` (and the
 * `spend_at` / `authchain_link_at` statements of 070/080) from it
 * (`yarn clickhouse:views:generate`; `view-ddl.spec.ts` fails when the
 * committed DDL differs from the generated text).
 */
export interface PinnedViewDefinition {
  /** The base table (in the `cg` database). */
  table: string;
  /** Select list items (column names or `expr AS name`). */
  select: readonly string[];
  /** node: node-scoped gate; agnostic: visible(0) and the committed tail; spend: the spend watermark. */
  kind: 'agnostic' | 'node' | 'spend';
  /** Membership views: GROUP BY these key columns with a positive sum of signs. */
  groupBy?: readonly string[];
  /** The one-line comment above the view in the DDL. */
  comment?: string;
}

const membershipSelect = (
  keys: readonly string[],
  values: readonly string[]
): readonly string[] => [...keys, ...values];

/** Every pinned view, in DDL order (node-scoped, node-agnostic, spend). */
/* eslint-disable sort-keys */
export const pinnedViewDefinitions: {
  readonly [name: string]: PinnedViewDefinition;
} = {
  node_block_at: {
    comment: 'block-accepted(n, b)',
    groupBy: ['node_internal_id', 'block_internal_id'],
    kind: 'node',
    select: membershipSelect(
      ['node_internal_id', 'block_internal_id'],
      [
        'any(block_hash) AS block_hash',
        'any(height) AS height',
        'argMaxIf(accepted_at, version, sign > 0) AS accepted_at',
      ]
    ),
    table: 'node_block',
  },
  node_transaction_at: {
    comment: "n's current mempool",
    groupBy: ['node_internal_id', 'transaction_internal_id'],
    kind: 'node',
    select: membershipSelect(
      ['node_internal_id', 'transaction_internal_id'],
      [
        'any(transaction_hash) AS transaction_hash',
        'argMaxIf(validated_at, version, sign > 0) AS validated_at',
      ]
    ),
    table: 'node_transaction',
  },
  tx_acceptance_at: {
    comment:
      "tx-accepted(n, t): one row per (tx, accepting block of n, or 0 = n's mempool)",
    groupBy: ['transaction_hash', 'node_internal_id', 'block_internal_id'],
    kind: 'node',
    select: membershipSelect(
      ['transaction_hash', 'node_internal_id', 'block_internal_id'],
      [
        'any(transaction_internal_id) AS transaction_internal_id',
        'any(height) AS height',
        'argMaxIf(accepted_at, version, sign > 0) AS accepted_at',
      ]
    ),
    table: 'tx_acceptance',
  },
  utxo_at: {
    comment: 'unspent(n, o), by category + commitment',
    groupBy: [
      'node_internal_id',
      'token_category',
      'nonfungible_token_commitment_key',
      'transaction_hash',
      'output_index',
    ],
    kind: 'node',
    select: membershipSelect(
      [
        'node_internal_id',
        'token_category',
        'nonfungible_token_commitment_key',
        'transaction_hash',
        'output_index',
      ],
      [
        'any(transaction_internal_id) AS transaction_internal_id',
        'any(created_height) AS created_height',
        'any(value_satoshis) AS value_satoshis',
        'any(locking_bytecode) AS locking_bytecode',
        'any(fungible_token_amount) AS fungible_token_amount',
        'any(nonfungible_token_capability) AS nonfungible_token_capability',
        'any(nonfungible_token_commitment) AS nonfungible_token_commitment',
      ]
    ),
    table: 'utxo',
  },
  utxo_by_script_at: {
    comment: 'unspent(n, o), by locking bytecode prefix',
    groupBy: [
      'node_internal_id',
      'locking_bytecode_prefix',
      'transaction_hash',
      'output_index',
    ],
    kind: 'node',
    select: membershipSelect(
      [
        'node_internal_id',
        'locking_bytecode_prefix',
        'transaction_hash',
        'output_index',
      ],
      [
        'any(transaction_internal_id) AS transaction_internal_id',
        'any(created_height) AS created_height',
        'any(value_satoshis) AS value_satoshis',
        'any(locking_bytecode) AS locking_bytecode',
        'any(token_category) AS token_category',
        'any(fungible_token_amount) AS fungible_token_amount',
        'any(nonfungible_token_capability) AS nonfungible_token_capability',
        'any(nonfungible_token_commitment) AS nonfungible_token_commitment',
      ]
    ),
    table: 'utxo_by_script',
  },
  node_block_history_at: {
    comment: 'per-node block history',
    kind: 'node',
    select: [
      'node_internal_id',
      'removed_at',
      'block_internal_id',
      'internal_id',
      'accepted_at',
    ],
    table: 'node_block_history',
  },
  node_transaction_history_at: {
    comment: 'per-node transaction history (replaced_at NULL = confirmed)',
    kind: 'node',
    select: [
      'node_internal_id',
      'transaction_internal_id',
      'internal_id',
      'validated_at',
      'replaced_at',
    ],
    table: 'node_transaction_history',
  },
  block_at: { kind: 'agnostic', select: ['*'], table: 'block' },
  transaction_at: { kind: 'agnostic', select: ['*'], table: 'transaction' },
  block_transaction_at: {
    kind: 'agnostic',
    select: ['*'],
    table: 'block_transaction',
  },
  output_at: {
    kind: 'agnostic',
    /* MATERIALIZED columns are not part of `*` */
    select: [
      '*',
      'locking_bytecode_prefix',
      'nonfungible_token_commitment_key',
    ],
    table: 'output',
  },
  input_at: { kind: 'agnostic', select: ['*'], table: 'input' },
  spend_at: {
    kind: 'spend',
    select: [
      '*',
      'locking_bytecode_prefix',
      'nonfungible_token_commitment_key',
    ],
    table: 'spend',
  },
  authchain_link_at: { kind: 'spend', select: ['*'], table: 'authchain_link' },
};
/* eslint-enable sort-keys */

/** The views `inlinePinnedView` can inline. */
export const inlinablePinnedViews = Object.keys(pinnedViewDefinitions);

/**
 * The gate terms (AND-ed) of a pinned view of `kind`: the general form of
 * the DDL views (`snapshot` undefined: every term, for any parameter
 * values), or the inlined form for a snapshot, which leaves out terms that
 * are constant for the arrays bound with it (the committed-tail term when
 * `committedTail` is empty, the void terms when `void` is empty, the
 * `commit_void` fallback unless `void` is the overflow sentinel). Both give
 * the same rows for the parameters of that snapshot (visibility.spec.ts).
 * `qualifier`: database prefix of `commit_void` (the DDL writes `cg.`).
 */
export const pinnedGateTerms = (
  kind: PinnedViewDefinition['kind'],
  {
    params = { node: 'node', visible: 'visible' },
    qualifier = '',
    snapshot,
  }: {
    params?: { node: string; visible: string };
    qualifier?: string;
    snapshot?: Pick<VisibilitySnapshot, 'committedTail' | 'void'>;
  } = {}
): string[] => {
  if (kind === 'spend') return ['batch <= {spend:UInt64}'];
  const commitVoid = `commit_seq NOT IN (SELECT commit_seq FROM ${qualifier}commit_void)`;
  const voidTerms =
    snapshot === undefined
      ? [
          'NOT has({void:Array(UInt64)}, commit_seq)',
          `(NOT has({void:Array(UInt64)}, ${voidOverflowSentinel.toString()}) OR ${commitVoid})`,
        ]
      : snapshot.void.length === 0
      ? []
      : [
          'NOT has({void:Array(UInt64)}, commit_seq)',
          ...(snapshot.void[0] === voidOverflowSentinel ? [commitVoid] : []),
        ];
  return [
    ...(kind === 'node'
      ? [
          `node_internal_id = {${params.node}:UInt32}`,
          `commit_seq <= {${params.visible}:UInt64}`,
        ]
      : [
          snapshot !== undefined && snapshot.committedTail.length === 0
            ? 'commit_seq <= {visible0:UInt64}'
            : '(commit_seq <= {visible0:UInt64} OR has({tail:Array(UInt64)}, commit_seq))',
        ]),
    `bitAnd(commit_seq, ${counterMaskSeq.toString()}) <= arrayElement({fence:Array(UInt64)}, bitShiftRight(commit_seq, 40))`,
    ...voidTerms,
  ];
};

/**
 * The rows of the pinned view `name` as a subquery over its base table with
 * the gate predicate inlined: `(SELECT … FROM table WHERE <gate>)`. Same
 * placeholders as `pinnedView` (bind `snapshotParams(snapshot)`; a further
 * pinned node's view uses `params.node` / `params.visible` as its
 * placeholder names), same rows as the view for any parameter values.
 *
 * Why: every view reference costs the analyzer about 3-4 ms on ClickHouse
 * 26.8 (the view's query is analysed again per reference, with its array
 * parameters), so a statement of 12-20 references paid 40-80 ms before
 * reading a row (phase2-api.md §6.1). The inlined form costs a fraction.
 *
 * The text depends on `snapshot` only to leave out terms that are constant
 * for the arrays bound with it (`pinnedGateTerms`). The fence term always
 * stays (it also bounds a forged watermark, as in the views). The
 * parameters still come only from `readSnapshot(Multi)`.
 *
 * `keyFilter`: a predicate on the view's key columns (for a membership view,
 * its GROUP BY columns), applied to the base rows before the gate. Same rows
 * as filtering the view, but the primary key is used even where ClickHouse
 * does not push a filter below the GROUP BY (a recursive CTE step). It may
 * also use a column that is constant within each group, which ClickHouse
 * never pushes down itself: tx_acceptance's `height` is the accepting
 * block's height in every row of a (transaction, node, block) group, and 0
 * in a mempool group (fix-pass-6.md §3).
 */
/* eslint-disable max-params */
export const inlinePinnedView = (
  name: string,
  snapshot: Pick<VisibilitySnapshot, 'committedTail' | 'void'>,
  params: { node: string; visible: string } = {
    node: 'node',
    visible: 'visible',
  },
  keyFilter?: string
) => {
  const body = pinnedViewDefinitions[name];
  if (body === undefined) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new RangeError(`No inline form for the pinned view ${name}.`);
  }
  const terms = [
    ...(keyFilter === undefined ? [] : [`(${keyFilter})`]),
    ...pinnedGateTerms(body.kind, { params, snapshot }),
  ];
  return `(SELECT ${body.select.join(', ')} FROM ${
    body.table
  } WHERE ${terms.join(' AND ')}${
    body.groupBy === undefined
      ? ''
      : ` GROUP BY ${body.groupBy.join(', ')} HAVING sum(sign) > 0`
  })`;
};
/* eslint-enable max-params */

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
  /** Called after a publish that advanced a watermark (background readers: the UTXO set maintainer). */
  onPublished?: () => void;
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
      this.options.onPublished?.();
    }
    return watermarks;
  }
}
