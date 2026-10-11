/* eslint-disable functional/no-let, functional/no-loop-statement, no-await-in-loop, @typescript-eslint/no-magic-numbers */
// cspell:ignore clickhouse authchain authchains
/**
 * The authchain index (ddl/080_authchain.sql): identity-output chains as
 * heavy-path segments, filled per spend batch.
 *
 * Transaction c continues p when it spends p's output 0. For every
 * transaction of a batch that spends at least one output 0 (the batch's
 * spend rows with `outpoint_index = 0`, forward and reverse passes alike)
 * and has no row yet, one row places it on the segment of its heaviest such
 * parent (largest `size`; the lowest input index on a tie) at the parent's
 * position + 1, with size = 1 + the parents' sizes. A parent without a row
 * starts its own segment (its transaction internal id, position 0, size 1).
 * Parents in the same batch are placed first (topological order).
 *
 * The rows are a placement, not the chain: the reader keeps the members its
 * node accepts and follows the input table across segment ends, so missing
 * or late parent information only adds a segment boundary. Heavy-path
 * placement bounds the boundaries on any chain by log2 of the transactions
 * (chipnet: at most 6 over every chain, against 245 when following the first
 * output-0 input).
 */
import type { ClickHouseClient } from './client.js';
import { RowBinaryWriter } from './row-binary.js';

export const authchainLinkColumns = [
  'transaction_hash',
  'segment',
  'position',
  'size',
  'commit_seq',
  'batch',
] as const;

/** Sizes saturate here (conflicting spends of one output count a subtree twice). */
const maxSize = 2n ** 62n;

interface Edge {
  child: string;
  inputIndex: number;
  parent: string;
  commitSeq: bigint;
}

interface Placement {
  segment: bigint;
  position: number;
  size: bigint;
}

/**
 * Place the transactions of spend batch `batch` (its spend rows must be
 * written). Returns the rows written. Idempotent: transactions that already
 * have a row are left alone, and the insert is deduplicated by `token`.
 */
export const fillAuthchainLinks = async (
  client: ClickHouseClient,
  batch: bigint,
  token: string
): Promise<number> => {
  const edges = (
    await client.query<{
      c: string;
      p: string;
      i: number;
      s: string;
    }>(
      `SELECT lower(hex(transaction_hash)) AS c, lower(hex(outpoint_transaction_hash)) AS p,
         input_index AS i, toString(commit_seq) AS s
       FROM spend WHERE batch = {batch:UInt64} AND outpoint_index = 0`,
      { batch }
    )
  ).map(
    (row): Edge => ({
      child: row.c,
      commitSeq: BigInt(row.s),
      inputIndex: Number(row.i),
      parent: row.p,
    })
  );
  if (edges.length === 0) return 0;
  const batchEdges = `SELECT transaction_hash, outpoint_transaction_hash FROM spend
    WHERE batch = {batch:UInt64} AND outpoint_index = 0`;
  /* rows of the batch's children and parents placed by earlier batches */
  const placed = new Map<string, Placement>(
    (
      await client.query<{
        h: string;
        s: string;
        p: number;
        z: string;
      }>(
        `SELECT lower(hex(transaction_hash)) AS h, toString(min(segment)) AS s,
           argMin(position, segment) AS p, toString(argMin(size, segment)) AS z
         FROM authchain_link
         WHERE batch < {batch:UInt64} AND transaction_hash IN (
           SELECT transaction_hash FROM (${batchEdges})
           UNION ALL SELECT outpoint_transaction_hash FROM (${batchEdges}))
         GROUP BY transaction_hash`,
        { batch }
      )
    ).map((row) => [
      row.h,
      {
        position: Number(row.p),
        segment: BigInt(row.s),
        size: BigInt(row.z),
      },
    ])
  );
  /* internal ids of the parents (a parent without a row starts a segment) */
  const internalIds = new Map(
    (
      await client.query<{ h: string; id: string }>(
        `SELECT lower(hex(hash)) AS h, toString(min(internal_id)) AS id FROM transaction
         WHERE hash IN (SELECT outpoint_transaction_hash FROM (${batchEdges}))
         GROUP BY hash`,
        { batch }
      )
    ).map((row) => [row.h, BigInt(row.id)])
  );
  const parentsOf = new Map<string, Edge[]>();
  edges.forEach((edge) => {
    if (placed.has(edge.child)) return;
    const list = parentsOf.get(edge.child);
    if (list === undefined) parentsOf.set(edge.child, [edge]);
    else list.push(edge);
  });
  /* topological order: a child after its parents placed in this batch */
  const waiting = new Map<string, number>();
  const dependents = new Map<string, string[]>();
  parentsOf.forEach((parents, child) => {
    const inBatch = [
      ...new Set(
        parents
          .map((edge) => edge.parent)
          .filter((parent) => parent !== child && parentsOf.has(parent))
      ),
    ];
    waiting.set(child, inBatch.length);
    inBatch.forEach((parent) => {
      const list = dependents.get(parent);
      if (list === undefined) dependents.set(parent, [child]);
      else list.push(child);
    });
  });
  const ready = [...waiting]
    .filter(([, count]) => count === 0)
    .map(([child]) => child);
  const order: string[] = [];
  /* `ready` grows while it is walked */
  for (const child of ready) {
    order.push(child);
    (dependents.get(child) ?? []).forEach((dependent) => {
      const count = waiting.get(dependent)! - 1;
      waiting.set(dependent, count);
      if (count === 0) ready.push(dependent);
    });
  }
  /* a cycle cannot happen in valid data; place anything left in any order */
  waiting.forEach((count, child) => {
    if (count > 0) order.push(child);
  });
  const placement = (parent: string): Placement =>
    placed.get(parent) ?? {
      position: 0,
      segment: internalIds.get(parent) ?? 0n,
      size: 1n,
    };
  const writer = new RowBinaryWriter();
  let written = 0;
  order.forEach((child) => {
    const parents = parentsOf.get(child)!;
    let heavy = parents[0]!;
    let heavySize = -1n;
    let size = 1n;
    parents.forEach((edge) => {
      const parentSize = placement(edge.parent).size;
      size += parentSize;
      if (
        parentSize > heavySize ||
        (parentSize === heavySize && edge.inputIndex < heavy.inputIndex)
      ) {
        heavy = edge;
        heavySize = parentSize;
      }
    });
    const from = placement(heavy.parent);
    /* the parent transaction is not stored: no placement */
    if (from.segment === 0n) return;
    const row: Placement = {
      position: from.position + 1,
      segment: from.segment,
      size: size > maxSize ? maxSize : size,
    };
    placed.set(child, row);
    writer
      .fixedString32(child)
      .uint64(row.segment)
      .uint32(row.position)
      .uint64(row.size)
      .uint64(heavy.commitSeq)
      .uint64(batch)
      .endRow();
    written += 1;
  });
  await client.insertRowBinary(
    'authchain_link',
    authchainLinkColumns,
    writer.finish(),
    { deduplicationToken: token }
  );
  return written;
};

export interface AuthchainBackfillProgress {
  event: 'authchain_backfill_progress';
  batch: string;
  batches: number;
  of: number;
  rows: number;
}

/**
 * Place the transactions of every finished spend batch, in batch order (for
 * a database whose spend index predates the authchain index). Batches whose
 * transactions are already placed write nothing. Safe to run while the
 * filler runs: a batch it fills concurrently at most adds boundaries.
 */
export const backfillAuthchainLinks = async (
  client: ClickHouseClient,
  options: {
    runId?: string;
    onProgress?: (progress: AuthchainBackfillProgress) => void;
  } = {}
) => {
  const batches = (
    await client.query<{ w: string }>(
      `SELECT DISTINCT toString(batch) AS w FROM spend
       WHERE batch <= (SELECT max(watermark) FROM spend_watermark)
       ORDER BY batch`
    )
  ).map((row) => BigInt(row.w));
  const runId = options.runId ?? Date.now().toString(36);
  let rows = 0;
  for (const [index, batch] of batches.entries()) {
    rows += await fillAuthchainLinks(
      client,
      batch,
      `authchain:backfill:${batch}:${runId}`
    );
    options.onProgress?.({
      batch: batch.toString(),
      batches: index + 1,
      event: 'authchain_backfill_progress',
      of: batches.length,
      rows,
    });
  }
  return { batches: batches.length, rows };
};
