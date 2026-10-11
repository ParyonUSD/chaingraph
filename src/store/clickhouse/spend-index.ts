/* eslint-disable functional/no-let, functional/no-loop-statement, no-await-in-loop, functional/no-try-statement, @typescript-eslint/naming-convention, camelcase, @typescript-eslint/no-magic-numbers, @typescript-eslint/parameter-properties, complexity, max-params, no-continue, @typescript-eslint/member-ordering, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse unhex seqs dedup
/**
 * The asynchronous spend index (`ddl/070_spend.sql`,
 * docs/clickhouse-port/spend-index.md).
 *
 * A background task of the writer (the lease holder): it fills `spend` from
 * `input ⋈ output`, server-side (`INSERT … SELECT`), in batches of commit
 * seqs `(from, to]` at or below the published node-agnostic watermark
 * visible(0), and publishes `to` as the spend watermark once the batch's rows
 * are written. It never runs on the commit path and never waits for a
 * commit.
 *
 * Exactness: every seq at or below visible(0) is terminal; a valid one is
 * committed (an aborted seq is in `commit_void` before it is terminal). A
 * batch writes:
 * - forward: inputs of commits in `(from, to]` joined to their spent output
 *   in a commit at or below `to`;
 * - orphans: inputs of `(from, to]` whose output is not stored at or below
 *   `to` (into `spend_orphan`);
 * - reverse: earlier orphans whose output a commit in `(from, to]` stored;
 * - the authchain index's placement of the batch's output-0 spenders
 *   (authchain-index.ts), tagged with the same batch.
 * So after batch `to` the index holds exactly the pairs whose input and
 * output commits are both at or below `to`, each once, tagged `batch` =
 * the batch that completed the pair. Readers take `spend` rows with
 * `batch <= W` (their snapshot's spend watermark) and every pair with
 * `greatest(input seq, output seq) > W` from the join.
 *
 * Restart-safe: rows of a batch that did not publish its watermark have
 * `batch` above the watermark (invisible) and are deleted at the next start
 * before filling resumes. Dedup tokens carry a per-run id, so a re-run after
 * that delete is never swallowed by deduplication of the earlier attempt,
 * while a retried request of one attempt is.
 */
import { fillAuthchainLinks } from './authchain-index.js';
import type { ClickHouseClient } from './client.js';
import { validCommitSql } from './utxo.js';
import { nodeAgnosticId, readWatermark } from './visibility.js';

/** A progress report of the filler (logged by the store, `spend_index_progress`). */
export interface SpendIndexProgress {
  event: 'spend_index_progress';
  /** The spend watermark after this batch. */
  watermark: string;
  /** visible(0) the batch was bounded by. */
  visible0: string;
  /** Committed commits above the spend watermark at visible(0) (the lag). */
  lagCommits: number;
  /** Rows written by the batch (forward + reverse) and orphans recorded. */
  rows: number;
  orphans: number;
  batchMs: number;
}

export interface SpendIndexStatus {
  watermark: bigint;
  visible0: bigint;
  lagCommits: number;
  running: boolean;
  batches: number;
  rows: number;
}

export interface SpendIndexOptions {
  /** Poll interval while caught up (default 250 ms). */
  intervalMs?: number;
  /** Batch bound: input + output rows of the commits in one batch (default 400k; at least one commit). */
  maxRowsPerBatch?: number;
  /** Progress reports at most this often (default 10 s; every batch while lagging when 0). */
  reportIntervalMs?: number;
  onProgress?: (progress: SpendIndexProgress) => void;
  onError?: (error: unknown) => void;
}

const zeroHash = "toFixedString(unhex(repeat('00', 32)), 32)";

export const spendColumns = [
  'transaction_hash',
  'input_index',
  'transaction_internal_id',
  'outpoint_transaction_hash',
  'outpoint_index',
  'value_satoshis',
  'locking_bytecode',
  'token_category',
  'fungible_token_amount',
  'nonfungible_token_capability',
  'nonfungible_token_commitment',
  'commit_seq',
  'spent_commit_seq',
  'batch',
] as const;

const spendOrphanColumns = [
  'outpoint_transaction_hash',
  'outpoint_index',
  'transaction_hash',
  'input_index',
  'transaction_internal_id',
  'commit_seq',
  'batch',
] as const;

/** The inputs of the batch's commits (not coinbase), valid. */
const batchInputsSql = `SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash,
    outpoint_index, commit_seq
  FROM input
  WHERE commit_seq > {from:UInt64} AND commit_seq <= {to:UInt64}
    AND outpoint_transaction_hash != ${zeroHash}
    AND ${validCommitSql()}`;

/** Outputs stored at or below `to` (valid) spent by the batch's inputs, by primary key. */
const spentOutputsSql = `SELECT transaction_hash, output_index, value_satoshis, locking_bytecode, token_category,
    fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, commit_seq
  FROM output
  WHERE (transaction_hash, output_index) IN (
      SELECT outpoint_transaction_hash, outpoint_index FROM input
      WHERE commit_seq > {from:UInt64} AND commit_seq <= {to:UInt64}
        AND outpoint_transaction_hash != ${zeroHash}
        AND ${validCommitSql()})
    AND commit_seq <= {to:UInt64}
    AND ${validCommitSql()}`;

/** Forward pass: inputs of `(from, to]` with their spent output. */
export const spendForwardSql = `INSERT INTO spend (${spendColumns.join(', ')})
SELECT i.transaction_hash, i.input_index, i.transaction_internal_id, i.outpoint_transaction_hash, i.outpoint_index,
  o.value_satoshis, o.locking_bytecode, o.token_category, o.fungible_token_amount, o.nonfungible_token_capability,
  o.nonfungible_token_commitment, i.commit_seq, o.commit_seq, {to:UInt64}
FROM (${batchInputsSql}) AS i
INNER JOIN (${spentOutputsSql}) AS o
  ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index`;

/** Inputs of `(from, to]` whose spent output is not stored at or below `to`. */
export const spendOrphanSql = `INSERT INTO spend_orphan (${spendOrphanColumns.join(
  ', '
)})
SELECT outpoint_transaction_hash, outpoint_index, transaction_hash, input_index, transaction_internal_id, commit_seq,
  {to:UInt64}
FROM (${batchInputsSql})
WHERE (outpoint_transaction_hash, outpoint_index) NOT IN (
  SELECT transaction_hash, output_index FROM (${spentOutputsSql}))`;

/** Reverse pass: orphans of finished batches whose output a commit in `(from, to]` stored. */
export const spendReverseSql = `INSERT INTO spend (${spendColumns.join(', ')})
SELECT s.transaction_hash, s.input_index, s.transaction_internal_id, s.outpoint_transaction_hash, s.outpoint_index,
  o.value_satoshis, o.locking_bytecode, o.token_category, o.fungible_token_amount, o.nonfungible_token_capability,
  o.nonfungible_token_commitment, s.commit_seq, o.commit_seq, {to:UInt64}
FROM (SELECT * FROM spend_orphan WHERE batch <= {from:UInt64}) AS s
INNER JOIN (
  SELECT transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount,
    nonfungible_token_capability, nonfungible_token_commitment, commit_seq
  FROM output
  WHERE (transaction_hash, output_index) IN (
      SELECT outpoint_transaction_hash, outpoint_index FROM spend_orphan WHERE batch <= {from:UInt64})
    AND commit_seq > {from:UInt64} AND commit_seq <= {to:UInt64}
    AND ${validCommitSql()}) AS o
  ON o.transaction_hash = s.outpoint_transaction_hash AND o.output_index = s.outpoint_index`;

/** The spend watermark (0: nothing indexed yet). */
export const readSpendWatermark = async (
  client: Pick<ClickHouseClient, 'query'>
): Promise<bigint> => {
  const rows = await client.query<{ watermark: string }>(
    'SELECT toString(max(watermark)) AS watermark FROM spend_watermark'
  );
  return BigInt(rows[0]?.watermark ?? '0');
};

/**
 * The upper seq of the next batch after `from`, at most `visible0`: the
 * committed commits in `(from, visible0]` in seq order, as many as fit in
 * `maxRows` input + output rows (at least one). `visible0` when every
 * commit in the range fits, or none is committed (aborted seqs only).
 */
export const nextBatchEnd = (
  from: bigint,
  visible0: bigint,
  commits: readonly { seq: bigint; rows: number }[],
  maxRows: number
): bigint => {
  let total = 0;
  let end = from;
  for (const commit of commits) {
    if (commit.seq <= from || commit.seq > visible0) continue;
    if (end !== from && total + commit.rows > maxRows) return end;
    total += commit.rows;
    end = commit.seq;
  }
  return visible0;
};

export interface SpendIndexContext {
  client: ClickHouseClient;
  /** Throws unless this process holds the writer lease. */
  assertHeld: () => void;
  epoch: () => bigint;
  /** The dense epoch-fence array (validCommitSql's `{fence}`). */
  fence: () => bigint[];
}

const sleep = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** The background filler of one writer epoch. */
export class SpendIndexFiller {
  private stopped = false;

  private loop: Promise<void> | undefined;

  private wake: (() => void) | undefined;

  private lastReportAt = 0;

  /** A per-run id in every dedup token (see the module comment). */
  private readonly runId = `${Date.now().toString(36)}${Math.random()
    .toString(36)
    .slice(2, 8)}`;

  readonly status: SpendIndexStatus = {
    batches: 0,
    lagCommits: 0,
    rows: 0,
    running: false,
    visible0: 0n,
    watermark: 0n,
  };

  constructor(
    private readonly context: SpendIndexContext,
    private readonly options: SpendIndexOptions = {}
  ) {}

  /** Start the loop (after the store's recovery and first publish). */
  start() {
    if (this.loop !== undefined) return;
    this.status.running = true;
    this.loop = this.run().finally(() => {
      this.status.running = false;
    });
  }

  /** Run a pass now instead of at the next poll (e.g. after a publish). */
  poke() {
    this.wake?.();
  }

  /** Stop after the current statement; resolves once the loop has ended. */
  async stop() {
    this.stopped = true;
    this.wake?.();
    await this.loop?.catch(() => undefined);
  }

  /**
   * Stop the loop, fill until caught up with visible(0) as read now, then
   * resume the loop if `resume()` (tests and tools). Returns the watermark.
   */
  async catchUpAndResume(resume: () => boolean): Promise<bigint> {
    const wasRunning = this.loop !== undefined && !this.stopped;
    await this.stop();
    try {
      return await this.catchUp();
    } finally {
      if (wasRunning && resume()) {
        this.stopped = false;
        this.loop = undefined;
        this.start();
      }
    }
  }

  /**
   * Fill until caught up with visible(0) as read now (the loop must not
   * run); returns the watermark reached.
   */
  async catchUp(): Promise<bigint> {
    const visible0 = await readWatermark(this.context.client, nodeAgnosticId);
    let watermark = await readSpendWatermark(this.context.client);
    await this.removeUnfinished(watermark);
    while (watermark < visible0) {
      watermark = await this.fillNext(watermark, visible0);
    }
    return watermark;
  }

  /** Delete rows of batches that never published their watermark. */
  async removeUnfinished(watermark: bigint) {
    const { client } = this.context;
    for (const table of ['spend', 'spend_orphan', 'authchain_link']) {
      const [row] = await client.query<{ n: string }>(
        `SELECT toString(count()) AS n FROM ${table} WHERE batch > {watermark:UInt64}`,
        { watermark }
      );
      if (row !== undefined && row.n !== '0') {
        this.context.assertHeld();
        await client.command(
          `ALTER TABLE ${table} DELETE WHERE batch > {watermark:UInt64}`,
          { watermark },
          { mutations_sync: '2' }
        );
      }
    }
  }

  private async run() {
    const { client } = this.context;
    const intervalMs = this.options.intervalMs ?? 250;
    let watermark = await readSpendWatermark(client);
    await this.removeUnfinished(watermark);
    this.status.watermark = watermark;
    while (!this.stopped) {
      try {
        const visible0 = await readWatermark(client, nodeAgnosticId);
        this.status.visible0 = visible0;
        if (visible0 > watermark) {
          watermark = await this.fillNext(watermark, visible0);
          continue;
        }
        this.status.lagCommits = 0;
      } catch (error: unknown) {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        if (this.stopped) return;
        this.options.onError?.(error);
        // a failed batch left rows above the watermark: remove them first
        await sleep(intervalMs * 4);
        try {
          watermark = await readSpendWatermark(client);
          await this.removeUnfinished(watermark);
        } catch (cleanupError: unknown) {
          this.options.onError?.(cleanupError);
        }
        continue;
      }
      await new Promise<void>((resolve) => {
        const timer = setTimeout(resolve, intervalMs);
        this.wake = () => {
          clearTimeout(timer);
          resolve();
        };
      });
      this.wake = undefined;
    }
  }

  /** Fill one batch after `from`, bounded by `visible0`; returns its end. */
  private async fillNext(from: bigint, visible0: bigint): Promise<bigint> {
    const { client } = this.context;
    const started = Date.now();
    const commits = (
      await client.query<{ seq: string; rows: string }>(
        `SELECT toString(commit_seq) AS seq,
           toString(row_counts['input'] + row_counts['output']) AS rows
         FROM commit_log FINAL
         WHERE commit_seq > {from:UInt64} AND commit_seq <= {visible0:UInt64} AND state = 'committed'
         ORDER BY commit_seq`,
        { from, visible0 }
      )
    ).map((row) => ({ rows: Number(row.rows), seq: BigInt(row.seq) }));
    const to = nextBatchEnd(
      from,
      visible0,
      commits,
      this.options.maxRowsPerBatch ?? 400_000
    );
    const params = { fence: this.context.fence(), from, to };
    const token = (step: string) => `spend:${to}:${this.runId}:${step}`;
    let rows = 0;
    let orphans = 0;
    if (commits.some((commit) => commit.seq <= to)) {
      /* outputs by primary key (granularity 128), not p_category / p_script */
      const settings = { optimize_use_projections: 0 } as const;
      this.context.assertHeld();
      await client.insertSelect(spendForwardSql, params, {
        deduplicationToken: token('f'),
        settings,
      });
      await client.insertSelect(spendOrphanSql, params, {
        deduplicationToken: token('o'),
        settings,
      });
      await client.insertSelect(spendReverseSql, params, {
        deduplicationToken: token('r'),
        settings,
      });
      /* the authchain index places the batch's output-0 spenders (080_authchain.sql) */
      this.context.assertHeld();
      await fillAuthchainLinks(client, to, token('a'));
      const [counted] = await client.query<{ rows: string; orphans: string }>(
        `SELECT (SELECT toString(count()) FROM spend WHERE batch = {to:UInt64}) AS rows,
           (SELECT toString(count()) FROM spend_orphan WHERE batch = {to:UInt64}) AS orphans`,
        { to }
      );
      rows = Number(counted?.rows ?? '0');
      orphans = Number(counted?.orphans ?? '0');
    }
    this.context.assertHeld();
    await client.insertSelect(
      `INSERT INTO spend_watermark (watermark, batch_from, rows, orphans, writer_epoch, filled_at)
       SELECT {to:UInt64}, {from:UInt64}, {rows:UInt64}, {orphans:UInt64}, {epoch:UInt64}, now64(3)`,
      { epoch: this.context.epoch(), from, orphans, rows, to },
      { deduplicationToken: token('w') }
    );
    this.status.watermark = to;
    this.status.batches += 1;
    this.status.rows += rows;
    this.status.lagCommits = commits.filter((commit) => commit.seq > to).length;
    const now = Date.now();
    const reportIntervalMs = this.options.reportIntervalMs ?? 10_000;
    if (
      this.options.onProgress !== undefined &&
      (now - this.lastReportAt >= reportIntervalMs ||
        (this.status.lagCommits === 0 && this.lastReportAt === 0))
    ) {
      this.lastReportAt = now;
      this.options.onProgress({
        batchMs: now - started,
        event: 'spend_index_progress',
        lagCommits: this.status.lagCommits,
        orphans,
        rows,
        visible0: visible0.toString(),
        watermark: to.toString(),
      });
    }
    return to;
  }
}
