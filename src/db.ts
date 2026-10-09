/* eslint-disable max-lines */
import pg from 'pg';

import type { Agent } from './agent.js';
import {
  copyStageTableSql,
  createStageTablesSql,
  encodeStageBlockTransactions,
  encodeStageInputs,
  encodeStageOutputs,
  encodeStageSpends,
  encodeStageTransactions,
} from './components/block-copy-rows.js';
import {
  computeIndexCreationProgress,
  indexDefinitions,
} from './components/db-utils.js';
import { copyFromBuffers } from './components/pg-binary-copy.js';
import type {
  DeferredBatchResult,
  SettleCandidate,
  StallState,
  UnspentDeferredKind,
} from './components/unspent-deferred.js';
import {
  backlogSql,
  batchDidWork,
  blockReacceptedEventsSql,
  configureDeferredTriggersSql,
  deferredIndexDefinitions,
  deferredQueryRootSql,
  deferredTriggerNames,
  formatBatchLog,
  headersAcceptedEventsSql,
  initializeSql,
  nextSkipThrough,
  nextStallState,
  nextTransactionIdSql,
  parseBatchResult,
  progressSql,
  readSequencesSql,
  runBatchSql,
  snapshotSettledSql,
  transactionReacceptedEventsSql,
} from './components/unspent-deferred.js';
import {
  bitmaskAcceptBlocksSql,
  bitmaskAcceptSql,
  bitmaskClearSpentSql,
  bitmaskMaxNodeInternalId,
  bitmaskNodeIndexDefinitions,
  bitmaskResolveBlocksSql,
  bitmaskResolveOutputsSql,
  bitmaskResolveSql,
  buildDeleteSpentFromSetSql,
  buildMarkSpentOutputsSql,
  buildResolveNewOutputsSql,
  collectBlockSpends,
  configureUnspentTrackingTriggersSql,
  deleteStagedSpentFromSetSql,
  firstAcceptedBlockTransactionsSql,
  markStagedSpentOutputsSql,
  outputMarkerInsertParts,
  postCommitBlocksSql,
  postCommitReleaseBlocksSql,
  postCommitReleaseTransactionsSql,
  postCommitTransactionsSql,
  reacceptSpendsSql,
  resolveMempoolOutputsSql,
  resolveStagedNewOutputsSql,
  unspentSetInsertCte,
  unspentTrackingTriggerNames,
} from './components/unspent-tracking.js';
import {
  chaingraphWritePath,
  postgresConnectionString,
  postgresMaxConnections,
  postgresSynchronousCommit,
  unspentDeferredJob,
  unspentDeferredKind,
  unspentPostCommit,
  unspentResolveNewOutputs,
  unspentTracking,
} from './config.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from './types/chaingraph.js';

export const pool = new pg.Pool({
  connectionString: postgresConnectionString,
  max: postgresMaxConnections,
});

/**
 * E17 deferred tracking job: its own connection, outside the agent's pool
 * (block saves can hold every pooled connection for minutes during a sync).
 */
export const unspentDeferredJobPool = new pg.Pool({
  connectionString: postgresConnectionString,
  max: 1,
});

/**
 * `CHAINGRAPH_UNSPENT_TRACKING` (experiment): how new outputs are written.
 * - `none`: mode `off`;
 * - `unaudited`: `spent_by_index` or `block_inclusions_index` is missing
 *   (initial sync), so POLICY A cannot run: new outputs get a NULL marker / no
 *   set row (POLICY B's "unaudited", to be audited later);
 * - `resolve`: new outputs written as unspent, then resolved (POLICY A);
 * - `trust`: as `resolve` without the resolve statement (cost measurement only,
 *   `CHAINGRAPH_UNSPENT_RESOLVE_NEW_OUTPUTS=false`).
 */
type NewOutputPolicy = 'none' | 'resolve' | 'trust' | 'unaudited';
// eslint-disable-next-line functional/no-let
let resolveIndexesPresent = false;
// eslint-disable-next-line functional/no-let
let resolveIndexesCheckedAt = 0;
/**
 * The check in flight, shared by concurrent callers: a save must not be
 * written "unaudited" just because another save's check has not returned yet.
 */
// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let resolveIndexesCheck: Promise<void> | undefined;
const resolveIndexesRecheckMs = 5_000;
const refreshResolveIndexesPresent = async (client: pg.PoolClient) => {
  if (resolveIndexesPresent) {
    return;
  }
  if (resolveIndexesCheck !== undefined) {
    await resolveIndexesCheck;
    return;
  }
  const now = Date.now();
  if (now - resolveIndexesCheckedAt <= resolveIndexesRecheckMs) {
    return;
  }
  resolveIndexesCheckedAt = now;
  resolveIndexesCheck = (async () => {
    const present =
      (
        await client.query<{ present: boolean }>(
          /* sql */ `SELECT to_regclass('public.spent_by_index') IS NOT NULL AND to_regclass('public.block_inclusions_index') IS NOT NULL AS present;`
        )
      ).rows[0]?.present === true;
    resolveIndexesPresent = present;
  })();
  // eslint-disable-next-line functional/no-try-statement
  try {
    await resolveIndexesCheck;
  } finally {
    // eslint-disable-next-line require-atomic-updates -- only the creator of the check clears it
    resolveIndexesCheck = undefined;
  }
};
const newOutputPolicy = async (
  client: pg.PoolClient
): Promise<NewOutputPolicy> => {
  if (unspentTracking === 'off') {
    return 'none';
  }
  await refreshResolveIndexesPresent(client);
  if (!resolveIndexesPresent) {
    return 'unaudited';
  }
  return unspentResolveNewOutputs ? 'resolve' : 'trust';
};
const tracksNewOutputs = (policy: NewOutputPolicy) =>
  policy === 'resolve' || policy === 'trust';
/**
 * Extra column/value for output inserts (empty unless `marker` mode tracks
 * new outputs).
 */
const outputMarkerFor = (
  policy: NewOutputPolicy,
  acceptingNodeIds: number[] = []
) => {
  const parts = outputMarkerInsertParts(
    tracksNewOutputs(policy) ? unspentTracking : 'off'
  );
  if (unspentTracking !== 'bitmask' || !tracksNewOutputs(policy)) {
    return parts;
  }
  /*
   * bitmask: new outputs are inserted with the bits of the nodes accepting
   * this save, so `unspent_bits_accept` has nothing to set on them.
   */
  const mask = acceptingNodeIds.reduce(
    // eslint-disable-next-line no-bitwise
    (bits, id) => bits | (1n << BigInt(id)),
    0n
  );
  return { column: parts.column, value: `, ${mask.toString()}::bigint` };
};
const setInsertCteFor = (
  policy: NewOutputPolicy,
  outputSource: string,
  newTransactions: string
) =>
  unspentSetInsertCte(
    tracksNewOutputs(policy) ? unspentTracking : 'off',
    outputSource,
    newTransactions
  );

/**
 * Per-save timings of the tracking statements (ms), for the agent's log.
 */
export interface UnspentTrackingTimings {
  markMs: number;
  policy: NewOutputPolicy;
  postCommit?: PostCommitResult;
  resolveMs: number;
}

/**
 * Result of the post-commit pass of one save: rows fixed by (a) (new outputs
 * resolved against now-committed spenders) and (b) (spends re-applied to
 * now-committed outputs), elapsed ms, and failed attempts that were retried.
 */
export interface PostCommitResult {
  failedAttempts: number;
  ms: number;
  newOutputsFixed: number;
  spentOutputsFixed: number;
}

/**
 * Warnings from the post-commit pass (retries); the agent sets its logger.
 */
// eslint-disable-next-line functional/no-let
let unspentTrackingWarn: (message: string) => void = () => undefined;
// eslint-disable-next-line functional/no-let
let unspentTrackingInfo: (message: string) => void = () => undefined;
export const setUnspentTrackingLoggers = (loggers: {
  info: (message: string) => void;
  warn: (message: string) => void;
}) => {
  unspentTrackingWarn = loggers.warn;
  unspentTrackingInfo = loggers.info;
};
/**
 * Mempool and headers saves have no per-save log line: report the rows their
 * post-commit pass fixed (if any).
 */
const reportPostCommit = (
  label: string,
  result: PostCommitResult | undefined
) => {
  if (
    result !== undefined &&
    (result.newOutputsFixed > 0 || result.spentOutputsFixed > 0)
  ) {
    unspentTrackingInfo(
      `Unspent tracking post-commit (${label}): fixed ${result.newOutputsFixed} new-output and ${result.spentOutputsFixed} spent-output rows in ${result.ms} ms`
    );
  }
  return result;
};
const postCommitBackoffInitialMs = 100;
const postCommitBackoffMaxMs = 10_000;
const sleep = async (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/**
 * The post-commit pass (`CHAINGRAPH_UNSPENT_POST_COMMIT`, POLICY A only): run
 * after the save's COMMIT, in its own short transaction, so a parent and a
 * child saved concurrently see each other whichever commits second. The
 * statements are idempotent, so a failed attempt (deadlock, serialization or
 * connection error) is retried with exponential backoff until it succeeds;
 * it is never skipped.
 */
const postCommitBackoffMs = (failedAttempts: number) =>
  Math.min(
    postCommitBackoffMaxMs,
    // eslint-disable-next-line @typescript-eslint/no-magic-numbers
    postCommitBackoffInitialMs * 2 ** Math.min(failedAttempts - 1, 10)
  );

interface PostCommitAttempt {
  client: pg.PoolClient;
  description: string;
  failedAttempts: number;
  parameters: unknown[];
  sql: string;
  start: number;
}

/**
 * One attempt of the post-commit statement; on failure, wait (exponential
 * backoff, capped) and try again. Never gives up: the statement is idempotent
 * and the save is already committed.
 */
const attemptPostCommit = async (
  attempt: PostCommitAttempt
): Promise<PostCommitResult> => {
  const result = await attempt.client
    .query<{ newOutputsFixed: string; spentOutputsFixed: string }>(
      attempt.sql,
      attempt.parameters
    )
    .catch((err: unknown) => err as Error);
  if (result instanceof Error) {
    const failedAttempts = attempt.failedAttempts + 1;
    const backoffMs = postCommitBackoffMs(failedAttempts);
    unspentTrackingWarn(
      `Unspent tracking post-commit pass failed (attempt ${failedAttempts}, ${
        attempt.description
      }); retrying in ${backoffMs} ms: ${String(result)}`
    );
    await sleep(backoffMs);
    return attemptPostCommit({ ...attempt, failedAttempts });
  }
  return {
    failedAttempts: attempt.failedAttempts,
    ms: Date.now() - attempt.start,
    newOutputsFixed: Number(result.rows[0]?.newOutputsFixed ?? 0),
    spentOutputsFixed: Number(result.rows[0]?.spentOutputsFixed ?? 0),
  };
};

const postCommitApplies = (policy: NewOutputPolicy, hashes: string[]) =>
  unspentTracking !== 'off' &&
  unspentPostCommit &&
  policy === 'resolve' &&
  hashes.length > 0;

/**
 * The post-commit pass (`CHAINGRAPH_UNSPENT_POST_COMMIT`, POLICY A only): run
 * after the save's COMMIT, in its own short transaction, so a parent and a
 * child saved concurrently see each other whichever commits second. The
 * statements are idempotent, so a failed attempt (deadlock, serialization or
 * connection error) is retried with exponential backoff until it succeeds;
 * it is never skipped. `kind`: `blocks` for block hashes (block saves and
 * headers acceptance), `transactions` for mempool transaction hashes.
 */
const runPostCommit = async (
  client: pg.PoolClient,
  policy: NewOutputPolicy,
  target: { hashes: string[]; kind: 'blocks' | 'transactions' }
): Promise<PostCommitResult | undefined> =>
  postCommitApplies(policy, target.hashes)
    ? attemptPostCommit({
        client,
        description: `${target.kind} ${target.hashes[0]!}${
          target.hashes.length > 1 ? ` +${target.hashes.length - 1}` : ''
        }`,
        failedAttempts: 0,
        parameters: [
          unspentTracking,
          target.hashes.map((hash) => Buffer.from(hash, 'hex')),
        ],
        sql:
          target.kind === 'blocks'
            ? postCommitBlocksSql
            : postCommitTransactionsSql,
        start: Date.now(),
      })
    : undefined;

/**
 * Post-commit pass after a delete of acceptance rows (`marker`/`settable`;
 * see `postCommitReleaseBlocksSql`). Retried like `runPostCommit`.
 */
const releasePostCommitApplies = () =>
  (unspentTracking === 'marker' || unspentTracking === 'settable') &&
  unspentPostCommit;
type PostCommitReleaseTarget =
  | { blockHashes: string[]; kind: 'blocks' }
  | { kind: 'transactions'; transactionInternalIds: number[] };
const runPostCommitRelease = async (
  client: pg.PoolClient,
  target: PostCommitReleaseTarget
) => {
  if (!releasePostCommitApplies()) {
    return undefined;
  }
  return attemptPostCommit({
    client,
    description: `release ${target.kind}`,
    failedAttempts: 0,
    parameters: [
      unspentTracking,
      target.kind === 'blocks'
        ? target.blockHashes.map((hash) => Buffer.from(hash, 'hex'))
        : target.transactionInternalIds,
    ],
    sql:
      target.kind === 'blocks'
        ? postCommitReleaseBlocksSql
        : postCommitReleaseTransactionsSql,
    start: Date.now(),
  });
};

/**
 * Release passes run detached from the delete that triggered them, on their
 * own pooled connection: the agent's re-org bookkeeping (stale removal, then
 * headers acceptance) must not wait for them. Shutdown drains them.
 */
const pendingPostCommitReleases = new Set<Promise<unknown>>();
const detachPostCommitRelease = (target: PostCommitReleaseTarget) => {
  if (!releasePostCommitApplies()) {
    return;
  }
  const pending = pool
    .connect()
    .then(async (client) =>
      runPostCommitRelease(client, target).finally(() => {
        client.release();
      })
    )
    .catch((err: unknown) => {
      unspentTrackingWarn(
        `Unspent tracking post-commit release failed: ${String(err)}`
      );
    })
    .finally(() => {
      pendingPostCommitReleases.delete(pending);
    });
  pendingPostCommitReleases.add(pending);
};
/**
 * Wait for detached post-commit release passes (call before ending the pool).
 */
export const drainUnspentTrackingPostCommits = async () =>
  Promise.all([...pendingPostCommitReleases]);

/**
 * `bitmask` mode: transactions of a block save that existed before the save
 * (their outputs were not inserted with the accepting nodes' bits). If any
 * "unknown" transaction turned out to exist already (cache miss), every
 * transaction is treated as known.
 */
const knownTransactionHashes = (
  block: ChaingraphBlock,
  attemptedTransactions: ChaingraphTransaction[],
  transactionCacheMisses: number
) => {
  if (unspentTracking !== 'bitmask') {
    return [];
  }
  if (transactionCacheMisses !== 0) {
    return block.transactions.map((transaction) => transaction.hash);
  }
  const attempted = new Set(
    attemptedTransactions.map((transaction) => transaction.hash)
  );
  return block.transactions
    .filter((transaction) => !attempted.has(transaction.hash))
    .map((transaction) => transaction.hash);
};

/**
 * `bitmask` mode, block save (after recording the acceptance): clear the
 * accepting nodes' bits on the block's spends, then POLICY A for the new
 * outputs; one statement each for all nodes. Returns elapsed ms per step.
 */
const bitmaskBlockSteps = async (
  client: pg.PoolClient,
  policy: NewOutputPolicy,
  save: {
    acceptingNodeIds: number[];
    block: ChaingraphBlock;
    newTransactionHashes: string[];
  }
) => {
  if (unspentTracking !== 'bitmask' || !tracksNewOutputs(policy)) {
    return { clearMs: 0, resolveMs: 0 };
  }
  const clearStart = Date.now();
  await client.query(
    bitmaskClearSpentSql(save.acceptingNodeIds, save.block.hash)
  );
  const resolveStart = Date.now();
  if (policy === 'resolve' && save.newTransactionHashes.length > 0) {
    await client.query(bitmaskResolveOutputsSql, [
      save.newTransactionHashes.map((hash) => Buffer.from(hash, 'hex')),
    ]);
  }
  return {
    clearMs: resolveStart - clearStart,
    resolveMs: Date.now() - resolveStart,
  };
};
const timed = async (
  sql: string | undefined,
  run: (query: string) => Promise<unknown>
) => {
  if (sql === undefined) {
    return 0;
  }
  const start = Date.now();
  await run(sql);
  return Date.now() - start;
};

/**
 * `bitmask` mode: node acceptance (call before recording it) and POLICY A
 * (call after), batched per save: one statement per accepting node. Returns
 * the elapsed ms (0 if not applicable).
 */
/* eslint-disable max-params */
const bitmaskStep = async (
  client: pg.PoolClient,
  sql: string,
  policy: NewOutputPolicy,
  nodeInternalIds: number[],
  transactionHashes: string[]
) => {
  if (
    unspentTracking !== 'bitmask' ||
    !tracksNewOutputs(policy) ||
    transactionHashes.length === 0
  ) {
    return 0;
  }
  const hashes = transactionHashes.map((hash) => Buffer.from(hash, 'hex'));
  const start = Date.now();
  await nodeInternalIds.reduce<Promise<unknown>>(
    async (chain, nodeInternalId) =>
      chain.then(async () => client.query(sql, [nodeInternalId, hashes])),
    Promise.resolve()
  );
  return Date.now() - start;
};
const bitmaskAccept = async (
  client: pg.PoolClient,
  policy: NewOutputPolicy,
  nodeInternalIds: number[],
  transactionHashes: string[]
) =>
  bitmaskStep(
    client,
    bitmaskAcceptSql,
    policy,
    nodeInternalIds,
    transactionHashes
  );
const bitmaskResolve = async (
  client: pg.PoolClient,
  policy: NewOutputPolicy,
  nodeInternalIds: number[],
  transactionHashes: string[]
) =>
  policy === 'resolve'
    ? bitmaskStep(
        client,
        bitmaskResolveSql,
        policy,
        nodeInternalIds,
        transactionHashes
      )
    : 0;
/* eslint-enable max-params */

/**
 * Re-mark (or remove from the set) the outpoints spent by transactions that
 * just gained an acceptance. No-op in `off` mode.
 */
const reacceptSpends = async (
  client: pg.PoolClient,
  transactionInternalIds: (number | string)[],
  blockAccepted = false
) => {
  const sql = reacceptSpendsSql(unspentTracking, blockAccepted);
  if (sql === undefined || transactionInternalIds.length === 0) {
    return;
  }
  await client.query(sql, [transactionInternalIds]);
};

/**
 * `bitmask` mode: node internal IDs are the bit positions, so start-up is
 * refused if any node has an ID above 63. Returns the per-node partial index
 * statements still missing.
 */
const bitmaskIndexStatements = async (client: pg.PoolClient) => {
  const nodeIds = (
    await client.query<{ id: string }>(
      /* sql */ `SELECT internal_id AS id FROM node ORDER BY internal_id;`
    )
  ).rows.map((row) => Number(row.id));
  const tooHigh = nodeIds.filter((id) => id > bitmaskMaxNodeInternalId);
  if (tooHigh.length > 0) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `CHAINGRAPH_UNSPENT_TRACKING=bitmask needs node internal IDs <= ${bitmaskMaxNodeInternalId} (bit positions); found ${tooHigh.join(
        ', '
      )}.`
    );
  }
  const existingIndexes = (
    await client.query<{ name: string }>(
      /* sql */ `SELECT indexname AS name FROM pg_indexes WHERE schemaname = 'public';`
    )
  ).rows.map((row) => row.name);
  return nodeIds.flatMap((id) =>
    Object.entries(bitmaskNodeIndexDefinitions(id))
      .filter(([name]) => !existingIndexes.includes(name))
      .map(([, definition]) => definition)
  );
};

/**
 * E17 deferred modes: enable the release-event triggers (disable them in every
 * other mode) and point the generic query root at the mode's function.
 * Returns the statements to run.
 */

const deferredConfigurationStatements = async (
  client: pg.PoolClient
): Promise<string[]> => {
  const existing = Object.fromEntries(
    (
      await client.query<{ enabled: boolean; tgname: string }>(
        /* sql */ `SELECT tgname, tgenabled <> 'D' AS enabled FROM pg_trigger WHERE tgname = ANY ($1::text[]);`,
        [deferredTriggerNames]
      )
    ).rows.map((row) => [row.tgname, row.enabled])
  );
  if (unspentDeferredKind === undefined) {
    return configureDeferredTriggersSql(false, existing);
  }
  if (Object.keys(existing).length === 0) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `CHAINGRAPH_UNSPENT_TRACKING=deferred-${unspentDeferredKind} requires migration 1791400002000_unspent_deferred.`
    );
  }
  if (unspentDeferredKind === 'bitmask') {
    const tooHigh = (
      await client.query<{ id: string }>(
        /* sql */ `SELECT internal_id AS id FROM node WHERE internal_id > $1 ORDER BY internal_id;`,
        [bitmaskMaxNodeInternalId]
      )
    ).rows.map((row) => row.id);
    if (tooHigh.length > 0) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `CHAINGRAPH_UNSPENT_TRACKING=deferred-bitmask needs node internal IDs <= ${bitmaskMaxNodeInternalId} (bit positions); found ${tooHigh.join(
          ', '
        )}.`
      );
    }
  }
  return [
    ...configureDeferredTriggersSql(true, existing),
    deferredQueryRootSql(unspentDeferredKind),
  ];
};

/**
 * E17 deferred modes: create the mode's partial indexes if missing. Called
 * when the tracking job starts (after the initial sync and the managed
 * indexes, like them), so the initial sync does not maintain them. Returns
 * the statements run.
 */
export const ensureUnspentDeferredIndexes = async () => {
  if (unspentDeferredKind === undefined) {
    return [];
  }
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const existingIndexes = (
      await client.query<{ name: string }>(
        /* sql */ `SELECT indexname AS name FROM pg_indexes WHERE schemaname = 'public';`
      )
    ).rows.map((row) => row.name);
    const nodeIds = (
      await client.query<{ id: string }>(
        /* sql */ `SELECT internal_id AS id FROM node ORDER BY internal_id;`
      )
    ).rows.map((row) => Number(row.id));
    const statements = Object.entries(
      deferredIndexDefinitions(unspentDeferredKind, nodeIds)
    )
      .filter(([name]) => !existingIndexes.includes(name))
      .map(([, definition]) => definition);
    await statements.reduce<Promise<unknown>>(
      async (chain, statement) =>
        chain.then(async () => client.query(statement)),
      Promise.resolve()
    );
    if (unspentDeferredKind === 'array') {
      // the array root has one stored-set arm per node: rebuild it for the registered nodes
      await client.query(
        /* sql */ `SELECT unspent_deferred_array_build_root();`
      );
    }
    return statements;
  } finally {
    client.release();
  }
};

/**
 * Enable the spend-release triggers of the configured
 * `CHAINGRAPH_UNSPENT_TRACKING` mode and disable the others. Returns the
 * statements run (none if the migration is missing and the mode is `off`).
 */
export const configureUnspentTracking = async () => {
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const existing = Object.fromEntries(
      (
        await client.query<{ enabled: boolean; tgname: string }>(
          // cspell:ignore tgname tgenabled
          /* sql */ `SELECT tgname, tgenabled <> 'D' AS enabled FROM pg_trigger WHERE tgname = ANY ($1::text[]);`,
          [unspentTrackingTriggerNames]
        )
      ).rows.map((row) => [row.tgname, row.enabled])
    );
    if (unspentTracking !== 'off' && Object.keys(existing).length === 0) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `CHAINGRAPH_UNSPENT_TRACKING=${unspentTracking} requires migration 1791400000000_unspent_tracking.`
      );
    }
    const statements = configureUnspentTrackingTriggersSql(
      unspentTracking,
      existing
    );
    const indexStatements =
      unspentTracking === 'bitmask' ? await bitmaskIndexStatements(client) : [];
    const deferredStatements = await deferredConfigurationStatements(client);
    await [...statements, ...indexStatements, ...deferredStatements].reduce<
      Promise<unknown>
    >(
      async (chain, statement) =>
        chain.then(async () => client.query(statement)),
      Promise.resolve()
    );
    return [...statements, ...indexStatements, ...deferredStatements];
  } finally {
    client.release();
  }
};

/**
 * Trim a Postgres "bytea"-formatted string (e.g. `\xc0de`), returning just the
 * hex (e.g. `c0de`).
 * @param bytea - the string in Postgres bytea format
 */
export const byteaStringToHex = (bytea: string) => bytea.replace('\\x', '');

/**
 * Format a hex-encoded string as a Postgres "bytea" string (e.g. `\xc0de`).
 * @param bin - the Uint8Array to format
 */
export const hexToByteaString = (hex: string) => `\\x${hex}`;

/**
 * Because Postgres accepts timestamps in simplified ISO 8601 format, this
 * method does not need to remove the trailing `Z` (which indicates UTC).
 * @param date - the data to format
 */
export const dateToTimestampWithoutTimezone = (date: Date) =>
  `'${date.toISOString()}'::timestamp`;

/**
 * The JavaScript `Date` constructor assumes timestamps without a time zone are
 * in the environment's current time zone. This method simply appends a `Z` to
 * indicate UTC time prior to constructing the `Date`.
 * @param timestampWithoutTimezone - the simplified ISO 8601-formatted date
 */
export const timestampWithoutTimezoneToDate = (
  timestampWithoutTimezone: string
) => new Date(`${timestampWithoutTimezone}Z`);

/**
 * Given a list of Chaingraph blocks from the database, return an array of
 * hashes in the positions specified by `height`. If a height is missing, `null`
 * is used to fill that position.
 * @param blocks - an array of blocks where each object contains at least a
 * height and a hash (in Postgres bytea format)
 */
export const blockArrayToHashChain = (
  blocks: { height: string; hash: Buffer }[]
) => {
  if (blocks.length === 0) {
    return [];
  }
  const sortedByHeight = blocks.sort(
    (a, b) => Number(a.height) - Number(b.height)
  );
  const bestHeight = Number(sortedByHeight[sortedByHeight.length - 1]!.height);
  const chain = Array.from({ length: bestHeight + 1 }).fill(null) as (
    | string
    | null
  )[];
  blocks.forEach((entry) => {
    chain[Number(entry.height)] = entry.hash.toString('hex');
  });
  return chain;
};

/**
 * Request the full list of all known block hashes from the database.
 */
export const getAllKnownBlockHashes = async () => {
  const client = await pool.connect();
  /*
   * Hex-encoding in Postgres avoids materializing millions of `Buffer`s and
   * converting each to hex on the (single-threaded) agent event loop, which
   * dominated startup time on large databases.
   */
  const allKnownBlockHashes = await client.query<{ hash: string }>(
    `SELECT encode("hash", 'hex') AS "hash" from "block";`
  );
  client.release();
  return allKnownBlockHashes.rows.map(({ hash }) => hash);
};

export interface IncompleteBlock {
  hash: string;
  height: number;
  linkedSizeBytes: number;
  sizeBytes: number;
  transactionCount: number;
}

export interface IncompleteBlockScan {
  incompleteBlocks: IncompleteBlock[];
  scannedBlockCount: number;
}

export interface ExpiringMempoolTransaction {
  expiresAt: Date;
  hash: string;
  nodeInternalId: number;
  nodeName: string;
  transactionInternalId: number;
  validatedAt: Date;
}

export interface ArchivedMempoolTransaction {
  hash: string;
  nodeName: string;
  replacedAt: Date | null;
}

/**
 * Find blocks for which the locally saved block_transaction rows don't sum to
 * the block's saved byte size. This avoids the SQL block encoder so it can
 * detect incomplete blocks even if encoder functions have bugs (e.g. #75).
 */
export const getIncompleteBlocks = async ({
  heightLowerBound,
  heightUpperBound,
  limit,
  nodeInternalIds,
  excludedBlockHashes,
}: {
  excludedBlockHashes: string[];
  heightLowerBound: number;
  heightUpperBound: number;
  limit: number;
  nodeInternalIds: number[];
}): Promise<IncompleteBlockScan> => {
  if (nodeInternalIds.length === 0) {
    return { incompleteBlocks: [], scannedBlockCount: 0 };
  }
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const incompleteBlockScan = await client.query<{
      incompleteBlocks: {
        hash: string;
        height: number | string;
        linkedSizeBytes: number | string;
        sizeBytes: number | string;
        transactionCount: number | string;
      }[];
      scannedBlockCount: string;
    }>(
      /* sql */ `
WITH linked_transactions AS (
  SELECT
    block.internal_id,
    block.height,
    block.hash,
    block.size_bytes,
    COUNT(block_transaction.transaction_internal_id)::bigint
      AS transaction_count,
    COALESCE(SUM(transaction.size_bytes), 0)::bigint
      AS transaction_size_bytes
    FROM block
    LEFT JOIN block_transaction
      ON block_transaction.block_internal_id = block.internal_id
    LEFT JOIN transaction
      ON transaction.internal_id = block_transaction.transaction_internal_id
    WHERE block.height >= $2
      AND block.height < $3
      AND NOT (encode(block.hash, 'hex') = ANY($5::text[]))
      AND EXISTS (
        SELECT 1 FROM node_block
          WHERE node_block.block_internal_id = block.internal_id
            AND node_block.node_internal_id = ANY($1::integer[])
      )
    GROUP BY block.internal_id
),
linked_block_sizes AS (
  SELECT
    hash,
    height,
    size_bytes,
    transaction_count,
    80 +
      CASE
        WHEN transaction_count <= 252 THEN 1
        WHEN transaction_count <= 65535 THEN 3
        WHEN transaction_count <= 4294967295 THEN 5
        ELSE 9
      END +
      transaction_size_bytes AS linked_size_bytes
    FROM linked_transactions
),
incomplete_blocks AS (
  SELECT
    encode(hash, 'hex') AS hash,
    height,
    linked_size_bytes,
    size_bytes,
    transaction_count
    FROM linked_block_sizes
    WHERE linked_size_bytes != size_bytes
    ORDER BY height ASC, hash ASC
    LIMIT $4
)
SELECT
  (SELECT COUNT(*)::bigint FROM linked_block_sizes) AS "scannedBlockCount",
  COALESCE(
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'hash', hash,
          'height', height,
          'linkedSizeBytes', linked_size_bytes,
          'sizeBytes', size_bytes,
          'transactionCount', transaction_count
        )
        ORDER BY height ASC, hash ASC
      )
      FROM incomplete_blocks
    ),
    '[]'::jsonb
  ) AS "incompleteBlocks";
`,
      [
        nodeInternalIds,
        heightLowerBound,
        heightUpperBound,
        limit,
        excludedBlockHashes,
      ]
    );
    const scan = incompleteBlockScan.rows[0]!;
    return {
      incompleteBlocks: scan.incompleteBlocks.map((block) => ({
        hash: block.hash,
        height: Number(block.height),
        linkedSizeBytes: Number(block.linkedSizeBytes),
        sizeBytes: Number(block.sizeBytes),
        transactionCount: Number(block.transactionCount),
      })),
      scannedBlockCount: Number(scan.scannedBlockCount),
    };
  } finally {
    client.release();
  }
};

/**
 * Find node_transaction rows which will expire before the provided timestamp.
 */
export const getMempoolTransactionsExpiringBefore = async ({
  expiresBefore,
  expirationMs,
}: {
  expirationMs: number;
  expiresBefore: Date;
}): Promise<ExpiringMempoolTransaction[]> => {
  const expirationInterval = `${expirationMs}::double precision * interval '1 millisecond'`;
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const transactions = await client.query<{
      expiresAt: string;
      hash: string;
      nodeInternalId: string;
      nodeName: string;
      transactionInternalId: string;
      validatedAt: string;
    }>(/* sql */ `
SELECT encode(transaction.hash, 'hex') AS "hash",
       node.name AS "nodeName",
       node_transaction.node_internal_id AS "nodeInternalId",
       node_transaction.transaction_internal_id AS "transactionInternalId",
       node_transaction.validated_at::text AS "validatedAt",
       (node_transaction.validated_at + (${expirationInterval}))::text AS "expiresAt"
  FROM node_transaction
  JOIN node
    ON node.internal_id = node_transaction.node_internal_id
  JOIN transaction
    ON transaction.internal_id = node_transaction.transaction_internal_id
  WHERE node_transaction.validated_at + (${expirationInterval}) <= ${dateToTimestampWithoutTimezone(
      expiresBefore
    )}
  ORDER BY "expiresAt", "nodeName", "hash";
`);
    return transactions.rows.map((transaction) => ({
      expiresAt: timestampWithoutTimezoneToDate(transaction.expiresAt),
      hash: transaction.hash,
      nodeInternalId: Number(transaction.nodeInternalId),
      nodeName: transaction.nodeName,
      transactionInternalId: Number(transaction.transactionInternalId),
      validatedAt: timestampWithoutTimezoneToDate(transaction.validatedAt),
    }));
  } finally {
    client.release();
  }
};

/**
 * Archive node_transaction rows for transactions that are already accepted or
 * replaced by accepted blocks for the same node. This repairs historical rows
 * missed when block inclusions are added after the node_block trigger has
 * already fired.
 *
 * The correlated input lookups use OFFSET 0 as a planning barrier. Without it,
 * Postgres can flatten the joins and scan the entire historical input table to
 * resolve conflicts for a comparatively small current mempool. Keep each lookup
 * constrained to a mempool transaction and then one of its outpoints.
 */
export const archiveMempoolTransactionsAcceptedByBlocks = async (): Promise<
  ArchivedMempoolTransaction[]
> => {
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const result = await client.query<{
      hash: string;
      nodeName: string;
      replacedAt: string | null;
    }>(/* sql */ `
WITH directly_accepted AS (
    SELECT node_transaction.node_internal_id,
           node_transaction.transaction_internal_id,
           NULL::timestamp without time zone AS replaced_at
      FROM node_transaction
      JOIN block_transaction
        ON block_transaction.transaction_internal_id = node_transaction.transaction_internal_id
      JOIN node_block
        ON node_block.node_internal_id = node_transaction.node_internal_id
       AND node_block.block_internal_id = block_transaction.block_internal_id
),
replaced_by_accepted AS (
    SELECT node_transaction.node_internal_id,
           node_transaction.transaction_internal_id,
           replacement.replaced_at
      FROM node_transaction
      CROSS JOIN LATERAL (
        SELECT MIN(node_block.accepted_at) AS replaced_at
          FROM LATERAL (
            SELECT outpoint_transaction_hash, outpoint_index
              FROM input
              WHERE transaction_internal_id = node_transaction.transaction_internal_id
                AND outpoint_transaction_hash != '\\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
              OFFSET 0
          ) mempool_input
          CROSS JOIN LATERAL (
            SELECT transaction_internal_id
              FROM input
              WHERE outpoint_transaction_hash = mempool_input.outpoint_transaction_hash
                AND outpoint_index = mempool_input.outpoint_index
                AND transaction_internal_id != node_transaction.transaction_internal_id
              OFFSET 0
          ) accepted_input
          JOIN block_transaction
            ON block_transaction.transaction_internal_id = accepted_input.transaction_internal_id
          JOIN node_block
            ON node_block.node_internal_id = node_transaction.node_internal_id
           AND node_block.block_internal_id = block_transaction.block_internal_id
          HAVING COUNT(*) > 0
      ) replacement
),
archive_candidates AS (
    SELECT node_internal_id, transaction_internal_id, replaced_at
      FROM directly_accepted
    UNION ALL
    SELECT node_internal_id, transaction_internal_id, replaced_at
      FROM replaced_by_accepted
),
archive_rows AS (
    SELECT node_internal_id,
           transaction_internal_id,
           CASE
             WHEN bool_or(replaced_at IS NULL) THEN NULL::timestamp without time zone
             ELSE MIN(replaced_at)
           END AS replaced_at
      FROM archive_candidates
      GROUP BY node_internal_id, transaction_internal_id
),
deleted_rows AS (
    DELETE FROM node_transaction
      USING archive_rows
      WHERE node_transaction.node_internal_id = archive_rows.node_internal_id
        AND node_transaction.transaction_internal_id = archive_rows.transaction_internal_id
      RETURNING node_transaction.node_internal_id,
                node_transaction.transaction_internal_id,
                node_transaction.validated_at,
                archive_rows.replaced_at
),
inserted_history AS (
    INSERT INTO node_transaction_history (node_internal_id, transaction_internal_id, validated_at, replaced_at)
      SELECT node_internal_id, transaction_internal_id, validated_at, replaced_at
        FROM deleted_rows
      RETURNING node_internal_id, transaction_internal_id, replaced_at
)
SELECT encode(transaction.hash, 'hex') AS "hash",
       node.name AS "nodeName",
       inserted_history.replaced_at::text AS "replacedAt"
  FROM inserted_history
  JOIN node
    ON node.internal_id = inserted_history.node_internal_id
  JOIN transaction
    ON transaction.internal_id = inserted_history.transaction_internal_id
  ORDER BY "nodeName", "hash";
`);
    return result.rows.map((row) => ({
      hash: row.hash,
      nodeName: row.nodeName,
      replacedAt:
        row.replacedAt === null
          ? null
          : timestampWithoutTimezoneToDate(row.replacedAt),
    }));
  } finally {
    client.release();
  }
};

/**
 * Archive a single node_transaction row. Existing history triggers handle any
 * same-node descendants with the same replaced_at timestamp.
 */
export const archiveMempoolTransaction = async ({
  nodeInternalId,
  replacedAt,
  transactionInternalId,
}: {
  nodeInternalId: number;
  replacedAt: Date;
  transactionInternalId: number;
}) => {
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const result = await client.query<{
      archivedCount: number;
    }>(
      /* sql */ `
WITH deleted_row AS (
    DELETE FROM node_transaction
      WHERE node_internal_id = $1
        AND transaction_internal_id = $2
      RETURNING node_internal_id,
                transaction_internal_id,
                validated_at,
                ${dateToTimestampWithoutTimezone(replacedAt)} AS replaced_at
),
inserted_history AS (
    INSERT INTO node_transaction_history (node_internal_id, transaction_internal_id, validated_at, replaced_at)
      SELECT node_internal_id, transaction_internal_id, validated_at, replaced_at
        FROM deleted_row
      RETURNING transaction_internal_id
)
SELECT COUNT(*)::integer AS "archivedCount" FROM inserted_history;
`,
      [nodeInternalId, transactionInternalId]
    );
    if (result.rows[0]!.archivedCount > 0) {
      detachPostCommitRelease({
        kind: 'transactions',
        transactionInternalIds: [transactionInternalId],
      });
    }
    return result.rows[0]!.archivedCount;
  } finally {
    client.release();
  }
};

/**
 * Create or update one or more trusted node in the Chaingraph database,
 * returning it's internal ID.
 */
export const registerTrustedNodeWithDb = async (node: {
  latestConnectionBeganAt: Date;
  nodeName: string;
  protocolVersion: number;
  userAgent: string;
}) => {
  const registerNode = /* sql */ `
  INSERT INTO node (name, protocol_version, user_agent, latest_connection_began_at)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT ON CONSTRAINT node_name_key
    DO UPDATE SET
      protocol_version = $2,
      user_agent = $3,
      latest_connection_began_at = $4
    RETURNING internal_id;
`;
  const client = await pool.connect();
  // eslint-disable-next-line @typescript-eslint/naming-convention
  const nodeInternalIdQuery = await client.query<{ internal_id: number }>(
    registerNode,
    [
      node.nodeName,
      node.protocolVersion,
      node.userAgent,
      node.latestConnectionBeganAt,
    ]
  );
  const internalId = nodeInternalIdQuery.rows[0]!.internal_id;
  const acceptedBlocksQuery = await client.query<{
    height: string;
    hash: Buffer;
  }>(
    /* sql */ `
  SELECT height, hash FROM block
  WHERE EXISTS
    (SELECT 1 FROM node_block WHERE
	   node_block.block_internal_id = block.internal_id AND
	   node_block.node_internal_id = $1)
  ORDER BY height ASC;
`,
    [internalId]
  );
  client.release();
  const syncedHeaderHashChain = blockArrayToHashChain(acceptedBlocksQuery.rows);
  return { internalId, syncedHeaderHashChain };
};

/**
 * Save a transaction to the known mempool of the specified nodes. If the
 * transaction already exists in the database, `transaction`, `output`, and
 * `input` insertions will be skipped, and only the new `node_transaction`s will
 * be written.
 */
export const saveTransactionForNodes = async (
  transaction: ChaingraphTransaction,
  nodeValidations: {
    nodeInternalId: number;
    validatedAt: Date;
  }[]
) => {
  const client = await pool.connect();
  const policy = await newOutputPolicy(client).catch(async (err: unknown) => {
    client.release();
    // eslint-disable-next-line functional/no-promise-reject
    return Promise.reject(err);
  });
  const outputMarker = outputMarkerFor(
    policy,
    nodeValidations.map((validation) => validation.nodeInternalId)
  );
  const saveTransaction = /* sql */ `
WITH transaction_values (hash, version, locktime, size_bytes, is_coinbase) AS (
  VALUES ('${hexToByteaString(transaction.hash)}'::bytea, ${
    transaction.version
  }::bigint, ${transaction.locktime}::bigint, ${
    transaction.sizeBytes
  }::bigint, ${transaction.isCoinbase.toString()}::boolean)
), output_values (output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment) AS (
  VALUES ${transaction.outputs
    .map(
      (output, outputIndex) =>
        `(${outputIndex}::bigint, ${output.valueSatoshis.toString()}::bigint, '${hexToByteaString(
          output.lockingBytecode
        )}'::bytea, ${
          output.tokenCategory === undefined
            ? 'NULL::bytea'
            : `'${hexToByteaString(output.tokenCategory)}'::bytea`
        }, ${
          output.fungibleTokenAmount === undefined
            ? 'NULL'
            : `${output.fungibleTokenAmount.toString()}::bigint`
        }, ${
          output.nonfungibleTokenCapability === undefined
            ? 'NULL'
            : `'${output.nonfungibleTokenCapability}'::enum_nonfungible_token_capability`
        }, ${
          output.nonfungibleTokenCommitment === undefined
            ? 'NULL'
            : `'${hexToByteaString(output.nonfungibleTokenCommitment)}'::bytea`
        })`
    )
    .join(',')}
), input_values (input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode) AS (
  VALUES ${transaction.inputs
    .map(
      (input, inputIndex) =>
        `(${inputIndex}::bigint, ${input.outpointIndex}::bigint, ${
          input.sequenceNumber
        }::bigint, '${hexToByteaString(
          input.outpointTransactionHash
        )}'::bytea, '${hexToByteaString(input.unlockingBytecode)}'::bytea)`
    )
    .join(',')}
), new_transaction (transaction_hash, transaction_internal_id) AS (
  INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
    SELECT hash, version, locktime, size_bytes, is_coinbase FROM transaction_values
    ON CONFLICT ON CONSTRAINT "transaction_hash_key" DO NOTHING
    RETURNING hash AS transaction_hash, internal_id AS transaction_internal_id
), insert_outputs AS (
  INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment${
    outputMarker.column
  })
    SELECT transaction_hash, output_index, value_satoshis, locking_bytecode, token_category::bytea, fungible_token_amount::bigint, nonfungible_token_capability::enum_nonfungible_token_capability, nonfungible_token_commitment::bytea${
      outputMarker.value
    } FROM output_values CROSS JOIN new_transaction
)${setInsertCteFor(
    policy,
    '(SELECT transaction_hash, output_index, token_category, locking_bytecode FROM output_values CROSS JOIN new_transaction) AS new_outputs',
    '(SELECT transaction_hash AS hash FROM new_transaction) AS new_hashes'
  )}, insert_inputs AS (
  INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
    SELECT transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode FROM input_values CROSS JOIN new_transaction
)
SELECT COUNT(*) FROM new_transaction;
`;
  const saveNodeValidations = /* sql */ `
WITH node_transaction_values (node_internal_id, validated_at) AS (
  VALUES ${nodeValidations
    .map(
      (validation) =>
        `(${
          validation.nodeInternalId
        }::bigint, ${dateToTimestampWithoutTimezone(validation.validatedAt)})`
    )
    .join(',')}
)
INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
  SELECT node_internal_id, $1::bigint, validated_at FROM node_transaction_values
  ON CONFLICT ON CONSTRAINT "node_transaction_pkey" DO NOTHING;
`;
  // eslint-disable-next-line functional/no-try-statement
  try {
    await client.query('BEGIN;');
    const savedTransactionResult = await client.query<{ count: string }>(
      saveTransaction
    );
    const transactionInternalIdResult = await client.query<{
      internalId: string;
    }>(
      /* sql */ `SELECT internal_id AS "internalId" FROM transaction WHERE hash = $1;`,
      [Buffer.from(transaction.hash, 'hex')]
    );
    const transactionInternalId =
      transactionInternalIdResult.rows[0]?.internalId;
    if (transactionInternalId === undefined) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `Failed to save or find transaction while recording node validation: ${transaction.hash}`
      );
    }
    const validatingNodeIds = nodeValidations.map(
      (validation) => validation.nodeInternalId
    );
    await bitmaskAccept(client, policy, validatingNodeIds, [transaction.hash]);
    await client.query(saveNodeValidations, [transactionInternalId]);
    if (
      unspentDeferredKind !== undefined &&
      Number(savedTransactionResult.rows[0]?.count ?? 0) === 0
    ) {
      /*
       * E17: the transaction already existed (its id may be below the job's
       * watermark): record the acceptance as an event.
       */
      await client.query(transactionReacceptedEventsSql, [
        transactionInternalId,
        validatingNodeIds,
      ]);
    }
    await reacceptSpends(client, [transactionInternalId]);
    const mempoolResolveSql = resolveMempoolOutputsSql(unspentTracking);
    if (policy === 'resolve' && mempoolResolveSql !== undefined) {
      await client.query(mempoolResolveSql, [
        Buffer.from(transaction.hash, 'hex'),
      ]);
    }
    await bitmaskResolve(client, policy, validatingNodeIds, [transaction.hash]);
    await client.query('COMMIT;');
  } catch (err) {
    await client.query('ROLLBACK;');
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  }
  // eslint-disable-next-line functional/no-try-statement
  try {
    return reportPostCommit(
      `mempool ${transaction.hash}`,
      await runPostCommit(client, policy, {
        hashes: [transaction.hash],
        kind: 'transactions',
      })
    );
  } finally {
    client.release();
  }
};

/**
 * Immediately mark a node as having validated a transaction already known to
 * exist in the database.
 */
export const recordNodeValidation = async (
  transactionHash: string,
  validation: {
    nodeInternalId: number;
    validatedAt: Date;
  }
) => {
  const client = await pool.connect();
  /*
   * The transaction is already saved, just insert `node_transaction`s.
   */
  // eslint-disable-next-line functional/no-try-statement
  try {
    const policy = await newOutputPolicy(client);
    await client.query('BEGIN;');
    await bitmaskAccept(
      client,
      policy,
      [validation.nodeInternalId],
      [transactionHash]
    );
    await client.query(/* sql */ `
    WITH node_transaction_values (node_internal_id, validated_at) AS (
      VALUES (
      ${validation.nodeInternalId}::bigint,
      ${dateToTimestampWithoutTimezone(validation.validatedAt)}
      )
    ), known_transaction (transaction_internal_id) AS (
      SELECT internal_id
        FROM transaction
        WHERE hash = '${hexToByteaString(transactionHash)}'::bytea
    )
    INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
      SELECT node_internal_id, transaction_internal_id, validated_at
        FROM node_transaction_values
        CROSS JOIN known_transaction
      ON CONFLICT ON CONSTRAINT "node_transaction_pkey" DO NOTHING;
  `);
    if (unspentTracking !== 'off' || unspentDeferredKind !== undefined) {
      const known = await client.query<{ internalId: string }>(
        /* sql */ `SELECT internal_id AS "internalId" FROM transaction WHERE hash = $1;`,
        [Buffer.from(transactionHash, 'hex')]
      );
      await reacceptSpends(
        client,
        known.rows.map((row) => row.internalId)
      );
      if (unspentDeferredKind !== undefined && known.rows[0] !== undefined) {
        // E17: a known transaction gains an acceptance
        await client.query(transactionReacceptedEventsSql, [
          known.rows[0].internalId,
          [validation.nodeInternalId],
        ]);
      }
    }
    await bitmaskResolve(
      client,
      policy,
      [validation.nodeInternalId],
      [transactionHash]
    );
    await client.query('COMMIT;');
    return reportPostCommit(
      `validation ${transactionHash}`,
      await runPostCommit(client, policy, {
        hashes: [transactionHash],
        kind: 'transactions',
      })
    );
  } catch (err) {
    await client.query('ROLLBACK;');
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  } finally {
    client.release();
  }
};

/**
 * Inside a block's DB transaction: throw (so the caller rolls back) unless
 * every transaction of the block was found and linked via block_transaction.
 */
const verifyBlockTransactionsLinked = async (
  client: pg.PoolClient,
  block: ChaingraphBlock,
  joinedTransactionCount: number
) => {
  const linkedBlockTransactionCount = Number(
    (
      await client.query<{ count: string }>(
        /* sql */ `
          SELECT COUNT(*)::bigint AS count
            FROM block_transaction
            INNER JOIN block ON block.internal_id = block_transaction.block_internal_id
            WHERE block.hash = $1;
        `,
        [Buffer.from(block.hash, 'hex')]
      )
    ).rows[0]!.count
  );
  if (
    joinedTransactionCount !== block.transactions.length ||
    linkedBlockTransactionCount !== block.transactions.length
  ) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `Failed to save all transactions for block ${block.height} (${block.hash}): joined ${joinedTransactionCount}/${block.transactions.length}, linked ${linkedBlockTransactionCount}/${block.transactions.length}.`
    );
  }
};

/**
 * E17 deferred modes: a block save that added node_block rows to a block that
 * already existed (its id may be below the job's block watermark) records the
 * acceptance as events. New blocks need none: the job scans block ids above
 * its watermark.
 */

const recordBlockReacceptance = async (
  client: pg.PoolClient,
  block: ChaingraphBlock,
  acceptingNodeIds: number[],
  insertedNodeBlockCount: number
) => {
  if (unspentDeferredKind === undefined || insertedNodeBlockCount === 0) {
    return;
  }
  await client.query(blockReacceptedEventsSql, [
    Buffer.from(block.hash, 'hex'),
    acceptingNodeIds,
  ]);
};

/**
 * `CHAINGRAPH_UNSPENT_TRACKING`: the per-block spend statement of the `sql`
 * write path (`undefined` in `off` mode or for a coinbase-only block).
 */
const blockSpendTrackingSql = (block: ChaingraphBlock) =>
  unspentTracking === 'marker'
    ? buildMarkSpentOutputsSql(collectBlockSpends(block))
    : unspentTracking === 'settable'
    ? buildDeleteSpentFromSetSql(collectBlockSpends(block))
    : undefined;

/**
 * `CHAINGRAPH_UNSPENT_TRACKING`: the per-block spend statement of the `copy`
 * write path (reads `pg_temp.chaingraph_stage_spend`).
 */
const stagedSpendTrackingSql =
  unspentTracking === 'marker'
    ? markStagedSpentOutputsSql
    : unspentTracking === 'settable'
    ? deleteStagedSpentFromSetSql
    : undefined;

const stagedSpendCopies = (stagedSpends: Buffer | undefined) =>
  stagedSpends === undefined
    ? []
    : [
        {
          messages: stagedSpends,
          statement: copyStageTableSql('chaingraph_stage_spend'),
        },
      ];

const encodeStagedSpendsIfTracked = (block: ChaingraphBlock) =>
  unspentTracking === 'off' || unspentTracking === 'bitmask'
    ? undefined
    : encodeStageSpends(collectBlockSpends(block));

/**
 * Pooled connections on which the `copy` write path's staging tables exist.
 * (A replaced connection is a new client object, so it is set up again.)
 */
const clientsWithStageTables = new WeakSet<pg.PoolClient>();

/**
 * The `copy` write path's statement for transactions, outputs and inputs: the
 * same CTE as `addAllTransactions` in `saveBlock`, reading the staged rows
 * instead of `VALUES` lists. The text never changes, so Postgres parses it
 * cheaply; it is deliberately not a named prepared statement, so each block
 * is planned with the current size of the staging tables.
 */
const buildAddAllStagedTransactions = (
  policy: NewOutputPolicy,
  acceptingNodeIds: number[] = []
) => {
  const outputMarker = outputMarkerFor(policy, acceptingNodeIds);
  return /* sql */ `
WITH newly_saved_transactions (hash, internal_id) AS (
  INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
    SELECT hash, version, locktime, size_bytes, is_coinbase FROM pg_temp.chaingraph_stage_transaction
    ON CONFLICT ON CONSTRAINT "transaction_hash_key" DO NOTHING
    RETURNING hash, internal_id
),
newly_saved_outputs AS (
  INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment${
    outputMarker.column
  })
    SELECT transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment${
      outputMarker.value
    } FROM pg_temp.chaingraph_stage_output
    WHERE transaction_hash IN (SELECT hash FROM newly_saved_transactions)
)${setInsertCteFor(
    policy,
    'pg_temp.chaingraph_stage_output',
    'newly_saved_transactions'
  )},
newly_saved_inputs AS (
  INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
    SELECT internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode
    FROM pg_temp.chaingraph_stage_input val INNER JOIN newly_saved_transactions txs ON val.transaction_hash = txs.hash
)
SELECT COUNT(*) FROM newly_saved_transactions;`;
};
const addAllStagedTransactions = Object.fromEntries(
  (['none', 'resolve', 'trust', 'unaudited'] as const).map((policy) => [
    policy,
    buildAddAllStagedTransactions(policy),
  ])
) as { [policy in NewOutputPolicy]: string };

/**
 * The `copy` write path's block statement: the same CTE as `addBlockQuery` in
 * `saveBlock`, with the block's transaction list read from the staging table
 * and every other value passed as a parameter (so the text is fixed).
 *
 * `joined_transactions` is an index lookup per staged transaction (the
 * `OFFSET 0` keeps the lateral subquery from being flattened into a join).
 * Temporary tables have no statistics, and Postgres assumes at least 10 pages
 * for a never-vacuumed table, so a small block's list is estimated at ~1,200
 * rows; with a free choice the planner can pick a hash join over a full scan
 * of `transaction`.
 */
const addStagedBlock = /* sql */ `
WITH accepting_nodes (node_internal_id, accepted_at) AS (
  SELECT * FROM unnest($10::bigint[], $11::timestamp[])
),
joined_transactions (internal_id, transaction_index) AS (
  SELECT db.internal_id, val.transaction_index
    FROM pg_temp.chaingraph_stage_block_transaction val
    CROSS JOIN LATERAL (
      SELECT internal_id FROM transaction WHERE transaction.hash = val.hash OFFSET 0
    ) db
),
inserted_block (internal_id) AS (
  INSERT INTO block (height, version, timestamp, hash, previous_block_hash, merkle_root, bits, nonce, size_bytes)
    VALUES ($1::bigint, $2::bigint, $3::bigint, $4::bytea, $5::bytea, $6::bytea, $7::bigint, $8::bigint, $9::bigint)
  ON CONFLICT ON CONSTRAINT "block_hash_key" DO NOTHING
  RETURNING internal_id
),
new_or_existing_block (internal_id) AS (
  SELECT COALESCE (
    (SELECT internal_id FROM inserted_block),
    (SELECT internal_id FROM block WHERE block.hash = $4::bytea)
  )
),
inserted_block_transactions AS (
  INSERT INTO block_transaction (block_internal_id, transaction_internal_id, transaction_index)
    SELECT blk.internal_id, tx.internal_id, tx.transaction_index
      FROM new_or_existing_block blk CROSS JOIN joined_transactions tx
    ON CONFLICT ON CONSTRAINT "block_transaction_pkey" DO NOTHING
    RETURNING transaction_internal_id
),
inserted_node_blocks AS (
  INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at)
  SELECT node.node_internal_id, blk.internal_id, node.accepted_at
    FROM new_or_existing_block blk CROSS JOIN accepting_nodes node
  ON CONFLICT ON CONSTRAINT "node_block_pkey" DO NOTHING
  RETURNING block_internal_id
)
SELECT
  (SELECT COUNT(*)::bigint FROM joined_transactions) AS "joinedTransactionCount",
  (SELECT COUNT(*)::bigint FROM inserted_block_transactions) AS "insertedBlockTransactionCount",
  (SELECT COUNT(*)::bigint FROM inserted_node_blocks) AS "insertedNodeBlockCount";`;

/**
 * The `copy` write path (`CHAINGRAPH_WRITE_PATH=copy`) of `saveBlock`: one DB
 * transaction per block, as in the `sql` path. Rows are streamed with binary
 * `COPY` into per-connection temporary staging tables, then inserted with the
 * same statements, constraints and `ON CONFLICT DO NOTHING` handling.
 */
const saveBlockViaCopy = async ({
  block,
  nodeAcceptances,
  unknownTransactions,
}: {
  block: ChaingraphBlock;
  nodeAcceptances: {
    nodeInternalId: number;
    acceptedAt: Date | null;
  }[];
  unknownTransactions: ChaingraphTransaction[];
}) => {
  const stagedTransactions = encodeStageTransactions(unknownTransactions);
  const stagedOutputs = encodeStageOutputs(unknownTransactions);
  const stagedInputs = encodeStageInputs(unknownTransactions);
  const stagedBlockTransactions = encodeStageBlockTransactions(block);
  const stagedSpends = encodeStagedSpendsIfTracked(block);
  const blockParameters = [
    block.height,
    block.version,
    block.timestamp,
    Buffer.from(block.hash, 'hex'),
    Buffer.from(block.previousBlockHash, 'hex'),
    Buffer.from(block.merkleRoot, 'hex'),
    block.bits,
    block.nonce,
    block.sizeBytes,
    nodeAcceptances.map((acceptance) => acceptance.nodeInternalId),
    nodeAcceptances.map((acceptance) =>
      acceptance.acceptedAt === null
        ? null
        : acceptance.acceptedAt.toISOString()
    ),
  ];
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    if (!clientsWithStageTables.has(client)) {
      /*
       * Outside of the block's DB transaction, so a rolled-back block can't
       * also roll back the table creation.
       */
      await client.query(createStageTablesSql);
      clientsWithStageTables.add(client);
    }
    const policy = await newOutputPolicy(client);
    await client.query('BEGIN;');
    await copyFromBuffers(client, [
      {
        messages: stagedTransactions,
        statement: copyStageTableSql('chaingraph_stage_transaction'),
      },
      {
        messages: stagedOutputs,
        statement: copyStageTableSql('chaingraph_stage_output'),
      },
      {
        messages: stagedInputs,
        statement: copyStageTableSql('chaingraph_stage_input'),
      },
      {
        messages: stagedBlockTransactions,
        statement: copyStageTableSql('chaingraph_stage_block_transaction'),
      },
      ...stagedSpendCopies(stagedSpends),
    ]);
    const saveTransactionsResult = await client.query<{ count: string }>(
      unspentTracking === 'bitmask'
        ? buildAddAllStagedTransactions(
            policy,
            nodeAcceptances.map((acceptance) => acceptance.nodeInternalId)
          )
        : addAllStagedTransactions[policy]
    );
    const savedTransactionCount = Number(saveTransactionsResult.rows[0]!.count);
    const transactionCacheMisses =
      unknownTransactions.length - savedTransactionCount;
    const acceptingNodeIds = nodeAcceptances.map(
      (acceptance) => acceptance.nodeInternalId
    );
    /*
     * bitmask: only transactions that existed before this save need the
     * per-node acceptance path; new outputs were inserted with the bits.
     */
    const bitmaskAcceptMs = await bitmaskAccept(
      client,
      policy,
      acceptingNodeIds,
      knownTransactionHashes(block, unknownTransactions, transactionCacheMisses)
    );
    const addBlockResult = await client.query<{
      insertedBlockTransactionCount: string;
      insertedNodeBlockCount: string;
      joinedTransactionCount: string;
    }>(addStagedBlock, blockParameters);
    await verifyBlockTransactionsLinked(
      client,
      block,
      Number(addBlockResult.rows[0]!.joinedTransactionCount)
    );
    await recordBlockReacceptance(
      client,
      block,
      acceptingNodeIds,
      Number(addBlockResult.rows[0]!.insertedNodeBlockCount)
    );
    const query = async (sql: string) => client.query(sql);
    const bitmaskSteps = await bitmaskBlockSteps(client, policy, {
      acceptingNodeIds,
      block,
      newTransactionHashes: unknownTransactions.map(
        (transaction) => transaction.hash
      ),
    });
    const markMs =
      bitmaskAcceptMs +
      bitmaskSteps.clearMs +
      (await timed(stagedSpendTrackingSql, query));
    const resolveMs =
      (await timed(
        policy === 'resolve'
          ? resolveStagedNewOutputsSql(unspentTracking)
          : undefined,
        query
      )) + bitmaskSteps.resolveMs;
    await client.query('COMMIT;');
    const postCommit = await runPostCommit(client, policy, {
      hashes: [block.hash],
      kind: 'blocks',
    });
    return {
      attemptedSavedTransactions: unknownTransactions,
      transactionCacheMisses,
      unspentTrackingTimings: { markMs, policy, postCommit, resolveMs },
    };
  } catch (err) {
    await client.query('ROLLBACK;');
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  } finally {
    client.release();
  }
};

/**
 * Save a block to the database, inserting all transactions which aren't already
 * known to exist in the database. (This method should only be used for blocks
 * which are not already saved to the database.)
 *
 * Note: this method trusts its input, and data is not sanitized. (Because all
 * inserted data is of type `number`, `boolean`, or `Uint8Array`, we assume SQL
 * injections are not a concern.)
 */
export const saveBlock = async ({
  block,
  nodeAcceptances,
  transactionCache,
}: {
  block: ChaingraphBlock;
  nodeAcceptances: {
    nodeInternalId: number;
    acceptedAt: Date | null;
    nodeName: string;
  }[];
  transactionCache: Agent['transactionCache'];
}) => {
  const blockTransactions = block.transactions.reduce<{
    /**
     * Transactions known to be successfully saved to the database.
     */
    alreadySaved: ChaingraphTransaction[];
    /**
     * Transactions in the block which aren't yet known to be saved to the
     * database. These must be saved before the block can be saved.
     */
    unknown: ChaingraphTransaction[];
  }>(
    (transactions, transaction) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-expressions
      transactionCache.get(transaction.hash)?.db === true
        ? transactions.alreadySaved.push(transaction)
        : transactions.unknown.push(transaction);
      return transactions;
    },
    { alreadySaved: [], unknown: [] }
  );

  if (chaingraphWritePath === 'copy') {
    return saveBlockViaCopy({
      block,
      nodeAcceptances,
      unknownTransactions: blockTransactions.unknown,
    });
  }
  const client = await pool.connect();
  const policy = await newOutputPolicy(client).catch(async (err: unknown) => {
    client.release();
    // eslint-disable-next-line functional/no-promise-reject
    return Promise.reject(err);
  });
  const outputMarker = outputMarkerFor(
    policy,
    nodeAcceptances.map((acceptance) => acceptance.nodeInternalId)
  );

  const inputs: {
    inputIndex: number;
    transactionHash: string;
    content: ChaingraphTransaction['inputs'][number];
  }[] = [];
  const outputs: {
    outputIndex: number;
    transactionHash: string;
    content: ChaingraphTransaction['outputs'][number];
  }[] = [];

  blockTransactions.unknown.forEach((transaction) => {
    inputs.push(
      ...transaction.inputs.map((content, inputIndex) => ({
        content,
        inputIndex,
        transactionHash: transaction.hash,
      }))
    );
    outputs.push(
      ...transaction.outputs.map((content, outputIndex) => ({
        content,
        outputIndex,
        transactionHash: transaction.hash,
      }))
    );
  });

  const addAllTransactions = /* sql */ `
WITH unknown_transaction_values (hash, version, locktime, size_bytes, is_coinbase) AS (
  VALUES ${blockTransactions.unknown
    .map(
      (transaction) =>
        `('${hexToByteaString(transaction.hash)}'::bytea, ${
          transaction.version
        }::bigint, ${transaction.locktime}::bigint, ${
          transaction.sizeBytes
        }::bigint, ${transaction.isCoinbase.toString()}::boolean)`
    )
    .join(',')}
),
unknown_input_values (transaction_hash, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode) AS (
  VALUES ${inputs
    .map(
      (input) =>
        `('${hexToByteaString(input.transactionHash)}'::bytea, ${
          input.inputIndex
        }::bigint, ${input.content.outpointIndex}::bigint, ${
          input.content.sequenceNumber
        }::bigint, '${hexToByteaString(
          input.content.outpointTransactionHash
        )}'::bytea, '${hexToByteaString(
          input.content.unlockingBytecode
        )}'::bytea)`
    )
    .join(',')}
),
unknown_output_values (transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment) AS (
  VALUES ${outputs
    .map(
      (output) =>
        `('${hexToByteaString(output.transactionHash)}'::bytea, ${
          output.outputIndex
        }::bigint, ${output.content.valueSatoshis.toString()}::bigint, '${hexToByteaString(
          output.content.lockingBytecode
        )}'::bytea, ${
          output.content.tokenCategory === undefined
            ? 'NULL::bytea'
            : `'${hexToByteaString(output.content.tokenCategory)}'::bytea`
        }, ${
          output.content.fungibleTokenAmount === undefined
            ? 'NULL::bigint'
            : `${output.content.fungibleTokenAmount.toString()}::bigint`
        }, ${
          output.content.nonfungibleTokenCapability === undefined
            ? 'NULL::enum_nonfungible_token_capability'
            : `'${output.content.nonfungibleTokenCapability}'::enum_nonfungible_token_capability`
        }, ${
          output.content.nonfungibleTokenCommitment === undefined
            ? 'NULL::bytea'
            : `'${hexToByteaString(
                output.content.nonfungibleTokenCommitment
              )}'::bytea`
        })`
    )
    .join(',')}
),
newly_saved_transactions (hash, internal_id) AS (
  INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
    SELECT hash, version, locktime, size_bytes, is_coinbase FROM unknown_transaction_values
    ON CONFLICT ON CONSTRAINT "transaction_hash_key" DO NOTHING
    RETURNING hash, internal_id
),
newly_saved_outputs AS (
  INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment${
    outputMarker.column
  })
    SELECT transaction_hash, output_index, value_satoshis, locking_bytecode, token_category::bytea, fungible_token_amount::bigint, nonfungible_token_capability::enum_nonfungible_token_capability, nonfungible_token_commitment::bytea${
      outputMarker.value
    } FROM unknown_output_values
    WHERE transaction_hash IN (SELECT hash FROM newly_saved_transactions)
)${setInsertCteFor(
    policy,
    'unknown_output_values',
    'newly_saved_transactions'
  )},
newly_saved_inputs AS (
  INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
    SELECT internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode
    FROM unknown_input_values val INNER JOIN newly_saved_transactions txs ON val.transaction_hash = txs.hash
)
SELECT COUNT(*) FROM newly_saved_transactions;`;

  /**
   * TODO: perf – consider baking this into `addAllTransactions` to avoid re-sending the list of transaction hashes?
   * TODO: perf – consider batching blocks during initial sync (targeting 100KB to 1MB queries)
   * TODO: perf – use prepared statements
   */
  const addBlockQuery = /* sql */ `
WITH transactions_in_block (hash, transaction_index) AS (
  VALUES ${block.transactions
    .map(
      (transaction, index) =>
        `('${hexToByteaString(transaction.hash)}'::bytea, ${index}::bigint)`
    )
    .join(',')}
),
accepting_nodes (node_internal_id, accepted_at) AS (
  VALUES ${nodeAcceptances
    .map(
      (acceptance) =>
        `(${acceptance.nodeInternalId}, ${
          acceptance.acceptedAt === null
            ? 'NULL::timestamp'
            : dateToTimestampWithoutTimezone(acceptance.acceptedAt)
        })`
    )
    .join(',')}
),
joined_transactions (internal_id, transaction_index) AS (
  SELECT db.internal_id, val.transaction_index
    FROM transaction db INNER JOIN transactions_in_block val ON val.hash = db.hash
),
inserted_block (internal_id) AS (
  INSERT INTO block (height, version, timestamp, hash, previous_block_hash, merkle_root, bits, nonce, size_bytes)
    VALUES (${block.height}, ${block.version}, ${block.timestamp},
      '${hexToByteaString(block.hash)}'::bytea,
      '${hexToByteaString(block.previousBlockHash)}'::bytea,
      '${hexToByteaString(block.merkleRoot)}'::bytea,
      ${block.bits}::bigint, ${block.nonce}::bigint, ${block.sizeBytes}::bigint)
  ON CONFLICT ON CONSTRAINT "block_hash_key" DO NOTHING
  RETURNING internal_id
),
new_or_existing_block (internal_id) AS (
  SELECT COALESCE (
    (SELECT internal_id FROM inserted_block),
    (SELECT internal_id FROM block WHERE block.hash = '${hexToByteaString(
      block.hash
    )}'::bytea)
  )
),
inserted_block_transactions AS (
  INSERT INTO block_transaction (block_internal_id, transaction_internal_id, transaction_index)
    SELECT blk.internal_id, tx.internal_id, tx.transaction_index
      FROM new_or_existing_block blk CROSS JOIN joined_transactions tx
    ON CONFLICT ON CONSTRAINT "block_transaction_pkey" DO NOTHING
    RETURNING transaction_internal_id
),
inserted_node_blocks AS (
  INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at)
  SELECT node.node_internal_id, blk.internal_id, node.accepted_at
    FROM new_or_existing_block blk CROSS JOIN accepting_nodes node
  ON CONFLICT ON CONSTRAINT "node_block_pkey" DO NOTHING
  RETURNING block_internal_id
)
SELECT
  (SELECT COUNT(*)::bigint FROM joined_transactions) AS "joinedTransactionCount",
  (SELECT COUNT(*)::bigint FROM inserted_block_transactions) AS "insertedBlockTransactionCount",
  (SELECT COUNT(*)::bigint FROM inserted_node_blocks) AS "insertedNodeBlockCount";`;
  // eslint-disable-next-line functional/no-try-statement
  try {
    await client.query('BEGIN;');
    const saveTransactionsResult = await client.query<{ count: string }>(
      addAllTransactions
    );
    const attemptedSavedTransactions = blockTransactions.unknown;
    const savedTransactionCount = Number(saveTransactionsResult.rows[0]!.count);
    const transactionCacheMisses =
      attemptedSavedTransactions.length - savedTransactionCount;
    const acceptingNodeIds = nodeAcceptances.map(
      (acceptance) => acceptance.nodeInternalId
    );
    const bitmaskAcceptMs = await bitmaskAccept(
      client,
      policy,
      acceptingNodeIds,
      knownTransactionHashes(
        block,
        attemptedSavedTransactions,
        transactionCacheMisses
      )
    );
    const addBlockResult = await client.query<{
      insertedBlockTransactionCount: string;
      insertedNodeBlockCount: string;
      joinedTransactionCount: string;
    }>(addBlockQuery);
    await verifyBlockTransactionsLinked(
      client,
      block,
      Number(addBlockResult.rows[0]!.joinedTransactionCount)
    );
    await recordBlockReacceptance(
      client,
      block,
      acceptingNodeIds,
      Number(addBlockResult.rows[0]!.insertedNodeBlockCount)
    );
    const query = async (sql: string) => client.query(sql);
    const newHashes = attemptedSavedTransactions.map(
      (transaction) => transaction.hash
    );
    const bitmaskSteps = await bitmaskBlockSteps(client, policy, {
      acceptingNodeIds,
      block,
      newTransactionHashes: newHashes,
    });
    const markMs =
      bitmaskAcceptMs +
      bitmaskSteps.clearMs +
      (await timed(blockSpendTrackingSql(block), query));
    const resolveMs =
      (await timed(
        policy === 'resolve'
          ? buildResolveNewOutputsSql(unspentTracking, newHashes)
          : undefined,
        query
      )) + bitmaskSteps.resolveMs;
    await client.query('COMMIT;');
    const postCommit = await runPostCommit(client, policy, {
      hashes: [block.hash],
      kind: 'blocks',
    });
    return {
      attemptedSavedTransactions,
      transactionCacheMisses,
      unspentTrackingTimings: { markMs, policy, postCommit, resolveMs },
    };
  } catch (err) {
    await client.query('ROLLBACK;');
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  } finally {
    client.release();
  }
};

/**
 * Used when a node catches up to one or more other nodes via headers-sync.
 *
 * Returns the number of node_blocks inserted.
 */
/* eslint-disable complexity */
export const acceptBlocksViaHeaders = async (
  nodeInternalId: number,
  acceptedBlocks: {
    height: number;
    hash: string;
  }[],
  acceptedAt: Date
) => {
  const secondsPerMs = 1_000;
  const acceptedAtTimestamp = acceptedAt.getTime() / secondsPerMs;

  const twoHoursSeconds = 7200;
  /**
   * Chaingraph does not save "acceptedAt" times for blocks older than 2 hours.
   * See `agent.saveBlock` for details.
   */
  const nullifyAcceptedTimeBeforeBlockTimestamp = Math.round(
    acceptedAtTimestamp - twoHoursSeconds
  );

  const insertNodeBlocks = /* sql */ `
  WITH matching_blocks (internal_id, use_null) AS (
    SELECT internal_id, (timestamp < ${nullifyAcceptedTimeBeforeBlockTimestamp}::bigint) AS use_null
    FROM block WHERE hash IN (VALUES ${acceptedBlocks
      .map((block) => `('${hexToByteaString(block.hash)}'::bytea)`)
      .join(',')})
  )
    INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at)
      SELECT n.id, blk.internal_id, CASE WHEN blk.use_null=true THEN NULL ELSE ${dateToTimestampWithoutTimezone(
        acceptedAt
      )} END
      FROM matching_blocks blk CROSS JOIN (VALUES (${nodeInternalId}::bigint)) n(id)
      ON CONFLICT DO NOTHING
      RETURNING block_internal_id AS "blockInternalId"
  `;
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    if (unspentTracking === 'off') {
      const nodeBlockInsertResult = await client.query(
        unspentDeferredKind === undefined
          ? insertNodeBlocks
          : headersAcceptedEventsSql(nodeInternalId, insertNodeBlocks)
      );
      return nodeBlockInsertResult.rowCount;
    }
    /*
     * A block that gains its first accepting node may have had its spends
     * released (stale for every node, then accepted again): re-mark them.
     */
    await client.query('BEGIN;');
    const policy = await newOutputPolicy(client);
    const nodeBlockInsertResult = await client.query<{
      blockInternalId: string;
    }>(insertNodeBlocks);
    const insertedBlockIds = nodeBlockInsertResult.rows.map(
      (row) => row.blockInternalId
    );
    if (
      unspentTracking === 'bitmask' &&
      policy !== 'unaudited' &&
      insertedBlockIds.length > 0
    ) {
      await client.query(bitmaskAcceptBlocksSql, [
        nodeInternalId,
        insertedBlockIds,
      ]);
      if (policy === 'resolve') {
        await client.query(bitmaskResolveBlocksSql, [
          nodeInternalId,
          insertedBlockIds,
        ]);
      }
    }
    const reaccepted = await client.query<{ ids: string[] | null }>(
      firstAcceptedBlockTransactionsSql,
      [
        nodeBlockInsertResult.rows.map((row) => row.blockInternalId),
        nodeInternalId,
      ]
    );
    await reacceptSpends(client, reaccepted.rows[0]?.ids ?? [], true);
    await client.query('COMMIT;');
    if (insertedBlockIds.length > 0) {
      reportPostCommit(
        `headers, ${insertedBlockIds.length} block(s)`,
        await runPostCommit(client, policy, {
          hashes: acceptedBlocks.map((acceptedBlock) => acceptedBlock.hash),
          kind: 'blocks',
        })
      );
    }
    return nodeBlockInsertResult.rowCount;
  } catch (err) {
    if (unspentTracking !== 'off') {
      await client.query('ROLLBACK;');
    }
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  } finally {
    client.release();
  }
};
/* eslint-enable complexity */

/**
 * Remove a list of stale blocks for the specified node. This is called during
 * re-organizations before the newly-accepted history is synced to the database.
 *
 * This method does not re-introduce transactions from the stale blocks to the
 * node's mempool (`node_transaction`), as most most real world re-organizations
 * do not ultimately cause many confirmed transactions to become unconfirmed.
 * (Rather, the new blocks will typically include the removed transactions and
 * more.)
 *
 * For use cases which require carefully handling these transactions, downstream
 * applications should subscribe to changes in the `node_block_history` table.
 */
export const removeStaleBlocksForNode = async (
  nodeInternalId: number,
  staleChain: string[]
) => {
  const client = await pool.connect();
  await client.query(/* sql */ `
DELETE FROM node_block WHERE
  node_internal_id IN (VALUES (${nodeInternalId}::bigint)) AND
  block_internal_id IN (SELECT internal_id from block WHERE hash IN (VALUES ${staleChain
    .map((hash) => `('${hexToByteaString(hash)}'::bytea)`)
    .join(',')}))
`);
  client.release();
  detachPostCommitRelease({ blockHashes: staleChain, kind: 'blocks' });
};

/**
 * After initial sync, Chaingraph begins tracking each node's mempool.
 *
 * To maintain consistency, triggers which are disabled before initial sync must
 * be reenabled to clear any confirmed or conflicting transactions when a block
 * is accepted.
 */
export const reenableMempoolCleaning = async () => {
  const client = await pool.connect();
  await client.query(
    `ALTER TABLE node_block ENABLE TRIGGER trigger_public_node_block_insert;`
  );
  const triggerExists =
    // cspell:ignore tgrelid tgname
    (
      await client.query<{ triggerExists: boolean }>(/* sql */ `
SELECT EXISTS (
  SELECT 1 FROM pg_trigger
    WHERE tgrelid = 'node_transaction_history'::regclass
      AND tgname = 'trigger_public_node_transaction_history_insert'
) AS "triggerExists";
`)
    ).rows[0]?.triggerExists === true;
  if (triggerExists) {
    await client.query(
      `ALTER TABLE node_transaction_history ENABLE TRIGGER trigger_public_node_transaction_history_insert;`
    );
  }
  client.release();
  return triggerExists;
};

/**
 * If configured, disable `synchronous_commit` for the database. (Returns false
 * if synchronous_commit is not disabled.)
 *
 * Chaingraph can disable `synchronous_commit` in an effort to improve initial
 * sync performance. This would normally risk data loss (but not corruption) in
 * the event of a database crash, but because Chaingraph can simply re-request
 * blocks from the trusted nodes, synchronous commits aren't valuable during
 * initial sync.
 *
 * Note: in real-world testing, this usually reduces the speed of Chaingraph's
 * initial sync, so Chaingraph leaves "synchronous_commit = on" by default.
 */
export const optionallyDisableSynchronousCommit = async () => {
  if (postgresSynchronousCommit) {
    return false;
  }
  const client = await pool.connect();
  await client.query(
    `DO $$ BEGIN execute 'ALTER DATABASE ' || current_database() || ' SET synchronous_commit TO OFF'; END $$;`
  );
  client.release();
  return true;
};

/**
 * Re-enable `synchronous_commit` for the database. (Returns false if
 * synchronous_commit was not disabled.)
 *
 * See `disableSynchronousCommit` for details.
 */
export const optionallyEnableSynchronousCommit = async () => {
  if (postgresSynchronousCommit) {
    return false;
  }
  const client = await pool.connect();
  await client.query(
    `DO $$ BEGIN execute 'ALTER DATABASE ' || current_database() || ' SET synchronous_commit TO ON'; END $$;`
  );
  client.release();
  return true;
};

/**
 * Fetch a list of all indexes which already exist in this database.
 */
export const listExistingIndexes = async () => {
  const client = await pool.connect();
  const res = await client.query<{
    indexname: string;
  }>(/* sql */ `
SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname;
`);
  client.release();
  return res.rows.map((row) => row.indexname);
};

/**
 * Start building each of the provided indexes. Returns a promise which
 * completes when all indexes have been built.
 */
export const createIndexes = async (
  indexNames: (keyof typeof indexDefinitions)[]
) => {
  const indexCreations = indexNames.map(async (indexName) => {
    const client = await pool.connect();
    const res = await client.query(indexDefinitions[indexName]);
    client.release();
    return res.rowCount;
  });
  const created = await Promise.all(indexCreations);
  /*
   * `CHAINGRAPH_UNSPENT_TRACKING`: POLICY A needs spent_by_index and
   * block_inclusions_index; re-check on the next save.
   */
  resolveIndexesCheckedAt = 0;
  return created;
};

/**
 * Fetch index creation progress from the database, returning a map of index
 * names to completion percentages.
 */
export const getIndexCreationProgress = async () => {
  const client = await pool.connect();
  const res = await client.query<{
    query: string;
    /* eslint-disable @typescript-eslint/naming-convention */
    blocks_done: string;
    blocks_total: string;
    tuples_done: string;
    tuples_total: string;
    /* eslint-enable @typescript-eslint/naming-convention */
  }>(/* sql */ `
SELECT a.query, p.blocks_total, p.blocks_done, p.tuples_total, p.tuples_done
FROM pg_stat_progress_create_index p
JOIN pg_stat_activity a ON p.pid = a.pid;
`);
  client.release();
  return computeIndexCreationProgress(res.rows);
};

/* eslint-disable complexity, max-params, @typescript-eslint/no-magic-numbers, require-atomic-updates, @typescript-eslint/init-declarations, prefer-destructuring */
/*
 * E17 deferred unspent tracking: the recurring job (one instance per agent;
 * `unspent_deferred_run_batch` also takes an advisory lock, so two agents
 * never run it at once).
 */

/**
 * Sequence values whose allocating transactions have all finished (see
 * `readSequencesSql`), and the candidate being settled.
 */
// eslint-disable-next-line functional/no-let
let deferredSettledLimits:
  | { blockLimit: number; transactionLimit: number }
  | undefined;
// eslint-disable-next-line functional/no-let
let deferredCandidate: SettleCandidate | undefined;
// eslint-disable-next-line functional/no-let
let deferredStall: StallState | undefined;
// eslint-disable-next-line functional/no-let
let deferredSkipThrough = 0;
// eslint-disable-next-line functional/no-let
let deferredPassInFlight: Promise<DeferredPassSummary> | undefined;

export interface DeferredPassSummary {
  batches: number;
  busy: boolean;
  caughtUp: boolean;
  changed: number;
  inputs: number;
  kind: UnspentDeferredKind | undefined;
  lastBatch?: DeferredBatchResult;
  ms: number;
}

const settlePollMs = 25;

/**
 * Advance `deferredSettledLimits`: read the sequences (new candidate), wait
 * the grace, take the next xid, then poll until every xid up to it has
 * finished or `waitMs` elapsed (the candidate is kept for the next call).
 */
const settleDeferredLimits = async (client: pg.PoolClient, waitMs: number) => {
  const start = Date.now();
  if (deferredCandidate === undefined) {
    const row = (
      await client.query<{ blockLimit: string; transactionLimit: string }>(
        readSequencesSql
      )
    ).rows[0]!;
    deferredCandidate = {
      blockLimit: Number(row.blockLimit),
      readAt: Date.now(),
      transactionLimit: Number(row.transactionLimit),
    };
  }
  const candidate = deferredCandidate;
  if (candidate.nextXid === undefined) {
    const graceLeft =
      unspentDeferredJob.graceMs - (Date.now() - candidate.readAt);
    if (graceLeft > 0) {
      await sleep(graceLeft);
    }
    candidate.nextXid = (
      await client.query<{ nextXid: string }>(nextTransactionIdSql)
    ).rows[0]!.nextXid;
  }
  const poll = async (): Promise<boolean> => {
    const settled =
      (
        await client.query<{ settled: boolean }>(snapshotSettledSql, [
          candidate.nextXid,
        ])
      ).rows[0]?.settled === true;
    if (settled || Date.now() - start >= waitMs) {
      return settled;
    }
    await sleep(settlePollMs);
    return poll();
  };
  if (await poll()) {
    deferredSettledLimits = {
      blockLimit: candidate.blockLimit,
      transactionLimit: candidate.transactionLimit,
    };
    deferredCandidate = undefined;
    return true;
  }
  return false;
};

/**
 * One batch in its own REPEATABLE READ transaction (one snapshot for every
 * step; the stored values and the watermarks commit together).
 */
const runDeferredBatch = async (
  client: pg.PoolClient,
  kind: UnspentDeferredKind,
  limits: { blockLimit: number; transactionLimit: number },
  checkWatch: boolean
) => {
  const maxBlocks = 200;
  const maxEvents = 20_000;
  deferredSkipThrough = nextSkipThrough(
    deferredSkipThrough,
    deferredStall,
    Date.now(),
    unspentDeferredJob.stallMaxMs,
    limits.transactionLimit
  );
  await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ;');
  // eslint-disable-next-line functional/no-try-statement
  try {
    const raw = (
      await client.query<{ result: { [key: string]: unknown } }>(runBatchSql, [
        kind,
        limits.transactionLimit,
        limits.blockLimit,
        Math.max(unspentDeferredJob.batchInputs, 1),
        maxBlocks,
        maxEvents,
        unspentDeferredJob.sweepRows,
        deferredSkipThrough,
        checkWatch,
      ])
    ).rows[0]!.result;
    await client.query('COMMIT;');
    const result = parseBatchResult(raw);
    deferredStall = nextStallState(deferredStall, result.stalledAt, Date.now());
    return result;
  } catch (err) {
    await client.query('ROLLBACK;');
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  }
};

const ensureDeferredInitialized = async (
  client: pg.PoolClient,
  kind: UnspentDeferredKind,
  limits: { blockLimit: number; transactionLimit: number }
) => {
  const existing = await client.query(progressSql, [kind]);
  if (existing.rows.length > 0) {
    return false;
  }
  const initialized =
    (
      await client.query<{ initialized: boolean }>(initializeSql, [
        kind,
        unspentDeferredJob.startAtGenesis ? 0 : limits.transactionLimit,
        unspentDeferredJob.startAtGenesis ? 0 : limits.blockLimit,
      ])
    ).rows[0]?.initialized === true;
  if (initialized) {
    unspentTrackingInfo(
      `Unspent deferred job (${kind}): tracking started at ${
        unspentDeferredJob.startAtGenesis
          ? 'genesis'
          : `transaction ${limits.transactionLimit} / block ${limits.blockLimit}`
      }; earlier outputs stay unprocessed (NULL) until swept or backfilled.`
    );
  }
  return initialized;
};

/**
 * One pass of the E17 tracking job: settle new limits, then run batches until
 * the settled limits are reached (and nothing else is pending) or `maxMs`
 * elapsed. Concurrent callers share the pass in flight.
 */
export const runUnspentDeferredJobPass = async (
  options: { maxMs?: number; settleWaitMs?: number } = {}
): Promise<DeferredPassSummary> => {
  if (deferredPassInFlight !== undefined) {
    return deferredPassInFlight;
  }
  const kind = unspentDeferredKind;
  const start = Date.now();
  const maxMs = options.maxMs ?? unspentDeferredJob.passMaxMs;
  const pass = async (): Promise<DeferredPassSummary> => {
    const summary: DeferredPassSummary = {
      batches: 0,
      busy: false,
      caughtUp: false,
      changed: 0,
      inputs: 0,
      kind,
      ms: 0,
    };
    if (kind === undefined) {
      return summary;
    }
    const client = await unspentDeferredJobPool.connect();
    // eslint-disable-next-line functional/no-try-statement
    try {
      await settleDeferredLimits(
        client,
        options.settleWaitMs ?? Math.max(unspentDeferredJob.graceMs * 5, 1_000)
      );
      const limits = deferredSettledLimits;
      if (limits === undefined) {
        return summary;
      }
      await ensureDeferredInitialized(client, kind, limits);
      const loop = async (): Promise<void> => {
        const batchStart = Date.now();
        // the watch set is re-checked once per pass (first batch)
        const result = await runDeferredBatch(
          client,
          kind,
          limits,
          summary.batches === 0
        );
        summary.batches += 1;
        summary.lastBatch = result;
        if (result.busy === true || result.uninitialized === true) {
          summary.busy = result.busy === true;
          return;
        }
        summary.inputs += result.inputs;
        summary.changed += result.changed;
        const worked = batchDidWork(result);
        if (worked || result.stalledAt !== null) {
          unspentTrackingInfo(
            formatBatchLog(kind, result, Date.now() - batchStart)
          );
        }
        const reachedLimits =
          result.inputWatermark >= limits.transactionLimit &&
          result.blockWatermark >= limits.blockLimit;
        if (!worked || (reachedLimits && result.events === 0)) {
          summary.caughtUp = reachedLimits;
          return;
        }
        if (Date.now() - start >= maxMs) {
          return;
        }
        await loop();
      };
      await loop();
      return summary;
    } finally {
      summary.ms = Date.now() - start;
      client.release();
    }
  };
  deferredPassInFlight = pass().finally(() => {
    deferredPassInFlight = undefined;
  });
  return deferredPassInFlight;
};

/**
 * Run passes until the job has processed everything committed before this
 * call (tests, measurements): the settled limits reach the sequence values
 * read now and a batch finds nothing left (a stalled input counts as done
 * once its stall persists).
 */
export const drainUnspentDeferredJob = async (timeoutMs = 120_000) => {
  const client = await unspentDeferredJobPool.connect();
  const target = await client
    .query<{ blockLimit: string; transactionLimit: string }>(readSequencesSql)
    .then((result) => result.rows[0]!)
    .finally(() => {
      client.release();
    });
  const start = Date.now();
  const attempt = async (): Promise<DeferredPassSummary> => {
    const summary = await runUnspentDeferredJobPass({
      maxMs: timeoutMs,
      settleWaitMs: 2_000,
    });
    const limits = deferredSettledLimits;
    const done =
      summary.kind === undefined ||
      (limits !== undefined &&
        limits.transactionLimit >= Number(target.transactionLimit) &&
        limits.blockLimit >= Number(target.blockLimit) &&
        summary.lastBatch !== undefined &&
        !batchDidWork(summary.lastBatch) &&
        (summary.lastBatch.inputWatermark >= limits.transactionLimit ||
          summary.lastBatch.stalledAt !== null));
    if (done) {
      return summary;
    }
    if (Date.now() - start > timeoutMs) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `Unspent deferred job did not drain within ${timeoutMs} ms (last batch: ${JSON.stringify(
          summary.lastBatch
        )}).`
      );
    }
    return attempt();
  };
  return attempt();
};

/**
 * The job's watermark and backlog (logs, metrics, tests).
 */
export const getUnspentDeferredStatus = async () => {
  const kind = unspentDeferredKind;
  if (kind === undefined) {
    return undefined;
  }
  const client = await unspentDeferredJobPool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const progress = (
      await client.query<{
        blockWatermark: string;
        consumedEventId: string;
        inputWatermark: string;
      }>(progressSql, [kind])
    ).rows[0];
    const backlog = (
      await client.query<{
        events: string;
        maxTransactionId: string;
        oldestEventAgeSeconds: string;
      }>(backlogSql)
    ).rows[0]!;
    return {
      events: Number(backlog.events),
      inputWatermark:
        progress === undefined ? undefined : Number(progress.inputWatermark),
      kind,
      maxTransactionId: Number(backlog.maxTransactionId),
      oldestEventAgeSeconds: Number(backlog.oldestEventAgeSeconds),
      stalledAt: deferredStall?.transactionInternalId,
    };
  } finally {
    client.release();
  }
};
