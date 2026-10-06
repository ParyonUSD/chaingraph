// cspell:ignore tgenabled relnamespace nspname tgisinternal regprocedure indrelid indisready relname unnest
/* eslint-disable max-lines, functional/no-loop-statement, functional/no-let, no-await-in-loop */
import pg from 'pg';

import type { Agent } from './agent.js';
import {
  boundedValueRows,
  type QueryParameter,
  runMembershipTransaction,
} from './components/db-membership.js';
import { insertTransactions } from './components/db-transaction-writes.js';
import {
  computeIndexCreationProgress,
  indexDefinitions,
} from './components/db-utils.js';
import {
  outputMembershipMode,
  postgresConnectionString,
  postgresMaxConnections,
  postgresSynchronousCommit,
} from './config.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from './types/chaingraph.js';

export const pool = new pg.Pool({
  connectionString: postgresConnectionString,
  max: postgresMaxConnections,
});

const initialAcceptanceIds = (acceptances: { nodeInternalId: number }[]) =>
  outputMembershipMode === 'incremental'
    ? [
        ...new Set(acceptances.map((acceptance) => acceptance.nodeInternalId)),
      ].sort((a, b) => a - b)
    : undefined;

/** Fail closed before the agent connects to nodes; mode changes are operator actions. */
// eslint-disable-next-line complexity
export const validateOutputMembershipMode = async () => {
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const legacy = await client.query<{ count: number; disabled: number }>(`
      SELECT COUNT(*)::integer AS count,
        COUNT(*) FILTER (WHERE t.tgenabled = 'D')::integer AS disabled
      FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND NOT t.tgisinternal AND t.tgname IN
        ('trigger_output_membership_lock','trigger_zz_output_membership_insert',
         'trigger_zz_output_membership_delete','trigger_zz_output_membership_update');`);
    const expectedLegacyTriggers = 22;
    const retired =
      legacy.rows[0]?.count === expectedLegacyTriggers &&
      legacy.rows[0].disabled === expectedLegacyTriggers;
    if (outputMembershipMode === 'baseline') {
      if (legacy.rows[0]?.count !== 0) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(
          'Array membership is installed; explicitly select deferred or incremental mode.'
        );
      }
      // cspell:ignore attrelid attisdropped attname
      const arrayColumns = await client.query<{ present: boolean }>(`
        SELECT EXISTS (SELECT 1 FROM pg_attribute
          WHERE attrelid = 'public.output'::regclass AND NOT attisdropped
            AND attname IN ('accepted_node_ids', 'unspent_node_ids')) AS present;`);
      if (arrayColumns.rows[0]?.present !== false) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(
          'Baseline mode requires a schema without array membership columns.'
        );
      }
      return;
    }
    if (!retired) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        'Opt-in membership modes require all 22 legacy membership triggers disabled.'
      );
    }
    const state = await client.query<{ ready: boolean }>(
      'SELECT ready FROM output_membership.state WHERE id;'
    );
    const ready = state.rows[0]?.ready;
    if (ready !== (outputMembershipMode === 'incremental')) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `Membership mode ${outputMembershipMode} is incompatible with readiness=${String(
          ready
        )}.`
      );
    }
    if (outputMembershipMode === 'incremental') {
      const installed = await client.query<{ complete: boolean }>(`
        SELECT to_regprocedure('output_membership.lock_node(integer)') IS NOT NULL
          AND to_regprocedure('output_membership.begin_membership_changes()') IS NOT NULL
          AND to_regprocedure('output_membership.finish_membership_changes(integer[])') IS NOT NULL
          AND to_regprocedure('output_membership.note_membership_changes(integer,bigint[],jsonb)') IS NOT NULL
          AND current_setting('transaction_isolation') = 'read committed'
          AND (SELECT COUNT(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
            WHERE i.indrelid = 'output'::regclass AND i.indisvalid AND i.indisready
              AND c.relname IN ('output_acceptance_index','unspent_output_index',
                'unspent_output_category_index','unspent_output_search_index')) = 4
          AND (SELECT COUNT(*) FROM pg_trigger WHERE NOT tgisinternal AND tgenabled = 'O'
            AND tgrelid IN ('node_transaction'::regclass,'node_block'::regclass)
            AND tgname IN ('trigger_output_membership_collect_insert',
              'trigger_output_membership_collect_delete','trigger_output_membership_collect_update')) = 6 AS complete;`);
      if (installed.rows[0]?.complete !== true) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(
          'Incremental membership requires installed queue APIs, all six collectors, all four valid array indexes, and READ COMMITTED.'
        );
      }
    }
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
> =>
  runMembershipTransaction(
    pool,
    outputMembershipMode,
    async (client) =>
      (
        await client.query<{ internalId: number }>(
          'SELECT DISTINCT node_internal_id AS "internalId" FROM node_transaction;'
        )
      ).rows.map((row) => row.internalId),
    async (client, nodeIds) => {
      const result = await client.query<{
        hash: string;
        nodeName: string;
        replacedAt: string | null;
      }>(
        /* sql */ `
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
      WHERE node_transaction.node_internal_id = ANY($1::integer[])
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
      WHERE node_transaction.node_internal_id = ANY($1::integer[])
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
`,
        [nodeIds]
      );
      return result.rows.map((row) => ({
        hash: row.hash,
        nodeName: row.nodeName,
        replacedAt:
          row.replacedAt === null
            ? null
            : timestampWithoutTimezoneToDate(row.replacedAt),
      }));
    }
  );

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
}) =>
  runMembershipTransaction(
    pool,
    outputMembershipMode,
    [nodeInternalId],
    async (client) => {
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
      return result.rows[0]!.archivedCount;
    }
  );

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
  nodeValidations: { nodeInternalId: number; validatedAt: Date }[]
) =>
  runMembershipTransaction(
    pool,
    outputMembershipMode,
    'all',
    async (client, nodeIds) => {
      const saved = await insertTransactions(
        client,
        [transaction],
        initialAcceptanceIds(nodeValidations)
      );
      const result = await client.query<{ internalId: string }>(
        'SELECT internal_id AS "internalId" FROM transaction WHERE hash = $1;',
        [Buffer.from(transaction.hash, 'hex')]
      );
      const internalId = result.rows[0]?.internalId;
      if (internalId === undefined) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(
          `Failed to save or find transaction while recording node validation: ${transaction.hash}`
        );
      }
      for (const chunk of boundedValueRows(
        nodeValidations.map((validation) => [
          validation.nodeInternalId,
          internalId,
          validation.validatedAt.toISOString(),
        ])
      )) {
        await client.query(
          `INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
      VALUES ${chunk.values} ON CONFLICT ON CONSTRAINT node_transaction_pkey DO NOTHING;`,
          chunk.parameters
        );
      }
      if (outputMembershipMode === 'incremental' && saved.size > 0) {
        await client.query(
          `SELECT output_membership.note_membership_changes(node_id, $2::bigint[])
      FROM unnest($1::integer[]) AS node_id;`,
          [nodeIds, [...saved.values()]]
        );
      }
    }
  );

/**
 * Immediately mark a node as having validated a transaction already known to
 * exist in the database.
 */
export const recordNodeValidation = async (
  transactionHash: string,
  validation: { nodeInternalId: number; validatedAt: Date }
) =>
  runMembershipTransaction(
    pool,
    outputMembershipMode,
    [validation.nodeInternalId],
    async (client) => {
      await client.query(
        `INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
    SELECT $1::integer, internal_id, $3::timestamp FROM transaction WHERE hash = $2::bytea
    ON CONFLICT ON CONSTRAINT node_transaction_pkey DO NOTHING;`,
        [
          validation.nodeInternalId,
          Buffer.from(transactionHash, 'hex'),
          validation.validatedAt.toISOString(),
        ]
      );
    }
  );

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
  const attemptedSavedTransactions = block.transactions.filter(
    (transaction) => transactionCache.get(transaction.hash)?.db !== true
  );
  /*
   * Body/topology publication currently fences the node universe. This is a
   * correctness baseline for late parents and incomplete blocks; benchmark
   * contention before replacing it with narrower topology synchronization.
   */
  return runMembershipTransaction(
    pool,
    outputMembershipMode,
    'all',
    async (client, nodeIds) => {
      const newlySaved = await insertTransactions(
        client,
        attemptedSavedTransactions,
        initialAcceptanceIds(nodeAcceptances)
      );
      await client.query(
        `INSERT INTO block (height, version, timestamp, hash, previous_block_hash,
      merkle_root, bits, nonce, size_bytes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
      ON CONFLICT ON CONSTRAINT block_hash_key DO NOTHING;`,
        [
          block.height,
          block.version,
          block.timestamp,
          Buffer.from(block.hash, 'hex'),
          Buffer.from(block.previousBlockHash, 'hex'),
          Buffer.from(block.merkleRoot, 'hex'),
          block.bits,
          block.nonce,
          block.sizeBytes,
        ]
      );
      const blockResult = await client.query<{ internalId: string }>(
        'SELECT internal_id AS "internalId" FROM block WHERE hash = $1;',
        [Buffer.from(block.hash, 'hex')]
      );
      const blockId = blockResult.rows[0]!.internalId;
      const transactionRows = function* transactionRows(): Generator<
        QueryParameter[]
      > {
        for (const [index, transaction] of block.transactions.entries()) {
          yield [Buffer.from(transaction.hash, 'hex'), index];
        }
      };
      for (const chunk of boundedValueRows(transactionRows(), 1, [
        'bytea',
        'bigint',
      ])) {
        await client.query(
          `INSERT INTO block_transaction (block_internal_id, transaction_internal_id, transaction_index)
        SELECT $1::bigint, tx.internal_id, val.transaction_index::bigint
        FROM (VALUES ${chunk.values}) val(hash,transaction_index)
        JOIN transaction tx ON tx.hash = val.hash::bytea
        ON CONFLICT ON CONSTRAINT block_transaction_pkey DO NOTHING;`,
          [blockId, ...chunk.parameters]
        );
      }
      const linked = await client.query<{ count: string }>(
        'SELECT COUNT(*)::bigint AS count FROM block_transaction WHERE block_internal_id = $1;',
        [blockId]
      );
      if (Number(linked.rows[0]!.count) !== block.transactions.length) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(
          `Failed to save all transactions for block ${block.height} (${
            block.hash
          }): linked ${linked.rows[0]!.count}/${block.transactions.length}.`
        );
      }
      for (const chunk of boundedValueRows(
        nodeAcceptances.map((acceptance) => [
          acceptance.nodeInternalId,
          blockId,
          acceptance.acceptedAt?.toISOString() ?? null,
        ])
      )) {
        await client.query(
          `INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at)
        VALUES ${chunk.values} ON CONFLICT ON CONSTRAINT node_block_pkey DO NOTHING;`,
          chunk.parameters
        );
      }
      if (outputMembershipMode === 'incremental') {
        /*
         * Includes known transactions newly linked under existing accepted blocks,
         * and creator/input ingestion that changes another node's spender view.
         */
        await client.query(
          `SELECT output_membership.note_membership_changes(node_id,
        ARRAY(SELECT transaction_internal_id FROM block_transaction WHERE block_internal_id = $2))
        FROM unnest($1::integer[]) AS node_id;`,
          [nodeIds, blockId]
        );
      }
      return {
        attemptedSavedTransactions,
        transactionCacheMisses:
          attemptedSavedTransactions.length - newlySaved.size,
      };
    }
  );
};

/**
 * Used when a node catches up to one or more other nodes via headers-sync.
 *
 * Returns the number of node_blocks inserted.
 */
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

  return runMembershipTransaction(
    pool,
    outputMembershipMode,
    [nodeInternalId],
    async (client) => {
      let insertedCount = 0;
      const headerParameterCount = 3;
      const hashRows = function* hashRows(): Generator<QueryParameter[]> {
        for (const block of acceptedBlocks)
          yield [Buffer.from(block.hash, 'hex')];
      };
      for (const chunk of boundedValueRows(hashRows(), headerParameterCount, [
        'bytea',
      ])) {
        const result = await client.query(
          `INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at)
        SELECT $1::integer, block.internal_id,
          CASE WHEN block.timestamp < $2::bigint THEN NULL ELSE $3::timestamp END
        FROM block JOIN (VALUES ${chunk.values}) val(hash) ON block.hash = val.hash::bytea
        ON CONFLICT DO NOTHING;`,
          [
            nodeInternalId,
            nullifyAcceptedTimeBeforeBlockTimestamp,
            acceptedAt.toISOString(),
            ...chunk.parameters,
          ]
        );
        insertedCount += result.rowCount ?? 0;
      }
      return insertedCount;
    }
  );
};

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
) =>
  runMembershipTransaction(
    pool,
    outputMembershipMode,
    [nodeInternalId],
    async (client) => {
      const hashRows = function* hashRows(): Generator<QueryParameter[]> {
        for (const hash of staleChain) yield [Buffer.from(hash, 'hex')];
      };
      for (const chunk of boundedValueRows(hashRows(), 1, ['bytea'])) {
        await client.query(
          `DELETE FROM node_block WHERE node_internal_id = $1::integer
        AND block_internal_id IN (SELECT block.internal_id FROM block
          JOIN (VALUES ${chunk.values}) val(hash) ON block.hash = val.hash::bytea);`,
          [nodeInternalId, ...chunk.parameters]
        );
      }
    }
  );

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
  return Promise.all(indexCreations);
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
