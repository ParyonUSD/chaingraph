/* eslint-disable max-lines */
import pg from 'pg';

import type { Agent } from './agent.js';
import {
  computeIndexCreationProgress,
  indexDefinitions,
} from './components/db-utils.js';
import type {
  BatchResult,
  SettleCandidate,
  StallState,
} from './components/unspent-node-ids.js';
import {
  backfillBatchSql,
  batchDidWork,
  batchReachedLimits,
  blockReacceptedEventsSql,
  buildRootSql,
  configureTrackingTriggersSql,
  deleteConsumedEventsSql,
  formatBatchLog,
  headersAcceptedEventsSql,
  initializeSql,
  nextSkipThrough,
  nextStallState,
  nextTransactionIdSql,
  nodeIndexDefinitions,
  parseBatchResult,
  progressSql,
  readSequencesSql,
  repartitionSql,
  runBatchSql,
  snapshotSettledSql,
  trackingTriggerNames,
  transactionReacceptedEventsSql,
  writeSettingsSql,
} from './components/unspent-node-ids.js';
import {
  postgresConnectionString,
  postgresMaxConnections,
  postgresSynchronousCommit,
  unspentNodeIds,
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
 * Unspent tracking job: its own connections (one per partition, plus one for
 * settling and bookkeeping), outside the agent's pool (block saves can hold
 * every pooled connection for minutes during a sync).
 */
export const unspentNodeIdsJobPool = new pg.Pool({
  connectionString: postgresConnectionString,
  max: unspentNodeIds.connections + 1,
});

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
// eslint-disable-next-line complexity
export const saveTransactionForNodes = async (
  transaction: ChaingraphTransaction,
  nodeValidations: {
    nodeInternalId: number;
    validatedAt: Date;
  }[]
) => {
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
  INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment)
    SELECT transaction_hash, output_index, value_satoshis, locking_bytecode, token_category::bytea, fungible_token_amount::bigint, nonfungible_token_capability::enum_nonfungible_token_capability, nonfungible_token_commitment::bytea FROM output_values CROSS JOIN new_transaction
), insert_inputs AS (
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
  const client = await pool.connect();
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
    await client.query(saveNodeValidations, [transactionInternalId]);
    if (
      unspentNodeIds.enabled &&
      Number(savedTransactionResult.rows[0]?.count ?? 0) === 0
    ) {
      /*
       * Unspent tracking: the transaction already existed (its outputs may be
       * processed already), so its new acceptances are recorded as events.
       */
      await client.query(transactionReacceptedEventsSql, [
        transactionInternalId,
        nodeValidations.map((validation) => validation.nodeInternalId),
      ]);
    }
    await client.query('COMMIT;');
  } catch (err) {
    await client.query('ROLLBACK;');
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  } finally {
    client.release();
  }
};

/**
 * Immediately mark a node as having validated a transaction already known to
 * exist in the database.
 */
// eslint-disable-next-line complexity
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
    if (unspentNodeIds.enabled) {
      await client.query('BEGIN;');
    }
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
    if (unspentNodeIds.enabled) {
      // unspent tracking: a known transaction gains an acceptance
      const known = await client.query<{ internalId: string }>(
        /* sql */ `SELECT internal_id AS "internalId" FROM transaction WHERE hash = $1;`,
        [Buffer.from(transactionHash, 'hex')]
      );
      if (known.rows[0] !== undefined) {
        await client.query(transactionReacceptedEventsSql, [
          known.rows[0].internalId,
          [validation.nodeInternalId],
        ]);
      }
      await client.query('COMMIT;');
    }
  } catch (err) {
    if (unspentNodeIds.enabled) {
      await client.query('ROLLBACK;');
    }
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
// eslint-disable-next-line complexity
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
  INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment)
    SELECT transaction_hash, output_index, value_satoshis, locking_bytecode, token_category::bytea, fungible_token_amount::bigint, nonfungible_token_capability::enum_nonfungible_token_capability, nonfungible_token_commitment::bytea FROM unknown_output_values
    WHERE transaction_hash IN (SELECT hash FROM newly_saved_transactions)
),
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
  const client = await pool.connect();
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
    const addBlockResult = await client.query<{
      insertedBlockTransactionCount: string;
      insertedNodeBlockCount: string;
      joinedTransactionCount: string;
    }>(addBlockQuery);
    const joinedTransactionCount = Number(
      addBlockResult.rows[0]!.joinedTransactionCount
    );
    if (
      unspentNodeIds.enabled &&
      Number(addBlockResult.rows[0]!.insertedNodeBlockCount) > 0
    ) {
      /*
       * Unspent tracking: node_block rows added to a block that already
       * existed (its id may be below the job's block watermark) are recorded
       * as events. New blocks need none: the job takes every block above its
       * watermark.
       */
      await client.query(blockReacceptedEventsSql, [
        Buffer.from(block.hash, 'hex'),
        nodeAcceptances.map((acceptance) => acceptance.nodeInternalId),
      ]);
    }
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
    await client.query('COMMIT;');
    return {
      attemptedSavedTransactions,
      transactionCacheMisses,
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
    // unspent tracking: the inserted node_block rows are re-acceptances
    const nodeBlockInsertResult = await client.query(
      unspentNodeIds.enabled
        ? headersAcceptedEventsSql(nodeInternalId, insertNodeBlocks)
        : insertNodeBlocks
    );
    return nodeBlockInsertResult.rowCount;
  } finally {
    client.release();
  }
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

/* eslint-disable complexity, max-params, @typescript-eslint/no-magic-numbers, require-atomic-updates, @typescript-eslint/init-declarations */
/*
 * Stored per-node unspent set (`CHAINGRAPH_UNSPENT_NODE_IDS`): configuration,
 * per-node indexes and the tracking job. See migration
 * `1791500000000_unspent_node_ids`.
 */

const unspentTrackingLog: {
  info: (message: string) => void;
  warn: (message: string) => void;
} = {
  info: () => undefined,
  warn: () => undefined,
};
export const setUnspentNodeIdsLoggers = (
  loggers: typeof unspentTrackingLog
) => {
  unspentTrackingLog.info = loggers.info;
  unspentTrackingLog.warn = loggers.warn;
};

const runStatements = async (client: pg.PoolClient, statements: string[]) =>
  statements.reduce<Promise<unknown>>(
    async (chain, statement) => chain.then(async () => client.query(statement)),
    Promise.resolve()
  );

/**
 * Enable the release-event triggers when tracking is on (disable them when
 * off), write the read thresholds and rebuild the query root for the
 * registered nodes. Returns the statements run (none if the migration is
 * missing and tracking is off).
 */
export const configureUnspentNodeIds = async () => {
  const client = await pool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const existing = Object.fromEntries(
      (
        await client.query<{ enabled: boolean; tgname: string }>(
          // cspell:ignore tgname tgenabled
          /* sql */ `SELECT tgname, tgenabled <> 'D' AS enabled FROM pg_trigger WHERE tgname = ANY ($1::text[]);`,
          [trackingTriggerNames]
        )
      ).rows.map((row) => [row.tgname, row.enabled])
    );
    if (Object.keys(existing).length === 0) {
      if (unspentNodeIds.enabled) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new Error(
          'CHAINGRAPH_UNSPENT_NODE_IDS=true requires migration 1791500000000_unspent_node_ids.'
        );
      }
      return [];
    }
    const statements = configureTrackingTriggersSql(
      unspentNodeIds.enabled,
      existing
    );
    await runStatements(client, statements);
    if (!unspentNodeIds.enabled) {
      return statements;
    }
    await client.query(writeSettingsSql, [
      unspentNodeIds.hashMaxTransactions,
      unspentNodeIds.fallbackTransactions,
      unspentNodeIds.fallbackEvents,
    ]);
    await client.query(buildRootSql);
    return [...statements, writeSettingsSql, buildRootSql];
  } finally {
    client.release();
  }
};

/**
 * Create the per-node partial indexes that are missing (after the initial
 * sync and the managed indexes, so the initial sync does not maintain them),
 * then rebuild the query root for the registered nodes. Returns the
 * statements run.
 */
export const ensureUnspentNodeIdsIndexes = async () => {
  if (!unspentNodeIds.enabled) {
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
    const statements = nodeIds.flatMap((id) =>
      Object.entries(nodeIndexDefinitions(id))
        .filter(([name]) => !existingIndexes.includes(name))
        .map(([, definition]) => definition)
    );
    await runStatements(client, statements);
    await client.query(buildRootSql);
    return statements;
  } finally {
    client.release();
  }
};

/**
 * Sequence values whose allocating transactions have all finished (see
 * `readSequencesSql`), and the candidate being settled.
 */
// eslint-disable-next-line functional/no-let
let settledLimits: { blockLimit: number; transactionLimit: number } | undefined;
// eslint-disable-next-line functional/no-let
let settleCandidate: SettleCandidate | undefined;
const partitionStalls = new Map<number, StallState | undefined>();
const partitionSkipThrough = new Map<number, number>();
// eslint-disable-next-line functional/no-let
let passInFlight: Promise<UnspentNodeIdsPassSummary> | undefined;

export interface UnspentNodeIdsPassSummary {
  backfillBatches: number;
  backfillChanged: number;
  batches: number;
  busy: boolean;
  caughtUp: boolean;
  changed: number;
  inputs: number;
  lastBatches: (BatchResult | undefined)[];
  ms: number;
  partitions: number;
  stalled: boolean;
}

const settlePollMs = 25;
const sleep = async (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/**
 * Advance `settledLimits`: read the sequences (new candidate), wait the grace,
 * take the next xid, then poll until every xid up to it has finished or
 * `waitMs` elapsed (the candidate is kept for the next call).
 */
const settleLimits = async (client: pg.PoolClient, waitMs: number) => {
  const start = Date.now();
  if (settleCandidate === undefined) {
    const row = (
      await client.query<{ blockLimit: string; transactionLimit: string }>(
        readSequencesSql
      )
    ).rows[0]!;
    settleCandidate = {
      blockLimit: Number(row.blockLimit),
      readAt: Date.now(),
      transactionLimit: Number(row.transactionLimit),
    };
  }
  const candidate = settleCandidate;
  if (candidate.nextXid === undefined) {
    const graceLeft = unspentNodeIds.graceMs - (Date.now() - candidate.readAt);
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
    settledLimits = {
      blockLimit: candidate.blockLimit,
      transactionLimit: candidate.transactionLimit,
    };
    settleCandidate = undefined;
    return true;
  }
  return false;
};

const serializationFailure = '40001';
const isSerializationFailure = (err: unknown) =>
  (err as { code?: string } | undefined)?.code === serializationFailure;

/**
 * One batch of one partition in its own REPEATABLE READ transaction (one
 * snapshot for every step; the stored values and the partition's watermarks
 * commit together). A serialization failure (a concurrent event or watch
 * recompute of the same row by another agent's job) is retried next loop.
 */
const runPartitionBatch = async (
  client: pg.PoolClient,
  partition: number,
  partitionCount: number,
  limits: { blockLimit: number; transactionLimit: number },
  checkWatch: boolean
) => {
  const maxBlocks = 200;
  const maxEvents = 20_000;
  const skipThrough = nextSkipThrough(
    partitionSkipThrough.get(partition) ?? 0,
    partitionStalls.get(partition),
    Date.now(),
    unspentNodeIds.stallMaxMs,
    limits.transactionLimit
  );
  partitionSkipThrough.set(partition, skipThrough);
  await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ;');
  // eslint-disable-next-line functional/no-try-statement
  try {
    const raw = (
      await client.query<{ result: { [key: string]: unknown } }>(runBatchSql, [
        partition,
        partitionCount,
        limits.transactionLimit,
        limits.blockLimit,
        Math.max(unspentNodeIds.batchInputs * partitionCount, 1),
        maxBlocks,
        maxEvents,
        skipThrough,
        checkWatch,
      ])
    ).rows[0]!.result;
    await client.query('COMMIT;');
    const result = parseBatchResult(raw);
    partitionStalls.set(
      partition,
      nextStallState(
        partitionStalls.get(partition),
        result.stalledAt,
        Date.now()
      )
    );
    return result;
  } catch (err) {
    await client.query('ROLLBACK;');
    if (isSerializationFailure(err)) {
      return undefined;
    }
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  }
};

const runBackfillBatch = async (
  client: pg.PoolClient,
  partition: number,
  partitionCount: number
) => {
  await client.query('BEGIN ISOLATION LEVEL REPEATABLE READ;');
  // eslint-disable-next-line functional/no-try-statement
  try {
    const raw = (
      await client.query<{ result: { [key: string]: unknown } }>(
        backfillBatchSql,
        [partition, partitionCount, unspentNodeIds.backfillTransactions]
      )
    ).rows[0]!.result;
    await client.query('COMMIT;');
    return raw;
  } catch (err) {
    await client.query('ROLLBACK;');
    if (isSerializationFailure(err)) {
      return undefined;
    }
    // eslint-disable-next-line functional/no-throw-statement
    throw err;
  }
};

/**
 * Start tracking if it has not started (at the settled limits, or at genesis),
 * and re-partition if the configured connection count changed.
 */
const ensureInitialized = async (
  client: pg.PoolClient,
  partitionCount: number,
  limits: { blockLimit: number; transactionLimit: number }
) => {
  const initialized =
    (
      await client.query<{ initialized: boolean }>(initializeSql, [
        partitionCount,
        unspentNodeIds.startAtGenesis ? 0 : limits.transactionLimit,
        unspentNodeIds.startAtGenesis ? 0 : limits.blockLimit,
      ])
    ).rows[0]?.initialized === true;
  if (initialized) {
    unspentTrackingLog.info(
      `Unspent tracking job: tracking started at ${
        unspentNodeIds.startAtGenesis
          ? 'genesis'
          : `transaction ${limits.transactionLimit} / block ${limits.blockLimit}; earlier outputs stay unprocessed (NULL) until backfilled`
      } (${partitionCount} partitions).`
    );
    return;
  }
  const repartitioned =
    (
      await client.query<{ repartitioned: boolean }>(repartitionSql, [
        partitionCount,
      ])
    ).rows[0]?.repartitioned === true;
  if (repartitioned) {
    partitionStalls.clear();
    partitionSkipThrough.clear();
    unspentTrackingLog.info(
      `Unspent tracking job: re-partitioned into ${partitionCount} partitions (every partition restarts from the lowest watermark).`
    );
  }
};

/**
 * One pass of the tracking job: settle new limits, then run every partition
 * on its own connection, in parallel, until it reaches the settled limits
 * (and nothing else is pending) or `maxMs` elapsed; then delete the events
 * every partition has consumed and, if enabled and caught up, backfill.
 * Concurrent callers share the pass in flight.
 */
export const runUnspentNodeIdsJobPass = async (
  options: { backfill?: boolean; maxMs?: number; settleWaitMs?: number } = {}
): Promise<UnspentNodeIdsPassSummary> => {
  if (passInFlight !== undefined) {
    return passInFlight;
  }
  const partitionCount = unspentNodeIds.connections;
  const start = Date.now();
  const maxMs = options.maxMs ?? unspentNodeIds.passMaxMs;
  const pass = async (): Promise<UnspentNodeIdsPassSummary> => {
    const summary: UnspentNodeIdsPassSummary = {
      backfillBatches: 0,
      backfillChanged: 0,
      batches: 0,
      busy: false,
      caughtUp: false,
      changed: 0,
      inputs: 0,
      lastBatches: [],
      ms: 0,
      partitions: partitionCount,
      stalled: false,
    };
    if (!unspentNodeIds.enabled) {
      return summary;
    }
    const coordinator = await unspentNodeIdsJobPool.connect();
    // eslint-disable-next-line functional/no-try-statement
    try {
      await settleLimits(
        coordinator,
        options.settleWaitMs ?? Math.max(unspentNodeIds.graceMs * 5, 1_000)
      );
      const limits = settledLimits;
      if (limits === undefined) {
        return summary;
      }
      await ensureInitialized(coordinator, partitionCount, limits);
      const partitionResults = await Promise.all(
        Array.from({ length: partitionCount }, async (_, partition) => {
          const client = await unspentNodeIdsJobPool.connect();
          const state = {
            batches: 0,
            caughtUp: false,
            changed: 0,
            inputs: 0,
            last: undefined as BatchResult | undefined,
            stopped: false,
          };
          // eslint-disable-next-line functional/no-try-statement
          try {
            const loop = async (): Promise<void> => {
              const batchStart = Date.now();
              // the watch set is re-checked once per pass (first batch)
              const result = await runPartitionBatch(
                client,
                partition,
                partitionCount,
                limits,
                state.batches === 0
              );
              state.batches += 1;
              if (result === undefined) {
                // serialization failure: retried
                return Date.now() - start >= maxMs ? undefined : loop();
              }
              state.last = result;
              if (
                result.busy === true ||
                result.uninitialized === true ||
                result.repartition === true
              ) {
                state.stopped = true;
                return undefined;
              }
              state.inputs += result.inputs;
              state.changed += result.changed;
              const worked = batchDidWork(result);
              if (worked || result.stalledAt !== null) {
                unspentTrackingLog.info(
                  formatBatchLog(
                    partition,
                    partitionCount,
                    result,
                    Date.now() - batchStart
                  )
                );
              }
              const reached = batchReachedLimits(result, limits);
              if (!worked || (reached && result.events === 0)) {
                state.caughtUp = reached;
                return undefined;
              }
              if (Date.now() - start >= maxMs) {
                return undefined;
              }
              return loop();
            };
            await loop();
            return state;
          } finally {
            client.release();
          }
        })
      );
      summary.batches = partitionResults.reduce((sum, p) => sum + p.batches, 0);
      summary.inputs = partitionResults.reduce((sum, p) => sum + p.inputs, 0);
      summary.changed = partitionResults.reduce((sum, p) => sum + p.changed, 0);
      summary.lastBatches = partitionResults.map((p) => p.last);
      summary.busy = partitionResults.some((p) => p.last?.busy === true);
      summary.caughtUp = partitionResults.every((p) => p.caughtUp);
      summary.stalled = partitionResults.some(
        (p) => p.last?.stalledAt !== null && p.last?.stalledAt !== undefined
      );
      await coordinator.query(deleteConsumedEventsSql);
      if (
        (options.backfill ?? unspentNodeIds.backfill) &&
        summary.caughtUp &&
        Date.now() - start < maxMs
      ) {
        const backfillResults = await Promise.all(
          Array.from({ length: partitionCount }, async (_, partition) => {
            const client = await unspentNodeIdsJobPool.connect();
            const state = { batches: 0, changed: 0 };
            // eslint-disable-next-line functional/no-try-statement
            try {
              const loop = async (): Promise<void> => {
                const raw = await runBackfillBatch(
                  client,
                  partition,
                  partitionCount
                );
                state.batches += 1;
                if (raw !== undefined) {
                  state.changed += Number(raw.changed ?? 0);
                  if (
                    raw.done === true ||
                    raw.busy === true ||
                    raw.uninitialized === true ||
                    raw.repartition === true
                  ) {
                    return undefined;
                  }
                }
                return Date.now() - start >= maxMs ? undefined : loop();
              };
              await loop();
              return state;
            } finally {
              client.release();
            }
          })
        );
        summary.backfillBatches = backfillResults.reduce(
          (sum, p) => sum + p.batches,
          0
        );
        summary.backfillChanged = backfillResults.reduce(
          (sum, p) => sum + p.changed,
          0
        );
        await coordinator.query(deleteConsumedEventsSql);
      }
      return summary;
    } finally {
      summary.ms = Date.now() - start;
      coordinator.release();
    }
  };
  passInFlight = pass().finally(() => {
    passInFlight = undefined;
  });
  return passInFlight;
};

/**
 * Run passes until the job has processed everything committed before this
 * call (tests, measurements): the settled limits reach the sequence values
 * read now and every partition's last batch found nothing left (a stalled
 * partition counts as done once its stall persists).
 */
export const drainUnspentNodeIdsJob = async (timeoutMs = 120_000) => {
  const client = await unspentNodeIdsJobPool.connect();
  const target = await client
    .query<{ blockLimit: string; transactionLimit: string }>(readSequencesSql)
    .then((result) => result.rows[0]!)
    .finally(() => {
      client.release();
    });
  const start = Date.now();
  const attempt = async (): Promise<UnspentNodeIdsPassSummary> => {
    const summary = await runUnspentNodeIdsJobPass({
      maxMs: timeoutMs,
      settleWaitMs: 2_000,
    });
    const limits = settledLimits;
    const done =
      !unspentNodeIds.enabled ||
      (limits !== undefined &&
        limits.transactionLimit >= Number(target.transactionLimit) &&
        limits.blockLimit >= Number(target.blockLimit) &&
        summary.lastBatches.length === summary.partitions &&
        summary.lastBatches.every(
          (last) =>
            last !== undefined &&
            !batchDidWork(last) &&
            (last.inputWatermark >= limits.transactionLimit ||
              last.stalledAt !== null)
        ));
    if (done) {
      return summary;
    }
    if (Date.now() - start > timeoutMs) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `Unspent tracking job did not drain within ${timeoutMs} ms (last batches: ${JSON.stringify(
          summary.lastBatches
        )}).`
      );
    }
    return attempt();
  };
  return attempt();
};

/**
 * The job's watermarks and backlog (logs, metrics, tests).
 */
export const getUnspentNodeIdsStatus = async () => {
  if (!unspentNodeIds.enabled) {
    return undefined;
  }
  const client = await unspentNodeIdsJobPool.connect();
  // eslint-disable-next-line functional/no-try-statement
  try {
    const [progress] = (
      await client.query<{ [key: string]: boolean | string | null }>(
        progressSql
      )
    ).rows;
    return progress;
  } finally {
    client.release();
  }
};
/* eslint-enable complexity, max-params, @typescript-eslint/no-magic-numbers, require-atomic-updates, @typescript-eslint/init-declarations */
