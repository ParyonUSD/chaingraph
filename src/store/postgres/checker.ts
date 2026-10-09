// cspell:ignore tgenabled tgname tgisinternal unnest
import type pg from 'pg';

import type {
  AcceptedBlock,
  BlockHistoryEntry,
  BlockSelector,
  BlockValueAggregates,
  CheckerInput,
  CheckerOutpoint,
  CheckerOutput,
  MempoolEntry,
  SchemaReport,
  StoreChecker,
  TransactionHistoryEntry,
} from '../checker.js';

/**
 * Anything that can run a query: a `pg.Client` (which also lets the checker
 * see rows of a transaction the caller has open) or a `pg.Pool`.
 */
export type PostgresQueryable = pg.ClientBase | pg.Pool;

const bin = (hex: string) => Buffer.from(hex, 'hex');

/**
 * `timestamp without time zone` columns hold UTC; convert so `pg` parses
 * them as UTC instead of the local zone.
 */
const utc = (column: string) => `(${column} AT TIME ZONE 'UTC')`;

/**
 * Matches `transaction.hash` against a `text[]` parameter of hex hashes.
 */
const hashInHexList = (parameter: string) =>
  `transaction.hash IN (SELECT decode(hex_hash, 'hex') FROM unnest(${parameter}::text[]) AS hex_hash)`;

const blockWhere = (by: BlockSelector): [string, unknown[]] => {
  if (by.hash !== undefined) {
    return ['block.hash = $1', [bin(by.hash)]];
  }
  if (by.height !== undefined) {
    return ['block.height = $1', [by.height]];
  }
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error('BlockSelector requires a hash or a height.');
};

/**
 * Transactions accepted by the node `$1` (by name): in its mempool or in a
 * block it accepts. Yields `internal_id`.
 */
const acceptedTransactionIds = /* sql */ `
  SELECT node_transaction.transaction_internal_id AS internal_id
    FROM node_transaction
    JOIN node ON node.internal_id = node_transaction.node_internal_id
    WHERE node.name = $1
  UNION
  SELECT block_transaction.transaction_internal_id AS internal_id
    FROM block_transaction
    JOIN node_block ON node_block.block_internal_id = block_transaction.block_internal_id
    JOIN node ON node.internal_id = node_block.node_internal_id
    WHERE node.name = $1
`;

export const createPostgresChecker = (db: PostgresQueryable): StoreChecker => {
  const rows = async <Row extends object>(
    text: string,
    values: unknown[] = []
  ) => (await db.query<Row>(text, values)).rows;

  const hexColumn = (rowsOfHashes: { hash: string }[]) =>
    rowsOfHashes.map((row) => row.hash);

  return {
    acceptedBlockCount: async (node, hashes) =>
      Number(
        (
          await rows<{ count: string }>(
            /* sql */ `
            SELECT COUNT(*) AS count
              FROM node_block
              JOIN node ON node.internal_id = node_block.node_internal_id
              JOIN block ON block.internal_id = node_block.block_internal_id
              WHERE node.name = $1
                AND block.hash IN (SELECT decode(hex_hash, 'hex') FROM unnest($2::text[]) AS hex_hash);`,
            [node, hashes]
          )
        )[0]!.count
      ),

    acceptedBlocks: async (node, filter = {}) =>
      (
        await rows<{
          acceptedAt: Date | null;
          hash: string;
          height: string;
        }>(
          /* sql */ `
          SELECT encode(block.hash, 'hex') AS hash,
                 block.height,
                 ${utc('node_block.accepted_at')} AS "acceptedAt"
            FROM node_block
            JOIN node ON node.internal_id = node_block.node_internal_id
            JOIN block ON block.internal_id = node_block.block_internal_id
            WHERE node.name = $1
              AND ($2::bigint IS NULL OR block.height = $2::bigint)
            ORDER BY block.height, block.hash;`,
          [node, filter.height ?? null]
        )
      ).map<AcceptedBlock>((row) => ({
        acceptedAt: row.acceptedAt,
        hash: row.hash,
        height: Number(row.height),
      })),

    allBlockHashes: async () =>
      hexColumn(
        await rows<{ hash: string }>(
          /* sql */ `SELECT encode(hash, 'hex') AS hash FROM block ORDER BY hash;`
        )
      ),

    blockHistory: async (node) =>
      rows<BlockHistoryEntry>(
        /* sql */ `
        SELECT encode(block.hash, 'hex') AS hash,
               ${utc('node_block_history.accepted_at')} AS "acceptedAt",
               ${utc('node_block_history.removed_at')} AS "removedAt"
          FROM node_block_history
          JOIN node ON node.internal_id = node_block_history.node_internal_id
          JOIN block ON block.internal_id = node_block_history.block_internal_id
          WHERE node.name = $1
          ORDER BY node_block_history.removed_at, block.hash;`,
        [node]
      ),

    blockTransactionAt: async (blockHash, index) =>
      (
        await rows<{ hash: string }>(
          /* sql */ `
          SELECT encode(transaction.hash, 'hex') AS hash
            FROM block_transaction
            JOIN block ON block.internal_id = block_transaction.block_internal_id
            JOIN transaction ON transaction.internal_id = block_transaction.transaction_internal_id
            WHERE block.hash = $1
              AND block_transaction.transaction_index = $2;`,
          [bin(blockHash), index]
        )
      )[0]?.hash,

    blockTransactionCount: async (blockHash) =>
      Number(
        (
          await rows<{ count: string }>(
            /* sql */ `
            SELECT COUNT(*) AS count
              FROM block_transaction
              JOIN block ON block.internal_id = block_transaction.block_internal_id
              WHERE block.hash = $1;`,
            [bin(blockHash)]
          )
        )[0]!.count
      ),

    blockValueAggregates: async (by) => {
      const [where, values] = blockWhere(by);
      const [row] = await rows<{
        fee: string;
        generated: string;
        input: string;
        output: string;
      }>(
        /* sql */ `
          SELECT block_fee_satoshis(block)::text AS fee,
                 block_generated_value_satoshis(block)::text AS generated,
                 block_input_value_satoshis(block)::text AS input,
                 block_output_value_satoshis(block)::text AS output
            FROM block WHERE ${where};`,
        values
      );
      return row === undefined
        ? undefined
        : ({
            fee: BigInt(row.fee),
            generated: BigInt(row.generated),
            input: BigInt(row.input),
            output: BigInt(row.output),
          } as BlockValueAggregates);
    },

    confirmedButInMempool: async (node) =>
      hexColumn(
        await rows<{ hash: string }>(
          /* sql */ `
          SELECT DISTINCT encode(transaction.hash, 'hex') AS hash
            FROM node_transaction
            JOIN node ON node.internal_id = node_transaction.node_internal_id
            JOIN transaction ON transaction.internal_id = node_transaction.transaction_internal_id
            JOIN block_transaction ON block_transaction.transaction_internal_id = node_transaction.transaction_internal_id
            JOIN node_block ON node_block.block_internal_id = block_transaction.block_internal_id
             AND node_block.node_internal_id = node_transaction.node_internal_id
            WHERE node.name = $1
            ORDER BY hash;`,
          [node]
        )
      ),

    dropBlockTransactionLink: async (blockHash, index) => {
      await db.query(
        /* sql */ `
        DELETE FROM block_transaction
          USING block
          WHERE block.internal_id = block_transaction.block_internal_id
            AND block.hash = $1
            AND block_transaction.transaction_index = $2;`,
        [bin(blockHash), index]
      );
    },

    encodedBlockHeaderHex: async (by) => {
      const [where, values] = blockWhere(by);
      return (
        await rows<{ hex: string }>(
          /* sql */ `SELECT block_header_encoded_hex(block) AS hex FROM block WHERE ${where};`,
          values
        )
      )[0]?.hex;
    },

    encodedBlockHex: async (by) => {
      const [where, values] = blockWhere(by);
      return (
        await rows<{ hex: string }>(
          /* sql */ `SELECT block_encoded_hex(block) AS hex FROM block WHERE ${where};`,
          values
        )
      )[0]?.hex;
    },

    encodedTransactionHex: async (hash) =>
      (
        await rows<{ hex: string }>(
          /* sql */ `SELECT encode(encode_transaction(transaction), 'hex') AS hex FROM transaction WHERE hash = $1;`,
          [bin(hash)]
        )
      )[0]?.hex,

    forgetNodeValidation: async (node, hash) => {
      await db.query(
        /* sql */ `
        DELETE FROM node_transaction
          USING node, transaction
          WHERE node_transaction.node_internal_id = node.internal_id
            AND node_transaction.transaction_internal_id = transaction.internal_id
            AND node.name = $1
            AND transaction.hash = $2;`,
        [node, bin(hash)]
      );
    },

    inputsOfTx: async (hash) =>
      (
        await rows<{
          inputIndex: string;
          outpointIndex: string;
          outpointTransactionHash: string;
          sequenceNumber: string;
          unlockingBytecode: string;
        }>(
          /* sql */ `
          SELECT input.input_index AS "inputIndex",
                 encode(input.outpoint_transaction_hash, 'hex') AS "outpointTransactionHash",
                 input.outpoint_index AS "outpointIndex",
                 input.sequence_number AS "sequenceNumber",
                 encode(input.unlocking_bytecode, 'hex') AS "unlockingBytecode"
            FROM input
            JOIN transaction ON transaction.internal_id = input.transaction_internal_id
            WHERE transaction.hash = $1
            ORDER BY input.input_index;`,
          [bin(hash)]
        )
      ).map<CheckerInput>((row) => ({
        inputIndex: Number(row.inputIndex),
        outpointIndex: Number(row.outpointIndex),
        outpointTransactionHash: row.outpointTransactionHash,
        sequenceNumber: Number(row.sequenceNumber),
        unlockingBytecode: row.unlockingBytecode,
      })),

    inputsSpending: async (outpointHash, index) =>
      (
        await rows<{ inputIndex: string; txHash: string }>(
          /* sql */ `
          SELECT encode(transaction.hash, 'hex') AS "txHash",
                 input.input_index AS "inputIndex"
            FROM input
            JOIN transaction ON transaction.internal_id = input.transaction_internal_id
            WHERE input.outpoint_transaction_hash = $1
              AND input.outpoint_index = $2
            ORDER BY transaction.hash, input.input_index;`,
          [bin(outpointHash), index]
        )
      ).map((row) => ({
        inputIndex: Number(row.inputIndex),
        txHash: row.txHash,
      })),

    mempool: async (node) =>
      rows<MempoolEntry>(
        /* sql */ `
        SELECT encode(transaction.hash, 'hex') AS hash,
               ${utc('node_transaction.validated_at')} AS "validatedAt"
          FROM node_transaction
          JOIN node ON node.internal_id = node_transaction.node_internal_id
          JOIN transaction ON transaction.internal_id = node_transaction.transaction_internal_id
          WHERE node.name = $1
          ORDER BY transaction.hash;`,
        [node]
      ),

    mempoolMembership: async (node, hashes) =>
      new Set(
        hexColumn(
          await rows<{ hash: string }>(
            /* sql */ `
            SELECT encode(transaction.hash, 'hex') AS hash
              FROM node_transaction
              JOIN node ON node.internal_id = node_transaction.node_internal_id
              JOIN transaction ON transaction.internal_id = node_transaction.transaction_internal_id
              WHERE node.name = $1
                AND ${hashInHexList('$2')}
              ORDER BY transaction.hash;`,
            [node, hashes]
          )
        )
      ),

    nodeInternalId: async (node) => {
      const [row] = await rows<{ internalId: number }>(
        /* sql */ `SELECT internal_id AS "internalId" FROM node WHERE name = $1;`,
        [node]
      );
      return row === undefined ? undefined : Number(row.internalId);
    },

    nodeNamesOrdered: async () =>
      (
        await rows<{ internalId: number; name: string }>(
          /* sql */ `SELECT name, internal_id AS "internalId" FROM node ORDER BY name;`
        )
      ).map((row) => ({ internalId: Number(row.internalId), name: row.name })),

    orphanMempoolDescendants: async (node) =>
      hexColumn(
        await rows<{ hash: string }>(
          /* sql */ `
          WITH RECURSIVE selected_node AS (
              SELECT internal_id FROM node WHERE name = $1
          ),
          orphans AS (
              -- mempool transactions spending an output of a transaction this
              -- node archived as replaced
              SELECT node_transaction.transaction_internal_id
                FROM node_transaction
                JOIN selected_node ON selected_node.internal_id = node_transaction.node_internal_id
                JOIN input child_input ON child_input.transaction_internal_id = node_transaction.transaction_internal_id
                JOIN transaction parent ON parent.hash = child_input.outpoint_transaction_hash
                JOIN node_transaction_history history
                  ON history.transaction_internal_id = parent.internal_id
                 AND history.node_internal_id = selected_node.internal_id
                WHERE history.replaced_at IS NOT NULL
              UNION
              SELECT child.transaction_internal_id
                FROM orphans
                JOIN transaction parent ON parent.internal_id = orphans.transaction_internal_id
                JOIN input child_input ON child_input.outpoint_transaction_hash = parent.hash
                JOIN node_transaction child ON child.transaction_internal_id = child_input.transaction_internal_id
                JOIN selected_node ON selected_node.internal_id = child.node_internal_id
          )
          SELECT DISTINCT encode(transaction.hash, 'hex') AS hash
            FROM orphans
            JOIN transaction ON transaction.internal_id = orphans.transaction_internal_id
            ORDER BY hash;`,
          [node]
        )
      ),

    outputsOfTx: async (hash) =>
      (
        await rows<{
          fungibleTokenAmount: string | null;
          lockingBytecode: string;
          nonfungibleTokenCapability:
            | CheckerOutput['nonfungibleTokenCapability']
            | null;
          nonfungibleTokenCommitment: string | null;
          outputIndex: string;
          tokenCategory: string | null;
          valueSatoshis: string;
        }>(
          /* sql */ `
          SELECT output_index AS "outputIndex",
                 value_satoshis::text AS "valueSatoshis",
                 encode(locking_bytecode, 'hex') AS "lockingBytecode",
                 encode(token_category, 'hex') AS "tokenCategory",
                 fungible_token_amount::text AS "fungibleTokenAmount",
                 nonfungible_token_capability::text AS "nonfungibleTokenCapability",
                 encode(nonfungible_token_commitment, 'hex') AS "nonfungibleTokenCommitment"
            FROM output
            WHERE transaction_hash = $1
            ORDER BY output_index;`,
          [bin(hash)]
        )
      ).map<CheckerOutput>((row) => ({
        lockingBytecode: row.lockingBytecode,
        outputIndex: Number(row.outputIndex),
        valueSatoshis: BigInt(row.valueSatoshis),
        ...(row.tokenCategory === null
          ? {}
          : { tokenCategory: row.tokenCategory }),
        ...(row.fungibleTokenAmount === null
          ? {}
          : { fungibleTokenAmount: BigInt(row.fungibleTokenAmount) }),
        ...(row.nonfungibleTokenCapability === null
          ? {}
          : { nonfungibleTokenCapability: row.nonfungibleTokenCapability }),
        ...(row.nonfungibleTokenCommitment === null
          ? {}
          : { nonfungibleTokenCommitment: row.nonfungibleTokenCommitment }),
      })),

    schemaReport: async (): Promise<SchemaReport> => {
      const indexes = (
        await rows<{ indexname: string }>(
          /* sql */ `SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname;`
        )
      ).map((row) => row.indexname);
      const triggers = (
        await rows<{ tgenabled: string; tgname: string }>(
          /* sql */ `SELECT tgname, tgenabled FROM pg_trigger WHERE NOT tgisinternal ORDER BY tgname;`
        )
      ).reduce<SchemaReport['triggers']>(
        (all, row) => ({ ...all, [row.tgname]: row.tgenabled }),
        {}
      );
      return { indexes, triggers };
    },

    transactionExists: async (hash) =>
      (
        await rows<{ exists: boolean }>(
          /* sql */ `SELECT EXISTS (SELECT 1 FROM transaction WHERE hash = $1) AS exists;`,
          [bin(hash)]
        )
      )[0]!.exists,

    transactionHistory: async (node, hashes) =>
      rows<TransactionHistoryEntry>(
        /* sql */ `
        SELECT encode(transaction.hash, 'hex') AS hash,
               ${utc('history.validated_at')} AS "validatedAt",
               ${utc('history.replaced_at')} AS "replacedAt"
          FROM node_transaction_history history
          JOIN node ON node.internal_id = history.node_internal_id
          JOIN transaction ON transaction.internal_id = history.transaction_internal_id
          WHERE node.name = $1
            AND ($2::text[] IS NULL OR ${hashInHexList('$2')})
          ORDER BY history.validated_at, history.replaced_at, transaction.hash;`,
        [node, hashes ?? null]
      ),

    transactionRowCount: async (hash) =>
      Number(
        (
          await rows<{ count: string }>(
            /* sql */ `SELECT COUNT(*) AS count FROM transaction WHERE hash = $1;`,
            [bin(hash)]
          )
        )[0]!.count
      ),

    txAccepted: async (node, hash) =>
      (
        await rows<{ accepted: boolean }>(
          /* sql */ `
          SELECT EXISTS (
            SELECT 1
              FROM (${acceptedTransactionIds}) accepted
              JOIN transaction ON transaction.internal_id = accepted.internal_id
              WHERE transaction.hash = $2
          ) AS accepted;`,
          [node, bin(hash)]
        )
      )[0]!.accepted,

    unspent: async (node, scope) =>
      (
        await rows<{ outputIndex: string; transactionHash: string }>(
          /* sql */ `
          WITH accepted AS (${acceptedTransactionIds})
          SELECT encode(output.transaction_hash, 'hex') AS "transactionHash",
                 output.output_index AS "outputIndex"
            FROM output
            JOIN transaction ON transaction.hash = output.transaction_hash
            JOIN accepted ON accepted.internal_id = transaction.internal_id
            WHERE ($2::bytea IS NULL OR output.token_category = $2::bytea)
              AND ($3::bytea IS NULL OR output.locking_bytecode = $3::bytea)
              AND NOT EXISTS (
                SELECT 1
                  FROM input
                  JOIN accepted spender ON spender.internal_id = input.transaction_internal_id
                  WHERE input.outpoint_transaction_hash = output.transaction_hash
                    AND input.outpoint_index = output.output_index
              )
            ORDER BY output.transaction_hash, output.output_index;`,
          [
            node,
            scope.category === undefined ? null : bin(scope.category),
            scope.lockingBytecode === undefined
              ? null
              : bin(scope.lockingBytecode),
          ]
        )
      ).map<CheckerOutpoint>((row) => ({
        outputIndex: Number(row.outputIndex),
        transactionHash: row.transactionHash,
      })),

    validatingNodes: async (hash) =>
      (
        await rows<{ name: string }>(
          /* sql */ `
          SELECT node.name
            FROM node_transaction
            JOIN node ON node.internal_id = node_transaction.node_internal_id
            JOIN transaction ON transaction.internal_id = node_transaction.transaction_internal_id
            WHERE transaction.hash = $1
            ORDER BY node.name;`,
          [bin(hash)]
        )
      ).map((row) => row.name),
  };
};
