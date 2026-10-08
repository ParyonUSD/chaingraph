/* eslint-disable @typescript-eslint/no-magic-numbers */
/**
 * Binary COPY encoders for the per-connection staging tables used by the
 * `copy` write path (`CHAINGRAPH_WRITE_PATH=copy`, see `saveBlock` in
 * `db.ts`). Column order here must match `stageTableColumns`.
 */
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

import { BinaryCopyWriter } from './pg-binary-copy.js';
import type { Spend } from './unspent-tracking.js';

/**
 * Temporary staging tables, created once per pooled connection. Rows are
 * removed at every COMMIT (and discarded by ROLLBACK), so each block's DB
 * transaction starts with empty tables.
 *
 * Names are prefixed to avoid shadowing permanent tables (`pg_temp` is
 * searched first) and always referenced as `pg_temp.<name>`.
 */
export const stageTableColumns = {
  /* eslint-disable camelcase, @typescript-eslint/naming-convention */
  chaingraph_stage_block_transaction: [
    ['hash', 'bytea'],
    ['transaction_index', 'bigint'],
  ],
  chaingraph_stage_input: [
    ['transaction_hash', 'bytea'],
    ['input_index', 'bigint'],
    ['outpoint_index', 'bigint'],
    ['sequence_number', 'bigint'],
    ['outpoint_transaction_hash', 'bytea'],
    ['unlocking_bytecode', 'bytea'],
  ],
  chaingraph_stage_output: [
    ['transaction_hash', 'bytea'],
    ['output_index', 'bigint'],
    ['value_satoshis', 'bigint'],
    ['locking_bytecode', 'bytea'],
    ['token_category', 'bytea'],
    ['fungible_token_amount', 'bigint'],
    ['nonfungible_token_capability', 'enum_nonfungible_token_capability'],
    ['nonfungible_token_commitment', 'bytea'],
  ],
  chaingraph_stage_spend: [
    ['outpoint_transaction_hash', 'bytea'],
    ['outpoint_index', 'bigint'],
    ['spender_hash', 'bytea'],
  ],
  chaingraph_stage_transaction: [
    ['hash', 'bytea'],
    ['version', 'bigint'],
    ['locktime', 'bigint'],
    ['size_bytes', 'bigint'],
    ['is_coinbase', 'boolean'],
  ],
  /* eslint-enable camelcase, @typescript-eslint/naming-convention */
} as const;

export type StageTable = keyof typeof stageTableColumns;

export const createStageTablesSql = Object.entries(stageTableColumns)
  .map(
    ([table, columns]) =>
      `CREATE TEMP TABLE IF NOT EXISTS ${table} (${columns
        .map(([name, type]) => `${name} ${type}`)
        .join(', ')}) ON COMMIT DELETE ROWS;`
  )
  .join('\n');

const buildCopyStageTableSql = (table: StageTable) =>
  `COPY pg_temp.${table} (${stageTableColumns[table]
    .map(([name]) => name)
    .join(', ')}) FROM STDIN (FORMAT binary)`;

const copyStageTableStatements = Object.fromEntries(
  Object.keys(stageTableColumns).map((table) => [
    table,
    buildCopyStageTableSql(table as StageTable),
  ])
) as { [table in StageTable]: string };

export const copyStageTableSql = (table: StageTable) =>
  copyStageTableStatements[table];

const hashBytes = 32;
const copyHeaderAndTrailerBytes = 64;
const transactionRowBytes = 2 + 5 * 4 + hashBytes + 3 * 8 + 1;
const blockTransactionRowBytes = 2 + 2 * 4 + hashBytes + 8;
const hexCharsPerByte = 2;
const inputRowFixedBytes = 2 + 6 * 4 + 2 * hashBytes + 3 * 8;
const outputRowFixedBytes = 2 + 8 * 4 + hashBytes + 2 * 8;

export const encodeStageTransactions = (
  transactions: ChaingraphTransaction[]
) => {
  const writer = new BinaryCopyWriter(
    transactions.length * transactionRowBytes + copyHeaderAndTrailerBytes
  );
  transactions.forEach((transaction) => {
    writer
      .startRow(stageTableColumns.chaingraph_stage_transaction.length)
      .hexBytea(transaction.hash)
      .int8(transaction.version)
      .int8(transaction.locktime)
      .int8(transaction.sizeBytes)
      .boolean(transaction.isCoinbase);
  });
  return writer.finish();
};

export const encodeStageInputs = (transactions: ChaingraphTransaction[]) => {
  const expectedBytes = transactions.reduce(
    (total, transaction) =>
      total +
      transaction.inputs.reduce(
        (sum, input) =>
          sum +
          inputRowFixedBytes +
          input.unlockingBytecode.length / hexCharsPerByte,
        0
      ),
    copyHeaderAndTrailerBytes
  );
  const writer = new BinaryCopyWriter(expectedBytes);
  transactions.forEach((transaction) => {
    transaction.inputs.forEach((input, inputIndex) => {
      writer
        .startRow(stageTableColumns.chaingraph_stage_input.length)
        .hexBytea(transaction.hash)
        .int8(inputIndex)
        .int8(input.outpointIndex)
        .int8(input.sequenceNumber)
        .hexBytea(input.outpointTransactionHash)
        .hexBytea(input.unlockingBytecode);
    });
  });
  return writer.finish();
};

export const encodeStageOutputs = (transactions: ChaingraphTransaction[]) => {
  const expectedBytes = transactions.reduce(
    (total, transaction) =>
      total +
      transaction.outputs.reduce(
        (sum, output) =>
          sum +
          outputRowFixedBytes +
          output.lockingBytecode.length / hexCharsPerByte,
        0
      ),
    copyHeaderAndTrailerBytes
  );
  const writer = new BinaryCopyWriter(expectedBytes);
  transactions.forEach((transaction) => {
    transaction.outputs.forEach((output, outputIndex) => {
      writer
        .startRow(stageTableColumns.chaingraph_stage_output.length)
        .hexBytea(transaction.hash)
        .int8(outputIndex)
        .int8(output.valueSatoshis)
        .hexBytea(output.lockingBytecode);
      if (output.tokenCategory === undefined) {
        writer.null();
      } else {
        writer.hexBytea(output.tokenCategory);
      }
      if (output.fungibleTokenAmount === undefined) {
        writer.null();
      } else {
        writer.int8(output.fungibleTokenAmount);
      }
      if (output.nonfungibleTokenCapability === undefined) {
        writer.null();
      } else {
        writer.text(output.nonfungibleTokenCapability);
      }
      if (output.nonfungibleTokenCommitment === undefined) {
        writer.null();
      } else {
        writer.hexBytea(output.nonfungibleTokenCommitment);
      }
    });
  });
  return writer.finish();
};

/**
 * Every transaction in the block (including those already saved), with its
 * index in the block.
 */
export const encodeStageBlockTransactions = (block: ChaingraphBlock) => {
  const writer = new BinaryCopyWriter(
    block.transactions.length * blockTransactionRowBytes +
      copyHeaderAndTrailerBytes
  );
  block.transactions.forEach((transaction, transactionIndex) => {
    writer
      .startRow(stageTableColumns.chaingraph_stage_block_transaction.length)
      .hexBytea(transaction.hash)
      .int8(transactionIndex);
  });
  return writer.finish();
};

const spendRowBytes = 2 + 3 * 4 + 2 * hashBytes + 8;

/**
 * Every outpoint spent by the block (`CHAINGRAPH_UNSPENT_TRACKING` modes other
 * than `off`), with the spending transaction's hash.
 */
export const encodeStageSpends = (spends: Spend[]) => {
  const writer = new BinaryCopyWriter(
    spends.length * spendRowBytes + copyHeaderAndTrailerBytes
  );
  spends.forEach((spend) => {
    writer
      .startRow(stageTableColumns.chaingraph_stage_spend.length)
      .hexBytea(spend.outpointTransactionHash)
      .int8(spend.outpointIndex)
      .hexBytea(spend.spenderHash);
  });
  return writer.finish();
};
