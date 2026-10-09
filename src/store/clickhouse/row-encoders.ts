/* eslint-disable @typescript-eslint/no-magic-numbers */
/**
 * RowBinary encoders for the agent-written core tables (`ddl/010_core.sql`):
 * `block`, `transaction`, `block_transaction`, `output` and `input`. Column
 * order and types here must match the DDL exactly (the spec checks them
 * against the DDL file); MATERIALIZED columns are not sent.
 *
 * Every row carries `commit_seq`, the save that wrote it. Internal ids are
 * allocated by the caller. Hashes are stored as given (the hex shown by the
 * agent, as in Postgres `bytea`); "no token" is a zero `token_category`.
 */
import type {
  ChaingraphBlock,
  ChaingraphOutput,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import { RowBinaryWriter } from './row-binary.js';

/* eslint-disable camelcase, @typescript-eslint/naming-convention */
/**
 * `[name, ClickHouse type]` per inserted column, in DDL order.
 */
export const rowBinaryTableColumns = {
  block: [
    ['hash', 'FixedString(32)'],
    ['internal_id', 'UInt64'],
    ['height', 'UInt32'],
    ['version', 'Int32'],
    ['timestamp', 'UInt32'],
    ['previous_block_hash', 'FixedString(32)'],
    ['merkle_root', 'FixedString(32)'],
    ['bits', 'UInt32'],
    ['nonce', 'UInt32'],
    ['size_bytes', 'UInt32'],
    ['transaction_count', 'UInt32'],
    ['output_value_satoshis', 'Int64'],
    ['generated_value_satoshis', 'Int64'],
    ['commit_seq', 'UInt64'],
  ],
  block_transaction: [
    ['block_internal_id', 'UInt64'],
    ['transaction_index', 'UInt32'],
    ['transaction_internal_id', 'UInt64'],
    ['transaction_hash', 'FixedString(32)'],
    ['commit_seq', 'UInt64'],
  ],
  input: [
    ['transaction_hash', 'FixedString(32)'],
    ['input_index', 'UInt32'],
    ['transaction_internal_id', 'UInt64'],
    ['outpoint_transaction_hash', 'FixedString(32)'],
    ['outpoint_index', 'UInt32'],
    ['sequence_number', 'UInt32'],
    ['unlocking_bytecode', 'String'],
    ['value_satoshis', 'Int64'],
    ['token_category', 'FixedString(32)'],
    [
      'nonfungible_token_capability',
      "Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3))",
    ],
    ['nonfungible_token_commitment', 'Nullable(String)'],
    ['locking_bytecode', 'String'],
    ['commit_seq', 'UInt64'],
  ],
  output: [
    ['transaction_hash', 'FixedString(32)'],
    ['output_index', 'UInt32'],
    ['transaction_internal_id', 'UInt64'],
    ['value_satoshis', 'Int64'],
    ['locking_bytecode', 'String'],
    ['token_category', 'FixedString(32)'],
    ['fungible_token_amount', 'Nullable(Int64)'],
    [
      'nonfungible_token_capability',
      "Nullable(Enum8('none' = 1, 'mutable' = 2, 'minting' = 3))",
    ],
    ['nonfungible_token_commitment', 'Nullable(String)'],
    ['commit_seq', 'UInt64'],
  ],
  transaction: [
    ['hash', 'FixedString(32)'],
    ['internal_id', 'UInt64'],
    ['version', 'Int32'],
    ['locktime', 'UInt32'],
    ['size_bytes', 'UInt32'],
    ['is_coinbase', 'Bool'],
    ['input_count', 'UInt32'],
    ['output_count', 'UInt32'],
    ['output_value_satoshis', 'Int64'],
    ['commit_seq', 'UInt64'],
  ],
} as const;
/* eslint-enable camelcase, @typescript-eslint/naming-convention */

export type RowBinaryTable = keyof typeof rowBinaryTableColumns;

/**
 * `INSERT INTO <database>.<table> (<columns>) FORMAT RowBinary`, the
 * statement for the data from the matching encoder.
 */
export const insertRowBinarySql = (table: RowBinaryTable, database = 'cg') =>
  `INSERT INTO ${database}.${table} (${rowBinaryTableColumns[table]
    .map(([name]) => name)
    .join(', ')}) FORMAT RowBinary`;

/**
 * `Enum8('none' = 1, 'mutable' = 2, 'minting' = 3)`
 */
export const nonfungibleTokenCapabilityEnum8 = {
  minting: 3,
  mutable: 2,
  none: 1,
} as const;

export interface EncodedRows {
  data: Buffer;
  rowCount: number;
}

/**
 * The attributes of a spent output copied onto its `input` row (a
 * `ChaingraphOutput` satisfies this).
 */
export type SpentOutput = Pick<
  ChaingraphOutput,
  | 'lockingBytecode'
  | 'nonfungibleTokenCapability'
  | 'nonfungibleTokenCommitment'
  | 'tokenCategory'
  | 'valueSatoshis'
>;

export type ResolveSpentOutput = (
  outpointTransactionHash: string,
  outpointIndex: number
) => SpentOutput | undefined;

export interface PendingInput {
  transactionHash: string;
  inputIndex: number;
}

export interface EncodedInputRows extends EncodedRows {
  /**
   * Inputs whose spent output could not be resolved; no row was written for
   * them (the `fill_pending` commit writes them later).
   */
  pending: PendingInput[];
}

export interface TransactionRowsContext {
  commitSeq: bigint | number;
  /**
   * The internal id of each transaction, by index.
   */
  transactionInternalIds: readonly (bigint | number)[];
}

export interface BlockRow {
  block: ChaingraphBlock;
  internalId: bigint | number;
  /**
   * Block output value minus the value of the outputs spent by its inputs
   * (needs the spent outputs, so the caller computes it).
   */
  generatedValueSatoshis: bigint | number;
}

const hashBytes = 32;
const hexCharsPerByte = 2;
/** Generous per-row allowance for LEB128 prefixes and Nullable flags. */
const rowSlackBytes = 16;
const transactionRowBytes = hashBytes + 8 + 4 + 4 + 4 + 1 + 4 + 4 + 8 + 8;
const blockRowBytes = 3 * hashBytes + 8 + 4 * 7 + 8 + 8 + 8;
const blockTransactionRowBytes = 8 + 4 + 8 + hashBytes + 8;
const outputRowFixedBytes = 2 * hashBytes + 4 + 8 + 8 + 8 + 8 + rowSlackBytes;
const inputRowFixedBytes =
  3 * hashBytes + 4 + 8 + 4 + 4 + 8 + 8 + rowSlackBytes;

const internalIdAt = (
  internalIds: readonly (bigint | number)[],
  index: number
) => {
  const internalId = internalIds[index];
  if (internalId === undefined) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new RangeError(`Missing internal id for transaction ${index}.`);
  }
  return internalId;
};

/**
 * The total value of the outputs of `transaction`.
 */
export const transactionOutputValueSatoshis = (
  transaction: ChaingraphTransaction
) =>
  transaction.outputs.reduce(
    (total, output) => total + output.valueSatoshis,
    0n
  );

/**
 * The total value of the outputs of every transaction in `block`.
 */
export const blockOutputValueSatoshis = (block: ChaingraphBlock) =>
  block.transactions.reduce(
    (total, transaction) => total + transactionOutputValueSatoshis(transaction),
    0n
  );

const writeTokenCategory = (writer: RowBinaryWriter, category?: string) =>
  category === undefined
    ? writer.zeros(hashBytes)
    : writer.fixedString32(category);

const writeNonfungibleToken = (
  writer: RowBinaryWriter,
  output: Pick<
    ChaingraphOutput,
    'nonfungibleTokenCapability' | 'nonfungibleTokenCommitment'
  >
) =>
  writer
    .nullable(output.nonfungibleTokenCapability, (w, capability) =>
      w.enum8(nonfungibleTokenCapabilityEnum8[capability])
    )
    .nullable(output.nonfungibleTokenCommitment, (w, commitment) =>
      w.hexBytes(commitment)
    );

export const encodeBlockRows = (
  rows: readonly BlockRow[],
  commitSeq: bigint | number
): EncodedRows => {
  const writer = new RowBinaryWriter(rows.length * blockRowBytes);
  rows.forEach(({ block, generatedValueSatoshis, internalId }) => {
    writer
      .fixedString32(block.hash)
      .uint64(internalId)
      .uint32(block.height)
      .int32(block.version)
      .uint32(block.timestamp)
      .fixedString32(block.previousBlockHash)
      .fixedString32(block.merkleRoot)
      .uint32(block.bits)
      .uint32(block.nonce)
      .uint32(block.sizeBytes)
      .uint32(block.transactions.length)
      .int64(blockOutputValueSatoshis(block))
      .int64(generatedValueSatoshis)
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

export const encodeTransactionRows = (
  transactions: readonly ChaingraphTransaction[],
  { commitSeq, transactionInternalIds }: TransactionRowsContext
): EncodedRows => {
  const writer = new RowBinaryWriter(transactions.length * transactionRowBytes);
  transactions.forEach((transaction, index) => {
    writer
      .fixedString32(transaction.hash)
      .uint64(internalIdAt(transactionInternalIds, index))
      .int32(transaction.version)
      .uint32(transaction.locktime)
      .uint32(transaction.sizeBytes)
      .bool(transaction.isCoinbase)
      .uint32(transaction.inputs.length)
      .uint32(transaction.outputs.length)
      .int64(transactionOutputValueSatoshis(transaction))
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

/**
 * Every transaction in the block (including any already saved), with its
 * index in the block. `transactionInternalIds` is indexed like
 * `block.transactions`.
 */
export const encodeBlockTransactionRows = (
  block: ChaingraphBlock,
  blockInternalId: bigint | number,
  { commitSeq, transactionInternalIds }: TransactionRowsContext
): EncodedRows => {
  const writer = new RowBinaryWriter(
    block.transactions.length * blockTransactionRowBytes
  );
  block.transactions.forEach((transaction, index) => {
    writer
      .uint64(blockInternalId)
      .uint32(index)
      .uint64(internalIdAt(transactionInternalIds, index))
      .fixedString32(transaction.hash)
      .uint64(commitSeq)
      .endRow();
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

export const encodeOutputRows = (
  transactions: readonly ChaingraphTransaction[],
  { commitSeq, transactionInternalIds }: TransactionRowsContext
): EncodedRows => {
  const expectedBytes = transactions.reduce(
    (total, transaction) =>
      transaction.outputs.reduce(
        (sum, output) =>
          sum +
          outputRowFixedBytes +
          (output.lockingBytecode.length +
            (output.nonfungibleTokenCommitment?.length ?? 0)) /
            hexCharsPerByte,
        total
      ),
    0
  );
  const writer = new RowBinaryWriter(expectedBytes);
  transactions.forEach((transaction, index) => {
    const transactionInternalId = internalIdAt(transactionInternalIds, index);
    transaction.outputs.forEach((output, outputIndex) => {
      writer
        .fixedString32(transaction.hash)
        .uint32(outputIndex)
        .uint64(transactionInternalId)
        .int64(output.valueSatoshis)
        .hexBytes(output.lockingBytecode);
      writeTokenCategory(writer, output.tokenCategory).nullable(
        output.fungibleTokenAmount,
        (w, amount) => w.int64(amount)
      );
      writeNonfungibleToken(writer, output).uint64(commitSeq).endRow();
    });
  });
  return { data: writer.finish(), rowCount: writer.rowCount };
};

/**
 * Input rows carry the attributes of the output they spend, from
 * `resolveSpentOutput` (the caller's lookup over this save's own outputs and
 * the store). Coinbase inputs spend nothing: value 0, no token, empty
 * locking bytecode. Unresolved inputs get no row and are returned in
 * `pending`.
 */
export const encodeInputRows = (
  transactions: readonly ChaingraphTransaction[],
  { commitSeq, transactionInternalIds }: TransactionRowsContext,
  resolveSpentOutput: ResolveSpentOutput
): EncodedInputRows => {
  const expectedBytes = transactions.reduce(
    (total, transaction) =>
      transaction.inputs.reduce(
        (sum, input) =>
          sum +
          inputRowFixedBytes +
          input.unlockingBytecode.length / hexCharsPerByte,
        total
      ),
    0
  );
  const writer = new RowBinaryWriter(expectedBytes);
  const pending: PendingInput[] = [];
  const coinbaseSpentOutput: SpentOutput = {
    lockingBytecode: '',
    valueSatoshis: 0n,
  };
  transactions.forEach((transaction, index) => {
    const transactionInternalId = internalIdAt(transactionInternalIds, index);
    transaction.inputs.forEach((input, inputIndex) => {
      const spent = transaction.isCoinbase
        ? coinbaseSpentOutput
        : resolveSpentOutput(
            input.outpointTransactionHash,
            input.outpointIndex
          );
      if (spent === undefined) {
        pending.push({ inputIndex, transactionHash: transaction.hash });
        return;
      }
      writer
        .fixedString32(transaction.hash)
        .uint32(inputIndex)
        .uint64(transactionInternalId)
        .fixedString32(input.outpointTransactionHash)
        .uint32(input.outpointIndex)
        .uint32(input.sequenceNumber)
        .hexBytes(input.unlockingBytecode)
        .int64(spent.valueSatoshis);
      writeTokenCategory(writer, spent.tokenCategory);
      writeNonfungibleToken(writer, spent)
        .hexBytes(spent.lockingBytecode)
        .uint64(commitSeq)
        .endRow();
    });
  });
  return { data: writer.finish(), pending, rowCount: writer.rowCount };
};
