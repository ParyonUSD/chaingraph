/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-let, functional/no-loop-statement, complexity */
// cspell:ignore clickhouse
/**
 * Row templates: the RowBinary rows of a block's node-agnostic tables
 * (`output`, `input`, `transaction`, `block_transaction`), encoded off the
 * main thread (a block worker, `components/block-workers.ts`) before the
 * rows' ids are known, with zero placeholders for every id and for
 * `commit_seq`. A commit copies the rows it inserts and patches the
 * placeholders (fixed-width `UInt64`s at known offsets), so the bytes are
 * exactly those of `row-encoders.ts` (the spec checks this) and the main
 * thread does two 8-byte writes per row instead of encoding it
 * (docs/clickhouse-port/pre-phase2-concurrency-mempool.md §1).
 *
 * Templates belong to a `ChaingraphBlock` object (`blockRowTemplates`, a
 * WeakMap, so they live exactly as long as the block). A block without
 * templates (genesis, mempool, tests, workers off) is encoded as before.
 */
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import { RowBinaryWriter } from './row-binary.js';
import type { EncodedRows } from './row-encoders.js';
import {
  encodeInputRows,
  encodeOutputRows,
  encodeTransactionRows,
} from './row-encoders.js';

/** A table's template rows: the bytes and the end offset of each row. */
export interface VariableRowTemplate {
  data: Uint8Array;
  rowEnds: Uint32Array;
}

export interface BlockRowTemplates {
  output: VariableRowTemplate;
  input: VariableRowTemplate;
  /** Fixed `transactionRowBytes`-byte rows, one per transaction. */
  transaction: Uint8Array;
  /** Fixed `blockTransactionRowBytes`-byte rows, one per transaction. */
  blockTransaction: Uint8Array;
  /** Per transaction, its first `output` row (length: transactions + 1). */
  outputRowStart: Uint32Array;
  /** Per transaction, its first `input` row (length: transactions + 1). */
  inputRowStart: Uint32Array;
}

/** `transaction` row: hash, internal_id, …, commit_seq (row-encoders.ts). */
export const transactionRowBytes = 32 + 8 + 4 + 4 + 4 + 1 + 4 + 4 + 8 + 8;
/** `block_transaction` row: block id, index, transaction id, hash, commit_seq. */
export const blockTransactionRowBytes = 8 + 4 + 8 + 32 + 8;
/** `transaction_internal_id` of an `output` / `input` row: after hash and index. */
const idOffsetInOutputOrInputRow = 32 + 4;
const transactionIdOffset = 32;
const blockTransactionBlockIdOffset = 0;
const blockTransactionTransactionIdOffset = 8 + 4;
const uint64Bytes = 8;

/**
 * The templates of a block, in parts: a large block is converted by several
 * workers, each taking a range of its transactions (`first` is the block
 * index of the part's first transaction; parts are in block order).
 */
export interface BlockTemplateParts {
  parts: { first: number; count: number; templates: BlockRowTemplates }[];
}

/** Templates of the blocks converted by block workers. */
export const blockRowTemplates = new WeakMap<
  ChaingraphBlock,
  BlockTemplateParts
>();

/** The template part holding transaction `index` of `block`, if any. */
export const templateAt = (
  parts: BlockTemplateParts,
  index: number
): { templates: BlockRowTemplates; index: number } | undefined => {
  for (const part of parts.parts) {
    if (index >= part.first && index < part.first + part.count) {
      return { index: index - part.first, templates: part.templates };
    }
  }
  return undefined;
};

/** A copy in an ArrayBuffer of its own (transferable; never a pool slice). */
const ownCopy = (bytes: Uint8Array) => {
  const copy = new Uint8Array(bytes.length);
  copy.set(bytes);
  return copy;
};

const rowStarts = (counts: readonly number[]) => {
  const starts = new Uint32Array(counts.length + 1);
  counts.forEach((count, index) => {
    starts[index + 1] = starts[index]! + count;
  });
  return starts;
};

/**
 * Encode the templates of `transactions`, block indexes `first` onwards
 * (placeholders: every id and seq is 0).
 */
export const encodeTransactionRowTemplates = (
  transactions: readonly ChaingraphTransaction[],
  first = 0
): BlockRowTemplates => {
  const zeros = new Array<number>(transactions.length).fill(0);
  const outputEnds: number[] = [];
  const inputEnds: number[] = [];
  const output = encodeOutputRows(transactions, {
    commitSeq: 0,
    rowEnds: outputEnds,
    transactionInternalIds: zeros,
  });
  const input = encodeInputRows(transactions, {
    commitSeq: 0,
    rowEnds: inputEnds,
    transactionInternalIds: zeros,
  });
  const transaction = encodeTransactionRows(transactions, {
    commitSeq: 0,
    transactionInternalIds: zeros,
  });
  /* as encodeBlockTransactionRows, with the block index of each transaction */
  const blockTransaction = new RowBinaryWriter(
    transactions.length * blockTransactionRowBytes
  );
  transactions.forEach((tx, index) => {
    blockTransaction
      .uint64(0)
      .uint32(first + index)
      .uint64(0)
      .fixedString32(tx.hash)
      .uint64(0)
      .endRow();
  });
  /* own copies (the writers' buffers may be larger), transferable */
  return {
    blockTransaction: ownCopy(blockTransaction.finish()),
    input: {
      data: ownCopy(input.data),
      rowEnds: Uint32Array.from(inputEnds),
    },
    inputRowStart: rowStarts(transactions.map((tx) => tx.inputs.length)),
    output: {
      data: ownCopy(output.data),
      rowEnds: Uint32Array.from(outputEnds),
    },
    outputRowStart: rowStarts(transactions.map((tx) => tx.outputs.length)),
    transaction: ownCopy(transaction.data),
  };
};

/** Templates of a whole block, as one part (tests, main-thread use). */
export const encodeBlockRowTemplates = (
  block: ChaingraphBlock
): BlockTemplateParts => ({
  parts: [
    {
      count: block.transactions.length,
      first: 0,
      templates: encodeTransactionRowTemplates(block.transactions),
    },
  ],
});

/** The ArrayBuffers of `templates` (a worker transfers them). */
export const templateBuffers = (templates: BlockRowTemplates) => [
  templates.output.data.buffer,
  templates.output.rowEnds.buffer,
  templates.input.data.buffer,
  templates.input.rowEnds.buffer,
  templates.transaction.buffer,
  templates.blockTransaction.buffer,
  templates.outputRowStart.buffer,
  templates.inputRowStart.buffer,
];

/** One transaction (by index) of a templated block. */
export interface TemplatedTransaction {
  templates: BlockRowTemplates;
  index: number;
  internalId: bigint | number;
}

const asBigint = (value: bigint | number) =>
  typeof value === 'bigint' ? value : BigInt(value);

/** A contiguous byte range of one template, rows `[firstRow, endRow)`. */
interface Run {
  template: VariableRowTemplate;
  firstRow: number;
  endRow: number;
  /** Per row of the run, the id to write. */
  ids: bigint[];
}

const rowStartOf = (template: VariableRowTemplate, row: number) =>
  row === 0 ? 0 : template.rowEnds[row - 1]!;

/**
 * The `output` or `input` rows of `transactions` (in that order), patched
 * with each transaction's id and `commitSeq`: the bytes `encodeOutputRows` /
 * `encodeInputRows` give for the same transactions.
 */
export const assembleVariableRows = (
  table: 'input' | 'output',
  transactions: readonly TemplatedTransaction[],
  commitSeq: bigint | number
): EncodedRows => {
  const runs: Run[] = [];
  let bytes = 0;
  let rowCount = 0;
  transactions.forEach(({ index, internalId, templates }) => {
    const template = templates[table];
    const starts =
      table === 'output' ? templates.outputRowStart : templates.inputRowStart;
    const firstRow = starts[index]!;
    const endRow = starts[index + 1]!;
    if (endRow === firstRow) return;
    const id = asBigint(internalId);
    const last = runs[runs.length - 1];
    const run =
      last !== undefined &&
      last.template === template &&
      last.endRow === firstRow
        ? last
        : undefined;
    if (run === undefined) {
      runs.push({ endRow, firstRow, ids: [], template });
    } else {
      run.endRow = endRow;
    }
    const target = runs[runs.length - 1]!;
    for (let row = firstRow; row < endRow; row += 1) target.ids.push(id);
    bytes += rowStartOf(template, endRow) - rowStartOf(template, firstRow);
    rowCount += endRow - firstRow;
  });
  const data = Buffer.allocUnsafe(bytes);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const seq = asBigint(commitSeq);
  let position = 0;
  runs.forEach(({ endRow, firstRow, ids, template }) => {
    const runStart = rowStartOf(template, firstRow);
    const runEnd = rowStartOf(template, endRow);
    data.set(template.data.subarray(runStart, runEnd), position);
    for (let row = firstRow; row < endRow; row += 1) {
      const rowStart = position + rowStartOf(template, row) - runStart;
      const rowEnd = position + template.rowEnds[row]! - runStart;
      view.setBigUint64(
        rowStart + idOffsetInOutputOrInputRow,
        ids[row - firstRow]!,
        true
      );
      view.setBigUint64(rowEnd - uint64Bytes, seq, true);
    }
    position += runEnd - runStart;
  });
  return { data, rowCount };
};

/**
 * The `transaction` rows of `transactions` (in that order), patched: the
 * bytes `encodeTransactionRows` gives.
 */
export const assembleTransactionRows = (
  transactions: readonly TemplatedTransaction[],
  commitSeq: bigint | number
): EncodedRows => {
  const data = Buffer.allocUnsafe(transactions.length * transactionRowBytes);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const seq = asBigint(commitSeq);
  transactions.forEach(({ index, internalId, templates }, row) => {
    const position = row * transactionRowBytes;
    const source = index * transactionRowBytes;
    data.set(
      templates.transaction.subarray(source, source + transactionRowBytes),
      position
    );
    view.setBigUint64(
      position + transactionIdOffset,
      asBigint(internalId),
      true
    );
    view.setBigUint64(position + transactionRowBytes - uint64Bytes, seq, true);
  });
  return { data, rowCount: transactions.length };
};

/**
 * `block_transaction` rows for the given transaction indexes of one
 * templated block (in that order), patched with the block's id, each
 * transaction's id and `commitSeq`.
 */
export const assembleBlockTransactionRows = (
  rows: readonly {
    templates: BlockRowTemplates;
    blockInternalId: bigint | number;
    index: number;
    internalId: bigint | number;
  }[],
  commitSeq: bigint | number
): EncodedRows => {
  const data = Buffer.allocUnsafe(rows.length * blockTransactionRowBytes);
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const seq = asBigint(commitSeq);
  rows.forEach(({ blockInternalId, index, internalId, templates }, row) => {
    const position = row * blockTransactionRowBytes;
    const source = index * blockTransactionRowBytes;
    data.set(
      templates.blockTransaction.subarray(
        source,
        source + blockTransactionRowBytes
      ),
      position
    );
    view.setBigUint64(
      position + blockTransactionBlockIdOffset,
      asBigint(blockInternalId),
      true
    );
    view.setBigUint64(
      position + blockTransactionTransactionIdOffset,
      asBigint(internalId),
      true
    );
    view.setBigUint64(
      position + blockTransactionRowBytes - uint64Bytes,
      seq,
      true
    );
  });
  return { data, rowCount: rows.length };
};
