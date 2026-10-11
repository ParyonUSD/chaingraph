/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-try-statement */
// cspell:ignore clickhouse
import test from 'ava';

import { bitcoreBlockToChaingraphBlock } from '../bitcore.js';
import { Block, generateMockchain } from '../e2e/e2e.spec.mockchain.helper.js';
import {
  encodeBlockTransactionRows,
  encodeInputRows,
  encodeOutputRows,
  encodeTransactionRows,
} from '../store/clickhouse/row-encoders.js';
import {
  assembleBlockTransactionRows,
  assembleTransactionRows,
  assembleVariableRows,
  blockRowTemplates,
  encodeBlockRowTemplates,
  templateAt,
} from '../store/clickhouse/row-templates.js';
import type { ChaingraphBlock } from '../types/chaingraph.js';

import { BlockWorkerPool } from './block-workers.js';

const zeroHash = '00'.repeat(32);
const blocks = generateMockchain({ length: 40, previousBlockHash: zeroHash });
const raws = blocks.map((block) => Buffer.from(block.toBuffer()));
/**
 * bitcore's parse reverses token categories in the buffer it reads, so every
 * parse here gets a fresh copy (the pool copies what it sends to a worker).
 */
const onMainThread = (height: number) =>
  bitcoreBlockToChaingraphBlock(
    Block.fromBuffer(Buffer.from(raws[height]!)),
    height
  );

/** Every second transaction of every block, with ids far above 2^32. */
const selection = (converted: readonly ChaingraphBlock[]) =>
  converted.flatMap((block) =>
    block.transactions
      .map((transaction, index) => ({ block, index, transaction }))
      .filter(({ index }) => index % 2 === 0 || block.transactions.length < 3)
  );
const idOf = (position: number) => 0x1_0000_0000n * 7n + BigInt(position);
const seq = 0x0000_0100_0000_0042n;

const refOf = (block: ChaingraphBlock, index: number) =>
  templateAt(blockRowTemplates.get(block)!, index)!;

test('a block worker converts exactly as the main thread does, and its templates give the encoders’ bytes (blocks split in parts)', async (t) => {
  const pool = new BlockWorkerPool({
    partTransactions: 2,
    rowTemplates: true,
    size: 3,
    sliceTransactions: 3,
  });
  try {
    const converted = await Promise.all(
      raws.map(async (raw, height) => pool.convert(raw, height))
    );
    converted.forEach((block, height) => {
      t.deepEqual(block, onMainThread(height));
      t.true(blockRowTemplates.has(block));
    });
    t.is(pool.stats.blocks, raws.length);
    t.true(pool.stats.parts > raws.length);
    t.true(converted.some((block) => block.transactions.length > 3));
    t.true(
      converted.some((block) =>
        block.transactions.some((transaction) =>
          transaction.outputs.some(
            (output) => output.nonfungibleTokenCommitment !== undefined
          )
        )
      )
    );

    const picked = selection(converted);
    const ids = picked.map((_, position) => idOf(position));
    const transactions = picked.map(({ transaction }) => transaction);
    const context = { commitSeq: seq, transactionInternalIds: ids };
    const templated = picked.map(({ block, index }, position) => ({
      ...refOf(block, index),
      internalId: ids[position]!,
    }));
    const same = (a: { data: Uint8Array; rowCount: number }, b: typeof a) => {
      t.is(a.rowCount, b.rowCount);
      t.true(Buffer.from(a.data).equals(Buffer.from(b.data)));
    };
    same(
      assembleVariableRows('output', templated, seq),
      encodeOutputRows(transactions, context)
    );
    same(
      assembleVariableRows('input', templated, seq),
      encodeInputRows(transactions, context)
    );
    same(
      assembleTransactionRows(templated, seq),
      encodeTransactionRows(transactions, context)
    );
    const block = converted[converted.length - 1]!;
    const blockIds = block.transactions.map((_, index) => idOf(index + 1000));
    same(
      assembleBlockTransactionRows(
        block.transactions.map((_, index) => ({
          ...refOf(block, index),
          blockInternalId: 99n,
          internalId: blockIds[index]!,
        })),
        seq
      ),
      encodeBlockTransactionRows(block, 99n, {
        commitSeq: seq,
        transactionInternalIds: blockIds,
      })
    );
  } finally {
    await pool.close();
  }
});

test('templates encoded on the main thread match too (numbers as ids)', (t) => {
  const block = onMainThread(7);
  const { templates } = encodeBlockRowTemplates(block).parts[0]!;
  const ids = block.transactions.map((_, index) => 1_000_000 + index);
  const context = { commitSeq: 5, transactionInternalIds: ids };
  const templated = block.transactions.map((_, index) => ({
    index,
    internalId: ids[index]!,
    templates,
  }));
  t.true(
    Buffer.from(assembleVariableRows('output', templated, 5).data).equals(
      Buffer.from(encodeOutputRows(block.transactions, context).data)
    )
  );
  t.true(
    Buffer.from(assembleVariableRows('input', templated, 5).data).equals(
      Buffer.from(encodeInputRows(block.transactions, context).data)
    )
  );
  t.is(assembleVariableRows('output', [], 5).rowCount, 0);
});

test('a malformed block is rejected and the worker keeps working', async (t) => {
  const pool = new BlockWorkerPool({ rowTemplates: false, size: 1 });
  try {
    await t.throwsAsync(pool.convert(raws[3]!.subarray(0, 90), 3));
    const block = await pool.convert(raws[3]!, 3);
    t.deepEqual(block, onMainThread(3));
    t.false(blockRowTemplates.has(block));
  } finally {
    await pool.close();
  }
});
