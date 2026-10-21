/* eslint-disable @typescript-eslint/no-magic-numbers */
import test from 'ava';

import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

import {
  copyStageTableSql,
  createStageTablesSql,
  encodeStageBlockTransactions,
  encodeStageInputs,
  encodeStageOutputs,
  encodeStageTransactions,
  stageTableColumns,
} from './block-copy-rows.js';
import { decodeBinaryCopy } from './pg-binary-copy.js';

const hash = (byte: string) => byte.repeat(32);
const int8 = (field: Buffer | null | undefined) => field!.readBigInt64BE(0);
const hex = (field: Buffer | null | undefined) => field!.toString('hex');

const coinbase: ChaingraphTransaction = {
  hash: hash('aa'),
  inputs: [
    {
      outpointIndex: 0xffffffff,
      outpointTransactionHash: hash('00'),
      sequenceNumber: 0xffffffff,
      unlockingBytecode: '03a0bb0d',
    },
  ],
  isCoinbase: true,
  locktime: 0,
  outputs: [{ lockingBytecode: '', valueSatoshis: 625000000n }],
  sizeBytes: 100,
  version: 1,
};

const tokenTransaction: ChaingraphTransaction = {
  hash: hash('bb'),
  inputs: [
    {
      outpointIndex: 1,
      outpointTransactionHash: hash('cc'),
      sequenceNumber: 0,
      unlockingBytecode: '',
    },
    {
      outpointIndex: 0,
      outpointTransactionHash: hash('dd'),
      sequenceNumber: 0xfffffffe,
      unlockingBytecode: '0051',
    },
  ],
  isCoinbase: false,
  locktime: 500000000,
  outputs: [
    {
      fungibleTokenAmount: 9223372036854775807n,
      lockingBytecode: '76a914',
      nonfungibleTokenCapability: 'minting',
      nonfungibleTokenCommitment: '',
      tokenCategory: hash('ee'),
      valueSatoshis: 1000n,
    },
    {
      lockingBytecode: '6a',
      nonfungibleTokenCapability: 'none',
      nonfungibleTokenCommitment: '01020304',
      tokenCategory: hash('ff'),
      valueSatoshis: 0n,
    },
    {
      fungibleTokenAmount: 1n,
      lockingBytecode: 'aa20',
      tokenCategory: hash('11'),
      valueSatoshis: 800n,
    },
  ],
  sizeBytes: 300,
  version: 2,
};

test('encodeStageTransactions: one row per transaction in column order', (t) => {
  const rows = decodeBinaryCopy(
    encodeStageTransactions([coinbase, tokenTransaction])
  );
  t.is(rows.length, 2);
  t.is(rows[0]!.length, stageTableColumns.chaingraph_stage_transaction.length);
  t.deepEqual(
    rows.map((row) => [
      hex(row[0]),
      int8(row[1]),
      int8(row[2]),
      int8(row[3]),
      row[4]![0],
    ]),
    [
      [hash('aa'), 1n, 0n, 100n, 1],
      [hash('bb'), 2n, 500000000n, 300n, 0],
    ]
  );
});

test('encodeStageInputs: indexes, uint32 sequence numbers and empty bytecode', (t) => {
  const rows = decodeBinaryCopy(
    encodeStageInputs([coinbase, tokenTransaction])
  );
  t.deepEqual(
    rows.map((row) => [
      hex(row[0]),
      int8(row[1]),
      int8(row[2]),
      int8(row[3]),
      hex(row[4]),
      hex(row[5]),
    ]),
    [
      [hash('aa'), 0n, 4294967295n, 4294967295n, hash('00'), '03a0bb0d'],
      [hash('bb'), 0n, 1n, 0n, hash('cc'), ''],
      [hash('bb'), 1n, 0n, 4294967294n, hash('dd'), '0051'],
    ]
  );
});

test('encodeStageOutputs: token fields, enum labels and NULLs', (t) => {
  const rows = decodeBinaryCopy(
    encodeStageOutputs([coinbase, tokenTransaction])
  );
  t.is(rows[0]!.length, stageTableColumns.chaingraph_stage_output.length);
  t.deepEqual(
    rows.map((row) => [
      hex(row[0]),
      int8(row[1]),
      int8(row[2]),
      hex(row[3]),
      row[4] === null ? null : hex(row[4]),
      row[5] === null ? null : int8(row[5]),
      row[6] === null ? null : row[6]!.toString('utf8'),
      row[7] === null ? null : hex(row[7]),
    ]),
    [
      [hash('aa'), 0n, 625000000n, '', null, null, null, null],
      [
        hash('bb'),
        0n,
        1000n,
        '76a914',
        hash('ee'),
        9223372036854775807n,
        'minting',
        '',
      ],
      [hash('bb'), 1n, 0n, '6a', hash('ff'), null, 'none', '01020304'],
      [hash('bb'), 2n, 800n, 'aa20', hash('11'), 1n, null, null],
    ]
  );
});

test('encodeStage*: empty transaction lists encode zero rows', (t) => {
  t.deepEqual(decodeBinaryCopy(encodeStageTransactions([])), []);
  t.deepEqual(decodeBinaryCopy(encodeStageInputs([])), []);
  t.deepEqual(decodeBinaryCopy(encodeStageOutputs([])), []);
});

test('encodeStageBlockTransactions: every transaction with its block index', (t) => {
  const block = {
    transactions: [coinbase, tokenTransaction],
  } as unknown as ChaingraphBlock;
  const rows = decodeBinaryCopy(encodeStageBlockTransactions(block));
  t.deepEqual(
    rows.map((row) => [hex(row[0]), int8(row[1])]),
    [
      [hash('aa'), 0n],
      [hash('bb'), 1n],
    ]
  );
});

test('stage table SQL', (t) => {
  t.is(
    copyStageTableSql('chaingraph_stage_block_transaction'),
    'COPY pg_temp.chaingraph_stage_block_transaction (hash, transaction_index) FROM STDIN (FORMAT binary)'
  );
  t.regex(
    createStageTablesSql,
    /CREATE TEMP TABLE IF NOT EXISTS chaingraph_stage_output \(transaction_hash bytea, .*nonfungible_token_capability enum_nonfungible_token_capability, nonfungible_token_commitment bytea\) ON COMMIT DELETE ROWS;/u
  );
  t.is(createStageTablesSql.split('\n').length, 4);
});
