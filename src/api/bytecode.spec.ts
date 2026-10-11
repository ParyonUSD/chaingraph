/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, camelcase */
// cspell:ignore bytecode pushdata
/**
 * R27/R28 (unit): the TypeScript port of Chaingraph v1's bytecode pattern
 * functions against the vectors shared with the Postgres macro tests, and
 * the P2P encoders against libauth (transactions with CashTokens prefixes,
 * block headers): an encoding hashes to its transaction/block hash.
 */
import { createHash } from 'node:crypto';

import {
  binToHex,
  encodeTransactionBCH,
  hexToBin,
  swapEndianness,
} from '@bitauth/libauth';
import test from 'ava';

import { patternVectors } from './bytecode-pattern.vectors.js';
import {
  compactUint,
  encodeBlock,
  encodeBlockHeader,
  encodeTransaction,
  parseBytecodePattern,
  parseBytecodePatternRedeem,
  parseBytecodePatternWithPushdataLengths,
} from './bytecode.js';

const functions = {
  parse_bytecode_pattern: parseBytecodePattern,
  parse_bytecode_pattern_redeem: parseBytecodePatternRedeem,
  parse_bytecode_pattern_with_pushdata_lengths:
    parseBytecodePatternWithPushdataLengths,
};

patternVectors.forEach((vector) => {
  test(`R27 ${vector.fn}: ${vector.title}`, (t) => {
    t.is(functions[vector.fn](vector.bytecode), vector.pattern);
  });
});

test('R27: a negative OP_PUSHDATA4 length ends the pattern (no loop)', (t) => {
  t.is(parseBytecodePatternWithPushdataLengths('514e00000080'), '514e00000080');
  t.is(parseBytecodePattern('514e00000080'), '514e');
});

const sha256d = (hex: string) =>
  createHash('sha256')
    .update(createHash('sha256').update(Buffer.from(hex, 'hex')).digest())
    .digest('hex');

test('R28: compact sizes', (t) => {
  t.is(compactUint(0).toString('hex'), '00');
  t.is(compactUint(252).toString('hex'), 'fc');
  t.is(compactUint(253).toString('hex'), 'fdfd00');
  t.is(compactUint(65_536).toString('hex'), 'fe00000100');
  t.is(compactUint(2n ** 32n).toString('hex'), 'ff0000000001000000');
});

test('R28: transaction encoding equals libauth (tokens: FT, NFT with and without commitment, both)', (t) => {
  const category = 'aa'.repeat(32);
  const outputs = [
    {
      lockingBytecode: '76a914000000000000000000000000000000000000000088ac',
      value: 1000n,
    },
    { lockingBytecode: '6a', token: { amount: 5n, category }, value: 800n },
    {
      lockingBytecode: '51',
      token: {
        amount: 0n,
        category,
        nft: { capability: 'mutable' as const, commitment: '01aa' },
      },
      value: 800n,
    },
    {
      lockingBytecode: '52',
      token: {
        amount: 7n,
        category,
        nft: { capability: 'minting' as const, commitment: '' },
      },
      value: 800n,
    },
    {
      lockingBytecode: '53',
      token: {
        amount: 0n,
        category,
        nft: { capability: 'none' as const, commitment: '00'.repeat(40) },
      },
      value: 0n,
    },
  ];
  const libauth = binToHex(
    encodeTransactionBCH({
      inputs: [
        {
          outpointIndex: 3,
          outpointTransactionHash: hexToBin('bb'.repeat(32)),
          sequenceNumber: 0xfffffffe,
          unlockingBytecode: hexToBin('51'.repeat(300)),
        },
      ],
      locktime: 500_000,
      outputs: outputs.map((output) => ({
        lockingBytecode: hexToBin(output.lockingBytecode),
        ...(output.token === undefined
          ? {}
          : {
              token: {
                amount: output.token.amount,
                category: hexToBin(output.token.category),
                ...(output.token.nft === undefined
                  ? {}
                  : {
                      nft: {
                        capability: output.token.nft.capability,
                        commitment: hexToBin(output.token.nft.commitment),
                      },
                    }),
              },
            }),
        valueSatoshis: output.value,
      })),
      version: 2,
    })
  );
  const ours = encodeTransaction({
    inputs: [
      {
        outpoint_index: 3,
        /* Chaingraph stores hashes in display order (reversed) */
        outpoint_transaction_hash: swapEndianness('bb'.repeat(32)),
        sequence_number: 0xfffffffe,
        unlocking_bytecode: '51'.repeat(300),
      },
    ],
    locktime: 500_000,
    outputs: outputs.map((output) => ({
      fungible_token_amount:
        output.token === undefined ? null : output.token.amount,
      locking_bytecode: output.lockingBytecode,
      nonfungible_token_capability: output.token?.nft?.capability ?? null,
      nonfungible_token_commitment: output.token?.nft?.commitment ?? null,
      /* display order, like every hash Chaingraph stores */
      token_category:
        output.token === undefined
          ? null
          : swapEndianness(output.token.category),
      value_satoshis: output.value,
    })),
    version: 2,
  });
  t.is(ours, libauth);
});

test('R28: block header and block encodings hash to the block hash (genesis)', (t) => {
  const header = {
    bits: 486_604_799,
    merkle_root:
      '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b',
    nonce: 2_083_236_893,
    previous_block_hash: '00'.repeat(32),
    timestamp: 1_231_006_505,
    version: 1,
  };
  const encoded = encodeBlockHeader(header);
  t.is(encoded.length, 160);
  t.is(
    swapEndianness(sha256d(encoded)),
    '000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f'
  );
  const coinbase = encodeTransaction({
    inputs: [
      {
        outpoint_index: 4_294_967_295,
        outpoint_transaction_hash: '00'.repeat(32),
        sequence_number: 4_294_967_295,
        unlocking_bytecode:
          '04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73',
      },
    ],
    locktime: 0,
    outputs: [
      {
        fungible_token_amount: null,
        locking_bytecode:
          '4104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac',
        nonfungible_token_capability: null,
        nonfungible_token_commitment: null,
        token_category: null,
        value_satoshis: 5_000_000_000n,
      },
    ],
    version: 1,
  });
  t.is(
    swapEndianness(sha256d(coinbase)),
    '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
  );
  t.is(encodeBlock(header, [coinbase]), `${encoded}01${coinbase}`);
});
