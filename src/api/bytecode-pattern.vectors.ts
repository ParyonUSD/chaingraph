 
// cspell:ignore pushdata bytecode memo
/**
 * Bytecode pattern vectors shared by the Postgres macro tests of Chaingraph
 * v1's functions (src/e2e/e2e.spec.ts, `[postgres]`) and the TypeScript port
 * of the v2 API (bytecode.spec.ts). `pattern: null` = the function returns
 * NULL.
 */
import {
  binToHex,
  encodeDataPush,
  flattenBinArray,
  hexToBin,
} from '@bitauth/libauth';

export type PatternFunction =
  'parse_bytecode_pattern_redeem' | 'parse_bytecode_pattern_with_pushdata_lengths' | 'parse_bytecode_pattern';

export interface PatternVector {
  title: string;
  fn: PatternFunction;
  bytecode: string;
  pattern: string | null;
}

const allOnes = 0x11;
const minPushData1 = 76;
const minPushData2 = 256;
const minPushData4 = 65536;

const pushOf = (length: number) =>
  binToHex(encodeDataPush(new Uint8Array(length).fill(allOnes)));

const redeemPush = (length: number) =>
  `00020000${binToHex(
    encodeDataPush(
      flattenBinArray([
        hexToBin('00'),
        encodeDataPush(new Uint8Array(length).fill(allOnes)),
        hexToBin('515253'),
      ])
    )
  )}`;

const p2pkh = '76a914000000000000000000000000000000000000000088ac';
const p2sh = 'a914000000000000000000000000000000000000000087';
const fixedPushes =
  '6a04000000005120000000000000000000000000000000000000000000000000000000000000000004000000000400000000';
const memo =
  '6a026d0c090000000000000000004c5c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000';

export const patternVectors: readonly PatternVector[] = [
  {
    bytecode: p2pkh,
    fn: 'parse_bytecode_pattern',
    pattern: '76a91488ac',
    title: 'P2PKH',
  },
  {
    bytecode: p2sh,
    fn: 'parse_bytecode_pattern',
    pattern: 'a91487',
    title: 'P2SH',
  },
  {
    bytecode: fixedPushes,
    fn: 'parse_bytecode_pattern',
    pattern: '6a0451200404',
    title: 'OP_RETURN (fixed pushes)',
  },
  {
    bytecode: memo,
    fn: 'parse_bytecode_pattern',
    pattern: '6a02094c',
    title: 'OP_RETURN with OP_PUSHDATA1',
  },
  {
    bytecode: `6a${pushOf(minPushData2)}515151`,
    fn: 'parse_bytecode_pattern',
    pattern: '6a4d515151',
    title: 'OP_RETURN with OP_PUSHDATA2',
  },
  {
    bytecode: `6a${pushOf(minPushData4)}515151`,
    fn: 'parse_bytecode_pattern',
    pattern: '6a4e515151',
    title: 'OP_RETURN with OP_PUSHDATA4',
  },
  {
    bytecode: '515102',
    fn: 'parse_bytecode_pattern',
    pattern: '515102',
    title: 'malformed OP_PUSHBYTES',
  },
  {
    bytecode: '51514c',
    fn: 'parse_bytecode_pattern',
    pattern: '51514c',
    title: 'malformed OP_PUSHDATA1',
  },
  {
    bytecode: '51514d11',
    fn: 'parse_bytecode_pattern',
    pattern: '51514d',
    title: 'malformed OP_PUSHDATA2',
  },
  {
    bytecode: '51514e112233',
    fn: 'parse_bytecode_pattern',
    pattern: '51514e',
    title: 'malformed OP_PUSHDATA4',
  },

  {
    bytecode: p2pkh,
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '76a91488ac',
    title: 'P2PKH',
  },
  {
    bytecode: p2sh,
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: 'a91487',
    title: 'P2SH',
  },
  {
    bytecode: fixedPushes,
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '6a0451200404',
    title: 'OP_RETURN (fixed pushes)',
  },
  {
    bytecode: memo,
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '6a02094c5c',
    title: 'OP_RETURN with OP_PUSHDATA1 (memo.cash)',
  },
  {
    bytecode: '4c00',
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '4c00',
    title: 'zero-length OP_PUSHDATA1',
  },
  {
    bytecode: '4d0000',
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '4d0000',
    title: 'zero-length OP_PUSHDATA2',
  },
  {
    bytecode: '4e00000000',
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '4e00000000',
    title: 'zero-length OP_PUSHDATA4',
  },
  {
    bytecode: `6a${pushOf(minPushData2)}515151`,
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '6a4d0001515151',
    title: 'OP_RETURN with OP_PUSHDATA2',
  },
  {
    bytecode: `6a${pushOf(minPushData4)}515151`,
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '6a4e00000100515151',
    title: 'OP_RETURN with OP_PUSHDATA4',
  },
  {
    bytecode: '515102',
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '515102',
    title: 'malformed OP_PUSHBYTES',
  },
  {
    bytecode: '51514c',
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '51514c',
    title: 'malformed OP_PUSHDATA1',
  },
  {
    bytecode: '51514d11',
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '51514d',
    title: 'malformed OP_PUSHDATA2',
  },
  {
    bytecode: '51514e112233',
    fn: 'parse_bytecode_pattern_with_pushdata_lengths',
    pattern: '51514e',
    title: 'malformed OP_PUSHDATA4',
  },

  {
    bytecode: '0002000051',
    fn: 'parse_bytecode_pattern_redeem',
    pattern: null,
    title: 'no redeem',
  },
  {
    bytecode: '0003019951',
    fn: 'parse_bytecode_pattern_redeem',
    pattern: '0151',
    title: 'OP_PUSHBYTES redeem',
  },
  {
    bytecode: redeemPush(minPushData1),
    fn: 'parse_bytecode_pattern_redeem',
    pattern: '004c515253',
    title: 'OP_PUSHDATA1 redeem',
  },
  {
    bytecode: redeemPush(minPushData2),
    fn: 'parse_bytecode_pattern_redeem',
    pattern: '004d515253',
    title: 'OP_PUSHDATA2 redeem',
  },
  {
    bytecode: redeemPush(minPushData4),
    fn: 'parse_bytecode_pattern_redeem',
    pattern: '004e515253',
    title: 'OP_PUSHDATA4 redeem',
  },
  {
    bytecode: '4c',
    fn: 'parse_bytecode_pattern_redeem',
    pattern: null,
    title: 'malformed OP_PUSHDATA1 redeem',
  },
  {
    bytecode: '4d11',
    fn: 'parse_bytecode_pattern_redeem',
    pattern: null,
    title: 'malformed OP_PUSHDATA2 redeem',
  },
  {
    bytecode: '4e112233',
    fn: 'parse_bytecode_pattern_redeem',
    pattern: null,
    title: 'malformed OP_PUSHDATA4 redeem',
  },
  {
    bytecode: '4effffffff',
    fn: 'parse_bytecode_pattern_redeem',
    pattern: null,
    title: 'oversized OP_PUSHDATA4 redeem',
  },
];
