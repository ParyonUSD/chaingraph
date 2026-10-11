/* eslint-disable @typescript-eslint/no-magic-numbers, no-bitwise, functional/no-let, functional/no-loop-statement, @typescript-eslint/naming-convention, complexity */
// cspell:ignore pushdata bytecode
/**
 * Computed byte fields of the v2 API, ported from Chaingraph v1's Postgres
 * functions (images/hasura/hasura-data/migrations, latest versions):
 * - `parse_bytecode_pattern`, `parse_bytecode_pattern_with_pushdata_lengths`,
 *   `parse_bytecode_pattern_redeem` (R27; vectors in
 *   bytecode-pattern.vectors.ts, shared with the Postgres macro tests);
 * - `encode_transaction` (with the CashTokens prefix), `encode_block_header`,
 *   `encode_block` (R28).
 * All take and return lowercase hex.
 */

const toBytes = (hex: string) => Buffer.from(hex, 'hex');
const toHex = (bytes: Uint8Array) => Buffer.from(bytes).toString('hex');

/**
 * The first byte of each instruction (pushed data excluded). Malformed
 * pushes end the pattern at their opcode, as v1 does.
 */
export const parseBytecodePattern = (bytecodeHex: string): string => {
  const bytecode = toBytes(bytecodeHex);
  const pattern: number[] = [];
  const { length } = bytecode;
  let i = 0;
  while (i < length) {
    const selected = bytecode[i]!;
    pattern.push(selected);
    if (selected > 78 || selected === 0) {
      i += 1;
    } else if (selected <= 75) {
      i += 1 + selected;
    } else if (selected === 76) {
      if (length - i < 2) return toHex(Uint8Array.from(pattern));
      i += 2 + bytecode[i + 1]!;
    } else if (selected === 77) {
      if (length - i < 3) return toHex(Uint8Array.from(pattern));
      i += 3 + (bytecode[i + 1]! | (bytecode[i + 2]! << 8));
    } else {
      if (length - i < 5) return toHex(Uint8Array.from(pattern));
      /* v1: a length above the maximum signed int (> 2 GB) ends the pattern */
      if (bytecode[i + 4]! >= 0x80) return toHex(Uint8Array.from(pattern));
      i +=
        5 +
        (bytecode[i + 1]! |
          (bytecode[i + 2]! << 8) |
          (bytecode[i + 3]! << 16) |
          (bytecode[i + 4]! << 24));
    }
  }
  return toHex(Uint8Array.from(pattern));
};

/** As `parseBytecodePattern`, keeping the length bytes of OP_PUSHDATA_1/2/4. */
export const parseBytecodePatternWithPushdataLengths = (
  bytecodeHex: string
): string => {
  const bytecode = toBytes(bytecodeHex);
  const pattern: number[] = [];
  const { length } = bytecode;
  let i = 0;
  while (i < length) {
    const selected = bytecode[i]!;
    pattern.push(selected);
    if (selected > 78 || selected === 0) {
      i += 1;
    } else if (selected <= 75) {
      i += 1 + selected;
    } else if (selected === 76) {
      if (length - i < 2) return toHex(Uint8Array.from(pattern));
      pattern.push(bytecode[i + 1]!);
      i += 2 + bytecode[i + 1]!;
    } else if (selected === 77) {
      if (length - i < 3) return toHex(Uint8Array.from(pattern));
      pattern.push(bytecode[i + 1]!, bytecode[i + 2]!);
      i += 3 + (bytecode[i + 1]! | (bytecode[i + 2]! << 8));
    } else {
      if (length - i < 5) return toHex(Uint8Array.from(pattern));
      pattern.push(
        bytecode[i + 1]!,
        bytecode[i + 2]!,
        bytecode[i + 3]!,
        bytecode[i + 4]!
      );
      /*
       * v1 (plpgsql integer arithmetic) wraps a length above 2^31 to a
       * negative step; end the pattern there instead of looping.
       */
      if (bytecode[i + 4]! >= 0x80) return toHex(Uint8Array.from(pattern));
      i +=
        5 +
        (bytecode[i + 1]! |
          (bytecode[i + 2]! << 8) |
          (bytecode[i + 3]! << 16) |
          (bytecode[i + 4]! << 24));
    }
  }
  return toHex(Uint8Array.from(pattern));
};

/**
 * If the last instruction of unlocking bytecode is a push, the pattern of
 * its contents (a P2SH redeem bytecode); null if it is not a push or the
 * bytecode is malformed.
 */
export const parseBytecodePatternRedeem = (
  bytecodeHex: string
): string | null => {
  const bytecode = toBytes(bytecodeHex);
  const { length } = bytecode;
  let maybeRedeem: Uint8Array | null = null;
  let i = 0;
  while (i < length) {
    const selected = bytecode[i]!;
    if (selected > 78 || selected === 0) {
      i += 1;
      maybeRedeem = null;
    } else if (selected <= 75) {
      if (length - i < 1 + selected) return null;
      maybeRedeem = bytecode.subarray(i + 1, i + 1 + selected);
      i += 1 + selected;
    } else if (selected === 76) {
      if (length - i < 2) return null;
      const pushLength = bytecode[i + 1]!;
      if (length - i < 2 + pushLength) return null;
      maybeRedeem = bytecode.subarray(i + 2, i + 2 + pushLength);
      i += 2 + pushLength;
    } else if (selected === 77) {
      if (length - i < 3) return null;
      const pushLength = bytecode[i + 1]! | (bytecode[i + 2]! << 8);
      if (length - i < 3 + pushLength) return null;
      maybeRedeem = bytecode.subarray(i + 3, i + 3 + pushLength);
      i += 3 + pushLength;
    } else {
      if (length - i < 5) return null;
      if (bytecode[i + 4]! >= 0x80) return null;
      const pushLength =
        bytecode[i + 1]! |
        (bytecode[i + 2]! << 8) |
        (bytecode[i + 3]! << 16) |
        (bytecode[i + 4]! << 24);
      if (length - i - 5 < pushLength) return null;
      maybeRedeem = bytecode.subarray(i + 5, i + 5 + pushLength);
      i += 5 + pushLength;
    }
  }
  return maybeRedeem === null ? null : parseBytecodePattern(toHex(maybeRedeem));
};

/* ------------------------------------------------------------------ */
/* encoding                                                             */
/* ------------------------------------------------------------------ */

const uint32le = (value: bigint | number) => {
  const out = Buffer.alloc(4);
  out.writeUInt32LE(Number(BigInt.asUintN(32, BigInt(value))));
  return out;
};

const int32le = (value: bigint | number) => {
  const out = Buffer.alloc(4);
  out.writeInt32LE(Number(BigInt.asIntN(32, BigInt(value))));
  return out;
};

const uint64le = (value: bigint) => {
  const out = Buffer.alloc(8);
  out.writeBigUInt64LE(BigInt.asUintN(64, value));
  return out;
};

/** CompactSize ("VarInt"). */
export const compactUint = (value: bigint | number) => {
  const n = BigInt(value);
  if (n < 0xfdn) return Buffer.from([Number(n)]);
  if (n <= 0xffffn) {
    const out = Buffer.alloc(3);
    out[0] = 0xfd;
    out.writeUInt16LE(Number(n), 1);
    return out;
  }
  if (n <= 0xffffffffn) {
    const out = Buffer.alloc(5);
    out[0] = 0xfe;
    out.writeUInt32LE(Number(n), 1);
    return out;
  }
  const out = Buffer.alloc(9);
  out[0] = 0xff;
  out.writeBigUInt64LE(n, 1);
  return out;
};

const reversed = (hex: string) => Buffer.from(hex, 'hex').reverse();

export interface EncodableInput {
  outpoint_transaction_hash: string;
  outpoint_index: bigint | number | string;
  unlocking_bytecode: string;
  sequence_number: bigint | number | string;
}

export interface EncodableOutput {
  value_satoshis: bigint | number | string;
  locking_bytecode: string;
  token_category: string | null;
  fungible_token_amount: bigint | number | string | null;
  nonfungible_token_capability: string | null;
  nonfungible_token_commitment: string | null;
}

const capabilityBits: { [capability: string]: number } = {
  minting: 0x02,
  mutable: 0x01,
  none: 0x00,
};

/** The CashTokens prefix of an output (v1 `encode_transaction`). */
const tokenPrefix = (output: EncodableOutput) => {
  if (output.token_category === null) return Buffer.alloc(0);
  const amount =
    output.fungible_token_amount === null
      ? null
      : BigInt(output.fungible_token_amount);
  const parts: Buffer[] = [
    Buffer.from([0xef]),
    reversed(output.token_category),
  ];
  let bitfield = amount === 0n ? 0x00 : 0x10;
  if (output.nonfungible_token_commitment === null) {
    parts.push(Buffer.from([bitfield]));
  } else {
    bitfield |=
      0x20 |
      (capabilityBits[output.nonfungible_token_capability ?? 'none'] ?? 0);
    const commitment = Buffer.from(output.nonfungible_token_commitment, 'hex');
    if (commitment.length === 0) {
      parts.push(Buffer.from([bitfield]));
    } else {
      parts.push(
        Buffer.from([bitfield | 0x40]),
        compactUint(commitment.length),
        commitment
      );
    }
  }
  if (amount !== null && amount !== 0n) parts.push(compactUint(amount));
  return Buffer.concat(parts);
};

/** A transaction in the P2P network format (v1 `encode_transaction`), hex. */
export const encodeTransaction = (transaction: {
  version: bigint | number | string;
  locktime: bigint | number | string;
  inputs: readonly EncodableInput[];
  outputs: readonly EncodableOutput[];
}): string => {
  const parts: Buffer[] = [
    int32le(BigInt(transaction.version)),
    compactUint(transaction.inputs.length),
  ];
  transaction.inputs.forEach((input) => {
    const unlocking = Buffer.from(input.unlocking_bytecode, 'hex');
    parts.push(
      reversed(input.outpoint_transaction_hash),
      uint32le(BigInt(input.outpoint_index)),
      compactUint(unlocking.length),
      unlocking,
      uint32le(BigInt(input.sequence_number))
    );
  });
  parts.push(compactUint(transaction.outputs.length));
  transaction.outputs.forEach((output) => {
    const prefix = tokenPrefix(output);
    const locking = Buffer.from(output.locking_bytecode, 'hex');
    parts.push(
      uint64le(BigInt(output.value_satoshis)),
      compactUint(prefix.length + locking.length),
      prefix,
      locking
    );
  });
  parts.push(uint32le(BigInt(transaction.locktime)));
  return Buffer.concat(parts).toString('hex');
};

export interface EncodableHeader {
  version: bigint | number | string;
  previous_block_hash: string;
  merkle_root: string;
  timestamp: bigint | number | string;
  bits: bigint | number | string;
  nonce: bigint | number | string;
}

/** An 80-byte block header (v1 `encode_block_header`), hex. */
export const encodeBlockHeader = (header: EncodableHeader): string =>
  Buffer.concat([
    int32le(BigInt(header.version)),
    reversed(header.previous_block_hash),
    reversed(header.merkle_root),
    uint32le(BigInt(header.timestamp)),
    uint32le(BigInt(header.bits)),
    uint32le(BigInt(header.nonce)),
  ]).toString('hex');

/** A block in the P2P network format (v1 `encode_block`), hex. */
export const encodeBlock = (
  header: EncodableHeader,
  transactionsHex: readonly string[]
): string =>
  [
    encodeBlockHeader(header),
    compactUint(transactionsHex.length).toString('hex'),
    ...transactionsHex,
  ].join('');
