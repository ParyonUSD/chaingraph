/* eslint-disable @typescript-eslint/no-magic-numbers */
/**
 * A dependency-free encoder for the ClickHouse `RowBinary` input format
 * (https://clickhouse.com/docs/interfaces/formats/RowBinary): rows are
 * written column by column, in the column order of the `INSERT` statement,
 * with no row or column delimiters.
 *
 * Wire format summary:
 * - (U)Int8/16/32/64: fixed-width little-endian (two's complement if signed).
 * - Bool: one byte, 0 or 1. Enum8: its Int8 value.
 * - String: unsigned LEB128 byte length, then the bytes.
 * - FixedString(N): exactly N bytes.
 * - Nullable(T): one byte, 1 = NULL (nothing follows), 0 = a T follows.
 * - Array(T): unsigned LEB128 element count, then the elements.
 * - DateTime: UInt32 seconds; DateTime64(3): Int64 milliseconds.
 */

const bytesPerKilobyte = 1024;
const initialCapacityKilobytes = 64;
const initialCapacity = initialCapacityKilobytes * bytesPerKilobyte;
const growthFactor = 2;
const uint32Range = 0x1_0000_0000;
const hexCharsPerByte = 2;
/** The longest unsigned LEB128 encoding of a value below 2^64. */
const maxLeb128Bytes = 10;
const leb128ContinuationBit = 0x80;
const leb128Divisor = 0x80;
const nullFlag = 1;
const notNullFlag = 0;

const int64Min = -(2n ** 63n);
const int64Max = 2n ** 63n - 1n;
const uint64Max = 2n ** 64n - 1n;
const leb128DivisorBigint = BigInt(leb128Divisor);

/** Inclusive `[min, max]` ranges of the number-typed integer writers. */
const ranges = {
  int16: [-(2 ** 15), 2 ** 15 - 1],
  int32: [-(2 ** 31), 2 ** 31 - 1],
  int64: [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER],
  int8: [-(2 ** 7), 2 ** 7 - 1],
  uint16: [0, 2 ** 16 - 1],
  uint32: [0, 2 ** 32 - 1],
  uint64: [0, Number.MAX_SAFE_INTEGER],
  uint8: [0, 2 ** 8 - 1],
} as const;

const fail = (message: string): never => {
  // eslint-disable-next-line functional/no-throw-statement
  throw new RangeError(message);
};

const assertIntegerInRange = (value: number, type: keyof typeof ranges) => {
  const [min, max] = ranges[type];
  if (!Number.isInteger(value) || value < min || value > max) {
    fail(`Cannot encode ${value} as ${type}.`);
  }
};

const isValidLeb128Bigint = (value: bigint) =>
  value >= 0n && value <= uint64Max;

const isValidLeb128Number = (value: number) =>
  Number.isSafeInteger(value) && value >= 0;

const assertLeb128Value = (value: bigint | number) => {
  const valid =
    typeof value === 'bigint'
      ? isValidLeb128Bigint(value)
      : isValidLeb128Number(value);
  if (!valid) {
    fail(`Cannot encode ${value} as LEB128 (UInt64).`);
  }
};

/**
 * The number of bytes of the unsigned LEB128 encoding of `value`.
 */
export const leb128Length = (value: bigint | number) => {
  assertLeb128Value(value);
  // eslint-disable-next-line functional/no-let
  let remaining = BigInt(value) / leb128DivisorBigint;
  // eslint-disable-next-line functional/no-let
  let length = 1;
  // eslint-disable-next-line functional/no-loop-statement
  while (remaining > 0n) {
    remaining /= leb128DivisorBigint;
    length += 1;
  }
  return length;
};

/**
 * Write the unsigned LEB128 encoding of `value` (a safe non-negative integer
 * or a bigint below 2^64) into `buffer` at `offset`; returns the new offset.
 * Each byte holds 7 bits, least significant first; all but the last byte
 * have the high bit set.
 */
export const writeLeb128 = (
  buffer: Buffer,
  offset: number,
  value: bigint | number
) => {
  assertLeb128Value(value);
  // eslint-disable-next-line functional/no-let
  let position = offset;
  if (typeof value === 'bigint') {
    // eslint-disable-next-line functional/no-let
    let remaining = value;
    // eslint-disable-next-line functional/no-loop-statement
    while (remaining >= leb128DivisorBigint) {
      buffer[position] =
        Number(remaining % leb128DivisorBigint) + leb128ContinuationBit;
      remaining /= leb128DivisorBigint;
      position += 1;
    }
    buffer[position] = Number(remaining);
    return position + 1;
  }
  // eslint-disable-next-line functional/no-let
  let remaining = value;
  // eslint-disable-next-line functional/no-loop-statement
  while (remaining >= leb128Divisor) {
    buffer[position] = (remaining % leb128Divisor) + leb128ContinuationBit;
    remaining = Math.floor(remaining / leb128Divisor);
    position += 1;
  }
  buffer[position] = remaining;
  return position + 1;
};

/**
 * Encode `value` (a safe non-negative integer or a bigint below 2^64) as
 * unsigned LEB128, the length prefix of RowBinary `String` and `Array`.
 */
export const encodeLeb128 = (value: bigint | number) => {
  const buffer = Buffer.alloc(maxLeb128Bytes);
  const length = writeLeb128(buffer, 0, value);
  return buffer.subarray(0, length);
};

/**
 * Validate that `hex` is an even-length hex string and return its byte
 * length.
 */
const hexByteLength = (hex: string) => {
  if (hex.length % hexCharsPerByte !== 0) {
    fail(`Invalid hex (odd length ${hex.length}).`);
  }
  return hex.length / hexCharsPerByte;
};

/**
 * Encode rows in the ClickHouse `RowBinary` format into a growable buffer.
 * Every writer returns `this`, so a row reads as one chain. Call `endRow`
 * after each row to maintain `rowCount` (purely informational), and `finish`
 * to get the encoded bytes.
 */
export class RowBinaryWriter {
  buffer: Buffer;

  offset = 0;

  rowCount = 0;

  constructor(expectedSizeBytes = initialCapacity) {
    this.buffer = Buffer.allocUnsafe(Math.max(expectedSizeBytes, 1));
  }

  ensure(additionalBytes: number) {
    const required = this.offset + additionalBytes;
    if (required <= this.buffer.length) {
      return;
    }
    const grown = Buffer.allocUnsafe(
      Math.max(required, this.buffer.length * growthFactor)
    );
    this.buffer.copy(grown, 0, 0, this.offset);
    this.buffer = grown;
  }

  endRow() {
    this.rowCount += 1;
    return this;
  }

  uint8(value: number) {
    assertIntegerInRange(value, 'uint8');
    this.ensure(1);
    this.buffer[this.offset] = value;
    this.offset += 1;
    return this;
  }

  uint16(value: number) {
    assertIntegerInRange(value, 'uint16');
    this.ensure(2);
    this.buffer.writeUInt16LE(value, this.offset);
    this.offset += 2;
    return this;
  }

  uint32(value: number) {
    assertIntegerInRange(value, 'uint32');
    this.ensure(4);
    this.buffer.writeUInt32LE(value, this.offset);
    this.offset += 4;
    return this;
  }

  /**
   * Numbers must be safe integers; use a bigint for values above 2^53 - 1.
   */
  uint64(value: bigint | number) {
    this.ensure(8);
    if (typeof value === 'bigint') {
      if (value < 0n || value > uint64Max) {
        fail(`Cannot encode ${value} as UInt64.`);
      }
      this.buffer.writeBigUInt64LE(value, this.offset);
    } else {
      assertIntegerInRange(value, 'uint64');
      const high = Math.floor(value / uint32Range);
      this.buffer.writeUInt32LE(value - high * uint32Range, this.offset);
      this.buffer.writeUInt32LE(high, this.offset + 4);
    }
    this.offset += 8;
    return this;
  }

  int8(value: number) {
    assertIntegerInRange(value, 'int8');
    this.ensure(1);
    this.buffer.writeInt8(value, this.offset);
    this.offset += 1;
    return this;
  }

  int16(value: number) {
    assertIntegerInRange(value, 'int16');
    this.ensure(2);
    this.buffer.writeInt16LE(value, this.offset);
    this.offset += 2;
    return this;
  }

  int32(value: number) {
    assertIntegerInRange(value, 'int32');
    this.ensure(4);
    this.buffer.writeInt32LE(value, this.offset);
    this.offset += 4;
    return this;
  }

  /**
   * Numbers must be safe integers; use a bigint for values beyond ±(2^53 - 1).
   */
  int64(value: bigint | number) {
    this.ensure(8);
    if (typeof value === 'bigint') {
      if (value < int64Min || value > int64Max) {
        fail(`Cannot encode ${value} as Int64.`);
      }
      this.buffer.writeBigInt64LE(value, this.offset);
    } else {
      assertIntegerInRange(value, 'int64');
      const high = Math.floor(value / uint32Range);
      this.buffer.writeUInt32LE(value - high * uint32Range, this.offset);
      this.buffer.writeInt32LE(high, this.offset + 4);
    }
    this.offset += 8;
    return this;
  }

  /**
   * A `sign` column (`CollapsingMergeTree` / `VersionedCollapsingMergeTree`):
   * Int8, 1 or -1.
   */
  int8Sign(sign: number) {
    if (sign !== 1 && sign !== -1) {
      fail(`Invalid sign ${String(sign)}.`);
    }
    return this.int8(sign);
  }

  bool(value: boolean) {
    this.ensure(1);
    this.buffer[this.offset] = value ? 1 : 0;
    this.offset += 1;
    return this;
  }

  /**
   * An `Enum8` value (its Int8 number, not its label).
   */
  enum8(value: number) {
    return this.int8(value);
  }

  /**
   * `DateTime`: UInt32 seconds since the Unix epoch.
   */
  dateTime(epochSeconds: number) {
    return this.uint32(epochSeconds);
  }

  /**
   * `DateTime64(3)`: Int64 milliseconds since the Unix epoch. Accepts a
   * `Date` or a millisecond count.
   */
  dateTime64(value: Date | bigint | number) {
    return this.int64(value instanceof Date ? value.getTime() : value);
  }

  /**
   * `FixedString(length)` from raw bytes or a hex string. The value must be
   * exactly `length` bytes unless `pad` is set, in which case shorter values
   * are right-padded with zero bytes (as ClickHouse does for text input).
   */
  fixedString(length: number, value: Buffer | string, pad = false) {
    const byteLength =
      typeof value === 'string' ? hexByteLength(value) : value.length;
    if (byteLength !== length && !(pad && byteLength < length)) {
      fail(
        `Expected ${length} bytes for FixedString(${length}), got ${byteLength}.`
      );
    }
    this.ensure(length);
    this.writeRaw(value, byteLength);
    return this.zeros(length - byteLength);
  }

  /**
   * Copy raw bytes (or the bytes of a hex string) without a length prefix.
   */
  writeRaw(value: Buffer | string, byteLength: number) {
    if (typeof value === 'string') {
      const written = this.buffer.write(value, this.offset, byteLength, 'hex');
      if (written !== byteLength) {
        fail('Invalid hex.');
      }
    } else {
      value.copy(this.buffer, this.offset);
    }
    this.offset += byteLength;
    return this;
  }

  /**
   * A zero-filled `FixedString(length)` (e.g. "no token category").
   */
  zeros(length: number) {
    this.ensure(length);
    this.buffer.fill(0, this.offset, this.offset + length);
    this.offset += length;
    return this;
  }

  /**
   * `String` from raw bytes, or from a JS string encoded as UTF-8.
   */
  string(value: Buffer | string) {
    const byteLength =
      typeof value === 'string'
        ? Buffer.byteLength(value, 'utf8')
        : value.length;
    this.ensure(maxLeb128Bytes + byteLength);
    this.offset = writeLeb128(this.buffer, this.offset, byteLength);
    if (typeof value === 'string') {
      this.buffer.write(value, this.offset, byteLength, 'utf8');
    } else {
      value.copy(this.buffer, this.offset);
    }
    this.offset += byteLength;
    return this;
  }

  /**
   * `String` holding the bytes of a hex string.
   */
  hexBytes(hex: string) {
    const byteLength = hexByteLength(hex);
    this.ensure(maxLeb128Bytes + byteLength);
    this.offset = writeLeb128(this.buffer, this.offset, byteLength);
    return this.writeRaw(hex, byteLength);
  }

  /**
   * `Nullable(T)`: writes the NULL flag, then (if `value` is neither `null`
   * nor `undefined`) calls `write` to encode the value.
   */
  nullable<T>(
    value: T | null | undefined,
    write: (writer: this, present: T) => unknown
  ) {
    this.ensure(1);
    if (value === null || value === undefined) {
      this.buffer[this.offset] = nullFlag;
      this.offset += 1;
      return this;
    }
    this.buffer[this.offset] = notNullFlag;
    this.offset += 1;
    write(this, value);
    return this;
  }

  /**
   * `Array(T)`: writes the element count, then calls `write` once per index.
   */
  array(length: number, write: (writer: this, index: number) => unknown) {
    this.ensure(maxLeb128Bytes);
    this.offset = writeLeb128(this.buffer, this.offset, length);
    // eslint-disable-next-line functional/no-loop-statement, functional/no-let
    for (let index = 0; index < length; index += 1) {
      write(this, index);
    }
    return this;
  }

  /**
   * The encoded rows. The result shares memory with the writer's buffer.
   */
  finish() {
    return this.buffer.subarray(0, this.offset);
  }
}
