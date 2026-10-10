/* eslint-disable @typescript-eslint/no-magic-numbers */
// cspell:ignore aabbcc clickhouse feffffffffffffff héllo Milli rowbinary xffff
import test from 'ava';

import { encodeLeb128, leb128Length, RowBinaryWriter } from './row-binary.js';

const hex = (writer: RowBinaryWriter) => writer.finish().toString('hex');

test('RowBinaryWriter: unsigned integers are little-endian', (t) => {
  t.is(hex(new RowBinaryWriter().uint8(0).uint8(255)), '00ff');
  t.is(hex(new RowBinaryWriter().uint16(0x1234).uint16(0xffff)), '3412ffff');
  t.is(
    hex(new RowBinaryWriter().uint32(0x12345678).uint32(0xffff_ffff)),
    '78563412ffffffff'
  );
  t.is(hex(new RowBinaryWriter().uint64(0)), '0000000000000000');
  t.is(hex(new RowBinaryWriter().uint64(0x0102_0304_0506)), '0605040302010000');
  t.is(
    hex(new RowBinaryWriter().uint64(Number.MAX_SAFE_INTEGER)),
    'ffffffffffff1f00'
  );
  t.is(hex(new RowBinaryWriter().uint64(0n)), '0000000000000000');
  t.is(hex(new RowBinaryWriter().uint64(2n ** 63n - 1n)), 'ffffffffffffff7f');
  t.is(hex(new RowBinaryWriter().uint64(2n ** 64n - 1n)), 'ffffffffffffffff');
  t.is(
    hex(new RowBinaryWriter().uint64(0x0102030405060708n)),
    '0807060504030201'
  );
});

test('RowBinaryWriter: signed integers are twos-complement little-endian', (t) => {
  t.is(hex(new RowBinaryWriter().int8(-1).int8(127).int8(-128)), 'ff7f80');
  t.is(hex(new RowBinaryWriter().int16(-2).int16(0x7fff)), 'feffff7f');
  t.is(
    hex(new RowBinaryWriter().int32(-2).int32(-0x8000_0000)),
    'feffffff00000080'
  );
  t.is(hex(new RowBinaryWriter().int64(0)), '0000000000000000');
  t.is(hex(new RowBinaryWriter().int64(-1)), 'ffffffffffffffff');
  t.is(hex(new RowBinaryWriter().int64(-2)), 'feffffffffffffff');
  t.is(hex(new RowBinaryWriter().int64(0x1_0000_0000)), '0000000001000000');
  t.is(hex(new RowBinaryWriter().int64(-0x1_0000_0000)), '00000000ffffffff');
  t.is(
    hex(new RowBinaryWriter().int64(Number.MIN_SAFE_INTEGER)),
    '010000000000e0ff'
  );
  t.is(hex(new RowBinaryWriter().int64(2n ** 63n - 1n)), 'ffffffffffffff7f');
  t.is(hex(new RowBinaryWriter().int64(-(2n ** 63n))), '0000000000000080');
  t.is(hex(new RowBinaryWriter().int64(-1n)), 'ffffffffffffffff');
});

test('RowBinaryWriter: integer range errors', (t) => {
  t.throws(() => new RowBinaryWriter().uint8(256), { instanceOf: RangeError });
  t.throws(() => new RowBinaryWriter().uint8(-1), { instanceOf: RangeError });
  t.throws(() => new RowBinaryWriter().uint8(1.5), { instanceOf: RangeError });
  t.throws(() => new RowBinaryWriter().uint16(0x10000), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().uint32(0x1_0000_0000), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().uint64(-1), { instanceOf: RangeError });
  t.throws(() => new RowBinaryWriter().uint64(2 ** 53), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().uint64(2n ** 64n), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().uint64(-1n), { instanceOf: RangeError });
  t.throws(() => new RowBinaryWriter().int8(128), { instanceOf: RangeError });
  t.throws(() => new RowBinaryWriter().int16(-0x8001), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().int32(0x8000_0000), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().int64(2n ** 63n), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().int64(-(2n ** 63n) - 1n), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().int64(Number.NaN), {
    instanceOf: RangeError,
  });
});

test('RowBinaryWriter: bool, enum8, int8Sign, dateTime, dateTime64', (t) => {
  t.is(hex(new RowBinaryWriter().bool(true).bool(false)), '0100');
  t.is(hex(new RowBinaryWriter().enum8(3).enum8(-1)), '03ff');
  t.is(hex(new RowBinaryWriter().int8Sign(1).int8Sign(-1)), '01ff');
  t.throws(() => new RowBinaryWriter().int8Sign(0), {
    instanceOf: RangeError,
  });
  // 2009-01-03T18:15:05Z (genesis block timestamp) = 1231006505 = 0x495fab29
  t.is(hex(new RowBinaryWriter().dateTime(1231006505)), '29ab5f49');
  // 1231006505123 ms = 0x011e_9db4_98a3
  t.is(
    hex(new RowBinaryWriter().dateTime64(1231006505123)),
    'a398b49d1e010000'
  );
  t.is(
    hex(new RowBinaryWriter().dateTime64(new Date(1231006505123))),
    'a398b49d1e010000'
  );
  t.is(hex(new RowBinaryWriter().dateTime64(-1)), 'ffffffffffffffff');
});

test('encodeLeb128: edge cases', (t) => {
  const cases: [bigint | number, string][] = [
    [0, '00'],
    [1, '01'],
    [127, '7f'],
    [128, '8001'],
    [255, 'ff01'],
    [300, 'ac02'],
    [16383, 'ff7f'],
    [16384, '808001'],
    [2 ** 32, '8080808010'],
    [Number.MAX_SAFE_INTEGER, 'ffffffffffffff0f'],
    [0n, '00'],
    [128n, '8001'],
    [2n ** 32n, '8080808010'],
    [2n ** 64n - 1n, 'ffffffffffffffffff01'],
  ];
  cases.forEach(([value, expected]) => {
    t.is(encodeLeb128(value).toString('hex'), expected, String(value));
    t.is(leb128Length(value), expected.length / 2, `length ${String(value)}`);
  });
  t.throws(() => encodeLeb128(-1), { instanceOf: RangeError });
  t.throws(() => encodeLeb128(1.5), { instanceOf: RangeError });
  t.throws(() => encodeLeb128(2n ** 64n), { instanceOf: RangeError });
  t.throws(() => encodeLeb128(-1n), { instanceOf: RangeError });
});

test('RowBinaryWriter: string and hexBytes', (t) => {
  t.is(hex(new RowBinaryWriter().string('')), '00');
  t.is(hex(new RowBinaryWriter().string('abc')), '03616263');
  // 'é' is two UTF-8 bytes
  t.is(hex(new RowBinaryWriter().string('é')), '02c3a9');
  t.is(hex(new RowBinaryWriter().string(Buffer.from([0, 1, 2]))), '03000102');
  t.is(hex(new RowBinaryWriter().hexBytes('')), '00');
  t.is(hex(new RowBinaryWriter().hexBytes('76a914')), '0376a914');
  const long = 'ab'.repeat(200);
  t.is(hex(new RowBinaryWriter().hexBytes(long)), `c801${long}`);
  t.is(
    hex(new RowBinaryWriter().string('x'.repeat(128))).slice(0, 6),
    '800178'
  );
  t.throws(() => new RowBinaryWriter().hexBytes('abc'), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().hexBytes('zz'), {
    instanceOf: RangeError,
  });
});

test('RowBinaryWriter: fixedString', (t) => {
  const hash = `${'00'.repeat(31)}ff`;
  t.is(hex(new RowBinaryWriter().fixedString(32, hash)), hash);
  t.is(
    hex(new RowBinaryWriter().fixedString(4, Buffer.from([1, 2, 3, 4]))),
    '01020304'
  );
  t.is(hex(new RowBinaryWriter().fixedString(4, 'abcd', true)), 'abcd0000');
  t.is(
    hex(new RowBinaryWriter().fixedString(3, Buffer.from([9]), true)),
    '090000'
  );
  t.is(hex(new RowBinaryWriter().zeros(3)), '000000');
  t.throws(() => new RowBinaryWriter().fixedString(32, 'ab'.repeat(31)), {
    instanceOf: RangeError,
    message: /Expected 32 bytes/u,
  });
  t.throws(() => new RowBinaryWriter().fixedString(2, 'aabbcc', true), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().fixedString(2, Buffer.alloc(3)), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().fixedString(2, 'abc'), {
    instanceOf: RangeError,
  });
  t.throws(() => new RowBinaryWriter().fixedString(2, 'zzzz'), {
    instanceOf: RangeError,
  });
});

test('RowBinaryWriter: nullable and array', (t) => {
  t.is(
    hex(
      new RowBinaryWriter()
        .nullable(null, (w, v: number) => w.uint8(v))
        .nullable(undefined, (w, v: number) => w.uint8(v))
        .nullable(7, (w, v) => w.uint8(v))
        .nullable(0, (w, v) => w.int64(v))
    ),
    '01010007000000000000000000'
  );
  t.is(
    hex(new RowBinaryWriter().nullable('ab', (w, v) => w.hexBytes(v))),
    '0001ab'
  );
  const values = [1, 2, 3];
  t.is(
    hex(
      new RowBinaryWriter().array(values.length, (w, index) =>
        w.uint16(values[index] ?? 0)
      )
    ),
    '03010002000300'
  );
  t.is(hex(new RowBinaryWriter().array(0, (w) => w.uint8(1))), '00');
  t.is(
    hex(
      new RowBinaryWriter().array(2, (w, index) =>
        w.nullable(index === 0 ? null : 'x', (inner, v) => inner.string(v))
      )
    ),
    '0201000178'
  );
});

test('RowBinaryWriter: grows from a tiny buffer; rowCount and finish', (t) => {
  const writer = new RowBinaryWriter(1);
  const rows = 1000;
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let index = 0; index < rows; index += 1) {
    writer
      .uint32(index)
      .fixedString(32, 'ab'.repeat(32))
      .string('row')
      .int64(-index)
      .endRow();
  }
  t.is(writer.rowCount, rows);
  const data = writer.finish();
  const rowBytes = 4 + 32 + 4 + 8;
  t.is(data.length, rows * rowBytes);
  t.is(data.readUInt32LE(999 * rowBytes), 999);
  t.is(data.readBigInt64LE(999 * rowBytes + 40), -999n);
  t.is(new RowBinaryWriter(0).finish().length, 0);
});

const clickhouseUrl = process.env.CHAINGRAPH_E2E_CLICKHOUSE_URL;
const testDatabase = 'ch1_rowbinary_test';

const clickhouseQuery = async (query: string, body?: Buffer) => {
  const url = new URL(clickhouseUrl ?? '');
  url.searchParams.set('query', query);
  const response = await fetch(url, {
    body: body === undefined ? '' : Uint8Array.from(body),
    method: 'POST',
  });
  const text = await response.text();
  if (!response.ok) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(`ClickHouse ${response.status}: ${text}`);
  }
  return text;
};

(clickhouseUrl === undefined ? test.skip : test.serial)(
  '[e2e] RowBinaryWriter: round-trip through ClickHouse',
  async (t) => {
    const table = `${testDatabase}.row_binary_writer_${Date.now()}`;
    await clickhouseQuery(`CREATE DATABASE IF NOT EXISTS ${testDatabase}`);
    t.teardown(async () => {
      await clickhouseQuery(`DROP DATABASE IF EXISTS ${testDatabase}`);
    });
    await clickhouseQuery(
      `CREATE TABLE ${table} (
          u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64,
          i8 Int8, i16 Int16, i32 Int32, i64 Int64,
          b Bool, e Enum8('none' = 1, 'mutable' = 2, 'minting' = 3),
          dt DateTime('UTC'), dt64 DateTime64(3, 'UTC'),
          fs FixedString(4), s String, hb String,
          ns Nullable(String), ni Nullable(Int64),
          arr Array(UInt32), an Array(Nullable(String)), sign Int8
        ) ENGINE = MergeTree ORDER BY u32`
    );
    const writer = new RowBinaryWriter(16);
    writer
      .uint8(255)
      .uint16(65535)
      .uint32(4294967295)
      .uint64(2n ** 64n - 1n)
      .int8(-128)
      .int16(-32768)
      .int32(-2147483648)
      .int64(-(2n ** 63n))
      .bool(true)
      .enum8(3)
      .dateTime(1231006505)
      .dateTime64(1231006505123)
      .fixedString(4, 'deadbeef')
      .string('héllo')
      .hexBytes('00ff')
      .nullable(null, (w, v: string) => w.string(v))
      .nullable(2n ** 63n - 1n, (w, v) => w.int64(v))
      .array(3, (w, index) => w.uint32([0, 128, 16384][index] ?? 0))
      .array(2, (w, index) =>
        w.nullable(index === 0 ? 'a' : null, (inner, v) => inner.string(v))
      )
      .int8Sign(-1)
      .endRow();
    writer
      .uint8(0)
      .uint16(0)
      .uint32(0)
      .uint64(0)
      .int8(127)
      .int16(32767)
      .int32(2147483647)
      .int64(2n ** 63n - 1n)
      .bool(false)
      .enum8(1)
      .dateTime(0)
      .dateTime64(0)
      .fixedString(4, 'ab', true)
      .string('x'.repeat(200))
      .hexBytes('')
      .nullable('', (w, v) => w.string(v))
      .nullable(undefined, (w, v: number) => w.int64(v))
      .array(0, (w) => w.uint32(0))
      .array(0, (w) => w.uint8(0))
      .int8Sign(1)
      .endRow();
    await clickhouseQuery(
      `INSERT INTO ${table} FORMAT RowBinary`,
      writer.finish()
    );
    const rows = (
      await clickhouseQuery(
        `SELECT u8, u16, u32, toString(u64) AS u64, i8, i16, i32,
            toString(i64) AS i64, b, e, toUnixTimestamp(dt) AS dt,
            toUnixTimestamp64Milli(dt64) AS dt64, hex(fs) AS fs, s, hex(hb) AS hb,
            ns, toString(ni) AS ni, arr, an, sign
          FROM ${table} ORDER BY u32 DESC FORMAT JSONEachRow
          SETTINGS output_format_json_quote_64bit_integers = 0`
      )
    )
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as unknown);
    t.deepEqual(rows, [
      {
        an: ['a', null],
        arr: [0, 128, 16384],
        b: true,
        dt: 1231006505,
        dt64: 1231006505123,
        e: 'minting',
        fs: 'DEADBEEF',
        hb: '00FF',
        i16: -32768,
        i32: -2147483648,
        i64: '-9223372036854775808',
        i8: -128,
        ni: '9223372036854775807',
        ns: null,
        s: 'héllo',
        sign: -1,
        u16: 65535,
        u32: 4294967295,
        u64: '18446744073709551615',
        u8: 255,
      },
      {
        an: [],
        arr: [],
        b: false,
        dt: 0,
        dt64: 0,
        e: 'none',
        fs: 'AB000000',
        hb: '',
        i16: 32767,
        i32: 2147483647,
        i64: '9223372036854775807',
        i8: 127,
        ni: null,
        ns: '',
        s: 'x'.repeat(200),
        sign: 1,
        u16: 0,
        u32: 0,
        u64: '0',
        u8: 0,
      },
    ]);
  }
);
