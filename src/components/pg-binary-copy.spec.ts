/* eslint-disable @typescript-eslint/no-magic-numbers */
import test from 'ava';

import {
  binaryCopySignature,
  BinaryCopyWriter,
  CopyFromBufferQuery,
  decodeBinaryCopy,
} from './pg-binary-copy.js';

const header = Buffer.concat([
  binaryCopySignature,
  Buffer.from('0000000000000000', 'hex'),
]);
const trailer = Buffer.from('ffff', 'hex');

test('BinaryCopyWriter: empty payload is header + trailer', (t) => {
  const payload = new BinaryCopyWriter().finish();
  t.deepEqual(payload, Buffer.concat([header, trailer]));
  t.deepEqual(decodeBinaryCopy(payload), []);
});

test('BinaryCopyWriter: exact wire bytes for every field type', (t) => {
  const payload = new BinaryCopyWriter()
    .startRow(6)
    .hexBytea('c0de')
    .int8(1)
    .int8(-2n)
    .boolean(true)
    .text('minting')
    .null()
    .finish();
  t.deepEqual(
    payload,
    Buffer.concat([
      header,
      Buffer.from('0006', 'hex'),
      Buffer.from('00000002c0de', 'hex'),
      Buffer.from('000000080000000000000001', 'hex'),
      Buffer.from('00000008fffffffffffffffe', 'hex'),
      Buffer.from('0000000101', 'hex'),
      Buffer.from('000000076d696e74696e67', 'hex'),
      Buffer.from('ffffffff', 'hex'),
      trailer,
    ])
  );
});

const int8Cases: [bigint | number, string][] = [
  [0, '0000000000000000'],
  [1, '0000000000000001'],
  [-1, 'ffffffffffffffff'],
  [0xffffffff, '00000000ffffffff'],
  [0x1_0000_0000, '0000000100000000'],
  [-0x1_0000_0000, 'ffffffff00000000'],
  // cspell:disable-next-line
  [-0x1_0000_0001, 'fffffffeffffffff'],
  [Number.MAX_SAFE_INTEGER, '001fffffffffffff'],
  [Number.MIN_SAFE_INTEGER, 'ffe0000000000001'],
  [9223372036854775807n, '7fffffffffffffff'],
  [-9223372036854775808n, '8000000000000000'],
  [2_100_000_000_000_000n, '000775f05a074000'],
];

test('BinaryCopyWriter: int8 round-trips (number and bigint)', (t) => {
  const writer = new BinaryCopyWriter(1);
  int8Cases.forEach(([value]) => writer.startRow(1).int8(value));
  const rows = decodeBinaryCopy(writer.finish());
  t.is(rows.length, int8Cases.length);
  rows.forEach((row, index) => {
    const [value, hex] = int8Cases[index]!;
    t.is(row[0]!.toString('hex'), hex);
    t.is(row[0]!.readBigInt64BE(0), BigInt(value));
  });
});

test('BinaryCopyWriter: int8 rejects unsafe or fractional numbers', (t) => {
  t.throws(() => new BinaryCopyWriter().int8(Number.MAX_SAFE_INTEGER + 1));
  t.throws(() => new BinaryCopyWriter().int8(1.5));
  t.throws(() => new BinaryCopyWriter().int8(NaN));
  t.throws(() => new BinaryCopyWriter().int8(2n ** 63n));
});

test('BinaryCopyWriter: bytea round-trips (empty, hex, raw, large) and rejects bad hex', (t) => {
  const large = Buffer.alloc(3 * 1024 * 1024, 0xab);
  const rows = decodeBinaryCopy(
    new BinaryCopyWriter(1)
      .startRow(4)
      .hexBytea('')
      .hexBytea('00ff10')
      .bytea(Uint8Array.from([1, 2, 3]))
      .hexBytea(large.toString('hex'))
      .finish()
  );
  t.is(rows.length, 1);
  t.deepEqual(rows[0]![0], Buffer.alloc(0));
  t.deepEqual(rows[0]![1], Buffer.from('00ff10', 'hex'));
  t.deepEqual(rows[0]![2], Buffer.from([1, 2, 3]));
  t.true(rows[0]![3]!.equals(large));
  t.throws(() => new BinaryCopyWriter().hexBytea('abc'));
  t.throws(() => new BinaryCopyWriter().hexBytea('zz'));
});

test('BinaryCopyWriter: booleans, enum labels and nulls', (t) => {
  const rows = decodeBinaryCopy(
    new BinaryCopyWriter()
      .startRow(5)
      .boolean(false)
      .boolean(true)
      .text('none')
      .text('mutable')
      .null()
      .finish()
  );
  t.deepEqual(rows, [
    [
      Buffer.from([0]),
      Buffer.from([1]),
      Buffer.from('none'),
      Buffer.from('mutable'),
      null,
    ],
  ]);
});

test('BinaryCopyWriter: grows past its initial capacity', (t) => {
  const writer = new BinaryCopyWriter(1);
  const rowCount = 10_000;
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let index = 0; index < rowCount; index += 1) {
    writer.startRow(2).int8(index).hexBytea('0102');
  }
  const rows = decodeBinaryCopy(writer.finish());
  t.is(writer.rowCount, rowCount);
  t.is(rows.length, rowCount);
  t.is(rows[rowCount - 1]![0]!.readBigInt64BE(0), BigInt(rowCount - 1));
});

test('CopyFromBufferQuery: sends the statement, chunks the payload, reports COPY count', (t) => {
  const payload = Buffer.alloc(2.5 * 1024 * 1024, 1);
  const sent: string[] = [];
  const chunks: Buffer[] = [];
  const connection = {
    endCopyFrom: () => sent.push('done'),
    query: (text: string) => sent.push(text),
    sendCopyFail: () => sent.push('fail'),
    sendCopyFromChunk: (chunk: Buffer) => chunks.push(chunk),
  };
  const query = new CopyFromBufferQuery('COPY x FROM STDIN', payload);
  const results: [Error | undefined, number | null][] = [];
  query.callback = (err, rowCount) => results.push([err, rowCount]);
  query.submit(connection as never);
  query.handleCopyInResponse(connection);
  query.handleCommandComplete({ text: 'COPY 42' });
  query.handleReadyForQuery();
  query.handleReadyForQuery();
  t.deepEqual(sent, ['COPY x FROM STDIN', 'done']);
  t.is(chunks.length, 3);
  t.true(Buffer.concat(chunks).equals(payload));
  t.deepEqual(results, [[undefined, 42]]);
});

test('CopyFromBufferQuery: errors reach the callback once', (t) => {
  const query = new CopyFromBufferQuery('COPY x FROM STDIN', Buffer.alloc(0));
  const results: (Error | undefined)[] = [];
  query.callback = (err) => results.push(err);
  const error = new Error('boom');
  query.handleError(error);
  query.handleReadyForQuery();
  t.deepEqual(results, [error]);
});
