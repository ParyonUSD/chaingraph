/* eslint-disable @typescript-eslint/no-magic-numbers */
import test from 'ava';

import {
  binaryCopySignature,
  BinaryCopyWriter,
  copyDataPayload,
  CopyFromBuffersQuery,
  decodeBinaryCopy,
} from './pg-binary-copy.js';

const header = Buffer.concat([
  binaryCopySignature,
  Buffer.from('0000000000000000', 'hex'),
]);
const trailer = Buffer.from('ffff', 'hex');

const copyDone = Buffer.from('6300000004', 'hex');
const copyData = (payload: Buffer) => {
  const messageHeader = Buffer.alloc(5);
  messageHeader[0] = 0x64;
  messageHeader.writeInt32BE(4 + payload.length, 1);
  return Buffer.concat([messageHeader, payload, copyDone]);
};

test('BinaryCopyWriter: empty payload is header + trailer, framed as CopyData + CopyDone', (t) => {
  const messages = new BinaryCopyWriter().finish();
  t.deepEqual(messages, copyData(Buffer.concat([header, trailer])));
  t.deepEqual(copyDataPayload(messages), Buffer.concat([header, trailer]));
  t.deepEqual(decodeBinaryCopy(messages), []);
});

test('copyDataPayload: rejects anything but one CopyData + CopyDone', (t) => {
  const messages = new BinaryCopyWriter().finish();
  t.throws(() => copyDataPayload(messages.subarray(0, messages.length - 1)));
  t.throws(() => copyDataPayload(Buffer.concat([messages, copyDone])));
  const wrongCode = Buffer.from(messages);
  wrongCode[0] = 0x51;
  t.throws(() => copyDataPayload(wrongCode));
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
    copyDataPayload(payload),
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

const parseFrontendMessages = (bytes: Buffer) => {
  const messages: { code: string; body: Buffer }[] = [];
  // eslint-disable-next-line functional/no-let
  let offset = 0;
  // eslint-disable-next-line functional/no-loop-statement
  while (offset < bytes.length) {
    const code = String.fromCharCode(bytes[offset]!);
    const length = bytes.readInt32BE(offset + 1);
    messages.push({
      body: bytes.subarray(offset + 5, offset + 1 + length),
      code,
    });
    offset += 1 + length;
  }
  return messages;
};

const mockConnection = () => {
  const writes: Buffer[] = [];
  const events: string[] = [];
  const connection = {
    query: (text: string) => {
      const body = Buffer.from(`${text}\0`);
      const queryHeader = Buffer.alloc(5);
      queryHeader[0] = 'Q'.charCodeAt(0);
      queryHeader.writeInt32BE(4 + body.length, 1);
      writes.push(queryHeader, body);
    },
    stream: {
      cork: () => events.push('cork'),
      uncork: () => events.push('uncork'),
      write: (chunk: Buffer) => {
        writes.push(Buffer.from(chunk));
        return true;
      },
    },
  };
  return { connection, events, writes };
};

test('CopyFromBuffersQuery: one corked write of Query + CopyData/CopyDone per statement', (t) => {
  const first = new BinaryCopyWriter().startRow(1).int8(7).finish();
  const second = new BinaryCopyWriter().finish();
  const { connection, events, writes } = mockConnection();
  const query = new CopyFromBuffersQuery([
    { messages: first, statement: 'COPY a FROM STDIN' },
    { messages: second, statement: 'COPY b FROM STDIN' },
  ]);
  const results: [Error | undefined, (number | null)[]][] = [];
  query.callback = (err, rowCounts) => results.push([err, rowCounts]);
  query.submit(connection as never);
  t.deepEqual(events, ['cork', 'uncork']);
  const messages = parseFrontendMessages(Buffer.concat(writes));
  t.deepEqual(
    messages.map((message) => message.code),
    ['Q', 'd', 'c', 'd', 'c']
  );
  t.is(
    messages[0]!.body.toString(),
    'COPY a FROM STDIN;\nCOPY b FROM STDIN;\0'
  );
  t.deepEqual(messages[1]!.body, copyDataPayload(first));
  t.deepEqual(messages[3]!.body, copyDataPayload(second));
  t.deepEqual(messages[2]!.body.length, 0);
  query.handleCopyInResponse();
  query.handleCommandComplete({ text: 'COPY 1' });
  query.handleCopyInResponse();
  query.handleCommandComplete({ text: 'COPY 0' });
  query.handleReadyForQuery();
  query.handleReadyForQuery();
  t.deepEqual(results, [[undefined, [1, 0]]]);
});

test('CopyFromBuffersQuery: errors reach the callback once', (t) => {
  const query = new CopyFromBuffersQuery([
    {
      messages: new BinaryCopyWriter().finish(),
      statement: 'COPY x FROM STDIN',
    },
  ]);
  const results: (Error | undefined)[] = [];
  query.callback = (err) => results.push(err);
  const error = new Error('boom');
  query.handleError(error);
  query.handleReadyForQuery();
  t.deepEqual(results, [error]);
});
