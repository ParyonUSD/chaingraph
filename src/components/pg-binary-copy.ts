/* eslint-disable max-classes-per-file */
/**
 * Minimal support for Postgres `COPY … FROM STDIN (FORMAT binary)` on top of
 * the `pg` driver, without additional dependencies.
 *
 * - `BinaryCopyWriter` encodes rows in the binary COPY file format
 *   (https://www.postgresql.org/docs/current/sql-copy.html#id-1.9.3.55.9.4)
 *   directly inside the frontend protocol messages which carry them (one
 *   CopyData message followed by CopyDone), so they can be written to the
 *   socket without another copy.
 * - `CopyFromBuffersQuery` is a `pg` "submittable" which runs one or more
 *   `COPY … FROM STDIN` statements and pipelines their encoded data (the same
 *   protocol hooks `pg-copy-streams` uses).
 */
import type pg from 'pg';

/**
 * `PGCOPY\n\377\r\n\0`, followed by a 32-bit flags field (0) and a 32-bit
 * header extension length (0).
 */
export const binaryCopySignature = Buffer.from([
  /* eslint-disable @typescript-eslint/no-magic-numbers */
  0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
  /* eslint-enable @typescript-eslint/no-magic-numbers */
]);
const flagsAndExtensionBytes = 8;
const headerBytes = binaryCopySignature.length + flagsAndExtensionBytes;
/** A frontend message's type byte plus its int32 length. */
const messageHeaderBytes = 5;
const messageLengthBytes = 4;
/** CopyData header before the data, CopyDone after it. */
const framingBytes = messageHeaderBytes + messageHeaderBytes;
/** `d` */
const copyDataCode = 0x64;
/** `c` */
const copyDoneCode = 0x63;
/**
 * Postgres rejects messages over 1 GB (`PQ_LARGE_MESSAGE_LIMIT`); even a
 * 32 MB block encodes to well under 100 MB per table.
 */
const maxCopyDataBytes = 0x3fff_fff0;
const fieldCountBytes = 2;
const fieldLengthBytes = 4;
const int8Bytes = 8;
const booleanBytes = 1;
const nullFieldLength = -1;
const trailer = -1;
const uint32Range = 0x1_0000_0000;
const hexCharsPerByte = 2;
const maxErrorHexChars = 64;
const bytesPerKilobyte = 1024;
const initialCapacityKilobytes = 64;
const initialCapacity = initialCapacityKilobytes * bytesPerKilobyte;
const growthFactor = 2;

/**
 * Encode rows in the Postgres binary COPY format. Fields must be written in
 * the column order of the `COPY` statement. Integer types are written as
 * `int8`, so every integer column must be `bigint`; `enum` values are sent as
 * their label (that is the binary format of `enum_send`/`enum_recv`).
 *
 * The data is written after a reserved CopyData message header, and `finish`
 * appends a CopyDone message: the result is ready to send as-is after a
 * `COPY … FROM STDIN` Query message.
 */
export class BinaryCopyWriter {
  buffer: Buffer;

  offset: number;

  rowCount = 0;

  constructor(expectedSizeBytes = initialCapacity) {
    this.buffer = Buffer.allocUnsafe(
      Math.max(
        expectedSizeBytes + framingBytes,
        framingBytes + headerBytes + fieldCountBytes
      )
    );
    const start = messageHeaderBytes;
    binaryCopySignature.copy(this.buffer, start);
    this.buffer.writeInt32BE(0, start + binaryCopySignature.length);
    this.buffer.writeInt32BE(
      0,
      start + binaryCopySignature.length + fieldLengthBytes
    );
    this.offset = start + headerBytes;
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

  startRow(fieldCount: number) {
    this.ensure(fieldCountBytes);
    this.buffer.writeInt16BE(fieldCount, this.offset);
    this.offset += fieldCountBytes;
    this.rowCount += 1;
    return this;
  }

  null() {
    this.ensure(fieldLengthBytes);
    this.buffer.writeInt32BE(nullFieldLength, this.offset);
    this.offset += fieldLengthBytes;
    return this;
  }

  /**
   * Write a `bigint` (int8) field. Numbers must be safe integers.
   */
  int8(value: bigint | number) {
    this.ensure(fieldLengthBytes + int8Bytes);
    this.buffer.writeInt32BE(int8Bytes, this.offset);
    this.offset += fieldLengthBytes;
    if (typeof value === 'bigint') {
      this.buffer.writeBigInt64BE(value, this.offset);
    } else {
      if (!Number.isSafeInteger(value)) {
        // eslint-disable-next-line functional/no-throw-statement
        throw new RangeError(`Cannot encode ${value} as int8.`);
      }
      const high = Math.floor(value / uint32Range);
      const low = value - high * uint32Range;
      this.buffer.writeInt32BE(high, this.offset);
      this.buffer.writeUInt32BE(low, this.offset + fieldLengthBytes);
    }
    this.offset += int8Bytes;
    return this;
  }

  boolean(value: boolean) {
    this.ensure(fieldLengthBytes + booleanBytes);
    this.buffer.writeInt32BE(booleanBytes, this.offset);
    this.offset += fieldLengthBytes;
    this.buffer[this.offset] = value ? 1 : 0;
    this.offset += booleanBytes;
    return this;
  }

  /**
   * Write a `bytea` field from a hex string (without `\x` prefix).
   */
  hexBytea(hex: string) {
    if (hex.length % hexCharsPerByte !== 0) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `Invalid hex (odd length): ${hex.slice(0, maxErrorHexChars)}`
      );
    }
    const byteLength = hex.length / hexCharsPerByte;
    this.ensure(fieldLengthBytes + byteLength);
    this.buffer.writeInt32BE(byteLength, this.offset);
    this.offset += fieldLengthBytes;
    const written = this.buffer.write(hex, this.offset, byteLength, 'hex');
    if (written !== byteLength) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(`Invalid hex: ${hex.slice(0, maxErrorHexChars)}`);
    }
    this.offset += byteLength;
    return this;
  }

  bytea(bytes: Uint8Array) {
    this.ensure(fieldLengthBytes + bytes.length);
    this.buffer.writeInt32BE(bytes.length, this.offset);
    this.offset += fieldLengthBytes;
    this.buffer.set(bytes, this.offset);
    this.offset += bytes.length;
    return this;
  }

  /**
   * Write a `text` (or `enum` label) field.
   */
  text(value: string) {
    const byteLength = Buffer.byteLength(value, 'utf8');
    this.ensure(fieldLengthBytes + byteLength);
    this.buffer.writeInt32BE(byteLength, this.offset);
    this.offset += fieldLengthBytes;
    this.buffer.write(value, this.offset, byteLength, 'utf8');
    this.offset += byteLength;
    return this;
  }

  /**
   * Append the trailer and return the frontend messages carrying the data: a
   * CopyData message with the complete binary COPY payload, then CopyDone (a
   * view of the internal buffer). The writer must not be used afterwards.
   */
  finish() {
    this.ensure(fieldCountBytes + messageHeaderBytes);
    this.buffer.writeInt16BE(trailer, this.offset);
    this.offset += fieldCountBytes;
    const payloadBytes = this.offset - messageHeaderBytes;
    if (payloadBytes > maxCopyDataBytes) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new RangeError(
        `COPY payload of ${payloadBytes} bytes exceeds the CopyData message limit.`
      );
    }
    this.buffer[0] = copyDataCode;
    this.buffer.writeInt32BE(messageLengthBytes + payloadBytes, 1);
    this.buffer[this.offset] = copyDoneCode;
    this.buffer.writeInt32BE(messageLengthBytes, this.offset + 1);
    this.offset += messageHeaderBytes;
    return this.buffer.subarray(0, this.offset);
  }
}

const decodeRow = (payload: Buffer, start: number, fieldCount: number) => {
  const row: (Buffer | null)[] = [];
  // eslint-disable-next-line functional/no-let
  let offset = start;
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let field = 0; field < fieldCount; field += 1) {
    const length = payload.readInt32BE(offset);
    offset += fieldLengthBytes;
    const isNull = length === nullFieldLength;
    row.push(isNull ? null : payload.subarray(offset, offset + length));
    offset += isNull ? 0 : length;
  }
  return { offset, row };
};

/**
 * Extract the binary COPY payload from the messages built by
 * `BinaryCopyWriter.finish` (one CopyData message, then CopyDone).
 */
export const copyDataPayload = (messages: Buffer) => {
  const copyDataLength = messages.readInt32BE(1);
  const copyDoneOffset = 1 + copyDataLength;
  if (
    messages[0] !== copyDataCode ||
    messages[copyDoneOffset] !== copyDoneCode ||
    messages.readInt32BE(copyDoneOffset + 1) !== messageLengthBytes ||
    copyDoneOffset + messageHeaderBytes !== messages.length
  ) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('Expected exactly one CopyData message and CopyDone.');
  }
  return messages.subarray(messageHeaderBytes, copyDoneOffset);
};

/**
 * Decode the messages built by `BinaryCopyWriter.finish`. Only used to verify
 * encoders: every non-null field is returned as raw bytes.
 */
export const decodeBinaryCopy = (messages: Buffer) => {
  const payload = copyDataPayload(messages);
  if (
    !payload.subarray(0, binaryCopySignature.length).equals(binaryCopySignature)
  ) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('Missing binary COPY signature.');
  }
  const extensionLength = payload.readInt32BE(
    binaryCopySignature.length + fieldLengthBytes
  );
  const rows: (Buffer | null)[][] = [];
  // eslint-disable-next-line functional/no-let
  let offset = headerBytes + extensionLength;
  // eslint-disable-next-line functional/no-let
  let fieldCount = payload.readInt16BE(offset);
  // eslint-disable-next-line functional/no-loop-statement
  while (fieldCount !== trailer) {
    const { offset: rowEnd, row } = decodeRow(
      payload,
      offset + fieldCountBytes,
      fieldCount
    );
    rows.push(row);
    offset = rowEnd;
    fieldCount = payload.readInt16BE(offset);
  }
  if (offset + fieldCountBytes !== payload.length) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('Trailing bytes after binary COPY trailer.');
  }
  return rows;
};

// eslint-disable-next-line functional/no-mixed-type
interface CopyConnection {
  query: (text: string) => void;
  stream: {
    cork: () => void;
    uncork: () => void;
    write: (chunk: Buffer) => boolean;
  };
}

export interface CopyFromStdin {
  /**
   * A `COPY … FROM STDIN` statement (without trailing semicolon).
   */
  statement: string;
  /**
   * The CopyData and CopyDone messages for this statement, from
   * `BinaryCopyWriter.finish`.
   */
  messages: Buffer;
}

/**
 * A `pg` submittable which runs one or more `COPY … FROM STDIN` statements in
 * a single simple-protocol Query message and pipelines all of their data
 * right behind it (Query, then CopyData + CopyDone per statement, in one
 * corked socket write). This costs one round trip regardless of the number of
 * statements, instead of one round trip per statement plus one per
 * CopyInResponse.
 *
 * Pipelining is safe: if a statement fails, Postgres skips the rest of the
 * Query message, sends ReadyForQuery and drops the remaining CopyData and
 * CopyDone messages ("Copy-In Mode" in the protocol documentation).
 *
 * Use via `copyFromBuffers`.
 */
export class CopyFromBuffersQuery implements pg.Submittable {
  copies: CopyFromStdin[];

  rowCounts: (number | null)[] = [];

  error: Error | undefined;

  done = false;

  callback:
    | ((err: Error | undefined, rowCounts: (number | null)[]) => void)
    | undefined;

  constructor(copies: CopyFromStdin[]) {
    this.copies = copies;
  }

  finish(err: Error | undefined) {
    if (this.done) {
      return;
    }
    this.done = true;
    this.callback?.(err, this.rowCounts);
  }

  submit(connection: pg.Connection) {
    const copyConnection = connection as unknown as CopyConnection;
    const { stream } = copyConnection;
    stream.cork();
    copyConnection.query(
      this.copies.map((copy) => `${copy.statement};`).join('\n')
    );
    this.copies.forEach(({ messages }) => {
      stream.write(messages);
    });
    stream.uncork();
  }

  /**
   * The data was already sent in `submit`.
   */
  // eslint-disable-next-line class-methods-use-this
  handleCopyInResponse() {
    /* already sent */
  }

  handleCommandComplete(message: { text?: string }) {
    const match = /^COPY (?<count>\d+)$/u.exec(message.text ?? '');
    this.rowCounts.push(
      match?.groups?.count === undefined ? null : Number(match.groups.count)
    );
  }

  handleReadyForQuery() {
    this.finish(this.error);
  }

  handleError(err: Error) {
    this.error = err;
    this.finish(err);
  }

  /* The remaining hooks are required by `pg` but unused by COPY FROM. */
  // eslint-disable-next-line class-methods-use-this
  handleRowDescription() {
    /* unused */
  }

  // eslint-disable-next-line class-methods-use-this
  handleDataRow() {
    /* unused */
  }

  // eslint-disable-next-line class-methods-use-this
  handleEmptyQuery() {
    /* unused */
  }

  // eslint-disable-next-line class-methods-use-this
  handlePortalSuspended() {
    /* unused */
  }

  // eslint-disable-next-line class-methods-use-this
  handleCopyData() {
    /* unused */
  }
}

/**
 * Run `COPY … FROM STDIN` statements on `client` with already-encoded data,
 * in one round trip. Resolves to the row count reported for each statement.
 */
export const copyFromBuffers = async (
  client: pg.ClientBase,
  copies: CopyFromStdin[]
) =>
  new Promise<(number | null)[]>((resolve, reject) => {
    const query = new CopyFromBuffersQuery(copies);
    query.callback = (err, rowCounts) => {
      if (err === undefined) {
        resolve(rowCounts);
      } else {
        reject(err);
      }
    };
    client.query(query);
  });
