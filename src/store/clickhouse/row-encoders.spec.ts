/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention */
// cspell:ignore aabbcc clickhouse rowbinary
import { readFileSync } from 'node:fs';

import test from 'ava';

import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import type { RowBinaryTable, SpentOutput } from './row-encoders.js';
import {
  encodeBlockRows,
  encodeBlockTransactionRows,
  encodeInputRows,
  encodeOutputRows,
  encodeTransactionRows,
  insertRowBinarySql,
  rowBinaryTableColumns,
} from './row-encoders.js';

const coreDdl = readFileSync(
  new URL('../../../src/store/clickhouse/ddl/010_core.sql', import.meta.url),
  'utf8'
);

/**
 * The inserted (non-MATERIALIZED) columns of each table in the DDL, with
 * CODECs removed.
 */
const ddlColumns = (table: string) => {
  const match = new RegExp(
    `CREATE TABLE IF NOT EXISTS cg\\.${table}\\s*\\(([\\s\\S]*?)\\n\\)\\nENGINE`,
    'u'
  ).exec(coreDdl);
  return (match?.[1] ?? '')
    .split('\n')
    .map((line) => line.trim().replace(/,$/u, ''))
    .filter(
      (line) =>
        line !== '' &&
        !line.startsWith('INDEX ') &&
        !line.includes(' MATERIALIZED ')
    )
    .map((line) => {
      const [name = '', ...rest] = line.split(/\s+/u);
      return [
        name,
        rest
          .join(' ')
          .replace(/\s*CODEC\(.*\)$/u, '')
          .trim(),
      ];
    });
};

test('row encoders: columns match the DDL (names, order, types)', (t) => {
  (Object.keys(rowBinaryTableColumns) as RowBinaryTable[]).forEach((table) => {
    t.deepEqual(
      rowBinaryTableColumns[table].map(([name, type]) => [name, type]),
      ddlColumns(table),
      table
    );
  });
  t.is(
    insertRowBinarySql('block_transaction', 'scratch'),
    'INSERT INTO scratch.block_transaction (block_internal_id, transaction_index, transaction_internal_id, transaction_hash, commit_seq) FORMAT RowBinary'
  );
});

/**
 * Decode RowBinary data using the column types (only the types used by the
 * encoders). Returns each row's values as strings (hex for byte columns),
 * numbers or null, and fails unless the data ends exactly on a row boundary.
 */
const decodeRows = (data: Buffer, table: RowBinaryTable) => {
  const columns = rowBinaryTableColumns[table];
  const rows: { [column: string]: unknown }[] = [];
  // eslint-disable-next-line functional/no-let
  let offset = 0;
  const scalarReaders: {
    [type: string]: [size: number, read: (at: number) => unknown];
  } = {
    Bool: [1, (at) => data.readUInt8(at) === 1],
    Enum8: [1, (at) => data.readInt8(at)],
    Int32: [4, (at) => data.readInt32LE(at)],
    Int64: [8, (at) => data.readBigInt64LE(at).toString()],
    UInt32: [4, (at) => data.readUInt32LE(at)],
    UInt64: [8, (at) => data.readBigUInt64LE(at).toString()],
  };
  const readBytes = (length: number) => {
    const value = data.subarray(offset, offset + length).toString('hex');
    offset += length;
    return value;
  };
  const readLeb128 = () => {
    // eslint-disable-next-line functional/no-let
    let value = 0;
    // eslint-disable-next-line functional/no-let
    let multiplier = 1;
    // eslint-disable-next-line functional/no-let
    let byte = 0;
    // eslint-disable-next-line functional/no-loop-statement
    do {
      byte = data[offset] ?? 0;
      offset += 1;
      value += (byte % 0x80) * multiplier;
      multiplier *= 0x80;
    } while (byte >= 0x80);
    return value;
  };
  const readScalar = (type: string) => {
    const reader = scalarReaders[type.startsWith('Enum8(') ? 'Enum8' : type];
    if (reader === undefined) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(`No reader for ${type}`);
    }
    const value = reader[1](offset);
    offset += reader[0];
    return value;
  };
  const read = (type: string): unknown => {
    if (type.startsWith('Nullable(')) {
      offset += 1;
      return data[offset - 1] === 1
        ? null
        : read(type.slice('Nullable('.length, -1));
    }
    if (type === 'String') {
      return readBytes(readLeb128());
    }
    if (type.startsWith('FixedString(')) {
      return readBytes(Number(type.slice('FixedString('.length, -1)));
    }
    return readScalar(type);
  };
  // eslint-disable-next-line functional/no-loop-statement
  while (offset < data.length) {
    rows.push(
      Object.fromEntries(columns.map(([name, type]) => [name, read(type)]))
    );
  }
  if (offset !== data.length) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('Truncated row.');
  }
  return rows;
};

const hash = (byte: string) => byte.repeat(32);
const nullHash = hash('00');
const tokenCategory = hash('cc');

const coinbase: ChaingraphTransaction = {
  hash: hash('11'),
  inputs: [
    {
      outpointIndex: 0xffffffff,
      outpointTransactionHash: nullHash,
      sequenceNumber: 0xffffffff,
      unlockingBytecode: '03aabbcc',
    },
  ],
  isCoinbase: true,
  locktime: 0,
  outputs: [{ lockingBytecode: '76a914', valueSatoshis: 625_000_000n }],
  sizeBytes: 100,
  version: 1,
};

const spender: ChaingraphTransaction = {
  hash: hash('22'),
  inputs: [
    {
      outpointIndex: 0,
      outpointTransactionHash: hash('11'),
      sequenceNumber: 0xfffffffe,
      unlockingBytecode: '',
    },
    {
      outpointIndex: 7,
      outpointTransactionHash: hash('99'),
      sequenceNumber: 0,
      unlockingBytecode: '51',
    },
  ],
  isCoinbase: false,
  locktime: 500_000,
  outputs: [
    { lockingBytecode: 'a914', valueSatoshis: 1000n },
    {
      fungibleTokenAmount: 2n ** 63n - 1n,
      lockingBytecode: 'aa20',
      nonfungibleTokenCapability: 'mutable',
      nonfungibleTokenCommitment: 'beef',
      tokenCategory,
      valueSatoshis: 800n,
    },
    {
      lockingBytecode: 'aa20',
      nonfungibleTokenCapability: 'none',
      nonfungibleTokenCommitment: '',
      tokenCategory,
      valueSatoshis: 800n,
    },
  ],
  sizeBytes: 250,
  version: 2,
};

const block: ChaingraphBlock = {
  bits: 0x1d00ffff,
  hash: hash('aa'),
  height: 840_000,
  merkleRoot: hash('bb'),
  nonce: 0xffffffff,
  previousBlockHash: hash('ab'),
  sizeBytes: 350,
  timestamp: 1_700_000_000,
  transactions: [coinbase, spender],
  version: 0x20000000,
};

const commitSeq = 42n;
const transactionInternalIds = [1001n, 2n ** 64n - 1n];
const context = { commitSeq, transactionInternalIds };
const spentOutputs = new Map<string, SpentOutput>([
  [
    `${hash('11')}:0`,
    { lockingBytecode: '76a914', valueSatoshis: 625_000_000n },
  ],
]);
const resolveSpentOutput = (outpointHash: string, outpointIndex: number) =>
  spentOutputs.get(`${outpointHash}:${outpointIndex}`);

const expectedRows = {
  block: [
    {
      bits: 0x1d00ffff,
      commit_seq: '42',
      generated_value_satoshis: '625000000',
      hash: hash('aa'),
      height: 840_000,
      internal_id: '7',
      merkle_root: hash('bb'),
      nonce: 0xffffffff,
      output_value_satoshis: '625002600',
      previous_block_hash: hash('ab'),
      size_bytes: 350,
      timestamp: 1_700_000_000,
      transaction_count: 2,
      version: 0x20000000,
    },
  ],
  block_transaction: [
    {
      block_internal_id: '7',
      commit_seq: '42',
      transaction_hash: hash('11'),
      transaction_index: 0,
      transaction_internal_id: '1001',
    },
    {
      block_internal_id: '7',
      commit_seq: '42',
      transaction_hash: hash('22'),
      transaction_index: 1,
      transaction_internal_id: '18446744073709551615',
    },
  ],
  input: [
    {
      commit_seq: '42',
      fungible_token_amount: null,
      input_index: 0,
      locking_bytecode: '',
      nonfungible_token_capability: null,
      nonfungible_token_commitment: null,
      outpoint_index: 0xffffffff,
      outpoint_transaction_hash: nullHash,
      sequence_number: 0xffffffff,
      token_category: nullHash,
      transaction_hash: hash('11'),
      transaction_internal_id: '1001',
      unlocking_bytecode: '03aabbcc',
      value_satoshis: '0',
    },
    {
      commit_seq: '42',
      fungible_token_amount: null,
      input_index: 0,
      locking_bytecode: '76a914',
      nonfungible_token_capability: null,
      nonfungible_token_commitment: null,
      outpoint_index: 0,
      outpoint_transaction_hash: hash('11'),
      sequence_number: 0xfffffffe,
      token_category: nullHash,
      transaction_hash: hash('22'),
      transaction_internal_id: '18446744073709551615',
      unlocking_bytecode: '',
      value_satoshis: '625000000',
    },
  ],
  output: [
    {
      commit_seq: '42',
      fungible_token_amount: null,
      locking_bytecode: '76a914',
      nonfungible_token_capability: null,
      nonfungible_token_commitment: null,
      output_index: 0,
      token_category: nullHash,
      transaction_hash: hash('11'),
      transaction_internal_id: '1001',
      value_satoshis: '625000000',
    },
    {
      commit_seq: '42',
      fungible_token_amount: null,
      locking_bytecode: 'a914',
      nonfungible_token_capability: null,
      nonfungible_token_commitment: null,
      output_index: 0,
      token_category: nullHash,
      transaction_hash: hash('22'),
      transaction_internal_id: '18446744073709551615',
      value_satoshis: '1000',
    },
    {
      commit_seq: '42',
      fungible_token_amount: '9223372036854775807',
      locking_bytecode: 'aa20',
      nonfungible_token_capability: 2,
      nonfungible_token_commitment: 'beef',
      output_index: 1,
      token_category: tokenCategory,
      transaction_hash: hash('22'),
      transaction_internal_id: '18446744073709551615',
      value_satoshis: '800',
    },
    {
      commit_seq: '42',
      fungible_token_amount: null,
      locking_bytecode: 'aa20',
      nonfungible_token_capability: 1,
      nonfungible_token_commitment: '',
      output_index: 2,
      token_category: tokenCategory,
      transaction_hash: hash('22'),
      transaction_internal_id: '18446744073709551615',
      value_satoshis: '800',
    },
  ],
  transaction: [
    {
      commit_seq: '42',
      hash: hash('11'),
      input_count: 1,
      internal_id: '1001',
      is_coinbase: true,
      locktime: 0,
      output_count: 1,
      output_value_satoshis: '625000000',
      size_bytes: 100,
      version: 1,
    },
    {
      commit_seq: '42',
      hash: hash('22'),
      input_count: 2,
      internal_id: '18446744073709551615',
      is_coinbase: false,
      locktime: 500_000,
      output_count: 3,
      output_value_satoshis: '2600',
      size_bytes: 250,
      version: 2,
    },
  ],
};

const encodeFixture = () => {
  const inputs = encodeInputRows(
    block.transactions,
    context,
    resolveSpentOutput
  );
  return {
    encoded: {
      block: encodeBlockRows(
        [{ block, generatedValueSatoshis: 625_000_000n, internalId: 7n }],
        commitSeq
      ),
      block_transaction: encodeBlockTransactionRows(block, 7n, context),
      input: inputs,
      output: encodeOutputRows(block.transactions, context),
      transaction: encodeTransactionRows(block.transactions, context),
    },
    pending: inputs.pending,
  };
};

/**
 * Bytes per row: fixed-width columns plus the LEB128 length of each String
 * and the flag of each Nullable.
 */
const expectedRowBytes = {
  block: [32 + 8 + 4 + 4 + 4 + 32 + 32 + 4 + 4 + 4 + 4 + 8 + 8 + 8],
  block_transaction: [8 + 4 + 8 + 32 + 8, 8 + 4 + 8 + 32 + 8],
  // hashes, ints, unlocking (1+n), value, category, 3 null flags, locking (1+n), commit_seq
  input: [
    32 + 4 + 8 + 32 + 4 + 4 + (1 + 4) + 8 + 32 + 1 + 1 + 1 + (1 + 0) + 8,
    32 + 4 + 8 + 32 + 4 + 4 + (1 + 0) + 8 + 32 + 1 + 1 + 1 + (1 + 3) + 8,
  ],
  output: [
    32 + 4 + 8 + 8 + (1 + 3) + 32 + 1 + 1 + 1 + 8,
    32 + 4 + 8 + 8 + (1 + 2) + 32 + 1 + 1 + 1 + 8,
    32 + 4 + 8 + 8 + (1 + 2) + 32 + (1 + 8) + (1 + 1) + (1 + 1 + 2) + 8,
    32 + 4 + 8 + 8 + (1 + 2) + 32 + 1 + (1 + 1) + (1 + 1 + 0) + 8,
  ],
  transaction: [
    32 + 8 + 4 + 4 + 4 + 1 + 4 + 4 + 8 + 8,
    32 + 8 + 4 + 4 + 4 + 1 + 4 + 4 + 8 + 8,
  ],
};

test('row encoders: fixture rows decode to the expected values and sizes', (t) => {
  const { encoded, pending } = encodeFixture();
  t.deepEqual(pending, [{ inputIndex: 1, transactionHash: hash('22') }]);
  (Object.keys(expectedRows) as RowBinaryTable[]).forEach((table) => {
    const { data, rowCount } = encoded[table];
    t.is(rowCount, expectedRows[table].length, `${table} rowCount`);
    t.is(
      data.length,
      expectedRowBytes[table].reduce((sum, bytes) => sum + bytes, 0),
      `${table} bytes`
    );
    const rows = decodeRows(data, table);
    rows.forEach((row) => {
      t.is(
        Object.keys(row).length,
        rowBinaryTableColumns[table].length,
        `${table} column count`
      );
    });
    t.deepEqual(rows, expectedRows[table], table);
  });
});

test('row encoders: reject missing internal ids and bad hashes', (t) => {
  t.throws(
    () =>
      encodeTransactionRows(block.transactions, {
        commitSeq,
        transactionInternalIds: [1n],
      }),
    {
      instanceOf: RangeError,
      message: /Missing internal id for transaction 1/u,
    }
  );
  t.throws(
    () =>
      encodeTransactionRows([{ ...coinbase, hash: 'abcd' }], {
        commitSeq,
        transactionInternalIds: [1n],
      }),
    { instanceOf: RangeError }
  );
});

const clickhouseUrl = process.env.CHAINGRAPH_E2E_CLICKHOUSE_URL;
const testDatabase = 'ch1_rowbinary_test_encoders';

const clickhouseQuery = async (query: string, body?: Buffer) => {
  const url = new URL(clickhouseUrl ?? '');
  url.searchParams.set('query', query);
  const response = await fetch(url, { body: body ?? '', method: 'POST' });
  const text = await response.text();
  if (!response.ok) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(`ClickHouse ${response.status}: ${text}`);
  }
  return text;
};

/**
 * Read a table back in the shape of `decodeRows`: bytes as lowercase hex,
 * 64-bit integers as strings, enums as numbers.
 */
const selectExpressions: [
  matches: (type: string) => boolean,
  expression: (name: string) => string
][] = [
  [
    (type) => type.startsWith('FixedString') || type === 'String',
    (name) => `lower(hex(${name}))`,
  ],
  [
    (type) => type === 'Nullable(String)',
    (name) => `if(isNull(${name}), NULL, lower(hex(assumeNotNull(${name}))))`,
  ],
  [
    (type) => type.startsWith('Nullable(Enum8'),
    (name) => `CAST(${name}, 'Nullable(Int8)')`,
  ],
  [(type) => type.includes('64'), (name) => `toString(${name})`],
];

const selectRows = async (table: RowBinaryTable) => {
  const columns = rowBinaryTableColumns[table];
  const expressions = columns.map(([name, type]) => {
    const expression =
      selectExpressions.find(([matches]) => matches(type))?.[1] ??
      ((column: string) => column);
    return `${expression(name)} AS ${name}`;
  });
  const order = columns
    .slice(0, 2)
    .map(([name]) => name)
    .join(', ');
  const text = await clickhouseQuery(
    `SELECT ${expressions.join(
      ', '
    )} FROM ${testDatabase}.${table} ORDER BY ${order} FORMAT JSONEachRow`
  );
  return text
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line) as unknown);
};

(clickhouseUrl === undefined ? test.skip : test.serial)(
  '[e2e] row encoders: round-trip through the ClickHouse core tables',
  async (t) => {
    t.teardown(async () => {
      await clickhouseQuery(`DROP DATABASE IF EXISTS ${testDatabase}`);
    });
    await clickhouseQuery(`DROP DATABASE IF EXISTS ${testDatabase}`);
    await clickhouseQuery(`CREATE DATABASE ${testDatabase}`);
    const statements = coreDdl
      .replace(/^\s*--.*\n/gmu, '')
      .replace(/\bcg\./gu, `${testDatabase}.`)
      .split(/;[ \t]*(?:\n|$)/u)
      .filter((statement) => statement.trim() !== '');
    // eslint-disable-next-line functional/no-loop-statement
    for (const statement of statements) {
      // eslint-disable-next-line no-await-in-loop
      await clickhouseQuery(statement);
    }
    const { encoded } = encodeFixture();
    // eslint-disable-next-line functional/no-loop-statement
    for (const table of Object.keys(expectedRows) as RowBinaryTable[]) {
      // eslint-disable-next-line no-await-in-loop
      await clickhouseQuery(
        insertRowBinarySql(table, testDatabase),
        encoded[table].data
      );
      // eslint-disable-next-line no-await-in-loop
      t.deepEqual(await selectRows(table), expectedRows[table], table);
    }
    const materialized = await clickhouseQuery(
      `SELECT lower(hex(locking_bytecode_prefix)) AS p, lower(hex(nonfungible_token_commitment_key)) AS k FROM ${testDatabase}.output ORDER BY transaction_hash, output_index FORMAT CSV`
    );
    t.is(
      materialized,
      '"76a914",""\n"a914",""\n"aa20","beef"\n"aa20",""\n',
      'materialized columns'
    );
  }
);
