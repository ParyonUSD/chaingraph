#!/usr/bin/env node
// cspell:ignore bytewise clickhouse nullness Milli unhex prefilter denormalised selftest orged unnest
/**
 * Self-test of the parity harness (docs/clickhouse-port/parity-harness.md §6).
 *
 *   node scripts/parity/selftest.mjs [--pg-admin postgres://chaingraph:…@localhost:15432/postgres]
 *                                    [--ch http://localhost:18123] [--out <dir>] [--keep]
 *
 * 1. Digest primitive: the `sum` and `ordered` digests of a fixed string set
 *    agree between JS, Postgres and ClickHouse.
 * 2. Seeds IDENTICAL facts into a scratch Postgres (`ch1_parity_*`, all
 *    hasura migrations + agent indexes + F1g `unspent_output`) and a scratch
 *    ClickHouse (`ch1_parity_*`, ddl/apply.sh; rows written with committed
 *    commit_log + visibility rows so the WP4 gate shows them), with DIFFERENT
 *    internal ids on each side: 5 blocks incl. a stale fork block that node-a
 *    re-orged away (node_block_history) and node-b still follows, a shared tx
 *    in both fork blocks, token outputs (FT, mutable/none NFTs, empty
 *    commitment), spends, a mempool tx per node, a confirmed and a replaced
 *    mempool history row.
 * 3. compare.mjs in several modes -> every chunk matches.
 * 4. Mutations, each reverted afterwards, must be detected in exactly the
 *    expected node/table and nowhere else (checklist item 2 isolation):
 *    node-b gains a node_block fact; node-b gains a tx_acceptance fact; a
 *    Postgres output value changes; a ClickHouse accepted_at drifts by 3 s
 *    (tolerance 1 s: only the timestamp row; tolerance 5 s: nothing).
 * Scratch databases are dropped unless --keep. Exit 0 = every expectation met.
 */
import { execFileSync, spawnSync } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

import pg from 'pg';

import { ClickHouseHttp } from './lib/engines.mjs';
import { digestFromSums, referenceDigest } from './lib/digest.mjs';

const harnessDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(harnessDirectory, '../..');
const migrationsDirectory = join(
  repositoryRoot,
  'images/hasura/hasura-data/migrations/default'
);
const ddlApply = join(repositoryRoot, 'src/store/clickhouse/ddl/apply.sh');

const { values: options } = parseArgs({
  options: {
    ch: { default: 'http://localhost:18123', type: 'string' },
    keep: { default: false, type: 'boolean' },
    out: {
      default: join(tmpdir(), `ch1-parity-selftest-${Date.now()}`),
      type: 'string',
    },
    'pg-admin': {
      default:
        'postgres://chaingraph:very_insecure_postgres_password@localhost:15432/postgres',
      type: 'string',
    },
  },
});

// Agent-created Postgres indexes (src/components/db-utils.ts).
const agentIndexes = [
  'CREATE INDEX block_height_index ON block USING btree (height)',
  'CREATE INDEX block_inclusions_index ON block_transaction USING btree (transaction_internal_id)',
  'CREATE INDEX output_search_index ON output USING btree (substring(locking_bytecode, 0, 26))',
  'CREATE INDEX spent_by_index ON input USING btree (outpoint_transaction_hash, outpoint_index)',
  'CREATE INDEX token_category_index ON output USING btree (token_category)',
];

// ------------------------------------------------------------------ fixture

const hashOf = (label) => createHash('sha256').update(label).digest();
const zeroHash = Buffer.alloc(32);
const category = hashOf('category K');
const lockOf = (label) =>
  Buffer.concat([
    Buffer.from('76a914', 'hex'),
    hashOf(label).subarray(0, 20),
    Buffer.from('88ac', 'hex'),
  ]);
const baseTime = Date.parse('2026-10-09T10:00:00.000Z');
const at = (seconds, extraMs = 0) => baseTime + seconds * 1000 + extraMs;

const coinbase = (height) => ({
  coinbase: true,
  inputs: [
    {
      index: 0xffffffff,
      sequence: 0xffffffff,
      tx: undefined,
      unlocking: Buffer.from([3, height, 0, 0]),
    },
  ],
  locktime: 0,
  outputs: [{ lock: lockOf(`coinbase ${height}`), value: 5000 }],
  version: 1,
});
const spend = (tx, index, unlockingLabel) => ({
  index,
  sequence: 0xfffffffe,
  tx,
  unlocking: hashOf(unlockingLabel),
});

const transactions = {
  C0: coinbase(0),
  C1: coinbase(1),
  C2: coinbase(2),
  C2x: {
    ...coinbase(2),
    outputs: [{ lock: lockOf('coinbase 2x'), value: 5000 }],
  },
  C3: coinbase(3),
  T1: {
    inputs: [spend('C0', 0, 'sig T1')],
    locktime: 0,
    outputs: [
      { category, ft: 1000n, lock: lockOf('L1'), value: 1000 },
      {
        capability: 'mutable',
        category,
        commitment: Buffer.from('0102', 'hex'),
        lock: lockOf('L2'),
        value: 800,
      },
      {
        capability: 'none',
        category,
        commitment: Buffer.alloc(0),
        ft: 5n,
        lock: lockOf('L3'),
        value: 700,
      },
      { lock: lockOf('L4'), value: 100 },
    ],
    version: 2,
  },
  T2: {
    inputs: [spend('T1', 1, 'sig T2')],
    locktime: 0,
    outputs: [
      {
        capability: 'mutable',
        category,
        commitment: Buffer.from('0103', 'hex'),
        lock: lockOf('L5'),
        value: 790,
      },
    ],
    version: 2,
  },
  T3: {
    inputs: [spend('T1', 3, 'sig T3')],
    locktime: 500,
    outputs: [{ lock: lockOf('L6'), value: 90 }],
    version: 1,
  },
  M: {
    inputs: [spend('T1', 0, 'sig M')],
    locktime: 0,
    outputs: [{ category, ft: 1000n, lock: lockOf('L7'), value: 990 }],
    version: 2,
  },
  R: {
    inputs: [spend('C1', 0, 'sig R')],
    locktime: 0,
    outputs: [{ lock: lockOf('L8'), value: 4000 }],
    version: 2,
  },
};
const transactionKeys = Object.keys(transactions);
for (const [key, tx] of Object.entries(transactions)) {
  tx.key = key;
  tx.hash = hashOf(`tx ${key}`);
  tx.pgId = transactionKeys.indexOf(key) + 1;
  tx.chId = transactionKeys.indexOf(key) + 1001;
  tx.size = 200 + tx.pgId;
}
const outputOf = (key, index) => transactions[key].outputs[index];

const blocks = [
  { height: 0, key: 'B0', prev: undefined, txs: ['C0'] },
  { height: 1, key: 'B1', prev: 'B0', txs: ['C1', 'T1'] },
  { height: 2, key: 'B2', prev: 'B1', txs: ['C2', 'T2'] },
  { height: 2, key: 'B2x', prev: 'B1', txs: ['C2x', 'T2'] },
  { height: 3, key: 'B3', prev: 'B2', txs: ['C3', 'T3'] },
];
const blockByKey = Object.fromEntries(
  blocks.map((block) => [block.key, block])
);
blocks.forEach((block, index) => {
  block.hash = hashOf(`block ${block.key}`);
  block.pgId = index + 1;
  block.chId = index + 101;
  block.timestamp = 1_700_000_000 + block.height * 600;
});

const nodes = [
  { chId: 5, name: 'node-a', pgId: 1 },
  { chId: 9, name: 'node-b', pgId: 2 },
];
// accepted blocks (accepted_at null = before monitoring), mempools, histories
const nodeBlocks = {
  'node-a': [
    ['B0', null],
    ['B1', at(60)],
    ['B2', at(120)],
    ['B3', at(180)],
  ],
  'node-b': [
    ['B0', null],
    ['B1', at(61, 250)],
    ['B2x', at(100)],
  ],
};
const nodeMempool = { 'node-a': [['M', at(190)]], 'node-b': [['T3', at(170)]] };
const blockHistory = { 'node-a': [['B2x', at(100, 5), at(119)]], 'node-b': [] };
const transactionHistory = {
  'node-a': [['T3', at(170, 30), null]],
  'node-b': [['R', at(65), at(66)]],
};

/** unspent(n, o) rows as the WP5a sign rules write them: +1 per output of an accepted tx, -1 per spent outpoint. */
const utxoRows = (nodeName) => {
  const accepted = new Set();
  for (const [blockKey] of nodeBlocks[nodeName])
    for (const tx of blockByKey[blockKey].txs) accepted.add(tx);
  for (const [tx] of nodeMempool[nodeName]) accepted.add(tx);
  const rows = [];
  for (const key of accepted) {
    const tx = transactions[key];
    tx.outputs.forEach((output, index) =>
      rows.push({ index, output, sign: 1, tx: key })
    );
    for (const input of tx.inputs)
      if (input.tx !== undefined)
        rows.push({
          index: input.index,
          output: outputOf(input.tx, input.index),
          sign: -1,
          tx: input.tx,
        });
  }
  return rows;
};

// ------------------------------------------------------------------ Postgres seed

const pgTimestamp = (ms) =>
  ms === null
    ? null
    : new Date(ms).toISOString().replace('T', ' ').replace('Z', '');

const seedPostgres = async (client) => {
  await client.query("SET session_replication_role = 'replica'"); // no triggers: rows exactly as listed
  for (const node of nodes) {
    await client.query(
      'INSERT INTO node (internal_id, name, protocol_version, user_agent) VALUES ($1, $2, 70016, $3)',
      [node.pgId, node.name, '/Bitcoin Cash Node:28.0.0/']
    );
  }
  for (const block of blocks) {
    await client.query(
      `INSERT INTO block (internal_id, height, version, "timestamp", hash, previous_block_hash, merkle_root, bits, nonce, size_bytes)
       VALUES ($1, $2, 536870912, $3, $4, $5, $6, 486604799, $7, $8)`,
      [
        block.pgId,
        block.height,
        block.timestamp,
        block.hash,
        block.prev ? blockByKey[block.prev].hash : zeroHash,
        hashOf(`merkle ${block.key}`),
        1000 + block.pgId,
        1000 + block.height,
      ]
    );
    for (const [index, key] of block.txs.entries()) {
      await client.query(
        'INSERT INTO block_transaction (block_internal_id, transaction_internal_id, transaction_index) VALUES ($1, $2, $3)',
        [block.pgId, transactions[key].pgId, index]
      );
    }
  }
  for (const tx of Object.values(transactions)) {
    await client.query(
      'INSERT INTO transaction (internal_id, hash, version, locktime, size_bytes, is_coinbase) VALUES ($1, $2, $3, $4, $5, $6)',
      [tx.pgId, tx.hash, tx.version, tx.locktime, tx.size, tx.coinbase === true]
    );
    for (const [index, output] of tx.outputs.entries()) {
      await client.query(
        `INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount,
           nonfungible_token_capability, nonfungible_token_commitment) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
        [
          tx.hash,
          index,
          output.value,
          output.lock,
          output.category ?? null,
          output.ft?.toString() ?? null,
          output.capability ?? null,
          output.commitment ?? null,
        ]
      );
    }
    for (const [index, input] of tx.inputs.entries()) {
      await client.query(
        `INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          tx.pgId,
          index,
          input.index,
          input.sequence,
          input.tx ? transactions[input.tx].hash : zeroHash,
          input.unlocking,
        ]
      );
    }
  }
  let historyId = 1;
  for (const node of nodes) {
    for (const [blockKey, acceptedAt] of nodeBlocks[node.name]) {
      await client.query(
        'INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at) VALUES ($1, $2, $3)',
        [node.pgId, blockByKey[blockKey].pgId, pgTimestamp(acceptedAt)]
      );
    }
    for (const [txKey, validatedAt] of nodeMempool[node.name]) {
      await client.query(
        'INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at) VALUES ($1, $2, $3)',
        [node.pgId, transactions[txKey].pgId, pgTimestamp(validatedAt)]
      );
    }
    for (const [blockKey, acceptedAt, removedAt] of blockHistory[node.name]) {
      await client.query(
        'INSERT INTO node_block_history (internal_id, node_internal_id, block_internal_id, accepted_at, removed_at) VALUES ($1, $2, $3, $4, $5)',
        [
          historyId,
          node.pgId,
          blockByKey[blockKey].pgId,
          pgTimestamp(acceptedAt),
          pgTimestamp(removedAt),
        ]
      );
      historyId += 1;
    }
    for (const [txKey, validatedAt, replacedAt] of transactionHistory[
      node.name
    ]) {
      await client.query(
        'INSERT INTO node_transaction_history (internal_id, node_internal_id, transaction_internal_id, validated_at, replaced_at) VALUES ($1, $2, $3, $4, $5)',
        [
          historyId,
          node.pgId,
          transactions[txKey].pgId,
          pgTimestamp(validatedAt),
          pgTimestamp(replacedAt),
        ]
      );
      historyId += 1;
    }
  }
  await client.query("SET session_replication_role = 'origin'");
};

// ------------------------------------------------------------------ ClickHouse seed

const epochOneSeq = (counter) => (1n << 40n) + BigInt(counter);
const lit = {
  bytes: (buffer) => `unhex('${buffer.toString('hex')}')`,
  nullable: (value, render) =>
    value === undefined || value === null ? 'NULL' : render(value),
  string: (value) =>
    `'${String(value).replace(/\\/g, '\\\\').replace(/'/g, "\\'")}'`,
  time: (ms) =>
    ms === null
      ? 'NULL'
      : `toDateTime64('${new Date(ms)
          .toISOString()
          .replace('T', ' ')
          .replace('Z', '')}', 3, 'UTC')`,
};
const outputLiterals = (output) => [
  lit.bytes(output.category ?? zeroHash),
  lit.nullable(output.ft, (value) => value.toString()),
  lit.nullable(output.capability, lit.string),
  lit.nullable(output.commitment, lit.bytes),
];

const insert = (clickhouse, table, columns, rows) =>
  rows.length === 0
    ? undefined
    : clickhouse.command(
        `INSERT INTO ${table} (${columns.join(', ')}) VALUES ${rows
          .map((row) => `(${row.join(', ')})`)
          .join(', ')}`
      );

/** Commit `seq` and publish it as the watermark of node 0 and both nodes (WP4 gate). */
const commitAndPublish = async (clickhouse, seq, kind = 'block') => {
  await clickhouse.command(
    `INSERT INTO commit_log (commit_seq, state, node_scope, kind, block_hash, row_counts, writer_epoch, started_at, finished_at)
     VALUES (${seq}, 'intent', [${nodes
      .map((node) => node.chId)
      .join(', ')}], '${kind}', unhex('${zeroHash.toString(
      'hex'
    )}'), map(), 1, now64(3), NULL)`
  );
  await clickhouse.command(
    `INSERT INTO commit_log (commit_seq, state, node_scope, kind, block_hash, row_counts, writer_epoch, started_at, finished_at)
     VALUES (${seq}, 'committed', [${nodes
      .map((node) => node.chId)
      .join(', ')}], '${kind}', unhex('${zeroHash.toString(
      'hex'
    )}'), map(), 1, now64(3), now64(3))`
  );
  await insert(
    clickhouse,
    'visibility',
    ['node_internal_id', 'visible_seq', 'updated_at'],
    [0, ...nodes.map((node) => node.chId)].map((id) => [
      id,
      seq.toString(),
      'now64(3)',
    ])
  );
};

const seedClickHouse = async (clickhouse) => {
  const seq = epochOneSeq(1).toString();
  await insert(
    clickhouse,
    'node',
    [
      'internal_id',
      'name',
      'protocol_version',
      'user_agent',
      'first_connected_at',
      'latest_connection_began_at',
      'updated_at',
      'commit_seq',
    ],
    nodes.map((node) => [
      node.chId,
      lit.string(node.name),
      70016,
      lit.string('/Bitcoin Cash Node:28.0.0/'),
      'now64(3)',
      'now64(3)',
      'now64(3)',
      seq,
    ])
  );
  const outputValue = (tx) =>
    tx.outputs.reduce((sum, output) => sum + output.value, 0);
  await insert(
    clickhouse,
    'block',
    [
      'hash',
      'internal_id',
      'height',
      'version',
      'timestamp',
      'previous_block_hash',
      'merkle_root',
      'bits',
      'nonce',
      'size_bytes',
      'transaction_count',
      'output_value_satoshis',
      'generated_value_satoshis',
      'commit_seq',
    ],
    blocks.map((block) => [
      lit.bytes(block.hash),
      block.chId,
      block.height,
      536870912,
      block.timestamp,
      lit.bytes(block.prev ? blockByKey[block.prev].hash : zeroHash),
      lit.bytes(hashOf(`merkle ${block.key}`)),
      486604799,
      1000 + block.pgId,
      1000 + block.height,
      block.txs.length,
      block.txs.reduce((sum, key) => sum + outputValue(transactions[key]), 0),
      5000,
      seq,
    ])
  );
  await insert(
    clickhouse,
    'block_transaction',
    [
      'block_internal_id',
      'transaction_index',
      'transaction_internal_id',
      'transaction_hash',
      'commit_seq',
    ],
    blocks.flatMap((block) =>
      block.txs.map((key, index) => [
        block.chId,
        index,
        transactions[key].chId,
        lit.bytes(transactions[key].hash),
        seq,
      ])
    )
  );
  const txs = Object.values(transactions);
  await insert(
    clickhouse,
    'transaction',
    [
      'hash',
      'internal_id',
      'version',
      'locktime',
      'size_bytes',
      'is_coinbase',
      'input_count',
      'output_count',
      'output_value_satoshis',
      'commit_seq',
    ],
    txs.map((tx) => [
      lit.bytes(tx.hash),
      tx.chId,
      tx.version,
      tx.locktime,
      tx.size,
      tx.coinbase === true ? 'true' : 'false',
      tx.inputs.length,
      tx.outputs.length,
      outputValue(tx),
      seq,
    ])
  );
  await insert(
    clickhouse,
    'output',
    [
      'transaction_hash',
      'output_index',
      'transaction_internal_id',
      'value_satoshis',
      'locking_bytecode',
      'token_category',
      'fungible_token_amount',
      'nonfungible_token_capability',
      'nonfungible_token_commitment',
      'commit_seq',
    ],
    txs.flatMap((tx) =>
      tx.outputs.map((output, index) => [
        lit.bytes(tx.hash),
        index,
        tx.chId,
        output.value,
        lit.bytes(output.lock),
        ...outputLiterals(output),
        seq,
      ])
    )
  );
  await insert(
    clickhouse,
    'input',
    [
      'transaction_hash',
      'input_index',
      'transaction_internal_id',
      'outpoint_transaction_hash',
      'outpoint_index',
      'sequence_number',
      'unlocking_bytecode',
      'value_satoshis',
      'token_category',
      'fungible_token_amount',
      'nonfungible_token_capability',
      'nonfungible_token_commitment',
      'locking_bytecode',
      'commit_seq',
    ],
    txs.flatMap((tx) =>
      tx.inputs.map((input, index) => {
        const spent =
          input.tx === undefined
            ? { lock: Buffer.alloc(0), value: 0 }
            : outputOf(input.tx, input.index);
        const literals = outputLiterals(spent);
        return [
          lit.bytes(tx.hash),
          index,
          tx.chId,
          lit.bytes(input.tx ? transactions[input.tx].hash : zeroHash),
          input.index,
          input.sequence,
          lit.bytes(input.unlocking),
          spent.value,
          literals[0],
          literals[1],
          literals[2],
          literals[3],
          lit.bytes(spent.lock),
          seq,
        ];
      })
    )
  );
  for (const node of nodes) {
    const acceptedAtOf = new Map(nodeBlocks[node.name]);
    await insert(
      clickhouse,
      'node_block',
      [
        'node_internal_id',
        'block_internal_id',
        'block_hash',
        'height',
        'accepted_at',
        'sign',
        'version',
        'commit_seq',
      ],
      nodeBlocks[node.name].map(([key, acceptedAt]) => [
        node.chId,
        blockByKey[key].chId,
        lit.bytes(blockByKey[key].hash),
        blockByKey[key].height,
        lit.time(acceptedAt),
        1,
        seq,
        seq,
      ])
    );
    await insert(
      clickhouse,
      'node_transaction',
      [
        'node_internal_id',
        'transaction_internal_id',
        'transaction_hash',
        'validated_at',
        'sign',
        'version',
        'commit_seq',
      ],
      nodeMempool[node.name].map(([key, validatedAt]) => [
        node.chId,
        transactions[key].chId,
        lit.bytes(transactions[key].hash),
        lit.time(validatedAt),
        1,
        seq,
        seq,
      ])
    );
    await insert(
      clickhouse,
      'tx_acceptance',
      [
        'transaction_hash',
        'node_internal_id',
        'block_internal_id',
        'transaction_internal_id',
        'height',
        'accepted_at',
        'sign',
        'version',
        'commit_seq',
      ],
      [
        ...nodeBlocks[node.name].flatMap(([blockKey]) =>
          blockByKey[blockKey].txs.map((key) => [
            lit.bytes(transactions[key].hash),
            node.chId,
            blockByKey[blockKey].chId,
            transactions[key].chId,
            blockByKey[blockKey].height,
            lit.time(acceptedAtOf.get(blockKey)),
            1,
            seq,
            seq,
          ])
        ),
        ...nodeMempool[node.name].map(([key, validatedAt]) => [
          lit.bytes(transactions[key].hash),
          node.chId,
          0,
          transactions[key].chId,
          0,
          lit.time(validatedAt),
          1,
          seq,
          seq,
        ]),
      ]
    );
    await insert(
      clickhouse,
      'node_block_history',
      [
        'node_internal_id',
        'removed_at',
        'block_internal_id',
        'internal_id',
        'accepted_at',
        'commit_seq',
      ],
      blockHistory[node.name].map(([key, acceptedAt, removedAt], index) => [
        node.chId,
        lit.time(removedAt),
        blockByKey[key].chId,
        500 + index + node.chId * 10,
        lit.time(acceptedAt),
        seq,
      ])
    );
    await insert(
      clickhouse,
      'node_transaction_history',
      [
        'node_internal_id',
        'transaction_internal_id',
        'internal_id',
        'validated_at',
        'replaced_at',
        'commit_seq',
      ],
      transactionHistory[node.name].map(
        ([key, validatedAt, replacedAt], index) => [
          node.chId,
          transactions[key].chId,
          700 + index + node.chId * 10,
          lit.time(validatedAt),
          lit.time(replacedAt),
          seq,
        ]
      )
    );
    const rows = utxoRows(node.name).map(({ index, output, sign, tx }) => ({
      key: [
        lit.bytes(transactions[tx].hash),
        index,
        transactions[tx].chId,
        0,
        output.value,
        lit.bytes(output.lock),
      ],
      output,
      sign,
    }));
    const tail = (row) => {
      const [categoryLiteral, ft, capability, commitment] = outputLiterals(
        row.output
      );
      return {
        categoryLiteral,
        rest: [ft, capability, commitment, row.sign, seq, seq],
      };
    };
    await insert(
      clickhouse,
      'utxo',
      [
        'node_internal_id',
        'token_category',
        'transaction_hash',
        'output_index',
        'transaction_internal_id',
        'created_height',
        'value_satoshis',
        'locking_bytecode',
        'fungible_token_amount',
        'nonfungible_token_capability',
        'nonfungible_token_commitment',
        'sign',
        'version',
        'commit_seq',
      ],
      rows.map((row) => [
        node.chId,
        tail(row).categoryLiteral,
        ...row.key,
        ...tail(row).rest,
      ])
    );
    await insert(
      clickhouse,
      'utxo_by_script',
      [
        'node_internal_id',
        'transaction_hash',
        'output_index',
        'transaction_internal_id',
        'created_height',
        'value_satoshis',
        'locking_bytecode',
        'token_category',
        'fungible_token_amount',
        'nonfungible_token_capability',
        'nonfungible_token_commitment',
        'sign',
        'version',
        'commit_seq',
      ],
      rows.map((row) => [
        node.chId,
        ...row.key,
        tail(row).categoryLiteral,
        ...tail(row).rest,
      ])
    );
  }
  await commitAndPublish(clickhouse, epochOneSeq(1));
};

// ------------------------------------------------------------------ runs

const results = [];
const record = (name, ok, detail) => {
  results.push({ detail, name, ok });
  console.log(
    `${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`
  );
};

const runCompare = (label, connection, extra) => {
  const out = join(options.out, label);
  const run = spawnSync(
    process.execPath,
    [
      join(harnessDirectory, 'compare.mjs'),
      '--pg',
      connection.pg,
      '--ch',
      connection.ch,
      '--ch-db',
      connection.chDb,
      '--nodes',
      'node-a,node-b',
      '--out',
      out,
      '--parallel',
      '3',
      '--quiet',
      ...extra,
    ],
    { encoding: 'utf8' }
  );
  if (run.status === 2)
    throw new Error(`compare ${label} errored:\n${run.stderr}${run.stdout}`);
  const summary = JSON.parse(readFileSync(join(out, 'summary.json'), 'utf8'));
  const tsv = readFileSync(join(out, 'parity.tsv'), 'utf8');
  return {
    diff: extra.includes('--diff')
      ? readFileSync(join(out, 'diff.txt'), 'utf8')
      : '',
    exit: run.status,
    summary,
    tsv,
  };
};

/** Expect exit code and exactly this set of mismatched node/table pairs. */
const expectRun = (label, connection, extra, expectedTables) => {
  const { diff, exit, summary, tsv } = runCompare(label, connection, extra);
  const got = [...summary.mismatchedTables].sort();
  const want = [...expectedTables].sort();
  const chunkRows = tsv.trim().split('\n').length - 1;
  const ok =
    JSON.stringify(got) === JSON.stringify(want) &&
    exit === (want.length === 0 ? 0 : 1);
  record(
    label,
    ok,
    `${chunkRows} rows, exit ${exit}, mismatched ${
      got.length === 0
        ? 'none'
        : got.map((pair) => pair.replace('\t', '/')).join(', ')
    }${
      summary.skipped.length > 0
        ? `; skipped: ${summary.skipped.join('; ')}`
        : ''
    }`
  );
  if (diff) console.log(diff.trimEnd().replace(/^/gm, '      '));
  return summary;
};

const digestPrimitive = async (postgres, clickhouse) => {
  const strings = [
    '',
    'a',
    'a|b|NULL',
    '00ff|0|set|2026-10-09T10:00:00.000Z',
    'x'.repeat(5000),
    'a|b|NULL',
  ];
  const pgValues = strings.map((value) => `'${value}'`).join(', ');
  const chValues = strings.map((value) => `'${value}'`).join(', ');
  const { rows } = await postgres.query(`SELECT count(*)::text AS n,
      coalesce(sum(('x' || substr(md5(s), 1, 16))::bit(64)::bigint::numeric), 0)::text AS a,
      coalesce(sum(('x' || substr(md5(s), 17, 16))::bit(64)::bigint::numeric), 0)::text AS b,
      md5(string_agg(s, E'\\n' ORDER BY s COLLATE "C")) AS m
    FROM unnest(ARRAY[${pgValues}]::text[]) AS s`);
  const [chRow] = await clickhouse.query(`SELECT toString(count()) AS n,
      toString(sum(reinterpretAsUInt64(reverse(substring(MD5(s), 1, 8))))) AS a,
      toString(sum(reinterpretAsUInt64(reverse(substring(MD5(s), 9, 8))))) AS b,
      lower(hex(MD5(arrayStringConcat(arraySort(groupArray(s)), '\\n')))) AS m
    FROM (SELECT arrayJoin([${chValues}]) AS s)`);
  const reference = referenceDigest(strings);
  const orderedReference = createHash('md5')
    .update([...strings].sort().join('\n'))
    .digest('hex');
  const pgSum = digestFromSums(rows[0].a, rows[0].b);
  const chSum = digestFromSums(chRow.a, chRow.b);
  record(
    'digest primitive: JS = Postgres = ClickHouse (sum and ordered md5)',
    pgSum === reference &&
      chSum === reference &&
      rows[0].m === orderedReference &&
      chRow.m === orderedReference,
    `sum ${reference}, ordered ${orderedReference}`
  );
};

const main = async () => {
  mkdirSync(options.out, { recursive: true });
  const suffix = randomBytes(4).toString('hex');
  const database = `ch1_parity_${suffix}`;
  const adminUrl = new URL(options['pg-admin']);
  const pgUrl = new URL(adminUrl);
  pgUrl.pathname = `/${database}`;
  const admin = new pg.Client({ connectionString: adminUrl.toString() });
  await admin.connect();
  const clickhouse = new ClickHouseHttp(options.ch, database);
  const clickhouseRoot = new ClickHouseHttp(options.ch, 'default');
  let postgres;
  try {
    await admin.query(`CREATE DATABASE ${database}`);
    postgres = new pg.Client({ connectionString: pgUrl.toString() });
    await postgres.connect();
    const migrations = readdirSync(migrationsDirectory).sort();
    for (const migration of migrations)
      await postgres.query(
        readFileSync(join(migrationsDirectory, migration, 'up.sql'), 'utf8')
      );
    for (const statement of agentIndexes) await postgres.query(statement);
    await postgres.query(
      readFileSync(join(harnessDirectory, 'sql/f1g-unspent-output.sql'), 'utf8')
    );
    execFileSync('bash', [ddlApply, options.ch, database], { stdio: 'ignore' });
    console.log(
      `scratch: postgres ${database} (${migrations.length} migrations + agent indexes + F1g), clickhouse ${database}; out ${options.out}`
    );

    await digestPrimitive(postgres, clickhouse);
    await seedPostgres(postgres);
    await seedClickHouse(clickhouse);
    const connection = { ch: options.ch, chDb: database, pg: pgUrl.toString() };
    const mempool = ['--include-mempool'];
    const small = [
      '--chunk-blocks',
      '2',
      '--hash-chunks',
      '16',
      '--utxo-chunks',
      '16',
    ];

    expectRun(
      'identical: sum digest, exact timestamps, mempool',
      connection,
      [...small, ...mempool, '--timestamps', 'exact'],
      []
    );
    expectRun(
      'identical: ordered md5, exact timestamps, mempool',
      connection,
      [...small, ...mempool, '--hash', 'ordered', '--timestamps', 'exact'],
      []
    );
    expectRun(
      'identical: sum digest, timestamp tolerance 0 ms, mempool',
      connection,
      [...small, ...mempool, '--ts-tolerance-ms', '0'],
      []
    );
    expectRun(
      'identical: no mempool, 256 hash chunks',
      connection,
      ['--chunk-blocks', '1', '--hash-chunks', '256'],
      []
    );
    expectRun(
      'identical: --at-height 2 (utxo only where both tips are 2)',
      connection,
      [...small, '--at-height', '2'],
      []
    );
    expectRun(
      'identical: --every 1 --from 0 --to 3 (windows + cumulative)',
      connection,
      ['--every', '1', '--from', '0', '--to', '3'],
      []
    );

    // Mutation 1: node-b gains a node_block fact (B3) in ClickHouse.
    const b3 = blockByKey.B3;
    const nodeB = nodes[1];
    const nodeBlockRow = (sign, seq) =>
      `INSERT INTO node_block (node_internal_id, block_internal_id, block_hash, height, accepted_at, sign, version, commit_seq)
       VALUES (${nodeB.chId}, ${b3.chId}, ${lit.bytes(b3.hash)}, 3, ${lit.time(
        at(181)
      )}, ${sign}, ${seq}, ${seq})`;
    await clickhouse.command(nodeBlockRow(1, epochOneSeq(2)));
    await commitAndPublish(clickhouse, epochOneSeq(2));
    expectRun(
      'mutation: node-b gains node_block B3 (ClickHouse)',
      connection,
      [...small, ...mempool, '--timestamps', 'exact', '--diff'],
      ['node-b\tnode_block']
    );
    await clickhouse.command(nodeBlockRow(-1, epochOneSeq(3)));
    await commitAndPublish(clickhouse, epochOneSeq(3));
    expectRun(
      'reverted (a -1 row in a later commit)',
      connection,
      [...small, ...mempool, '--timestamps', 'exact'],
      []
    );

    // Mutation 2: node-b gains a tx_acceptance fact (T3 in B3) in ClickHouse.
    const t3 = transactions.T3;
    const accRow = (sign, seq) =>
      `INSERT INTO tx_acceptance (transaction_hash, node_internal_id, block_internal_id, transaction_internal_id, height, accepted_at, sign, version, commit_seq)
       VALUES (${lit.bytes(t3.hash)}, ${nodeB.chId}, ${b3.chId}, ${
        t3.chId
      }, 3, ${lit.time(at(181))}, ${sign}, ${seq}, ${seq})`;
    await clickhouse.command(accRow(1, epochOneSeq(4)));
    await commitAndPublish(clickhouse, epochOneSeq(4));
    expectRun(
      'mutation: node-b gains tx_acceptance T3@B3 (ClickHouse)',
      connection,
      [...small, ...mempool, '--diff'],
      ['node-b\ttx_acceptance']
    );
    await clickhouse.command(accRow(-1, epochOneSeq(5)));
    await commitAndPublish(clickhouse, epochOneSeq(5));

    // Mutation 3: a spent output's value changes in Postgres (node-agnostic base fact).
    await postgres.query(
      `UPDATE output SET value_satoshis = value_satoshis + 1 WHERE transaction_hash = '\\x${transactions.T1.hash.toString(
        'hex'
      )}' AND output_index = 3`
    );
    expectRun(
      'mutation: output T1:3 value +1 (Postgres)',
      connection,
      [...small, ...mempool, '--diff'],
      ['*\toutput']
    );
    await postgres.query(
      `UPDATE output SET value_satoshis = value_satoshis - 1 WHERE transaction_hash = '\\x${transactions.T1.hash.toString(
        'hex'
      )}' AND output_index = 3`
    );

    // Mutation 4: node-a's accepted_at of B1 drifts by +3 s in ClickHouse (a later +1 row wins argMaxIf).
    const nodeA = nodes[0];
    const b1 = blockByKey.B1;
    const driftRow = (acceptedAt, sign, version, seq) =>
      `INSERT INTO node_block (node_internal_id, block_internal_id, block_hash, height, accepted_at, sign, version, commit_seq)
       VALUES (${nodeA.chId}, ${b1.chId}, ${lit.bytes(b1.hash)}, 1, ${lit.time(
        acceptedAt
      )}, ${sign}, ${version}, ${seq})`;
    await clickhouse.command(
      driftRow(at(63), 1, epochOneSeq(6), epochOneSeq(6))
    );
    await commitAndPublish(clickhouse, epochOneSeq(6));
    expectRun(
      'mutation: accepted_at +3 s, tolerance 1 s',
      connection,
      [...small, ...mempool, '--ts-tolerance-ms', '1000'],
      ['node-a\tnode_block:accepted_at']
    );
    expectRun(
      'mutation: accepted_at +3 s, tolerance 5 s',
      connection,
      [...small, ...mempool, '--ts-tolerance-ms', '5000'],
      []
    );
    expectRun(
      'mutation: accepted_at +3 s, exact timestamps',
      connection,
      [...small, ...mempool, '--timestamps', 'exact'],
      ['node-a\tnode_block']
    );
    // revert: cancel both +1 rows and re-accept with the original time at a higher version
    await clickhouse.command(
      driftRow(at(63), -1, epochOneSeq(6), epochOneSeq(7))
    );
    await clickhouse.command(
      driftRow(at(60), -1, epochOneSeq(1), epochOneSeq(7))
    );
    await clickhouse.command(
      driftRow(at(60), 1, epochOneSeq(7), epochOneSeq(7))
    );
    await commitAndPublish(clickhouse, epochOneSeq(7));
    expectRun(
      'all mutations reverted',
      connection,
      [...small, ...mempool, '--timestamps', 'exact'],
      []
    );
  } finally {
    if (postgres !== undefined) await postgres.end();
    if (!options.keep) {
      await admin.query(`DROP DATABASE IF EXISTS ${database}`);
      await clickhouseRoot.command(`DROP DATABASE IF EXISTS ${database}`);
    } else {
      console.log(`kept scratch databases ${database}`);
    }
    await admin.end();
  }
  writeFileSync(
    join(options.out, 'selftest.json'),
    `${JSON.stringify(results, null, 2)}\n`
  );
  const failed = results.filter((result) => !result.ok).length;
  console.log(`selftest: ${results.length - failed}/${results.length} passed`);
  return failed === 0 ? 0 : 1;
};

main().then(
  (code) => process.exit(code),
  (error) => {
    console.error(error);
    process.exit(2);
  }
);
