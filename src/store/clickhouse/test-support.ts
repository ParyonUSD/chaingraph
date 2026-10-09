/* eslint-disable camelcase, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, complexity */
// cspell:ignore clickhouse
/**
 * Test support for the ClickHouse store's `[e2e]` specs (WP4): scratch
 * databases with the full DDL applied, and a minimal per-node "save" written
 * through the real commit protocol. Not used by production code.
 *
 * The specs run only when CHAINGRAPH_E2E_CLICKHOUSE_URL is set (local only;
 * scratch databases are named `ch1_wp4_*` and dropped on teardown).
 */
import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import { ClickHouseClient } from './client.js';
import type { OpenCommit } from './commit-log.js';
import { RowBinaryWriter } from './row-binary.js';

const execFileAsync = promisify(execFile);

export const e2eClickHouseUrl = process.env.CHAINGRAPH_E2E_CLICKHOUSE_URL;

const applyScript = fileURLToPath(
  new URL('../../../src/store/clickhouse/ddl/apply.sh', import.meta.url)
);

const scratchSuffixBytes = 4;
const applyTimeoutMs = 120_000;

export interface ScratchDatabase {
  name: string;
  client: ClickHouseClient;
  /** A second client (e.g. a reader or a second writer) on the same database. */
  newClient: () => ClickHouseClient;
  drop: () => Promise<void>;
}

/** Create `ch1_wp4_<label>_<random>` with ddl/*.sql applied. */
export const createScratchDatabase = async (
  label: string
): Promise<ScratchDatabase> => {
  const url = e2eClickHouseUrl;
  if (url === undefined) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error('CHAINGRAPH_E2E_CLICKHOUSE_URL is not set.');
  }
  const name = `ch1_wp4_${label}_${randomBytes(scratchSuffixBytes).toString(
    'hex'
  )}`;
  await execFileAsync('bash', [applyScript, url, name], {
    env: process.env,
    timeout: applyTimeoutMs,
  });
  const newClient = () =>
    new ClickHouseClient({
      database: name,
      password: process.env.CHAINGRAPH_E2E_CLICKHOUSE_PASSWORD ?? '',
      requestTimeoutMs: applyTimeoutMs,
      url,
      username: process.env.CHAINGRAPH_E2E_CLICKHOUSE_USER ?? '',
    });
  const client = newClient();
  return {
    client,
    drop: async () => {
      await client.command(`DROP DATABASE IF EXISTS ${name}`);
      await client.close();
    },
    name,
    newClient,
  };
};

/** A distinct 32-byte hash per integer. */
export const hashOf = (value: number) => {
  const bytes = Buffer.alloc(32);
  bytes.writeUInt32BE(value, 28);
  return bytes;
};

const outputColumns = [
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
];

const txAcceptanceColumns = [
  'transaction_hash',
  'node_internal_id',
  'block_internal_id',
  'transaction_internal_id',
  'height',
  'accepted_at',
  'sign',
  'version',
  'commit_seq',
];

const utxoColumns = [
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
];

export interface TestSave {
  nodeId: number;
  transactionId: number;
  outputs: number;
  category: Buffer;
}

/**
 * The data steps of a minimal "mempool accept of one tx by one node":
 * step 0 node-agnostic `output`, step 1 `tx_acceptance` (+1), step 2 `utxo`
 * and `utxo_by_script` (+1 per output). Each step is one deduplicated insert.
 */
export const testSaveSteps = (
  client: ClickHouseClient,
  commit: OpenCommit,
  save: TestSave
): (() => Promise<void>)[] => {
  const txHash = hashOf(save.transactionId);
  const outputRows = () => {
    const writer = new RowBinaryWriter();
    // eslint-disable-next-line functional/no-loop-statement, functional/no-let
    for (let index = 0; index < save.outputs; index += 1) {
      writer
        .fixedString32(txHash)
        .uint32(index)
        .uint64(save.transactionId)
        .int64(1000 + index)
        .string(`script-${save.transactionId}-${index}`)
        .fixedString32(save.category)
        .nullable(null, () => undefined)
        .nullable(null, () => undefined)
        .nullable(null, () => undefined)
        .uint64(commit.seq)
        .endRow();
    }
    return writer.finish();
  };
  const utxoRows = () => {
    const writer = new RowBinaryWriter();
    // eslint-disable-next-line functional/no-loop-statement, functional/no-let
    for (let index = 0; index < save.outputs; index += 1) {
      writer
        .uint32(save.nodeId)
        .fixedString32(save.category)
        .fixedString32(txHash)
        .uint32(index)
        .uint64(save.transactionId)
        .uint32(0)
        .int64(1000 + index)
        .string(`script-${save.transactionId}-${index}`)
        .nullable(null, () => undefined)
        .nullable(null, () => undefined)
        .nullable(null, () => undefined)
        .int8(1)
        .uint64(1)
        .uint64(commit.seq)
        .endRow();
    }
    return writer.finish();
  };
  const utxoByScriptColumns = utxoColumns.filter(
    (column) => column !== 'token_category'
  );
  const utxoByScriptRows = () => {
    const writer = new RowBinaryWriter();
    // eslint-disable-next-line functional/no-loop-statement, functional/no-let
    for (let index = 0; index < save.outputs; index += 1) {
      writer
        .uint32(save.nodeId)
        .fixedString32(txHash)
        .uint32(index)
        .uint64(save.transactionId)
        .uint32(0)
        .int64(1000 + index)
        .string(`script-${save.transactionId}-${index}`)
        .nullable(null, () => undefined)
        .nullable(null, () => undefined)
        .nullable(null, () => undefined)
        .int8(1)
        .uint64(1)
        .uint64(commit.seq)
        .endRow();
    }
    return writer.finish();
  };
  return [
    async () =>
      client.insertRowBinary('output', outputColumns, outputRows(), {
        deduplicationToken: commit.token('output'),
      }),
    async () =>
      client.insertRowBinary(
        'tx_acceptance',
        txAcceptanceColumns,
        new RowBinaryWriter()
          .fixedString32(txHash)
          .uint32(save.nodeId)
          .uint64(0)
          .uint64(save.transactionId)
          .uint32(0)
          .nullable(null, () => undefined)
          .int8(1)
          .uint64(commit.seq)
          .uint64(commit.seq)
          .endRow()
          .finish(),
        { deduplicationToken: commit.token('tx_acceptance') }
      ),
    async () => {
      await client.insertRowBinary('utxo', utxoColumns, utxoRows(), {
        deduplicationToken: commit.token('utxo'),
      });
      await client.insertRowBinary(
        'utxo_by_script',
        utxoByScriptColumns,
        utxoByScriptRows(),
        { deduplicationToken: commit.token('utxo_by_script') }
      );
    },
  ];
};

export const testSaveRowCounts = (save: TestSave) => ({
  output: save.outputs,
  tx_acceptance: 1,
  utxo: save.outputs,
  utxo_by_script: save.outputs,
});

/** What a reader of `nodeId` sees of a test save through the live views. */
export const visibleOfSave = async (
  client: ClickHouseClient,
  save: TestSave
) => {
  const params = { node: save.nodeId, tx: save.transactionId };
  const [acceptance, utxo, utxoByScript, outputs] = await Promise.all([
    client.query<{ c: string }>(
      'SELECT count() AS c FROM tx_acceptance_v(node = {node:UInt32}) WHERE transaction_internal_id = {tx:UInt64}',
      params
    ),
    client.query<{ c: string }>(
      'SELECT count() AS c FROM utxo_v(node = {node:UInt32}) WHERE transaction_internal_id = {tx:UInt64}',
      params
    ),
    client.query<{ c: string }>(
      'SELECT count() AS c FROM utxo_by_script_v(node = {node:UInt32}) WHERE transaction_internal_id = {tx:UInt64}',
      params
    ),
    client.query<{ c: string }>(
      'SELECT count() AS c FROM output_v WHERE transaction_internal_id = {tx:UInt64}',
      params
    ),
  ]);
  return {
    output: Number(outputs[0]?.c),
    tx_acceptance: Number(acceptance[0]?.c),
    utxo: Number(utxo[0]?.c),
    utxo_by_script: Number(utxoByScript[0]?.c),
  };
};

/** `'all'`, `'none'` or `'torn'` for the node-n facts of a save. */
export const nodeFactsVisibility = (
  seen: {
    tx_acceptance: number;
    utxo: number;
    utxo_by_script: number;
  },
  save: TestSave
) => {
  if (
    seen.tx_acceptance === 0 &&
    seen.utxo === 0 &&
    seen.utxo_by_script === 0
  ) {
    return 'none';
  }
  if (
    seen.tx_acceptance === 1 &&
    seen.utxo === save.outputs &&
    seen.utxo_by_script === save.outputs
  ) {
    return 'all';
  }
  return 'torn';
};
