/* eslint-disable @typescript-eslint/no-magic-numbers, max-params, @typescript-eslint/require-array-sort-compare */
// cspell:ignore clickhouse unhex aabb varint seqs
/**
 * Shared fixtures of the ClickHouse store specs (`clickhouse-store.spec.ts`,
 * `mempool-commit.spec.ts`): transactions, blocks, scratch databases
 * (`ch1_wp5a_*`, dropped on teardown) and per-node views.
 */
import { execFile } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

import type { ExecutionContext } from 'ava';

import type {
  ChaingraphBlock,
  ChaingraphOutput,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import type { FaultInjector } from './block-commit.js';
import type { ClickHouseStoreOptions } from './clickhouse-store.js';
import { ClickHouseStore, linkedBlockSize } from './clickhouse-store.js';
import { ClickHouseClient } from './client.js';
import { e2eClickHouseUrl } from './test-support.js';
import { pinnedView, readSnapshot, snapshotParams } from './visibility.js';

const execFileAsync = promisify(execFile);

/* -------------------------------------------------------------------- */
/* fixtures                                                              */
/* -------------------------------------------------------------------- */

export const sha = (label: string) =>
  createHash('sha256').update(label).digest('hex');
export const zeroHash = '00'.repeat(32);
export const category = sha('category');

export interface TxSpec {
  label: string;
  spends?: [string, number][];
  outputs: ChaingraphOutput[];
  coinbase?: boolean;
}

export const makeTx = (spec: TxSpec): ChaingraphTransaction => ({
  hash: sha(spec.label),
  inputs:
    spec.coinbase === true
      ? [
          {
            outpointIndex: 0xffffffff,
            outpointTransactionHash: zeroHash,
            sequenceNumber: 0xffffffff,
            unlockingBytecode: Buffer.from(spec.label).toString('hex'),
          },
        ]
      : (spec.spends ?? []).map(([hash, index]) => ({
          outpointIndex: index,
          outpointTransactionHash: hash,
          sequenceNumber: 0xfffffffe,
          unlockingBytecode: 'aabb',
        })),
  isCoinbase: spec.coinbase === true,
  locktime: 0,
  outputs: spec.outputs,
  sizeBytes: 100 + spec.label.length,
  version: 2,
});

export const makeBlock = (
  height: number,
  previousBlockHash: string,
  transactions: ChaingraphTransaction[],
  label = `block-${height}`,
  timestamp = 1_600_000_000 + height * 600
): ChaingraphBlock => ({
  bits: 0x1d00ffff,
  hash: sha(label),
  height,
  merkleRoot: sha(`${label}-merkle`),
  nonce: height,
  previousBlockHash,
  sizeBytes: linkedBlockSize(
    transactions.length,
    transactions.reduce((sum, tx) => sum + tx.sizeBytes, 0)
  ),
  timestamp,
  transactions,
  version: 4,
});

export const p2pkh = (label: string) => `76a914${sha(label).slice(0, 40)}88ac`;

/**
 * A 3-block chain with a transaction chain across blocks:
 * - block 0: coinbase c0 (2 outputs, the second a token output: FT + NFT)
 * - block 1: coinbase c1; tx a spends c0:0 → a:0 (FT), a:1
 * - block 2: coinbase c2; tx b spends a:0 and c0:1 → b:0
 */
export const threeBlockChain = () => {
  const c0 = makeTx({
    coinbase: true,
    label: 'c0',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
      {
        fungibleTokenAmount: 1_000n,
        lockingBytecode: p2pkh('holder'),
        nonfungibleTokenCapability: 'mutable',
        nonfungibleTokenCommitment: '01',
        tokenCategory: category,
        valueSatoshis: 1_000n,
      },
    ],
  });
  const block0 = makeBlock(0, zeroHash, [c0]);
  const c1 = makeTx({
    coinbase: true,
    label: 'c1',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
    ],
  });
  const a = makeTx({
    label: 'a',
    outputs: [
      {
        fungibleTokenAmount: 400n,
        lockingBytecode: p2pkh('alice'),
        tokenCategory: category,
        valueSatoshis: 800n,
      },
      { lockingBytecode: p2pkh('change'), valueSatoshis: 4_999_998_000n },
    ],
    spends: [[c0.hash, 0]],
  });
  const block1 = makeBlock(1, block0.hash, [c1, a]);
  const c2 = makeTx({
    coinbase: true,
    label: 'c2',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_500n },
    ],
  });
  const b = makeTx({
    label: 'b',
    outputs: [
      {
        fungibleTokenAmount: 400n,
        lockingBytecode: p2pkh('bob'),
        tokenCategory: category,
        valueSatoshis: 1_300n,
      },
    ],
    spends: [
      [a.hash, 0],
      [c0.hash, 1],
    ],
  });
  const block2 = makeBlock(2, block1.hash, [c2, b]);
  return { a, b, block0, block1, block2, c0, c1, c2 };
};

/** Definitional unspent(n, o) from the blocks a node accepts. */
export const expectedUnspent = (blocks: readonly ChaingraphBlock[]) => {
  const txs = blocks.flatMap((block) => block.transactions);
  const spent = new Set(
    txs
      .filter((tx) => !tx.isCoinbase)
      .flatMap((tx) =>
        tx.inputs.map(
          (input) => `${input.outpointTransactionHash}:${input.outpointIndex}`
        )
      )
  );
  return txs
    .flatMap((tx) => tx.outputs.map((_, index) => `${tx.hash}:${index}`))
    .filter((key) => !spent.has(key))
    .sort();
};

/* -------------------------------------------------------------------- */
/* scratch databases and stores                                          */
/* -------------------------------------------------------------------- */

/** `CHAINGRAPH_E2E_DDL_APPLY`: another DDL checkout's apply.sh (e.g. a pinned copy). */
const applyScript =
  process.env.CHAINGRAPH_E2E_DDL_APPLY ??
  fileURLToPath(
    new URL('../../../src/store/clickhouse/ddl/apply.sh', import.meta.url)
  );

export const connectionFor = (database: string) => ({
  database,
  password: process.env.CHAINGRAPH_E2E_CLICKHOUSE_PASSWORD ?? '',
  requestTimeoutMs: 120_000,
  url: e2eClickHouseUrl!,
  username: process.env.CHAINGRAPH_E2E_CLICKHOUSE_USER ?? '',
});

export const leaseTtlMs = 1_500;

export const scratch = async (
  t: ExecutionContext,
  label: string,
  storeOptions: Partial<ClickHouseStoreOptions> = {},
  prefix = 'ch1_wp5a'
) => {
  const name = `${prefix}_${label}_${randomBytes(4).toString('hex')}`;
  await execFileAsync('bash', [applyScript, e2eClickHouseUrl!, name], {
    env: process.env,
    timeout: 120_000,
  });
  const client = new ClickHouseClient(connectionFor(name));
  const stores: ClickHouseStore[] = [];
  t.teardown(async () => {
    await Promise.all(
      stores.map(async (store) => store.close().catch(() => undefined))
    );
    if (process.env.WP5A_KEEP_DB === undefined) {
      await client.command(`DROP DATABASE IF EXISTS ${name}`);
    }
    await client.close();
  });
  const openStore = async (fault?: FaultInjector) => {
    const store = new ClickHouseStore({
      connection: connectionFor(name),
      fault,
      lease: { safetyMarginMs: 500, settleMs: 20, ttlMs: leaseTtlMs },
      pendingSpendTimeoutMs: 20_000,
      publisher: { minIntervalMs: 10 },
      ...storeOptions,
    });
    await store.init();
    stores.push(store);
    return store;
  };
  return { client, name, openStore };
};

export const registerNodes = async (store: ClickHouseStore) => {
  const node1 = await store.registerNode({
    latestConnectionBeganAt: new Date('2026-10-09T00:00:00Z'),
    nodeName: 'node-one',
    protocolVersion: 70016,
    userAgent: '/BCHN:28.0.0/',
  });
  const node2 = await store.registerNode({
    latestConnectionBeganAt: new Date('2026-10-09T00:00:00Z'),
    nodeName: 'node-two',
    protocolVersion: 70016,
    userAgent: '/BCHN:28.0.0/',
  });
  return { node1: node1.internalId, node2: node2.internalId };
};

export const acceptance = (
  nodeInternalId: number,
  acceptedAt: Date | null = null
) => ({
  acceptedAt,
  nodeInternalId,
  nodeName: `node-${nodeInternalId}`,
});

export const notSaved = () => false;

/** What a reader of node n sees, through the pinned `*_at` views of one snapshot. */
export const nodeView = async (client: ClickHouseClient, node: number) => {
  const snapshot = await readSnapshot(client, node);
  const params = snapshotParams(snapshot);
  const [blocks, txs, utxo, utxoByScript, history, inputs] = await Promise.all([
    client.query<{ hash: string; height: number; accepted: string | null }>(
      `SELECT lower(hex(block_hash)) AS hash, height, toString(accepted_at) AS accepted
       FROM ${pinnedView('node_block_at')} ORDER BY height`,
      params
    ),
    client.query<{ hash: string; block: string }>(
      `SELECT lower(hex(transaction_hash)) AS hash, toString(block_internal_id) AS block
       FROM ${pinnedView('tx_acceptance_at')} ORDER BY hash`,
      params
    ),
    client.query<{ key: string; amount: string | null; category: string }>(
      `SELECT concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS key,
         fungible_token_amount AS amount, lower(hex(token_category)) AS category
       FROM ${pinnedView('utxo_at')} ORDER BY key`,
      params
    ),
    client.query<{ key: string }>(
      `SELECT concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS key
       FROM ${pinnedView('utxo_by_script_at')} ORDER BY key`,
      params
    ),
    client.query<{ block: string; removed: string }>(
      `SELECT toString(block_internal_id) AS block, toString(removed_at) AS removed
       FROM ${pinnedView('node_block_history_at')}`,
      params
    ),
    client.query<{ key: string; amount: string | null; value: string }>(
      `SELECT concat(lower(hex(transaction_hash)), ':', toString(input_index)) AS key,
         fungible_token_amount AS amount, toString(value_satoshis) AS value
       FROM ${pinnedView('input_at')} ORDER BY key`,
      params
    ),
  ]);
  return {
    blockRows: blocks,
    blocks: blocks.map((row) => row.hash),
    history,
    inputs,
    txs: [...new Set(txs.map((row) => row.hash))].sort(),
    utxo: utxo.map((row) => row.key),
    utxoByScript: utxoByScript.map((row) => row.key),
    utxoRows: utxo,
  };
};

/** Every (node, outpoint) sum over valid (non-void) rows is 0 or 1. */
export const badUtxoSums = async (client: ClickHouseClient) =>
  client.query<{ node: number; key: string; s: string }>(
    `SELECT node_internal_id AS node, concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS key,
       sum(sign) AS s
     FROM utxo WHERE commit_seq NOT IN (SELECT commit_seq FROM commit_void)
     GROUP BY node, transaction_hash, output_index HAVING s NOT IN (0, 1)`
  );

export const txHashes = (blocks: readonly ChaingraphBlock[]) =>
  [
    ...new Set(
      blocks.flatMap((block) => block.transactions.map((tx) => tx.hash))
    ),
  ].sort();
