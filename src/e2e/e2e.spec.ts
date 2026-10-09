/* eslint-disable max-lines */
import { readFileSync } from 'fs';
import { dirname, resolve } from 'path';
import { fileURLToPath } from 'url';
import { inspect } from 'util';

import {
  binToHex,
  encodeDataPush,
  flattenBinArray,
  hexToBin,
  swapEndianness,
  utf8ToBin,
} from '@bitauth/libauth';
import type {
  BitcoreBlock,
  GetDataMessage,
  GetHeadersMessage,
  Peer,
} from '@chaingraph/bitcore-p2p-cash';
import bitcoreP2pCash, {
  BitcoreInventoryType,
} from '@chaingraph/bitcore-p2p-cash';
import test from 'ava';
import type { ExecaChildProcess } from 'execa';
import { execa } from 'execa';
import got from 'got';
import pg from 'pg';

import { indexDefinitions } from '../components/db-utils.js';
import type * as DbModule from '../db.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

import { chaingraphE2eLogPath, logger } from './e2e.spec.logging.helper.js';
import {
  chipnetCashTokensTx,
  chipnetCashTokensTxHash,
  generateMockchain,
  generateMockDoubleSpend,
  genesisBlock,
  genesisBlockRaw,
  halTxHash,
  halTxRaw,
  halTxSpent,
  halTxSpentRaw,
  selectHeaders,
  testnetGenesisBlockRaw,
  Transaction,
} from './e2e.spec.mockchain.helper.js';

// eslint-disable-next-line @typescript-eslint/naming-convention
const { Pool, internalBitcore } = bitcoreP2pCash;

/**
 * Set to `true` to log all P2P messages.
 */
const logP2pMessage = false as boolean;

logger.info('\n\n---- Beginning new E2E test run. ----\n');

const e2eTestDbName = 'chaingraph_e2e_test';
const recreateDbOnStartup = true as boolean;
const dir = dirname(fileURLToPath(import.meta.url));
const migration = (path: string) =>
  resolve(dir, '../../images/hasura/hasura-data/migrations/', path);
const backfillOrphanMempoolDescendantsMigrationPath = migration(
  'default/1778158619747_backfill_orphan_mempool_descendants/up.sql'
);
const dbUpMigrationPaths = [
  migration('default/1616195337538_init/up.sql'),
  migration('default/1673124945608_tokens/up.sql'),
  migration('default/1676794104752_parse_bytecode_pattern/up.sql'),
  migration(
    'default/1778151011521_cascade_invalidate_mempool_descendants/up.sql'
  ),
  backfillOrphanMempoolDescendantsMigrationPath,
  migration('default/1778351597200_fix_block_encoded_transaction_count/up.sql'),
  migration(
    'default/1778415174939_fix_data_carrier_outputs_empty_bytecode/up.sql'
  ),
  migration('default/1778429124205_fix_coinbase_only_value_aggregates/up.sql'),
  migration(
    'default/1778435997270_add_node_transaction_history_primary_key/up.sql'
  ),
  migration('default/1778437612917_fix_zero_length_pushdata_patterns/up.sql'),
  migration('default/1778438318512_fix_redeem_bytecode_parser/up.sql'),
  migration('default/1791100000000_fix_search_output_exact_matching/up.sql'),
  migration(
    'default/1791100001000_fix_search_output_prefix_literal_bytes/up.sql'
  ),
  migration('default/1791400000000_unspent_tracking/up.sql'),
  migration('default/1791400001000_unspent_post_commit/up.sql'),
];

const chaingraphInternalApiPort = '3201';

/**
 * TODO: test multiple network magic values
 */
/* eslint-disable no-bitwise, @typescript-eslint/no-magic-numbers */
// cspell:disable-next-line
const e2eTestNetworkMagic = Buffer.from(utf8ToBin('grph').map((x) => x | 128));
/* eslint-enable no-bitwise, @typescript-eslint/no-magic-numbers */
const e2eTestNetworkMagicHex = e2eTestNetworkMagic.toString('hex');
const e2eTestNetworkMagicAsNum = parseInt(e2eTestNetworkMagicHex, 16);
const e2eTestNetworkNode1Port = 19333;
const e2eTestNetworkNode2Port = 19334;
const e2eTestNetworkNode3Port = 19335;
const node1Version = 70012;
const node1UserAgent = '/chaingraph-e2e-node-1:0.0.0/';
const node2Version = 70013;
const node2UserAgent = '/chaingraph-e2e-node-2:0.0.0/';
const node3Version = 70014;
const node3UserAgent = '/chaingraph-e2e-node-3:0.0.0/';

const e2eTrustedNodesSet1 = `node1:127.0.0.1:${e2eTestNetworkNode1Port}:${e2eTestNetworkMagicHex},node2:127.0.0.1:${e2eTestNetworkNode2Port}:${e2eTestNetworkMagicHex},node3:127.0.0.1:${e2eTestNetworkNode3Port}:${e2eTestNetworkMagicHex}`;

const e2eTrustedNodesSet2 = `node1:127.0.0.1:${e2eTestNetworkNode1Port}:${e2eTestNetworkMagicHex},node2:127.0.0.1:${e2eTestNetworkNode2Port}:${e2eTestNetworkMagicHex},node4:127.0.0.1:${e2eTestNetworkNode3Port}:${e2eTestNetworkMagicHex}`;

internalBitcore.Networks.add({
  name: 'node1net',
  networkMagic: e2eTestNetworkMagicAsNum,
  port: e2eTestNetworkNode1Port,
});
internalBitcore.Networks.add({
  name: 'node2net',
  networkMagic: e2eTestNetworkMagicAsNum,
  port: e2eTestNetworkNode2Port,
});
internalBitcore.Networks.add({
  name: 'node3net',
  networkMagic: e2eTestNetworkMagicAsNum,
  port: e2eTestNetworkNode3Port,
});

const node1 = new Pool({
  dnsSeed: false,
  listenAddr: false,
  network: 'node1net',
  subversion: node1UserAgent,
  version: node1Version,
});
const node2 = new Pool({
  dnsSeed: false,
  listenAddr: false,
  network: 'node2net',
  subversion: node2UserAgent,
  version: node2Version,
});
const node3 = new Pool({
  dnsSeed: false,
  listenAddr: false,
  network: 'node3net',
  subversion: node3UserAgent,
  version: node3Version,
});

const host = process.env.CHAINGRAPH_E2E_POSTGRES_HOST ?? 'localhost';
const port = process.env.CHAINGRAPH_E2E_POSTGRES_PORT ?? '5432';
logger.debug(`Connecting to Postgres at port: ${port}`);
const postgresE2eConnectionStringBase = `postgres://chaingraph:very_insecure_postgres_password@${host}:${port}`;
const postgresE2eConnectionStringDefaultDb = `${postgresE2eConnectionStringBase}/postgres`;
const postgresE2eConnectionStringTestDb = `${postgresE2eConnectionStringBase}/${e2eTestDbName}`;

const e2eEnvVariables = {
  /* eslint-disable @typescript-eslint/naming-convention */
  CHAINGRAPH_GENESIS_BLOCKS: `${e2eTestNetworkMagicHex}:${genesisBlockRaw},e3e1f3e8:${genesisBlockRaw},dab5bffa:${testnetGenesisBlockRaw}`,
  CHAINGRAPH_INCOMPLETE_BLOCK_REPAIR_BATCH_SIZE: '10000',
  CHAINGRAPH_INTERNAL_API_PORT: chaingraphInternalApiPort,
  CHAINGRAPH_LOG_FIREHOSE: logP2pMessage.toString(),
  CHAINGRAPH_LOG_PATH: chaingraphE2eLogPath,
  CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_SCAN_INTERVAL_MS: '100',
  CHAINGRAPH_POSTGRES_CONNECTION_STRING: postgresE2eConnectionStringTestDb,
  CHAINGRAPH_TRUSTED_NODES: e2eTrustedNodesSet1,
  NODE_ENV: 'production',
  /* eslint-enable @typescript-eslint/naming-convention */
};

const e2eEnvVariables2 = {
  /* eslint-disable @typescript-eslint/naming-convention */
  ...e2eEnvVariables,
  CHAINGRAPH_TRUSTED_NODES: e2eTrustedNodesSet2,
  /* eslint-enable @typescript-eslint/naming-convention */
};

const placeholder = undefined as unknown as Peer;
/**
 * Object which holds a reference to each node's `peer`. Set to `undefined`
 * before Chaingraph connects to each node.
 */
const peers = {
  node1: placeholder,
  node2: placeholder,
  node3: placeholder,
};

test.beforeEach((t) => {
  logger.debug(`Starting test: ${t.title}`);
});

test.afterEach((t) => {
  logger.debug(`Completed test: ${t.title}`);
});

// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let client: pg.Client;
/**
 * Before connecting to the e2e test database, drop and recreate it:
 */
test.before(async () => {
  if (recreateDbOnStartup) {
    const defaultClient = new pg.Client({
      connectionString: postgresE2eConnectionStringDefaultDb,
    });
    await defaultClient.connect();
    await defaultClient.query(
      `DROP DATABASE IF EXISTS ${e2eTestDbName} WITH (FORCE);`
    );
    logger.info(`Dropped database: ${e2eTestDbName}`);
    await defaultClient.query(`CREATE DATABASE ${e2eTestDbName};`);
    logger.info(`Created database: ${e2eTestDbName}`);
    await defaultClient.end();
  }
  client = new pg.Client({
    connectionString: postgresE2eConnectionStringTestDb,
  });
  await client.connect();
  if (recreateDbOnStartup) {
    await dbUpMigrationPaths.reduce<Promise<pg.QueryResult | undefined>>(
      async (chain, path) => {
        const dbUpMigration = readFileSync(path, 'utf8');
        return chain.then(async () => client.query(dbUpMigration));
      },
      Promise.resolve(undefined)
    );
  }

  node1.listen();
  node2.listen();
  node3.listen();

  const logPeerConnection = (nodeName: string, peer: Peer) => {
    logger.info(
      `${nodeName} inbound connection from host ${peer.host} at port ${peer.port}; user-agent: ${peer.subversion}`
    );
  };
  node1.on('peerready', (peer) => {
    logPeerConnection(`node1`, peer);
    if (!peer.subversion.includes('tx-broadcast')) peers.node1 = peer;
  });
  node2.on('peerready', (peer) => {
    logPeerConnection(`node2`, peer);
    if (!peer.subversion.includes('tx-broadcast')) peers.node2 = peer;
  });
  node3.on('peerready', (peer) => {
    logPeerConnection(`node3`, peer);
    if (!peer.subversion.includes('tx-broadcast')) peers.node3 = peer;
  });

  if (logP2pMessage) {
    const logNodeMessage = (
      nodeName: string,
      eventName: string,
      message: unknown
    ) => {
      logger.trace(
        `${eventName} received from chaingraph to ${nodeName}: ${inspect(
          message,
          {
            compact: true,
            depth: 5,
            maxArrayLength: 20,
            maxStringLength: 1_000,
          }
        )}`
      );
    };
    node1.on('*', (_, message, eventName) => {
      logNodeMessage('node1', eventName, message);
    });
    node2.on('*', (_, message, eventName) => {
      logNodeMessage('node2', eventName, message);
    });
    node3.on('*', (_, message, eventName) => {
      logNodeMessage('node3', eventName, message);
    });
    logger.info('E2e tests are set to log all P2P messages.');
  }
});

/**
 * Prepare each "mockchain"
 */
const splitHeight = 3000;
const mockchainBeforeFork = [
  genesisBlock,
  ...generateMockchain({
    length: splitHeight,
    previousBlockHash: genesisBlock.header.hash,
  }),
];
const preSplitLastBlockHash =
  mockchainBeforeFork[mockchainBeforeFork.length - 1]!.header.hash;
/**
 * At `splitHeight`, the mockchain splits into two tips (A) and (B):
 *  - Node1 follows (A).
 *  - Node2 follows (B).
 *  - Node3 can support either, and switches between tips when one tip finds
 *    more blocks than the other. (Note, few real implementations would allow
 *    deep reorgs, but this contrived example allows for most of Chaingraph's
 *    expected functionality to be tested.)
 *
 *  Node3 later accepts `tipAStale150`, but switches back to tip A, testing a
 * string of stale blocks.
 */

const tipLengths = 200;
const tipA = generateMockchain({
  length: tipLengths,
  previousBlockHash: preSplitLastBlockHash,
});

const tipB = generateMockchain({
  length: tipLengths,
  previousBlockHash: preSplitLastBlockHash,
});

const tipAStale150 = generateMockchain({
  length: 3,
  previousBlockHash: tipA[149]!.header.hash,
});

/**
 * Initially, all nodes agree on `mockchainBeforeFork`:
 */
const chainStates = {
  node1: [...mockchainBeforeFork],
  node2: [...mockchainBeforeFork],
  node3: [...mockchainBeforeFork],
};

const respondWithChainState =
  (peerName: string, chain: BitcoreBlock[]) =>
  (peer: Peer, message: GetHeadersMessage) => {
    const selected = selectHeaders(
      message.starts.map((hash) => hash.slice().reverse().toString('hex')),
      chain
    );
    if (selected === false) {
      logger.error(
        'Chaingraph requested headers beginning with an unknown genesis block. This is a bug in the E2E tests.'
      );
      return;
    }
    logger.debug(
      `e2e: getheaders received by ${peerName}, sending ${selected.length} headers.`
    );
    const headerMessage = new peer.messages.Headers(selected);
    peer.sendMessage(headerMessage);
  };
node1.on('peergetheaders', respondWithChainState('node1', chainStates.node1));
node2.on('peergetheaders', respondWithChainState('node2', chainStates.node2));
node3.on('peergetheaders', respondWithChainState('node3', chainStates.node3));

const invTestTx =
  'deadbeef00000000000000000000000000000000000000000000000000000000';

const mempool: { [x: string]: string | false | undefined } = {
  [swapEndianness(halTxSpent)]: halTxSpentRaw,
  [swapEndianness(invTestTx)]: false,
};

const respondToGetData = ({
  chain,
  message,
  nodeName,
  peer,
}: {
  chain: BitcoreBlock[];
  message: GetDataMessage;
  nodeName: string;
  peer: Peer;
}) => {
  // eslint-disable-next-line complexity
  message.inventory.forEach((inv) => {
    const hash = inv.hash.slice().reverse().toString('hex');
    if (inv.type === BitcoreInventoryType.MSG_BLOCK) {
      const block = chain.find((b) => b.header.hash === hash);
      if (block === undefined) {
        logger.error(
          `No matching block found in ${nodeName} chain for hash: ${hash}`
        );
        return;
      }
      peer.sendMessage(new peer.messages.Block(block));
      return;
    } else if (inv.type === BitcoreInventoryType.MSG_TX) {
      const txRaw = mempool[hash];
      logger.debug(`e2e: getdata received for hash: ${hash}`);
      if (txRaw === undefined) {
        logger.error(
          `No matching transaction found in e2e mempool for hash: ${hash}`
        );
        return;
      } else if (txRaw === false) {
        logger.debug(`e2e: ignoring request.`);
        return;
      }
      const tx = new Transaction(txRaw);
      logger.debug(`e2e: sending transaction with hash: ${tx.hash}`);
      peer.sendMessage(new peer.messages.Transaction(tx));
    }
    logger.warn(`Unhandled INV type in GetData from ${nodeName}.`);
  });
};

/**
 * Respond to `GetData` messages:
 */
node1.on('peergetdata', (peer, message) => {
  respondToGetData({
    chain: chainStates.node1,
    message,
    nodeName: 'node1',
    peer,
  });
});
node2.on('peergetdata', (peer, message) => {
  respondToGetData({
    chain: chainStates.node2,
    message,
    nodeName: 'node2',
    peer,
  });
});
node3.on('peergetdata', (peer, message) => {
  respondToGetData({
    chain: chainStates.node3,
    message,
    nodeName: 'node3',
    peer,
  });
});

logger.info(
  `Mockchain generated (${mockchainBeforeFork.length} initial blocks, ${tipLengths} blocks after split), mock nodes prepared to respond to P2P messages.`
);

// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let chaingraphProcess: ExecaChildProcess | undefined;
// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let chaingraphProcess2: ExecaChildProcess | undefined;
// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let chaingraphProcess3: ExecaChildProcess | undefined;
// eslint-disable-next-line functional/no-let
let stdoutBuffer = '';
// eslint-disable-next-line functional/no-let
let waitingForStdout: { pattern: RegExp | string; resolver: () => void }[] = [];

const handleStdout = () => {
  waitingForStdout = waitingForStdout.filter((task) => {
    if (
      typeof task.pattern === 'string'
        ? stdoutBuffer.includes(task.pattern)
        : task.pattern.test(stdoutBuffer)
    ) {
      task.resolver();
      return false;
    }
    return true;
  });
};

const seconds = 1000;
const tenSeconds = 10_000;
/**
 * Returns a promise that resolves when the `search` string is found in stdout.
 * @param search - the string to search for in stdout
 *
 * TODO: if AVA is running in debug mode, disable timeout (https://github.com/avajs/ava/issues/3152)
 */
const waitForStdout = async (search: RegExp | string, timeout = tenSeconds) => {
  logger.debug(`Waiting for stdout: ${search.toString()}`);
  const timeoutId = setTimeout(() => {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `Test failed after waiting ${
        timeout / seconds
      }s for the stdout search: ${search.toString()}`
    );
  }, timeout);
  const promise = new Promise<void>((res) => {
    waitingForStdout.push({
      pattern: search,
      resolver: () => {
        logger.debug(`Heard stdout: ${search.toString()}`);
        clearTimeout(timeoutId);
        res();
      },
    });
  });
  handleStdout();
  return promise;
};

const clearStdoutBuffer = () => {
  stdoutBuffer = '';
};

test.serial('[e2e] spawn chaingraph', async (t) => {
  chaingraphProcess = execa('node', ['./bin/chaingraph.js'], {
    env: e2eEnvVariables,
    stdio: 'pipe',
  });
  if (chaingraphProcess.stdout === null) {
    t.fail('`chaingraphProcess` stdout is not available.');
    return;
  }
  chaingraphProcess.stdout.on('data', (chunk) => {
    stdoutBuffer += chunk;
    handleStdout();
  });
  await waitForStdout('Starting Chaingraph...');
  t.pass();
});

const enum StatusCode {
  success = 200,
  badRequest = 400,
  notFound = 404,
}

test.serial('[e2e] api /health-check is alive', async (t) => {
  const healthCheckResponse = await got(
    `http://localhost:${chaingraphInternalApiPort}/health-check`
  );
  t.deepEqual(healthCheckResponse.statusCode, StatusCode.success);
  t.deepEqual(healthCheckResponse.body, '{"status":"alive"}');
});

test.serial('[e2e] connects to trusted nodes', async (t) => {
  await waitForStdout('node1: connected to node');
  await waitForStdout('node2: connected to node');
  await waitForStdout('node3: connected to node');
  t.pass();
});

test.serial('[e2e] downloads all header chains', async (t) => {
  await waitForStdout(/node1[^\n]+headers-syncing completed/u);
  await waitForStdout(/node2[^\n]+headers-syncing completed/u);
  await waitForStdout(/node3[^\n]+headers-syncing completed/u);
  t.pass();
});

test.serial(
  '[e2e] restores sync-state from database on restart (during initial sync)',
  async (t) => {
    await waitForStdout(
      /Saved new block – height:\s+10[^\n]+nodes: node1, node2, node3/u
    );
    logger.info('e2e: testing sync restoration on restart. Sending SIGTERM...');
    chaingraphProcess!.kill('SIGINT');
    // chaingraphProcess!.kill('SIGTERM');
    await waitForStdout('Shutting down...');
    await waitForStdout('Exiting...');
    chaingraphProcess2 = execa('node', ['./bin/chaingraph.js'], {
      env: e2eEnvVariables,
      stdio: 'pipe',
    });
    if (chaingraphProcess2.stdout === null) {
      t.fail('`chaingraphProcess2` stdout is not available.');
      return;
    }
    chaingraphProcess2.stdout.on('data', (chunk) => {
      stdoutBuffer += chunk;
      handleStdout();
    });
    await waitForStdout('Starting Chaingraph...');
    await waitForStdout('Restored chain for node node1');
    await waitForStdout('Restored chain for node node2');
    await waitForStdout('Restored chain for node node3');
    t.pass();
  }
);
const sleep = async (ms: number) =>
  new Promise((res) => {
    setTimeout(res, ms);
  });

const repeatedHashByteLength = 32;
const transactionSaveConflictPollingAttempts = 50;
const transactionSaveConflictPollingIntervalMs = 20;
const waitForTransactionSaveConflict = async (
  transactionHash: string,
  remainingAttempts = transactionSaveConflictPollingAttempts
): Promise<void> => {
  const result = await client.query<{ waiting: boolean }>(
    /* sql */ `
    SELECT EXISTS (
      SELECT 1
        FROM pg_stat_activity
        -- cspell:disable-next-line
        WHERE datname = $1
          AND query LIKE '%INSERT INTO transaction%'
          AND query LIKE $2
          AND wait_event_type IS NOT NULL
    ) AS waiting;
  `,
    [e2eTestDbName, `%${transactionHash}%`]
  );
  if (result.rows[0]!.waiting) {
    return;
  }
  if (remainingAttempts === 0) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `Timed out waiting for saveTransactionForNodes conflict on ${transactionHash}.`
    );
  }
  await sleep(transactionSaveConflictPollingIntervalMs);
  await waitForTransactionSaveConflict(transactionHash, remainingAttempts - 1);
};

const blockRepairPollingAttempts = 40;
const blockRepairPollingIntervalMs = 250;
const getBlockTransactionCount = async (blockHash: string) =>
  Number(
    (
      await client.query<{ count: string }>(
        /* sql */ `
        SELECT COUNT(*) FROM block_transaction
          INNER JOIN block ON block.internal_id = block_transaction.block_internal_id
          WHERE block.hash = $1;
      `,
        [hexToBin(blockHash)]
      )
    ).rows[0]!.count
  );
const waitForBlockTransactionCount = async (
  blockHash: string,
  expectedCount: number,
  remainingAttempts = blockRepairPollingAttempts
): Promise<number> => {
  const count = await getBlockTransactionCount(blockHash);
  if (count === expectedCount || remainingAttempts === 0) {
    return count;
  }
  await sleep(blockRepairPollingIntervalMs);
  return waitForBlockTransactionCount(
    blockHash,
    expectedCount,
    remainingAttempts - 1
  );
};

const mempoolExpirationPollingAttempts = 50;
const mempoolExpirationPollingIntervalMs = 100;
const getExpiredMempoolArchiveState = async () =>
  (
    await client.query<{
      historyRowCount: number;
      inMempool: boolean;
      replacedAt: string | null;
      transactionName: string;
    }>(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('expiry_parent_a', decode(repeat('d1', 32), 'hex')),
      ('expiry_child_b',  decode(repeat('d2', 32), 'hex')),
      ('expiry_child_c',  decode(repeat('d3', 32), 'hex'))
),
selected_node AS (
    SELECT internal_id
      FROM node
      WHERE name = 'node1'
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
SELECT named_transactions.name AS "transactionName",
       (node_transaction.transaction_internal_id IS NOT NULL) AS "inMempool",
       COUNT(node_transaction_history.transaction_internal_id)::integer AS "historyRowCount",
       MIN(node_transaction_history.replaced_at)::text AS "replacedAt"
  FROM named_transactions
  CROSS JOIN selected_node
  LEFT JOIN node_transaction
    ON node_transaction.node_internal_id = selected_node.internal_id
   AND node_transaction.transaction_internal_id = named_transactions.internal_id
  LEFT JOIN node_transaction_history
    ON node_transaction_history.node_internal_id = selected_node.internal_id
   AND node_transaction_history.transaction_internal_id = named_transactions.internal_id
  GROUP BY named_transactions.name, node_transaction.transaction_internal_id
  ORDER BY named_transactions.name;
`)
  ).rows;
const waitForExpiredMempoolArchive = async (
  remainingAttempts = mempoolExpirationPollingAttempts
): Promise<Awaited<ReturnType<typeof getExpiredMempoolArchiveState>>> => {
  const rows = await getExpiredMempoolArchiveState();
  const expectedReplacedAt = '2026-01-15 00:00:00';
  if (
    rows.every(
      (row) =>
        !row.inMempool &&
        row.historyRowCount === 1 &&
        row.replacedAt === expectedReplacedAt
    ) ||
    remainingAttempts === 0
  ) {
    return rows;
  }
  await sleep(mempoolExpirationPollingIntervalMs);
  return waitForExpiredMempoolArchive(remainingAttempts - 1);
};

const getConfirmedMempoolArchiveState = async () =>
  (
    await client.query<{
      historyRowCount: number;
      inMempool: boolean;
      replacedAt: string | null;
    }>(/* sql */ `
WITH selected_node AS (
    SELECT internal_id
      FROM node
      WHERE name = 'node1'
),
selected_transaction AS (
    SELECT internal_id
      FROM transaction
      WHERE hash = decode(repeat('d5', 32), 'hex')
)
SELECT (node_transaction.transaction_internal_id IS NOT NULL) AS "inMempool",
       COUNT(node_transaction_history.transaction_internal_id)::integer AS "historyRowCount",
       MIN(node_transaction_history.replaced_at)::text AS "replacedAt"
  FROM selected_transaction
  CROSS JOIN selected_node
  LEFT JOIN node_transaction
    ON node_transaction.node_internal_id = selected_node.internal_id
   AND node_transaction.transaction_internal_id = selected_transaction.internal_id
  LEFT JOIN node_transaction_history
    ON node_transaction_history.node_internal_id = selected_node.internal_id
   AND node_transaction_history.transaction_internal_id = selected_transaction.internal_id
  GROUP BY node_transaction.transaction_internal_id;
`)
  ).rows[0];

const confirmedMempoolArchiveCompleted = (
  row: Awaited<ReturnType<typeof getConfirmedMempoolArchiveState>>
) =>
  row !== undefined &&
  !row.inMempool &&
  row.historyRowCount === 1 &&
  row.replacedAt === null;

const waitForConfirmedMempoolArchive = async (
  remainingAttempts = mempoolExpirationPollingAttempts
): Promise<Awaited<ReturnType<typeof getConfirmedMempoolArchiveState>>> => {
  const row = await getConfirmedMempoolArchiveState();
  if (confirmedMempoolArchiveCompleted(row) || remainingAttempts === 0) {
    return row;
  }
  await sleep(mempoolExpirationPollingIntervalMs);
  return waitForConfirmedMempoolArchive(remainingAttempts - 1);
};

test.serial(
  '[e2e] ignores inbound transactions before initial sync is complete',
  async (t) => {
    peers.node1.sendMessage(
      new peers.node1.messages.Transaction(new Transaction(halTxRaw))
    );
    const delay = 1000;
    await sleep(delay);
    const result = await client.query<{ encode: string }>(
      /* sql */ `SELECT encode(hash, 'hex') FROM transaction WHERE hash = $1;`,
      [hexToBin(halTxHash)]
    );
    t.deepEqual(result.rowCount, 0);
    t.pass();
  }
);

const oneMinute = 60_000;
test.serial('[e2e] completes initial sync', async (t) => {
  t.timeout(oneMinute);
  await waitForStdout(
    /Saved new block – height:\s+3000[^\n]+nodes: node1, node2, node3/u,
    oneMinute
  );
  await waitForStdout('Agent: initial sync is complete.');
  t.pass();
});

test.serial('[e2e] creates expected indexes after initial sync', async (t) => {
  await waitForStdout('Agent: all managed indexes have been created.');
  await waitForStdout('Agent: enabled mempool tracking.');
  const indexes = (
    await client.query<{
      indexname: string;
    }>(/* sql */ `
  SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname;
  `)
  ).rows
    .map((row) => row.indexname)
    /*
     * CHAINGRAPH_UNSPENT_TRACKING=bitmask adds one pair of partial indexes
     * per node (checked by the unspent tracking test).
     */
    .filter((name) => !/^output_unspent_node_\d+_/u.test(name));
  t.deepEqual(indexes, [
    'block_hash_key',
    'block_height_index',
    'block_inclusions_index',
    'block_internal_id_key',
    'block_pkey',
    'block_transaction_pkey',
    'input_pkey',
    'node_block_history_pkey',
    'node_block_pkey',
    'node_internal_id_key',
    'node_name_key',
    'node_pkey',
    'node_transaction_history_pkey',
    'node_transaction_pkey',
    'output_pkey',
    'output_search_index',
    'output_unspent_search_index',
    'output_unspent_token_category_index',
    'spent_by_index',
    'token_category_index',
    'transaction_hash_key',
    'transaction_pkey',
    'unspent_output_set_pkey',
    'unspent_output_set_prefix_index',
    'unspent_output_set_token_category_index',
  ]);
  // cspell:ignore tgenabled tgname
  const triggers = (
    await client.query<{
      tgenabled: string;
      tgname: string;
    }>(/* sql */ `
  SELECT tgname, tgenabled FROM pg_trigger
    WHERE tgname IN (
      'trigger_public_node_block_insert',
      'trigger_public_node_transaction_history_insert'
    )
    ORDER BY tgname;
  `)
  ).rows;
  t.deepEqual(triggers, [
    { tgenabled: 'O', tgname: 'trigger_public_node_block_insert' },
    {
      tgenabled: 'O',
      tgname: 'trigger_public_node_transaction_history_insert',
    },
  ]);
  clearStdoutBuffer();
  t.pass();
});

test.serial(
  '[e2e] getAllKnownBlockHashes returns hex hashes for every known block',
  async (t) => {
    const originalPostgresConnectionString =
      process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
    process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
      postgresE2eConnectionStringTestDb;
    const { getAllKnownBlockHashes } = await import('../db.js');
    // eslint-disable-next-line functional/no-try-statement
    try {
      const hashes = await getAllKnownBlockHashes();
      /*
       * Convert client-side (the previous implementation) to verify the
       * SQL-side `encode(...)` used by `getAllKnownBlockHashes` matches it.
       */
      const expected = (
        await client.query<{ hash: Buffer }>(/* sql */ `
  SELECT hash FROM block ORDER BY hash;
  `)
      ).rows.map((row) => row.hash.toString('hex'));
      t.true(expected.length > 0);
      t.deepEqual(
        [...hashes].sort((a, b) => (a < b ? -1 : Number(a > b))),
        expected
      );
      const hexPattern = /^[0-9a-f]{64}$/u;
      t.true(hashes.every((hash) => hexPattern.test(hash)));
    } finally {
      if (originalPostgresConnectionString === undefined) {
        delete process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
      } else {
        process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
          originalPostgresConnectionString;
      }
    }
  }
);

test.serial(
  '[e2e] records node validation after concurrent transaction insert conflict',
  async (t) => {
    const transactionHash = 'c1'.repeat(repeatedHashByteLength);
    const validatedAt = new Date('2026-01-01T00:00:00.000Z');
    const transaction: ChaingraphTransaction = {
      hash: transactionHash,
      inputs: [
        {
          outpointIndex: 0,
          outpointTransactionHash: 'c2'.repeat(repeatedHashByteLength),
          sequenceNumber: 0,
          unlockingBytecode: '51',
        },
      ],
      isCoinbase: false,
      locktime: 0,
      outputs: [
        {
          lockingBytecode: '51',
          valueSatoshis: 1000n,
        },
      ],
      sizeBytes: 100,
      version: 1,
    };
    const nodeInternalId = Number(
      (
        await client.query<{ internalId: number }>(
          /* sql */ `SELECT internal_id AS "internalId" FROM node WHERE name = 'node1';`
        )
      ).rows[0]!.internalId
    );
    await client.query(
      /* sql */ `
      DELETE FROM node_transaction
        USING transaction
        WHERE node_transaction.transaction_internal_id = transaction.internal_id
          AND transaction.hash = $1;
    `,
      [Buffer.from(transactionHash, 'hex')]
    );
    await client.query(
      /* sql */ `
      DELETE FROM input
        USING transaction
        WHERE input.transaction_internal_id = transaction.internal_id
          AND transaction.hash = $1;
    `,
      [Buffer.from(transactionHash, 'hex')]
    );
    await client.query(
      /* sql */ `DELETE FROM output WHERE transaction_hash = $1;`,
      [Buffer.from(transactionHash, 'hex')]
    );
    await client.query(/* sql */ `DELETE FROM transaction WHERE hash = $1;`, [
      Buffer.from(transactionHash, 'hex'),
    ]);
    const originalPostgresConnectionString =
      process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
    process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
      postgresE2eConnectionStringTestDb;
    const { pool: dbPool, saveTransactionForNodes } = await import('../db.js');
    const competingClient = new pg.Client({
      connectionString: postgresE2eConnectionStringTestDb,
    });
    await competingClient.connect();
    // eslint-disable-next-line functional/no-let
    let competingTransactionOpen = false;
    // eslint-disable-next-line functional/no-try-statement
    try {
      await competingClient.query(/* sql */ `BEGIN;`);
      competingTransactionOpen = true;
      await competingClient.query(
        /* sql */ `
        INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
          VALUES ($1, 1, 0, 100, false);
      `,
        [Buffer.from(transactionHash, 'hex')]
      );
      const savePromise = saveTransactionForNodes(transaction, [
        { nodeInternalId, validatedAt },
      ]);
      await waitForTransactionSaveConflict(transactionHash);
      await competingClient.query(/* sql */ `COMMIT;`);
      competingTransactionOpen = false;
      await t.notThrowsAsync(savePromise);
      const savedValidationCount = Number(
        (
          await client.query<{ count: string }>(
            /* sql */ `
            SELECT COUNT(*)::bigint AS count
              FROM node_transaction
              JOIN transaction
                ON transaction.internal_id = node_transaction.transaction_internal_id
              WHERE transaction.hash = $1
                AND node_transaction.node_internal_id = $2
                AND node_transaction.validated_at = $3;
          `,
            [Buffer.from(transactionHash, 'hex'), nodeInternalId, validatedAt]
          )
        ).rows[0]!.count
      );
      t.deepEqual(savedValidationCount, 1);
    } finally {
      if (competingTransactionOpen) {
        await competingClient.query(/* sql */ `ROLLBACK;`).catch((err) => {
          logger.debug(err);
        });
      }
      await client.query(
        /* sql */ `
        DELETE FROM node_transaction
          USING transaction
          WHERE node_transaction.transaction_internal_id = transaction.internal_id
            AND transaction.hash = $1;
      `,
        [Buffer.from(transactionHash, 'hex')]
      );
      await client.query(
        /* sql */ `
        DELETE FROM input
          USING transaction
          WHERE input.transaction_internal_id = transaction.internal_id
            AND transaction.hash = $1;
      `,
        [Buffer.from(transactionHash, 'hex')]
      );
      await client.query(
        /* sql */ `DELETE FROM output WHERE transaction_hash = $1;`,
        [Buffer.from(transactionHash, 'hex')]
      );
      await client.query(/* sql */ `DELETE FROM transaction WHERE hash = $1;`, [
        Buffer.from(transactionHash, 'hex'),
      ]);
      await competingClient.end();
      await dbPool.end();
      if (originalPostgresConnectionString === undefined) {
        delete process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
      } else {
        process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
          originalPostgresConnectionString;
      }
    }
  }
);

test.serial(
  '[e2e] cascades replaced mempool transaction history to same-node descendants',
  async (t) => {
    await client.query(/* sql */ `BEGIN;`);
    // eslint-disable-next-line functional/no-try-statement
    try {
      await client.query(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('parent_a', decode(repeat('f1', 32), 'hex')),
      ('child_b',  decode(repeat('f3', 32), 'hex')),
      ('child_c',  decode(repeat('f4', 32), 'hex'))
)
INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
  SELECT hash, 1, 0, 100, false
    FROM transaction_values;
`);
      await client.query(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('parent_a', decode(repeat('f1', 32), 'hex')),
      ('child_b',  decode(repeat('f3', 32), 'hex'))
)
INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode)
  SELECT hash, 0, 1000, '\\x51'::bytea
    FROM transaction_values;
`);
      await client.query(/* sql */ `
WITH input_values (child_name, parent_name, input_index) AS (
    VALUES
      ('child_b', 'parent_a', 0),
      ('child_c', 'child_b',  0)
),
transaction_values (name, hash) AS (
    VALUES
      ('parent_a', decode(repeat('f1', 32), 'hex')),
      ('child_b',  decode(repeat('f3', 32), 'hex')),
      ('child_c',  decode(repeat('f4', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id, transaction.hash
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
  SELECT child.internal_id, input_values.input_index, 0, 0, parent.hash, '\\x51'::bytea
    FROM input_values
    JOIN named_transactions child
      ON child.name = input_values.child_name
    JOIN named_transactions parent
      ON parent.name = input_values.parent_name;
`);
      await client.query(/* sql */ `
WITH selected_nodes AS (
    SELECT name, internal_id
      FROM node
      WHERE name IN ('node1', 'node2')
),
transaction_values (name, hash) AS (
    VALUES
      ('child_b', decode(repeat('f3', 32), 'hex')),
      ('child_c', decode(repeat('f4', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
  SELECT selected_nodes.internal_id, named_transactions.internal_id, timestamp '2026-01-01 00:00:00'
    FROM selected_nodes
    CROSS JOIN named_transactions;
`);
      await client.query(/* sql */ `
WITH selected_nodes AS (
    SELECT name, internal_id
      FROM node
      WHERE name = 'node1'
),
transaction_values (name, hash) AS (
    VALUES
      ('parent_a', decode(repeat('f1', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO node_transaction_history (node_internal_id, transaction_internal_id, validated_at, replaced_at)
  SELECT selected_nodes.internal_id,
         named_transactions.internal_id,
         timestamp '2026-01-01 00:00:00',
         timestamp '2026-01-01 00:10:00'
    FROM selected_nodes
    CROSS JOIN named_transactions;
`);
      const remainingMempool = (
        await client.query<{
          nodeName: string;
          transactionName: string;
        }>(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('child_b', decode(repeat('f3', 32), 'hex')),
      ('child_c', decode(repeat('f4', 32), 'hex'))
)
SELECT node.name AS "nodeName", transaction_values.name AS "transactionName"
  FROM node_transaction
  JOIN node
    ON node.internal_id = node_transaction.node_internal_id
  JOIN transaction
    ON transaction.internal_id = node_transaction.transaction_internal_id
  JOIN transaction_values
    ON transaction_values.hash = transaction.hash
  ORDER BY "nodeName", "transactionName";
`)
      ).rows;
      t.deepEqual(remainingMempool, [
        { nodeName: 'node2', transactionName: 'child_b' },
        { nodeName: 'node2', transactionName: 'child_c' },
      ]);
      const archivedDescendants = (
        await client.query<{
          replacedAt: string;
          transactionName: string;
        }>(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('child_b', decode(repeat('f3', 32), 'hex')),
      ('child_c', decode(repeat('f4', 32), 'hex'))
)
SELECT transaction_values.name AS "transactionName",
       node_transaction_history.replaced_at::text AS "replacedAt"
  FROM node_transaction_history
  JOIN node
    ON node.internal_id = node_transaction_history.node_internal_id
  JOIN transaction
    ON transaction.internal_id = node_transaction_history.transaction_internal_id
  JOIN transaction_values
    ON transaction_values.hash = transaction.hash
  WHERE node.name = 'node1'
  ORDER BY "transactionName";
`)
      ).rows;
      t.deepEqual(archivedDescendants, [
        { replacedAt: '2026-01-01 00:10:00', transactionName: 'child_b' },
        { replacedAt: '2026-01-01 00:10:00', transactionName: 'child_c' },
      ]);
    } finally {
      await client.query(/* sql */ `ROLLBACK;`);
    }
  }
);

test.serial(
  '[e2e] archives expired mempool transactions and descendants',
  async (t) => {
    await client.query(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('expiry_parent_a', decode(repeat('d1', 32), 'hex')),
      ('expiry_child_b',  decode(repeat('d2', 32), 'hex')),
      ('expiry_child_c',  decode(repeat('d3', 32), 'hex'))
)
INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
  SELECT hash, 1, 0, 100, false
    FROM transaction_values;
`);
    // eslint-disable-next-line functional/no-try-statement
    try {
      await client.query(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('expiry_parent_a', decode(repeat('d1', 32), 'hex')),
      ('expiry_child_b',  decode(repeat('d2', 32), 'hex'))
)
INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode)
  SELECT hash, 0, 1000, '\\x51'::bytea
    FROM transaction_values;
`);
      await client.query(/* sql */ `
WITH input_values (child_name, parent_name, input_index) AS (
    VALUES
      ('expiry_child_b', 'expiry_parent_a', 0),
      ('expiry_child_c', 'expiry_child_b',  0)
),
transaction_values (name, hash) AS (
    VALUES
      ('expiry_parent_a', decode(repeat('d1', 32), 'hex')),
      ('expiry_child_b',  decode(repeat('d2', 32), 'hex')),
      ('expiry_child_c',  decode(repeat('d3', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id, transaction.hash
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
  SELECT child.internal_id, input_values.input_index, 0, 0, parent.hash, '\\x51'::bytea
    FROM input_values
    JOIN named_transactions child
      ON child.name = input_values.child_name
    JOIN named_transactions parent
      ON parent.name = input_values.parent_name;
`);
      await client.query(/* sql */ `
WITH selected_node AS (
    SELECT internal_id
      FROM node
      WHERE name = 'node1'
),
transaction_values (name, hash) AS (
    VALUES
      ('expiry_parent_a', decode(repeat('d1', 32), 'hex')),
      ('expiry_child_b',  decode(repeat('d2', 32), 'hex')),
      ('expiry_child_c',  decode(repeat('d3', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
  SELECT selected_node.internal_id,
         named_transactions.internal_id,
         CASE
           WHEN named_transactions.name = 'expiry_parent_a'
             THEN timestamp '2026-01-01 00:00:00'
           ELSE CURRENT_TIMESTAMP + interval '1 day'
         END
    FROM selected_node
    CROSS JOIN named_transactions;
`);
      const archivedTransactions = await waitForExpiredMempoolArchive();
      t.deepEqual(archivedTransactions, [
        {
          historyRowCount: 1,
          inMempool: false,
          replacedAt: '2026-01-15 00:00:00',
          transactionName: 'expiry_child_b',
        },
        {
          historyRowCount: 1,
          inMempool: false,
          replacedAt: '2026-01-15 00:00:00',
          transactionName: 'expiry_child_c',
        },
        {
          historyRowCount: 1,
          inMempool: false,
          replacedAt: '2026-01-15 00:00:00',
          transactionName: 'expiry_parent_a',
        },
      ]);
    } finally {
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d1', 32), 'hex')),
      (decode(repeat('d2', 32), 'hex')),
      (decode(repeat('d3', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
DELETE FROM node_transaction
  USING named_transactions
  WHERE node_transaction.transaction_internal_id = named_transactions.internal_id;
`);
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d1', 32), 'hex')),
      (decode(repeat('d2', 32), 'hex')),
      (decode(repeat('d3', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
DELETE FROM node_transaction_history
  USING named_transactions
  WHERE node_transaction_history.transaction_internal_id = named_transactions.internal_id;
`);
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d1', 32), 'hex')),
      (decode(repeat('d2', 32), 'hex')),
      (decode(repeat('d3', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
DELETE FROM input
  USING named_transactions
  WHERE input.transaction_internal_id = named_transactions.internal_id;
`);
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d1', 32), 'hex')),
      (decode(repeat('d2', 32), 'hex')),
      (decode(repeat('d3', 32), 'hex'))
)
DELETE FROM output
  USING transaction_values
  WHERE output.transaction_hash = transaction_values.hash;
`);
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d1', 32), 'hex')),
      (decode(repeat('d2', 32), 'hex')),
      (decode(repeat('d3', 32), 'hex'))
)
DELETE FROM transaction
  USING transaction_values
  WHERE transaction.hash = transaction_values.hash;
`);
    }
  }
);

test.serial(
  '[e2e] archives stale mempool transactions already accepted by blocks',
  async (t) => {
    await client.query(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('confirmed_parent_a', decode(repeat('d4', 32), 'hex')),
      ('confirmed_child_b',  decode(repeat('d5', 32), 'hex'))
)
INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
  SELECT hash, 1, 0, 100, false
    FROM transaction_values;
`);
    // eslint-disable-next-line functional/no-try-statement
    try {
      await client.query(/* sql */ `
INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode)
  VALUES (decode(repeat('d4', 32), 'hex'), 0, 1000, '\\x51'::bytea);
`);
      await client.query(/* sql */ `
WITH selected_transaction AS (
    SELECT internal_id
      FROM transaction
      WHERE hash = decode(repeat('d5', 32), 'hex')
)
INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
  SELECT selected_transaction.internal_id,
         0,
         0,
         0,
         decode(repeat('d4', 32), 'hex'),
         '\\x51'::bytea
    FROM selected_transaction;
`);
      await client.query(/* sql */ `
WITH selected_transaction AS (
    SELECT internal_id
      FROM transaction
      WHERE hash = decode(repeat('d5', 32), 'hex')
),
inserted_block AS (
    INSERT INTO block (height, version, "timestamp", hash, previous_block_hash, merkle_root, bits, nonce, size_bytes)
      VALUES (4001, 1, 0, decode(repeat('d6', 32), 'hex'), decode(repeat('d7', 32), 'hex'), decode(repeat('d8', 32), 'hex'), 0, 0, 181)
      RETURNING internal_id
)
INSERT INTO block_transaction (block_internal_id, transaction_internal_id, transaction_index)
  SELECT inserted_block.internal_id, selected_transaction.internal_id, 1
    FROM inserted_block
    CROSS JOIN selected_transaction;
`);
      await client.query(/* sql */ `
WITH selected_node AS (
    SELECT internal_id
      FROM node
      WHERE name = 'node1'
),
selected_block AS (
    SELECT internal_id
      FROM block
      WHERE hash = decode(repeat('d6', 32), 'hex')
)
INSERT INTO node_block (node_internal_id, block_internal_id, accepted_at)
  SELECT selected_node.internal_id,
         selected_block.internal_id,
         timestamp '2026-01-01 00:10:00'
    FROM selected_node
    CROSS JOIN selected_block;
`);
      await client.query(/* sql */ `
WITH selected_node AS (
    SELECT internal_id
      FROM node
      WHERE name = 'node1'
),
selected_transaction AS (
    SELECT internal_id
      FROM transaction
      WHERE hash = decode(repeat('d5', 32), 'hex')
)
INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
  SELECT selected_node.internal_id,
         selected_transaction.internal_id,
         timestamp '2026-01-01 00:00:00'
    FROM selected_node
    CROSS JOIN selected_transaction;
`);
      const archivedTransaction = await waitForConfirmedMempoolArchive();
      t.deepEqual(archivedTransaction, {
        historyRowCount: 1,
        inMempool: false,
        replacedAt: null,
      });
    } finally {
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d4', 32), 'hex')),
      (decode(repeat('d5', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
DELETE FROM node_transaction
  USING named_transactions
  WHERE node_transaction.transaction_internal_id = named_transactions.internal_id;
`);
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d4', 32), 'hex')),
      (decode(repeat('d5', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
DELETE FROM node_transaction_history
  USING named_transactions
  WHERE node_transaction_history.transaction_internal_id = named_transactions.internal_id;
`);
      await client.query(/* sql */ `
WITH selected_block AS (
    SELECT internal_id
      FROM block
      WHERE hash = decode(repeat('d6', 32), 'hex')
)
DELETE FROM node_block
  USING selected_block
  WHERE node_block.block_internal_id = selected_block.internal_id;
`);
      await client.query(/* sql */ `
WITH selected_block AS (
    SELECT internal_id
      FROM block
      WHERE hash = decode(repeat('d6', 32), 'hex')
)
DELETE FROM node_block_history
  USING selected_block
  WHERE node_block_history.block_internal_id = selected_block.internal_id;
`);
      await client.query(/* sql */ `
WITH selected_block AS (
    SELECT internal_id
      FROM block
      WHERE hash = decode(repeat('d6', 32), 'hex')
)
DELETE FROM block_transaction
  USING selected_block
  WHERE block_transaction.block_internal_id = selected_block.internal_id;
`);
      await client.query(/* sql */ `
DELETE FROM block
  WHERE hash = decode(repeat('d6', 32), 'hex');
`);
      await client.query(/* sql */ `
WITH selected_transaction AS (
    SELECT internal_id
      FROM transaction
      WHERE hash = decode(repeat('d5', 32), 'hex')
)
DELETE FROM input
  USING selected_transaction
  WHERE input.transaction_internal_id = selected_transaction.internal_id;
`);
      await client.query(/* sql */ `
DELETE FROM output
  WHERE transaction_hash = decode(repeat('d4', 32), 'hex');
`);
      await client.query(/* sql */ `
WITH transaction_values (hash) AS (
    VALUES
      (decode(repeat('d4', 32), 'hex')),
      (decode(repeat('d5', 32), 'hex'))
)
DELETE FROM transaction
  USING transaction_values
  WHERE transaction.hash = transaction_values.hash;
`);
    }
  }
);

test.serial(
  '[e2e] backfills existing orphan mempool descendants with idempotence',
  async (t) => {
    await client.query(/* sql */ `BEGIN;`);
    // eslint-disable-next-line functional/no-try-statement
    try {
      const backfillMigration = readFileSync(
        backfillOrphanMempoolDescendantsMigrationPath,
        'utf8'
      );
      await client.query(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('backfill_parent_a', decode(repeat('e1', 32), 'hex')),
      ('backfill_child_b',  decode(repeat('e2', 32), 'hex')),
      ('backfill_child_c',  decode(repeat('e3', 32), 'hex'))
)
INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
  SELECT hash, 1, 0, 100, false
    FROM transaction_values;
`);
      await client.query(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('backfill_parent_a', decode(repeat('e1', 32), 'hex')),
      ('backfill_child_b',  decode(repeat('e2', 32), 'hex'))
)
INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode)
  SELECT hash, 0, 1000, '\\x51'::bytea
    FROM transaction_values;
`);
      await client.query(/* sql */ `
WITH input_values (child_name, parent_name, input_index) AS (
    VALUES
      ('backfill_child_b', 'backfill_parent_a', 0),
      ('backfill_child_c', 'backfill_child_b',  0)
),
transaction_values (name, hash) AS (
    VALUES
      ('backfill_parent_a', decode(repeat('e1', 32), 'hex')),
      ('backfill_child_b',  decode(repeat('e2', 32), 'hex')),
      ('backfill_child_c',  decode(repeat('e3', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id, transaction.hash
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO input (transaction_internal_id, input_index, outpoint_index, sequence_number, outpoint_transaction_hash, unlocking_bytecode)
  SELECT child.internal_id, input_values.input_index, 0, 0, parent.hash, '\\x51'::bytea
    FROM input_values
    JOIN named_transactions child
      ON child.name = input_values.child_name
    JOIN named_transactions parent
      ON parent.name = input_values.parent_name;
`);
      await client.query(/* sql */ `
WITH selected_nodes AS (
    SELECT name, internal_id
      FROM node
      WHERE name = 'node1'
),
transaction_values (name, hash) AS (
    VALUES
      ('backfill_parent_a', decode(repeat('e1', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO node_transaction_history (node_internal_id, transaction_internal_id, validated_at, replaced_at)
  SELECT selected_nodes.internal_id,
         named_transactions.internal_id,
         timestamp '2026-01-01 00:00:00',
         timestamp '2026-01-01 00:10:00'
    FROM selected_nodes
    CROSS JOIN named_transactions;
`);
      await client.query(/* sql */ `
WITH selected_nodes AS (
    SELECT name, internal_id
      FROM node
      WHERE name = 'node1'
),
transaction_values (name, hash) AS (
    VALUES
      ('backfill_child_b', decode(repeat('e2', 32), 'hex')),
      ('backfill_child_c', decode(repeat('e3', 32), 'hex'))
),
named_transactions AS (
    SELECT transaction_values.name, transaction.internal_id
      FROM transaction
      JOIN transaction_values
        ON transaction_values.hash = transaction.hash
)
INSERT INTO node_transaction (node_internal_id, transaction_internal_id, validated_at)
  SELECT selected_nodes.internal_id, named_transactions.internal_id, timestamp '2026-01-01 00:00:00'
    FROM selected_nodes
    CROSS JOIN named_transactions;
`);
      await client.query(backfillMigration);
      await client.query(backfillMigration);

      const remainingMempool = (
        await client.query<{
          transactionName: string;
        }>(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('backfill_child_b', decode(repeat('e2', 32), 'hex')),
      ('backfill_child_c', decode(repeat('e3', 32), 'hex'))
)
SELECT transaction_values.name AS "transactionName"
  FROM node_transaction
  JOIN node
    ON node.internal_id = node_transaction.node_internal_id
  JOIN transaction
    ON transaction.internal_id = node_transaction.transaction_internal_id
  JOIN transaction_values
    ON transaction_values.hash = transaction.hash
  WHERE node.name = 'node1'
  ORDER BY "transactionName";
`)
      ).rows;
      t.deepEqual(remainingMempool, []);

      const archivedTransactions = (
        await client.query<{
          historyRowCount: number;
          replacedAt: string;
          transactionName: string;
        }>(/* sql */ `
WITH transaction_values (name, hash) AS (
    VALUES
      ('backfill_parent_a', decode(repeat('e1', 32), 'hex')),
      ('backfill_child_b',  decode(repeat('e2', 32), 'hex')),
      ('backfill_child_c',  decode(repeat('e3', 32), 'hex'))
)
SELECT transaction_values.name AS "transactionName",
       COUNT(*)::integer AS "historyRowCount",
       MIN(node_transaction_history.replaced_at)::text AS "replacedAt"
  FROM node_transaction_history
  JOIN node
    ON node.internal_id = node_transaction_history.node_internal_id
  JOIN transaction
    ON transaction.internal_id = node_transaction_history.transaction_internal_id
  JOIN transaction_values
    ON transaction_values.hash = transaction.hash
  WHERE node.name = 'node1'
  GROUP BY transaction_values.name
  ORDER BY "transactionName";
`)
      ).rows;
      t.deepEqual(archivedTransactions, [
        {
          historyRowCount: 1,
          replacedAt: '2026-01-01 00:10:00',
          transactionName: 'backfill_child_b',
        },
        {
          historyRowCount: 1,
          replacedAt: '2026-01-01 00:10:00',
          transactionName: 'backfill_child_c',
        },
        {
          historyRowCount: 1,
          replacedAt: '2026-01-01 00:10:00',
          transactionName: 'backfill_parent_a',
        },
      ]);
    } finally {
      await client.query(/* sql */ `ROLLBACK;`);
    }
  }
);

test.serial(
  '[e2e] after initial sync is complete, requests transactions as they are announced',
  async (t) => {
    const node3RequestedTx = new Promise((res) => {
      node3.once('peergetdata', (_, message) => {
        res(message.inventory);
      });
    });
    const announcedHash = Buffer.from(invTestTx, 'hex');
    peers.node3.sendMessage(
      peers.node3.messages.Inventory.forTransaction(announcedHash)
    );
    const result = await node3RequestedTx;
    t.deepEqual(result, [{ hash: announcedHash.reverse(), type: 1 }]);
  }
);

test.serial(
  '[e2e] after initial sync is complete, saves inbound transactions as they are received',
  async (t) => {
    peers.node1.sendMessage(
      new peers.node1.messages.Transaction(new Transaction(halTxRaw))
    );
    const delay = 1000;
    await sleep(delay);
    const result = await client.query<{ encode: string }>(
      /* sql */ `SELECT encode(encode_transaction(transaction), 'hex') FROM transaction WHERE hash = $1;`,
      [hexToBin(halTxHash)]
    );
    t.deepEqual(result.rows[0]!.encode, halTxRaw);
    t.pass();
  }
);

test.serial(
  '[e2e] records validation when another node announces a known transaction',
  async (t) => {
    peers.node2.sendMessage(
      new peers.node2.messages.Transaction(new Transaction(halTxRaw))
    );
    const delay = 1000;
    await sleep(delay);
    const validations = await client.query<{ name: string }>(
      /* sql */ `
      SELECT node.name
        FROM node_transaction
        INNER JOIN node
          ON node.internal_id = node_transaction.node_internal_id
        INNER JOIN transaction
          ON transaction.internal_id = node_transaction.transaction_internal_id
        WHERE transaction.hash = $1
        ORDER BY node.name ASC;
      `,
      [hexToBin(halTxHash)]
    );
    t.deepEqual(
      validations.rows.map(({ name }) => name),
      ['node1', 'node2']
    );
    await client.query(
      /* sql */ `
      DELETE FROM node_transaction
        USING node, transaction
        WHERE node_transaction.node_internal_id = node.internal_id
          AND node_transaction.transaction_internal_id = transaction.internal_id
          AND node.name = 'node2'
          AND transaction.hash = $1;
      `,
      [hexToBin(halTxHash)]
    );
  }
);

test.serial('[e2e] handles first chipnet CashTokens transaction', async (t) => {
  peers.node1.sendMessage(
    new peers.node1.messages.Transaction(new Transaction(chipnetCashTokensTx))
  );
  const delay = 1000;
  await sleep(delay);
  const result = await client.query<{ encode: string }>(
    /* sql */ `SELECT encode(encode_transaction(transaction), 'hex') FROM transaction WHERE hash = $1;`,
    [hexToBin(chipnetCashTokensTxHash)]
  );
  t.deepEqual(result.rows[0]!.encode, chipnetCashTokensTx);
  t.pass();
});

test.serial(
  '[e2e] after initial sync is complete, requests and saves inbound transactions as they are announced',
  async (t) => {
    peers.node1.sendMessage(
      peers.node1.messages.Inventory.forTransaction(
        Buffer.from(halTxSpent, 'hex')
      )
    );
    const delay = 1000;
    await sleep(delay);
    const result = await client.query<{ encode: string }>(
      /* sql */ `SELECT encode(encode_transaction(transaction), 'hex') FROM transaction WHERE hash = $1;`,
      [hexToBin(halTxSpent)]
    );
    t.deepEqual(result.rows[0]!.encode, halTxSpentRaw);
    t.pass();
  }
);

test.serial('[e2e] get hex-encoded genesis block header', async (t) => {
  /* eslint-disable @typescript-eslint/naming-convention */
  const encodedHex = (
    await client.query<{ block_header_encoded_hex: string }>(
      /* sql */ `SELECT block_header_encoded_hex (block) FROM block WHERE height = 0;`
    )
  ).rows[0]!.block_header_encoded_hex;
  /* eslint-enable @typescript-eslint/naming-convention */
  t.deepEqual(
    encodedHex,
    '0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c'
  );
});

test.serial('[e2e] get hex-encoded genesis block transaction', async (t) => {
  const genesisTxHash = hexToBin(
    '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
  );
  /* eslint-disable @typescript-eslint/naming-convention */
  const encodedHex = (
    await client.query<{ transaction_encoded_hex: string }>(
      /* sql */ `SELECT transaction_encoded_hex (transaction) FROM transaction WHERE hash = $1::bytea;`,
      [genesisTxHash]
    )
  ).rows[0]!.transaction_encoded_hex;
  /* eslint-enable @typescript-eslint/naming-convention */
  t.deepEqual(
    encodedHex,
    '01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000'
  );
});

test.serial(
  '[e2e] get hex-encoded genesis block (with transaction)',
  async (t) => {
    /* eslint-disable @typescript-eslint/naming-convention */
    const encodedHex = (
      await client.query<{ block_encoded_hex: string }>(
        /* sql */ `SELECT block_encoded_hex (block) FROM block WHERE height = 0;`
      )
    ).rows[0]!.block_encoded_hex;
    /* eslint-enable @typescript-eslint/naming-convention */
    t.deepEqual(encodedHex, genesisBlockRaw);
  }
);

test.serial(
  '[e2e] [postgres] value aggregates handle coinbase-only blocks',
  async (t) => {
    const aggregates = (
      await client.query<{
        feeSatoshis: string;
        generatedValueSatoshis: string;
        inputValueSatoshis: string;
        outputValueSatoshis: string;
      }>(/* sql */ `
        SELECT
          block_fee_satoshis(block)::text AS "feeSatoshis",
          block_generated_value_satoshis(block)::text AS "generatedValueSatoshis",
          block_input_value_satoshis(block)::text AS "inputValueSatoshis",
          block_output_value_satoshis(block)::text AS "outputValueSatoshis"
          FROM block WHERE height = 0;
      `)
    ).rows[0]!;
    t.deepEqual(aggregates, {
      feeSatoshis: '0',
      generatedValueSatoshis: '5000000000',
      inputValueSatoshis: '0',
      outputValueSatoshis: '5000000000',
    });
  }
);

test.serial(
  '[e2e] get hex-encoded block with multiple transactions',
  async (t) => {
    const blockWithMultipleTransactions = mockchainBeforeFork[1]!;
    t.true(blockWithMultipleTransactions.transactions.length > 1);
    /* eslint-disable @typescript-eslint/naming-convention */
    const encodedHex = (
      await client.query<{ block_encoded_hex: string }>(
        /* sql */ `SELECT block_encoded_hex(block) FROM block WHERE hash = $1::bytea;`,
        [hexToBin(blockWithMultipleTransactions.header.hash)]
      )
    ).rows[0]!.block_encoded_hex;
    /* eslint-enable @typescript-eslint/naming-convention */
    t.deepEqual(encodedHex, binToHex(blockWithMultipleTransactions.toBuffer()));
  }
);

test.serial(
  '[e2e] [postgres] transaction_data_carrier_outputs ignores empty locking bytecode',
  async (t) => {
    const txHash =
      '0000000000000000000000000000000000000000000000000000000000000075';
    await client.query(
      /* sql */ `
      INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase)
        VALUES ($1::bytea, 1, 0, 10, false);
    `,
      [hexToBin(txHash)]
    );
    await client.query(
      /* sql */ `
      INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode)
        VALUES ($1::bytea, 0, 1, $2::bytea);
    `,
      [hexToBin(txHash), hexToBin('')]
    );
    const outputs = await client.query<{ outputIndex: string }>(
      /* sql */ `
      SELECT output_index AS "outputIndex"
        FROM transaction_data_carrier_outputs(
          (SELECT transaction FROM transaction WHERE hash = $1::bytea)
        );
    `,
      [hexToBin(txHash)]
    );
    t.deepEqual(outputs.rows, []);
  }
);

const newBlocks = (
  node: 'node1' | 'node2' | 'node3',
  blocks: BitcoreBlock[]
) => {
  chainStates[node].push(...blocks);
  peers[node].sendMessage(
    new peers[node].messages.Headers(blocks.map((block) => block.header))
  );
};

test.serial(
  '[e2e] syncs blocks as they arrive, handles multiple chain tips',
  async (t) => {
    const [, tx1] = tipA[0]!.transactions;
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx1));
    logger.debug(`node1: sent tipA[0] transaction 0: ${tx1!.hash}`);
    newBlocks('node1', [tipA[0]!]);
    newBlocks('node2', [tipB[0]!]);
    newBlocks('node3', [tipB[0]!]);
    t.deepEqual(
      chainStates.node2.map((block) => block.header.hash),
      chainStates.node3.map((block) => block.header.hash)
    );
    await waitForStdout(/Saved new block – height:\s+3001[^\n]+nodes: node1/u);
    t.true(
      /Saved new block – height:\s+3001[^\n]+new txs: 3\/4[^\n]+nodes: node1/u.test(
        stdoutBuffer
      ),
      '3 of 4 transactions should be new in tip A block 3001. Has the mockchain changed? (If so, update this test.)'
    );
    await waitForStdout(
      /Saved new block – height:\s+3001[^\n]+nodes: node2, node3/u
    );
    t.pass();
  }
);

test.serial('[e2e] handles re-org of a single block', async (t) => {
  newBlocks('node1', [tipA[1]!]);
  newBlocks('node2', [tipB[1]!]);
  chainStates.node3.pop();
  newBlocks('node3', [tipA[0]!, tipA[1]!]);
  t.deepEqual(
    chainStates.node1.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  await waitForStdout(
    /node3: re-organization detected beginning at height: 3001. The following stale blocks were removed:/u
  );
  await waitForStdout(
    /Saved new block – height:\s+3002[^\n]+hash: a44a664d5acc560305fceb9ba3c7f195a5d78236c1705d5ae7434ba784005689[^\n]+nodes: node2/u
  );
  await waitForStdout(
    /Saved new block – height:\s+3002[^\n]+hash: 9c4feec6f35a54f2244f2ab14e1370e60713097be2ead8402e4ef68f96e07c8c[^\n]+nodes: node1, node3/u
  );
  t.pass();
});

test.serial('[e2e] new block saved after reorg', async (t) => {
  const acceptedBlocks = (
    await client.query<{
      hash: string;
      nodeName: string;
    }>(
      /* sql */ `
      SELECT node.name AS "nodeName", encode(block.hash, 'hex') AS hash
        FROM node_block
        INNER JOIN node
          ON node.internal_id = node_block.node_internal_id
        INNER JOIN block
          ON block.internal_id = node_block.block_internal_id
        WHERE node.name = 'node3'
          AND block.height = $1
        ORDER BY block.hash;
    `,
      [splitHeight + 1]
    )
  ).rows;
  t.deepEqual(acceptedBlocks, [
    { hash: tipA[0]!.header.hash, nodeName: 'node3' },
  ]);
});

test.serial('[e2e] handles reversal of single-block re-org', async (t) => {
  const tipStartIndex = 2;
  const tipEnd = 6;
  newBlocks('node1', tipA.slice(tipStartIndex, tipEnd));
  newBlocks('node2', tipB.slice(tipStartIndex, tipEnd));
  chainStates.node3.splice(splitHeight + 1);
  newBlocks('node3', tipB.slice(0, tipEnd));
  t.deepEqual(
    chainStates.node2.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  await waitForStdout(
    /node3: re-organization detected beginning at height: 3001. The following stale blocks were removed:/u
  );
  await waitForStdout(/Saved new block – height:\s+3006[^\n]+nodes: node1/u);
  await waitForStdout(
    /Saved new block – height:\s+3006[^\n]+nodes: node2, node3/u
  );
  t.pass();
});

test.serial('[e2e] handles re-org of 6 blocks', async (t) => {
  const tipStartIndex = 6;
  const tipEnd = 7;
  newBlocks('node1', tipA.slice(tipStartIndex, tipEnd));
  chainStates.node3.splice(splitHeight + 1);
  newBlocks('node3', tipA.slice(0, tipEnd));
  t.deepEqual(
    chainStates.node1.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  await waitForStdout(
    /node3: re-organization detected beginning at height: 3001. The following stale blocks were removed:/u
  );
  await waitForStdout(
    /Saved new block – height:\s+3007[^\n]+nodes: node1, node3/u
  );
  t.pass();
});

test.serial('[e2e] handles reversal of 6 block re-org', async (t) => {
  const tipStartIndex = 6;
  const tipEnd = 8;
  newBlocks('node2', tipB.slice(tipStartIndex, tipEnd));
  chainStates.node3.splice(splitHeight + 1);
  newBlocks('node3', tipB.slice(0, tipEnd));
  t.deepEqual(
    chainStates.node2.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  await waitForStdout(
    /node3: re-organization detected beginning at height: 3001. The following stale blocks were removed:/u
  );
  await waitForStdout(
    /Saved new block – height:\s+3008[^\n]+nodes: node2, node3/u
  );
  t.pass();
});

/**
 * Re-orgs larger than 8 blocks are only announced via INV message; this method
 * simulates blocks coming in as expected via headers messages.
 */
/* eslint-disable @typescript-eslint/no-magic-numbers */
const slowFeedBlocks = (
  node: 'node1' | 'node2' | 'node3',
  blocks: BitcoreBlock[],
  chunk = 6
) => {
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let i = 0; i < blocks.length; i += chunk) {
    newBlocks(node, blocks.slice(i, i + chunk));
  }
};

test.serial('[e2e] handles re-org of 100 blocks', async (t) => {
  const tipEnd = 101;
  slowFeedBlocks('node1', tipA.slice(7, tipEnd));
  slowFeedBlocks('node2', tipB.slice(8, tipEnd));
  chainStates.node3.splice(splitHeight + 1);
  chainStates.node3.push(...tipA.slice(0, tipEnd));
  t.deepEqual(
    chainStates.node1.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  peers.node3.sendMessage(
    peers.node3.messages.Inventory.forBlock(tipA[tipEnd - 1]!.header.hash)
  );
  await waitForStdout(
    `node3: received unexpected block inventory item with hash: ${swapEndianness(
      tipA[tipEnd - 1]!.header.hash
    )}`
  );
  await waitForStdout(
    /node3: re-organization detected beginning at height: 3001. The following stale blocks were removed:/u
  );
  await waitForStdout(/Saved new block – height:\s+3100[^\n]+nodes: node2/u);
  await waitForStdout(
    /Saved new block – height:\s+3100[^\n]+nodes: node1, node3/u
  );
  t.pass();
});

test.serial('[e2e] records stale blocks', async (t) => {
  const tipStartIndex = 101;
  const tipEnd1 = 150;
  const tipEnd2 = 160;
  slowFeedBlocks('node1', tipA.slice(tipStartIndex, tipEnd1));
  slowFeedBlocks('node2', tipB.slice(tipStartIndex, tipEnd1));
  slowFeedBlocks('node3', tipA.slice(tipStartIndex, tipEnd1));
  newBlocks('node3', tipAStale150);
  await waitForStdout(/Saved new block – height:\s+3153[^\n]+nodes: node3/u);
  chainStates.node3.splice(splitHeight + tipEnd1 + 1);
  slowFeedBlocks('node1', tipA.slice(tipEnd1, tipEnd2));
  slowFeedBlocks('node2', tipB.slice(tipEnd1, tipEnd2));
  slowFeedBlocks('node3', tipA.slice(tipEnd1, tipEnd2));
  t.deepEqual(
    chainStates.node1.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  await waitForStdout(
    /node3: re-organization detected beginning at height: 3151. The following stale blocks were removed:/u
  );
  await waitForStdout(/Saved new block – height:\s+3160[^\n]+nodes: node2/u);
  await waitForStdout(
    /Saved new block – height:\s+3160[^\n]+nodes: node1, node3/u
  );
  t.pass();
});
/* eslint-enable @typescript-eslint/no-magic-numbers */

test.serial(
  '[e2e] records double-spends accepted via mempool and via block',
  async (t) => {
    // eslint-disable-next-line prefer-destructuring
    const tx1 = tipA[160]!.transactions[1];
    const mock1 = generateMockDoubleSpend(tx1!.inputs, true);
    // TODO: race condition – this should work without a delay?
    const delay = 1000;
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx1));
    logger.debug(
      `node1: sent original transaction to double-spend: ${tx1!.hash}`
    );
    await sleep(delay);
    peers.node1.sendMessage(new peers.node1.messages.Transaction(mock1));
    logger.debug(`node1: sent double-spending transaction: ${mock1.hash}`);
    await sleep(delay);
    newBlocks('node1', [tipA[160]!]);
    newBlocks('node2', [tipB[160]!]);
    newBlocks('node3', [tipA[160]!]);
    logger.debug(
      `node1: sent block including original transaction: ${tx1!.hash}`
    );
    await waitForStdout(/Saved new block – height:\s+3161[^\n]+nodes: node2/u);
    await waitForStdout(
      /Saved new block – height:\s+3161[^\n]+nodes: node1, node3/u
    );
    /* eslint-disable @typescript-eslint/naming-convention */
    const res = await client.query<{
      internal_id: number;
      node_internal_id: number;
      transaction_internal_id: number;
      validated_at: string;
      replaced_at: string;
    }>(
      /* sql */ `SELECT * FROM node_transaction_history WHERE transaction_internal_id IN (SELECT internal_id FROM transaction WHERE hash IN ($1::bytea, $2::bytea)) ORDER BY validated_at ASC;
         `,
      [hexToBin(tx1!.hash), hexToBin(mock1.hash)]
    );
    /* eslint-enable @typescript-eslint/naming-convention */
    // eslint-disable-next-line @typescript-eslint/no-magic-numbers
    t.deepEqual(res.rows.length, 2);
    t.deepEqual(res.rows[0]!.node_internal_id, res.rows[1]!.node_internal_id);
    t.deepEqual(res.rows[0]!.replaced_at, res.rows[1]!.validated_at);
    t.true(
      new Date(res.rows[0]!.validated_at) <= new Date(res.rows[0]!.replaced_at)
    );
    t.true(
      new Date(res.rows[1]!.validated_at) <= new Date(res.rows[1]!.replaced_at)
    );
    t.pass();
  }
);

test.serial(
  '[e2e] removes node_transaction entries which are confirmed by a block',
  async (t) => {
    const [, tx1, tx2, tx3] = tipA[161]!.transactions;
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx1));
    logger.debug(`node1: sent tx1: ${tx1!.hash}`);
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx2));
    logger.debug(`node1: sent tx2: ${tx2!.hash}`);
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx3));
    logger.debug(`node1: sent tx3: ${tx2!.hash}`);
    const delay = 100;
    await sleep(delay);
    const mempool1 = await client.query<{ encode: string }>(
      /* sql */ `SELECT encode(hash, 'hex') FROM node_transaction JOIN transaction ON node_transaction.transaction_internal_id = transaction.internal_id ORDER BY hash ASC;`
    );
    t.deepEqual(mempool1.rows, [
      { encode: tx1!.hash },
      { encode: tx3!.hash },
      { encode: tx2!.hash },
      { encode: chipnetCashTokensTxHash },
      { encode: halTxSpent },
      { encode: halTxHash },
    ]);
    newBlocks('node1', [tipA[161]!]);
    newBlocks('node2', [tipB[161]!]);
    newBlocks('node3', [tipA[161]!]);
    t.deepEqual(
      chainStates.node1.map((block) => block.header.hash),
      chainStates.node3.map((block) => block.header.hash)
    );
    await waitForStdout(/Saved new block – height:\s+3162[^\n]+nodes: node2/u);
    await waitForStdout(
      /Saved new block – height:\s+3162[^\n]+nodes: node1, node3/u
    );
    const mempool2 = await client.query<{ encode: string }>(
      /* sql */ `SELECT encode(hash, 'hex') FROM node_transaction JOIN transaction ON node_transaction.transaction_internal_id = transaction.internal_id ORDER BY hash ASC;`
    );
    t.deepEqual(mempool2.rows, [
      { encode: chipnetCashTokensTxHash },
      { encode: halTxSpent },
      { encode: halTxHash },
    ]);
    t.pass();
  }
);

test.serial('[e2e] shuts down with SIGINT', async (t) => {
  chaingraphProcess2!.kill('SIGINT');
  await waitForStdout('Shutting down...');
  await waitForStdout('Exiting...');
  await chaingraphProcess2;
  t.pass();
});

const historicalRepairTipIndex = 161;
const historicalRepairTransactionIndex = 1;
const historicalRepairBlock = tipA[historicalRepairTipIndex]!;
const historicalRepairBlockHash = historicalRepairBlock.header.hash;

test.serial(
  '[e2e] prepares incomplete historical block transaction before restart',
  async (t) => {
    const transactionHash =
      historicalRepairBlock.transactions[historicalRepairTransactionIndex]!
        .hash;
    const selectedTransaction = (
      await client.query<{ hash: string }>(
        /* sql */ `
        SELECT encode(transaction.hash, 'hex') AS hash
          FROM block_transaction
          INNER JOIN block
            ON block.internal_id = block_transaction.block_internal_id
          INNER JOIN transaction
            ON transaction.internal_id =
              block_transaction.transaction_internal_id
          WHERE block.hash = $1
            AND block_transaction.transaction_index = $2;
      `,
        [hexToBin(historicalRepairBlockHash), historicalRepairTransactionIndex]
      )
    ).rows;
    t.deepEqual(selectedTransaction, [{ hash: transactionHash }]);
    await client.query(
      /* sql */ `
        DELETE FROM block_transaction
          USING block
          WHERE block.internal_id = block_transaction.block_internal_id
            AND block.hash = $1
            AND block_transaction.transaction_index = $2;
      `,
      [hexToBin(historicalRepairBlockHash), historicalRepairTransactionIndex]
    );
    t.deepEqual(
      await getBlockTransactionCount(historicalRepairBlockHash),
      historicalRepairBlock.transactions.length - 1
    );
  }
);

test.serial(
  '[e2e] restores sync-state from database on restart (after initial sync)',
  async (t) => {
    chaingraphProcess3 = execa('node', ['./bin/chaingraph.js'], {
      env: e2eEnvVariables2,
      stdio: 'pipe',
    });
    if (chaingraphProcess3.stdout === null) {
      t.fail('`chaingraphProcess2` stdout is not available.');
      return;
    }
    chaingraphProcess3.stdout.on('data', (chunk) => {
      stdoutBuffer += chunk;
      handleStdout();
    });
    await waitForStdout('Starting Chaingraph...');
    await waitForStdout('Restored chain for node node1');
    await waitForStdout('Restored chain for node node2');
    await waitForStdout('Restored chain for node node4');
    t.pass();
  }
);

test.serial('[e2e] catches up a new node via headers', async (t) => {
  t.timeout(oneMinute);
  await waitForStdout(
    `node4: accepted 2000 existing blocks from height 1 to height 2000 (hash: ${
      chainStates.node3[2000]!.header.hash
    })`,
    oneMinute
  );
  await waitForStdout(
    `node4: accepted 1162 existing blocks from height 2001 to height 3162 (hash: ${
      chainStates.node3[3162]!.header.hash
    })`
  );
  const node4Blocks = await client.query<{ encode: string; height: string }>(
    /* sql */ `SELECT encode(hash, 'hex'), height from node_block JOIN node ON node.internal_id = node_block.node_internal_id JOIN block ON block.internal_id = node_block.block_internal_id WHERE node.name = 'node4' ORDER BY height DESC;`
  );
  const expectedCount = 3163;
  t.deepEqual(node4Blocks.rowCount, expectedCount);
  t.deepEqual(node4Blocks.rows[0], {
    encode: tipA[161]!.header.hash,
    height: '3162',
  });
  await waitForStdout('Agent: enabled mempool tracking.');
  t.pass();
});

test.serial(
  '[e2e] self-heals incomplete historical block transactions on startup',
  async (t) => {
    t.timeout(oneMinute);
    t.deepEqual(
      await waitForBlockTransactionCount(
        historicalRepairBlockHash,
        historicalRepairBlock.transactions.length
      ),
      historicalRepairBlock.transactions.length
    );
  }
);

test.serial(
  '[e2e] handles empty headers messages (fully-synced)',
  async (t) => {
    peers.node1.sendMessage(new peers.node1.messages.Headers([]));
    await waitForStdout(
      'node1: received empty headers message – headers-syncing completed'
    );
    t.pass();
  }
);

test.serial(
  '[e2e] saves block transactions if previously announced tx is seen but not yet saved',
  async (t) => {
    const tipStartIndex = 162;
    const [, tx1] = tipA[tipStartIndex]!.transactions;
    mempool[swapEndianness(tx1!.hash)] = false;
    const node1RequestedTx = new Promise((res) => {
      node1.once('peergetdata', (_, message) => {
        res(message.inventory);
      });
    });
    peers.node1.sendMessage(
      peers.node1.messages.Inventory.forTransaction(
        Buffer.from(tx1!.hash, 'hex')
      )
    );
    await node1RequestedTx;
    logger.debug(
      `node1: announced tipA[${tipStartIndex}] transaction 1 without providing the transaction: ${
        tx1!.hash
      }`
    );
    newBlocks('node1', [tipA[tipStartIndex]!]);
    newBlocks('node2', [tipB[tipStartIndex]!]);
    newBlocks('node3', [tipA[tipStartIndex]!]);
    await waitForStdout(/Saved new block – height:\s+3163[^\n]+nodes: node2/u);
    await waitForStdout(
      /Saved new block – height:\s+3163[^\n]+nodes: node1, node4/u
    );
    const blockTransactionCount = (
      await client.query<{ count: string }>(
        /* sql */ `
        SELECT COUNT(*) FROM block_transaction
          INNER JOIN block ON block.internal_id = block_transaction.block_internal_id
          WHERE block.hash = $1;
      `,
        [hexToBin(tipA[tipStartIndex]!.header.hash)]
      )
    ).rows[0]!.count;
    t.deepEqual(
      blockTransactionCount,
      tipA[tipStartIndex]!.transactions.length.toString()
    );
  }
);

test.serial('[e2e] syncs remaining blocks one-by-one', async (t) => {
  const tipStartIndex = 163;
  slowFeedBlocks('node1', tipA.slice(tipStartIndex), 1);
  slowFeedBlocks('node2', tipB.slice(tipStartIndex), 1);
  /**
   * (Renamed to node4 via env variables)
   */
  slowFeedBlocks('node3', tipA.slice(tipStartIndex), 1);
  t.deepEqual(
    chainStates.node1.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  t.deepEqual(
    chainStates.node1
      .slice(0, splitHeight + 1)
      .map((block) => block.header.hash),
    chainStates.node2
      .slice(0, splitHeight + 1)
      .map((block) => block.header.hash)
  );
  await waitForStdout(/Saved new block – height:\s+3200[^\n]+nodes: node2/u);
  await waitForStdout(
    /Saved new block – height:\s+3200[^\n]+nodes: node1, node4/u
  );
  t.pass();
});

test.serial('[e2e] [api] 404: logs unknown request urls', async (t) => {
  const res = await got(
    `http://localhost:${chaingraphInternalApiPort}/unknown-URL`,
    { throwHttpErrors: false }
  );
  t.deepEqual(res.statusCode, StatusCode.notFound);
  t.deepEqual(res.body, '{"error":"not found"}');
  await waitForStdout('[API] issued 404 for req.url: /unknown-URL');
  t.pass();
});

/* eslint-disable @typescript-eslint/naming-convention, camelcase */
const validRequestWithoutNode = {
  action: { name: 'send_transaction' },
  input: {
    request: {
      encoded_hex:
        '0100000001c9cf39d7d29ecb8ea68e8fd2019ae047ca3c36e6e4d6a2f4d8c070eced00fdd1010000006441db7f7de61e8ed43984921ded4fb6148152085a5f31725c56b1da86d929d4fc4346c516edf93ac81dcf5b84518ae1dd5dec8ad863f891ac4a464ae8fabd22375e41210334242a73fe4b0d88ddfe6dc7202fa1b60785dac3fe5f7d92a616f8792f5f3a47feffffff0200e87648170000001976a914ab4cc0d4c6ffadbce88ee74a7b856fe6dd02acb688acd4de9265170100001976a91422afddf849a9f2f27aabb7d88e06a1919c0a77d688acbc000200',
      // node_internal_id: [number],
    },
  },
  request_query:
    'mutation {\n  send_transaction(request: {node_internal_id: 1, encoded_hex: "0100000001c9cf39d7d29ecb8ea68e8fd2019ae047ca3c36e6e4d6a2f4d8c070eced00fdd1010000006441db7f7de61e8ed43984921ded4fb6148152085a5f31725c56b1da86d929d4fc4346c516edf93ac81dcf5b84518ae1dd5dec8ad863f891ac4a464ae8fabd22375e41210334242a73fe4b0d88ddfe6dc7202fa1b60785dac3fe5f7d92a616f8792f5f3a47feffffff0200e87648170000001976a914ab4cc0d4c6ffadbce88ee74a7b856fe6dd02acb688acd4de9265170100001976a91422afddf849a9f2f27aabb7d88e06a1919c0a77d688acbc000200"}) {\n    transaction_hash\n    validation_error_message\n    validation_success\n    transmission_error_message\n    transmission_success\n  }\n}\n',
  session_variables: { 'x-hasura-role': 'public' },
};

test.serial(
  '[e2e] [api] /send-transaction: malformed (missing input)',
  async (t) => {
    const res = await got.post(
      `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
      {
        json: { ...validRequestWithoutNode, input: {} },
        throwHttpErrors: false,
      }
    );
    t.deepEqual(res.statusCode, StatusCode.badRequest);
    t.deepEqual(res.body, '{"message":"malformed request"}');
    await waitForStdout('[API] /send-transaction: malformed request.');
    t.pass();
  }
);

test.serial(
  '[e2e] [api] /send-transaction: malformed (missing encoded_hex)',
  async (t) => {
    const res = await got.post(
      `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
      {
        json: {
          ...validRequestWithoutNode,
          input: { request: { node_internal_id: 1 } },
        },
        throwHttpErrors: false,
      }
    );
    t.deepEqual(res.statusCode, StatusCode.badRequest);
    t.deepEqual(res.body, `{"message":"'encoded_hex' must be a string"}`);
  }
);

test.serial(
  '[e2e] [api] /send-transaction: malformed (missing node_internal_id)',
  async (t) => {
    const res = await got.post(
      `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
      {
        json: validRequestWithoutNode,
        throwHttpErrors: false,
      }
    );
    t.deepEqual(res.statusCode, StatusCode.badRequest);
    t.deepEqual(res.body, `{"message":"'node_internal_id' must be a number"}`);
  }
);

test.serial(
  '[e2e] [api] /send-transaction: malformed (node_internal_id is not a number)',
  async (t) => {
    const res = await got.post(
      `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
      {
        json: {
          ...validRequestWithoutNode,
          input: {
            request: {
              encoded_hex: validRequestWithoutNode.input.request.encoded_hex,
              node_internal_id: 'invalid',
            },
          },
        },
        throwHttpErrors: false,
      }
    );
    t.deepEqual(res.statusCode, StatusCode.badRequest);
    t.deepEqual(res.body, `{"message":"'node_internal_id' must be a number"}`);
  }
);

test.serial(
  '[e2e] [api] /send-transaction: malformed (encoded_hex is not a string)',
  async (t) => {
    const res = await got.post(
      `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
      {
        json: {
          ...validRequestWithoutNode,
          input: {
            request: {
              encoded_hex: 1,
              node_internal_id: 0,
            },
          },
        },
        throwHttpErrors: false,
      }
    );
    t.deepEqual(res.statusCode, StatusCode.badRequest);
    t.deepEqual(res.body, `{"message":"'encoded_hex' must be a string"}`);
  }
);

test.serial(
  '[e2e] [api] /send-transaction: unknown node_internal_id',
  async (t) => {
    const res = await got.post(
      `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
      {
        json: {
          ...validRequestWithoutNode,
          input: {
            request: {
              encoded_hex: validRequestWithoutNode.input.request.encoded_hex,
              node_internal_id: 100,
            },
          },
        },
        throwHttpErrors: false,
      }
    );
    t.deepEqual(res.statusCode, StatusCode.success);
    t.deepEqual(
      res.body,
      JSON.stringify({
        transaction_hash:
          'dd92bef1b09c1c2eaf9a9f0ce29d330bffa54a3003d44767d3e32b3f6fbab0dd',
        validation_success: true,
        // eslint-disable-next-line sort-keys
        transmission_error_message:
          'Unable to connect to the requested node (node_internal_id: 100).',
        transmission_success: false,
      })
    );
  }
);

// eslint-disable-next-line functional/no-let
let node1InternalId = 0;
test.serial('[e2e] [api] /send-transaction: invalid TX', async (t) => {
  node1InternalId = (
    await client.query<{ internal_id: number }>(
      /* sql */ `SELECT internal_id FROM node ORDER BY name ASC`
    )
  ).rows[0]!.internal_id;
  const res = await got.post(
    `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
    {
      json: {
        ...validRequestWithoutNode,
        input: {
          request: {
            encoded_hex: '00',
            node_internal_id: node1InternalId,
          },
        },
      },
    }
  );
  t.deepEqual(res.statusCode, StatusCode.success);
  t.deepEqual(
    res.body,
    JSON.stringify({
      transaction_hash:
        '9a538906e6466ebd2617d321f71bc94e56056ce213d366773699e28158e00614',
      validation_error_message:
        'Error reading transaction. Error reading Uint32LE: requires 4 bytes. Provided length: 1',
      validation_success: false,
      // eslint-disable-next-line sort-keys
      transmission_success: false,
    })
  );
});

test.serial('[e2e] [api] /send-transaction: valid', async (t) => {
  const res = await got.post(
    `http://localhost:${chaingraphInternalApiPort}/send-transaction`,
    {
      json: {
        ...validRequestWithoutNode,
        input: {
          request: {
            encoded_hex: validRequestWithoutNode.input.request.encoded_hex,
            node_internal_id: node1InternalId,
          },
        },
      },
    }
  );
  t.deepEqual(res.statusCode, StatusCode.success);
  t.deepEqual(
    res.body,
    JSON.stringify({
      transaction_hash:
        'dd92bef1b09c1c2eaf9a9f0ce29d330bffa54a3003d44767d3e32b3f6fbab0dd',
      validation_success: true,
      // eslint-disable-next-line sort-keys
      transmission_success: true,
    })
  );
});
/* eslint-enable @typescript-eslint/naming-convention, camelcase */

/**
 * `CHAINGRAPH_UNSPENT_TRACKING` (experiment): after every scenario above
 * (mempool spends, double-spends, expired and replaced mempool transactions,
 * stale blocks and re-orgs on several nodes), the stored read model must equal
 * the F1g `unspent_output` reference. The read model tracks spends across all
 * nodes, so the reference uses "accepted by any node" in place of one node.
 * Run the e2e suite once per mode (`CHAINGRAPH_UNSPENT_TRACKING=off|marker|settable`).
 */
const unspentTrackingMode = process.env.CHAINGRAPH_UNSPENT_TRACKING ?? 'off';
const anyNodeAccepts = (transactionInternalId: string) => /* sql */ `
  (EXISTS (SELECT 1 FROM block_transaction bt CROSS JOIN node n
             JOIN node_block nb ON nb.node_internal_id = n.internal_id AND nb.block_internal_id = bt.block_internal_id
             WHERE bt.transaction_internal_id = ${transactionInternalId})
   OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = ${transactionInternalId}))`;
const nodeAccepts = (
  transactionInternalId: string,
  nodeId: number
) => /* sql */ `
  (EXISTS (SELECT 1 FROM block_transaction bt
             JOIN node_block nb ON nb.node_internal_id = ${nodeId} AND nb.block_internal_id = bt.block_internal_id
             WHERE bt.transaction_internal_id = ${transactionInternalId})
   OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = ${transactionInternalId} AND nt.node_internal_id = ${nodeId}))`;
/**
 * F1g for one node (`unspent_output(node)` with the node resolved to its id).
 */
const nodeUnspentReferenceSql = (nodeId: number) => /* sql */ `
  SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint FROM output o
    WHERE CASE
      WHEN EXISTS (SELECT 1 FROM input i WHERE i.outpoint_transaction_hash = o.transaction_hash
                     AND i.outpoint_index = o.output_index AND ${nodeAccepts(
                       'i.transaction_internal_id',
                       nodeId
                     )})
      THEN false
      ELSE EXISTS (SELECT 1 FROM transaction t WHERE t.hash = o.transaction_hash AND ${nodeAccepts(
        't.internal_id',
        nodeId
      )})
    END`;
const unspentReferenceSql = /* sql */ `
  SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint FROM output o
    WHERE CASE
      WHEN EXISTS (SELECT 1 FROM input i WHERE i.outpoint_transaction_hash = o.transaction_hash
                     AND i.outpoint_index = o.output_index AND ${anyNodeAccepts(
                       'i.transaction_internal_id'
                     )})
      THEN false
      ELSE EXISTS (SELECT 1 FROM transaction t WHERE t.hash = o.transaction_hash AND ${anyNodeAccepts(
        't.internal_id'
      )})
    END`;
const createdByAcceptedSql = /* sql */ `
  EXISTS (SELECT 1 FROM transaction t WHERE t.hash = o.transaction_hash AND ${anyNodeAccepts(
    't.internal_id'
  )})`;

/* eslint-disable @typescript-eslint/no-magic-numbers */
/**
 * Hash for the explicit unspent-tracking scenario (`e7` + one distinct byte).
 */
const scenarioHash = (byte: string) => `e7${byte.repeat(31)}`;
const scenarioTransaction = (
  byte: string,
  spends: (number | [string, number])[],
  outputCount = 1
): ChaingraphTransaction => ({
  hash: scenarioHash(byte),
  inputs: spends.map((spend) => ({
    outpointIndex: typeof spend === 'number' ? spend : spend[1],
    outpointTransactionHash: scenarioHash(
      typeof spend === 'number' ? 'f0' : spend[0]
    ),
    sequenceNumber: 0,
    unlockingBytecode: '51',
  })),
  isCoinbase: false,
  locktime: 0,
  outputs: Array.from({ length: outputCount }, () => ({
    lockingBytecode: '51',
    valueSatoshis: 1000n,
  })),
  sizeBytes: 60,
  version: 2,
});

/**
 * Drive the agent's own DB functions (as the agent calls them) through the
 * cases the read model must follow, on top of the e2e chain state: a mined
 * spend, a mempool spend, a dropped mempool spender, a block spender removed by a re-org
 * and its re-acceptance via headers on another node, and a cross-node
 * double-spend whose first spender is dropped. After each step, `check()`
 * compares the read model with the reference.
 */
const runUnspentTrackingScenario = async (
  check: (step: string) => Promise<void>
) => {
  const originalPostgresConnectionString =
    process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
  process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
    postgresE2eConnectionStringTestDb;
  /*
   * A fresh module instance: an earlier test ends the shared module's pool.
   */
  const scenarioDbModule = '../db.js?unspent-tracking-scenario';
  const db = (await import(scenarioDbModule)) as typeof DbModule;
  const nodeId = async (name: string) =>
    Number(
      (
        await client.query<{ id: string }>(
          /* sql */ `SELECT internal_id AS id FROM node WHERE name = $1;`,
          [name]
        )
      ).rows[0]!.id
    );
  const [nodeA, nodeB] = [await nodeId('node1'), await nodeId('node2')];
  const block = (byte: string, transactions: ChaingraphTransaction[]) => ({
    bits: 0,
    hash: scenarioHash(byte),
    height: 999_000,
    merkleRoot: '00'.repeat(32),
    nonce: 0,
    previousBlockHash: '00'.repeat(32),
    sizeBytes: 0,
    timestamp: 0,
    transactions,
    version: 1,
  });
  const saveBlockFor = async (
    blockToSave: ChaingraphBlock,
    nodeInternalId: number
  ) =>
    db.saveBlock({
      block: blockToSave,
      nodeAcceptances: [
        { acceptedAt: new Date(), nodeInternalId, nodeName: 'scenario' },
      ],
      transactionCache: new Map() as unknown as Parameters<
        typeof db.saveBlock
      >[0]['transactionCache'],
    });
  const saveMempoolTransaction = async (
    transaction: ChaingraphTransaction,
    nodeInternalId: number
  ) =>
    db.saveTransactionForNodes(transaction, [
      { nodeInternalId, validatedAt: new Date() },
    ]);
  const transactionId = async (byte: string) =>
    Number(
      (
        await client.query<{ id: string }>(
          /* sql */ `SELECT internal_id AS id FROM transaction WHERE hash = $1;`,
          [hexToBin(scenarioHash(byte))]
        )
      ).rows[0]!.id
    );
  // eslint-disable-next-line functional/no-try-statement
  try {
    // fund:0 stays unspent; fund:1 is spent in the same block
    await saveBlockFor(
      block('b1', [
        scenarioTransaction('f0', [], 10),
        scenarioTransaction('a1', [1]),
      ]),
      nodeA
    );
    await check('mined spend');
    /*
     * node b also accepts block b1 (via headers); a transaction only node b
     * accepts spends f0:9: unspent for node a only, its output for node b only
     */
    await db.acceptBlocksViaHeaders(
      nodeB,
      [{ hash: scenarioHash('b1'), height: 999_000 }],
      new Date()
    );
    await check('block accepted by a second node via headers');
    await saveMempoolTransaction(scenarioTransaction('e2', [9]), nodeB);
    await check('transaction accepted by one node only');
    await saveMempoolTransaction(scenarioTransaction('a2', [2]), nodeA);
    await check('mempool spend');
    await saveMempoolTransaction(scenarioTransaction('a3', [3]), nodeA);
    await check('mempool spend before drop');
    await db.archiveMempoolTransaction({
      nodeInternalId: nodeA,
      replacedAt: new Date(),
      transactionInternalId: await transactionId('a3'),
    });
    await check('dropped mempool spender');
    const staleBlock = block('b2', [scenarioTransaction('a4', [4])]);
    await saveBlockFor(staleBlock, nodeA);
    await check('block spend before re-org');
    await db.removeStaleBlocksForNode(nodeA, [staleBlock.hash]);
    await check('block spender removed by a re-org');
    await db.acceptBlocksViaHeaders(
      nodeB,
      [{ hash: staleBlock.hash, height: staleBlock.height }],
      new Date()
    );
    await check('stale block re-accepted via headers by another node');
    await saveMempoolTransaction(scenarioTransaction('a5', [5]), nodeA);
    await saveMempoolTransaction(scenarioTransaction('a6', [5]), nodeB);
    await check('cross-node double-spend');
    await db.archiveMempoolTransaction({
      nodeInternalId: nodeA,
      replacedAt: new Date(),
      transactionInternalId: await transactionId('a5'),
    });
    await check('first of a cross-node double-spend dropped');
    // child-before-parent across two saves: a mempool child, then its parent
    await saveMempoolTransaction(scenarioTransaction('c1', [['b0', 0]]), nodeA);
    await check('mempool child saved before its parent');
    await saveBlockFor(
      block('b6', [scenarioTransaction('b0', [['ff', 0]], 2)]),
      nodeA
    );
    await check('parent saved after its mempool child (policy A)');
    // child-before-parent across two blocks
    await saveBlockFor(
      block('b7', [scenarioTransaction('c2', [['b1', 0]])]),
      nodeA
    );
    await saveBlockFor(
      block('b8', [scenarioTransaction('b1', [['ff', 1]], 1)]),
      nodeA
    );
    await check('parent block saved after its child block (policy A)');
    // a later mempool-only spender must not replace a block-accepted one
    await saveMempoolTransaction(scenarioTransaction('e1', [1]), nodeB);
    await check('conflicting mempool spender of a mined output');
    // a block-accepted spender replaces a mempool-only one
    await saveMempoolTransaction(scenarioTransaction('a7', [6]), nodeA);
    await saveBlockFor(block('b9', [scenarioTransaction('a8', [6])]), nodeA);
    await check('block spender replaces a mempool spender');
    // re-org replacing the spender: replacement block saved first
    const replacedBlock = block('ba', [scenarioTransaction('91', [7])]);
    await saveBlockFor(replacedBlock, nodeA);
    await saveBlockFor(block('bb', [scenarioTransaction('92', [7])]), nodeA);
    await check('competing block spender while the first is accepted');
    await db.removeStaleBlocksForNode(nodeA, [replacedBlock.hash]);
    await check('re-org replaced the spender (replacement saved first)');
    // re-org replacing the spender: stale block removed first
    const staleFirstBlock = block('bc', [scenarioTransaction('93', [8])]);
    await saveBlockFor(staleFirstBlock, nodeA);
    await db.removeStaleBlocksForNode(nodeA, [staleFirstBlock.hash]);
    await saveBlockFor(block('bd', [scenarioTransaction('94', [8])]), nodeA);
    await check('re-org replaced the spender (stale removed first)');
  } finally {
    await db.pool.end();
    if (originalPostgresConnectionString === undefined) {
      delete process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
    } else {
      process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
        originalPostgresConnectionString;
    }
  }
};

test.serial(
  `[e2e] unspent tracking (${unspentTrackingMode}): read model equals the F1g reference`,
  async (t) => {
    const rows = async (sql: string) =>
      (await client.query<{ outpoint: string }>(sql)).rows
        .map((row) => row.outpoint)
        .sort((a, b) => a.localeCompare(b));
    const count = async (sql: string) =>
      Number((await client.query<{ n: string }>(sql)).rows[0]!.n);
    /*
     * Outputs inserted directly by the fixtures above (not by the agent) are
     * not tracked (NULL marker, no set row ever written). Compare on outputs
     * of transactions some node ever accepted (current or history) and, for
     * `marker`, with a non-NULL marker.
     */
    const trackedDomain = /* sql */ `
      o.transaction_hash IN (SELECT t.hash FROM transaction t
        WHERE EXISTS (SELECT 1 FROM block_transaction bt WHERE bt.transaction_internal_id = t.internal_id)
           OR EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id = t.internal_id)
           OR EXISTS (SELECT 1 FROM node_transaction_history h WHERE h.transaction_internal_id = t.internal_id))`;
    /*
     * `settable` writes no set rows while POLICY A cannot run (initial sync,
     * no spent_by_index): those outputs are unaudited, so the comparison
     * starts at the transactions saved after this point.
     */
    const settableWatermark = (
      await client.query<{ id: string }>(
        /* sql */ `SELECT COALESCE(max(internal_id), 0) AS id FROM transaction;`
      )
    ).rows[0]!.id;
    const domain = async () =>
      new Set(
        await rows(
          /* sql */ `SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint FROM output o WHERE ${trackedDomain}${
            unspentTrackingMode === 'marker'
              ? ' AND o.spent_by_transaction_internal_id IS NOT NULL'
              : unspentTrackingMode === 'bitmask'
              ? ' AND o.unspent_node_bits IS NOT NULL'
              : ` AND o.transaction_hash IN (SELECT hash FROM transaction WHERE internal_id > ${settableWatermark})`
          }`
        )
      );
    const inDomain = async (outpoints: string[]) => {
      const tracked = await domain();
      return outpoints.filter((outpoint) => tracked.has(outpoint));
    };
    const reference = async () => inDomain(await rows(unspentReferenceSql));
    const stored = async () =>
      inDomain(
        await rows(
          unspentTrackingMode === 'marker'
            ? /* sql */ `SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint FROM output o
            WHERE o.spent_by_transaction_internal_id = 0 AND ${createdByAcceptedSql}`
            : /* sql */ `SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint
            FROM unspent_output_set u JOIN output o ON o.transaction_hash = u.transaction_hash AND o.output_index = u.output_index
            WHERE ${createdByAcceptedSql}`
        )
      );
    // eslint-disable-next-line complexity
    const check = async (step: string) => {
      /*
       * Fresh statistics: with stale estimates the reference query can plan
       * correlated scans instead of hashed sub-plans (minutes, not ms).
       */
      await client.query('ANALYZE;');
      if (unspentTrackingMode === 'off') {
        t.is(
          await count(
            `SELECT count(*) AS n FROM output WHERE spent_by_transaction_internal_id IS NOT NULL OR unspent_node_bits IS NOT NULL`
          ),
          0,
          step
        );
        t.is(await count(`SELECT count(*) AS n FROM unspent_output_set`), 0);
        return;
      }
      if (unspentTrackingMode === 'bitmask') {
        const nodeIds = (
          await client.query<{ id: string }>(
            /* sql */ `SELECT internal_id AS id FROM node ORDER BY internal_id;`
          )
        ).rows.map((row) => Number(row.id));
        await nodeIds.reduce<Promise<unknown>>(
          async (chain, nodeId) =>
            chain.then(async () => {
              t.deepEqual(
                await inDomain(
                  await rows(/* sql */ `SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint FROM output o
                  WHERE (o.unspent_node_bits & (1::bigint << ${nodeId})) <> 0`)
                ),
                await inDomain(await rows(nodeUnspentReferenceSql(nodeId))),
                `${step}: node ${nodeId}`
              );
            }),
          Promise.resolve()
        );
        return;
      }
      const expected = await reference();
      t.true(
        expected.length > 0 ||
          (unspentTrackingMode === 'settable' && step === 'e2e chain state'),
        step
      );
      t.deepEqual(await stored(), expected, step);
      if (unspentTrackingMode === 'marker') {
        t.is(
          await count(/* sql */ `
          SELECT count(*) AS n FROM output o WHERE o.spent_by_transaction_internal_id > 0
            AND NOT EXISTS (SELECT 1 FROM input i WHERE i.transaction_internal_id = o.spent_by_transaction_internal_id
              AND i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index
              AND ${anyNodeAccepts('i.transaction_internal_id')})`),
          0,
          `${step}: every marker points at an accepted spender of that output`
        );
        t.is(
          await count(/* sql */ `
          SELECT count(*) AS n FROM output o WHERE o.spent_by_transaction_internal_id > 0
            AND NOT EXISTS (SELECT 1 FROM block_transaction bt JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
              WHERE bt.transaction_internal_id = o.spent_by_transaction_internal_id)
            AND EXISTS (SELECT 1 FROM input i JOIN block_transaction bt ON bt.transaction_internal_id = i.transaction_internal_id
              JOIN node_block nb ON nb.block_internal_id = bt.block_internal_id
              WHERE i.outpoint_transaction_hash = o.transaction_hash AND i.outpoint_index = o.output_index)`),
          0,
          `${step}: a block-accepted spender is preferred over a mempool-only one`
        );
      }
    };
    await check('e2e chain state');
    await runUnspentTrackingScenario(check);
    /*
     * The fixture now covers every case the read model must follow: an output
     * whose only accepted spender is in a mempool, an output whose spender
     * was dropped from every mempool (unspent again), an output whose spender
     * is only in a stale (removed by a re-org) block (unspent again until re-accepted)
     * and a re-accepted stale block.
     */
    const scenarioOutpoints = (await reference()).filter((outpoint) =>
      outpoint.startsWith(scenarioHash('f0'))
    );
    t.deepEqual(
      scenarioOutpoints.map((outpoint) => outpoint.split(':')[1]),
      ['0', '3']
    );
    t.deepEqual(
      (await reference()).filter((outpoint) =>
        [scenarioHash('b0'), scenarioHash('b1')].some((hash) =>
          outpoint.startsWith(hash)
        )
      ),
      [`${scenarioHash('b0')}:1`]
    );
    const cases = {
      droppedSpender: await count(/* sql */ `
        SELECT count(*) AS n FROM input i JOIN transaction s ON s.internal_id = i.transaction_internal_id
          WHERE NOT ${anyNodeAccepts('s.internal_id')}
            AND EXISTS (SELECT 1 FROM node_transaction_history h WHERE h.transaction_internal_id = s.internal_id)
            AND (encode(i.outpoint_transaction_hash, 'hex') || ':' || i.outpoint_index) IN (${unspentReferenceSql})`),
      mempoolSpend: await count(/* sql */ `
        SELECT count(*) AS n FROM input i JOIN node_transaction nt ON nt.transaction_internal_id = i.transaction_internal_id
          JOIN output o ON o.transaction_hash = i.outpoint_transaction_hash AND o.output_index = i.outpoint_index`),
      reacceptedStaleBlock: await count(/* sql */ `
        SELECT count(*) AS n FROM node_block_history h JOIN node_block nb ON nb.block_internal_id = h.block_internal_id`),
    };
    t.log({
      cases,
      untracked: await count(
        unspentTrackingMode === 'marker'
          ? `SELECT count(*) AS n FROM output WHERE spent_by_transaction_internal_id IS NULL`
          : `SELECT count(*) AS n FROM output o WHERE NOT (${trackedDomain})`
      ),
    });
    t.true(cases.mempoolSpend > 0, 'fixture has a mempool spend');
    t.true(cases.droppedSpender > 0, 'fixture has a dropped spender');
    t.true(cases.reacceptedStaleBlock > 0, 'fixture has a re-org');
  }
);
/**
 * `CHAINGRAPH_UNSPENT_TRACKING`: concurrent parent/child saves. The agent saves
 * up to 16 blocks at once; under READ COMMITTED a child's spend statement
 * cannot see its parent's uncommitted outputs, and the parent's resolve cannot
 * see the child's uncommitted inputs. The post-commit pass must close that
 * race. A chain of blocks (each spending the previous one) and mempool
 * parent/child pairs (child save issued first) are all saved at once, with at
 * least 16 saves in flight; the read model must then equal the F1g reference
 * for every output they created.
 */
const concurrencyHash = (kind: number, group: number, index: number) =>
  `e8${kind.toString(16).padStart(2, '0')}${group
    .toString(16)
    .padStart(4, '0')}${index.toString(16).padStart(4, '0')}${'00'.repeat(26)}`;
const concurrencyTransaction = (
  hash: string,
  spends: [string, number][]
): ChaingraphTransaction => ({
  hash,
  inputs: spends.map(([outpointTransactionHash, outpointIndex]) => ({
    outpointIndex,
    outpointTransactionHash,
    sequenceNumber: 0,
    unlockingBytecode: '51',
  })),
  isCoinbase: false,
  locktime: 0,
  outputs: [
    { lockingBytecode: '51', valueSatoshis: 1000n },
    { lockingBytecode: '52', valueSatoshis: 1000n },
  ],
  sizeBytes: 60,
  version: 2,
});

test.serial(
  `[e2e] unspent tracking (${unspentTrackingMode}): concurrent parent/child saves match the F1g reference`,
  async (t) => {
    if (unspentTrackingMode === 'off') {
      t.pass();
      return;
    }
    const blockCount = 24;
    const transactionsPerBlock = 40;
    const mempoolPairs = 24;
    const minimumInFlight = 16;
    const originalPostgresConnectionString =
      process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
    process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
      postgresE2eConnectionStringTestDb;
    const scenarioDbModule = '../db.js?unspent-tracking-concurrency';
    const db = (await import(scenarioDbModule)) as typeof DbModule;
    /*
     * The pool size comes from CHAINGRAPH_POSTGRES_MAX_CONNECTIONS (default:
     * CPU count); make sure at least 16 saves can be in flight at once.
     */
    (db.pool as unknown as { options: { max: number } }).options.max = Math.max(
      (db.pool as unknown as { options: { max: number } }).options.max,
      minimumInFlight + 8
    );
    const nodeA = Number(
      (
        await client.query<{ id: string }>(
          /* sql */ `SELECT internal_id AS id FROM node WHERE name = 'node1';`
        )
      ).rows[0]!.id
    );
    // block k's transaction j spends output 0 of block k-1's transaction j
    const blocks = Array.from({ length: blockCount }, (_, blockIndex) => ({
      bits: 0,
      hash: concurrencyHash(1, blockIndex, 0xffff),
      height: 999_100 + blockIndex,
      merkleRoot: '00'.repeat(32),
      nonce: 0,
      previousBlockHash: '00'.repeat(32),
      sizeBytes: 0,
      timestamp: 0,
      transactions: Array.from(
        { length: transactionsPerBlock },
        (__, transactionIndex) =>
          concurrencyTransaction(
            concurrencyHash(2, blockIndex, transactionIndex),
            blockIndex === 0
              ? [[concurrencyHash(9, 0, transactionIndex), 0]]
              : [[concurrencyHash(2, blockIndex - 1, transactionIndex), 0]]
          )
      ),
      version: 1,
    }));
    const pairs = Array.from({ length: mempoolPairs }, (_, pairIndex) => {
      const parent = concurrencyTransaction(concurrencyHash(3, pairIndex, 0), [
        [concurrencyHash(9, 1, pairIndex), 0],
      ]);
      const child = concurrencyTransaction(concurrencyHash(3, pairIndex, 1), [
        [parent.hash, 0],
      ]);
      return { child, parent };
    });
    // eslint-disable-next-line functional/no-let
    let inFlight = 0;
    // eslint-disable-next-line functional/no-let
    let maxInFlight = 0;
    const track = async <T>(work: () => Promise<T>) => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      // eslint-disable-next-line functional/no-try-statement
      try {
        return await work();
      } finally {
        inFlight -= 1;
      }
    };
    // eslint-disable-next-line functional/no-try-statement
    try {
      const results = await Promise.all([
        ...blocks.map(async (block) =>
          track(async () =>
            db.saveBlock({
              block,
              nodeAcceptances: [
                {
                  acceptedAt: new Date(),
                  nodeInternalId: nodeA,
                  nodeName: 'concurrency',
                },
              ],
              transactionCache: new Map() as unknown as Parameters<
                typeof db.saveBlock
              >[0]['transactionCache'],
            })
          )
        ),
        ...pairs.flatMap(({ child, parent }) => [
          track(async () =>
            db.saveTransactionForNodes(child, [
              { nodeInternalId: nodeA, validatedAt: new Date() },
            ])
          ),
          track(async () =>
            db.saveTransactionForNodes(parent, [
              { nodeInternalId: nodeA, validatedAt: new Date() },
            ])
          ),
        ]),
      ]);
      const fixed = results.reduce(
        (totals, result) => {
          const postCommit =
            result !== undefined && 'unspentTrackingTimings' in result
              ? result.unspentTrackingTimings.postCommit
              : result;
          return {
            newOutputs: totals.newOutputs + (postCommit?.newOutputsFixed ?? 0),
            spentOutputs:
              totals.spentOutputs + (postCommit?.spentOutputsFixed ?? 0),
          };
        },
        { newOutputs: 0, spentOutputs: 0 }
      );
      t.log({ fixedByPostCommit: fixed, maxInFlight });
      t.true(maxInFlight >= minimumInFlight, `in flight: ${maxInFlight}`);
    } finally {
      await db.pool.end();
      if (originalPostgresConnectionString === undefined) {
        delete process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
      } else {
        process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
          originalPostgresConnectionString;
      }
    }
    await client.query('ANALYZE;');
    const createdTransactions = new Set([
      ...blocks.flatMap((block) =>
        block.transactions.map((transaction) => transaction.hash)
      ),
      ...pairs.flatMap(({ child, parent }) => [child.hash, parent.hash]),
    ]);
    const rows = async (sql: string) =>
      (await client.query<{ outpoint: string }>(sql)).rows
        .map((row) => row.outpoint)
        .filter((outpoint) =>
          createdTransactions.has(outpoint.split(':')[0] ?? '')
        )
        .sort((a, b) => a.localeCompare(b));
    const createdHere = /* sql */ `o.transaction_hash >= '\\xe8'::bytea AND o.transaction_hash < '\\xe9'::bytea`;
    /*
     * unspent: output 1 of every block transaction, output 0 of the last
     * block's, and per mempool pair the parent's output 1 and both child outputs
     */
    const expectedUnspent =
      blockCount * transactionsPerBlock +
      transactionsPerBlock +
      3 * mempoolPairs;
    if (unspentTrackingMode === 'bitmask') {
      const reference = await rows(nodeUnspentReferenceSql(nodeA));
      t.is(reference.length, expectedUnspent);
      t.deepEqual(
        await rows(/* sql */ `SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint FROM output o
          WHERE ${createdHere} AND (o.unspent_node_bits & (1::bigint << ${nodeA})) <> 0`),
        reference
      );
      return;
    }
    const reference = await rows(unspentReferenceSql);
    t.is(reference.length, expectedUnspent);
    t.deepEqual(
      await rows(
        unspentTrackingMode === 'marker'
          ? /* sql */ `SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint FROM output o
            WHERE ${createdHere} AND o.spent_by_transaction_internal_id = 0 AND ${createdByAcceptedSql}`
          : /* sql */ `SELECT encode(o.transaction_hash, 'hex') || ':' || o.output_index AS outpoint
            FROM unspent_output_set u JOIN output o ON o.transaction_hash = u.transaction_hash AND o.output_index = u.output_index
            WHERE ${createdHere} AND ${createdByAcceptedSql}`
      ),
      reference
    );
  }
);

/* eslint-enable @typescript-eslint/no-magic-numbers */

/**
 * The below tests run concurrently after all serial tests have completed.
 */

/**
 * Macro to test Chaingraph's built-in Postgres functions; these can also be run
 * independently via `yarn test:e2e:postgres`.
 */
const bytecodeFunction = test.macro<[string, string, string]>({
  // eslint-disable-next-line max-params
  exec: async (t, functionName, bytecodeHex, patternHex) => {
    const result = await client.query<{ encode: string }>(
      /* sql */ `SELECT encode(${functionName} ($1), 'hex');`,
      [hexToBin(bytecodeHex)]
    );
    t.deepEqual(result.rows[0]!.encode, patternHex);
  },
  // eslint-disable-next-line max-params
  title: (providedTitle, functionName, _bytecodeHex, patternHex) =>
    `[e2e] [postgres] ${functionName} – ${patternHex}: ${providedTitle ?? ''}`,
});

const bytecodeFunctionReturnsNull = test.macro<[string, string]>({
  exec: async (t, functionName, bytecodeHex) => {
    const result = await client.query<{ isNull: boolean }>(
      /* sql */ `SELECT ${functionName} ($1) IS NULL AS "isNull";`,
      [hexToBin(bytecodeHex)]
    );
    t.true(result.rows[0]!.isNull);
  },
  title: (providedTitle, functionName, bytecodeHex) =>
    `[e2e] [postgres] ${functionName} – ${bytecodeHex}: ${providedTitle ?? ''}`,
});

test(
  'P2PKH',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '76a914000000000000000000000000000000000000000088ac',
  '76a91488ac'
);
test(
  'P2SH',
  bytecodeFunction,
  'parse_bytecode_pattern',
  'a914000000000000000000000000000000000000000087',
  'a91487'
);
test(
  'OP_RETURN (fixed pushes)',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '6a04000000005120000000000000000000000000000000000000000000000000000000000000000004000000000400000000',
  '6a0451200404'
);
test(
  'OP_RETURN with OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '6a026d0c090000000000000000004c5c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000',
  '6a02094c'
);

const allOnes = 0x11;
const minPushData2 = 256;
test(
  'OP_RETURN with OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData2).fill(allOnes))
  )}515151`,
  '6a4d515151'
);

const minPushData4 = 65536;
test(
  'OP_RETURN with OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData4).fill(allOnes))
  )}515151`,
  '6a4e515151'
);

test(
  'malformed OP_PUSHBYTES',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '515102',
  '515102'
);
test(
  'malformed OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '51514c',
  '51514c'
);
test(
  'malformed OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '51514d11',
  '51514d'
);
test(
  'malformed OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '51514e112233',
  '51514e'
);

test(
  'P2PKH',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '76a914000000000000000000000000000000000000000088ac',
  '76a91488ac'
);
test(
  'P2SH',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  'a914000000000000000000000000000000000000000087',
  'a91487'
);
test(
  'OP_RETURN (fixed pushes)',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '6a04000000005120000000000000000000000000000000000000000000000000000000000000000004000000000400000000',
  '6a0451200404'
);
test(
  'OP_RETURN with OP_PUSHDATA1 (memo.cash)',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '6a026d0c090000000000000000004c5c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000',
  '6a02094c5c'
);

test(
  'zero-length OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '4c00',
  '4c00'
);
test(
  'zero-length OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '4d0000',
  '4d0000'
);
test(
  'zero-length OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '4e00000000',
  '4e00000000'
);

test(
  'OP_RETURN with OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData2).fill(allOnes))
  )}515151`,
  '6a4d0001515151'
);

test(
  'OP_RETURN with OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData4).fill(allOnes))
  )}515151`,
  '6a4e00000100515151'
);

test(
  'malformed OP_PUSHBYTES',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '515102',
  '515102'
);
test(
  'malformed OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '51514c',
  '51514c'
);
test(
  'malformed OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '51514d11',
  '51514d'
);
test(
  'malformed OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '51514e112233',
  '51514e'
);

test(
  'no redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  `0002000051`
);

test(
  'OP_PUSHBYTES redeem',
  bytecodeFunction,
  'parse_bytecode_pattern_redeem',
  `0003019951`,
  '0151'
);

const minPushData1 = 76;
test(
  'OP_PUSHDATA1 redeem',
  bytecodeFunction,
  'parse_bytecode_pattern_redeem',
  `00020000${binToHex(
    encodeDataPush(
      flattenBinArray([
        hexToBin('00'),
        encodeDataPush(new Uint8Array(minPushData1).fill(allOnes)),
        hexToBin('515253'),
      ])
    )
  )}`,
  '004c515253'
);

test(
  'OP_PUSHDATA2 redeem',
  bytecodeFunction,
  'parse_bytecode_pattern_redeem',
  `00020000${binToHex(
    encodeDataPush(
      flattenBinArray([
        hexToBin('00'),
        encodeDataPush(new Uint8Array(minPushData2).fill(allOnes)),
        hexToBin('515253'),
      ])
    )
  )}`,
  '004d515253'
);

test(
  'OP_PUSHDATA4 redeem',
  bytecodeFunction,
  'parse_bytecode_pattern_redeem',
  `00020000${binToHex(
    encodeDataPush(
      flattenBinArray([
        hexToBin('00'),
        encodeDataPush(new Uint8Array(minPushData4).fill(allOnes)),
        hexToBin('515253'),
      ])
    )
  )}`,
  '004e515253'
);

test(
  'malformed OP_PUSHDATA1 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4c'
);
test(
  'malformed OP_PUSHDATA2 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4d11'
);
test(
  'malformed OP_PUSHDATA4 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4e112233'
);
test(
  'oversized OP_PUSHDATA4 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4effffffff'
);

test('[e2e] [postgres] encode_uint16le', async (t) => {
  const query = async (encoded: number) =>
    (
      await client.query<{ encode: string }>(
        /* sql */ `SELECT encode(encode_uint16le ($1), 'hex');`,
        [encoded]
      )
    ).rows[0]!.encode;
  /* eslint-disable @typescript-eslint/no-magic-numbers */
  t.deepEqual(await query(0), '0000');
  t.deepEqual(await query(1), '0100');
  t.deepEqual(await query(2), '0200');
  t.deepEqual(await query(254), 'fe00');
  t.deepEqual(await query(255), 'ff00');
  t.deepEqual(await query(256), '0001');
  t.deepEqual(await query(1000), 'e803');
  // cspell: disable-next-line
  t.deepEqual(await query(65534), 'feff');
  t.deepEqual(await query(65535), 'ffff');
  /* eslint-enable @typescript-eslint/no-magic-numbers */
});

test('[e2e] [postgres] encode_uint32le', async (t) => {
  const query = async (encoded: number) =>
    (
      await client.query<{ encode: string }>(
        /* sql */ `SELECT encode(encode_uint32le ($1), 'hex');`,
        [encoded]
      )
    ).rows[0]!.encode;
  /* eslint-disable @typescript-eslint/no-magic-numbers */
  t.deepEqual(await query(0), '00000000');
  t.deepEqual(await query(1), '01000000');
  t.deepEqual(await query(536870912), '00000020');
  t.deepEqual(await query(541065216), '00004020');
  t.deepEqual(await query(545259520), '00008020');
  t.deepEqual(await query(549453824), '0000c020');
  t.deepEqual(await query(536928256), '00e00020');
  t.deepEqual(await query(536870913), '01000020');
  t.deepEqual(await query(536870914), '02000020');
  t.deepEqual(await query(1073676288), '0000ff3f');
  t.deepEqual(await query(1073733632), '00e0ff3f');
  t.deepEqual(await query(2147483647), 'ffffff7f');
  t.deepEqual(await query(2147483648), '00000080');
  t.deepEqual(await query(2147483649), '01000080');
  // cspell: disable-next-line
  t.deepEqual(await query(4294967294), 'feffffff');
  t.deepEqual(await query(4294967295), 'ffffffff');
  /* eslint-enable @typescript-eslint/no-magic-numbers */
});

test('[e2e] [postgres] encode_int32le', async (t) => {
  const query = async (encoded: number) =>
    (
      await client.query<{ encode: string }>(
        /* sql */ `SELECT encode(encode_int32le ($1), 'hex');`,
        [encoded]
      )
    ).rows[0]!.encode;
  /* eslint-disable @typescript-eslint/no-magic-numbers, line-comment-position */
  t.deepEqual(await query(1), '01000000');
  t.deepEqual(await query(2), '02000000');
  t.deepEqual(await query(3), '03000000'); // version of TX: 110da331fd5336038316c4709404aea5855afed21f054f5bba01bfef099d5da1
  t.deepEqual(await query(4), '04000000'); // version of TX: 6ae17e22dba03522126f9268de58de5a440ccdb334e137861f90766901e806fd
  t.deepEqual(await query(2147483647), 'ffffff7f');
  // cspell: disable-next-line
  t.deepEqual(await query(-2), 'feffffff');
  t.deepEqual(await query(-1), 'ffffffff');
  t.deepEqual(await query(0), '00000000'); // version of TX: 64147d3d27268778c9d27aa434e8f270f96b2be859658950accde95a2f0ce79d
  t.deepEqual(await query(-2147483648), '00000080');
  t.deepEqual(await query(-2147483647), '01000080');
  t.deepEqual(await query(-2130706433), 'ffffff80'); // version of TX: 35e79ee733fad376e76d16d1f10088273c2f4c2eaba1374a837378a88e530005
  t.deepEqual(await query(-2107285824), 'c05e6582'); // version of TX: 637dd1a3418386a418ceeac7bb58633a904dbf127fa47bbea9cc8f86fef7413f
  t.deepEqual(await query(-1703168784), 'f0b47b9a'); // version of TX: c659729a7fea5071361c2c1a68551ca2bf77679b27086cc415adeeb03852e369
  /* eslint-enable @typescript-eslint/no-magic-numbers, line-comment-position */
});

test('[e2e] [postgres] encode_uint64le', async (t) => {
  const query = async (encoded: bigint | number) =>
    (
      await client.query<{ encode: string }>(
        /* sql */ `SELECT encode(encode_uint64le ($1), 'hex');`,
        [encoded]
      )
    ).rows[0]!.encode;
  /* eslint-disable @typescript-eslint/no-magic-numbers */
  t.deepEqual(await query(0), '0000000000000000');
  t.deepEqual(await query(1), '0100000000000000');
  t.deepEqual(await query(2), '0200000000000000');
  t.deepEqual(await query(254), 'fe00000000000000');
  t.deepEqual(await query(255), 'ff00000000000000');
  t.deepEqual(await query(256), '0001000000000000');
  t.deepEqual(await query(1000), 'e803000000000000');
  t.deepEqual(await query(65534), 'feff000000000000');
  t.deepEqual(await query(65535), 'ffff000000000000');
  t.deepEqual(await query(0xffffffff), 'ffffffff00000000');
  t.deepEqual(await query(BigInt('0xffffffffffff')), 'ffffffffffff0000');
  t.deepEqual(await query(BigInt('9223372036854775807')), 'ffffffffffffff7f');
  /* eslint-enable @typescript-eslint/no-magic-numbers */
});

test('[e2e] [postgres] encode_compact_uint', async (t) => {
  const query = async (encoded: bigint | number) =>
    (
      await client.query<{ encode: string }>(
        /* sql */ `SELECT encode(encode_compact_uint ($1), 'hex');`,
        [encoded]
      )
    ).rows[0]!.encode;
  /* eslint-disable @typescript-eslint/no-magic-numbers */
  t.deepEqual(await query(0), '00');
  t.deepEqual(await query(1), '01');
  t.deepEqual(await query(2), '02');
  t.deepEqual(await query(251), 'fb');
  t.deepEqual(await query(252), 'fc');
  // cspell: disable-next-line
  t.deepEqual(await query(253), 'fdfd00');
  // cspell: disable-next-line
  t.deepEqual(await query(254), 'fdfe00');
  // cspell: disable-next-line
  t.deepEqual(await query(255), 'fdff00');
  t.deepEqual(await query(256), 'fd0001');
  // cspell: disable-next-line
  t.deepEqual(await query(65534), 'fdfeff');
  // cspell: disable-next-line
  t.deepEqual(await query(65535), 'fdffff');
  t.deepEqual(await query(65536), 'fe00000100');
  t.deepEqual(await query(65537), 'fe01000100');
  t.deepEqual(await query(65538), 'fe02000100');
  // cspell: disable-next-line
  t.deepEqual(await query(4294967294), 'fefeffffff');
  // cspell: disable-next-line
  t.deepEqual(await query(4294967295), 'feffffffff');
  t.deepEqual(await query(4294967296), 'ff0000000001000000');
  t.deepEqual(await query(4294967297), 'ff0100000001000000');
  t.deepEqual(await query(4294967298), 'ff0200000001000000');
  t.deepEqual(await query(BigInt('9223372036854775806')), 'fffeffffffffffff7f');
  t.deepEqual(await query(BigInt('9223372036854775807')), 'ffffffffffffffff7f');
  /* eslint-enable @typescript-eslint/no-magic-numbers */
});

/* cspell: disable */
const searchFixtureTxHash =
  'f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0';
const searchFixtureOutputs = {
  /** P2PKH whose hash contains 0x5c (the LIKE escape character) */
  backslash: '76a9144444444444444444445c4444444444444444444488ac',
  /** P2PKH that only a wildcard reading of 0x25 or 0x5f would match */
  other: '76a914997777777777777777777777777777777777777788ac',
  /** 25-byte P2PKH */
  p2pkh: '76a914111111111111111111111111111111111111111188ac',
  /** a longer script sharing the P2PKH's first 25 bytes */
  p2pkhPrefixed: '76a914111111111111111111111111111111111111111188acab',
  /** 35-byte P2SH32 */
  p2sh32:
    'aa20222222222222222222222222222222222222222222222222222222222222222287',
  /** shares the P2SH32's first 25 bytes, differs afterwards */
  p2sh32SamePrefix:
    'aa20222222222222222222222222222222222222222222333333333333333333333387',
  /** P2PKH whose hash starts with 0x25 (LIKE "%") */
  percent: '76a914255555555555555555555555555555555555555588ac',
  /** P2PKH whose hash starts with 0x5f (LIKE "_") */
  underscore: '76a9145f6666666666666666666666666666666666666688ac',
};
/* cspell: enable */

/**
 * Insert synthetic outputs in a database transaction that is always rolled
 * back, run `query` restricted to the fixture transaction, and return the
 * names of the matched fixture outputs.
 */
const searchFixtureMatches = async (query: string, parameter: unknown) => {
  const names = Object.keys(searchFixtureOutputs);
  await client.query('BEGIN;');
  const insertAndQuery = async () => {
    await client.query(
      /* sql */ `INSERT INTO transaction (hash, version, locktime, size_bytes, is_coinbase) VALUES ($1, 2, 0, 0, false);`,
      [hexToBin(searchFixtureTxHash)]
    );
    await Object.values(searchFixtureOutputs).reduce<Promise<unknown>>(
      async (chain, bytecode, index) =>
        chain.then(async () =>
          client.query(
            /* sql */ `INSERT INTO output (transaction_hash, output_index, value_satoshis, locking_bytecode) VALUES ($1, $2, 1000, $3);`,
            [hexToBin(searchFixtureTxHash), index, hexToBin(bytecode)]
          )
        ),
      Promise.resolve(undefined)
    );
    const result = await client.query<{ outputIndex: string }>(
      /* sql */ `SELECT output_index AS "outputIndex" FROM ${query} AS o WHERE o.transaction_hash = $2 ORDER BY output_index;`,
      [parameter, hexToBin(searchFixtureTxHash)]
    );
    return result.rows.map((row) => names[Number(row.outputIndex)]);
  };
  const rollback = async () => client.query('ROLLBACK;');
  return insertAndQuery().then(
    async (matches) => rollback().then(() => matches),
    // eslint-disable-next-line functional/no-promise-reject -- roll back, then propagate the original failure
    async (error: unknown) => rollback().then(async () => Promise.reject(error))
  );
};

test.serial(
  '[e2e] [sql] search_output: exact matches for locking bytecode of any length',
  async (t) => {
    const search = async (scripts: string[]) =>
      searchFixtureMatches('search_output($1::text[])', scripts);
    t.deepEqual(await search([searchFixtureOutputs.p2sh32]), ['p2sh32']);
    t.deepEqual(await search([searchFixtureOutputs.p2pkh]), ['p2pkh']);
    t.deepEqual(await search([searchFixtureOutputs.p2pkhPrefixed]), [
      'p2pkhPrefixed',
    ]);
    t.deepEqual(
      await search([
        searchFixtureOutputs.p2sh32SamePrefix,
        searchFixtureOutputs.p2pkh,
        searchFixtureOutputs.p2pkh,
      ]),
      ['p2pkh', 'p2sh32SamePrefix']
    );
    t.deepEqual(await search([]), []);
  }
);

/* cspell: disable */
test.serial(
  '[e2e] [sql] search_output_prefix: treats every byte literally and accepts prefixes longer than 25 bytes',
  async (t) => {
    const search = async (prefix: string) =>
      searchFixtureMatches('search_output_prefix($1::text)', prefix);
    t.deepEqual(await search(searchFixtureOutputs.backslash), ['backslash']);
    t.deepEqual(await search('76a9144444444444444444445c'), ['backslash']);
    t.deepEqual(await search('76a91425'), ['percent']);
    t.deepEqual(await search('76a9145f'), ['underscore']);
    t.deepEqual(await search(searchFixtureOutputs.p2sh32), ['p2sh32']);
    t.deepEqual(await search(searchFixtureOutputs.p2pkh), [
      'p2pkh',
      'p2pkhPrefixed',
    ]);
    t.deepEqual(await search('aa20'), ['p2sh32', 'p2sh32SamePrefix']);
  }
);
/* cspell: enable */

test.serial(
  '[e2e] [sql] search functions use the 25-byte locking bytecode prefix index',
  async (t) => {
    const prefixIndexes = Object.entries(indexDefinitions).filter(
      ([, definition]) => definition.includes('locking_bytecode')
    );
    t.deepEqual(
      prefixIndexes.map(([name]) => name),
      ['output_search_index']
    );
    prefixIndexes.forEach(([name, definition]) => {
      t.true(definition.includes('substring(locking_bytecode, 0, 26)'), name);
    });
    await client.query('BEGIN;');
    const explainPlans = async () => {
      await prefixIndexes.reduce<Promise<unknown>>(
        async (chain, [name, definition]) =>
          chain.then(async () =>
            client.query(
              definition.replace(
                `CREATE INDEX ${name}`,
                `CREATE INDEX test_${name}`
              )
            )
          ),
        Promise.resolve(undefined)
      );
      // cspell: disable-next-line
      await client.query('SET LOCAL enable_seqscan = off;');
      const plan = async (query: string) =>
        (
          await client.query<{ [column: string]: string }>(
            `EXPLAIN (COSTS OFF) ${query}`
          )
        ).rows
          .map((row) => Object.values(row).join(''))
          .join('\n');
      return [
        // cspell: disable-next-line
        await plan(`SELECT * FROM search_output(ARRAY['76a91411'])`),
        // cspell: disable-next-line
        await plan(`SELECT * FROM search_output_prefix('76a914')`),
      ];
    };
    const rollback = async () => client.query('ROLLBACK;');
    const plans = await explainPlans().then(
      async (result) => rollback().then(() => result),
      async (error: unknown) =>
        // eslint-disable-next-line functional/no-promise-reject -- roll back, then propagate the original failure
        rollback().then(async () => Promise.reject(error))
    );
    /*
     * test_output_search_index duplicates output_search_index, so either may
     * win the cost tie depending on the data the earlier tests left behind;
     * both are the 25-byte prefix index.
     */
    plans.forEach((plan) => {
      t.true(plan.includes('output_search_index'), plan);
      t.false(plan.includes('Seq Scan'), plan);
    });
  }
);
