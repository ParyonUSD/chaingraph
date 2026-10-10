/* eslint-disable max-lines */
// cspell:ignore clickhouse
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
import type { ExecutionContext } from 'ava';
import test from 'ava';
import type { ExecaChildProcess } from 'execa';
import { execa } from 'execa';
import got from 'got';
import pg from 'pg';

import { indexDefinitions } from '../components/db-utils.js';
import { createChecker } from '../store/checker-factory.js';
import type { StoreChecker } from '../store/checker.js';
import { ClickHouseClient } from '../store/clickhouse/client.js';
import {
  applyClickHouseDdl,
  dropClickHouseDatabase,
} from '../store/clickhouse/ddl-apply.js';
import { eventually, eventuallyEqual, readTwice } from '../store/eventually.js';
import type { ChaingraphTransaction } from '../types/chaingraph.js';

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
import {
  dropStaleClickHouseE2eDatabases,
  e2eClickHouseDatabase,
  e2eClickHouseServer,
  e2eClickHouseUtxo,
  e2eStore,
  e2eStoreEnvironment,
  isClickHouseE2e,
  postgresTest,
} from './e2e.spec.store.helper.js';

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
  ...e2eStoreEnvironment(),
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

/**
 * Postgres only (`[postgres]` tests and the Postgres checker); not connected
 * when CHAINGRAPH_E2E_STORE=clickhouse.
 */
// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let client: pg.Client;
/**
 * ClickHouse only: the checker's client on `cg_e2e_<pid>`.
 */
// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let clickHouseClient: ClickHouseClient | undefined;
/**
 * Backend-neutral reads. On Postgres it is built on `client`, so it also sees
 * rows of a fixture transaction the test has open.
 */
// eslint-disable-next-line functional/no-let, @typescript-eslint/init-declarations
let checker: StoreChecker;

/**
 * ClickHouse: create `cg_e2e_<pid>` fresh and apply the DDL.
 */
const setUpClickHouse = async () => {
  const stale = await dropStaleClickHouseE2eDatabases();
  if (stale.length > 0) {
    logger.info(`Dropped stale ClickHouse databases: ${stale.join(', ')}`);
  }
  const statements = await applyClickHouseDdl(
    e2eClickHouseServer,
    e2eClickHouseDatabase,
    { recreate: true }
  );
  logger.info(
    `Created ClickHouse database ${e2eClickHouseDatabase} (${statements} DDL statements)`
  );
  clickHouseClient = new ClickHouseClient({
    database: e2eClickHouseDatabase,
    password: e2eClickHouseServer.password ?? '',
    requestTimeoutMs: 60_000,
    url: e2eClickHouseServer.url,
    username: e2eClickHouseServer.username ?? '',
  });
  checker = createChecker({
    backend: 'clickhouse',
    client: clickHouseClient,
    utxo: e2eClickHouseUtxo(),
  });
};

/**
 * Postgres: drop and recreate the e2e test database, then apply migrations.
 */
const setUpPostgres = async () => {
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
  checker = createChecker({ backend: 'postgres', db: client });
  if (recreateDbOnStartup) {
    await dbUpMigrationPaths.reduce<Promise<pg.QueryResult | undefined>>(
      async (chain, path) => {
        const dbUpMigration = readFileSync(path, 'utf8');
        return chain.then(async () => client.query(dbUpMigration));
      },
      Promise.resolve(undefined)
    );
  }
};

/**
 * Before connecting to the e2e test database, drop and recreate it:
 */
test.before(async () => {
  logger.info(`E2E store backend: ${e2eStore}`);
  await (isClickHouseE2e ? setUpClickHouse() : setUpPostgres());

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

/**
 * ClickHouse: stop the agent (it holds the writer lease on the run's
 * database), then drop `cg_e2e_<pid>`.
 */
test.after.always(async () => {
  if (!isClickHouseE2e) {
    return;
  }
  [chaingraphProcess, chaingraphProcess2, chaingraphProcess3].forEach(
    (agentProcess) => {
      if (agentProcess !== undefined && agentProcess.exitCode === null) {
        agentProcess.kill('SIGKILL');
      }
    }
  );
  await clickHouseClient?.close();
  await dropClickHouseDatabase(e2eClickHouseServer, e2eClickHouseDatabase);
  logger.info(`Dropped ClickHouse database ${e2eClickHouseDatabase}`);
});

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
/**
 * Waits on agent events scale with the backend. Postgres keeps the original
 * 10 s / 60 s; ClickHouse saves each small block with about 11 inserts, so its
 * initial sync of the 3,001-block mockchain takes 20–30 s (not 2 s) and it
 * shares the machine with other agents' gates.
 *
 * - `stdoutTimeoutMs`: one log line after one action (a block, a request).
 * - `batchTimeoutMs`: a log line after tens of blocks (re-org feeds,
 *   one-by-one sync, a shutdown that drains in-flight saves).
 * - `syncTimeoutMs`: initial sync / catch-up of the whole mockchain (cap).
 * - `visibleTimeoutMs`: a store read after a log line or event (the line can
 *   precede visibility, e.g. batched mempool commits).
 * - `syncVisibleTimeoutMs`: a store read that waits for sync-scale work
 *   (catch-up of a new node, the incomplete-block repair scan).
 * - `clickHouseTestTimeoutMs`: per-test AVA timeout on ClickHouse (overrides
 *   the CLI `--timeout`, which is an inactivity timeout shorter than a sync).
 */
/* eslint-disable @typescript-eslint/no-magic-numbers */
const stdoutTimeoutMs = isClickHouseE2e ? 30_000 : 10_000;
const batchTimeoutMs = isClickHouseE2e ? 60_000 : 10_000;
const syncTimeoutMs = isClickHouseE2e ? 120_000 : 60_000;
const visibleTimeoutMs = isClickHouseE2e ? 10_000 : 3_000;
const syncVisibleTimeoutMs = isClickHouseE2e ? 60_000 : 10_000;
const clickHouseTestTimeoutMs = 180_000;
/* eslint-enable @typescript-eslint/no-magic-numbers */
/**
 * Returns a promise that resolves when the `search` string is found in stdout
 * (the buffer since the last `clearStdoutBuffer`), or rejects after `timeout`
 * (a test failure, not an uncaught exception, so the rest of the run and
 * `test.after.always` still run).
 * @param search - the string to search for in stdout
 *
 * TODO: if AVA is running in debug mode, disable timeout (https://github.com/avajs/ava/issues/3152)
 */
const waitForStdout = async (
  search: RegExp | string,
  timeout = stdoutTimeoutMs
) => {
  logger.debug(`Waiting for stdout: ${search.toString()}`);
  const promise = new Promise<void>((res, reject) => {
    const timer: { id?: ReturnType<typeof setTimeout> } = {};
    const task = {
      pattern: search,
      resolver: () => {
        logger.debug(`Heard stdout: ${search.toString()}`);
        clearTimeout(timer.id);
        res();
      },
    };
    timer.id = setTimeout(() => {
      waitingForStdout = waitingForStdout.filter((other) => other !== task);
      reject(
        new Error(
          `Test failed after waiting ${
            timeout / seconds
          }s for the stdout search: ${search.toString()}`
        )
      );
    }, timeout);
    waitingForStdout.push(task);
  });
  handleStdout();
  return promise;
};

const clearStdoutBuffer = () => {
  stdoutBuffer = '';
};

/**
 * A healthy run prints a few MB between clears. If an agent loops on an error
 * the buffer would grow past V8's string limit and crash the worker (an
 * uncaught `RangeError: Invalid string length` that also skips
 * `test.after.always`), so keep only the newest `maxStdoutBufferLength`
 * characters; waits still fail on their own timeout.
 */
/* eslint-disable @typescript-eslint/no-magic-numbers */
const maxStdoutBufferLength = 64 * 1024 * 1024;
const keptStdoutLength = maxStdoutBufferLength / 2;
/* eslint-enable @typescript-eslint/no-magic-numbers */
const appendStdout = (chunk: unknown) => {
  stdoutBuffer += String(chunk);
  if (stdoutBuffer.length > maxStdoutBufferLength) {
    logger.warn('e2e: stdout buffer over 64 MiB; keeping the newest half.');
    stdoutBuffer = stdoutBuffer.slice(-keptStdoutLength);
  }
  handleStdout();
};

/**
 * `test.serial` for tests that drive the agent: on ClickHouse the test gets
 * `clickHouseTestTimeoutMs` (its waits are longer than the CLI timeout);
 * on Postgres the CLI `--timeout` applies, as before.
 */
const serialTest = (
  title: string,
  implementation: (t: ExecutionContext) => Promise<void> | void
) => {
  test.serial(title, async (t) => {
    if (isClickHouseE2e) {
      t.timeout(clickHouseTestTimeoutMs);
    }
    await implementation(t);
  });
};

/**
 * Wait (at most `timeout`) for an agent sent SIGINT to exit; resolves its exit
 * code (`-1` on timeout, after SIGKILL). A graceful shutdown drains the block
 * buffer, closes the store, logs "Exiting..." and exits 0; a forced or failed
 * shutdown exits 1. The exit code is asserted rather than the "Exiting..."
 * line: the agent logs through a pino transport worker, and its last stdout
 * lines can be lost when the process exits (seen on macOS, ClickHouse run,
 * HEAD 66a1620: "Exiting..." in the log file, never on stdout). Once the
 * process has exited, stdout is complete, so the line is checked and a
 * missing one is logged.
 */
const waitForGracefulExit = async (
  agentProcess: ExecaChildProcess,
  timeout: number
) => {
  const exitCode = await new Promise<number>((res) => {
    const timer = setTimeout(() => {
      agentProcess.kill('SIGKILL');
      res(-1);
    }, timeout);
    agentProcess
      .then(
        (result) => result.exitCode,
        (error: { exitCode?: number }) => error.exitCode ?? -1
      )
      .then((code) => {
        clearTimeout(timer);
        res(code);
      })
      .catch(() => {
        res(-1);
      });
  });
  if (!stdoutBuffer.includes('Exiting...')) {
    logger.warn(
      `e2e: agent exited with code ${exitCode} but "Exiting..." never reached stdout.`
    );
  }
  return exitCode;
};

serialTest('[e2e] spawn chaingraph', async (t) => {
  chaingraphProcess = execa('node', ['./bin/chaingraph.js'], {
    env: e2eEnvVariables,
    stdio: 'pipe',
  });
  if (chaingraphProcess.stdout === null) {
    t.fail('`chaingraphProcess` stdout is not available.');
    return;
  }
  chaingraphProcess.stdout.on('data', (chunk) => {
    appendStdout(chunk);
  });
  await waitForStdout('Starting Chaingraph...');
  t.pass();
});

const enum StatusCode {
  success = 200,
  badRequest = 400,
  notFound = 404,
}

serialTest('[e2e] api /health-check is alive', async (t) => {
  const healthCheckResponse = await got(
    `http://localhost:${chaingraphInternalApiPort}/health-check`
  );
  t.deepEqual(healthCheckResponse.statusCode, StatusCode.success);
  t.deepEqual(healthCheckResponse.body, '{"status":"alive"}');
});

serialTest('[e2e] connects to trusted nodes', async (t) => {
  await waitForStdout('node1: connected to node');
  await waitForStdout('node2: connected to node');
  await waitForStdout('node3: connected to node');
  t.pass();
});

serialTest('[e2e] downloads all header chains', async (t) => {
  await waitForStdout(/node1[^\n]+headers-syncing completed/u);
  await waitForStdout(/node2[^\n]+headers-syncing completed/u);
  await waitForStdout(/node3[^\n]+headers-syncing completed/u);
  t.pass();
});

serialTest(
  '[e2e] restores sync-state from database on restart (during initial sync)',
  async (t) => {
    await waitForStdout(
      /Saved new block – height:\s+10[^\n]+nodes: node1, node2, node3/u
    );
    logger.info('e2e: testing sync restoration on restart. Sending SIGTERM...');
    chaingraphProcess!.kill('SIGINT');
    // chaingraphProcess!.kill('SIGTERM');
    await waitForStdout('Shutting down...');
    /*
     * Shutdown waits for in-flight block saves (on ClickHouse, thousands of
     * initial-sync saves can be in flight).
     */
    t.deepEqual(
      await waitForGracefulExit(chaingraphProcess!, batchTimeoutMs),
      0
    );
    /*
     * From here on, lines must come from the restarted agent (the first one
     * also printed "Restored chain for node …").
     */
    clearStdoutBuffer();
    chaingraphProcess2 = execa('node', ['./bin/chaingraph.js'], {
      env: e2eEnvVariables,
      stdio: 'pipe',
    });
    if (chaingraphProcess2.stdout === null) {
      t.fail('`chaingraphProcess2` stdout is not available.');
      return;
    }
    chaingraphProcess2.stdout.on('data', (chunk) => {
      appendStdout(chunk);
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

/**
 * The incomplete-block repair scan runs after the restarted agent's catch-up.
 */
const blockRepairTimeoutMs = syncVisibleTimeoutMs;
const getBlockTransactionCount = async (blockHash: string) =>
  checker.blockTransactionCount(blockHash);
const waitForBlockTransactionCount = async (
  blockHash: string,
  expectedCount: number
): Promise<number> =>
  eventually(async () => getBlockTransactionCount(blockHash), {
    isDone: (count) => count === expectedCount,
    timeoutMs: blockRepairTimeoutMs,
  });

const mempoolArchiveTimeoutMs = 5_000;
/**
 * For each named transaction: is it in the node's mempool, how many history
 * rows does the node have for it, and the earliest `replaced_at` (ISO string).
 * Sorted by name.
 */
const getMempoolArchiveState = async (
  node: string,
  namedHashes: { [transactionName: string]: string }
) => {
  const hashes = Object.values(namedHashes);
  const membership = await checker.mempoolMembership(node, hashes);
  const history = await checker.transactionHistory(node, hashes);
  return Object.entries(namedHashes)
    .sort(([a], [b]) => (a < b ? -1 : Number(a > b)))
    .map(([transactionName, hash]) => {
      const rows = history.filter((row) => row.hash === hash);
      const [replacedAt] = rows
        .map((row) => row.replacedAt)
        .filter((date): date is Date => date !== null)
        .sort((a, b) => a.getTime() - b.getTime());
      return {
        historyRowCount: rows.length,
        inMempool: membership.has(hash),
        replacedAt: replacedAt === undefined ? null : replacedAt.toISOString(),
        transactionName,
      };
    });
};

const expiryTransactions = {
  /* eslint-disable @typescript-eslint/naming-convention, camelcase */
  expiry_child_b: 'd2'.repeat(repeatedHashByteLength),
  expiry_child_c: 'd3'.repeat(repeatedHashByteLength),
  expiry_parent_a: 'd1'.repeat(repeatedHashByteLength),
  /* eslint-enable @typescript-eslint/naming-convention, camelcase */
};
const expectedExpiryReplacedAt = '2026-01-15T00:00:00.000Z';
const waitForExpiredMempoolArchive = async () =>
  eventually(async () => getMempoolArchiveState('node1', expiryTransactions), {
    isDone: (rows) =>
      rows.every(
        (row) =>
          !row.inMempool &&
          row.historyRowCount === 1 &&
          row.replacedAt === expectedExpiryReplacedAt
      ),
    timeoutMs: mempoolArchiveTimeoutMs,
  });

const confirmedChildHash = 'd5'.repeat(repeatedHashByteLength);
const waitForConfirmedMempoolArchive = async () =>
  eventually(
    async () => {
      const [row] = await getMempoolArchiveState('node1', {
        confirmedChild: confirmedChildHash,
      });
      const { historyRowCount, inMempool, replacedAt } = row!;
      return { historyRowCount, inMempool, replacedAt };
    },
    {
      isDone: (row) =>
        !row.inMempool && row.historyRowCount === 1 && row.replacedAt === null,
      timeoutMs: mempoolArchiveTimeoutMs,
    }
  );

serialTest(
  '[e2e] ignores inbound transactions before initial sync is complete',
  async (t) => {
    peers.node1.sendMessage(
      new peers.node1.messages.Transaction(new Transaction(halTxRaw))
    );
    /*
     * Negative check: the transaction must still be absent after a bounded
     * wait, and stay absent across two reads 1 s apart.
     */
    const gapMs = 1000;
    t.deepEqual(
      await readTwice(async () => checker.transactionExists(halTxHash), gapMs),
      [false, false]
    );
  }
);

const oneMinute = 60_000;
serialTest('[e2e] completes initial sync', async (t) => {
  t.timeout(isClickHouseE2e ? clickHouseTestTimeoutMs : oneMinute);
  await waitForStdout(
    /Saved new block – height:\s+3000[^\n]+nodes: node1, node2, node3/u,
    syncTimeoutMs
  );
  /*
   * Blocks are saved concurrently, so height 3000 can be logged before lower
   * heights; the agent reports completion once every block is saved.
   */
  await waitForStdout('Agent: initial sync is complete.', syncTimeoutMs);
  if (isClickHouseE2e) {
    /*
     * On Postgres the `[postgres]` index test below waits for this and then
     * clears the stdout buffer; on ClickHouse (where it is skipped) the
     * bulk-horizon UTXO build runs before mempool tracking starts, and the
     * next tests announce transactions that are ignored until it has.
     */
    await waitForStdout('Agent: enabled mempool tracking.', syncTimeoutMs);
    clearStdoutBuffer();
  }
  t.pass();
});

postgresTest.serial(
  '[e2e] [postgres] creates expected indexes after initial sync',
  async (t) => {
    await waitForStdout('Agent: all managed indexes have been created.');
    await waitForStdout('Agent: enabled mempool tracking.');
    const indexes = (
      await client.query<{
        indexname: string;
      }>(/* sql */ `
  SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname;
  `)
    ).rows.map((row) => row.indexname);
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
      'spent_by_index',
      'token_category_index',
      'transaction_hash_key',
      'transaction_pkey',
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
  }
);

/**
 * The store's own `getAllKnownBlockHashes` (not the checker): Postgres via the
 * `../db.js` shim, ClickHouse via a read-only `createStore` instance on the
 * run's database (never `init()`ed, so it never takes the writer lease).
 */
const storeKnownBlockHashes = async () => {
  if (!isClickHouseE2e) {
    const { getAllKnownBlockHashes } = await import('../db.js');
    return getAllKnownBlockHashes();
  }
  const { createStore } = await import('../store/index.js');
  const store = createStore({
    backend: 'clickhouse',
    clickhouse: {
      database: e2eClickHouseDatabase,
      password: e2eClickHouseServer.password ?? '',
      url: e2eClickHouseServer.url,
      user: e2eClickHouseServer.username ?? '',
    },
  });
  // eslint-disable-next-line functional/no-try-statement
  try {
    return await store.getAllKnownBlockHashes();
  } finally {
    await store.close();
  }
};

serialTest(
  '[e2e] getAllKnownBlockHashes returns hex hashes for every known block',
  async (t) => {
    const originalPostgresConnectionString =
      process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING;
    process.env.CHAINGRAPH_POSTGRES_CONNECTION_STRING =
      postgresE2eConnectionStringTestDb;
    // eslint-disable-next-line functional/no-try-statement
    try {
      /*
       * Convert client-side (the previous implementation) to verify the
       * SQL-side `encode(...)` used by `getAllKnownBlockHashes` matches it.
       * Both reads are taken together until they agree (or the timeout).
       */
      const { expected, hashes } = await eventually(
        async () => ({
          expected: await checker.allBlockHashes(),
          hashes: await storeKnownBlockHashes(),
        }),
        {
          isDone: (reads) =>
            reads.expected.length > 0 &&
            reads.expected.join() ===
              [...reads.hashes]
                .sort((a, b) => (a < b ? -1 : Number(a > b)))
                .join(),
          timeoutMs: visibleTimeoutMs,
        }
      );
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

postgresTest.serial(
  '[e2e] [postgres] records node validation after concurrent transaction insert conflict',
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
    const nodeInternalId = (await checker.nodeInternalId('node1'))!;
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
      const savedValidationCount = (await checker.mempool('node1')).filter(
        (entry) =>
          entry.hash === transactionHash &&
          entry.validatedAt?.getTime() === validatedAt.getTime()
      ).length;
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

postgresTest.serial(
  '[e2e] [postgres] cascades replaced mempool transaction history to same-node descendants',
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
      const cascadeChildren = {
        /* eslint-disable @typescript-eslint/naming-convention, camelcase */
        child_b: 'f3'.repeat(repeatedHashByteLength),
        child_c: 'f4'.repeat(repeatedHashByteLength),
        /* eslint-enable @typescript-eslint/naming-convention, camelcase */
      };
      const cascadeReplacedAt = '2026-01-01T00:10:00.000Z';
      t.deepEqual(await getMempoolArchiveState('node1', cascadeChildren), [
        {
          historyRowCount: 1,
          inMempool: false,
          replacedAt: cascadeReplacedAt,
          transactionName: 'child_b',
        },
        {
          historyRowCount: 1,
          inMempool: false,
          replacedAt: cascadeReplacedAt,
          transactionName: 'child_c',
        },
      ]);
      t.deepEqual(await getMempoolArchiveState('node2', cascadeChildren), [
        {
          historyRowCount: 0,
          inMempool: true,
          replacedAt: null,
          transactionName: 'child_b',
        },
        {
          historyRowCount: 0,
          inMempool: true,
          replacedAt: null,
          transactionName: 'child_c',
        },
      ]);
    } finally {
      await client.query(/* sql */ `ROLLBACK;`);
    }
  }
);

postgresTest.serial(
  '[e2e] [postgres] archives expired mempool transactions and descendants',
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
          replacedAt: expectedExpiryReplacedAt,
          transactionName: 'expiry_child_b',
        },
        {
          historyRowCount: 1,
          inMempool: false,
          replacedAt: expectedExpiryReplacedAt,
          transactionName: 'expiry_child_c',
        },
        {
          historyRowCount: 1,
          inMempool: false,
          replacedAt: expectedExpiryReplacedAt,
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

postgresTest.serial(
  '[e2e] [postgres] archives stale mempool transactions already accepted by blocks',
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

postgresTest.serial(
  '[e2e] [postgres] backfills existing orphan mempool descendants with idempotence',
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

      const backfillReplacedAt = '2026-01-01T00:10:00.000Z';
      t.deepEqual(
        await getMempoolArchiveState('node1', {
          /* eslint-disable @typescript-eslint/naming-convention, camelcase */
          backfill_child_b: 'e2'.repeat(repeatedHashByteLength),
          backfill_child_c: 'e3'.repeat(repeatedHashByteLength),
          backfill_parent_a: 'e1'.repeat(repeatedHashByteLength),
          /* eslint-enable @typescript-eslint/naming-convention, camelcase */
        }),
        [
          {
            historyRowCount: 1,
            inMempool: false,
            replacedAt: backfillReplacedAt,
            transactionName: 'backfill_child_b',
          },
          {
            historyRowCount: 1,
            inMempool: false,
            replacedAt: backfillReplacedAt,
            transactionName: 'backfill_child_c',
          },
          {
            historyRowCount: 1,
            inMempool: false,
            replacedAt: backfillReplacedAt,
            transactionName: 'backfill_parent_a',
          },
        ]
      );
    } finally {
      await client.query(/* sql */ `ROLLBACK;`);
    }
  }
);

serialTest(
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

serialTest(
  '[e2e] after initial sync is complete, saves inbound transactions as they are received',
  async (t) => {
    peers.node1.sendMessage(
      new peers.node1.messages.Transaction(new Transaction(halTxRaw))
    );
    t.deepEqual(
      await eventuallyEqual(
        async () => checker.encodedTransactionHex(halTxHash),
        halTxRaw,
        { timeoutMs: visibleTimeoutMs }
      ),
      halTxRaw
    );
    t.deepEqual(
      await eventuallyEqual(
        async () => checker.validatingNodes(halTxHash),
        ['node1'],
        { timeoutMs: visibleTimeoutMs }
      ),
      ['node1']
    );
    t.pass();
  }
);

serialTest(
  '[e2e] records validation when another node announces a known transaction',
  async (t) => {
    peers.node2.sendMessage(
      new peers.node2.messages.Transaction(new Transaction(halTxRaw))
    );
    const expectedNodes = ['node1', 'node2'];
    t.deepEqual(
      await eventuallyEqual(
        async () => checker.validatingNodes(halTxHash),
        expectedNodes,
        { timeoutMs: visibleTimeoutMs }
      ),
      expectedNodes
    );
    t.deepEqual(await checker.transactionRowCount(halTxHash), 1);
    await checker.forgetNodeValidation('node2', halTxHash);
    t.deepEqual(
      await eventuallyEqual(
        async () => checker.validatingNodes(halTxHash),
        ['node1'],
        { timeoutMs: visibleTimeoutMs }
      ),
      ['node1']
    );
  }
);

serialTest('[e2e] handles first chipnet CashTokens transaction', async (t) => {
  peers.node1.sendMessage(
    new peers.node1.messages.Transaction(new Transaction(chipnetCashTokensTx))
  );
  t.deepEqual(
    await eventuallyEqual(
      async () => checker.encodedTransactionHex(chipnetCashTokensTxHash),
      chipnetCashTokensTx,
      { timeoutMs: visibleTimeoutMs }
    ),
    chipnetCashTokensTx
  );
  t.pass();
});

serialTest(
  '[e2e] after initial sync is complete, requests and saves inbound transactions as they are announced',
  async (t) => {
    peers.node1.sendMessage(
      peers.node1.messages.Inventory.forTransaction(
        Buffer.from(halTxSpent, 'hex')
      )
    );
    t.deepEqual(
      await eventuallyEqual(
        async () => checker.encodedTransactionHex(halTxSpent),
        halTxSpentRaw,
        { timeoutMs: visibleTimeoutMs }
      ),
      halTxSpentRaw
    );
    t.pass();
  }
);

serialTest('[e2e] get hex-encoded genesis block header', async (t) => {
  const expected =
    '0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c';
  t.deepEqual(
    await eventuallyEqual(
      async () => checker.encodedBlockHeaderHex({ height: 0 }),
      expected,
      { timeoutMs: visibleTimeoutMs }
    ),
    expected
  );
});

serialTest('[e2e] get hex-encoded genesis block transaction', async (t) => {
  t.deepEqual(
    await checker.encodedTransactionHex(
      '4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b'
    ),
    '01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000'
  );
});

serialTest(
  '[e2e] get hex-encoded genesis block (with transaction)',
  async (t) => {
    t.deepEqual(await checker.encodedBlockHex({ height: 0 }), genesisBlockRaw);
  }
);

serialTest('[e2e] value aggregates handle coinbase-only blocks', async (t) => {
  t.deepEqual(await checker.blockValueAggregates({ height: 0 }), {
    fee: 0n,
    generated: 5000000000n,
    input: 0n,
    output: 5000000000n,
  });
});

serialTest(
  '[e2e] get hex-encoded block with multiple transactions',
  async (t) => {
    const blockWithMultipleTransactions = mockchainBeforeFork[1]!;
    t.true(blockWithMultipleTransactions.transactions.length > 1);
    t.deepEqual(
      await checker.encodedBlockHex({
        hash: blockWithMultipleTransactions.header.hash,
      }),
      binToHex(blockWithMultipleTransactions.toBuffer())
    );
  }
);

postgresTest.serial(
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

serialTest(
  '[e2e] syncs blocks as they arrive, handles multiple chain tips',
  async (t) => {
    const [, tx1] = tipA[0]!.transactions;
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx1));
    logger.debug(`node1: sent tipA[0] transaction 0: ${tx1!.hash}`);
    /*
     * The block must find tx1 already saved ("new txs: 3/4" below), so wait
     * for it to reach node1's mempool before announcing the block.
     */
    const tx1InMempool = await eventually(
      async () => checker.mempoolMembership('node1', [tx1!.hash]),
      {
        isDone: (members) => members.has(tx1!.hash),
        timeoutMs: visibleTimeoutMs,
      }
    );
    t.true(tx1InMempool.has(tx1!.hash));
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

serialTest('[e2e] handles re-org of a single block', async (t) => {
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

serialTest('[e2e] new block saved after reorg', async (t) => {
  const expected = [{ hash: tipA[0]!.header.hash, height: splitHeight + 1 }];
  const acceptedBlocks = await eventuallyEqual(
    async () =>
      (
        await checker.acceptedBlocks('node3', {
          height: splitHeight + 1,
        })
      ).map(({ hash, height }) => ({ hash, height })),
    expected,
    { timeoutMs: visibleTimeoutMs }
  );
  t.deepEqual(acceptedBlocks, expected);
});

serialTest('[e2e] handles reversal of single-block re-org', async (t) => {
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

serialTest('[e2e] handles re-org of 6 blocks', async (t) => {
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

serialTest('[e2e] handles reversal of 6 block re-org', async (t) => {
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

serialTest('[e2e] handles re-org of 100 blocks', async (t) => {
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
    /node3: re-organization detected beginning at height: 3001. The following stale blocks were removed:/u,
    batchTimeoutMs
  );
  await waitForStdout(
    /Saved new block – height:\s+3100[^\n]+nodes: node2/u,
    batchTimeoutMs
  );
  await waitForStdout(
    /Saved new block – height:\s+3100[^\n]+nodes: node1, node3/u,
    batchTimeoutMs
  );
  t.pass();
});

serialTest('[e2e] records stale blocks', async (t) => {
  const tipStartIndex = 101;
  const tipEnd1 = 150;
  const tipEnd2 = 160;
  slowFeedBlocks('node1', tipA.slice(tipStartIndex, tipEnd1));
  slowFeedBlocks('node2', tipB.slice(tipStartIndex, tipEnd1));
  slowFeedBlocks('node3', tipA.slice(tipStartIndex, tipEnd1));
  newBlocks('node3', tipAStale150);
  await waitForStdout(
    /Saved new block – height:\s+3153[^\n]+nodes: node3/u,
    batchTimeoutMs
  );
  chainStates.node3.splice(splitHeight + tipEnd1 + 1);
  slowFeedBlocks('node1', tipA.slice(tipEnd1, tipEnd2));
  slowFeedBlocks('node2', tipB.slice(tipEnd1, tipEnd2));
  slowFeedBlocks('node3', tipA.slice(tipEnd1, tipEnd2));
  t.deepEqual(
    chainStates.node1.map((block) => block.header.hash),
    chainStates.node3.map((block) => block.header.hash)
  );
  await waitForStdout(
    /node3: re-organization detected beginning at height: 3151. The following stale blocks were removed:/u,
    batchTimeoutMs
  );
  await waitForStdout(
    /Saved new block – height:\s+3160[^\n]+nodes: node2/u,
    batchTimeoutMs
  );
  await waitForStdout(
    /Saved new block – height:\s+3160[^\n]+nodes: node1, node3/u,
    batchTimeoutMs
  );
  t.pass();
});
/* eslint-enable @typescript-eslint/no-magic-numbers */

// eslint-disable-next-line @typescript-eslint/no-magic-numbers
const doubleSpendSaveTimeoutMs = Math.max(visibleTimeoutMs, 5_000);
serialTest(
  '[e2e] records double-spends accepted via mempool and via block',
  async (t) => {
    // eslint-disable-next-line prefer-destructuring
    const tx1 = tipA[160]!.transactions[1];
    const mock1 = generateMockDoubleSpend(tx1!.inputs, true);
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx1));
    logger.debug(
      `node1: sent original transaction to double-spend: ${tx1!.hash}`
    );
    /*
     * Wait (instead of a fixed 1 s sleep) until each transaction is in
     * node1's mempool before sending the next message.
     */
    const inNode1Mempool = async (hash: string) =>
      eventually(async () => checker.mempoolMembership('node1', [hash]), {
        isDone: (members) => members.has(hash),
        timeoutMs: doubleSpendSaveTimeoutMs,
      });
    await inNode1Mempool(tx1!.hash);
    peers.node1.sendMessage(new peers.node1.messages.Transaction(mock1));
    logger.debug(`node1: sent double-spending transaction: ${mock1.hash}`);
    await inNode1Mempool(mock1.hash);
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
    const doubleSpendHashes = [tx1!.hash, mock1.hash];
    const history = await eventually(
      async () => checker.transactionHistory('node1', doubleSpendHashes),
      {
        isDone: (rows) => rows.length === doubleSpendHashes.length,
        timeoutMs: visibleTimeoutMs,
      }
    );
    t.deepEqual(
      history.map((row) => row.hash),
      doubleSpendHashes
    );
    const [replaced, doubleSpend] = history;
    t.deepEqual(replaced!.replacedAt, doubleSpend!.validatedAt);
    t.true(replaced!.validatedAt! <= replaced!.replacedAt!);
    t.true(doubleSpend!.validatedAt! <= doubleSpend!.replacedAt!);
    t.deepEqual(
      await checker.transactionHistory('node2', doubleSpendHashes),
      []
    );
    t.deepEqual(
      await checker.transactionHistory('node3', doubleSpendHashes),
      []
    );
    t.pass();
  }
);

const allNodesBeforeRestart = ['node1', 'node2', 'node3'];
const mempoolHashes = async (node: string) =>
  (await checker.mempool(node)).map((entry) => entry.hash);

serialTest(
  '[e2e] removes node_transaction entries which are confirmed by a block',
  async (t) => {
    const [, tx1, tx2, tx3] = tipA[161]!.transactions;
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx1));
    logger.debug(`node1: sent tx1: ${tx1!.hash}`);
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx2));
    logger.debug(`node1: sent tx2: ${tx2!.hash}`);
    peers.node1.sendMessage(new peers.node1.messages.Transaction(tx3));
    logger.debug(`node1: sent tx3: ${tx2!.hash}`);
    const expectedMempool1 = [
      tx1!.hash,
      tx3!.hash,
      tx2!.hash,
      chipnetCashTokensTxHash,
      halTxSpent,
      halTxHash,
    ];
    t.deepEqual(
      await eventuallyEqual(
        async () => mempoolHashes('node1'),
        expectedMempool1,
        { timeoutMs: visibleTimeoutMs }
      ),
      expectedMempool1
    );
    t.deepEqual(await mempoolHashes('node2'), []);
    t.deepEqual(await mempoolHashes('node3'), []);
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
    const expectedMempool2 = [chipnetCashTokensTxHash, halTxSpent, halTxHash];
    t.deepEqual(
      await eventuallyEqual(
        async () => mempoolHashes('node1'),
        expectedMempool2,
        { timeoutMs: visibleTimeoutMs }
      ),
      expectedMempool2
    );
    t.deepEqual(await mempoolHashes('node2'), []);
    t.deepEqual(await mempoolHashes('node3'), []);
    await allNodesBeforeRestart.reduce<Promise<unknown>>(
      async (chain, node) =>
        chain.then(async () => {
          t.deepEqual(await checker.confirmedButInMempool(node), [], node);
          t.deepEqual(await checker.orphanMempoolDescendants(node), [], node);
        }),
      Promise.resolve(undefined)
    );
    t.pass();
  }
);

serialTest('[e2e] shuts down with SIGINT', async (t) => {
  chaingraphProcess2!.kill('SIGINT');
  await waitForStdout('Shutting down...');
  t.deepEqual(
    await waitForGracefulExit(chaingraphProcess2!, batchTimeoutMs),
    0
  );
});

const historicalRepairTipIndex = 161;
const historicalRepairTransactionIndex = 1;
const historicalRepairBlock = tipA[historicalRepairTipIndex]!;
const historicalRepairBlockHash = historicalRepairBlock.header.hash;

serialTest(
  '[e2e] prepares incomplete historical block transaction before restart',
  async (t) => {
    const transactionHash =
      historicalRepairBlock.transactions[historicalRepairTransactionIndex]!
        .hash;
    t.deepEqual(
      await eventuallyEqual(
        async () =>
          checker.blockTransactionAt(
            historicalRepairBlockHash,
            historicalRepairTransactionIndex
          ),
        transactionHash,
        { timeoutMs: visibleTimeoutMs }
      ),
      transactionHash
    );
    await checker.dropBlockTransactionLink(
      historicalRepairBlockHash,
      historicalRepairTransactionIndex
    );
    const expectedCount = historicalRepairBlock.transactions.length - 1;
    t.deepEqual(
      await waitForBlockTransactionCount(
        historicalRepairBlockHash,
        expectedCount
      ),
      expectedCount
    );
  }
);

serialTest(
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
      appendStdout(chunk);
    });
    await waitForStdout('Starting Chaingraph...');
    await waitForStdout('Restored chain for node node1');
    await waitForStdout('Restored chain for node node2');
    await waitForStdout('Restored chain for node node4');
    t.pass();
  }
);

serialTest('[e2e] catches up a new node via headers', async (t) => {
  t.timeout(isClickHouseE2e ? clickHouseTestTimeoutMs : oneMinute);
  await waitForStdout(
    `node4: accepted 2000 existing blocks from height 1 to height 2000 (hash: ${
      chainStates.node3[2000]!.header.hash
    })`,
    syncTimeoutMs
  );
  await waitForStdout(
    `node4: accepted 1162 existing blocks from height 2001 to height 3162 (hash: ${
      chainStates.node3[3162]!.header.hash
    })`,
    syncTimeoutMs
  );
  const expectedCount = 3163;
  const node4Blocks = await eventually(
    async () => checker.acceptedBlocks('node4'),
    {
      isDone: (blocks) => blocks.length === expectedCount,
      timeoutMs: syncVisibleTimeoutMs,
    }
  );
  t.deepEqual(node4Blocks.length, expectedCount);
  const node4Tip = node4Blocks[node4Blocks.length - 1]!;
  t.deepEqual(
    { hash: node4Tip.hash, height: node4Tip.height },
    { hash: tipA[161]!.header.hash, height: 3162 }
  );
  await waitForStdout('Agent: enabled mempool tracking.', syncTimeoutMs);
  t.pass();
});

serialTest(
  '[e2e] self-heals incomplete historical block transactions on startup',
  async (t) => {
    t.timeout(isClickHouseE2e ? clickHouseTestTimeoutMs : oneMinute);
    t.deepEqual(
      await waitForBlockTransactionCount(
        historicalRepairBlockHash,
        historicalRepairBlock.transactions.length
      ),
      historicalRepairBlock.transactions.length
    );
  }
);

serialTest('[e2e] handles empty headers messages (fully-synced)', async (t) => {
  peers.node1.sendMessage(new peers.node1.messages.Headers([]));
  await waitForStdout(
    'node1: received empty headers message – headers-syncing completed'
  );
  t.pass();
});

serialTest(
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
    /**
     * The save log line can precede visibility (ClickHouse publishes the
     * watermark asynchronously), so poll for the expected count; a wrong
     * count still fails after the timeout with the last value read.
     */
    const expectedTransactionCount = tipA[tipStartIndex]!.transactions.length;
    t.deepEqual(
      await waitForBlockTransactionCount(
        tipA[tipStartIndex]!.header.hash,
        expectedTransactionCount
      ),
      expectedTransactionCount
    );
  }
);

serialTest('[e2e] syncs remaining blocks one-by-one', async (t) => {
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
  await waitForStdout(
    /Saved new block – height:\s+3200[^\n]+nodes: node2/u,
    batchTimeoutMs
  );
  await waitForStdout(
    /Saved new block – height:\s+3200[^\n]+nodes: node1, node4/u,
    batchTimeoutMs
  );
  t.pass();
});

serialTest('[e2e] [api] 404: logs unknown request urls', async (t) => {
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

serialTest(
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

serialTest(
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

serialTest(
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

serialTest(
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

serialTest(
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

serialTest(
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
serialTest('[e2e] [api] /send-transaction: invalid TX', async (t) => {
  node1InternalId = (await checker.nodeInternalId('node1'))!;
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

serialTest('[e2e] [api] /send-transaction: valid', async (t) => {
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

postgresTest.concurrent(
  'P2PKH',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '76a914000000000000000000000000000000000000000088ac',
  '76a91488ac'
);
postgresTest.concurrent(
  'P2SH',
  bytecodeFunction,
  'parse_bytecode_pattern',
  'a914000000000000000000000000000000000000000087',
  'a91487'
);
postgresTest.concurrent(
  'OP_RETURN (fixed pushes)',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '6a04000000005120000000000000000000000000000000000000000000000000000000000000000004000000000400000000',
  '6a0451200404'
);
postgresTest.concurrent(
  'OP_RETURN with OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '6a026d0c090000000000000000004c5c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000',
  '6a02094c'
);

const allOnes = 0x11;
const minPushData2 = 256;
postgresTest.concurrent(
  'OP_RETURN with OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData2).fill(allOnes))
  )}515151`,
  '6a4d515151'
);

const minPushData4 = 65536;
postgresTest.concurrent(
  'OP_RETURN with OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData4).fill(allOnes))
  )}515151`,
  '6a4e515151'
);

postgresTest.concurrent(
  'malformed OP_PUSHBYTES',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '515102',
  '515102'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '51514c',
  '51514c'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '51514d11',
  '51514d'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern',
  '51514e112233',
  '51514e'
);

postgresTest.concurrent(
  'P2PKH',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '76a914000000000000000000000000000000000000000088ac',
  '76a91488ac'
);
postgresTest.concurrent(
  'P2SH',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  'a914000000000000000000000000000000000000000087',
  'a91487'
);
postgresTest.concurrent(
  'OP_RETURN (fixed pushes)',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '6a04000000005120000000000000000000000000000000000000000000000000000000000000000004000000000400000000',
  '6a0451200404'
);
postgresTest.concurrent(
  'OP_RETURN with OP_PUSHDATA1 (memo.cash)',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '6a026d0c090000000000000000004c5c0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000',
  '6a02094c5c'
);

postgresTest.concurrent(
  'zero-length OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '4c00',
  '4c00'
);
postgresTest.concurrent(
  'zero-length OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '4d0000',
  '4d0000'
);
postgresTest.concurrent(
  'zero-length OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '4e00000000',
  '4e00000000'
);

postgresTest.concurrent(
  'OP_RETURN with OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData2).fill(allOnes))
  )}515151`,
  '6a4d0001515151'
);

postgresTest.concurrent(
  'OP_RETURN with OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  `6a${binToHex(
    encodeDataPush(new Uint8Array(minPushData4).fill(allOnes))
  )}515151`,
  '6a4e00000100515151'
);

postgresTest.concurrent(
  'malformed OP_PUSHBYTES',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '515102',
  '515102'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA1',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '51514c',
  '51514c'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA2',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '51514d11',
  '51514d'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA4',
  bytecodeFunction,
  'parse_bytecode_pattern_with_pushdata_lengths',
  '51514e112233',
  '51514e'
);

postgresTest.concurrent(
  'no redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  `0002000051`
);

postgresTest.concurrent(
  'OP_PUSHBYTES redeem',
  bytecodeFunction,
  'parse_bytecode_pattern_redeem',
  `0003019951`,
  '0151'
);

const minPushData1 = 76;
postgresTest.concurrent(
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

postgresTest.concurrent(
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

postgresTest.concurrent(
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

postgresTest.concurrent(
  'malformed OP_PUSHDATA1 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4c'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA2 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4d11'
);
postgresTest.concurrent(
  'malformed OP_PUSHDATA4 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4e112233'
);
postgresTest.concurrent(
  'oversized OP_PUSHDATA4 redeem',
  bytecodeFunctionReturnsNull,
  'parse_bytecode_pattern_redeem',
  '4effffffff'
);

postgresTest.concurrent('[e2e] [postgres] encode_uint16le', async (t) => {
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

postgresTest.concurrent('[e2e] [postgres] encode_uint32le', async (t) => {
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

postgresTest.concurrent('[e2e] [postgres] encode_int32le', async (t) => {
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

postgresTest.concurrent('[e2e] [postgres] encode_uint64le', async (t) => {
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

postgresTest.concurrent('[e2e] [postgres] encode_compact_uint', async (t) => {
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

postgresTest.serial(
  '[e2e] [postgres] [sql] search_output: exact matches for locking bytecode of any length',
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
postgresTest.serial(
  '[e2e] [postgres] [sql] search_output_prefix: treats every byte literally and accepts prefixes longer than 25 bytes',
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

postgresTest.serial(
  '[e2e] [postgres] [sql] search functions use the 25-byte locking bytecode prefix index',
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
    plans.forEach((plan) => {
      /*
       * Either the agent-created index or the identical test copy satisfies
       * the check; which one the planner picks depends on whether auto-vacuum
       * has analyzed `output` yet.
       */
      t.true(/\b(?:test_)?output_search_index\b/u.test(plan), plan);
      t.false(plan.includes('Seq Scan'), plan);
    });
  }
);
