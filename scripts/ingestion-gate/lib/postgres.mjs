/**
 * Throwaway Postgres (Docker container or a private host cluster) +
 * per-scenario database helpers.
 */
import { execFileSync } from 'node:child_process';
import { readdirSync, readFileSync, existsSync, mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const pg = require('pg');

export const postgresUser = 'chaingraph';
export const postgresPassword = 'very_insecure_postgres_password';

/** Server settings shared by both backends (kept small: VM memory matters). */
const serverSettings = [
  'shared_buffers=256MB',
  'max_wal_size=8GB',
  'checkpoint_timeout=30min',
  'max_connections=100',
];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** A pg.Client that won't crash the harness if the server dies mid-run. */
export const connectClient = async (connectionString) => {
  const client = new pg.Client({ connectionString });
  client.on('error', () => {});
  await client.connect();
  return client;
};

const waitUntilReady = async (baseUrl, requireRestart) => {
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      const client = await connectClient(`${baseUrl}/postgres`);
      await client.query('SELECT 1');
      await client.end();
      // the official Docker image restarts once after init; skip the first hit
      if (!requireRestart || attempt > 0) return baseUrl;
    } catch {
      // not ready
    }
    await sleep(500);
  }
  throw new Error('Postgres did not become ready');
};

/** Docker VM memory in bytes (0 if unknown). */
export const dockerMemoryBytes = () => {
  try {
    return Number(execFileSync('docker', ['info', '--format', '{{.MemTotal}}'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim());
  } catch {
    return 0;
  }
};

export const startDockerPostgres = async ({ image, port, name, log }) => {
  log(`starting ${image} as container ${name} on 127.0.0.1:${port}`);
  execFileSync('docker', [
    'run', '-d', '--rm', '--name', name,
    '-p', `127.0.0.1:${port}:5432`,
    '--shm-size=1g',
    '-e', `POSTGRES_USER=${postgresUser}`,
    '-e', `POSTGRES_PASSWORD=${postgresPassword}`,
    image,
    ...serverSettings.flatMap((setting) => ['-c', setting]),
  ], { stdio: 'ignore' });
  const baseUrl = await waitUntilReady(`postgres://${postgresUser}:${postgresPassword}@127.0.0.1:${port}`, true);
  const stop = () => {
    try {
      execFileSync('docker', ['rm', '-f', name], { stdio: 'ignore' });
    } catch {
      // already gone
    }
  };
  return { baseUrl, description: `docker ${image}`, stop };
};

/** initdb a private cluster with the host's Postgres binaries (`--pg-bin`). */
export const startHostPostgres = async ({ binDirectory, dataDirectory, port, log }) => {
  log(`starting host Postgres from ${binDirectory} in ${dataDirectory} on 127.0.0.1:${port}`);
  rmSync(dataDirectory, { force: true, recursive: true });
  mkdirSync(dataDirectory, { recursive: true });
  execFileSync(join(binDirectory, 'initdb'), ['-D', dataDirectory, '-U', postgresUser, '--auth=trust', '--no-locale', '-E', 'UTF8'], { stdio: 'ignore' });
  const serverOptions = [`-p ${port}`, `-k ${dataDirectory}`, '-c listen_addresses=127.0.0.1', ...serverSettings.map((setting) => `-c ${setting}`)].join(' ');
  try {
    execFileSync(join(binDirectory, 'pg_ctl'), ['-D', dataDirectory, '-o', serverOptions, '-l', join(dataDirectory, 'server.log'), '-w', 'start'], { stdio: 'ignore' });
  } catch (error) {
    const serverLog = existsSync(join(dataDirectory, 'server.log')) ? readFileSync(join(dataDirectory, 'server.log'), 'utf8').slice(-1500) : '';
    rmSync(dataDirectory, { force: true, recursive: true });
    throw new Error(`host Postgres failed to start on port ${port} (in use? try --pg-port):\n${serverLog}`);
  }
  const baseUrl = await waitUntilReady(`postgres://${postgresUser}:${postgresPassword}@127.0.0.1:${port}`, false);
  const version = execFileSync(join(binDirectory, 'postgres'), ['--version'], { encoding: 'utf8' }).trim();
  const stop = () => {
    try {
      execFileSync(join(binDirectory, 'pg_ctl'), ['-D', dataDirectory, '-m', 'immediate', '-w', 'stop'], { stdio: 'ignore' });
    } catch {
      // already stopped
    }
    rmSync(dataDirectory, { force: true, recursive: true });
  };
  return { baseUrl, description: `host ${version}`, stop };
};

/** Every `up.sql` under the agent's migrations directory, in timestamp order. */
export const listMigrations = (agentDirectory) => {
  const migrationsDirectory = join(agentDirectory, 'images/hasura/hasura-data/migrations/default');
  return readdirSync(migrationsDirectory)
    .filter((name) => existsSync(join(migrationsDirectory, name, 'up.sql')))
    .sort()
    .map((name) => join(migrationsDirectory, name, 'up.sql'));
};

export const recreateDatabase = async ({ baseUrl, databaseName, agentDirectory }) => {
  const admin = await connectClient(`${baseUrl}/postgres`);
  await admin.query(`DROP DATABASE IF EXISTS ${databaseName} WITH (FORCE)`);
  await admin.query(`CREATE DATABASE ${databaseName}`);
  await admin.end();
  const client = await connectClient(`${baseUrl}/${databaseName}`);
  for (const migrationPath of listMigrations(agentDirectory)) {
    await client.query(readFileSync(migrationPath, 'utf8'));
  }
  return client;
};

export const dropDatabase = async ({ baseUrl, databaseName }) => {
  const admin = await connectClient(`${baseUrl}/postgres`);
  await admin.query(`DROP DATABASE IF EXISTS ${databaseName} WITH (FORCE)`);
  await admin.end();
};

export const currentWalLsn = async (client) =>
  (await client.query('SELECT pg_current_wal_lsn()::text AS lsn')).rows[0].lsn;

export const walBytesSince = async (client, startLsn) =>
  Number(
    (await client.query('SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint AS bytes', [startLsn]))
      .rows[0].bytes
  );

export const countRows = async (client, table) =>
  Number((await client.query(`SELECT count(*)::bigint AS count FROM ${table}`)).rows[0].count);

/** Hashes (hex) of the given blocks that are accepted by the given node. */
export const acceptedBlockCount = async (client, nodeName, blockHashes) =>
  Number(
    (
      await client.query(
        `SELECT count(*)::bigint AS count
           FROM node_block
           JOIN node ON node.internal_id = node_block.node_internal_id
           JOIN block ON block.internal_id = node_block.block_internal_id
          WHERE node.name = $1 AND block.hash = ANY($2::bytea[])`,
        [nodeName, blockHashes.map((hash) => Buffer.from(hash, 'hex'))]
      )
    ).rows[0].count
  );

export const blockTransactionCount = async (client, blockHashes) =>
  Number(
    (
      await client.query(
        `SELECT count(*)::bigint AS count
           FROM block_transaction
           JOIN block ON block.internal_id = block_transaction.block_internal_id
          WHERE block.hash = ANY($1::bytea[])`,
        [blockHashes.map((hash) => Buffer.from(hash, 'hex'))]
      )
    ).rows[0].count
  );

export const waitFor = async (predicate, { timeoutMs, intervalMs = 25, description, agent }) => {
  const started = Date.now();
  for (;;) {
    if (await predicate()) return Date.now();
    // an agent that dies mid-scenario (e.g. a crash on the max block) fails at once instead of after timeoutMs
    if (agent?.exited) throw new Error(`agent exited (code ${agent.exitCode}) while waiting for: ${description}\n--- last output ---\n${agent.stdoutBuffer.slice(-3000)}`);
    if (Date.now() - started > timeoutMs) {
      throw new Error(`timed out after ${timeoutMs / 1000}s waiting for: ${description}`);
    }
    await sleep(intervalMs);
  }
};

/*
 * Backend surface shared with clickhouse.mjs (`--store`): sessions, agent
 * environment, correctness reads and write metrics. A Postgres session is a
 * pg.Client.
 */

export const openSession = async ({ baseUrl, databaseName }) => connectClient(`${baseUrl}/${databaseName}`);

export const closeSession = async (client) => client.end();

export const agentEnvironment = ({ baseUrl, databaseName }) => ({
  CHAINGRAPH_POSTGRES_CONNECTION_STRING: `${baseUrl}/${databaseName}`,
  CHAINGRAPH_STORE: 'postgres',
});

/** Write metrics of a measured window: WAL bytes (cluster-wide). */
export const writeMetricsStart = async (client) => ({ lsn: await currentWalLsn(client) });

export const writeMetricsSince = async (client, start) => ({ walBytes: await walBytesSince(client, start.lsn) });

export const acceptedChain = async (client, nodeName) =>
  (
    await client.query(
      `SELECT block.height::int AS height, encode(block.hash, 'hex') AS hash
         FROM node_block
         JOIN node ON node.internal_id = node_block.node_internal_id
         JOIN block ON block.internal_id = node_block.block_internal_id
        WHERE node.name = $1
        ORDER BY block.height`,
      [nodeName]
    )
  ).rows;

export const nodeBlockCount = async (client, nodeName) =>
  Number((await client.query(`SELECT count(*)::bigint AS count FROM node_block JOIN node ON node.internal_id = node_block.node_internal_id WHERE node.name = $1`, [nodeName])).rows[0].count);

export const mempoolRowCount = async (client, nodeName, transactionHashes) =>
  Number(
    (
      await client.query(
        `SELECT count(*)::bigint AS count
           FROM node_transaction
           JOIN node ON node.internal_id = node_transaction.node_internal_id
           JOIN transaction ON transaction.internal_id = node_transaction.transaction_internal_id
          WHERE node.name = $1 AND transaction.hash = ANY($2::bytea[])`,
        [nodeName, transactionHashes.map((hash) => Buffer.from(hash, 'hex'))]
      )
    ).rows[0].count
  );

export const historyNodeCount = async (client, nodeName, transactionHashes) =>
  Number(
    (
      await client.query(
        `SELECT count(DISTINCT node_transaction_history.transaction_internal_id)::bigint AS count
           FROM node_transaction_history
           JOIN node ON node.internal_id = node_transaction_history.node_internal_id
           JOIN transaction ON transaction.internal_id = node_transaction_history.transaction_internal_id
          WHERE node.name = $1 AND transaction.hash = ANY($2::bytea[])`,
        [nodeName, transactionHashes.map((hash) => Buffer.from(hash, 'hex'))]
      )
    ).rows[0].count
  );

/** Per node: node_transaction rows whose transaction is in a block the same node accepted. */
export const confirmedButInMempoolCount = async (client, nodeName) =>
  Number(
    (
      await client.query(
        `SELECT count(*)::bigint AS count
           FROM node_transaction
           JOIN node ON node.internal_id = node_transaction.node_internal_id
           JOIN block_transaction ON block_transaction.transaction_internal_id = node_transaction.transaction_internal_id
           JOIN node_block ON node_block.block_internal_id = block_transaction.block_internal_id
                          AND node_block.node_internal_id = node_transaction.node_internal_id
          WHERE node.name = $1`,
        [nodeName]
      )
    ).rows[0].count
  );
