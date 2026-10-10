// cspell:ignore clickhouse
/**
 * Note: this file only logs to STDOUT and STDERR, as logging relies on the
 * `CHAINGRAPH_LOG_PATH` configuration.
 */
import { readFileSync } from 'fs';
import { cpus, freemem, homedir } from 'os';
import { dirname, join, resolve } from 'path';
import { fileURLToPath } from 'url';

import { binToHex, hexToBin, isHex } from '@bitauth/libauth';
import dotenv from 'dotenv';

import { bitcoreBlockToChaingraphBlock, messages } from './bitcore.js';
import type { ChaingraphBlock } from './types/chaingraph.js';

const dotEnvConfig = dotenv.config();
if (dotEnvConfig.parsed === undefined) {
  const error = dotEnvConfig.error ?? new Error('Unknown dotenv error.');
  // eslint-disable-next-line functional/no-throw-statement
  throw error;
}
const defaults = dotenv.parse(
  readFileSync(
    resolve(dirname(fileURLToPath(import.meta.url)), '../defaults.env')
  )
);

const configuration = {
  ...defaults,
  ...dotEnvConfig.parsed,
  ...process.env,
} as {
  [x: string]: string | undefined;
};

const expectedOptions = [
  'CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB',
  'CHAINGRAPH_GENESIS_BLOCKS',
  'CHAINGRAPH_INCOMPLETE_BLOCK_REPAIR_BATCH_SIZE',
  'CHAINGRAPH_INTERNAL_API_PORT',
  'CHAINGRAPH_LOG_FIREHOSE',
  'CHAINGRAPH_LOG_LEVEL_STDOUT',
  'CHAINGRAPH_LOG_LEVEL_PATH',
  'CHAINGRAPH_LOG_PATH',
  'CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_MS',
  'CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_SCAN_INTERVAL_MS',
  'CHAINGRAPH_POSTGRES_MAX_CONNECTIONS',
  'CHAINGRAPH_POSTGRES_SYNCHRONOUS_COMMIT',
  'CHAINGRAPH_TRUSTED_NODES',
  'CHAINGRAPH_USER_AGENT',
  'NODE_ENV',
] as const;
const requireStringValues = <Key extends string>(
  conf: { [x: string]: string | undefined },
  keys: readonly Key[]
): conf is { [key in Key]: string } => {
  const missing = keys.find((key) => typeof conf[key] !== 'string');
  if (missing !== undefined) {
    // eslint-disable-next-line no-console
    console.error(`\n\nERROR: missing variable: ${missing}\n\n`);
    return false;
  }
  return true;
};
if (!requireStringValues(configuration, expectedOptions)) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error('Missing expected environment variable.');
}

const maxConnectionsValue = Number(
  configuration.CHAINGRAPH_POSTGRES_MAX_CONNECTIONS
);
/**
 * Set via the `CHAINGRAPH_POSTGRES_MAX_CONNECTIONS` environment variable.
 */
const postgresMaxConnections =
  maxConnectionsValue === 0 ? cpus().length : maxConnectionsValue;
if (!Number.isInteger(postgresMaxConnections) || postgresMaxConnections < 1) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'The CHAINGRAPH_POSTGRES_MAX_CONNECTIONS environment variable must be a number greater than 0.'
  );
}

const blockBufferTargetSizeMbValue = Number(
  configuration.CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB
);
const mb = 1_000_000;
const denominator = 3;
const oneThirdOfAvailableMemory = Math.round(freemem() / mb / denominator);
const expectedMaximumBlockSizeMb = 32;
const maximumUsefulBuffer = postgresMaxConnections * expectedMaximumBlockSizeMb;
const autoBufferSize = Math.min(oneThirdOfAvailableMemory, maximumUsefulBuffer);
/**
 * Set via the `CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB` environment variable.
 * If no block buffer target is provided, Chaingraph will allocate 1/3 of free
 * system memory up to the maximum useful value
 * (`postgresMaxConnections * maximum block size`).
 */
const blockBufferTargetSizeMb =
  blockBufferTargetSizeMbValue === 0
    ? autoBufferSize
    : blockBufferTargetSizeMbValue;
if (isNaN(blockBufferTargetSizeMb) || blockBufferTargetSizeMb <= 0) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'The CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB environment variable must be greater than 0.'
  );
}

const incompleteBlockRepairBatchSize = Number(
  configuration.CHAINGRAPH_INCOMPLETE_BLOCK_REPAIR_BATCH_SIZE
);
if (
  !Number.isInteger(incompleteBlockRepairBatchSize) ||
  incompleteBlockRepairBatchSize < 0
) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'The CHAINGRAPH_INCOMPLETE_BLOCK_REPAIR_BATCH_SIZE environment variable must be an integer greater than or equal to 0.'
  );
}

const mempoolTransactionExpirationMs = Number(
  configuration.CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_MS
);
if (
  !Number.isInteger(mempoolTransactionExpirationMs) ||
  mempoolTransactionExpirationMs <= 0
) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'The CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_MS environment variable must be an integer greater than 0.'
  );
}

const mempoolTransactionExpirationScanIntervalMs = Number(
  configuration.CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_SCAN_INTERVAL_MS
);
if (
  !Number.isInteger(mempoolTransactionExpirationScanIntervalMs) ||
  mempoolTransactionExpirationScanIntervalMs <= 0
) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'The CHAINGRAPH_MEMPOOL_TRANSACTION_EXPIRATION_SCAN_INTERVAL_MS environment variable must be an integer greater than 0.'
  );
}

const extendTildeAndResolvePath = (path: string) =>
  path.startsWith('~')
    ? resolve(join(homedir(), path.slice(1)))
    : resolve(path);

const logPathResolved = extendTildeAndResolvePath(
  configuration.CHAINGRAPH_LOG_PATH.endsWith('.ndjson')
    ? configuration.CHAINGRAPH_LOG_PATH
    : `${configuration.CHAINGRAPH_LOG_PATH}/log_${new Date()
        .toISOString()
        .replace(/[-:.]/gu, '_')}.ndjson`
);
const disabled = 'false';
/**
 * Set via the `CHAINGRAPH_LOG_PATH` environment variable. The tilde character
 * (`~`) is expanded via `os.homedir()`, and all paths are `path.resolve()`ed. If the path does not end in `.ndjson`, a new file will be created at the path (i.e. `log_${timestamp}.ndjson`) A
 * value of `false` disables logging to files.
 */
const chaingraphLogPath =
  configuration.CHAINGRAPH_LOG_PATH === disabled ? false : logPathResolved;

const allowedLevels = [
  'trace',
  'debug',
  'info',
  'warn',
  'error',
  'fatal',
] as const;
const isValidLoggingLevel = (
  level: string
): level is (typeof allowedLevels)[number] =>
  allowedLevels.includes(level as unknown as (typeof allowedLevels)[number]);

if (!isValidLoggingLevel(configuration.CHAINGRAPH_LOG_LEVEL_STDOUT)) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Invalid level provided in the 'CHAINGRAPH_LOG_LEVEL_STDOUT' environment variable: ${
      configuration.CHAINGRAPH_LOG_LEVEL_STDOUT
    }. Must be one of the following: ${allowedLevels.join(', ')}`
  );
}
if (!isValidLoggingLevel(configuration.CHAINGRAPH_LOG_LEVEL_PATH)) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Invalid level provided in the 'CHAINGRAPH_LOG_LEVEL_PATH' environment variable: ${
      configuration.CHAINGRAPH_LOG_LEVEL_PATH
    }. Must be one of the following: ${allowedLevels.join(', ')}`
  );
}
/**
 * Set via the `CHAINGRAPH_LOG_LEVEL_STDOUT` environment variable.
 */
const chaingraphLogLevelStdout = configuration.CHAINGRAPH_LOG_LEVEL_STDOUT;

/**
 * Set via the `CHAINGRAPH_LOG_LEVEL_PATH` environment variable.
 */
const chaingraphLogLevelPath = configuration.CHAINGRAPH_LOG_LEVEL_PATH;

if (
  configuration.CHAINGRAPH_LOG_FIREHOSE.toLowerCase() !== 'true' &&
  configuration.CHAINGRAPH_LOG_FIREHOSE.toLowerCase() !== 'false'
) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Invalid value provided in the 'CHAINGRAPH_LOG_FIREHOSE' environment variable: ${configuration.CHAINGRAPH_LOG_FIREHOSE}. Must be one of the following: true, false`
  );
}
/**
 * Set via the `CHAINGRAPH_LOG_FIREHOSE` environment variable.
 */
const chaingraphLogFirehose = configuration.CHAINGRAPH_LOG_FIREHOSE === 'true';

const networkMagicHexLength = 8;
/**
 * `0xe3e1f3e8` – from `utf8ToBin('cash').map(x => x | 128)`
 */
// eslint-disable-next-line @typescript-eslint/no-magic-numbers
const networkMagicBchMainnet = Uint8Array.from([0xe3, 0xe1, 0xf3, 0xe8]);
/**
 * `0xdab5bffa` (origin currently unknown)
 */
// eslint-disable-next-line @typescript-eslint/no-magic-numbers
const networkMagicBchTestnet3 = Uint8Array.from([0xda, 0xb5, 0xbf, 0xfa]);
/**
 * `0xe2b7daaf` (origin currently unknown)
 */
// eslint-disable-next-line @typescript-eslint/no-magic-numbers
const networkMagicBchTestnet4 = Uint8Array.from([0xe2, 0xb7, 0xda, 0xaf]);
// eslint-disable-next-line complexity
const validateNetworkMagic = (input: string) => {
  const magicBytes =
    input === 'mainnet'
      ? networkMagicBchMainnet
      : input === 'testnet' || input === 'testnet4' || input === 'chipnet'
      ? networkMagicBchTestnet4
      : input === 'testnet3'
      ? networkMagicBchTestnet3
      : isHex(input) && input.length === networkMagicHexLength
      ? hexToBin(input)
      : undefined;
  if (magicBytes === undefined) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      'Improperly formatted network magic bytes. Must be `mainnet`, `testnet`, or 4 hex-encoded bytes, e.g. `e3e1f3e8`.'
    );
  }
  return binToHex(magicBytes);
};

/**
 * Set via the `CHAINGRAPH_GENESIS_BLOCKS` environment variable.
 *
 * The `genesisBlocks` map is guaranteed to include the genesis block for every
 * network used by `trustedNodes` (an error is thrown at startup if any trusted
 * nodes reference a `NETWORK` for which the genesis block is unknown).
 */
const genesisBlocks = configuration.CHAINGRAPH_GENESIS_BLOCKS.split(
  ','
).reduce<{
  [networkMagicHex: string]: ChaingraphBlock;
}>((all, entry) => {
  const [networkMagicHex, hex] = entry.split(':');
  if (networkMagicHex!.length !== networkMagicHexLength) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `Improperly formatted 'CHAINGRAPH_GENESIS_BLOCKS' environment variable.

Expected format: NETWORK_MAGIC:RAW_GENESIS_BLOCK_HEX,NETWORK_MAGIC:RAW_GENESIS_BLOCK_HEX,...
E.g.: ${defaults.CHAINGRAPH_GENESIS_BLOCKS!}

Invalid segment: ${entry}`
    );
  }
  return {
    ...all,
    [networkMagicHex!]: bitcoreBlockToChaingraphBlock(
      messages.Block.fromBuffer(Buffer.from(hexToBin(hex!))).block,
      0
    ),
  };
}, {});

/**
 * Derived from the `CHAINGRAPH_TRUSTED_NODES` environment variable. Format:
 * `NODE_NAME:HOST:PORT_NUMBER:NETWORK`, nodes are separated by commas.
 * NETWORK may be provided as `main` (0xe3e1f3e8), `test` (0xdab5bffa), or 4
 * hex-encoded "magic bytes", e.g. `e3e1f3e8`.
 *
 * E.g. `TRUSTED_NODES=bchn:127.0.0.1:8333:mainnet,bchd:127.0.0.1:8334:testnet`
 */
const trustedNodes = configuration.CHAINGRAPH_TRUSTED_NODES.split(',').map(
  (node) => {
    const parts = node.split(':');
    const expectedParts = 4;
    const [name, host, portString, networkMagic] = parts;
    const networkMagicHex = validateNetworkMagic(networkMagic!);
    if (!Object.keys(genesisBlocks).includes(networkMagicHex)) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `The 'CHAINGRAPH_TRUSTED_NODES' environment variable references a 'NETWORK' for which the genesis block is unknown: 0x${networkMagicHex}. Please provide the missing genesis block using the 'CHAINGRAPH_GENESIS_BLOCKS' environment variable.`
      );
    }
    const port = parseInt(portString!, 10);
    if (
      parts.length !== expectedParts ||
      parts.every((part) => typeof part !== 'string') ||
      isNaN(port)
    ) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new Error(
        `Improperly formatted 'CHAINGRAPH_TRUSTED_NODES' environment variable.

Expected format: NODE_NAME:HOST:PORT_NUMBER:NETWORK,NODE_NAME:HOST:PORT_NUMBER:NETWORK,...
E.g.: TRUSTED_NODES=bchn:127.0.0.1:8333:mainnet,bchd:127.0.0.1:8334:e3e1f3e8

Invalid segment: ${node}`
      );
    }
    return {
      /**
       * The IP address of this trusted node.
       */
      host: host!,
      /**
       * The name of this trusted node – used as a stable identifier between
       * restarts.
       */
      name: name!,
      /**
       * The "network magic" expected by this node's P2P network interface.
       */
      networkMagicHex,
      /**
       * The listening port of this trusted node.
       */
      port,
    };
  }
);

const nodeNames = trustedNodes.map((n) => n.name);
const duplicateName = nodeNames.find(
  (value, index, list) => index !== list.lastIndexOf(value)
);
if (duplicateName !== undefined) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Multiple nodes in CHAINGRAPH_TRUSTED_NODES have been assigned the same name: ${duplicateName}`
  );
}

const nodeHostAndPort = trustedNodes.map((n) => `${n.host}:${n.port}`);
const duplicateHostAndPort = nodeHostAndPort.find(
  (value, index, list) => index !== list.lastIndexOf(value)
);
if (duplicateHostAndPort !== undefined) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Multiple nodes in CHAINGRAPH_TRUSTED_NODES have been assigned the same host and port: ${duplicateHostAndPort}`
  );
}

/**
 * Set via the `CHAINGRAPH_INTERNAL_API_PORT` environment variable.
 */
const chaingraphInternalApiPort = Number(
  configuration.CHAINGRAPH_INTERNAL_API_PORT
);
if (isNaN(chaingraphInternalApiPort)) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error('CHAINGRAPH_INTERNAL_API_PORT must be a number.');
}

/**
 * Can be overridden via the `CHAINGRAPH_USER_AGENT` environment variable.
 * TODO: use docker image tag (git commit short digest) from an environment variable for tagged images (passed in by kubernetes), or `dev-build` if not a production image
 */
const chaingraphUserAgent =
  configuration.CHAINGRAPH_USER_AGENT === ''
    ? `/chaingraph/`
    : configuration.CHAINGRAPH_USER_AGENT;

/**
 * Set via the `CHAINGRAPH_POSTGRES_SYNCHRONOUS_COMMIT` environment variable.
 */
const postgresSynchronousCommit =
  configuration.CHAINGRAPH_POSTGRES_SYNCHRONOUS_COMMIT !== 'false';

/**
 * Options which may be absent from both `defaults.env` and the environment.
 */
const optionalOptions = configuration as { [x: string]: string | undefined };

const allowedStores = ['postgres', 'clickhouse'] as const;
const isValidStore = (store: string): store is (typeof allowedStores)[number] =>
  allowedStores.includes(store as unknown as (typeof allowedStores)[number]);
const chaingraphStoreValue =
  optionalOptions.CHAINGRAPH_STORE === undefined ||
  optionalOptions.CHAINGRAPH_STORE === ''
    ? 'postgres'
    : optionalOptions.CHAINGRAPH_STORE;
if (!isValidStore(chaingraphStoreValue)) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Invalid value provided in the 'CHAINGRAPH_STORE' environment variable: ${chaingraphStoreValue}. Must be one of the following: ${allowedStores.join(
      ', '
    )}`
  );
}
/**
 * Set via the `CHAINGRAPH_STORE` environment variable (default: `postgres`).
 * Selects the storage backend used by the agent.
 */
const chaingraphStore = chaingraphStoreValue;

/**
 * Options required only by the Postgres store. With `CHAINGRAPH_STORE=clickhouse`
 * they are not required and are ignored (the image's `defaults.env` still
 * supplies a localhost connection string; nothing connects to it).
 */
const postgresOnlyOptions = ['CHAINGRAPH_POSTGRES_CONNECTION_STRING'] as const;
if (
  chaingraphStore === 'postgres' &&
  !requireStringValues(configuration, postgresOnlyOptions)
) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error('Missing expected environment variable.');
}

/**
 * Set via the `CHAINGRAPH_POSTGRES_CONNECTION_STRING` environment variable.
 * Required if `CHAINGRAPH_STORE` is `postgres` (the default); `undefined` when
 * it is `clickhouse` (the variable is ignored).
 */
const postgresConnectionString =
  chaingraphStore === 'postgres'
    ? optionalOptions.CHAINGRAPH_POSTGRES_CONNECTION_STRING
    : undefined;

const optionalString = (value: string | undefined, fallback: string) =>
  value === undefined || value === '' ? fallback : value;

/**
 * Set via the `CHAINGRAPH_CLICKHOUSE_URL` environment variable (HTTP
 * interface, e.g. `http://localhost:8123`). Required if `CHAINGRAPH_STORE` is
 * `clickhouse`.
 */
const clickhouseUrl = optionalString(
  optionalOptions.CHAINGRAPH_CLICKHOUSE_URL,
  ''
);
if (chaingraphStore === 'clickhouse') {
  if (clickhouseUrl === '') {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `The 'CHAINGRAPH_CLICKHOUSE_URL' environment variable is required when CHAINGRAPH_STORE is 'clickhouse'.`
    );
  }
  // eslint-disable-next-line functional/no-try-statement
  try {
    // eslint-disable-next-line no-new
    new URL(clickhouseUrl);
  } catch {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `Invalid value provided in the 'CHAINGRAPH_CLICKHOUSE_URL' environment variable (value not shown, it may contain credentials). Must be a URL, e.g. http://localhost:8123`
    );
  }
}

/**
 * Set via the `CHAINGRAPH_CLICKHOUSE_DATABASE` environment variable (default:
 * `cg`).
 */
const clickhouseDatabase = optionalString(
  optionalOptions.CHAINGRAPH_CLICKHOUSE_DATABASE,
  'cg'
);
if (!/^[A-Za-z_][0-9A-Za-z_]*$/u.test(clickhouseDatabase)) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Invalid value provided in the 'CHAINGRAPH_CLICKHOUSE_DATABASE' environment variable: ${clickhouseDatabase}. Must be a plain identifier (letters, digits and underscores).`
  );
}

/**
 * Set via the `CHAINGRAPH_CLICKHOUSE_USER` environment variable (default:
 * `default`).
 */
const clickhouseUser = optionalString(
  optionalOptions.CHAINGRAPH_CLICKHOUSE_USER,
  'default'
);

/**
 * Set via the `CHAINGRAPH_CLICKHOUSE_PASSWORD` environment variable (default:
 * empty). Never log this value.
 */
const clickhousePassword = optionalString(
  optionalOptions.CHAINGRAPH_CLICKHOUSE_PASSWORD,
  ''
);

const maxInFlightSavesValue = Number(
  optionalString(optionalOptions.CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES, '0')
);
if (!Number.isInteger(maxInFlightSavesValue) || maxInFlightSavesValue < 0) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'The CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES environment variable must be an integer greater than or equal to 0.'
  );
}
/**
 * Set via the `CHAINGRAPH_CLICKHOUSE_MAX_IN_FLIGHT_SAVES` environment variable
 * (default: `0`, unbounded). The ClickHouse store's in-flight cap: at most this
 * many block saves / header acceptances work at once; a call waiting on
 * another call does not hold a slot. Ignored by the Postgres store (its bound
 * is `CHAINGRAPH_POSTGRES_MAX_CONNECTIONS`).
 */
const clickhouseMaxInFlightSaves = maxInFlightSavesValue;

const clickhouseUtxoValue = optionalString(
  optionalOptions.CHAINGRAPH_CLICKHOUSE_UTXO,
  'on'
);
if (clickhouseUtxoValue !== 'on' && clickhouseUtxoValue !== 'off') {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    `Invalid value provided in the 'CHAINGRAPH_CLICKHOUSE_UTXO' environment variable: ${clickhouseUtxoValue}. Must be 'on' or 'off'.`
  );
}
/**
 * Set via the `CHAINGRAPH_CLICKHOUSE_UTXO` environment variable (default:
 * `on`). `off`: the ClickHouse store never writes the stored UTXO tables
 * (`utxo`, `utxo_by_script`) and skips the horizon UTXO build; unspent
 * outputs are computed at query time from output/input/acceptance, as in
 * upstream Chaingraph v1 (docs/clickhouse-port/utxo-off.md). Set it once per
 * database. Ignored by the Postgres store.
 */
const clickhouseUtxo: 'off' | 'on' = clickhouseUtxoValue;

const eventLoopDiagnosticValue = Number(
  optionalString(optionalOptions.CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS, '0')
);
if (
  !Number.isInteger(eventLoopDiagnosticValue) ||
  eventLoopDiagnosticValue < 0
) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error(
    'The CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS environment variable must be an integer greater than or equal to 0.'
  );
}
/**
 * Set via the `CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS` environment variable
 * (default: `0`, off). If set, the agent logs the event-loop delay of its JS
 * thread (p50 / p99 / max over the window) every this many milliseconds, as
 * one `eventLoopDelay` line (docs/clickhouse-port/g1-fix-pass-2.md).
 */
const eventLoopDiagnosticMs = eventLoopDiagnosticValue;

/**
 * `true` if the `NODE_ENV` environment variable is `production`.
 */
const isProduction = configuration.NODE_ENV === 'production';

export {
  blockBufferTargetSizeMb,
  chaingraphInternalApiPort,
  chaingraphLogFirehose,
  chaingraphLogPath,
  chaingraphLogLevelStdout,
  chaingraphLogLevelPath,
  chaingraphStore,
  chaingraphUserAgent,
  clickhouseDatabase,
  clickhouseMaxInFlightSaves,
  clickhousePassword,
  clickhouseUrl,
  clickhouseUser,
  clickhouseUtxo,
  eventLoopDiagnosticMs,
  genesisBlocks,
  incompleteBlockRepairBatchSize,
  mempoolTransactionExpirationMs,
  mempoolTransactionExpirationScanIntervalMs,
  postgresMaxConnections,
  postgresConnectionString,
  postgresSynchronousCommit,
  isProduction,
  trustedNodes,
};
