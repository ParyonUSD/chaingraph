// cspell:ignore clickhouse
import {
  chaingraphStore,
  clickhouseDatabase,
  clickhouseMaxInFlightSaves,
  clickhousePassword,
  clickhouseUrl,
  clickhouseUser,
} from '../config.js';
import { instances } from '../logging.js';

import { createClickHouseStore } from './clickhouse/index.js';
import { createPostgresStore } from './postgres/postgres-store.js';
import type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

export type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

const clickhouseRequestTimeoutMs = 300_000;

/**
 * Optional `CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS`: how long a block
 * waits for outputs it spends before taking them as unknown (default 60 s;
 * the e2e mockchain spends outputs that never exist).
 */
const pendingSpendTimeoutFromEnvironment = () => {
  const raw = process.env.CHAINGRAPH_CLICKHOUSE_PENDING_SPEND_TIMEOUT_MS;
  const value = raw === undefined ? Number.NaN : Number(raw);
  return Number.isFinite(value) && value > 0 ? value : undefined;
};

export interface StoreConfig {
  backend: ChaingraphStoreBackend;
  /**
   * ClickHouse: on SIGINT/SIGTERM, abandon in-flight writes so the agent's
   * shutdown (which drains the block buffer before closing the store) never
   * waits for a block whose parent will not be downloaded any more. Set by
   * `storeConfigFromEnvironment` (the agent process) only.
   */
  abandonOnShutdownSignals?: boolean;
  clickhouse: {
    database: string;
    password: string;
    url: string;
    user: string;
    /** In-flight cap (0 or undefined: unbounded). */
    maxInFlightSaves?: number;
  };
}

/**
 * The store configuration from the environment (`CHAINGRAPH_STORE` and
 * `CHAINGRAPH_CLICKHOUSE_*`, validated in `src/config.ts`).
 */
export const storeConfigFromEnvironment = (): StoreConfig => ({
  abandonOnShutdownSignals: true,
  backend: chaingraphStore,
  clickhouse: {
    database: clickhouseDatabase,
    maxInFlightSaves: clickhouseMaxInFlightSaves,
    password: clickhousePassword,
    url: clickhouseUrl,
    user: clickhouseUser,
  },
});

/**
 * Create the `ChaingraphStore` selected by `CHAINGRAPH_STORE` (default
 * `postgres`). Call `init()` on the result before use.
 */
export const createStore = (
  config: StoreConfig = storeConfigFromEnvironment()
): ChaingraphStore => {
  if (config.backend === 'clickhouse') {
    const store = createClickHouseStore({
      connection: {
        database: config.clickhouse.database,
        password: config.clickhouse.password,
        requestTimeoutMs: clickhouseRequestTimeoutMs,
        url: config.clickhouse.url,
        username: config.clickhouse.user,
      },
      maxInFlightSaves: config.clickhouse.maxInFlightSaves,
      onError: (error) => {
        instances.logger?.error(
          error,
          'ClickHouse store: background error (watermark publishing, writer lease or a parked block commit).'
        );
      },
      pendingSpendTimeoutMs: pendingSpendTimeoutFromEnvironment(),
    });
    if (config.abandonOnShutdownSignals === true) {
      ['SIGINT', 'SIGTERM'].forEach((signal) => {
        process.on(signal, () => {
          store.abandonInFlightWork(`received ${signal}`);
        });
      });
    }
    return store;
  }
  return createPostgresStore();
};
