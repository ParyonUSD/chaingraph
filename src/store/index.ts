// cspell:ignore clickhouse
import {
  chaingraphStore,
  clickhouseDatabase,
  clickhousePassword,
  clickhouseUrl,
  clickhouseUser,
} from '../config.js';

import { createClickHouseStore } from './clickhouse/index.js';
import { createPostgresStore } from './postgres/postgres-store.js';
import type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

export type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

const clickhouseRequestTimeoutMs = 300_000;

export interface StoreConfig {
  backend: ChaingraphStoreBackend;
  clickhouse: {
    database: string;
    password: string;
    url: string;
    user: string;
  };
}

/**
 * The store configuration from the environment (`CHAINGRAPH_STORE` and
 * `CHAINGRAPH_CLICKHOUSE_*`, validated in `src/config.ts`).
 */
export const storeConfigFromEnvironment = (): StoreConfig => ({
  backend: chaingraphStore,
  clickhouse: {
    database: clickhouseDatabase,
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
    return createClickHouseStore({
      connection: {
        database: config.clickhouse.database,
        password: config.clickhouse.password,
        requestTimeoutMs: clickhouseRequestTimeoutMs,
        url: config.clickhouse.url,
        username: config.clickhouse.user,
      },
    });
  }
  return createPostgresStore();
};
