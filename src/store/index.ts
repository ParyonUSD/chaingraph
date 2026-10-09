// cspell:ignore clickhouse
import {
  chaingraphStore,
  clickhouseDatabase,
  clickhousePassword,
  clickhouseUrl,
  clickhouseUser,
} from '../config.js';

import { createPostgresStore } from './postgres/postgres-store.js';
import type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

export type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

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
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      'CHAINGRAPH_STORE=clickhouse is not implemented yet (WP5).'
    );
  }
  return createPostgresStore();
};
