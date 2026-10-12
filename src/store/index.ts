// cspell:ignore clickhouse
import {
  chaingraphStore,
  clickhouseDatabase,
  clickhouseMaxInFlightSaves,
  clickhousePassword,
  clickhouseSpendIndex,
  clickhouseUrl,
  clickhouseUser,
  clickhouseUtxo,
  clickhouseUtxoSet,
  clickhouseUtxoSetSpotCheckMs,
} from '../config.js';
import { instances } from '../logging.js';

import { createClickHouseStore } from './clickhouse/index.js';
import { createPostgresStore } from './postgres/postgres-store.js';
import type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

export type { ChaingraphStore, ChaingraphStoreBackend } from './types.js';

const clickhouseRequestTimeoutMs = 300_000;

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
    /** Stored UTXO tables (`CHAINGRAPH_CLICKHOUSE_UTXO`; default `on`). */
    utxo?: 'off' | 'on';
    /** The spend index (`CHAINGRAPH_CLICKHOUSE_SPEND_INDEX`; default `on`). */
    spendIndex?: 'off' | 'on';
    /** The stored UTXO set (`CHAINGRAPH_CLICKHOUSE_UTXO_SET`; default `on`). */
    utxoSet?: 'off' | 'on';
    /** Its spot-check interval (`CHAINGRAPH_CLICKHOUSE_UTXO_SET_SPOT_CHECK_MS`; default 1 h, 0 = off). */
    utxoSetSpotCheckMs?: number;
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
    spendIndex: clickhouseSpendIndex,
    url: clickhouseUrl,
    user: clickhouseUser,
    utxo: clickhouseUtxo,
    utxoSet: clickhouseUtxoSet,
    utxoSetSpotCheckMs: clickhouseUtxoSetSpotCheckMs,
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
      // eslint-disable-next-line complexity
      onDiagnostic: (diagnostic) => {
        /*
         * One structured line per void, refused void, failed abort and
         * failed block batch (g1-fix-pass.md): the lab greps
         * `clickhouseDiagnostic.event`.
         */
        const { logger } = instances;
        if (logger === undefined) return;
        const message = `ClickHouse store: ${diagnostic.event}.`;
        if (
          diagnostic.event === 'spend_index_progress' ||
          diagnostic.event === 'utxo_set_progress'
        ) {
          // the spend index's / UTXO set's watermark and lag, every ~10 s
          logger.info({ clickhouseDiagnostic: diagnostic }, message);
        } else if (diagnostic.event === 'utxo_set_spot_check') {
          logger.info({ clickhouseDiagnostic: diagnostic }, message);
        } else if (diagnostic.event === 'utxo_set_spot_check_mismatch') {
          // the stored UTXO set differs from the query-time answer: a data discrepancy, loud
          logger.error(
            { clickhouseDiagnostic: diagnostic },
            `ClickHouse store: UTXO set spot-check MISMATCH on node ${diagnostic.node}, ${diagnostic.scope} ${diagnostic.value}: stored ${diagnostic.stored} vs query time ${diagnostic.reference}.`
          );
        } else if (diagnostic.event === 'utxo_set_invariant_violation') {
          // a data discrepancy in the stored UTXO set (utxo-async.md): loud
          logger.fatal({ clickhouseDiagnostic: diagnostic }, message);
        } else if (diagnostic.event === 'void_refused') {
          logger.fatal({ clickhouseDiagnostic: diagnostic }, message);
        } else if (
          diagnostic.event === 'block_batch_failed' ||
          diagnostic.event === 'abort_failed'
        ) {
          logger.error({ clickhouseDiagnostic: diagnostic }, message);
        } else {
          logger.warn({ clickhouseDiagnostic: diagnostic }, message);
        }
      },
      onError: (error) => {
        instances.logger?.error(
          error,
          'ClickHouse store: background error (watermark publishing or writer lease).'
        );
      },
      spendIndex: config.clickhouse.spendIndex,
      utxo: config.clickhouse.utxo,
      utxoSet: config.clickhouse.utxoSet,
      ...(config.clickhouse.utxoSetSpotCheckMs === undefined
        ? {}
        : {
            utxoSetOptions: {
              spotCheckIntervalMs: config.clickhouse.utxoSetSpotCheckMs,
            },
          }),
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
