// cspell:ignore clickhouse
/**
 * The ClickHouse backend of `ChaingraphStore` (WP4 + WP5a).
 */
export type { ClickHouseStoreOptions } from './clickhouse-store.js';
export {
  ClickHouseStore,
  createClickHouseStore,
  StoreClosedError,
} from './clickhouse-store.js';
