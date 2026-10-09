// cspell:ignore clickhouse
/**
 * Build the `StoreChecker` for a backend: the e2e harness
 * (`CHAINGRAPH_E2E_STORE`) and the ingestion gate (`--store`) use this so
 * both read through the same implementation.
 */
import type { StoreChecker } from './checker.js';
import { createClickHouseChecker } from './clickhouse/checker.js';
import type { ClickHouseClient } from './clickhouse/client.js';
import type { PostgresQueryable } from './postgres/checker.js';
import { createPostgresChecker } from './postgres/checker.js';
import type { ChaingraphStoreBackend } from './types.js';

export type CheckerConnection =
  | { backend: 'clickhouse'; client: ClickHouseClient; database?: string }
  | { backend: 'postgres'; db: PostgresQueryable };

export const createChecker = (connection: CheckerConnection): StoreChecker =>
  connection.backend === 'clickhouse'
    ? createClickHouseChecker(
        connection.client,
        connection.database ?? connection.client.database
      )
    : createPostgresChecker(connection.db);

const backends: readonly ChaingraphStoreBackend[] = ['postgres', 'clickhouse'];

/**
 * Parse a backend name (`postgres` when empty or undefined); throws on
 * anything else.
 */
export const parseStoreBackend = (
  value: string | undefined,
  variable = 'CHAINGRAPH_E2E_STORE'
): ChaingraphStoreBackend => {
  if (value === undefined || value === '') {
    return 'postgres';
  }
  const backend = backends.find((name) => name === value);
  if (backend === undefined) {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(
      `Invalid ${variable}: ${value}. Must be one of: ${backends.join(', ')}.`
    );
  }
  return backend;
};
