/* eslint-disable @typescript-eslint/naming-convention */
// cspell:ignore clickhouse
import test from 'ava';

import { defaultStatementTimeouts, statementTimeoutsFromEnv } from './db.js';

test('statementTimeoutsFromEnv: defaults, overrides, aggregate at most the data budget unless set', (t) => {
  t.deepEqual(statementTimeoutsFromEnv({}), defaultStatementTimeouts);
  t.deepEqual(
    statementTimeoutsFromEnv({ CHAINGRAPH_API_QUERY_TIMEOUT_MS: '10000' }),
    { aggregate: 10_000, data: 10_000 }
  );
  t.deepEqual(
    statementTimeoutsFromEnv({
      CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS: '120000',
      CHAINGRAPH_API_QUERY_TIMEOUT_MS: '10000',
    }),
    { aggregate: 120_000, data: 10_000 }
  );
  t.throws(
    () => statementTimeoutsFromEnv({ CHAINGRAPH_API_QUERY_TIMEOUT_MS: '0' }),
    { message: /CHAINGRAPH_API_QUERY_TIMEOUT_MS must be a positive number/u }
  );
  t.throws(
    () =>
      statementTimeoutsFromEnv({ CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS: 'x' }),
    { message: /CHAINGRAPH_API_AGGREGATE_TIMEOUT_MS/u }
  );
});
