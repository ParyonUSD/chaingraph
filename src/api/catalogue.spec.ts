/* eslint-disable functional/no-loop-statement, no-await-in-loop, @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-try-statement, no-negated-condition */
// cspell:ignore clickhouse chipnet
/**
 * `[e2e] [chipnet]`: every document of the v2 catalogue (L1–L47, X1–X16)
 * runs on the chipnet ClickHouse dataset and equals Chaingraph v1 (Hasura
 * over the Postgres reference) after normalisation (catalogue.ts). Opt-in:
 *
 *   CHAINGRAPH_API_CHIPNET_DATABASE=chipnet_ch_pp2 (on CHAINGRAPH_E2E_CLICKHOUSE_URL)
 *   CHAINGRAPH_API_V1_URL=http://localhost:18080/v1/graphql CHAINGRAPH_API_V1_ADMIN_SECRET=…
 *
 * Both databases are read only.
 */
import test from 'ava';

import { ClickHouseClient } from '../store/clickhouse/client.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import { loadCatalogue, runEntry } from './catalogue.js';
import { createApi } from './server.js';

const database = process.env.CHAINGRAPH_API_CHIPNET_DATABASE;
const v1Url = process.env.CHAINGRAPH_API_V1_URL;
const run =
  e2eClickHouseUrl === undefined ||
  database === undefined ||
  v1Url === undefined
    ? test.skip
    : test.serial;

run(
  '[e2e] [chipnet] v2 catalogue: every document runs and equals v1 (Hasura/Postgres) on chipnet',
  async (t) => {
    t.timeout(1_800_000);
    const client = ClickHouseClient.fromEnv({
      CHAINGRAPH_CLICKHOUSE_DATABASE: database,
      CHAINGRAPH_CLICKHOUSE_URL: e2eClickHouseUrl,
    });
    const api = createApi(client);
    const endpoint = {
      adminSecret: process.env.CHAINGRAPH_API_V1_ADMIN_SECRET ?? '',
      url: v1Url!,
    };
    try {
      for (const entry of loadCatalogue()) {
        const result = await runEntry(api, endpoint, entry);
        t.log(
          `${entry.id} ${entry.name}: ${result.status}, ${result.equality}`
        );
        if (entry.expect !== undefined) {
          t.true(
            result.equality.startsWith(entry.expect),
            `${entry.id}: expected ${entry.expect}, got ${result.equality}`
          );
        } else {
          t.is(result.status, 'works', `${entry.id}: ${result.error ?? ''}`);
          t.true(
            result.equality.startsWith('equal'),
            `${entry.id}: ${result.equality}`
          );
        }
      }
    } finally {
      await client.close();
    }
  }
);
