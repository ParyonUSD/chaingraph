/* eslint-disable functional/no-loop-statement, no-await-in-loop, @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-try-statement, no-negated-condition, camelcase, prefer-named-capture-group, @typescript-eslint/require-array-sort-compare */
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
import { createHash } from 'node:crypto';

import test from 'ava';

import { ClickHouseClient } from '../store/clickhouse/client.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import {
  documentText,
  fetchV2,
  loadCatalogue,
  runEntry,
  walkV1,
} from './catalogue.js';
import { dataQuerySettings } from './db.js';
import { createApi } from './server.js';
import { spendRelationSql, spendRowPredicate, Sql } from './sql.js';

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

run(
  '[e2e] [chipnet] R13 + R14: byte-prefix filters and locking-bytecode sets read the sort-key projections (p_script, p_category, spend key)',
  async (t) => {
    const client = ClickHouseClient.fromEnv({
      CHAINGRAPH_CLICKHOUSE_DATABASE: database,
      CHAINGRAPH_CLICKHOUSE_URL: e2eClickHouseUrl,
    });
    const api = createApi(client);
    const statements: { sql: string; params: { [name: string]: unknown } }[] =
      [];
    api.db.onQuery = (event) => {
      if (event.kind === 'data') statements.push(event);
    };
    const explain = async (query: string, variables: object) => {
      statements.length = 0;
      const response = await api.yoga.fetch('http://localhost/graphql', {
        body: JSON.stringify({ query, variables }),
        headers: { 'content-type': 'application/json' },
        method: 'POST',
      });
      const body = (await response.json()) as { errors?: unknown };
      t.is(body.errors, undefined);
      const [statement] = statements;
      const rows = await client.query<{ explain: string }>(
        `EXPLAIN indexes = 1 ${statement!.sql}`,
        statement!.params,
        dataQuerySettings
      );
      return rows.map((row) => row.explain).join('\n');
    };
    try {
      const prefix = await explain(
        `query($node: String!, $p: Hex!) { outputs(node: $node, limit: 10, where: { locking_bytecode: { _prefix: $p } }) { nodes { output_index } } }`,
        { node: 'chipnet-local', p: '6a142c7c8066' }
      );
      t.regex(
        prefix,
        /ReadFromMergeTree \(p_script\)/u,
        'locking bytecode prefix'
      );
      const long = await explain(
        `query($node: String!, $p: Hex!) { outputs(node: $node, limit: 10, where: { locking_bytecode: { _prefix: $p } }) { nodes { output_index } } }`,
        {
          node: 'chipnet-local',
          p: '76a914001b9e8d2a5e4f75f5a58cad60f33c56da7a1b8c88',
        }
      );
      t.regex(
        long,
        /ReadFromMergeTree \(p_script\)/u,
        'prefix longer than 25 bytes'
      );
      const commitment = await explain(
        `query($node: String!, $c: Hex!, $p: Hex!) { outputs(node: $node, limit: 10, where: { token_category: { _eq: $c }, nonfungible_token_commitment: { _prefix: $p } }) { nodes { output_index } } }`,
        {
          c: 'a2d3711c910fd83d37e2983f5dd876b7b62d371d6740733a3804b2d4d6ab6916',
          node: 'chipnet-local',
          p: '8ee26d6c9f58369f94864dc3630cdeb17fae2f2d',
        }
      );
      t.regex(
        commitment,
        /ReadFromMergeTree \(p_category\)/u,
        'commitment prefix'
      );
      // the spend relation of a spent-output set (the read rule's index part)
      const snapshot = await api.db.readSnapshot(
        await api.db.nodeId('chipnet-local')
      );
      const sql = new Sql(snapshot);
      const relation = `SELECT transaction_hash FROM ${spendRelationSql(
        sql,
        spendRowPredicate(sql, {
          outpoint: {
            locking_bytecode: {
              _in: [
                '76a914001b9e8d2a5e4f75f5a58cad60f33c56da7a1b8c88ac',
                '76a914004e71214db4a32e208be29ba0e17484d7ecfe9888ac',
              ],
            },
          },
        })
      )}`;
      const spent = (
        await client.query<{ explain: string }>(
          `EXPLAIN indexes = 1 ${relation}`,
          sql.params,
          dataQuerySettings
        )
      )
        .map((row) => row.explain)
        .join('\n');
      t.regex(
        spent,
        /Keys:\s*\n\s*locking_bytecode_prefix[\s\S]*?Granules: \d+\/\d+/u,
        'spent-output set by the spend index key'
      );
      const granules = /PrimaryKey[\s\S]*?Granules: (\d+)\/(\d+)/u.exec(spent)!;
      t.true(Number(granules[1]) * 100 < Number(granules[2]), granules[0]);
    } finally {
      await client.close();
    }
  }
);

run(
  '[e2e] [chipnet] R25 + R28: holder balances by group-by equal the client-side aggregation of v1 AllHolders; encodings hash to their transaction and block hashes',
  async (t) => {
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
      const entry = loadCatalogue().find((item) => item.id === 'L12')!;
      const v1 = await walkV1(endpoint, entry, documentText(entry.v1.file));
      const balances = new Map<string, bigint>();
      (
        v1.data.output as {
          locking_bytecode: string;
          fungible_token_amount: string;
        }[]
      ).forEach((row) => {
        const key = row.locking_bytecode.replace('\\x', '');
        balances.set(
          key,
          (balances.get(key) ?? 0n) + BigInt(row.fungible_token_amount)
        );
      });
      const v2 = await fetchV2(
        api,
        `query($node: String!, $paryon: Hex!) {
          output_groups(node: $node, unspent: true, limit: 1000, group_by: [LOCKING_BYTECODE],
            where: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _is_null: true }, locking_bytecode: { _neq: "6a" } }) {
            has_more nodes { key { locking_bytecode } aggregate { sum { fungible_token_amount } } }
          }
        }`,
        { node: 'chipnet-local', paryon: entry.v2.variables.paryonTokenId }
      );
      const page = v2.data!.output_groups as {
        has_more: boolean;
        nodes: {
          key: { locking_bytecode: string };
          aggregate: { sum: { fungible_token_amount: string } };
        }[];
      };
      t.false(page.has_more);
      t.deepEqual(
        page.nodes
          .map(
            (group) =>
              `${group.key.locking_bytecode}=${group.aggregate.sum.fungible_token_amount}`
          )
          .sort(),
        [...balances].map(([key, sum]) => `${key}=${sum}`).sort()
      );
      t.true(balances.size > 10, `${balances.size} holders`);

      // R28: encoded_hex → sha256d = hash; header → block hash (X6, X7 shapes)
      const encoded = await fetchV2(
        api,
        `query($node: String!) {
          transactions(node: $node, limit: 20, order: DESC) { nodes { hash encoded_hex } }
          blocks(node: $node, limit: 3, order_by: { height: DESC }) { nodes { hash header transaction_count encoded_hex } }
        }`,
        { node: 'chipnet-local' }
      );
      const sha256d = (hex: string) =>
        createHash('sha256')
          .update(createHash('sha256').update(Buffer.from(hex, 'hex')).digest())
          .digest()
          .reverse()
          .toString('hex');
      const { transactions, blocks } = encoded.data as {
        transactions: { nodes: { hash: string; encoded_hex: string }[] };
        blocks: {
          nodes: { hash: string; header: string; encoded_hex: string }[];
        };
      };
      transactions.nodes.forEach((tx) => {
        t.is(sha256d(tx.encoded_hex), tx.hash);
      });
      blocks.nodes.forEach((block) => {
        t.is(sha256d(block.header), block.hash);
        t.true(block.encoded_hex.startsWith(block.header));
      });
    } finally {
      await client.close();
    }
  }
);
