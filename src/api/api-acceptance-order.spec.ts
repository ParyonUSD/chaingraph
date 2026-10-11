/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-loop-statement, no-await-in-loop, functional/no-let, functional/no-throw-statement, functional/no-try-statement, @typescript-eslint/init-declarations, complexity */
// cspell:ignore clickhouse
/**
 * Unselective per-node roots (owner decision 2026-10-11, item 3):
 * `transactions(node:)` pages in internal-id order with no filter use the
 * optional projection p_node_tx when present (`Sql.acceptedInIdOrder`: a
 * window of acceptance rows read in id order, exact) and the id range on the
 * base acceptance rows otherwise. Both, and the window's fallback to the
 * whole range, must give exactly the pages (and `has_more`) of the
 * transactions the node accepts, in id order (the `*_v` views).
 */
import type { ExecutionContext } from 'ava';
import test from 'ava';

import { applyClickHouseDdl } from '../store/clickhouse/ddl-apply.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import type { Env } from './spec-env.js';
import { one, recordQueries, setup, two } from './spec-env.js';
import { acceptanceWindowRows } from './sql.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

interface Page {
  t: {
    has_more: boolean;
    end_cursor: string | null;
    nodes: { internal_id: string }[];
  };
}

/** Walk every page; returns the ids and checks has_more is false exactly on the last page. */
const walk = async (
  env: Env,
  node: string,
  {
    limit,
    order,
    window,
  }: { limit: number; order: 'ASC' | 'DESC'; window?: [number, number] }
) => {
  const ids: string[] = [];
  let cursor: string | null = null;
  for (let pages = 0; pages < 1_000; pages += 1) {
    const bound = order === 'ASC' ? 'after' : 'before';
    const args = [
      `node: "${node}"`,
      `limit: ${limit}`,
      `order: ${order}`,
      ...(window === undefined
        ? []
        : order === 'ASC'
        ? [`before: ${window[1]}`]
        : [`after: ${window[0]}`]),
      ...(cursor === null
        ? window === undefined
          ? []
          : [`${bound}: ${order === 'ASC' ? window[0] : window[1]}`]
        : [`${bound}: ${cursor}`]),
    ];
    const page: Page = await env.gql<Page>(
      `query { t: transactions(${args.join(
        ', '
      )}) { has_more end_cursor nodes { internal_id } } }`
    );
    ids.push(...page.t.nodes.map((row) => row.internal_id));
    if (!page.t.has_more) return ids;
    if (page.t.nodes.length !== limit) {
      throw new Error('a page with has_more must be full');
    }
    cursor = page.t.end_cursor;
  }
  throw new Error('too many pages');
};

const expectedIds = async (env: Env, node: number, order: 'ASC' | 'DESC') =>
  (
    await env.client.query<{ id: string }>(
      `SELECT toString(internal_id) AS id FROM transaction_v
       WHERE hash IN (SELECT transaction_hash FROM tx_acceptance_v(node = ${node}))
       ORDER BY internal_id ${order}`
    )
  ).map((row) => row.id);

const check = async (t: ExecutionContext, env: Env, label: string) => {
  for (const [name, id] of [
    [one, env.nodes.node1],
    [two, env.nodes.node2],
  ] as const) {
    for (const order of ['ASC', 'DESC'] as const) {
      const all = await expectedIds(env, id, order);
      t.true(all.length > 5, `${name} accepts several transactions`);
      for (const limit of [1, 2, 3, 7, 100]) {
        t.deepEqual(
          await walk(env, name, { limit, order }),
          all,
          `${label}: ${name} ${order} limit ${limit}`
        );
      }
      /* an id window (after and before) */
      const sorted = [...all].map(Number).sort((a, b) => a - b);
      const window: [number, number] = [sorted[1]!, sorted[sorted.length - 2]!];
      t.deepEqual(
        await walk(env, name, { limit: 2, order, window }),
        all.filter(
          (value) => Number(value) > window[0] && Number(value) < window[1]
        ),
        `${label}: ${name} ${order} window`
      );
    }
    /* offset on the first page */
    const page = await env.gql<Page>(
      `query { t: transactions(node: "${name}", limit: 2, offset: 3) { has_more nodes { internal_id } } }`
    );
    const all = await expectedIds(env, id, 'ASC');
    t.deepEqual(
      page.t.nodes.map((row) => row.internal_id),
      all.slice(3, 5),
      `${label}: ${name} offset`
    );
    t.is(page.t.has_more, all.length > 5);
  }
};

e2e(
  '[e2e] unselective transactions(node:) pages: with p_node_tx (window read in id order, its fallback) and without (id range) equal the accepted transactions in id order, has_more exact',
  async (t) => {
    t.timeout(300_000);
    const previous = process.env.CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL;
    process.env.CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL = 'p_node_tx';
    let env: Env;
    try {
      env = await setup(t, 'acc_order');
    } finally {
      if (previous === undefined) {
        delete process.env.CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL;
      } else {
        process.env.CHAINGRAPH_CLICKHOUSE_DDL_OPTIONAL = previous;
      }
    }
    /* churn: blocks on node-one confirm nothing new but add rows; a mempool transaction gets confirmed */
    await env.extendNodeOne([env.data.redeemB]);
    await env.extendNodeOne([]);

    t.deepEqual(
      await env.api.db.storeFeatures(),
      { acceptanceByIdProjection: true },
      'the projection is noticed'
    );
    const recorder = recordQueries(env.api);
    await env.gql(
      `query { transactions(node: "${one}", limit: 2) { has_more } }`
    );
    recorder.stop();
    t.true(
      recorder.events.some(
        (event) =>
          event.kind === 'data' &&
          event.sql.includes('ORDER BY node_internal_id ASC')
      ),
      'the window is read in (node, id) order'
    );
    await check(t, env, 'p_node_tx');
    for (const rows of [1, 2, 3, 5]) {
      env.api.db.acceptanceWindowRows = () => rows;
      await check(t, env, `p_node_tx, window of ${rows} raw rows (fallback)`);
    }
    env.api.db.acceptanceWindowRows = acceptanceWindowRows;
    env.api.db.forceFeatures = { acceptanceByIdProjection: false };
    await check(t, env, 'id range (no projection)');

    /* the DDL tool: the optional item is materialized once on a table with rows, never on a re-apply */
    const server = { url: e2eClickHouseUrl! };
    const database = (
      await env.client.query<{ d: string }>('SELECT currentDatabase() AS d')
    )[0]!.d;
    await env.client.command(
      'ALTER TABLE tx_acceptance DROP PROJECTION IF EXISTS p_node_tx'
    );
    const base = await applyClickHouseDdl(server, database, { optional: [] });
    t.is(
      await applyClickHouseDdl(server, database, { optional: ['p_node_tx'] }),
      base + 2,
      'added and materialized'
    );
    t.is(
      await applyClickHouseDdl(server, database, { optional: ['p_node_tx'] }),
      base + 1,
      'present: not materialized again'
    );
    await t.throwsAsync(
      applyClickHouseDdl(server, database, { optional: ['p_unknown'] }),
      { message: /Unknown optional DDL item/u }
    );
  }
);
