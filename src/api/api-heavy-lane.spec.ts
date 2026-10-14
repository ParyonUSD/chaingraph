/* eslint-disable @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention */
// cspell:ignore clickhouse
/**
 * The HTTP front path and the heavy lane (fix-pass-9.md §3, front.ts,
 * heavy-lane.ts): a POSTed query is decided before Yoga (cache hit,
 * refusal, light on the main thread, heavy on a worker thread); the
 * lane's answer is the main thread's, byte for byte, and its statements
 * reach the CPU ledger.
 */
import test from 'ava';
import { parse } from 'graphql';

import { connectionFor } from '../store/clickhouse/spec-fixtures.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import {
  defaultAdmissionOptions,
  overloadedCode,
  planKeys,
} from './admission.js';
import type { Api } from './server.js';
import { createApi, listen, operationNodeNames } from './server.js';
import { one, setup } from './spec-env.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const document = `query Lane($node: String!) { transactions(node: $node, limit: 50) { has_more nodes { hash input_count inputs { input_index outpoint_transaction_hash outpoint { value_satoshis token_category } } outputs { output_index value_satoshis locking_bytecode } } } }`;
const light = `query Light($node: String!) { block_tip(node: $node) { height hash } }`;

const post = async (
  url: string,
  query: string,
  headers: { [name: string]: string } = {}
) => {
  const response = await fetch(url, {
    body: JSON.stringify({ query, variables: { node: one } }),
    headers: { 'content-type': 'application/json', ...headers },
    method: 'POST',
  });
  return {
    body: await response.text(),
    contentType: response.headers.get('content-type'),
    retryAfter: response.headers.get('retry-after'),
    status: response.status,
  };
};

const keysOf = (query: string) => {
  const parsed = parse(query, { noLocation: true });
  const variables = { node: one };
  return planKeys(
    parsed,
    undefined,
    variables,
    operationNodeNames(parsed, undefined, variables)
  );
};

e2e(
  '[e2e] heavy lane: heavy requests answered on a worker thread equal the main thread answer; their statements reach the ledger; light ones stay on the main thread; refusals and cache hits are answered before Yoga',
  async (t) => {
    t.timeout(120_000);
    const env = await setup(t, 'heavy_lane');
    const database = (
      await env.client.query<{ d: string }>('SELECT currentDatabase() AS d')
    )[0]!.d;
    const admission = {
      ...defaultAdmissionOptions,
      cpuSource: 'rows' as const,
    };
    const withLane: Api = createApi(env.client, {
      admission,
      heavyLane: { clickhouse: connectionFor(database), threads: 1 },
      responseCache: {},
    });
    t.teardown(() => {
      withLane.db.admission.stop();
    });
    const server = await listen(withLane);
    t.teardown(async () => server.close());
    const lane = withLane.heavyLane!;
    t.is(lane.threads, 1);

    /* nothing known: heavy, on the lane; the same bytes as the main thread's answer */
    const reference = await post(env.server.url, document);
    t.is(reference.status, 200);
    const first = await post(server.url, document);
    t.is(first.status, 200);
    t.is(first.body, reference.body, 'byte-identical');
    t.is(first.contentType, 'application/json; charset=utf-8');
    t.is(lane.stats.served, 1);
    t.is(withLane.db.admission.stats.admitted.heavy, 1);
    t.true(
      withLane.db.admission.pendingStatements === 0 &&
        withLane.db.admission.stats.admitted.heavy === 1,
      'the ticket ended'
    );
    const documents = withLane.db.admission.model.documents();
    const [learned] = Object.values(documents);
    t.truthy(learned, 'the model learned the lane request');
    t.true(
      learned!.loopMs > 0,
      `lane loop time charged: ${learned!.loopMs} ms`
    );

    /* the lane answer was stored: the second request is a cache hit, not admitted */
    const second = await post(server.url, document);
    t.is(second.body, reference.body);
    t.is(withLane.responseCache!.stats.hits, 1);
    t.is(
      withLane.db.admission.stats.admitted.heavy,
      1,
      'a hit is not admitted'
    );
    t.is(lane.stats.served, 1);

    /* a light request runs on the main thread */
    withLane.db.admission.model.observe(keysOf(light), {
      cpuSeconds: 0.01,
      loopMs: 1,
    });
    const tip = await post(server.url, light);
    t.is(tip.status, 200);
    t.is(withLane.db.admission.stats.admitted.light, 1);
    t.is(lane.stats.served, 1, 'not on the lane');

    /* the graphql-response+json media type, as Yoga answers it */
    const typed = await post(server.url, light, {
      accept: 'application/graphql-response+json',
    });
    t.is(typed.contentType, 'application/graphql-response+json; charset=utf-8');

    /* refused before Yoga: a request in flight, the budgets spent */
    const blocker = withLane.db.admission.admit(undefined);
    withLane.db.admission.budgets.clickhouse.level.heavy = -100;
    withLane.db.admission.budgets.clickhouse.level.light = -100;
    const statementsBefore = withLane.db.stats.dataQueries;
    const started = performance.now();
    const refused = await post(
      server.url,
      document.replace('limit: 50', 'limit: 49')
    );
    const elapsed = performance.now() - started;
    t.is(refused.status, 503);
    t.truthy(refused.retryAfter);
    const body = JSON.parse(refused.body) as {
      errors: {
        extensions: { code: string; reason: string; http?: unknown };
      }[];
    };
    t.is(body.errors[0]!.extensions.code, overloadedCode);
    t.is(body.errors[0]!.extensions.reason, 'clickhouse');
    t.is(body.errors[0]!.extensions.http, undefined, 'no internals');
    t.true(elapsed < 250, `refused in ${Math.round(elapsed)} ms`);
    t.is(withLane.db.stats.dataQueries, statementsBefore, 'nothing sent');
    withLane.db.admission.finish(blocker);

    /* an invalid document: Yoga's own validation errors, as without the front path */
    const invalid = await post(server.url, 'query { nope }');
    const plain = await post(env.server.url, 'query { nope }');
    t.is(invalid.status, plain.status);
    t.is(invalid.body, plain.body);
  }
);
