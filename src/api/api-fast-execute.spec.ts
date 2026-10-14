/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, no-await-in-loop, complexity, functional/no-let, no-continue, prefer-named-capture-group, @typescript-eslint/naming-convention */
// cspell:ignore clickhouse
/**
 * The fast executor (fast-execute.ts, fix-pass-9.md §4) answers what
 * graphql-js answers: every document below runs in `verify` mode (the
 * fast path, then Yoga's executor on the same arguments, compared), and
 * the fast path is actually taken. The whole API suite also runs in
 * verify mode (CHAINGRAPH_API_FAST_EXECUTION=verify) in each fix pass.
 */
import test from 'ava';
import type { GraphQLField } from 'graphql';

import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import { disabledAdmissionOptions } from './admission.js';
import {
  fastExecutionMismatches,
  fastExecutionStats,
  keyResolvers,
} from './fast-execute.js';
import { schema } from './schema.js';
import { createApi, listen } from './server.js';
import { one, setup, two } from './spec-env.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

test('fast executor: a one-parameter resolver can read only its parent (no rest or default parameters)', (t) => {
  let checked = 0;
  for (const type of Object.values(schema.getTypeMap())) {
    if (type.name.startsWith('__') || !('getFields' in type)) continue;
    const fields = (
      type as { getFields: () => { [name: string]: unknown } }
    ).getFields();
    for (const field of Object.values(fields)) {
      const { resolve } = field as GraphQLField<unknown, unknown>;
      if (resolve !== undefined && resolve.length === 1) {
        const text = String(resolve);
        const parameters = /^(?:async\s*)?\(?\s*([^)=]*?)\s*\)?\s*=>/u.exec(
          text
        );
        t.truthy(parameters, `${type.name}: ${text.slice(0, 60)}`);
        t.regex(
          parameters![1]!,
          /^[A-Za-z_$][\w$]*$/u,
          `${type.name}: one plain parameter`
        );
        checked += 1;
      }
      if (resolve !== undefined && keyResolvers.has(resolve)) checked += 1;
    }
  }
  t.true(checked > 100, `${checked} resolvers`);
});

const documents: [string, string][] = [
  [
    'nested relations, computed values, aliases',
    `query { t: transactions(node: "${one}", limit: 20) { has_more end_cursor nodes { hash internal_id fee_satoshis input_value_satoshis
      a: inputs { input_index outpoint { value_satoshis token_category nonfungible_token_capability } }
      b: inputs(limit: 1) { input_index }
      outputs(order_by: { value_satoshis: DESC }) { output_index value_satoshis locking_bytecode locking_bytecode_pattern spent_by { input_index transaction { hash } } }
      block_inclusions { transaction_index block { height hash timestamp } } } } }`,
  ],
  [
    'several roots, fragments, @include / @skip, __typename',
    `fragment O on Output { __typename transaction_hash output_index }
     query ($n: String!, $yes: Boolean!) {
       u: unspent_outputs(node: $n, limit: 5) { nodes { ...O } }
       tip: block_tip(node: $n) @include(if: $yes) { height __typename }
       nope: block_tip(node: $n) @skip(if: $yes) { height }
       w: transaction_watermark
       nodes { name }
     }`,
  ],
  [
    'per-node relationships and the second node',
    `query { blocks(node: "${two}", limit: 5) { nodes { height transactions { transaction_index transaction { hash accepted(node: "${two}") } } } } }`,
  ],
  [
    'mempool and node metadata',
    `query { mempool(node: "${one}", limit: 5) { nodes { transaction_hash validated_at transaction { hash encoded_hex } } } node(node: "${one}") { name } }`,
  ],
  [
    'an error (unknown node): graphql-js reports it',
    `query { blocks(node: "nobody", limit: 1) { nodes { height } } }`,
  ],
];

e2e(
  '[e2e] fast executor: every document answers what graphql-js answers (verify mode); the fast path is taken',
  async (t) => {
    t.timeout(120_000);
    const env = await setup(t, 'fast_execute');
    const api = createApi(env.client, {
      admission: disabledAdmissionOptions,
      fastExecution: 'verify',
    });
    const server = await listen(api);
    t.teardown(async () => server.close());
    const before = { ...fastExecutionStats };
    fastExecutionMismatches.splice(0);
    for (const [label, query] of documents) {
      const response = await fetch(server.url, {
        body: JSON.stringify({ query, variables: { n: one, yes: true } }),
        headers: { 'content-type': 'application/json' },
        method: 'POST',
      });
      const body = (await response.json()) as {
        data?: unknown;
        errors?: { message: string }[];
      };
      if (!label.startsWith('an error')) {
        t.is(
          body.errors,
          undefined,
          `${label}: ${JSON.stringify(body.errors)}`
        );
        t.truthy(body.data);
      }
      t.false(
        body.errors?.some((error) => error.message.includes('verify mode')) ??
          false,
        `${label}: ${JSON.stringify(fastExecutionMismatches.at(-1) ?? {})}`
      );
    }
    t.deepEqual(fastExecutionMismatches, []);
    t.true(
      fastExecutionStats.fast - before.fast >= documents.length - 1,
      `fast path taken: ${JSON.stringify(fastExecutionStats)}`
    );
    t.true(fastExecutionStats.gaveUp > before.gaveUp, 'the error gave up');
  }
);
