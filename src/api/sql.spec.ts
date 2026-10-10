/* eslint-disable camelcase, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, @typescript-eslint/require-array-sort-compare, prefer-named-capture-group */
// cspell:ignore clickhouse
import test from 'ava';
import { parse } from 'graphql';

import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';

import { operationNodeNames } from './server.js';
import {
  isClientParamName,
  snapshotParameterNames,
  Sql,
  transactionPredicate,
  unspentOutputPredicate,
} from './sql.js';

const snapshot: VisibilitySnapshot = {
  committedTail: [7n],
  fence: [1_099_511_627_775n],
  nodeId: 3,
  visible: 5n,
  visible0: 6n,
  void: [],
  voidOverflow: false,
};

const hostile = `'); DROP TABLE output; --{visible:UInt64}`;
const hostileHex = Buffer.from(hostile).toString('hex');

test('client values are bound as pN parameters, never in SQL text; snapshot parameters are untouched', (t) => {
  const sql = new Sql(snapshot);
  const text = [
    transactionPredicate(sql, {
      _not: { has_output: { locking_bytecode: { _eq: hostileHex } } },
      _or: [
        {
          has_input: {
            input_index: { _eq: 4 },
            outpoint: { nonfungible_token_commitment: { _in: [hostileHex] } },
          },
        },
        { block_window: { from_height: 1, to_height: 9 } },
      ],
      internal_id: { _gt: 10n },
    }),
    unspentOutputPredicate(sql, {
      nonfungible_token_capability: { _eq: 'mutable' },
      token_category: { _eq: 'aa'.repeat(32) },
    }),
  ].join('\n');
  t.false(text.includes(hostileHex));
  t.false(text.includes('DROP'));
  t.false(text.includes('aa'.repeat(32)));
  const clientParams = Object.keys(sql.params).filter(
    (name) => !(snapshotParameterNames as readonly string[]).includes(name)
  );
  t.true(clientParams.length >= 7);
  t.true(clientParams.every(isClientParamName));
  t.deepEqual(
    Object.keys(sql.params)
      .filter((name) => !isClientParamName(name))
      .sort(),
    [...snapshotParameterNames].sort()
  );
  t.is(sql.params.visible, 5n);
  t.is(sql.params.node, 3);
  t.deepEqual(sql.params.tail, [7n]);
  // every placeholder in the text is either a snapshot parameter or a bound client value
  const placeholders = [...text.matchAll(/\{(\w+):/gu)].map(
    (match) => match[1]!
  );
  t.true(placeholders.every((name) => name in sql.params));
});

test('hash-typed comparisons reject values that are not 32 bytes', (t) => {
  const sql = new Sql(snapshot);
  t.throws(() =>
    unspentOutputPredicate(sql, { token_category: { _eq: 'aa' } })
  );
});

test('operationNodeNames reads root node arguments from literals, variables and fragments', (t) => {
  const document = parse(`
    query Q($n: String!) {
      a: transactions(node: $n, limit: 1) { has_more }
      ...F
      w: transaction_watermark
    }
    fragment F on Query { b: unspent_outputs(node: "node-two", limit: 1) { has_more } }
  `);
  t.deepEqual(operationNodeNames(document, 'Q', { n: 'node-one' }).sort(), [
    'node-one',
    'node-two',
  ]);
  t.deepEqual(
    operationNodeNames(parse('{ transaction_watermark }'), undefined, {}),
    []
  );
});
