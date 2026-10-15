/* eslint-disable camelcase, @typescript-eslint/naming-convention, @typescript-eslint/no-magic-numbers, @typescript-eslint/require-array-sort-compare, prefer-named-capture-group */
// cspell:ignore clickhouse
import test from 'ava';
import { parse } from 'graphql';

import type { VisibilitySnapshot } from '../store/clickhouse/visibility.js';

import { operationNodeNames } from './server.js';
import {
  candidateWindowRows,
  chainKeySetMax,
  isClientParamName,
  outputPredicate,
  snapshotParameterNames,
  Sql,
  transactionCandidates,
  transactionConjuncts,
  transactionPredicate,
  unspentOutputRows,
} from './sql.js';

const snapshot: VisibilitySnapshot = {
  committedTail: [7n],
  fence: [1_099_511_627_775n],
  nodeId: 3,
  spend: 4n,
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
    unspentOutputRows(sql, {
      nonfungible_token_capability: { _eq: 'mutable' },
      token_category: { _eq: 'aa'.repeat(32) },
    }).where,
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
    [...snapshotParameterNames]
      .filter((name) => !name.startsWith('utxo'))
      .sort(),
    'no stored UTXO set mark: no utxo parameters'
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

test('stored UTXO set: the mark is bound as snapshot parameters; client values stay pN parameters; the plan follows the policy and the lag', (t) => {
  const marked: VisibilitySnapshot = {
    ...snapshot,
    utxo: { generation: 2, lagCommits: 1, lagRows: 10, watermark: 4n },
  };
  const sql = new Sql(marked);
  const rows = unspentOutputRows(sql, {
    locking_bytecode: { _eq: hostileHex },
    token_category: { _eq: 'aa'.repeat(32) },
  });
  const text = `${sql.withClause()}${rows.source}`;
  t.true(text.includes('FROM utxo_set'), 'the stored set');
  t.true(text.includes('utxo_touched'), 'with the correction (lag 1)');
  t.false(text.includes(hostileHex));
  t.false(text.includes('aa'.repeat(32)));
  t.deepEqual(
    Object.keys(sql.params)
      .filter((name) => !isClientParamName(name))
      .sort(),
    [...snapshotParameterNames].sort()
  );
  t.is(sql.params.utxo, 4n);
  t.is(sql.params.utxo_generation, 2);
  const placeholders = [...text.matchAll(/\{(\w+):/gu)].map(
    (match) => match[1]!
  );
  t.true(placeholders.every((name) => name in sql.params));
  /* caught up: no correction */
  const caughtUp = new Sql({
    ...snapshot,
    utxo: { generation: 2, lagCommits: 0, lagRows: 0, watermark: 5n },
  });
  t.false(
    `${caughtUp.withClause()}${
      unspentOutputRows(caughtUp, {}).source
    }`.includes('utxo_touched')
  );
  /* above the plan bound, the policy off, or another node: query time */
  [
    new Sql(marked, new Map(), { enabled: true, maxLagRows: 9 }),
    new Sql(marked, new Map(), { enabled: false, maxLagRows: 100_000 }),
  ].forEach((other) => {
    t.false(unspentOutputRows(other, {}).source.includes('utxo_set'));
  });
  t.false(sql.storedUtxoSet(4), 'only the primary node has a mark');
  t.true(sql.storedUtxoSet(3));
});

test('commitment equality and _in add p_category key conditions (planning-floor.md); negated filters do not', (t) => {
  const sql = new Sql(snapshot);
  const eq = outputPredicate(sql, {
    nonfungible_token_commitment: { _eq: 'abcd' },
  });
  t.regex(eq, /nonfungible_token_commitment = unhex\(\{p\d+:String\}\)/u);
  t.regex(eq, /nonfungible_token_commitment_key = unhex\(\{p\d+:String\}\)/u);
  const list = outputPredicate(sql, {
    nonfungible_token_commitment: { _in: ['ab', 'cd'] },
  });
  t.regex(list, /nonfungible_token_commitment_key IN \(SELECT unhex\(x\)/u);
  const negated = outputPredicate(sql, {
    _not: { nonfungible_token_commitment: { _eq: 'abcd' } },
  });
  t.false(negated.includes('nonfungible_token_commitment_key'));
});

test('hash-typed comparisons reject values that are not 32 bytes', (t) => {
  const sql = new Sql(snapshot);
  t.throws(() => unspentOutputRows(sql, { token_category: { _eq: 'aa' } }));
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

test('candidate window: wider when the filter has conjuncts beyond its candidates (fix-pass-7.md §4)', (t) => {
  t.is(candidateWindowRows(10_001), 12_602);
  t.is(candidateWindowRows(10_001, true), 16 * 12_602);
  t.is(candidateWindowRows(2), 103);
  const hasInput = { outpoint: { token_category: { _eq: 'aa' } } };
  t.is(transactionConjuncts({ has_input: hasInput }), 1, 'L35 / L40');
  t.is(
    transactionConjuncts({
      _or: [
        { has_input: hasInput },
        { has_output: { token_category: { _eq: 'aa' } } },
      ],
    }),
    1,
    'L33'
  );
  t.is(
    transactionConjuncts({
      _not: { has_output: { locking_bytecode: { _eq: 'aa' } } },
      has_input: hasInput,
    }),
    2,
    'L23 / L28'
  );
  t.is(
    transactionConjuncts({
      _and: [{ _not: { has_input: hasInput }, has_input: hasInput }],
      has_input: hasInput,
    }),
    3,
    'L21: _and parts count'
  );
  t.is(transactionConjuncts({ has_input: null }), 0);
  t.is(transactionConjuncts(undefined), 0);
});

test('candidate chain: a later set takes the key on earlier candidates only up to chainKeySetMax (fix-pass-12.md §1)', (t) => {
  const category = 'aa'.repeat(32);
  const l27 = (scripts: string[]) => ({
    _and: [
      {
        has_output: {
          nonfungible_token_capability: { _eq: 'minting' as const },
          token_category: { _eq: category },
        },
      },
    ],
    has_output: { locking_bytecode: { _in: scripts } },
  });
  const short = transactionCandidates(new Sql(snapshot), l27(['76a914']))!;
  t.is(
    short.match(/use_index_for_in_with_subqueries_max_values = (\d+)/gu)
      ?.length,
    1,
    'one link, one limit'
  );
  t.true(short.includes(`max_values = ${chainKeySetMax}`));
  t.false(short.includes('identity('));
  /* a client list longer than the limit would lose its own key read under the setting: the counted gate */
  const long = transactionCandidates(
    new Sql(snapshot),
    l27(
      Array.from({ length: chainKeySetMax + 1 }, (_, index) =>
        index.toString(16).padStart(4, '0')
      )
    )
  )!;
  t.false(long.includes('use_index_for_in_with_subqueries_max_values'));
  t.regex(
    long,
    new RegExp(
      `if\\(\\(SELECT count\\(\\) FROM \\(SELECT 1 FROM \\([^]*LIMIT ${
        chainKeySetMax + 1
      }\\)\\) <= ${chainKeySetMax}, transaction_hash IN `,
      'u'
    )
  );
  t.true(
    long.includes('identity(transaction_hash) IN (SELECT transaction_hash')
  );
  /* one set: no chain */
  const single = transactionCandidates(new Sql(snapshot), {
    has_output: { locking_bytecode: { _in: ['76a914'] } },
  })!;
  t.false(single.includes('max_values'));
});
