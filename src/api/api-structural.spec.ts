/* eslint-disable functional/no-throw-statement, @typescript-eslint/no-magic-numbers, @typescript-eslint/naming-convention, functional/no-loop-statement, no-await-in-loop, @typescript-eslint/require-array-sort-compare */
// cspell:ignore clickhouse authchain authchains
/**
 * Structural facts vs acceptance (owner decision 2026-10-11, item 1):
 * `block_inclusions`, `spent_by` and `authchain_branches` are node-agnostic
 * and complete (every stored block with the transaction, stale and fork
 * blocks included; every stored spending input, conflicting spends
 * included); acceptance is opt-in and per node (`accepted_by: node`,
 * `accepted(node:)`), and acceptance-scoped roots keep their node.
 *
 * Fixture (all written through the ClickHouse store): blocks 0-1 accepted
 * by both nodes; a fork at height 2 (2a: node-one, 2b: node-two) where both
 * blocks include `shared` and spend coin 2 differently (x in 2a, y in 2b);
 * block 3s on 2a, accepted by node-one and then released (a stale block no
 * node accepts) with z spending coin 4; conflicting mempool spends of
 * shared's output 0 (m1 in node-one's mempool, m2 in node-two's). The
 * expectations are written from the fixture itself.
 */
import test from 'ava';

import {
  acceptance,
  makeBlock,
  makeTx,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  zeroHash,
} from '../store/clickhouse/spec-fixtures.js';
import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import { createApi, listen } from './server.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const coin = (label: string) => ({
  lockingBytecode: p2pkh(label),
  valueSatoshis: 1_000n,
});

e2e(
  '[e2e] structural facts: block_inclusions, spent_by and authchain_branches are complete (fork and stale blocks, conflicting spends); acceptance is opt-in and exact per node',
  async (t) => {
    t.timeout(120_000);
    const { client, openStore } = await scratch(
      t,
      'structural',
      { utxo: 'off' },
      'ch1_api'
    );
    const store = await openStore();
    const { node1, node2 } = await registerNodes(store);
    const c0 = makeTx({
      coinbase: true,
      label: 'st-c0',
      outputs: [0, 1, 2, 3, 4].map((index) => coin(`st-c0-${index}`)),
    });
    const coinbase = (label: string) =>
      makeTx({ coinbase: true, label, outputs: [coin(label)] });
    const spend = (label: string, spends: [string, number][]) =>
      makeTx({
        label,
        outputs: [coin(`${label}-0`), coin(`${label}-1`)],
        spends,
      });
    const t1 = spend('st-t1', [[c0.hash, 1]]);
    const shared = spend('st-shared', [[c0.hash, 3]]);
    const x = spend('st-x', [[c0.hash, 2]]);
    const y = spend('st-y', [[c0.hash, 2]]);
    const z = spend('st-z', [[c0.hash, 4]]);
    const m1 = spend('st-m1', [[shared.hash, 0]]);
    const m2 = spend('st-m2', [[shared.hash, 0]]);
    const block0 = makeBlock(0, zeroHash, [c0], 'st-0');
    const block1 = makeBlock(1, block0.hash, [coinbase('st-c1'), t1], 'st-1');
    const block2a = makeBlock(
      2,
      block1.hash,
      [coinbase('st-c2a'), shared, x],
      'st-2a'
    );
    const block2b = makeBlock(
      2,
      block1.hash,
      [coinbase('st-c2b'), shared, y],
      'st-2b'
    );
    const block3s = makeBlock(
      3,
      block2a.hash,
      [coinbase('st-c3s'), z],
      'st-3s'
    );
    const both = [acceptance(node1), acceptance(node2)];
    for (const [block, nodes] of [
      [block0, both],
      [block1, both],
      [block2a, [acceptance(node1)]],
      [block2b, [acceptance(node2)]],
      [block3s, [acceptance(node1)]],
    ] as const) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [...nodes],
      });
    }
    await store.removeStaleBlocksForNode(node1, [block3s.hash]);
    const at = new Date('2026-10-11T00:00:00Z');
    await store.saveMempoolTransaction(m1, [
      { nodeInternalId: node1, validatedAt: at },
    ]);
    await store.saveMempoolTransaction(m2, [
      { nodeInternalId: node2, validatedAt: at },
    ]);
    await store.publishWatermarks();
    await store.catchUpSpendIndex();
    await store.publishWatermarks();

    const api = createApi(client);
    const server = await listen(api);
    t.teardown(async () => server.close());
    const gql = async <T>(
      query: string,
      variables: { [key: string]: unknown } = {}
    ) => {
      const response = await fetch(server.url, {
        body: JSON.stringify({ query, variables }),
        headers: { 'content-type': 'application/json' },
        method: 'POST',
      });
      const body = (await response.json()) as { data?: T; errors?: unknown };
      if (body.errors !== undefined) {
        throw new Error(JSON.stringify(body.errors));
      }
      return body.data!;
    };

    /* ---- block_inclusions ---- */
    interface Inclusion {
      block: { hash: string; height: number };
      one: boolean;
      two: boolean;
    }
    const inclusions = await gql<{
      transactions_by_hash: {
        hash: string;
        all: Inclusion[];
        on_one: { block: { hash: string } }[];
        on_two: { block: { hash: string } }[];
        accepted_one: boolean;
        accepted_two: boolean;
      }[];
    }>(
      `query($hashes: [Hex!]!) {
        transactions_by_hash(hashes: $hashes) {
          hash
          all: block_inclusions { block { hash height } one: accepted(node: "node-one") two: accepted(node: "node-two") }
          on_one: block_inclusions(accepted_by: "node-one") { block { hash } }
          on_two: block_inclusions(accepted_by: "node-two") { block { hash } }
          accepted_one: accepted(node: "node-one")
          accepted_two: accepted(node: "node-two")
        }
      }`,
      { hashes: [c0, t1, shared, x, y, z, m1].map((tx) => tx.hash) }
    );
    const byHash = new Map(
      inclusions.transactions_by_hash.map((row) => [row.hash, row])
    );
    const expected: [
      string,
      [string, number, boolean, boolean][],
      boolean,
      boolean
    ][] = [
      ['c0', [[block0.hash, 0, true, true]], true, true],
      ['t1', [[block1.hash, 1, true, true]], true, true],
      [
        'shared',
        [
          [block2a.hash, 2, true, false],
          [block2b.hash, 2, false, true],
        ],
        true,
        true,
      ],
      ['x', [[block2a.hash, 2, true, false]], true, false],
      ['y', [[block2b.hash, 2, false, true]], false, true],
      ['z', [[block3s.hash, 3, false, false]], false, false],
      ['m1', [], true, false],
    ];
    const txOf = { c0, m1, shared, t1, x, y, z } as const;
    for (const [name, blocks, one, two] of expected) {
      const row = byHash.get(txOf[name as keyof typeof txOf].hash)!;
      t.deepEqual(
        row.all.map((inclusion) => [
          inclusion.block.hash,
          inclusion.block.height,
          inclusion.one,
          inclusion.two,
        ]),
        blocks,
        `${name}: every stored block, with per-node acceptance`
      );
      t.deepEqual(
        row.on_one.map((inclusion) => inclusion.block.hash),
        blocks.filter((block) => block[2]).map((block) => block[0]),
        `${name}: accepted_by node-one`
      );
      t.deepEqual(
        row.on_two.map((inclusion) => inclusion.block.hash),
        blocks.filter((block) => block[3]).map((block) => block[0]),
        `${name}: accepted_by node-two`
      );
      t.is(row.accepted_one, one, `${name}: accepted(node-one)`);
      t.is(row.accepted_two, two, `${name}: accepted(node-two)`);
    }

    /* ---- spent_by ---- */
    interface Spend {
      transaction: { hash: string };
      one: boolean;
      two: boolean;
    }
    const spends = await gql<{
      outputs_by_transaction_hash: {
        transaction_hash: string;
        output_index: number;
        all: Spend[];
        on_one: { transaction: { hash: string } }[];
        on_two: { transaction: { hash: string } }[];
      }[];
    }>(
      `query($hashes: [Hex!]!) {
        outputs_by_transaction_hash(transaction_hashes: $hashes) {
          transaction_hash output_index
          all: spent_by(order_by: { transaction: { hash: ASC } }) {
            transaction { hash } one: accepted(node: "node-one") two: accepted(node: "node-two")
          }
          on_one: spent_by(accepted_by: "node-one") { transaction { hash } }
          on_two: spent_by(accepted_by: "node-two") { transaction { hash } }
        }
      }`,
      { hashes: [c0.hash, shared.hash] }
    );
    const sortedHashes = (hashes: string[]) => [...hashes].sort();
    const spendExpected = new Map<string, [string, boolean, boolean][]>([
      [`${c0.hash}:1`, [[t1.hash, true, true]]],
      [`${c0.hash}:3`, [[shared.hash, true, true]]],
      [
        `${c0.hash}:2`,
        [
          [x.hash, true, false],
          [y.hash, false, true],
        ],
      ],
      [`${c0.hash}:4`, [[z.hash, false, false]]],
      [
        `${shared.hash}:0`,
        [
          [m1.hash, true, false],
          [m2.hash, false, true],
        ],
      ],
    ]);
    t.is(spends.outputs_by_transaction_hash.length, 7);
    spends.outputs_by_transaction_hash.forEach((row) => {
      const key = `${row.transaction_hash}:${row.output_index}`;
      const want = [...(spendExpected.get(key) ?? [])].sort((a, b) =>
        a[0] < b[0] ? -1 : 1
      );
      t.deepEqual(
        row.all.map((input) => [input.transaction.hash, input.one, input.two]),
        want,
        `${key}: every stored spend, with per-node acceptance`
      );
      t.deepEqual(
        sortedHashes(row.on_one.map((input) => input.transaction.hash)),
        want.filter((spender) => spender[1]).map((spender) => spender[0]),
        `${key}: accepted_by node-one`
      );
      t.deepEqual(
        sortedHashes(row.on_two.map((input) => input.transaction.hash)),
        want.filter((spender) => spender[2]).map((spender) => spender[0]),
        `${key}: accepted_by node-two`
      );
    });

    /* ---- acceptance-scoped roots keep their node; structure below them stays complete ---- */
    const scoped = await gql<{
      one: {
        nodes: {
          hash: string;
          block_inclusions: { block: { hash: string } }[];
          outputs: {
            output_index: number;
            spent_by: { transaction: { hash: string } }[];
          }[];
        }[];
      };
      unspent_one: {
        nodes: { transaction_hash: string; output_index: number }[];
      };
      unspent_two: {
        nodes: { transaction_hash: string; output_index: number }[];
      };
      spent_any: { nodes: { output_index: number }[] };
      spent_accepted: { nodes: { output_index: number }[] };
    }>(
      `query($shared: Hex!, $c0: Hex!) {
        one: transactions(node: "node-one", limit: 10, where: { hash: { _eq: $shared } }) {
          nodes { hash block_inclusions { block { hash } } outputs { output_index spent_by { transaction { hash } } } }
        }
        unspent_one: unspent_outputs(node: "node-one", limit: 100, where: { transaction_hash: { _eq: $c0 } }) { nodes { transaction_hash output_index } }
        unspent_two: unspent_outputs(node: "node-two", limit: 100, where: { transaction_hash: { _eq: $c0 } }) { nodes { transaction_hash output_index } }
        spent_any: outputs(node: "node-one", limit: 100, where: { transaction_hash: { _eq: $c0 }, spent_by: {} }) { nodes { output_index } }
        spent_accepted: outputs(node: "node-one", limit: 100, where: { transaction_hash: { _eq: $c0 }, spent_by: { transaction: { block_window: {} } } }) { nodes { output_index } }
      }`,
      { c0: c0.hash, shared: shared.hash }
    );
    t.deepEqual(
      scoped.one.nodes.map((row) =>
        row.block_inclusions.map((i) => i.block.hash)
      ),
      [[block2a.hash, block2b.hash]],
      'below a per-node root, block_inclusions is still every stored block'
    );
    t.deepEqual(
      sortedHashes(
        scoped.one.nodes[0]!.outputs[0]!.spent_by.map((i) => i.transaction.hash)
      ),
      sortedHashes([m1.hash, m2.hash]),
      'below a per-node root, spent_by is still every stored spend'
    );
    /* unspent at query time stays per node: coin 4 is spent only by the stale block's z */
    t.deepEqual(
      scoped.unspent_one.nodes.map((row) => row.output_index),
      [0, 4]
    );
    t.deepEqual(
      scoped.unspent_two.nodes.map((row) => row.output_index),
      [0, 4]
    );
    t.deepEqual(
      scoped.spent_any.nodes.map((row) => row.output_index),
      [1, 2, 3, 4],
      'the spent_by filter is node-agnostic'
    );
    t.deepEqual(
      scoped.spent_accepted.nodes.map((row) => row.output_index),
      [1, 2, 3],
      'opt-in: spends the root node accepts'
    );

    /* ---- authchains: per node by default, every branch without a node ---- */
    const chains = await gql<{
      transactions_by_hash: {
        branches: {
          authchain_length: number;
          authhead_transaction_hash: string;
          unspent_authhead: boolean;
          migrations: { transaction_hash: string }[];
        }[];
        one: { migrations: { transaction_hash: string }[] }[];
        two: { migrations: { transaction_hash: string }[] }[];
      }[];
    }>(
      `query($hashes: [Hex!]!) {
        transactions_by_hash(hashes: $hashes) {
          branches: authchain_branches { authchain_length authhead_transaction_hash unspent_authhead migrations { transaction_hash } }
          one: authchains(node: "node-one") { migrations { transaction_hash } }
          two: authchains(node: "node-two") { migrations { transaction_hash } }
        }
      }`,
      { hashes: [shared.hash] }
    );
    const [chain] = chains.transactions_by_hash;
    t.deepEqual(
      chain!.branches
        .map((branch) => [
          branch.authchain_length,
          branch.authhead_transaction_hash,
          branch.unspent_authhead,
          branch.migrations.map((m) => m.transaction_hash).join(','),
        ])
        .sort(),
      [
        [2, m1.hash, true, `${shared.hash},${m1.hash}`],
        [2, m2.hash, true, `${shared.hash},${m2.hash}`],
      ].sort()
    );
    t.deepEqual(
      chain!.one.map((c) => c.migrations.map((m) => m.transaction_hash)),
      [[shared.hash, m1.hash]]
    );
    t.deepEqual(
      chain!.two.map((c) => c.migrations.map((m) => m.transaction_hash)),
      [[shared.hash, m2.hash]]
    );
  }
);
