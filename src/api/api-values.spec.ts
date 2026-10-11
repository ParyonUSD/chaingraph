/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, functional/no-loop-statement, no-await-in-loop, @typescript-eslint/require-array-sort-compare, complexity, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse authchain authchains authhead
/**
 * Phase 2 G4 (docs/clickhouse-port/phase2-api.md): aggregates (R24) and
 * group-by (R25) per node, computed values (R26) and per-node authchains
 * (R29) on the two-node scratch dataset, each against the raw rows
 * aggregated in JavaScript (spec-oracle.ts). Chipnet equality with v1 for
 * R24, R26–R30 is in the catalogue (X1, X5–X16).
 */
import test from 'ava';

import { e2eClickHouseUrl } from '../store/clickhouse/test-support.js';

import { holder1, paryon, redeemer } from './spec-dataset.js';
import { one, setup, two } from './spec-env.js';
import type { Oracle, OracleOutput } from './spec-oracle.js';
import { accepted, loadOracle } from './spec-oracle.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const stats = (values: readonly bigint[]) =>
  values.length === 0
    ? { avg: null, count: '0', max: null, min: null, sum: null }
    : {
        avg: Number(values.reduce((a, b) => a + b, 0n)) / values.length,
        count: String(values.length),
        max: String(values.reduce((a, b) => (b > a ? b : a))),
        min: String(values.reduce((a, b) => (b < a ? b : a))),
        sum: String(values.reduce((a, b) => a + b, 0n)),
      };

const acceptedOutputs = (oracle: Oracle) =>
  oracle.outputs.filter((output) => accepted(oracle, output.transaction_hash));

const unspentOutputs = (oracle: Oracle) => {
  const spent = new Set(
    oracle.inputs
      .filter((input) => accepted(oracle, input.transaction_hash))
      .map(
        (input) => `${input.outpoint_transaction_hash}:${input.outpoint_index}`
      )
  );
  return acceptedOutputs(oracle).filter(
    (output) => !spent.has(`${output.transaction_hash}:${output.output_index}`)
  );
};

const groupBy = <T>(rows: readonly T[], key: (row: T) => string) => {
  const groups = new Map<string, T[]>();
  rows.forEach((row) => {
    groups.set(key(row), [...(groups.get(key(row)) ?? []), row]);
  });
  return groups;
};

e2e(
  '[e2e] G4 R24 + R25: aggregates and group-by per node (outputs, unspent outputs, spends, transactions, blocks, mempool) equal the raw rows aggregated in JS',
  async (t) => {
    const env = await setup(t, 'g4_aggregates');
    for (const node of [one, two]) {
      const oracle = await loadOracle(
        env.client,
        node === one ? env.nodes.node1 : env.nodes.node2
      );
      const result = await env.gql<{
        o: {
          aggregate: {
            count: string;
            sum: {
              value_satoshis: string;
              fungible_token_amount: string | null;
            };
            min: { value_satoshis: string };
            max: { output_index: string };
            avg: { value_satoshis: number };
          };
        };
        p: {
          aggregate: { count: string; sum: { fungible_token_amount: string } };
        };
        u: { aggregate: { count: string; sum: { value_satoshis: string } } };
        empty: {
          aggregate: {
            count: string;
            sum: { value_satoshis: string | null };
            avg: { value_satoshis: number | null };
          };
        };
        tx: {
          aggregate: {
            count: string;
            sum: { size_bytes: string; output_count: string };
            max: { input_count: string };
          };
        };
        blocks: {
          aggregate: {
            count: string;
            sum: { size_bytes: string };
            max: { height: string };
            avg: { transaction_count: number };
          };
        };
        mempool: { aggregate: { count: string } };
        spends: {
          aggregate: {
            count: string;
            sum: { outpoint_value_satoshis: string };
          };
        };
      }>(
        `query($node: String!, $paryon: Hex!) {
          o: outputs_aggregate(node: $node) { aggregate { count sum { value_satoshis fungible_token_amount } min { value_satoshis } max { output_index } avg { value_satoshis } } }
          p: outputs_aggregate(node: $node, where: { token_category: { _eq: $paryon }, fungible_token_amount: { _gt: 0 } }) { aggregate { count sum { fungible_token_amount } } }
          u: unspent_outputs_aggregate(node: $node) { aggregate { count sum { value_satoshis } } }
          empty: outputs_aggregate(node: $node, where: { output_index: { _gt: 100000 } }) { aggregate { count sum { value_satoshis } avg { value_satoshis } } }
          tx: transactions_aggregate(node: $node) { aggregate { count sum { size_bytes output_count } max { input_count } } }
          blocks: blocks_aggregate(node: $node) { aggregate { count sum { size_bytes } max { height } avg { transaction_count } } }
          mempool: mempool_aggregate(node: $node) { aggregate { count } }
          spends: spends_aggregate(node: $node) { aggregate { count sum { outpoint_value_satoshis } } }
        }`,
        { node, paryon }
      );
      const outputs = acceptedOutputs(oracle);
      const value = stats(outputs.map((output) => output.value_satoshis));
      t.is(result.o.aggregate.count, value.count, `${node} count`);
      t.is(result.o.aggregate.sum.value_satoshis, value.sum!);
      t.is(result.o.aggregate.min.value_satoshis, value.min!);
      t.is(
        result.o.aggregate.max.output_index,
        String(Math.max(...outputs.map((output) => output.output_index)))
      );
      t.true(
        Math.abs(result.o.aggregate.avg.value_satoshis - value.avg!) < 1e-6
      );
      t.is(
        result.o.aggregate.sum.fungible_token_amount,
        stats(
          outputs
            .map((output) => output.fungible_token_amount)
            .filter((amount): amount is bigint => amount !== null)
        ).sum
      );
      const ft = outputs.filter(
        (output) =>
          output.token_category === paryon &&
          (output.fungible_token_amount ?? 0n) > 0n
      );
      t.deepEqual(result.p.aggregate, {
        count: String(ft.length),
        sum: {
          fungible_token_amount: stats(
            ft.map((output) => output.fungible_token_amount!)
          ).sum!,
        },
      });
      const unspent = unspentOutputs(oracle);
      t.deepEqual(result.u.aggregate, {
        count: String(unspent.length),
        sum: {
          value_satoshis: stats(unspent.map((output) => output.value_satoshis))
            .sum!,
        },
      });
      t.deepEqual(result.empty.aggregate, {
        avg: { value_satoshis: null },
        count: '0',
        sum: { value_satoshis: null },
      });
      const transactions = [...oracle.transactions.values()].filter((tx) =>
        accepted(oracle, tx.hash)
      );
      t.deepEqual(result.tx.aggregate, {
        count: String(transactions.length),
        max: {
          input_count: stats(transactions.map((tx) => tx.input_count)).max!,
        },
        sum: {
          output_count: stats(transactions.map((tx) => tx.output_count)).sum!,
          size_bytes: stats(transactions.map((tx) => tx.size_bytes)).sum!,
        },
      });
      const blockIds = new Set(
        [...oracle.acceptance.values()].flat().filter((id) => id !== 0n)
      );
      const blocks = [...blockIds].map((id) => oracle.blocks.get(id)!);
      t.is(result.blocks.aggregate.count, String(blocks.length));
      t.is(
        result.blocks.aggregate.sum.size_bytes,
        stats(blocks.map((block) => block.size_bytes)).sum!
      );
      t.is(
        result.blocks.aggregate.max.height,
        stats(blocks.map((block) => block.height)).max!
      );
      t.true(
        Math.abs(
          result.blocks.aggregate.avg.transaction_count -
            stats(blocks.map((block) => block.transaction_count)).avg!
        ) < 1e-6
      );
      t.is(
        result.mempool.aggregate.count,
        String(
          [...oracle.acceptance.values()].filter((ids) => ids.includes(0n))
            .length
        )
      );
      const spends = oracle.inputs.filter(
        (input) =>
          accepted(oracle, input.transaction_hash) &&
          oracle.outputByOutpoint.has(
            `${input.outpoint_transaction_hash}:${input.outpoint_index}`
          )
      );
      t.deepEqual(result.spends.aggregate, {
        count: String(spends.length),
        sum: {
          outpoint_value_satoshis: stats(
            spends.map(
              (input) =>
                oracle.outputByOutpoint.get(
                  `${input.outpoint_transaction_hash}:${input.outpoint_index}`
                )!.value_satoshis
            )
          ).sum!,
        },
      });

      // group-by: unspent holder balances (S3 generalised), supply per category, spends per category, transactions per block, blocks per day
      const groups = await env.gql<{
        holders: {
          has_more: boolean;
          nodes: {
            key: { locking_bytecode: string };
            aggregate: {
              count: string;
              sum: { fungible_token_amount: string };
            };
          }[];
        };
        categories: {
          nodes: {
            key: {
              token_category: string | null;
              nonfungible_token_capability: string | null;
            };
            aggregate: { count: string; max: { value_satoshis: string } };
          }[];
        };
        spent: {
          nodes: {
            key: { outpoint_token_category: string | null };
            aggregate: {
              count: string;
              sum: { outpoint_value_satoshis: string };
            };
          }[];
        };
        perBlock: {
          nodes: {
            key: { block_height: number | null; is_coinbase: boolean };
            aggregate: { count: string };
          }[];
        };
        perDay: {
          nodes: {
            key: { day: string };
            aggregate: { count: string; sum: { size_bytes: string } };
          }[];
        };
      }>(
        `query($node: String!, $paryon: Hex!) {
          holders: output_groups(node: $node, unspent: true, limit: 2, group_by: [LOCKING_BYTECODE],
            where: { token_category: { _eq: $paryon }, nonfungible_token_capability: { _is_null: true } },
            order_by: [{ by: SUM_FUNGIBLE_TOKEN_AMOUNT, direction: DESC }]) {
            has_more nodes { key { locking_bytecode } aggregate { count sum { fungible_token_amount } } }
          }
          categories: output_groups(node: $node, limit: 100, group_by: [TOKEN_CATEGORY, NONFUNGIBLE_TOKEN_CAPABILITY], order_by: [{ by: KEY, direction: ASC }]) {
            nodes { key { token_category nonfungible_token_capability } aggregate { count max { value_satoshis } } }
          }
          spent: spend_groups(node: $node, limit: 100, group_by: [OUTPOINT_TOKEN_CATEGORY]) {
            nodes { key { outpoint_token_category } aggregate { count sum { outpoint_value_satoshis } } }
          }
          perBlock: transaction_groups(node: $node, limit: 100, group_by: [BLOCK_HEIGHT, IS_COINBASE]) {
            nodes { key { block_height is_coinbase } aggregate { count } }
          }
          perDay: block_groups(node: $node, limit: 100, group_by: [DAY]) {
            nodes { key { day } aggregate { count sum { size_bytes } } }
          }
        }`,
        { node, paryon }
      );
      const holderGroups = [
        ...groupBy(
          unspent.filter(
            (output) =>
              output.token_category === paryon &&
              output.nonfungible_token_capability === null
          ),
          (output) => output.locking_bytecode
        ),
      ]
        .map(([key, rows]) => ({
          aggregate: {
            count: String(rows.length),
            sum: {
              fungible_token_amount: stats(
                rows.map((row) => row.fungible_token_amount ?? 0n)
              ).sum!,
            },
          },
          key: { locking_bytecode: key },
        }))
        .sort((a, b) =>
          BigInt(b.aggregate.sum.fungible_token_amount) ===
          BigInt(a.aggregate.sum.fungible_token_amount)
            ? a.key.locking_bytecode.localeCompare(b.key.locking_bytecode)
            : BigInt(b.aggregate.sum.fungible_token_amount) >
              BigInt(a.aggregate.sum.fungible_token_amount)
            ? 1
            : -1
        );
      t.deepEqual(
        groups.holders.nodes,
        holderGroups.slice(0, 2),
        `${node} holders`
      );
      t.is(groups.holders.has_more, holderGroups.length > 2);
      const categoryKey = (output: OracleOutput) =>
        `${output.token_category ?? ''}|${
          output.nonfungible_token_capability ?? ''
        }`;
      const categories = groupBy(outputs, categoryKey);
      t.deepEqual(
        groups.categories.nodes
          .map(
            (group) =>
              `${group.key.token_category ?? ''}|${
                group.key.nonfungible_token_capability ?? ''
              }:${group.aggregate.count}:${group.aggregate.max.value_satoshis}`
          )
          .sort(),
        [...categories]
          .map(
            ([key, rows]) =>
              `${key}:${rows.length}:${stats(
                rows.map((row) => row.value_satoshis)
              ).max!}`
          )
          .sort(),
        `${node} categories`
      );
      const spentGroups = groupBy(spends, (input) => {
        const output = oracle.outputByOutpoint.get(
          `${input.outpoint_transaction_hash}:${input.outpoint_index}`
        )!;
        return output.token_category ?? '';
      });
      t.deepEqual(
        groups.spent.nodes
          .map(
            (group) =>
              `${group.key.outpoint_token_category ?? ''}:${
                group.aggregate.count
              }:${group.aggregate.sum.outpoint_value_satoshis}`
          )
          .sort(),
        [...spentGroups]
          .map(
            ([key, rows]) =>
              `${key}:${rows.length}:${stats(
                rows.map(
                  (input) =>
                    oracle.outputByOutpoint.get(
                      `${input.outpoint_transaction_hash}:${input.outpoint_index}`
                    )!.value_satoshis
                )
              ).sum!}`
          )
          .sort(),
        `${node} spends per category`
      );
      const height = (hash: string) => {
        const ids = (oracle.acceptance.get(hash) ?? []).filter(
          (id) => id !== 0n
        );
        return ids.length === 0
          ? null
          : Number(oracle.blocks.get(ids[0]!)!.height);
      };
      t.deepEqual(
        groups.perBlock.nodes
          .map(
            (group) =>
              `${group.key.block_height ?? 'mempool'}:${String(
                group.key.is_coinbase
              )}:${group.aggregate.count}`
          )
          .sort(),
        [
          ...groupBy(
            transactions,
            (tx) => `${height(tx.hash) ?? 'mempool'}:${String(tx.is_coinbase)}`
          ),
        ]
          .map(([key, rows]) => `${key}:${rows.length}`)
          .sort(),
        `${node} transactions per block`
      );
      const day = (timestamp: bigint) =>
        new Date(Number(timestamp) * 1000).toISOString().slice(0, 10);
      t.deepEqual(
        groups.perDay.nodes
          .map(
            (group) =>
              `${group.key.day}:${group.aggregate.count}:${group.aggregate.sum.size_bytes}`
          )
          .sort(),
        [...groupBy(blocks, (block) => day(block.timestamp))]
          .map(
            ([key, rows]) =>
              `${key}:${rows.length}:${stats(
                rows.map((block) => block.size_bytes)
              ).sum!}`
          )
          .sort(),
        `${node} blocks per day`
      );
    }
  }
);

e2e(
  '[e2e] G4 R26 + R29: computed values (input value, fee, counts) and per-node authchains equal the raw rows',
  async (t) => {
    const env = await setup(t, 'g4_values');
    const oracle = await loadOracle(env.client, env.nodes.node1);
    const hashes = [
      env.data.closeA.hash,
      env.data.interest.hash,
      env.data.c0.hash,
      env.data.redeemA.hash,
    ];
    const result = await env.gql<{
      transactions_by_hash: {
        hash: string;
        is_coinbase: boolean;
        input_value_satoshis: string;
        output_value_satoshis: string;
        fee_satoshis: string;
        inputs: { value_satoshis: string | null }[];
        one: {
          authchain_length: number;
          authhead_transaction_hash: string;
          unspent_authhead: boolean;
          migrations: {
            migration_index: number;
            transaction: { hash: string };
          }[];
        }[];
        two: { authchain_length: number; authhead: { hash: string } }[];
      }[];
      blocks: {
        nodes: {
          height: number;
          input_count: string;
          output_count: string;
          input_value_satoshis: string;
          fee_satoshis: string;
          generated_value_satoshis: string;
          output_value_satoshis: string;
        }[];
      };
    }>(
      `query($hashes: [Hex!]!) {
        transactions_by_hash(hashes: $hashes) {
          hash is_coinbase input_value_satoshis output_value_satoshis fee_satoshis
          inputs { value_satoshis }
          one: authchains(node: "node-one") { authchain_length authhead_transaction_hash unspent_authhead migrations { migration_index transaction { hash } } }
          two: authchains(node: "node-two") { authchain_length authhead { hash } }
        }
        blocks(node: "node-one", limit: 10) { nodes { height input_count output_count input_value_satoshis fee_satoshis generated_value_satoshis output_value_satoshis } }
      }`,
      { hashes }
    );
    const valueOf = (hash: string, index: bigint) =>
      oracle.outputByOutpoint.get(`${hash}:${index}`)?.value_satoshis;
    for (const tx of result.transactions_by_hash) {
      const inputs = oracle.inputsByTransaction.get(tx.hash) ?? [];
      const inputValue = inputs.reduce(
        (sum, input) =>
          sum +
          (valueOf(input.outpoint_transaction_hash, input.outpoint_index) ??
            0n),
        0n
      );
      const outputValue = (
        oracle.outputsByTransaction.get(tx.hash) ?? []
      ).reduce((sum, output) => sum + output.value_satoshis, 0n);
      t.is(tx.input_value_satoshis, String(inputValue), tx.hash);
      t.is(tx.output_value_satoshis, String(outputValue));
      t.is(
        tx.fee_satoshis,
        tx.is_coinbase ? '0' : String(inputValue - outputValue)
      );
      t.deepEqual(
        tx.inputs.map((input) => input.value_satoshis),
        [...inputs]
          .sort((a, b) => a.input_index - b.input_index)
          .map((input) => {
            const value = valueOf(
              input.outpoint_transaction_hash,
              input.outpoint_index
            );
            return value === undefined ? null : String(value);
          })
      );
    }
    // authchain of redeemA: output 0 is spent by redeemB, which only node-one accepts (its mempool)
    const redeemA = result.transactions_by_hash.find(
      (tx) => tx.hash === env.data.redeemA.hash
    )!;
    t.deepEqual(redeemA.one, [
      {
        authchain_length: 2,
        authhead_transaction_hash: env.data.redeemB.hash,
        migrations: [
          { migration_index: 0, transaction: { hash: env.data.redeemA.hash } },
          { migration_index: 1, transaction: { hash: env.data.redeemB.hash } },
        ],
        unspent_authhead: true,
      },
    ]);
    t.deepEqual(redeemA.two, [
      { authchain_length: 1, authhead: { hash: env.data.redeemA.hash } },
    ]);
    // blocks: counts and values from the block's transactions
    for (const block of result.blocks.nodes) {
      const id = [...oracle.blocks.values()].find(
        (entry) => Number(entry.height) === block.height
      )!;
      const txs = [...oracle.acceptance]
        .filter(([, ids]) => ids.includes(id.internal_id))
        .map(([hash]) => oracle.transactions.get(hash)!);
      const inputValue = txs
        .flatMap((tx) => oracle.inputsByTransaction.get(tx.hash) ?? [])
        .reduce(
          (sum, input) =>
            sum +
            (valueOf(input.outpoint_transaction_hash, input.outpoint_index) ??
              0n),
          0n
        );
      const outputValue = txs.reduce(
        (sum, tx) => sum + tx.output_value_satoshis,
        0n
      );
      const coinbaseOutput = txs
        .filter((tx) => tx.is_coinbase)
        .reduce((sum, tx) => sum + tx.output_value_satoshis, 0n);
      t.deepEqual(
        block,
        {
          fee_satoshis: String(inputValue - (outputValue - coinbaseOutput)),
          generated_value_satoshis: String(outputValue - inputValue),
          height: block.height,
          input_count: String(
            txs.reduce((sum, tx) => sum + tx.input_count, 0n)
          ),
          input_value_satoshis: String(inputValue),
          output_count: String(
            txs.reduce((sum, tx) => sum + tx.output_count, 0n)
          ),
          output_value_satoshis: String(outputValue),
        },
        `block ${block.height}`
      );
    }
    t.true(result.blocks.nodes.length >= 3);
    t.truthy(holder1 && redeemer);
  }
);
