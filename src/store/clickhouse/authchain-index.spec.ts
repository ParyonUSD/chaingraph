/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-let, functional/no-loop-statement, no-await-in-loop, @typescript-eslint/naming-convention, camelcase, complexity, @typescript-eslint/require-array-sort-compare */
// cspell:ignore clickhouse authchain authchains
/**
 * The authchain index (authchain-index.ts, ddl/080_authchain.sql) and the
 * API's segment walk (model.ts `authchainSql`) against an independent walk
 * in TypeScript: for every transaction of a dataset with long chains across
 * many spend batches, merges (a transaction spending two outputs 0, so one
 * chain leaves its segment), a conflict (two spenders of one output 0, one in
 * each node's mempool), a node that lacks the last blocks, and transactions
 * above the spend watermark. The walk must equal the model at every spend
 * watermark, after a backfill from scratch, and with an empty index.
 */
import test from 'ava';

import type { Ctx } from '../../api/compile.js';
import { authchainSql } from '../../api/model.js';
import { Sql } from '../../api/sql.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import { backfillAuthchainLinks } from './authchain-index.js';
import {
  acceptance,
  makeBlock,
  makeTx,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  zeroHash,
} from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';
import { readSnapshot } from './visibility.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const output = (label: string) => ({
  lockingBytecode: p2pkh(label),
  valueSatoshis: 1_000n,
});

/** A transaction spending the given outputs, with an identity output 0 and a change output 1. */
const spender = (label: string, spends: [string, number][]) =>
  makeTx({
    label,
    outputs: [output(`${label}-id`), output(`${label}-change`)],
    spends,
  });

e2e(
  '[e2e] authchain index: the segment walk equals a link-by-link walk per node, at every spend watermark, after a backfill and with no index',
  async (t) => {
    t.timeout(300_000);
    const { client, openStore } = await scratch(t, 'authchain', {
      spendIndexOptions: { intervalMs: 3_600_000, maxRowsPerBatch: 8 },
      utxo: 'off',
    });
    const store = await openStore();
    const { node1, node2 } = await registerNodes(store);

    /*
     * Block 0: coinbase. Chain t (60 links) from its output 0; side chain s
     * (5 links) merging into t at m1 (input 0: s5, input 1: t20); a bigger
     * chain b (80 links) merging with t at m2 (input 0: t40, input 1: b80),
     * where t is the light parent. Blocks of 6 transactions; node-two
     * lacks the last two blocks.
     */
    const transactions: ChaingraphTransaction[] = [];
    const add = (tx: ChaingraphTransaction) => {
      transactions.push(tx);
      return tx.hash;
    };
    const coinbase = makeTx({
      coinbase: true,
      label: 'ac-cb',
      outputs: [output('ac-cb-0'), output('ac-cb-1'), output('ac-cb-2')],
    });
    let b = add(spender('ac-b1', [[coinbase.hash, 1]]));
    for (let index = 2; index <= 80; index += 1) {
      b = add(spender(`ac-b${index}`, [[b, 0]]));
    }
    let s = add(spender('ac-s1', [[coinbase.hash, 2]]));
    for (let index = 2; index <= 5; index += 1) {
      s = add(spender(`ac-s${index}`, [[s, 0]]));
    }
    let chain = add(spender('ac-t1', [[coinbase.hash, 0]]));
    for (let index = 2; index <= 60; index += 1) {
      const name = `ac-t${index}`;
      if (index === 21) {
        chain = add(
          spender('ac-m1', [
            [s, 0],
            [chain, 0],
          ])
        );
      } else if (index === 41) {
        chain = add(
          spender('ac-m2', [
            [chain, 0],
            [b, 0],
          ])
        );
      } else {
        chain = add(spender(name, [[chain, 0]]));
      }
    }
    const blocks: ChaingraphBlock[] = [];
    let previous = zeroHash;
    const perBlock = 6;
    for (let start = 0; start < transactions.length; start += perBlock) {
      const height = blocks.length;
      const block = makeBlock(
        height,
        previous,
        [
          ...(height === 0
            ? [coinbase]
            : [
                makeTx({
                  coinbase: true,
                  label: `ac-cb-${height}`,
                  outputs: [output(`ac-cb-${height}`)],
                }),
              ]),
          ...transactions.slice(start, start + perBlock),
        ],
        `ac-${height}`
      );
      blocks.push(block);
      previous = block.hash;
    }
    const node2Blocks = blocks.length - 2;
    for (const [height, block] of blocks.entries()) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances:
          height < node2Blocks
            ? [acceptance(node1), acceptance(node2)]
            : [acceptance(node1)],
      });
    }
    /* the conflict: node-one's mempool continues t60 with k1 → k1b, node-two's (t60 is in a block it lacks) not */
    const k1 = spender('ac-k1', [[chain, 0]]);
    const k1b = spender('ac-k1b', [[k1.hash, 0]]);
    const k2 = spender('ac-k2', [[chain, 0]]);
    const at = new Date('2026-10-11T00:00:00Z');
    await store.saveMempoolTransaction(k1, [
      { nodeInternalId: node1, validatedAt: at },
    ]);
    await store.saveMempoolTransaction(k1b, [
      { nodeInternalId: node1, validatedAt: at },
    ]);
    await store.saveMempoolTransaction(k2, [
      { nodeInternalId: node2, validatedAt: at },
    ]);
    await store.publishWatermarks();
    await store.catchUpSpendIndex();
    /* above the spend watermark: two more links in node-one's mempool, not indexed */
    const k1c = spender('ac-k1c', [[k1b.hash, 0]]);
    const k1d = spender('ac-k1d', [[k1c.hash, 0]]);
    await store.saveMempoolTransaction(k1c, [
      { nodeInternalId: node1, validatedAt: at },
    ]);
    await store.saveMempoolTransaction(k1d, [
      { nodeInternalId: node1, validatedAt: at },
    ]);
    await store.publishWatermarks();

    /* the model: per node, follow the accepted spender of output 0 */
    const all = [
      ...blocks.flatMap((block) => block.transactions),
      k1,
      k1b,
      k2,
      k1c,
      k1d,
    ];
    const accepted = new Map<number, Set<string>>([
      [
        node1,
        new Set([
          ...blocks.flatMap((block) => block.transactions.map((tx) => tx.hash)),
          k1.hash,
          k1b.hash,
          k1c.hash,
          k1d.hash,
        ]),
      ],
      [
        node2,
        new Set([
          ...blocks
            .slice(0, node2Blocks)
            .flatMap((block) => block.transactions.map((tx) => tx.hash)),
          k2.hash,
        ]),
      ],
    ]);
    const spenders = new Map<string, string[]>();
    all.forEach((tx) => {
      tx.inputs
        .filter((input) => input.outpointIndex === 0)
        .forEach((input) => {
          const list = spenders.get(input.outpointTransactionHash) ?? [];
          list.push(tx.hash);
          spenders.set(input.outpointTransactionHash, list);
        });
    });
    const model = (node: number) => {
      const set = accepted.get(node)!;
      return all
        .filter((tx) => set.has(tx.hash))
        .map((tx) => {
          const path = [tx.hash];
          for (;;) {
            const next = (spenders.get(path[path.length - 1]!) ?? []).filter(
              (hash) => set.has(hash)
            );
            if (next.length === 0) break;
            t.is(next.length, 1, 'one accepted spender per output');
            path.push(next[0]!);
          }
          return `${tx.hash}:${path.join(',')}`;
        })
        .sort();
    };

    const snapshot = await readSnapshot(client, node1);
    const walk = async (node: number, spend: bigint) => {
      const sql = new Sql(
        { ...snapshot, nodeId: node1, spend },
        new Map([[node2, (await readSnapshot(client, node2)).visible]])
      );
      const c: Ctx = { node, nodeId: () => node, sql };
      const bases = sql.bind(
        all.map((tx) => tx.hash),
        'Array(String)'
      );
      const query = authchainSql(
        c,
        `SELECT toFixedString(unhex(h), 32) AS hash FROM (SELECT arrayJoin(${bases}) AS h)`
      );
      const rows = await client.query<{
        base: string;
        path: string[];
        length: number;
      }>(
        `${sql.withClause()}SELECT lower(hex(base)) AS base, arrayMap(h -> lower(hex(h)), path) AS path,
           length FROM (${query})`,
        sql.params,
        { max_recursive_cte_evaluation_depth: 10_000 } as never
      );
      rows.forEach((row) => {
        t.is(Number(row.length), row.path.length);
      });
      return rows.map((row) => `${row.base}:${row.path.join(',')}`).sort();
    };

    const expected = new Map([
      [node1, model(node1)],
      [node2, model(node2)],
    ]);
    t.true(
      expected.get(node1)!.some((row) => row.split(',').length > 100),
      'a chain over 100 links (b, then t)'
    );
    const marks = (
      await client.query<{ w: string }>(
        'SELECT toString(watermark) AS w FROM spend_watermark ORDER BY watermark'
      )
    ).map((row) => BigInt(row.w));
    t.true(marks.length > 10, `many spend batches (${marks.length})`);
    const links = async () =>
      Number(
        (
          await client.query<{ n: string }>(
            'SELECT toString(count()) AS n FROM authchain_link'
          )
        )[0]!.n
      );
    /* every transaction spending an output 0 is placed once, but the two above the spend watermark */
    const placed =
      all.filter(
        (tx) =>
          !tx.isCoinbase && tx.inputs.some((input) => input.outpointIndex === 0)
      ).length - 2;
    t.is(await links(), placed);
    const check = async (label: string, spends: readonly bigint[]) => {
      for (const node of [node1, node2]) {
        for (const spend of spends) {
          t.deepEqual(
            await walk(node, spend),
            expected.get(node),
            `${label}: node ${node}, spend watermark ${spend}`
          );
        }
      }
    };
    await check('filler', [
      0n,
      ...marks.filter((_, i) => i % 5 === 0),
      marks.at(-1)!,
    ]);
    /* segment ends crossed on the longest chain: few (heavy-path placement) */
    const segments = await client.query<{ n: string }>(
      `SELECT toString(uniqExact(segment)) AS n FROM authchain_link
       WHERE transaction_hash IN (SELECT unhex(arrayJoin({hashes:Array(String)})))`,
      {
        hashes: expected
          .get(node1)!
          .reduce((longest, row) =>
            row.split(',').length > longest.split(',').length ? row : longest
          )
          .split(':')[1]!
          .split(','),
      }
    );
    t.true(
      Number(segments[0]!.n) <= 3,
      `segments on the longest chain: ${segments[0]!.n}`
    );

    await client.command('TRUNCATE TABLE authchain_link');
    await check('no index', [marks.at(-1)!]);
    const backfill = await backfillAuthchainLinks(client);
    t.is(backfill.rows, placed);
    await check('backfill', [marks.at(-1)!]);
  }
);
