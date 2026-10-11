/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-let, functional/no-loop-statement, no-await-in-loop */
// cspell:ignore clickhouse seqs unhex
/**
 * The asynchronous spend index (spend-index.ts, ddl/070_spend.sql,
 * docs/clickhouse-port/spend-index.md): batches, orphans resolved by a later
 * batch, restart cleanup, lag status, and the read rule (index up to the
 * snapshot's spend watermark ∪ the join above it = the join) at every
 * watermark.
 */
import test from 'ava';

import { spendRelationSql, Sql } from '../../api/sql.js';
import type { ChaingraphBlock } from '../../types/chaingraph.js';

import { ClickHouseChecker } from './checker.js';
import {
  acceptance,
  category,
  makeBlock,
  makeTx,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  zeroHash,
} from './spec-fixtures.js';
import { nextBatchEnd } from './spend-index.js';
import { e2eClickHouseUrl } from './test-support.js';
import { readSnapshot } from './visibility.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

test('nextBatchEnd: as many committed commits as fit, at least one; visible0 when all fit', (t) => {
  const commits = [
    { rows: 10, seq: 5n },
    { rows: 10, seq: 7n },
    { rows: 50, seq: 9n },
    { rows: 1, seq: 12n },
  ];
  t.is(nextBatchEnd(4n, 20n, commits, 100), 20n);
  t.is(nextBatchEnd(4n, 20n, commits, 25), 7n);
  // a single commit over the budget still makes a batch
  t.is(nextBatchEnd(7n, 20n, commits, 5), 9n);
  t.is(nextBatchEnd(4n, 8n, commits, 100), 8n);
  // nothing committed in the range (aborted seqs only): straight to visible0
  t.is(nextBatchEnd(12n, 15n, commits, 5), 15n);
});

/**
 * `count` blocks: block i has a coinbase (a token output and a P2PKH one)
 * and, from block 1, a tx spending the previous coinbase's outputs.
 */
const tokenChain = (count: number, label: string) => {
  const blocks: ChaingraphBlock[] = [];
  let previous = zeroHash;
  // eslint-disable-next-line @typescript-eslint/init-declarations
  let previousCoinbase: string | undefined;
  for (let height = 0; height < count; height += 1) {
    const coinbase = makeTx({
      coinbase: true,
      label: `${label}-c${height}`,
      outputs: [
        { lockingBytecode: p2pkh(`${label}-miner`), valueSatoshis: 1_000n },
        {
          fungibleTokenAmount: BigInt(height + 1),
          lockingBytecode: p2pkh(`${label}-holder`),
          nonfungibleTokenCapability: 'mutable',
          nonfungibleTokenCommitment: `0${height % 4}`,
          tokenCategory: category,
          valueSatoshis: 800n,
        },
      ],
    });
    const transactions = [coinbase];
    if (previousCoinbase !== undefined) {
      transactions.push(
        makeTx({
          label: `${label}-t${height}`,
          outputs: [
            { lockingBytecode: p2pkh(`${label}-spender`), valueSatoshis: 1n },
          ],
          spends: [
            [previousCoinbase, 0],
            [previousCoinbase, 1],
          ],
        })
      );
    }
    const block = makeBlock(
      height,
      previous,
      transactions,
      `${label}-${height}`
    );
    blocks.push(block);
    previous = block.hash;
    previousCoinbase = coinbase.hash;
  }
  return blocks;
};

for (const utxo of ['off', 'on'] as const) {
  e2e(
    `[e2e] spend index (utxo ${utxo}): small batches, a mempool child before its parent (orphan, resolved later), a restart: equal to the join, no duplicate`,
    async (t) => {
      t.timeout(180_000);
      const progress: unknown[] = [];
      const { client, openStore } = await scratch(t, `spend_${utxo}`, {
        onDiagnostic: (diagnostic) => {
          if (diagnostic.event === 'spend_index_progress') {
            progress.push(diagnostic);
          }
        },
        spendIndexOptions: { intervalMs: 20, maxRowsPerBatch: 6 },
        utxo,
      });
      const store = await openStore();
      const { node1 } = await registerNodes(store);
      const blocks = tokenChain(12, `si-${utxo}`);
      for (const block of blocks) {
        await store.saveBlock({
          block,
          isSavedTransaction: notSaved,
          nodeAcceptances: [acceptance(node1)],
        });
      }
      // a mempool child before its parent: an orphan of the index until the parent is stored
      const tip = blocks[blocks.length - 1]!;
      const parent = makeTx({
        label: `si-${utxo}-parent`,
        outputs: [{ lockingBytecode: p2pkh('si-parent'), valueSatoshis: 500n }],
        spends: [[tip.transactions[0]!.hash, 0]],
      });
      const child = makeTx({
        label: `si-${utxo}-child`,
        outputs: [{ lockingBytecode: p2pkh('si-child'), valueSatoshis: 400n }],
        spends: [[parent.hash, 0]],
      });
      const at = new Date('2026-10-11T00:00:00Z');
      await store.saveMempoolTransaction(child, [
        { nodeInternalId: node1, validatedAt: at },
      ]);
      await store.publishWatermarks();
      await store.catchUpSpendIndex();
      const checker = new ClickHouseChecker(client, client.database);
      const orphans = await client.query<{ n: string }>(
        'SELECT toString(count()) AS n FROM spend_orphan'
      );
      t.is(orphans[0]?.n, '1', 'the child is an orphan of the index');
      await store.saveMempoolTransaction(parent, [
        { nodeInternalId: node1, validatedAt: at },
      ]);
      await store.publishWatermarks();
      const watermark = await store.catchUpSpendIndex();
      const report = await checker.spendIndexCheck();
      t.is(report.watermark, watermark);
      t.is(report.watermark, report.visible0, 'caught up with visible(0)');
      t.deepEqual(report.missing, []);
      t.deepEqual(report.extra, []);
      t.deepEqual(report.duplicated, []);
      t.deepEqual(report.mismatched, []);
      // 11 spending txs × 2 inputs, the parent and the child
      t.is(report.indexRows, 11 * 2 + 2);
      t.is(report.joinRows, report.indexRows);
      const batches = await client.query<{ n: string }>(
        'SELECT toString(count()) AS n FROM spend_watermark'
      );
      t.true(
        Number(batches[0]?.n) > 3,
        `several batches (${batches[0]?.n ?? 'none'})`
      );
      t.is(store.spendIndexStatus()?.watermark, watermark);

      // a batch that never published its watermark: its rows are removed at the next start
      await client.command(
        `INSERT INTO spend (transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash,
           outpoint_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount,
           nonfungible_token_capability, nonfungible_token_commitment, commit_seq, spent_commit_seq, batch)
         SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index,
           value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability,
           nonfungible_token_commitment, commit_seq, spent_commit_seq, batch + 1000000
         FROM spend LIMIT 3`
      );
      await store.close();
      const restarted = await openStore();
      await restarted.publishWatermarks();
      await restarted.catchUpSpendIndex();
      const after = await checker.spendIndexCheck();
      t.deepEqual(after.duplicated, []);
      t.deepEqual(after.missing, []);
      const leftovers = await client.query<{ n: string }>(
        'SELECT toString(count()) AS n FROM spend WHERE batch > (SELECT max(watermark) FROM spend_watermark)'
      );
      t.is(leftovers[0]?.n, '0', 'unfinished batch rows removed');
      t.true(progress.length > 0, 'progress reported');
    }
  );
}

e2e(
  '[e2e] spend index read rule: index up to the snapshot spend watermark ∪ the join above it = the join, at every watermark',
  async (t) => {
    t.timeout(180_000);
    const { client, openStore } = await scratch(t, 'spend_rule', {
      spendIndexOptions: { intervalMs: 20, maxRowsPerBatch: 4 },
      utxo: 'off',
    });
    const store = await openStore();
    const { node1 } = await registerNodes(store);
    const blocks = tokenChain(10, 'rule');
    for (const block of blocks) {
      await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(node1)],
      });
    }
    await store.publishWatermarks();
    await store.catchUpSpendIndex();
    const marks = (
      await client.query<{ w: string }>(
        'SELECT toString(watermark) AS w FROM spend_watermark ORDER BY watermark'
      )
    ).map((row) => BigInt(row.w));
    const snapshot = await readSnapshot(client, node1);
    const relation = async (spend: bigint, predicate = '1') => {
      const sql = new Sql({ ...snapshot, spend });
      return (
        await client.query<{ k: string }>(
          `SELECT concat(lower(hex(transaction_hash)), ':', toString(input_index), ':',
             toString(value_satoshis), ':', lower(hex(locking_bytecode))) AS k
           FROM ${spendRelationSql(sql, predicate)} ORDER BY k`,
          sql.params
        )
      ).map((row) => row.k);
    };
    const full = await relation(0n);
    t.is(full.length, 9 * 2);
    for (const spend of [...marks, snapshot.visible0]) {
      t.deepEqual(await relation(spend), full, `spend watermark ${spend}`);
    }
    // a category + commitment-prefix filter through the index and the tail alike
    const filter = `token_category = toFixedString(unhex('${category}'), 32)
      AND startsWith(nonfungible_token_commitment_key, unhex('01'))`;
    const filtered = await relation(0n, filter);
    t.true(filtered.length > 0);
    for (const spend of marks) {
      t.deepEqual(await relation(spend, filter), filtered);
    }
  }
);
