/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, no-await-in-loop, functional/no-throw-statement, @typescript-eslint/init-declarations, max-params, @typescript-eslint/no-loop-func */
// cspell:ignore clickhouse unhex standin seqs norestart
/**
 * Stand-in `input` rows of mempool children (docs/clickhouse-port/mempool-fill-fix.md;
 * found by the chipnet lab, chipnet-lab.md): a mempool transaction saved while its
 * parents are unknown carries stand-in spent outputs (value 0, empty bytecode, no
 * token) until a commit stores the parents. Every scenario runs with UTXO on and
 * off and checks, through the pinned views of one snapshot, that each input of the
 * child has exactly one visible row, with the real spent-output attributes once
 * the parent is visible (the checker's `standInCheck`).
 */
import { isDeepStrictEqual } from 'node:util';

import type { ExecutionContext } from 'ava';
import test from 'ava';

import type { FaultInjector } from './block-commit.js';
import { SimulatedCrash } from './block-commit.js';
import { ClickHouseChecker } from './checker.js';
import type { ClickHouseStore } from './clickhouse-store.js';
import type { ClickHouseClient } from './client.js';
import {
  acceptance,
  badUtxoSums,
  category,
  leaseTtlMs,
  makeBlock,
  makeTx,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  threeBlockChain,
} from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';
import { pinnedView, readSnapshot, snapshotParams } from './visibility.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const at = (seconds: number) =>
  new Date(Date.UTC(2026, 9, 10, 20, 0, 0) + seconds * 1_000);

/**
 * u spends c1:0 (outputs: a token output and a plain one), v spends a:1; the
 * child x spends u:0, u:1 and v:0 (two unknown parents, three inputs, as the
 * chipnet transaction d9444345…).
 */
const fixture = () => {
  const chain = threeBlockChain();
  const u = makeTx({
    label: 'standin-u',
    outputs: [
      {
        fungibleTokenAmount: 50n,
        lockingBytecode: p2pkh('u0'),
        nonfungibleTokenCapability: 'mutable',
        nonfungibleTokenCommitment: 'aa',
        tokenCategory: category,
        valueSatoshis: 2_000n,
      },
      { lockingBytecode: p2pkh('u1'), valueSatoshis: 3_000n },
    ],
    spends: [[chain.c1.hash, 0]],
  });
  const v = makeTx({
    label: 'standin-v',
    outputs: [{ lockingBytecode: p2pkh('v0'), valueSatoshis: 4_000n }],
    spends: [[chain.a.hash, 1]],
  });
  const x = makeTx({
    label: 'standin-x',
    outputs: [{ lockingBytecode: p2pkh('x0'), valueSatoshis: 8_500n }],
    spends: [
      [u.hash, 0],
      [u.hash, 1],
      [v.hash, 0],
    ],
  });
  const coinbase = (label: string) =>
    makeTx({
      coinbase: true,
      label,
      outputs: [
        { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
      ],
    });
  /** u, v and x in one block (the chipnet case) */
  const blockUVX = makeBlock(
    2,
    chain.block1.hash,
    [coinbase('standin-c2'), u, v, x],
    'standin-block-uvx'
  );
  /** u and v only; x follows in blockX */
  const blockUV = makeBlock(
    2,
    chain.block1.hash,
    [coinbase('standin-c2'), u, v],
    'standin-block-uv'
  );
  const blockX = makeBlock(
    3,
    blockUV.hash,
    [coinbase('standin-c3'), x],
    'standin-block-x'
  );
  /** what x's inputs carry once its parents are known */
  const expected = [
    {
      amount: '50',
      bytecode: p2pkh('u0'),
      capability: 'mutable',
      category,
      commitment: 'aa',
      index: 0,
      value: '2000',
    },
    {
      amount: null,
      bytecode: p2pkh('u1'),
      capability: null,
      category: '00'.repeat(32),
      commitment: null,
      index: 1,
      value: '3000',
    },
    {
      amount: null,
      bytecode: p2pkh('v0'),
      capability: null,
      category: '00'.repeat(32),
      commitment: null,
      index: 2,
      value: '4000',
    },
  ];
  const standIn = (index: number) => ({
    amount: null,
    bytecode: '',
    capability: null,
    category: '00'.repeat(32),
    commitment: null,
    index,
    value: '0',
  });
  return { blockUV, blockUVX, blockX, chain, expected, standIn, u, v, x };
};

type Fixture = ReturnType<typeof fixture>;

type Utxo = 'off' | 'on';

const setup = async (
  t: ExecutionContext,
  label: string,
  utxo: Utxo,
  fault?: FaultInjector
) => {
  const env = await scratch(
    t,
    `${label}_${utxo}`,
    { orphanGraceMs: 50, utxo },
    'ch1_standin'
  );
  const store = await env.openStore(fault);
  const nodes = await registerNodes(store);
  const fx = fixture();
  for (const block of [fx.chain.block0, fx.chain.block1]) {
    await store.saveBlock({
      block,
      isSavedTransaction: notSaved,
      nodeAcceptances: [
        acceptance(nodes.node1, at(0)),
        acceptance(nodes.node2, at(0)),
      ],
    });
  }
  const checker = new ClickHouseChecker(env.client, env.client.database, {
    utxo,
  });
  return { ...env, checker, fx, nodes, store };
};

/** x's input rows as one node-agnostic snapshot shows them. */
const inputsOf = async (client: ClickHouseClient, hash: string) => {
  const params = snapshotParams(await readSnapshot(client, 0));
  return client.query<{
    index: number;
    value: string;
    bytecode: string;
    category: string;
    amount: string | null;
    capability: string | null;
    commitment: string | null;
  }>(
    `SELECT input_index AS index, toString(value_satoshis) AS value, lower(hex(locking_bytecode)) AS bytecode,
       lower(hex(token_category)) AS category, toString(fungible_token_amount) AS amount,
       toString(nonfungible_token_capability) AS capability,
       lower(hex(nonfungible_token_commitment)) AS commitment
     FROM ${pinnedView('input_at')}
     WHERE transaction_hash = toFixedString(unhex({hash:String}), 32)
     ORDER BY input_index, value`,
    { ...params, hash }
  );
};

const assertClean = async (
  t: ExecutionContext,
  checker: ClickHouseChecker,
  label: string
) => {
  t.deepEqual(
    await checker.standInCheck(),
    { duplicated: [], mismatched: [] },
    `${label}: stand-in check`
  );
};

/** x saved for node 1 while u and v are unknown: after the grace period, with stand-ins. */
const saveOrphanX = async (
  t: ExecutionContext,
  store: ClickHouseStore,
  client: ClickHouseClient,
  fx: Fixture,
  node: number
) => {
  await store.saveMempoolTransaction(fx.x, [
    { nodeInternalId: node, validatedAt: at(30) },
  ]);
  await store.publishWatermarks();
  t.deepEqual(
    await inputsOf(client, fx.x.hash),
    [fx.standIn(0), fx.standIn(1), fx.standIn(2)],
    'x is stored with stand-in spent outputs'
  );
};

const pendingLive = async (client: ClickHouseClient, hash: string) =>
  client.query<{ node: number; live: string }>(
    `SELECT node_internal_id AS node, toString(sum(sign)) AS live FROM pending_spend
     WHERE spender_transaction_hash = toFixedString(unhex({hash:String}), 32)
       AND commit_seq NOT IN (SELECT commit_seq FROM commit_void)
     GROUP BY node ORDER BY node`,
    { hash }
  );

/**
 * Poll one snapshot at a time while `work` runs: every input of x has exactly
 * one visible row, real when its parent output is visible in that snapshot,
 * the stand-in otherwise. Returns the violations and the number of polls.
 */
const pollExactness = async (
  client: ClickHouseClient,
  fx: Fixture,
  work: Promise<unknown>
) => {
  const progress = { done: false };
  const finished = work.finally(() => {
    progress.done = true;
  });
  const violations: string[] = [];
  const states = new Set<string>();
  let polls = 0;
  const parents = new Map([
    [`${fx.u.hash}:0`, 0],
    [`${fx.u.hash}:1`, 1],
    [`${fx.v.hash}:0`, 2],
  ]);
  for (;;) {
    const last = progress.done;
    const params = snapshotParams(await readSnapshot(client, 0));
    const [inputs, outputs] = await Promise.all([
      client.query<{ index: number; value: string }>(
        `SELECT input_index AS index, toString(value_satoshis) AS value FROM ${pinnedView(
          'input_at'
        )} WHERE transaction_hash = toFixedString(unhex({hash:String}), 32)`,
        { ...params, hash: fx.x.hash }
      ),
      client.query<{ key: string }>(
        `SELECT concat(lower(hex(transaction_hash)), ':', toString(output_index)) AS key
         FROM ${pinnedView('output_at')}
         WHERE transaction_hash IN (toFixedString(unhex({u:String}), 32), toFixedString(unhex({v:String}), 32))`,
        { ...params, u: fx.u.hash, v: fx.v.hash }
      ),
    ]);
    polls += 1;
    if (inputs.length > 0) {
      const visible = new Set(outputs.map((row) => row.key));
      states.add(visible.size === 0 ? 'parents hidden' : 'parents visible');
      [0, 1, 2].forEach((index) => {
        const rows = inputs.filter((row) => Number(row.index) === index);
        const parentVisible = [...parents].some(
          ([key, input]) => input === index && visible.has(key)
        );
        const want = parentVisible ? fx.expected[index]!.value : '0';
        if (rows.length !== 1 || rows[0]!.value !== want) {
          violations.push(
            `poll ${polls}: input ${index} rows ${JSON.stringify(
              rows
            )}, parent visible ${String(parentVisible)}`
          );
        }
      });
    }
    if (last) break;
  }
  await finished;
  return {
    polls,
    states: [...states].sort((a, b) => (a < b ? -1 : Number(a > b))),
    violations,
  };
};

const modes: Utxo[] = ['on', 'off'];

for (const utxo of modes) {
  e2e(
    `[e2e] stand-in (utxo ${utxo}): mempool child with unknown parents, agent restart, then a block with parents and child: the child's inputs carry the real spent outputs`,
    async (t) => {
      const { checker, client, fx, nodes, openStore, store } = await setup(
        t,
        'restart',
        utxo
      );
      await saveOrphanX(t, store, client, fx, nodes.node1);
      t.deepEqual(await pendingLive(client, fx.x.hash), [
        { live: '3', node: nodes.node1 },
      ]);
      const before = await checker.standInCheck();
      t.deepEqual(before.mismatched, [], 'parents not stored: no mismatch yet');
      await store.close();
      await new Promise((resolve) => {
        setTimeout(resolve, leaseTtlMs + 100);
      });
      const restarted = await openStore();
      t.is(restarted.standIns.size, 2, 'two unresolved groups loaded');
      await restarted.saveBlock({
        block: fx.blockUVX,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(nodes.node1, at(40))],
      });
      await restarted.publishWatermarks();
      t.deepEqual(await inputsOf(client, fx.x.hash), fx.expected);
      t.deepEqual(await pendingLive(client, fx.x.hash), [
        { live: '0', node: nodes.node1 },
      ]);
      t.is(restarted.standIns.size, 0);
      await assertClean(t, checker, 'after the block');
      t.deepEqual(await badUtxoSums(client), []);
      t.deepEqual(await checker.mempool('node-one'), []);
    }
  );

  e2e(
    `[e2e] stand-in (utxo ${utxo}): the same without a restart; every snapshot during the block save shows exactly one row per input`,
    async (t) => {
      /* every step of the block commit takes 50 ms more, so the poller sees each state */
      let slow = false;
      const { checker, client, fx, nodes, store } = await setup(
        t,
        'norestart',
        utxo,
        async (_step, context) => {
          if (slow && context.kind === 'block') {
            await new Promise((resolve) => {
              setTimeout(resolve, 50);
            });
          }
        }
      );
      await saveOrphanX(t, store, client, fx, nodes.node1);
      slow = true;
      const { polls, states, violations } = await pollExactness(
        client,
        fx,
        store
          .saveBlock({
            block: fx.blockUVX,
            isSavedTransaction: notSaved,
            nodeAcceptances: [acceptance(nodes.node1, at(40))],
          })
          .then(async () => store.publishWatermarks())
      );
      t.log(`${polls} snapshots polled`);
      t.deepEqual(violations, []);
      t.deepEqual(states, ['parents hidden', 'parents visible']);
      t.deepEqual(await inputsOf(client, fx.x.hash), fx.expected);
      await assertClean(t, checker, 'after the block');
      t.deepEqual(await badUtxoSums(client), []);
    }
  );

  e2e(
    `[e2e] stand-in (utxo ${utxo}): parents in an earlier block than the child's block, then the child's block`,
    async (t) => {
      const { checker, client, fx, nodes, store } = await setup(
        t,
        'earlier',
        utxo
      );
      await saveOrphanX(t, store, client, fx, nodes.node1);
      await store.saveBlock({
        block: fx.blockUV,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(nodes.node1, at(40))],
      });
      await store.publishWatermarks();
      t.deepEqual(
        await inputsOf(client, fx.x.hash),
        fx.expected,
        'resolved while x is still in the mempool'
      );
      t.deepEqual(
        (await checker.mempool('node-one')).map((row) => row.hash),
        [fx.x.hash]
      );
      await assertClean(t, checker, 'after the parents block');
      await store.saveBlock({
        block: fx.blockX,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(nodes.node1, at(50))],
      });
      await store.publishWatermarks();
      t.deepEqual(await inputsOf(client, fx.x.hash), fx.expected);
      await assertClean(t, checker, 'after the child block');
      t.deepEqual(await badUtxoSums(client), []);
    }
  );

  e2e(
    `[e2e] stand-in (utxo ${utxo}): parents arrive in the mempool, one after the other`,
    async (t) => {
      const { checker, client, fx, nodes, store } = await setup(
        t,
        'mempool',
        utxo
      );
      await saveOrphanX(t, store, client, fx, nodes.node1);
      await store.saveMempoolTransaction(fx.u, [
        { nodeInternalId: nodes.node1, validatedAt: at(35) },
      ]);
      await store.publishWatermarks();
      t.deepEqual(await inputsOf(client, fx.x.hash), [
        fx.expected[0],
        fx.expected[1],
        fx.standIn(2),
      ]);
      await assertClean(t, checker, 'after u');
      await store.saveMempoolTransaction(fx.v, [
        { nodeInternalId: nodes.node2, validatedAt: at(36) },
      ]);
      await store.publishWatermarks();
      t.deepEqual(await inputsOf(client, fx.x.hash), fx.expected);
      await assertClean(t, checker, 'after v');
      t.deepEqual(await badUtxoSums(client), []);
      t.is(store.standIns.size, 0);
    }
  );
}

e2e(
  '[e2e] stand-in: a group the writer forgot (an epoch that stopped before resolving it) is repaired at the next start',
  async (t) => {
    const { checker, client, fx, nodes, openStore, store } = await setup(
      t,
      'repair',
      'on'
    );
    await saveOrphanX(t, store, client, fx, nodes.node1);
    store.standIns.clear();
    await store.saveBlock({
      block: fx.blockUV,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(nodes.node1, at(40))],
    });
    await store.publishWatermarks();
    t.is(
      (await checker.standInCheck()).mismatched.length,
      3,
      'without the registry the parents block leaves the stand-ins (the bug)'
    );
    await store.close();
    await new Promise((resolve) => {
      setTimeout(resolve, leaseTtlMs + 100);
    });
    const restarted = await openStore();
    await restarted.publishWatermarks();
    t.is(restarted.standIns.size, 0);
    t.deepEqual(await inputsOf(client, fx.x.hash), fx.expected);
    await assertClean(t, checker, 'after the repair');
    const repairs = (await restarted.commitLog.listCommits()).filter(
      (commit) => commit.kind === 'fill_pending' && commit.state === 'committed'
    );
    t.is(repairs.length, 3, 'two stand-in commits and one repair commit');
  }
);

const deferredSignal = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

for (const pauseAt of ['stand-in', 'stand-in-settled'] as const) {
  e2e(
    `[e2e] stand-in: the parents' block is saved while the child's commit is paused at "${pauseAt}" (group ${
      pauseAt === 'stand-in'
        ? 'open: the child resolves it'
        : 'live: the block resolves it'
    })`,
    async (t) => {
      const { checker, client, fx, nodes, openStore, store } = await setup(
        t,
        `race_${pauseAt.replace(/-/gu, '_')}`,
        'on'
      );
      await store.close();
      await new Promise((resolve) => {
        setTimeout(resolve, leaseTtlMs + 100);
      });
      const reached = deferredSignal();
      const release = deferredSignal();
      let paused = false;
      const racing = await openStore(async (step, context) => {
        if (!paused && step === pauseAt && context.kind === 'mempool_batch') {
          paused = true;
          reached.resolve();
          await release.promise;
        }
      });
      const savingX = racing.saveMempoolTransaction(fx.x, [
        { nodeInternalId: nodes.node1, validatedAt: at(30) },
      ]);
      await reached.promise;
      // node 2 only: the block does not wait for x's node-1 operation to settle
      const savingBlock = racing.saveBlock({
        block: fx.blockUV,
        isSavedTransaction: notSaved,
        nodeAcceptances: [acceptance(nodes.node2, at(40))],
      });
      if (pauseAt === 'stand-in') {
        await savingBlock;
      } else {
        await new Promise((resolve) => {
          setTimeout(resolve, 300);
        });
      }
      const poll = pollExactness(
        client,
        fx,
        Promise.all([savingX, savingBlock])
      );
      release.resolve();
      const { violations } = await poll;
      t.deepEqual(violations, []);
      await racing.publishWatermarks();
      t.deepEqual(await inputsOf(client, fx.x.hash), fx.expected);
      await assertClean(t, checker, 'after both');
      const commits = await racing.commitLog.listCommits();
      const resolutions = await client.query<{ by: string }>(
        'SELECT DISTINCT toString(commit_seq) AS by FROM input_stand_in_resolution'
      );
      const kinds = resolutions.map(
        (row) =>
          commits.find((commit) => commit.seq === BigInt(row.by))?.kind ?? '?'
      );
      t.deepEqual(
        kinds,
        [pauseAt === 'stand-in' ? 'mempool_batch' : 'block'],
        'one commit resolved both groups'
      );
      t.is(racing.standIns.size, 0);
    }
  );
}

e2e(
  '[e2e] stand-in: a crash between any two steps of the child commit leaves one row per input, before and after recovery',
  async (t) => {
    t.timeout(600_000);
    const { checker, client, fx, nodes, openStore, store } = await setup(
      t,
      'crash',
      'on'
    );
    await store.close();
    let crashPoints = 0;
    for (let crashAt = 0; ; crashAt += 1) {
      await new Promise((resolve) => {
        setTimeout(resolve, leaseTtlMs + 100);
      });
      let calls = 0;
      let step = '';
      const crashing = await openStore((name, context) => {
        if (context.kind !== 'mempool_batch') return;
        if (calls === crashAt) {
          step = name;
          throw new SimulatedCrash(`crash after ${name}`);
        }
        calls += 1;
      });
      const outcome = await crashing
        .saveMempoolTransaction(fx.x, [
          { nodeInternalId: nodes.node1, validatedAt: at(30) },
        ])
        .then(() => 'ok' as const)
        .catch((error: unknown) => {
          if (error instanceof SimulatedCrash) return 'crash' as const;
          throw error;
        });
      if (outcome === 'ok') {
        await crashing.close();
        break;
      }
      crashPoints += 1;
      await crashing.simulateCrash();
      const label = `crash after ${step}`;
      await assertClean(t, checker, `${label}: before recovery`);
      await new Promise((resolve) => {
        setTimeout(resolve, leaseTtlMs + 100);
      });
      const recovered = await openStore();
      await recovered.publishWatermarks();
      await assertClean(t, checker, `${label}: after recovery`);
      const rows = await inputsOf(client, fx.x.hash);
      t.true(
        rows.length === 0 ||
          isDeepStrictEqual(rows, [
            fx.standIn(0),
            fx.standIn(1),
            fx.standIn(2),
          ]),
        `${label}: x absent or with its three stand-ins (${JSON.stringify(
          rows
        )})`
      );
      if (rows.length > 0) {
        // x committed before the crash: the parents' block resolves it now
        await recovered.saveBlock({
          block: fx.blockUV,
          isSavedTransaction: notSaved,
          nodeAcceptances: [acceptance(nodes.node1, at(40))],
        });
        await recovered.publishWatermarks();
        t.deepEqual(await inputsOf(client, fx.x.hash), fx.expected, label);
        await assertClean(t, checker, `${label}: after the parents`);
        await recovered.close();
        break;
      }
      await recovered.close();
    }
    t.log(`${crashPoints} crash points`);
    t.true(crashPoints >= 4);
  }
);
