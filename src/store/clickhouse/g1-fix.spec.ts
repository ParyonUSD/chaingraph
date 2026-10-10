/* eslint-disable @typescript-eslint/no-magic-numbers, functional/no-loop-statement, functional/no-let, no-await-in-loop, complexity, functional/no-throw-statement, @typescript-eslint/init-declarations, max-params */
// cspell:ignore clickhouse seqs econnreset
/**
 * Gate G1 fix pass (docs/clickhouse-port/g1-fix-pass.md): the lab conditions
 * of the c1 replay at small scale. Multi-block batches, an in-flight cap,
 * child-before-parent across batches, a pressured recent-output cache, a
 * failed batch (mid-commit and before its seq), a transient ClickHouse error
 * mid-batch and a lost writer lease mid-batch. Every test asserts:
 * - no commit with a seq at or below a published watermark of its scope (or node 0) is
 *   ever voided (a poller watches `visibility` and `commit_void`);
 * - per-node parity with an independent recomputation from the blocks: the
 *   node sees a prefix of its chain (no hole), exactly its transactions and
 *   UTXOs, and every input carries its spent output's value (no stand-in);
 * - pending spends resolve without waiting for the timeout when the parent
 *   output is stored.
 * Scratch databases are `ch1_fix_*` (dropped on teardown).
 */
import { ClickHouseError } from '@clickhouse/client';
import test from 'ava';
import type { ExecutionContext } from 'ava';

import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import type { FaultInjector } from './block-commit.js';
import type {
  ClickHouseStore,
  ClickHouseStoreOptions,
  StoreDiagnostic,
} from './clickhouse-store.js';
import type { ClickHouseClient, ClickHouseRequestInfo } from './client.js';
import {
  acceptance,
  badUtxoSums,
  connectionFor,
  expectedUnspent,
  leaseTtlMs,
  makeBlock,
  makeTx,
  nodeView,
  notSaved,
  p2pkh,
  registerNodes,
  scratch,
  txHashes,
  zeroHash,
} from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const scratchPrefix = 'ch1_fix';

const sleep = async (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/* -------------------------------------------------------------------- */
/* fixtures                                                              */
/* -------------------------------------------------------------------- */

/**
 * `count` blocks; block i has a coinbase (2 outputs) and, when `spends`, a
 * tx spending the previous block's coinbase output 0 and the previous tx's
 * output 0 (so every batch spends outputs of the batch before it).
 */
const chainOf = (count: number, label: string, spends = true) => {
  const blocks: ChaingraphBlock[] = [];
  let previousHash = zeroHash;
  let previousCoinbase: string | undefined;
  let previousTx: string | undefined;
  for (let height = 0; height < count; height += 1) {
    const coinbase = makeTx({
      coinbase: true,
      label: `${label}-c${height}`,
      outputs: [
        { lockingBytecode: p2pkh(`${label}-miner`), valueSatoshis: 1_000n },
        {
          lockingBytecode: p2pkh(`${label}-keep`),
          valueSatoshis: BigInt(10 + height),
        },
      ],
    });
    const transactions: ChaingraphTransaction[] = [coinbase];
    const outpoints: [string, number][] = [];
    if (spends && previousCoinbase !== undefined) {
      outpoints.push([previousCoinbase, 0]);
    }
    if (spends && previousTx !== undefined) outpoints.push([previousTx, 0]);
    if (outpoints.length > 0) {
      const transaction = makeTx({
        label: `${label}-t${height}`,
        outputs: [
          { lockingBytecode: p2pkh(`${label}-chain`), valueSatoshis: 900n },
          { lockingBytecode: p2pkh(`${label}-side`), valueSatoshis: 1n },
        ],
        spends: outpoints,
      });
      transactions.push(transaction);
      previousTx = transaction.hash;
    }
    const block = makeBlock(
      height,
      previousHash,
      transactions,
      `${label}-block-${height}`
    );
    blocks.push(block);
    previousHash = block.hash;
    previousCoinbase = coinbase.hash;
  }
  return blocks;
};

/** Value of every output of `blocks`, by `hash:index`. */
const outputValues = (blocks: readonly ChaingraphBlock[]) => {
  const values = new Map<string, string>();
  blocks.forEach((block) => {
    block.transactions.forEach((transaction) => {
      transaction.outputs.forEach((output, index) => {
        values.set(
          `${transaction.hash}:${index}`,
          output.valueSatoshis.toString()
        );
      });
    });
  });
  return values;
};

/**
 * Per-node parity with an independent recomputation: the node sees a prefix
 * of `chain` (no hole), exactly the prefix's transactions and UTXOs, and
 * every non-coinbase input of the prefix carries its spent output's value
 * (never the stand-in of a spend whose output was unknown).
 */
const assertNodeParity = async (
  t: ExecutionContext,
  client: ClickHouseClient,
  node: number,
  chain: readonly ChaingraphBlock[],
  options: { utxo: boolean; minBlocks?: number; exactBlocks?: number }
) => {
  const view = await nodeView(client, node);
  const prefix = chain.slice(0, view.blocks.length);
  t.deepEqual(
    view.blocks,
    prefix.map((block) => block.hash),
    `node ${node}: a prefix of the chain, no hole (sees ${view.blocks.length} of ${chain.length})`
  );
  if (options.exactBlocks !== undefined) {
    t.is(view.blocks.length, options.exactBlocks, `node ${node}: block count`);
  }
  if (options.minBlocks !== undefined) {
    t.true(
      view.blocks.length >= options.minBlocks,
      `node ${node}: at least ${options.minBlocks} blocks (sees ${view.blocks.length})`
    );
  }
  t.deepEqual(view.txs, txHashes(prefix), `node ${node}: transactions`);
  if (options.utxo) {
    t.deepEqual(view.utxo, expectedUnspent(prefix), `node ${node}: utxo`);
    t.deepEqual(view.utxoByScript, view.utxo, `node ${node}: utxo_by_script`);
  } else {
    t.deepEqual(view.utxo, [], `node ${node}: no utxo rows with utxo off`);
  }
  const values = outputValues(chain);
  const inputs = new Map(view.inputs.map((row) => [row.key, row.value]));
  const wrong: string[] = [];
  prefix.forEach((block) => {
    block.transactions.forEach((transaction) => {
      if (transaction.isCoinbase) return;
      transaction.inputs.forEach((input, index) => {
        const key = `${transaction.hash}:${index}`;
        const expected = values.get(
          `${input.outpointTransactionHash}:${input.outpointIndex}`
        );
        const actual = inputs.get(key);
        if (actual !== expected) {
          wrong.push(
            `${key} = ${String(actual)} (expected ${String(expected)})`
          );
        }
      });
    });
  });
  t.deepEqual(wrong, [], `node ${node}: input values (stand-ins are 0)`);
  t.deepEqual(await badUtxoSums(client), [], 'UTXO sums 0 or 1');
};

/**
 * Watches the gate from outside, as a reader would: polls the published
 * watermarks and the void set; a seq that becomes void is checked against
 * every watermark published before it appeared (its node scope and node 0).
 */
const watchVoids = (client: ClickHouseClient) => {
  const maxSeen = new Map<number, bigint>();
  const seenVoid = new Set<string>();
  const violations: string[] = [];
  const voided: string[] = [];
  let polls = 0;
  let stopped = false;
  const poll = async () => {
    const marks = await client.query<{ node: number; v: string }>(
      'SELECT node_internal_id AS node, toString(max(visible_seq)) AS v FROM visibility GROUP BY node'
    );
    const voids = await client.query<{ seq: string; scope: number[] }>(
      `SELECT toString(v.commit_seq) AS seq, any(c.node_scope) AS scope
       FROM (SELECT DISTINCT commit_seq FROM commit_void) AS v
       LEFT JOIN commit_log AS c ON c.commit_seq = v.commit_seq
       GROUP BY v.commit_seq`
    );
    voids.forEach(({ seq, scope }) => {
      if (seenVoid.has(seq)) return;
      seenVoid.add(seq);
      voided.push(seq);
      [0, ...scope.map(Number)].forEach((node) => {
        const watermark = maxSeen.get(node) ?? 0n;
        if (watermark >= BigInt(seq)) {
          violations.push(
            `seq ${seq} voided after visible(${node}) reached ${watermark}`
          );
        }
      });
    });
    marks.forEach(({ node, v }) => {
      const value = BigInt(v);
      if (value > (maxSeen.get(Number(node)) ?? 0n)) {
        maxSeen.set(Number(node), value);
      }
    });
    polls += 1;
  };
  const loop = (async () => {
    // eslint-disable-next-line no-unmodified-loop-condition, @typescript-eslint/no-unnecessary-condition
    while (!stopped) {
      await poll().catch(() => undefined);
      await sleep(15);
    }
    await poll();
  })();
  return {
    stop: async () => {
      stopped = true;
      await loop;
      return { polls, violations, voided };
    },
  };
};

/** Save `blocks` concurrently (as the agent's buffer does) and wait for every commit. */
const saveAll = async (
  store: ClickHouseStore,
  blocks: readonly ChaingraphBlock[],
  nodes: readonly number[]
) => {
  const outcomes = await Promise.allSettled(
    blocks.map(async (block) => {
      const result = await store.saveBlock({
        block,
        isSavedTransaction: notSaved,
        nodeAcceptances: nodes.map((node) => acceptance(node)),
      });
      await result.committed;
      return result;
    })
  );
  return outcomes;
};

const commitStates = async (client: ClickHouseClient) =>
  client.query<{ seq: string; state: string; reason: string }>(
    `SELECT toString(commit_seq) AS seq, toString(state) AS state, abort_reason AS reason
     FROM commit_log FINAL WHERE kind = 'block' ORDER BY commit_seq`
  );

const fixScratch = async (
  t: ExecutionContext,
  label: string,
  options: Partial<ClickHouseStoreOptions>
) => scratch(t, label, options, scratchPrefix);

/** Open a store whose ClickHouse client runs `faultBeforeRequest`. */
const openWithClientFault = async (
  harness: Awaited<ReturnType<typeof fixScratch>>,
  faultBeforeRequest: (request: ClickHouseRequestInfo) => Promise<void> | void,
  fault?: FaultInjector
) =>
  harness.openStore(fault, {
    connection: { ...connectionFor(harness.name), faultBeforeRequest },
  });

/* -------------------------------------------------------------------- */
/* Bug 1: a failed batch must not leave a hole                           */
/* -------------------------------------------------------------------- */

const failedBatchScenario = (
  failAt: 'begin' | 'output',
  utxo: 'off' | 'on'
) => {
  e2e(
    `[e2e] G1 fix: a block batch failing at \`${failAt}\` (utxo ${utxo}) leaves no hole: nothing later of its node becomes visible; a restart re-saves exactly`,
    async (t) => {
      t.timeout(240_000);
      const chain = chainOf(40, `hole-${failAt}-${utxo}`);
      const errors: unknown[] = [];
      const harness = await fixScratch(t, `hole_${failAt}_${utxo}`, {
        maxBlocksPerCommit: 4,
        maxInFlightSaves: 16,
        onError: (error) => errors.push(error),
        pendingSpendTimeoutMs: 3_000,
        utxo,
      });
      const control = { begun: 0 };
      const store = await harness.openStore((step, context) => {
        if (context.kind !== 'block' || step !== failAt) return;
        control.begun += 1;
        // the third block commit fails (a non-transient error)
        if (control.begun === 3) {
          throw new Error(`injected failure at ${failAt}`);
        }
      });
      const { node1 } = await registerNodes(store);
      const watch = watchVoids(harness.client);
      const started = Date.now();
      const outcomes = await saveAll(store, chain, [node1]);
      await store.operations.drain();
      await store.publishWatermarks().catch(() => undefined);
      const elapsedMs = Date.now() - started;
      const rejected = outcomes.filter((o) => o.status === 'rejected').length;
      t.log(
        `${rejected} of ${chain.length} saves rejected, ${elapsedMs} ms, ${errors.length} background errors`
      );
      t.true(rejected > 0, 'the injected failure reached the caller');
      await assertNodeParity(t, harness.client, node1, chain, {
        utxo: utxo === 'on',
      });
      const { violations } = await watch.stop();
      t.deepEqual(violations, [], 'no void at or below a published watermark');

      // restart (as the agent does after a failed save): re-save everything
      await store.close();
      const restarted = await harness.openStore();
      const again = await saveAll(restarted, chain, [node1]);
      t.true(again.every((o) => o.status === 'fulfilled'));
      await restarted.operations.drain();
      await restarted.publishWatermarks();
      await assertNodeParity(t, harness.client, node1, chain, {
        exactBlocks: chain.length,
        utxo: utxo === 'on',
      });
    }
  );
};

failedBatchScenario('output', 'on');
failedBatchScenario('output', 'off');
failedBatchScenario('begin', 'on');
failedBatchScenario('begin', 'off');

e2e(
  '[e2e] G1 fix: a later independent batch never commits while an earlier batch of its node can still fail (commit order)',
  async (t) => {
    t.timeout(120_000);
    // coinbase-only blocks: no batch reads another's outputs
    const chain = chainOf(12, 'order', false);
    const harness = await fixScratch(t, 'order', {
      maxBlocksPerCommit: 2,
      maxInFlightSaves: 16,
      pendingSpendTimeoutMs: 3_000,
    });
    const control = { held: 0 };
    const store = await harness.openStore(async (step, context) => {
      if (context.kind !== 'block' || step !== 'rows-written') return;
      control.held += 1;
      if (control.held === 2) {
        // the second batch has written every row; it fails before `committed`
        await sleep(1_500);
        throw new Error('injected failure after rows-written');
      }
    });
    const { node1 } = await registerNodes(store);
    const watch = watchVoids(harness.client);
    await saveAll(store, chain, [node1]);
    await store.operations.drain();
    await store.publishWatermarks().catch(() => undefined);
    await assertNodeParity(t, harness.client, node1, chain, { utxo: true });
    const { violations } = await watch.stop();
    t.deepEqual(violations, []);
    const states = await commitStates(harness.client);
    t.log(states.map((row) => `${row.seq} ${row.state}`).join(', '));
  }
);

/* -------------------------------------------------------------------- */
/* transient ClickHouse errors                                           */
/* -------------------------------------------------------------------- */

e2e(
  '[e2e] G1 fix: a socket reset on a lookup and a server socket timeout (209) on an insert mid-batch are retried: nothing aborted, exact',
  async (t) => {
    t.timeout(180_000);
    const chain = chainOf(30, 'transient');
    const harness = await fixScratch(t, 'transient', {
      maxBlocksPerCommit: 4,
      maxInFlightSaves: 16,
      pendingSpendTimeoutMs: 20_000,
    });
    const injected = { insert: 0, query: 0 };
    const store = await openWithClientFault(harness, (request) => {
      if (
        request.kind === 'query' &&
        request.sql.includes('FROM transaction') &&
        injected.query < 2
      ) {
        injected.query += 1;
        throw Object.assign(new Error('socket hang up'), {
          code: 'ECONNRESET',
        });
      }
      if (
        request.kind === 'insert' &&
        request.sql.startsWith('INSERT INTO `input`') &&
        injected.insert < 2
      ) {
        injected.insert += 1;
        throw new ClickHouseError({
          code: '209',
          message:
            'Timeout exceeded while reading from socket (peer: test, 30000 ms).',
          type: 'SOCKET_TIMEOUT',
        });
      }
    });
    const { node1 } = await registerNodes(store);
    const watch = watchVoids(harness.client);
    const outcomes = await saveAll(store, chain, [node1]);
    await store.operations.drain();
    await store.publishWatermarks();
    t.deepEqual(injected, { insert: 2, query: 2 }, 'faults were injected');
    t.deepEqual(
      outcomes
        .filter((o): o is PromiseRejectedResult => o.status === 'rejected')
        .map((o) => String(o.reason)),
      [],
      'every save succeeded'
    );
    await assertNodeParity(t, harness.client, node1, chain, {
      exactBlocks: chain.length,
      utxo: true,
    });
    const { violations, voided } = await watch.stop();
    t.deepEqual(violations, []);
    t.deepEqual(voided, [], 'nothing was aborted');
  }
);

/* -------------------------------------------------------------------- */
/* Bug 2: pending spends resolve once the parent is stored               */
/* -------------------------------------------------------------------- */

e2e(
  '[e2e] G1 fix: a parked child resolves at once when its parent was committed (by another node) and evicted from the recent-output cache meanwhile',
  async (t) => {
    t.timeout(120_000);
    const timeoutMs = 10_000;
    const harness = await fixScratch(t, 'evicted', {
      pendingSpendTimeoutMs: timeoutMs,
      recentOutputCapacity: 1,
    });
    const parentTx = makeTx({
      label: 'evicted-parent',
      outputs: [
        { lockingBytecode: p2pkh('evicted-a'), valueSatoshis: 4_321n },
        { lockingBytecode: p2pkh('evicted-b'), valueSatoshis: 5n },
      ],
      spends: [],
    });
    const parentCoinbase = makeTx({
      coinbase: true,
      label: 'evicted-parent-cb',
      outputs: [{ lockingBytecode: p2pkh('miner'), valueSatoshis: 1n }],
    });
    const childTx = makeTx({
      label: 'evicted-child',
      outputs: [{ lockingBytecode: p2pkh('evicted-c'), valueSatoshis: 4_000n }],
      spends: [[parentTx.hash, 0]],
    });
    const childCoinbase = makeTx({
      coinbase: true,
      label: 'evicted-child-cb',
      outputs: [{ lockingBytecode: p2pkh('miner'), valueSatoshis: 2n }],
    });
    const fillerCoinbase = makeTx({
      coinbase: true,
      label: 'evicted-filler-cb',
      outputs: [
        { lockingBytecode: p2pkh('miner'), valueSatoshis: 3n },
        { lockingBytecode: p2pkh('miner2'), valueSatoshis: 4n },
      ],
    });
    const parentBlock = makeBlock(1, zeroHash, [parentCoinbase, parentTx]);
    const childBlock = makeBlock(2, parentBlock.hash, [childCoinbase, childTx]);
    const fillerBlock = makeBlock(3, childBlock.hash, [fillerCoinbase]);
    const hold = { release: undefined as (() => void) | undefined };
    let parked: Promise<void> | undefined;
    const store = await harness.openStore(async (step, context) => {
      if (
        context.kind === 'block' &&
        step === 'incomplete' &&
        parked === undefined
      ) {
        // the child is parked: hold it before it subscribes to its pending outpoints
        parked = new Promise<void>((resolve) => {
          hold.release = resolve;
        });
        await parked;
      }
    });
    const { node1, node2 } = await registerNodes(store);
    // the child (node 1) arrives first: its parent is unknown, it parks
    const child = store.saveBlock({
      block: childBlock,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    for (let tries = 0; tries < 500 && hold.release === undefined; tries += 1) {
      await sleep(10);
    }
    t.not(hold.release, undefined, 'the child parked');
    // the parent is saved and committed by node 2, then evicted from the cache
    await store.saveBlock({
      block: parentBlock,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node2)],
    });
    await store.saveBlock({
      block: fillerBlock,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node2)],
    });
    const released = Date.now();
    hold.release!();
    const result = await child;
    await result.committed;
    const waitedMs = Date.now() - released;
    t.log(`child committed ${waitedMs} ms after it was released`);
    t.true(
      waitedMs < timeoutMs / 2,
      `the child did not wait for the pending-spend timeout (${waitedMs} ms)`
    );
    await store.publishWatermarks();
    const view = await nodeView(harness.client, node1);
    const input = view.inputs.find((row) => row.key === `${childTx.hash}:0`);
    t.is(input?.value, '4321', 'the input carries the parent output value');
  }
);

/* -------------------------------------------------------------------- */
/* lab conditions                                                        */
/* -------------------------------------------------------------------- */

const labScenario = (utxo: 'off' | 'on') => {
  e2e(
    `[e2e] G1 fix: lab conditions (utxo ${utxo}): batches, cap 16, child-before-parent across batches, pressured cache, transient errors, a lease loss mid-batch: exact, no timeout wait, no void after visible`,
    async (t) => {
      t.timeout(300_000);
      const chain = chainOf(160, `lab-${utxo}`);
      const errors: unknown[] = [];
      const fatal: unknown[] = [];
      const diagnostics: unknown[] = [];
      const timeoutMs = 60_000;
      const harness = await fixScratch(t, `lab_${utxo}`, {
        maxBlocksPerCommit: 8,
        maxInFlightSaves: 16,
        onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
        onError: (error) => errors.push(error),
        onFatal: (error) => fatal.push(error),
        pendingSpendTimeoutMs: timeoutMs,
        recentOutputCapacity: 4,
        utxo,
      });
      const injected = { insert: 0, query: 0 };
      const stall = { commits: 0, done: false };
      const store = await openWithClientFault(
        harness,
        (request) => {
          if (
            request.kind === 'query' &&
            request.sql.includes('FROM output') &&
            injected.query < 3
          ) {
            injected.query += 1;
            throw Object.assign(new Error('socket hang up'), {
              code: 'ECONNRESET',
            });
          }
          if (
            request.kind === 'insert' &&
            request.sql.startsWith('INSERT INTO `transaction`') &&
            injected.insert < 3
          ) {
            injected.insert += 1;
            throw new ClickHouseError({
              code: '209',
              message: 'Timeout exceeded while reading from socket (test).',
              type: 'SOCKET_TIMEOUT',
            });
          }
        },
        (step, context) => {
          if (context.kind !== 'block' || step !== 'node_block') return;
          stall.commits += 1;
          if (stall.commits === 5 && !stall.done) {
            stall.done = true;
            // a synchronous stretch past the lease deadline, mid-batch
            const until = Date.now() + leaseTtlMs + 1_000;
            while (Date.now() < until) {
              // busy: the heartbeat timer cannot run
            }
          }
        }
      );
      const { node1 } = await registerNodes(store);
      const watch = watchVoids(harness.client);
      // chunks of 24 blocks in reverse chunk order: children before parents across batches
      const chunks: ChaingraphBlock[][] = [];
      for (let start = 0; start < chain.length; start += 24) {
        chunks.push(chain.slice(start, start + 24));
      }
      const order = [chunks[0]!, ...chunks.slice(1).reverse()].flat();
      const started = Date.now();
      const outcomes = await saveAll(store, order, [node1]);
      await store.operations.drain();
      await store.publishWatermarks();
      const elapsedMs = Date.now() - started;
      t.log(
        `${chain.length} blocks in ${elapsedMs} ms; injected ${JSON.stringify(
          injected
        )}; ${errors.length} background errors; diagnostics ${JSON.stringify(
          diagnostics
        )}`
      );
      t.deepEqual(
        outcomes
          .filter((o): o is PromiseRejectedResult => o.status === 'rejected')
          .map((o) => String(o.reason)),
        []
      );
      t.deepEqual(fatal, []);
      t.true(stall.done, 'the lease was lost mid-batch');
      t.true(
        elapsedMs < timeoutMs,
        `no save waited for the pending-spend timeout (${elapsedMs} ms)`
      );
      await assertNodeParity(t, harness.client, node1, chain, {
        exactBlocks: chain.length,
        utxo: utxo === 'on',
      });
      const { violations } = await watch.stop();
      t.deepEqual(violations, []);
    }
  );
};

labScenario('on');
labScenario('off');

/* -------------------------------------------------------------------- */
/* fix pass 3, item 1: shutdown never voids a committed commit           */
/* -------------------------------------------------------------------- */

const shutdownScenario = (utxo: 'off' | 'on') => {
  e2e(
    `[e2e] fix pass 3: SIGTERM while a batch's committed row is in flight (utxo ${utxo}) aborts only open commits: no void_refused, no abort_failed; a restart resumes exactly`,
    async (t) => {
      t.timeout(180_000);
      const chain = chainOf(40, `sigterm-${utxo}`);
      const diagnostics: StoreDiagnostic[] = [];
      const harness = await fixScratch(t, `sigterm_${utxo}`, {
        maxBlocksPerCommit: 4,
        maxInFlightSaves: 16,
        onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
        pendingSpendTimeoutMs: 20_000,
        utxo,
      });
      const control: {
        rowsWritten: number;
        armedSeq: bigint | undefined;
        fired: boolean;
        store: ClickHouseStore | undefined;
      } = {
        armedSeq: undefined,
        fired: false,
        rowsWritten: 0,
        store: undefined,
      };
      const store = await openWithClientFault(
        harness,
        async (request) => {
          if (
            control.fired ||
            control.armedSeq === undefined ||
            request.deduplicationToken !==
              `${control.armedSeq}:commit_log:committed`
          ) {
            return;
          }
          // SIGTERM (createStore's handler) lands while the committed row is sent
          control.fired = true;
          control.store?.abandonInFlightWork('received SIGTERM');
          await sleep(30);
        },
        (step, context) => {
          if (context.kind !== 'block' || step !== 'rows-written') return;
          control.rowsWritten += 1;
          if (control.rowsWritten === 3) control.armedSeq = context.seq;
        }
      );
      control.store = store;
      const { node1 } = await registerNodes(store);
      const watch = watchVoids(harness.client);
      await saveAll(store, chain, [node1]);
      await store.operations.drain();
      await store.publishWatermarks().catch(() => undefined);
      t.true(control.fired, 'the shutdown landed mid-commit');
      const states = await commitStates(harness.client);
      const armed = states.find((row) => row.seq === String(control.armedSeq));
      t.is(
        armed?.state,
        'committed',
        'the batch whose commit was in flight committed'
      );
      const events = diagnostics.map((diagnostic) => diagnostic.event);
      t.log(
        `diagnostics: ${JSON.stringify(
          diagnostics.map((d) => ({ event: d.event, seq: d.seq }))
        )}`
      );
      t.false(events.includes('void_refused'), 'no void_refused');
      t.false(events.includes('abort_failed'), 'no abort_failed');
      const committedSeqs = new Set(
        states.filter((row) => row.state === 'committed').map((row) => row.seq)
      );
      t.deepEqual(
        diagnostics
          .filter((d) => d.event === 'commit_void' && committedSeqs.has(d.seq))
          .map((d) => d.seq),
        [],
        'no committed seq was offered for voiding'
      );
      await assertNodeParity(t, harness.client, node1, chain, {
        minBlocks: 4,
        utxo: utxo === 'on',
      });
      const { violations } = await watch.stop();
      t.deepEqual(violations, []);

      // restart: the next start resumes from the visible prefix
      await store.close();
      const restarted = await harness.openStore();
      const again = await saveAll(restarted, chain, [node1]);
      t.true(again.every((o) => o.status === 'fulfilled'));
      await restarted.operations.drain();
      await restarted.publishWatermarks();
      await assertNodeParity(t, harness.client, node1, chain, {
        exactBlocks: chain.length,
        utxo: utxo === 'on',
      });
      t.deepEqual(
        diagnostics
          .filter(
            (d) => d.event === 'void_refused' || d.event === 'abort_failed'
          )
          .map((d) => d.event),
        []
      );
    }
  );
};

shutdownScenario('on');
shutdownScenario('off');

/* -------------------------------------------------------------------- */
/* fix pass 3, item 3: a stream of small blocks makes few commits        */
/* -------------------------------------------------------------------- */

const streamCommits = async (
  t: ExecutionContext,
  label: string,
  batchLingerMs: number
) => {
  const chain = chainOf(151, `stream-${label}`);
  const harness = await fixScratch(t, `stream_${label}`, {
    batchLingerMs,
    pendingSpendTimeoutMs: 20_000,
  });
  const store = await harness.openStore();
  const { node1 } = await registerNodes(store);
  // the agent's catch-up: a block every 4 ms, saves not awaited
  const saves: Promise<unknown>[] = [];
  for (const block of chain.slice(0, 150)) {
    saves.push(
      store
        .saveBlock({
          block,
          isSavedTransaction: notSaved,
          nodeAcceptances: [acceptance(node1)],
        })
        .then(async (result) => result.committed)
    );
    await sleep(4);
  }
  await Promise.all(saves);
  const streamed = (await commitStates(harness.client)).length;
  // a quiet lane: the next block starts at once (no linger at the tip)
  await sleep(Math.max(batchLingerMs, 50) * 2);
  const started = Date.now();
  await store.saveBlock({
    block: chain[150]!,
    isSavedTransaction: notSaved,
    nodeAcceptances: [acceptance(node1)],
  });
  const tipMs = Date.now() - started;
  await store.operations.drain();
  await store.publishWatermarks();
  await assertNodeParity(t, harness.client, node1, chain, {
    exactBlocks: chain.length,
    utxo: true,
  });
  return { streamed, tipMs };
};

e2e(
  '[e2e] fix pass 3: a stream of small blocks lingers into few large commits; a block on a quiet lane starts at once',
  async (t) => {
    t.timeout(180_000);
    const without = await streamCommits(t, 'direct', 0);
    const lingering = await streamCommits(t, 'linger', 300);
    t.log(
      `150 blocks streamed: ${without.streamed} commits without linger, ${lingering.streamed} with 300 ms; tip block saved in ${lingering.tipMs} ms`
    );
    t.true(
      lingering.streamed <= 6,
      `few commits with linger (${lingering.streamed})`
    );
    t.true(lingering.streamed < without.streamed);
    t.true(
      lingering.tipMs < 300,
      `no linger at the tip (${lingering.tipMs} ms)`
    );
  }
);
