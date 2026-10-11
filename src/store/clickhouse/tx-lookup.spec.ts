/* eslint-disable @typescript-eslint/no-magic-numbers, max-params, functional/no-let, @typescript-eslint/naming-convention, camelcase, functional/no-loop-statement */
// cspell:ignore clickhouse coinbases unhex txlookup
import test from 'ava';

import type { ChaingraphBlock } from '../../types/chaingraph.js';

import {
  acceptance,
  makeBlock,
  makeTx,
  notSaved,
  p2pkh,
  scratch,
  zeroHash,
} from './spec-fixtures.js';
import { e2eClickHouseUrl } from './test-support.js';
import {
  coinbaseHeight,
  NewTransactionProof,
  TransactionLookup,
  uniqueToItsBlock,
} from './tx-lookup.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

test('coinbaseHeight: the minimally pushed BIP34 height, else undefined', (t) => {
  t.is(coinbaseHeight('03a0bb0d'), 0x0dbba0);
  t.is(coinbaseHeight('0100'), 0);
  t.is(coinbaseHeight('01ff'), undefined, 'negative');
  t.is(coinbaseHeight('020100'), undefined, 'not minimal');
  // the genesis-era 0x1d00ffff push decodes to a height far ahead (why BIP30 returns at 1,983,702)
  t.is(coinbaseHeight('04ffff001d'), 486_604_799);
  t.is(coinbaseHeight('4c'), undefined);
  t.is(coinbaseHeight(''), undefined);
});

const block = (
  height: number,
  previous: string,
  label: string,
  coinbaseScript?: string
): ChaingraphBlock => {
  const coinbase = makeTx({
    coinbase: true,
    label: `${label}-c`,
    outputs: [{ lockingBytecode: p2pkh('m'), valueSatoshis: 1n }],
  });
  if (coinbaseScript !== undefined) {
    coinbase.inputs[0]!.unlockingBytecode = coinbaseScript;
  }
  return makeBlock(
    height,
    previous,
    [
      coinbase,
      makeTx({
        label: `${label}-t`,
        outputs: [{ lockingBytecode: p2pkh('x'), valueSatoshis: 1n }],
        spends: [['aa'.repeat(32), height]],
      }),
    ],
    label
  );
};

test('uniqueToItsBlock: non-coinbase always; a coinbase only when it commits to its height', (t) => {
  const b = block(5, zeroHash, 'u', '0105');
  t.true(uniqueToItsBlock(b.transactions[1]!, 5));
  t.true(uniqueToItsBlock(b.transactions[0]!, 5));
  t.false(uniqueToItsBlock(b.transactions[0]!, 6));
  t.false(uniqueToItsBlock(block(5, zeroHash, 'v').transactions[0]!, 5));
  t.false(
    uniqueToItsBlock(
      block(2_000_000, zeroHash, 'w', '0380841e').transactions[0]!,
      2_000_000
    ),
    'above the BIP30 re-enable height'
  );
});

const facts = {
  noReorgOrMempool: true,
  nodeCount: 1,
  oneBlockPerHeight: true,
  tipHash: 'tip'.padEnd(64, '0'),
  tipHeight: 9,
};

test('NewTransactionProof: blocks extending the stored tip in this process are new; a second block at a height ends the proof', (t) => {
  const proof = new NewTransactionProof(facts);
  const b10 = block(10, facts.tipHash, 'b10');
  const b11 = block(11, b10.hash, 'b11');
  const orphan12 = block(12, 'ff'.repeat(32), 'o12');
  t.true(proof.note(b10));
  t.true(proof.note(b11));
  t.false(proof.note(orphan12), 'parent not proven');
  t.false(proof.note(block(9, zeroHash, 'b9')), 'at or below the stored tip');
  t.true(proof.enabled);
  // a competing block at 11: nothing more is proven in this process
  t.false(proof.note(block(11, b10.hash, 'b11-alt')));
  t.false(proof.enabled);
  t.false(proof.note(block(12, b11.hash, 'b12')));
});

test('NewTransactionProof: never with two blocks at a stored height, a stored re-org or mempool commit, or two nodes', (t) => {
  const b10 = block(10, facts.tipHash, 'b10');
  t.false(
    new NewTransactionProof({ ...facts, oneBlockPerHeight: false }).note(b10)
  );
  t.false(
    new NewTransactionProof({ ...facts, noReorgOrMempool: false }).note(b10)
  );
  t.false(new NewTransactionProof({ ...facts, nodeCount: 2 }).note(b10));
  const disabled = new NewTransactionProof(facts);
  disabled.disable('re-org');
  t.false(disabled.note(b10));
  // an empty store: the genesis block extends the zero hash at height -1
  t.true(
    new NewTransactionProof({
      ...facts,
      tipHash: zeroHash,
      tipHeight: -1,
    }).note(block(0, zeroHash, 'genesis'))
  );
});

test('TransactionLookup: requests coalesce into sorted chunks of at most chunkSize, at most `concurrency` at once', async (t) => {
  const queries: string[][] = [];
  let inFlight = 0;
  let peak = 0;
  const client = {
    query: async <T>(_sql: string, params?: { [key: string]: unknown }) => {
      inFlight += 1;
      peak = Math.max(peak, inFlight);
      const hashes = (params?.hashes ?? []) as string[];
      queries.push(hashes);
      await new Promise((resolve) => {
        setTimeout(resolve, 10);
      });
      inFlight -= 1;
      return hashes
        .filter((hash) => hash.startsWith('a'))
        .map((hash) => ({
          commit_seq: '7',
          hash_hex: hash,
          internal_id: '1',
        })) as unknown as T[];
    },
  };
  const lookup = new TransactionLookup(client, () => [], {
    chunkSize: 5,
    concurrency: 2,
    gatherMs: 2,
  });
  const requests = Array.from({ length: 30 }, (_, index) => [
    `${index % 2 === 0 ? 'a' : 'b'}${String(index).padStart(2, '0')}`,
  ]);
  const results = await Promise.all(
    requests.map(async (hashes) => lookup.lookup(hashes))
  );
  results.forEach((found, index) => {
    t.is(found.size, index % 2 === 0 ? 1 : 0);
  });
  t.true(peak <= 2);
  t.true(queries.every((hashes) => hashes.length <= 5));
  queries.forEach((hashes) => {
    t.deepEqual(
      hashes,
      [...hashes].sort((a, b) => a.localeCompare(b))
    );
  });
  t.true(queries.length < 30, `${queries.length} queries for 30 requests`);
  t.is(lookup.stats.hashes, 30);
  t.deepEqual(await lookup.lookup([]), new Map());
});

e2e(
  '[e2e] tx-hash lookup: an initial sync of a chain from an empty store looks up only coinbases without a height; a restart re-saving a stored block finds its transactions',
  async (t) => {
    t.timeout(120_000);
    const { openStore } = await scratch(t, 'txlookup', { utxo: 'off' });
    const store = await openStore();
    // one node: a second node ends the proof
    const { internalId: node1 } = await store.registerNode({
      latestConnectionBeganAt: new Date('2026-10-09T00:00:00Z'),
      nodeName: 'node-one',
      protocolVersion: 70016,
      userAgent: '/BCHN:28.0.0/',
    });
    t.true(store.newTransactions?.enabled ?? false);
    const blocks: ChaingraphBlock[] = [];
    let previous = zeroHash;
    for (let height = 0; height < 40; height += 1) {
      // every other coinbase commits to its height (BIP34)
      const next = block(
        height,
        previous,
        `txl-${height}`,
        height % 2 === 0
          ? `01${height.toString(16).padStart(2, '0')}`
          : undefined
      );
      blocks.push(next);
      previous = next.hash;
    }
    await Promise.all(
      blocks.map(async (next) =>
        store.saveBlock({
          block: next,
          isSavedTransaction: notSaved,
          nodeAcceptances: [acceptance(node1)],
        })
      )
    );
    await store.operations.drain();
    const lookupStats = { ...store.transactionLookup!.stats };
    t.log(
      `lookups: ${JSON.stringify(lookupStats)}, proof: ${JSON.stringify(
        store.newTransactions!.stats
      )}`
    );
    t.is(store.newTransactions!.stats.blocks, 40);
    // 40 non-coinbase + 20 height-committing coinbases skipped
    t.is(store.newTransactions!.stats.skipped, 60);
    t.is(lookupStats.hashes, 20);
    // a restart: the stored chain is the tip now; re-saving a stored block looks its transactions up
    await store.close();
    const again = await openStore();
    const saved = await again.saveBlock({
      block: blocks[5]!,
      isSavedTransaction: notSaved,
      nodeAcceptances: [acceptance(node1)],
    });
    t.is(saved.transactionCacheMisses, 2, 'both found stored, not inserted');
    t.is(again.newTransactions!.stats.skipped, 0);
  }
);
