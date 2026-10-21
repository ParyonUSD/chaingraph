/* eslint-disable @typescript-eslint/no-magic-numbers */
import { hexToBin, range } from '@bitauth/libauth';
import type { BitcoreBlockHeader } from '@chaingraph/bitcore-p2p-cash';
import test from 'ava';
import { pino } from 'pino';

import { BlockTree, selectHeaderLocatorHashes } from './block-tree.js';

test('selectHeaderLocatorHashes', async (t) => {
  const testLength = 654247;
  const headerChain = range(testLength);
  /**
   * Expected heights produced by the Satoshi implementation at height 654247.
   */
  // prettier-ignore
  const expected = [654246, 654245, 654244, 654243, 654242, 654241, 654240, 654239, 654238, 654237, 654236, 654235, 654233, 654229, 654221, 654205, 654173, 654109, 653981, 653725, 653213, 652189, 650141, 646045, 637853, 621469, 588701, 523165, 392093, 129949, 0];
  t.deepEqual(selectHeaderLocatorHashes(headerChain), expected);
});

test('selectHeaderLocatorHashes: single', async (t) => {
  const headerChain = range(1);
  const expected = [0];
  t.deepEqual(selectHeaderLocatorHashes(headerChain), expected);
});

test('selectHeaderLocatorHashes: `10`', async (t) => {
  const headerChain = range(10);
  const expected = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0];
  t.deepEqual(selectHeaderLocatorHashes(headerChain), expected);
});

test('selectHeaderLocatorHashes: `100`', async (t) => {
  const headerChain = range(100);
  // prettier-ignore
  const expected = [99, 98, 97, 96, 95, 94, 93, 92, 91, 90, 89, 88, 86, 82, 74, 58, 26, 0];
  t.deepEqual(selectHeaderLocatorHashes(headerChain), expected);
});

/**
 * Build a fake header-hash chain: the hash of each block encodes its branch
 * and height, e.g. `blockHash('a', 3)`.
 */
const blockHash = (branch: string, height: number) =>
  `${branch}${height.toString(16).padStart(63, '0')}`;

const createHeaders = (hashes: string[], previousHash: string) =>
  hashes.map((hash, index) => ({
    hash,
    prevHash: Buffer.from(
      hexToBin(index === 0 ? previousHash : hashes[index - 1]!).reverse()
    ),
  })) as unknown as BitcoreBlockHeader[];

/**
 * Create a `BlockTree` for node `n` restored to the provided chain, recording
 * each `onStaleBlocks` call.
 */
const createBlockTree = (chain: string[]) => {
  const staleCalls: { staleChain: string[]; firstHeight: number }[] = [];
  const blockTree = new BlockTree({
    genesisBlockByNode: { n: [chain[0]!] },
    logger: pino({ level: 'silent' }),
    onStaleBlocks: (staleChain, firstHeight) => {
      staleCalls.push({ firstHeight, staleChain });
    },
  });
  blockTree.restoreChainForNode('n', chain);
  return { blockTree, staleCalls };
};

const readChain = (blockTree: BlockTree) =>
  range(blockTree.getBestHeights().n! + 1).map((height) =>
    blockTree.getBlockHeaderHash('n', height)
  );

/**
 * Genesis (height 0) followed by blocks `1` through `tip` of `branch`, with
 * heights up to `forkHeight` shared with branch `a`.
 */
const branchChain = (branch: string, forkHeight: number, tip: number) =>
  range(tip + 1).map((height) =>
    blockHash(height <= forkHeight ? 'a' : branch, height)
  );

test('BlockTree.updateHeaders: extends the chain without stale blocks', (t) => {
  const { blockTree, staleCalls } = createBlockTree(branchChain('a', 10, 10));
  const newHashes = branchChain('a', 13, 13).slice(11);
  blockTree.updateHeaders('n', createHeaders(newHashes, blockHash('a', 10)));
  t.deepEqual(staleCalls, []);
  t.deepEqual(readChain(blockTree), branchChain('a', 13, 13));
});

test('BlockTree.updateHeaders: shallow re-organization starting after the fork point', (t) => {
  const { blockTree, staleCalls } = createBlockTree(branchChain('a', 10, 10));
  const newHashes = branchChain('b', 8, 11).slice(9);
  blockTree.updateHeaders('n', createHeaders(newHashes, blockHash('a', 8)));
  t.deepEqual(staleCalls, [
    { firstHeight: 9, staleChain: [blockHash('a', 9), blockHash('a', 10)] },
  ]);
  t.deepEqual(readChain(blockTree), branchChain('b', 8, 11));
});

test('BlockTree.updateHeaders: deep re-organization with headers starting below the fork point only marks diverging blocks as stale', (t) => {
  const { blockTree, staleCalls } = createBlockTree(branchChain('a', 105, 105));
  /**
   * As when a node answers `getheaders` from the latest locator hash in its
   * active chain (here genesis): heights 1-5 are shared with the known chain.
   */
  const newHashes = branchChain('b', 5, 106).slice(1);
  blockTree.updateHeaders('n', createHeaders(newHashes, blockHash('a', 0)));
  t.deepEqual(staleCalls, [
    {
      firstHeight: 6,
      staleChain: range(100, 6).map((height) => blockHash('a', height)),
    },
  ]);
  t.deepEqual(readChain(blockTree), branchChain('b', 5, 106));
});
