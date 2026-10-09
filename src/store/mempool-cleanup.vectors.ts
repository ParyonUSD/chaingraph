/* eslint-disable @typescript-eslint/no-magic-numbers */
/**
 * Shared vectors for mempool cleanup, run through both the pure planner
 * (`mempool-graph.spec.ts`) and the Postgres SQL
 * (`e2e/mempool-cleanup.spec.ts`), which makes the pair a differential test.
 *
 * The data mirrors the cut-down Postgres temp tables: nodes are 1 (`alpha`)
 * and 2 (`beta`), transactions are small integers whose hash is that byte
 * repeated 32 times, and every transaction has output 0. Timestamps use the
 * Postgres `timestamp::text` format (`YYYY-MM-DD HH:MM:SS`, UTC).
 */
import type {
  AcceptedInclusion,
  NodeMempool,
  Outpoint,
  PlannedArchive,
  TxKey,
} from './mempool-graph.js';
import {
  applyArchives,
  coinbaseOutpointHash,
  outpoint,
  planAcceptedBlockCleanup,
} from './mempool-graph.js';

export type VectorTimestamp = string;

export interface VectorInput {
  tx: number;
  /**
   * Defaults to the input's position among `tx`'s inputs in the vector.
   */
  inputIndex?: number;
  /**
   * The spent transaction, or `coinbase` for the null outpoint hash.
   */
  outpointTx: number | 'coinbase';
  outpointIndex: number;
}

export interface VectorNodeTransaction {
  node: number;
  tx: number;
  /**
   * Defaults to `defaultValidatedAt`.
   */
  validatedAt?: VectorTimestamp;
}

export interface VectorArchive {
  node: number;
  tx: number;
  replacedAt: VectorTimestamp | null;
}

export interface VectorMembership {
  node: number;
  tx: number;
}

export interface MempoolCleanupVector {
  name: string;
  description: string;
  nodes: readonly { internalId: number; name: string }[];
  inputs: readonly VectorInput[];
  nodeTransactions: readonly VectorNodeTransaction[];
  blockTransactions: readonly { block: number; tx: number }[];
  nodeBlocks: readonly {
    node: number;
    block: number;
    acceptedAt: VectorTimestamp | null;
  }[];
  expected: {
    /**
     * The rows the sweep archives directly (what
     * `archiveMempoolTransactionsAcceptedByBlocks` returns), ordered by node
     * then tx. Cascaded descendants are only in `history`.
     */
    archived: readonly VectorArchive[];
    /**
     * node_transaction after the sweep, ordered by node then tx.
     */
    remaining: readonly VectorMembership[];
    /**
     * node_transaction_history after the sweep (direct and cascaded rows),
     * ordered by node then tx.
     */
    history: readonly VectorArchive[];
  };
}

export const defaultValidatedAt: VectorTimestamp = '2026-01-01 00:00:00';

export const vectorNodes = [
  { internalId: 1, name: 'alpha' },
  { internalId: 2, name: 'beta' },
] as const;

const repeatedHashBytes = 32;

/**
 * The hex hash of vector transaction `id` (the byte repeated 32 times).
 */
export const vectorTxHash = (id: number | 'coinbase'): TxKey =>
  id === 'coinbase'
    ? coinbaseOutpointHash
    : id.toString(16).padStart(2, '0').repeat(repeatedHashBytes);

export const vectorTxId = (hash: TxKey) => parseInt(hash.slice(0, 2), 16);

export const timestampToDate = (timestamp: VectorTimestamp) =>
  new Date(`${timestamp.replace(' ', 'T')}Z`);

export const dateToTimestamp = (date: Date): VectorTimestamp =>
  date.toISOString().replace('T', ' ').slice(0, '2026-01-01 00:00:00'.length);

/**
 * The inputs of a vector with every `inputIndex` filled in.
 */
export const vectorInputs = (vector: MempoolCleanupVector) => {
  const seen = new Map<number, number>();
  return vector.inputs.map((input) => {
    const position = seen.get(input.tx) ?? 0;
    seen.set(input.tx, position + 1);
    return { ...input, inputIndex: input.inputIndex ?? position };
  });
};

const spendsOf = (vector: MempoolCleanupVector, tx: number): Outpoint[] =>
  vectorInputs(vector)
    .filter((input) => input.tx === tx)
    .sort((a, b) => a.inputIndex - b.inputIndex)
    .map((input) =>
      outpoint(vectorTxHash(input.outpointTx), input.outpointIndex)
    );

/**
 * One node's mempool, built only from that node's node_transaction rows.
 */
export const vectorNodeMempool = (
  vector: MempoolCleanupVector,
  node: number
): NodeMempool => ({
  txs: new Map(
    vector.nodeTransactions
      .filter((row) => row.node === node)
      .map((row) => [
        vectorTxHash(row.tx),
        {
          spends: spendsOf(vector, row.tx),
          validatedAt: timestampToDate(row.validatedAt ?? defaultValidatedAt),
        },
      ])
  ),
});

/**
 * One node's accepted inclusions: block_transaction rows joined to that
 * node's node_block rows.
 */
export const vectorNodeInclusions = (
  vector: MempoolCleanupVector,
  node: number
): AcceptedInclusion[] =>
  vector.nodeBlocks
    .filter((nodeBlock) => nodeBlock.node === node)
    .flatMap((nodeBlock) =>
      vector.blockTransactions
        .filter((blockTx) => blockTx.block === nodeBlock.block)
        .map((blockTx) => ({
          acceptedAt:
            nodeBlock.acceptedAt === null
              ? null
              : timestampToDate(nodeBlock.acceptedAt),
          spends: spendsOf(vector, blockTx.tx),
          tx: vectorTxHash(blockTx.tx),
        }))
    );

const toVectorArchive = (
  node: number,
  archive: PlannedArchive
): VectorArchive => ({
  node,
  replacedAt:
    archive.replacedAt === null ? null : dateToTimestamp(archive.replacedAt),
  tx: vectorTxId(archive.tx),
});

const byNodeThenTx = (a: VectorMembership, b: VectorMembership) =>
  a.node - b.node || a.tx - b.tx;

/**
 * Run the planner over every node of a vector, one node at a time, and
 * return the outcome in the vector's `expected` shape.
 */
export const planVector = (
  vector: MempoolCleanupVector
): MempoolCleanupVector['expected'] => {
  const perNode = vector.nodes.map(({ internalId: node }) => {
    const mempool = vectorNodeMempool(vector, node);
    const plan = planAcceptedBlockCleanup(
      mempool,
      vectorNodeInclusions(vector, node)
    );
    return { mempool: applyArchives(mempool, plan), node, plan };
  });
  return {
    archived: perNode
      .flatMap(({ node, plan }) =>
        plan
          .filter((archive) => archive.cause !== 'descendant')
          .map((archive) => toVectorArchive(node, archive))
      )
      .sort(byNodeThenTx),
    history: perNode
      .flatMap(({ node, plan }) =>
        plan.map((archive) => toVectorArchive(node, archive))
      )
      .sort(byNodeThenTx),
    remaining: perNode
      .flatMap(({ mempool, node }) =>
        [...mempool.txs.keys()].map((tx) => ({ node, tx: vectorTxId(tx) }))
      )
      .sort(byNodeThenTx),
  };
};

/**
 * A copy of `vector` with the node_transaction rows already archived by its
 * expected outcome removed: running the sweep again must archive nothing.
 */
export const afterExpectedSweep = (
  vector: MempoolCleanupVector
): MempoolCleanupVector => ({
  ...vector,
  nodeTransactions: vector.nodeTransactions.filter((row) =>
    vector.expected.remaining.some(
      (kept) => kept.node === row.node && kept.tx === row.tx
    )
  ),
});

const bothNodesTxs1To3 = [1, 2].flatMap((node) =>
  [1, 2, 3].map((tx) => ({ node, tx }))
);

export const mempoolCleanupVectors: readonly MempoolCleanupVector[] = [
  {
    blockTransactions: [
      // Duplicate accepted inclusions must still produce only one history row.
      { block: 1, tx: 1 },
      { block: 2, tx: 1 },
    ],
    description:
      'cleanup confirms a parent without invalidating descendants or another node',
    expected: {
      archived: [{ node: 1, replacedAt: null, tx: 1 }],
      history: [{ node: 1, replacedAt: null, tx: 1 }],
      remaining: [
        { node: 1, tx: 2 },
        { node: 1, tx: 3 },
        { node: 2, tx: 1 },
        { node: 2, tx: 2 },
        { node: 2, tx: 3 },
      ],
    },
    inputs: [
      { outpointIndex: 0, outpointTx: 1, tx: 2 },
      { outpointIndex: 0, outpointTx: 2, tx: 3 },
    ],
    name: 'confirm-parent',
    nodeBlocks: [
      { acceptedAt: '2026-01-02 00:00:00', block: 1, node: 1 },
      { acceptedAt: '2026-01-03 00:00:00', block: 2, node: 1 },
    ],
    nodeTransactions: bothNodesTxs1To3,
    nodes: vectorNodes,
  },
  {
    blockTransactions: [
      { block: 1, tx: 4 },
      { block: 2, tx: 4 },
    ],
    description:
      'cleanup invalidates conflicts and descendants only for the accepting node',
    expected: {
      archived: [{ node: 1, replacedAt: '2026-01-02 00:00:00', tx: 1 }],
      history: [
        { node: 1, replacedAt: '2026-01-02 00:00:00', tx: 1 },
        { node: 1, replacedAt: '2026-01-02 00:00:00', tx: 2 },
        { node: 1, replacedAt: '2026-01-02 00:00:00', tx: 3 },
      ],
      remaining: [
        { node: 2, tx: 1 },
        { node: 2, tx: 2 },
        { node: 2, tx: 3 },
      ],
    },
    inputs: [
      { outpointIndex: 0, outpointTx: 10, tx: 1 },
      { outpointIndex: 0, outpointTx: 10, tx: 4 },
      { outpointIndex: 0, outpointTx: 1, tx: 2 },
      { outpointIndex: 0, outpointTx: 2, tx: 3 },
    ],
    name: 'conflict-cascade',
    nodeBlocks: [
      // replaced_at is the MIN over every accepting block, not the first.
      { acceptedAt: '2026-01-05 00:00:00', block: 1, node: 1 },
      { acceptedAt: '2026-01-02 00:00:00', block: 2, node: 1 },
    ],
    nodeTransactions: bothNodesTxs1To3,
    nodes: vectorNodes,
  },
  {
    // Deliberately inconsistent historical acceptance tests NULL precedence.
    blockTransactions: [
      { block: 1, tx: 1 },
      { block: 2, tx: 4 },
      { block: 3, tx: 8 },
    ],
    description:
      'cleanup gives confirmation precedence in mixed confirmation and invalidation batches',
    expected: {
      archived: [
        { node: 1, replacedAt: null, tx: 1 },
        { node: 1, replacedAt: '2026-01-04 00:00:00', tx: 5 },
      ],
      history: [
        { node: 1, replacedAt: null, tx: 1 },
        { node: 1, replacedAt: '2026-01-04 00:00:00', tx: 5 },
        { node: 1, replacedAt: '2026-01-04 00:00:00', tx: 6 },
        { node: 1, replacedAt: '2026-01-04 00:00:00', tx: 7 },
      ],
      remaining: [{ node: 1, tx: 2 }],
    },
    inputs: [
      { outpointIndex: 0, outpointTx: 10, tx: 1 },
      { outpointIndex: 0, outpointTx: 10, tx: 4 },
      { outpointIndex: 0, outpointTx: 1, tx: 2 },
      { outpointIndex: 0, outpointTx: 11, tx: 5 },
      { outpointIndex: 0, outpointTx: 11, tx: 8 },
      { outpointIndex: 0, outpointTx: 5, tx: 6 },
      { outpointIndex: 0, outpointTx: 6, tx: 7 },
    ],
    name: 'confirmation-precedence',
    nodeBlocks: [
      { acceptedAt: '2026-01-02 00:00:00', block: 1, node: 1 },
      { acceptedAt: '2026-01-03 00:00:00', block: 2, node: 1 },
      { acceptedAt: '2026-01-04 00:00:00', block: 3, node: 1 },
    ],
    nodeTransactions: [1, 2, 5, 6, 7].map((tx) => ({ node: 1, tx })),
    nodes: vectorNodes,
  },
  {
    blockTransactions: [
      { block: 1, tx: 1 },
      { block: 2, tx: 3 },
      { block: 3, tx: 5 },
      { block: 4, tx: 9 },
    ],
    description:
      'cleanup ignores other-node confirmations, self, coinbase and unconfirmed conflicts',
    expected: {
      archived: [],
      history: [],
      remaining: [1, 2, 4, 6, 8].map((tx) => ({ node: 1, tx })),
    },
    inputs: [
      { outpointIndex: 0, outpointTx: 10, tx: 1 },
      { outpointIndex: 0, outpointTx: 'coinbase', tx: 2 },
      { outpointIndex: 0, outpointTx: 'coinbase', tx: 3 },
      { outpointIndex: 0, outpointTx: 11, tx: 4 },
      { outpointIndex: 0, outpointTx: 11, tx: 5 },
      { outpointIndex: 0, outpointTx: 12, tx: 6 },
      { outpointIndex: 0, outpointTx: 12, tx: 7 },
      { outpointIndex: 1, outpointTx: 13, tx: 8 },
      { outpointIndex: 0, outpointTx: 13, tx: 9 },
    ],
    name: 'ignored',
    nodeBlocks: [
      { acceptedAt: '2026-01-02 00:00:00', block: 1, node: 2 },
      { acceptedAt: '2026-01-02 00:00:00', block: 2, node: 1 },
      { acceptedAt: '2026-01-02 00:00:00', block: 3, node: 2 },
      { acceptedAt: '2026-01-02 00:00:00', block: 4, node: 1 },
    ],
    nodeTransactions: [1, 2, 4, 6, 8].map((tx) => ({ node: 1, tx })),
    nodes: vectorNodes,
  },
  {
    blockTransactions: [],
    description: 'cleanup accepts an empty mempool',
    expected: { archived: [], history: [], remaining: [] },
    inputs: [],
    name: 'empty',
    nodeBlocks: [],
    nodeTransactions: [],
    nodes: vectorNodes,
  },
  {
    /*
     * QUIRK, reproduced deliberately (candidate fix for both backends): a
     * conflict whose only conflicting inclusions are in blocks with NULL
     * accepted_at (not accepted live, or header-accepted) is archived with
     * replaced_at NULL, indistinguishable from a confirmation, and its
     * descendants are not cascaded (tx 2 stays in node 1's mempool although
     * its parent can no longer confirm). One non-null accepted_at among the
     * conflicting inclusions is enough for a normal conflict (tx 3, 4).
     */
    blockTransactions: [
      { block: 1, tx: 5 },
      { block: 1, tx: 7 },
      { block: 2, tx: 6 },
      { block: 3, tx: 6 },
    ],
    description:
      'cleanup archives conflicts from NULL accepted_at blocks as confirmations (known quirk)',
    expected: {
      archived: [
        { node: 1, replacedAt: null, tx: 1 },
        { node: 1, replacedAt: '2026-01-03 00:00:00', tx: 3 },
        { node: 1, replacedAt: null, tx: 7 },
      ],
      history: [
        { node: 1, replacedAt: null, tx: 1 },
        { node: 1, replacedAt: '2026-01-03 00:00:00', tx: 3 },
        { node: 1, replacedAt: '2026-01-03 00:00:00', tx: 4 },
        { node: 1, replacedAt: null, tx: 7 },
      ],
      remaining: [
        { node: 1, tx: 2 },
        { node: 2, tx: 1 },
        { node: 2, tx: 2 },
      ],
    },
    inputs: [
      { outpointIndex: 0, outpointTx: 10, tx: 1 },
      { outpointIndex: 0, outpointTx: 1, tx: 2 },
      { outpointIndex: 0, outpointTx: 11, tx: 3 },
      { outpointIndex: 0, outpointTx: 3, tx: 4 },
      { outpointIndex: 0, outpointTx: 10, tx: 5 },
      { outpointIndex: 0, outpointTx: 11, tx: 6 },
      { outpointIndex: 0, outpointTx: 12, tx: 7 },
    ],
    name: 'null-accepted-at',
    nodeBlocks: [
      { acceptedAt: null, block: 1, node: 1 },
      { acceptedAt: null, block: 2, node: 1 },
      { acceptedAt: '2026-01-03 00:00:00', block: 3, node: 1 },
    ],
    nodeTransactions: [
      ...[1, 2, 3, 4, 7].map((tx) => ({ node: 1, tx })),
      { node: 2, tx: 1 },
      { node: 2, tx: 2 },
    ],
    nodes: vectorNodes,
  },
  {
    /*
     * A directly archived conflict keeps its own replaced_at even when it is
     * also a descendant of an earlier conflict: in Postgres the direct rows
     * are deleted before the cascade trigger walks node_transaction, so the
     * walk never passes through them. Tx 2 (child of tx 1, replaced at 01-02)
     * keeps its own 01-05, and tx 3 (child of tx 2 only) inherits 01-05, not
     * 01-02. Tx 4 is reachable from both tx 1 and tx 2 and takes the MIN.
     */
    blockTransactions: [
      { block: 1, tx: 5 },
      { block: 2, tx: 6 },
    ],
    description:
      'cleanup keeps a direct conflict replaced_at over an earlier ancestor conflict',
    expected: {
      archived: [
        { node: 1, replacedAt: '2026-01-02 00:00:00', tx: 1 },
        { node: 1, replacedAt: '2026-01-05 00:00:00', tx: 2 },
      ],
      history: [
        { node: 1, replacedAt: '2026-01-02 00:00:00', tx: 1 },
        { node: 1, replacedAt: '2026-01-05 00:00:00', tx: 2 },
        { node: 1, replacedAt: '2026-01-05 00:00:00', tx: 3 },
        { node: 1, replacedAt: '2026-01-02 00:00:00', tx: 4 },
      ],
      remaining: [],
    },
    inputs: [
      { outpointIndex: 0, outpointTx: 10, tx: 1 },
      { outpointIndex: 0, outpointTx: 1, tx: 2 },
      { outpointIndex: 0, outpointTx: 11, tx: 2 },
      { outpointIndex: 0, outpointTx: 2, tx: 3 },
      { outpointIndex: 1, outpointTx: 1, tx: 4 },
      { outpointIndex: 1, outpointTx: 2, tx: 4 },
      { outpointIndex: 0, outpointTx: 10, tx: 5 },
      { outpointIndex: 0, outpointTx: 11, tx: 6 },
    ],
    name: 'conflict-inside-cascade',
    nodeBlocks: [
      { acceptedAt: '2026-01-02 00:00:00', block: 1, node: 1 },
      { acceptedAt: '2026-01-05 00:00:00', block: 2, node: 1 },
    ],
    nodeTransactions: [1, 2, 3, 4].map((tx) => ({ node: 1, tx })),
    nodes: vectorNodes,
  },
];

/* eslint-disable no-bitwise, operator-assignment */
/**
 * A small seeded PRNG (mulberry32) for reproducible random vectors.
 */
export const seededRandom = (seed: number) => {
  // eslint-disable-next-line functional/no-let
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    // eslint-disable-next-line functional/no-let
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
};
/* eslint-enable no-bitwise, operator-assignment */

const randomMempoolTxCount = 16;
const randomExternalTxs = [17, 18] as const;
const randomBlockCount = 3;
const randomAcceptedAt: readonly (VectorTimestamp | null)[] = [
  '2026-01-02 00:00:00',
  '2026-01-03 00:00:00',
  '2026-01-04 00:00:00',
  null,
];

/**
 * A random two-node vector over a small outpoint space (so conflicts, chains
 * and multi-parent descendants are common). Its `expected` outcome comes from
 * the planner itself, so it is only meaningful when compared against an
 * independent implementation (the SQL), or for planner invariants.
 */
export const randomVector = (seed: number): MempoolCleanupVector => {
  const random = seededRandom(seed);
  const pick = <T>(items: readonly T[]) =>
    items[Math.floor(random() * items.length)]!;
  const txIds = Array.from({ length: randomMempoolTxCount }, (_, i) => i + 1);
  const inputs = txIds.flatMap((tx) => {
    const inputCount = random() < 0.3 ? 2 : 1;
    const candidates = Array.from({ length: inputCount }, () => {
      const roll = random();
      const outpointTx: number | 'coinbase' =
        roll < 0.05
          ? 'coinbase'
          : roll < 0.55 && tx > 1
          ? pick(txIds.slice(0, tx - 1))
          : pick(randomExternalTxs);
      return { outpointIndex: random() < 0.7 ? 0 : 1, outpointTx, tx };
    });
    return candidates.filter(
      (candidate, index) =>
        candidates.findIndex(
          (other) =>
            other.outpointTx === candidate.outpointTx &&
            other.outpointIndex === candidate.outpointIndex
        ) === index
    );
  });
  const nodeTransactions = vectorNodes.flatMap(({ internalId: node }) =>
    txIds.filter(() => random() < 0.6).map((tx) => ({ node, tx }))
  );
  const blocks = Array.from({ length: randomBlockCount }, (_, i) => i + 1);
  const blockTransactions = blocks.flatMap((block) =>
    txIds.filter(() => random() < 0.12).map((tx) => ({ block, tx }))
  );
  const nodeBlocks = vectorNodes.flatMap(({ internalId: node }) =>
    blocks
      .filter(() => random() < 0.6)
      .map((block) => ({ acceptedAt: pick(randomAcceptedAt), block, node }))
  );
  const vector: MempoolCleanupVector = {
    blockTransactions,
    description: `random vector seed ${seed}`,
    expected: { archived: [], history: [], remaining: [] },
    inputs,
    name: `random-${seed}`,
    nodeBlocks,
    nodeTransactions,
    nodes: vectorNodes,
  };
  return { ...vector, expected: planVector(vector) };
};
