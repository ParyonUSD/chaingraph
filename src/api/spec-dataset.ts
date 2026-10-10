/* eslint-disable @typescript-eslint/no-magic-numbers, camelcase, @typescript-eslint/naming-convention, class-methods-use-this, complexity, max-params */
// cspell:ignore clickhouse paryon
/**
 * A small two-node, Paryon-shaped dataset for the API spike specs, written
 * through the ClickHouse store, plus an in-memory model of it (the
 * independent computation the API results are compared with).
 *
 * node-one accepts blocks 0-2 (and later 3+) and has one mempool tx;
 * node-two accepts blocks 0-1 only and has an empty mempool.
 */
import {
  makeBlock,
  makeTx,
  p2pkh,
  sha,
  zeroHash,
} from '../store/clickhouse/spec-fixtures.js';
import type {
  ChaingraphBlock,
  ChaingraphOutput,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

export const paryon = sha('paryon');
export const redeemer = sha('redeemer');
export const p2sh = (label: string) => `a914${sha(label).slice(0, 40)}87`;
export const loanScript = p2sh('loan');
export const priceScript = p2sh('price');
export const user1 = p2pkh('user1');
export const user2 = p2pkh('user2');
export const holder1 = p2pkh('holder1');
export const holder2 = p2pkh('holder2');
export const holder3 = p2pkh('holder3');
export const opReturn = '6a';

const coins = (script: string, count: number): ChaingraphOutput[] =>
  Array.from({ length: count }, () => ({
    lockingBytecode: script,
    valueSatoshis: 1_000n,
  }));

const nft = (
  script: string,
  category: string,
  capability: 'minting' | 'mutable' | 'none',
  commitment: string,
  amount?: bigint
): ChaingraphOutput => ({
  ...(amount === undefined ? {} : { fungibleTokenAmount: amount }),
  lockingBytecode: script,
  nonfungibleTokenCapability: capability,
  nonfungibleTokenCommitment: commitment,
  tokenCategory: category,
  valueSatoshis: 1_000n,
});

const ft = (script: string, amount: bigint): ChaingraphOutput => ({
  fungibleTokenAmount: amount,
  lockingBytecode: script,
  tokenCategory: paryon,
  valueSatoshis: 800n,
});

/** Output indexes of the genesis coinbase. */
export const genesis = {
  finalize: [18, 19, 20, 21, 22, 23, 24, 25, 26, 27],
  ft: [13, 14, 15],
  loan1: 1,
  loan2: 2,
  opReturnFt: 16,
  price: 3,
  sidecar: 17,
  user1Coins: [4, 5, 6, 7, 8],
  user2Coins: [9, 10, 11, 12],
};

export const buildDataset = () => {
  const c0 = makeTx({
    coinbase: true,
    label: 'api-c0',
    outputs: [
      { lockingBytecode: p2pkh('miner'), valueSatoshis: 5_000_000_000n },
      nft(loanScript, paryon, 'mutable', '01aa'),
      nft(loanScript, paryon, 'mutable', '01bb'),
      nft(priceScript, paryon, 'mutable', '00'),
      ...coins(user1, 5),
      ...coins(user2, 4),
      ft(holder1, 100n),
      ft(holder2, 200n),
      ft(holder1, 50n),
      ft(opReturn, 7n),
      nft(user1, redeemer, 'none', 'c1'),
      ...Array.from({ length: 10 }, () =>
        nft(loanScript, paryon, 'none', '03')
      ),
    ],
  });
  const at = (index: number): [string, number] => [c0.hash, index];
  const block0 = makeBlock(0, zeroHash, [c0], 'api-block-0');

  const c1 = makeTx({
    coinbase: true,
    label: 'api-c1',
    outputs: coins(p2pkh('miner'), 1),
  });
  // L24 match: spends a loan, input 4 is user1's, no loan output
  const closeA = makeTx({
    label: 'api-closeA',
    outputs: coins(user1, 1),
    spends: [at(genesis.loan1), at(4), at(5), at(6), at(7)],
  });
  // spends a loan but re-creates it (interest payment): NOT EXISTS excludes it
  const interest = makeTx({
    label: 'api-interest',
    outputs: [nft(loanScript, paryon, 'mutable', '01bc'), ...coins(user2, 1)],
    spends: [at(genesis.loan2), at(9), at(10), at(11), at(8)],
  });
  const fin1 = makeTx({
    label: 'api-fin1',
    outputs: [ft(holder3, 30n)],
    spends: [at(18), at(19)],
  });
  const redeemA = makeTx({
    label: 'api-redeemA',
    outputs: [nft(user2, redeemer, 'none', 'c1')],
    spends: [at(genesis.sidecar)],
  });
  const block1 = makeBlock(
    1,
    block0.hash,
    [c1, closeA, interest, fin1, redeemA],
    'api-block-1'
  );

  const c2 = makeTx({
    coinbase: true,
    label: 'api-c2',
    outputs: coins(p2pkh('miner'), 1),
  });
  const fin2 = makeTx({
    label: 'api-fin2',
    outputs: [ft(holder2, 5n)],
    spends: [at(20), at(21)],
  });
  const priceUpdate = makeTx({
    label: 'api-price-1',
    outputs: [nft(priceScript, paryon, 'mutable', '01')],
    spends: [at(genesis.price), at(12)],
  });
  const transferFt = makeTx({
    label: 'api-transfer',
    outputs: [ft(holder3, 60n), ft(holder1, 40n)],
    spends: [at(13)],
  });
  const block2 = makeBlock(
    2,
    block1.hash,
    [c2, fin2, priceUpdate, transferFt],
    'api-block-2'
  );

  // node-one's mempool: a new redeemer sidecar (commitment c2)
  const redeemB = makeTx({
    label: 'api-redeemB',
    outputs: [nft(user2, redeemer, 'none', 'c2')],
    spends: [[redeemA.hash, 0]],
  });

  /** Later blocks for node-one (pagination walk, live queries). */
  const laterBlock = (
    height: number,
    previous: ChaingraphBlock,
    transactions: ChaingraphTransaction[]
  ) =>
    makeBlock(
      height,
      previous.hash,
      [
        makeTx({
          coinbase: true,
          label: `api-c${height}`,
          outputs: coins(p2pkh('miner'), 1),
        }),
        ...transactions,
      ],
      `api-block-${height}`
    );

  return {
    block0,
    block1,
    block2,
    c0,
    closeA,
    fin1,
    fin2,
    interest,
    laterBlock,
    priceUpdate,
    redeemA,
    redeemB,
    transferFt,
  };
};

export type Dataset = ReturnType<typeof buildDataset>;

/* ------------------------------------------------------------------ */
/* in-memory model                                                      */
/* ------------------------------------------------------------------ */

export interface ModelOutput {
  transaction_hash: string;
  output_index: number;
  locking_bytecode: string;
  value_satoshis: bigint;
  token_category: string | null;
  fungible_token_amount: bigint | null;
  nonfungible_token_capability: string | null;
  nonfungible_token_commitment: string | null;
}

export interface ModelInput {
  transaction_hash: string;
  input_index: number;
  outpoint_transaction_hash: string;
  outpoint_index: number;
  outpoint: ModelOutput | undefined;
}

export interface ModelTx {
  tx: ChaingraphTransaction;
  /** Height per accepting node; null = in that node's mempool. */
  acceptedBy: Map<string, number | null>;
}

const modelOutput = (
  tx: ChaingraphTransaction,
  output: ChaingraphOutput,
  index: number
): ModelOutput => ({
  fungible_token_amount: output.fungibleTokenAmount ?? null,
  locking_bytecode: output.lockingBytecode,
  nonfungible_token_capability: output.nonfungibleTokenCapability ?? null,
  nonfungible_token_commitment:
    output.nonfungibleTokenCapability === undefined
      ? null
      : output.nonfungibleTokenCommitment ?? '',
  output_index: index,
  token_category: output.tokenCategory ?? null,
  transaction_hash: tx.hash,
  value_satoshis: output.valueSatoshis,
});

export class Model {
  readonly txs = new Map<string, ModelTx>();

  addBlock(block: ChaingraphBlock, nodes: readonly string[]) {
    block.transactions.forEach((tx) => {
      const entry = this.txs.get(tx.hash) ?? { acceptedBy: new Map(), tx };
      nodes.forEach((node) => entry.acceptedBy.set(node, block.height));
      this.txs.set(tx.hash, entry);
    });
  }

  addMempool(tx: ChaingraphTransaction, nodes: readonly string[]) {
    const entry = this.txs.get(tx.hash) ?? { acceptedBy: new Map(), tx };
    nodes.forEach((node) => entry.acceptedBy.set(node, null));
    this.txs.set(tx.hash, entry);
  }

  accepted(node: string) {
    return [...this.txs.values()].filter((entry) => entry.acceptedBy.has(node));
  }

  outputs(tx: ChaingraphTransaction) {
    return tx.outputs.map((output, index) => modelOutput(tx, output, index));
  }

  output(hash: string, index: number): ModelOutput | undefined {
    const tx = this.txs.get(hash)?.tx;
    const output = tx?.outputs[index];
    return tx === undefined || output === undefined
      ? undefined
      : modelOutput(tx, output, index);
  }

  inputs(tx: ChaingraphTransaction): ModelInput[] {
    if (tx.isCoinbase) return [];
    return tx.inputs.map((input, index) => ({
      input_index: index,
      outpoint: this.output(input.outpointTransactionHash, input.outpointIndex),
      outpoint_index: input.outpointIndex,
      outpoint_transaction_hash: input.outpointTransactionHash,
      transaction_hash: tx.hash,
    }));
  }

  /** unspent(n, o): Chaingraph v1's definition. */
  unspent(node: string) {
    const accepted = this.accepted(node);
    const spent = new Set(
      accepted.flatMap((entry) =>
        this.inputs(entry.tx).map(
          (input) =>
            `${input.outpoint_transaction_hash}:${input.outpoint_index}`
        )
      )
    );
    return accepted
      .flatMap((entry) => this.outputs(entry.tx))
      .filter(
        (output) =>
          !spent.has(`${output.transaction_hash}:${output.output_index}`)
      );
  }
}
