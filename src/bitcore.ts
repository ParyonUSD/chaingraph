import type {
  BitcoreBlock,
  BitcoreTransaction,
} from '@chaingraph/bitcore-p2p-cash';
import bitcoreP2pCash from '@chaingraph/bitcore-p2p-cash';

import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from './types/chaingraph.js';

export const bitcoreTransactionToChaingraphTransaction = (
  bitcoreTransaction: BitcoreTransaction
): ChaingraphTransaction => {
  const txObject = bitcoreTransaction.toObject();
  const isCoinbase = bitcoreTransaction.isCoinbase();
  return {
    hash: txObject.hash,
    inputs: txObject.inputs.map((input) => ({
      outpointIndex: input.outputIndex,
      outpointTransactionHash: input.prevTxId,
      sequenceNumber: input.sequenceNumber,
      unlockingBytecode: input.script,
    })),
    isCoinbase,
    locktime: txObject.nLockTime,
    outputs: txObject.outputs.map((output) => ({
      fungibleTokenAmount:
        output.tokenData === undefined
          ? undefined
          : BigInt(output.tokenData.amount),
      lockingBytecode: output.script,
      nonfungibleTokenCapability: output.tokenData?.nft?.capability,
      nonfungibleTokenCommitment: output.tokenData?.nft?.commitment,
      tokenCategory: output.tokenData?.category,
      valueSatoshis: BigInt(output.satoshis),
    })),
    sizeBytes: bitcoreTransaction.toBuffer().length,
    version: txObject.version,
  };
};

export const bitcoreBlockToChaingraphBlock = (
  bitcoreBlock: BitcoreBlock,
  height: number
): ChaingraphBlock => {
  const bitcoreBlockHeader = bitcoreBlock.header.toObject();
  return {
    bits: bitcoreBlockHeader.bits,
    hash: bitcoreBlockHeader.hash,
    height,
    merkleRoot: bitcoreBlockHeader.merkleRoot.toString('hex'),
    nonce: bitcoreBlockHeader.nonce,
    previousBlockHash: bitcoreBlockHeader.prevHash.toString('hex'),
    sizeBytes: bitcoreBlock.toBuffer().length,
    timestamp: bitcoreBlockHeader.time,
    transactions: bitcoreBlock.transactions.map(
      bitcoreTransactionToChaingraphTransaction
    ),
    version: bitcoreBlockHeader.version,
  };
};

/** Transactions converted per turn of the event loop (`bitcoreBlockToChaingraphBlockInSlices`). */
export const blockParseSliceTransactions = 5_000;

/**
 * As `bitcoreBlockToChaingraphBlock`, yielding to the event loop (setImmediate)
 * every `sliceTransactions` transactions: a 32 MB block (~100k transactions)
 * is ~0.8 s of conversion, which in one piece delays every timer, socket and
 * commit of the agent (g1-fix-pass-2.md §1).
 */
export const bitcoreBlockToChaingraphBlockInSlices = async (
  bitcoreBlock: BitcoreBlock,
  height: number,
  sliceTransactions = blockParseSliceTransactions
): Promise<ChaingraphBlock> => {
  const { transactions } = bitcoreBlock;
  const converted: ChaingraphBlock['transactions'] = [];
  const nextTurn = async () =>
    new Promise<void>((resolve) => {
      setImmediate(resolve);
    });
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let start = 0; start < transactions.length; start += sliceTransactions) {
    if (start > 0) {
      // eslint-disable-next-line no-await-in-loop
      await nextTurn();
    }
    transactions
      .slice(start, start + sliceTransactions)
      .forEach((transaction) => {
        converted.push(bitcoreTransactionToChaingraphTransaction(transaction));
      });
  }
  const header = bitcoreBlock.header.toObject();
  return {
    bits: header.bits,
    hash: header.hash,
    height,
    merkleRoot: header.merkleRoot.toString('hex'),
    nonce: header.nonce,
    previousBlockHash: header.prevHash.toString('hex'),
    sizeBytes: bitcoreBlock.toBuffer().length,
    timestamp: header.time,
    transactions: converted,
    version: header.version,
  };
};

export const messages = new bitcoreP2pCash.Messages();
