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

/** A block's fields other than its transactions, from its parsed header. */
export const bitcoreHeaderToChaingraph = (
  header: BitcoreBlock['header'],
  height: number,
  sizeBytes: number
): Omit<ChaingraphBlock, 'transactions'> => {
  const bitcoreBlockHeader = header.toObject();
  return {
    bits: bitcoreBlockHeader.bits,
    hash: bitcoreBlockHeader.hash,
    height,
    merkleRoot: bitcoreBlockHeader.merkleRoot.toString('hex'),
    nonce: bitcoreBlockHeader.nonce,
    previousBlockHash: bitcoreBlockHeader.prevHash.toString('hex'),
    sizeBytes,
    timestamp: bitcoreBlockHeader.time,
    version: bitcoreBlockHeader.version,
  };
};

/** A block's fields other than its transactions (`bitcoreBlockToChaingraphBlock`). */
export const bitcoreBlockHeaderToChaingraph = (
  bitcoreBlock: BitcoreBlock,
  height: number
): Omit<ChaingraphBlock, 'transactions'> =>
  bitcoreHeaderToChaingraph(
    bitcoreBlock.header,
    height,
    bitcoreBlock.toBuffer().length
  );

/**
 * The byte offset just after the serialized transaction at `offset` in
 * `bytes` (version, inputs, outputs, locktime; a CashTokens prefix is inside
 * the locking bytecode field). Throws if the transaction runs past the end.
 */
export const transactionEnd = (bytes: Uint8Array, offset: number) => {
  // eslint-disable-next-line functional/no-let
  let position = offset;
  const need = (length: number) => {
    if (position + length > bytes.length) {
      // eslint-disable-next-line functional/no-throw-statement
      throw new RangeError('Transaction runs past the end of the block.');
    }
  };
  const skip = (length: number) => {
    need(length);
    position += length;
  };
  /* eslint-disable @typescript-eslint/no-magic-numbers */
  const varInt = () => {
    need(1);
    const first = bytes[position]!;
    position += 1;
    if (first < 0xfd) return first;
    const length = first === 0xfd ? 2 : first === 0xfe ? 4 : 8;
    need(length);
    // eslint-disable-next-line functional/no-let
    let value = 0;
    // eslint-disable-next-line functional/no-let, functional/no-loop-statement
    for (let index = length - 1; index >= 0; index -= 1) {
      value = value * 256 + bytes[position + index]!;
    }
    position += length;
    return value;
  };
  skip(4);
  const inputs = varInt();
  // eslint-disable-next-line functional/no-let, functional/no-loop-statement
  for (let index = 0; index < inputs; index += 1) {
    skip(36);
    skip(varInt());
    skip(4);
  }
  const outputs = varInt();
  // eslint-disable-next-line functional/no-let, functional/no-loop-statement
  for (let index = 0; index < outputs; index += 1) {
    skip(8);
    skip(varInt());
  }
  skip(4);
  /* eslint-enable @typescript-eslint/no-magic-numbers */
  return position;
};

export const bitcoreBlockToChaingraphBlock = (
  bitcoreBlock: BitcoreBlock,
  height: number
): ChaingraphBlock => ({
  ...bitcoreBlockHeaderToChaingraph(bitcoreBlock, height),
  transactions: bitcoreBlock.transactions.map(
    bitcoreTransactionToChaingraphTransaction
  ),
});

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
