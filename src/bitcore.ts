import crypto, { createHash } from 'crypto';

import type {
  BitcoreBlock,
  BitcoreTransaction,
} from '@chaingraph/bitcore-p2p-cash';
import bitcoreP2pCash from '@chaingraph/bitcore-p2p-cash';

import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from './types/chaingraph.js';

const { internalBitcore } = bitcoreP2pCash;

/**
 * The serialized block as received from the P2P network, with the boundaries
 * of each transaction within it. Used to take block/transaction sizes and
 * transaction hashes directly from the wire bytes, rather than re-serializing
 * each transaction (and the full block) with bitcore.
 */
interface BlockWireBytes {
  /**
   * The serialized block.
   */
  encoded: Buffer;
  /**
   * The offset of each transaction within `encoded`, followed by the offset
   * at which the final transaction ends (`transactions.length + 1` items).
   */
  transactionBoundaries: number[];
}

const blockWireBytes = new WeakMap<BitcoreBlock, BlockWireBytes>();

/**
 * Decode a serialized block, retaining the serialized bytes and the boundaries
 * of each transaction for use by {@link bitcoreBlockToChaingraphBlock}. The
 * returned block is identical to the result of bitcore's `Block.fromBuffer`.
 */
export const decodeBlockRetainingWireBytes = (encoded: Buffer) => {
  /**
   * Bitcore decodes from a copy: its token prefix decoding reverses each token
   * category in place within the buffer being read, which would otherwise
   * corrupt the retained wire bytes.
   */
  const reader = new internalBitcore.encoding.BufferReader(
    Buffer.from(encoded)
  );
  const header = internalBitcore.BlockHeader.fromBufferReader(reader);
  const transactionCount = reader.readVarintNum();
  const transactions: BitcoreTransaction[] = [];
  const transactionBoundaries: number[] = [];
  // eslint-disable-next-line functional/no-loop-statement, functional/no-let
  for (let index = 0; index < transactionCount; index += 1) {
    transactionBoundaries.push(reader.pos);
    transactions.push(
      new internalBitcore.Transaction().fromBufferReader(reader)
    );
  }
  transactionBoundaries.push(reader.pos);
  const block = new internalBitcore.Block({ header, transactions });
  blockWireBytes.set(block, { encoded, transactionBoundaries });
  return block;
};

/**
 * A bitcore `Block` constructor which decodes blocks using
 * {@link decodeBlockRetainingWireBytes}. Provided to bitcore-p2p-cash's
 * `Messages` so blocks received from peers retain their wire bytes.
 */
export class BlockRetainingWireBytes extends internalBitcore.Block {
  static override fromBuffer = decodeBlockRetainingWireBytes;
}

/**
 * Create the bitcore-p2p-cash `Messages` used to communicate with a peer on
 * the provided network. Received blocks retain their wire bytes.
 */
export const createPeerMessages = (networkName: string) =>
  new bitcoreP2pCash.Messages({
    // eslint-disable-next-line @typescript-eslint/naming-convention
    Block: BlockRetainingWireBytes,
    network: internalBitcore.Networks.get(networkName),
  });

/**
 * Node.js' one-shot `crypto.hash` (Node.js v20.12+, v21.7+) avoids creating a
 * `Hash` object per call. (Not yet included in this project's `@types/node`.)
 */
const oneShotHash = (
  crypto as unknown as {
    hash?: (algorithm: string, data: Uint8Array, encoding: 'buffer') => Buffer;
  }
).hash;

const sha256 =
  oneShotHash === undefined
    ? (data: Uint8Array) => createHash('sha256').update(data).digest()
    : (data: Uint8Array) => oneShotHash('sha256', data, 'buffer');

/**
 * Compute the transaction hash (A.K.A. "transaction ID") of an encoded transaction:
 * the double-SHA256 hash, in user-interface (reversed) byte order, hex-encoded.
 */
export const encodedTransactionToHash = (encodedTransaction: Uint8Array) =>
  sha256(sha256(encodedTransaction)).reverse().toString('hex');

const coinbaseOutpointIndex = 0xffffffff;
const hashLength = 32;
const coinbaseOutpointTransactionHash = '00'.repeat(hashLength);

/**
 * Convert a bitcore transaction to the Chaingraph format.
 * @param bitcoreTransaction - the bitcore transaction
 * @param encodedTransaction - the serialized transaction, if available (e.g.
 * as received from the network); if not provided, the transaction is
 * re-serialized
 */
export const bitcoreTransactionToChaingraphTransaction = (
  bitcoreTransaction: BitcoreTransaction,
  encodedTransaction: Uint8Array = bitcoreTransaction.toBuffer()
): ChaingraphTransaction => {
  /**
   * Fields are read directly rather than via bitcore's `Input.toObject`, which
   * also decodes and stringifies each unlocking bytecode (`scriptString`).
   */
  const inputs = bitcoreTransaction.inputs.map((input) => ({
    outpointIndex: input.outputIndex,
    outpointTransactionHash: input.prevTxId.toString('hex'),
    sequenceNumber: input.sequenceNumber,
    // eslint-disable-next-line no-underscore-dangle
    unlockingBytecode: input._scriptBuffer.toString('hex'),
  }));
  /**
   * Equivalent to bitcore's `Transaction.isCoinbase`, reusing the hex-encoded
   * outpoint transaction hash.
   */
  const isCoinbase =
    inputs.length === 1 &&
    inputs[0]!.outpointIndex === coinbaseOutpointIndex &&
    inputs[0]!.outpointTransactionHash === coinbaseOutpointTransactionHash;
  return {
    hash: encodedTransactionToHash(encodedTransaction),
    inputs,
    isCoinbase,
    locktime: bitcoreTransaction.nLockTime,
    outputs: bitcoreTransaction.outputs.map((output) => {
      const { tokenData } = output;
      return {
        fungibleTokenAmount:
          tokenData === undefined
            ? undefined
            : BigInt(tokenData.amount.toString()),
        // eslint-disable-next-line no-underscore-dangle
        lockingBytecode: output._scriptBuffer.toString('hex'),
        nonfungibleTokenCapability: tokenData?.nft?.capability,
        nonfungibleTokenCommitment: tokenData?.nft?.commitment,
        tokenCategory: tokenData?.category,
        valueSatoshis: BigInt(output.satoshis),
      };
    }),
    sizeBytes: encodedTransaction.length,
    version: bitcoreTransaction.version,
  };
};

export const bitcoreBlockToChaingraphBlock = (
  bitcoreBlock: BitcoreBlock,
  height: number
): ChaingraphBlock => {
  const bitcoreBlockHeader = bitcoreBlock.header.toObject();
  const wireBytes = blockWireBytes.get(bitcoreBlock);
  const transactions =
    wireBytes === undefined
      ? bitcoreBlock.transactions.map((transaction) =>
          bitcoreTransactionToChaingraphTransaction(transaction)
        )
      : bitcoreBlock.transactions.map((transaction, index) =>
          bitcoreTransactionToChaingraphTransaction(
            transaction,
            wireBytes.encoded.subarray(
              wireBytes.transactionBoundaries[index],
              wireBytes.transactionBoundaries[index + 1]
            )
          )
        );
  return {
    bits: bitcoreBlockHeader.bits,
    hash: bitcoreBlockHeader.hash,
    height,
    merkleRoot: bitcoreBlockHeader.merkleRoot.toString('hex'),
    nonce: bitcoreBlockHeader.nonce,
    previousBlockHash: bitcoreBlockHeader.prevHash.toString('hex'),
    sizeBytes:
      wireBytes === undefined
        ? bitcoreBlock.toBuffer().length
        : wireBytes.transactionBoundaries[
            wireBytes.transactionBoundaries.length - 1
          ]!,
    timestamp: bitcoreBlockHeader.time,
    transactions,
    version: bitcoreBlockHeader.version,
  };
};

export const messages = new bitcoreP2pCash.Messages({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  Block: BlockRetainingWireBytes,
});
