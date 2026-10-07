import type {
  BitcoreBlock,
  BitcoreTransaction,
} from '@chaingraph/bitcore-p2p-cash';
import test from 'ava';

import {
  bitcoreBlockToChaingraphBlock,
  bitcoreTransactionToChaingraphTransaction,
  decodeBlockRetainingWireBytes,
  messages,
} from './bitcore.js';
import {
  Block,
  chipnetCashTokensTx,
  chipnetCashTokensTxHash,
  generateMockchain,
  genesisBlockRaw,
  halTxHash,
  halTxRaw,
  halTxSpent,
  halTxSpentRaw,
  testnetGenesisBlockRaw,
  Transaction,
} from './e2e/e2e.spec.mockchain.helper.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from './types/chaingraph.js';

/**
 * The implementation prior to retaining wire bytes (re-serializes every
 * transaction and the full block), kept as a reference for parity tests.
 */
const referenceTransactionConversion = (
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

const referenceBlockConversion = (
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
    transactions: bitcoreBlock.transactions.map(referenceTransactionConversion),
    version: bitcoreBlockHeader.version,
  };
};

const realTransactions = [halTxRaw, halTxSpentRaw, chipnetCashTokensTx];

/**
 * A block containing real mainnet and chipnet (CashTokens) transactions.
 */
const realTransactionsBlock = () => {
  const genesis = Block.fromString(genesisBlockRaw);
  const coinbase = genesis.transactions[0]!.toObject();
  return Block.fromObject({
    header: {
      bits: 0x1d00ffff,
      merkleRoot:
        '0000000000000000000000000000000000000000000000000000000000000000',
      nonce: 1,
      prevHash: genesis.header.hash,
      time: 1231006506,
      version: 1,
    },
    transactions: [
      coinbase,
      ...realTransactions.map((raw) => new Transaction(raw).toObject()),
    ],
  });
};

const encodedTestBlocks = () => [
  Buffer.from(genesisBlockRaw, 'hex'),
  Buffer.from(testnetGenesisBlockRaw, 'hex'),
  Buffer.from(realTransactionsBlock().toBuffer()),
  ...generateMockchain({
    length: 50,
    previousBlockHash: Block.fromString(genesisBlockRaw).header.hash,
  }).map((block) => Buffer.from(block.toBuffer())),
];

test('decodeBlockRetainingWireBytes: decodes like Block.fromBuffer', (t) => {
  encodedTestBlocks().forEach((encoded) => {
    const original = Buffer.from(encoded);
    const block = decodeBlockRetainingWireBytes(encoded);
    t.deepEqual(encoded, original, 'does not modify the encoded block');
    t.deepEqual(block.toObject(), Block.fromBuffer(original).toObject());
    t.deepEqual(
      decodeBlockRetainingWireBytes(encoded).toObject(),
      Block.fromBuffer(encoded).toObject()
    );
  });
});

test('bitcoreBlockToChaingraphBlock: matches the re-serializing implementation', (t) => {
  /**
   * Note: bitcore's `Block.fromBuffer` modifies the provided buffer (token
   * categories are reversed in place), so each decoding gets a fresh copy.
   */
  encodedTestBlocks().forEach((encoded, height) => {
    const expected = referenceBlockConversion(
      Block.fromBuffer(Buffer.from(encoded)),
      height
    );
    t.is(expected.sizeBytes, encoded.length);
    t.deepEqual(
      bitcoreBlockToChaingraphBlock(
        decodeBlockRetainingWireBytes(Buffer.from(encoded)),
        height
      ),
      expected,
      'with wire bytes'
    );
    t.deepEqual(
      bitcoreBlockToChaingraphBlock(
        messages.Block.fromBuffer(Buffer.from(encoded)).block,
        height
      ),
      expected,
      'decoded by Messages'
    );
    t.deepEqual(
      bitcoreBlockToChaingraphBlock(
        Block.fromBuffer(Buffer.from(encoded)),
        height
      ),
      expected,
      'without wire bytes'
    );
  });
});

test('bitcoreBlockToChaingraphBlock: real transaction hashes', (t) => {
  const block = bitcoreBlockToChaingraphBlock(
    decodeBlockRetainingWireBytes(
      Buffer.from(realTransactionsBlock().toBuffer())
    ),
    1
  );
  t.deepEqual(
    block.transactions.slice(1).map((tx) => tx.hash),
    [halTxHash, halTxSpent, chipnetCashTokensTxHash]
  );
  t.true(block.transactions[0]!.isCoinbase);
  t.true(
    block.transactions[3]!.outputs.some(
      (output) => output.tokenCategory !== undefined
    )
  );
});

test('bitcoreTransactionToChaingraphTransaction: matches the re-serializing implementation', (t) => {
  realTransactions.forEach((raw) => {
    t.deepEqual(
      bitcoreTransactionToChaingraphTransaction(new Transaction(raw)),
      referenceTransactionConversion(new Transaction(raw))
    );
  });
});
