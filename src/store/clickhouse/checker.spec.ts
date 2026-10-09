/* eslint-disable @typescript-eslint/no-magic-numbers, max-lines, max-params, functional/no-mixed-type */
// cspell:ignore clickhouse seqs
/**
 * `[e2e]` tests of the ClickHouse `StoreChecker` (WP5b): rows are seeded for
 * two nodes through the real commit protocol (CommitLog + VisibilityPublisher,
 * row encoders and test-support's save steps) in a scratch database
 * `ch1_wp5b_<random>`; every answer is asserted per node.
 *
 * Run with CHAINGRAPH_E2E_CLICKHOUSE_URL set (local ClickHouse only).
 */
import { randomBytes } from 'node:crypto';

import type { TransactionCommon } from '@bitauth/libauth';
import {
  binToHex,
  decodeTransaction,
  encodeTransaction,
  hashTransaction,
  hexToBin,
} from '@bitauth/libauth';
import test from 'ava';

import { bitcoreBlockToChaingraphBlock } from '../../bitcore.js';
import {
  chipnetCashTokensTx,
  genesisBlock,
  genesisBlockRaw,
  halTxRaw,
} from '../../e2e/e2e.spec.mockchain.helper.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../../types/chaingraph.js';

import { ClickHouseChecker } from './checker.js';
import { ClickHouseClient } from './client.js';
import type { CommitLease, OpenCommit } from './commit-log.js';
import { CommitLog } from './commit-log.js';
import { applyClickHouseDdl, dropClickHouseDatabase } from './ddl-apply.js';
import { RowBinaryWriter } from './row-binary.js';
import type { SpentOutput } from './row-encoders.js';
import {
  encodeBlockRows,
  encodeBlockTransactionRows,
  encodeInputRows,
  encodeOutputRows,
  encodeTransactionRows,
  rowBinaryTableColumns,
} from './row-encoders.js';
import { e2eClickHouseUrl, hashOf, testSaveSteps } from './test-support.js';
import { VisibilityPublisher } from './visibility.js';

const e2e = e2eClickHouseUrl === undefined ? test.skip : test.serial;

const nodeA = 1;
const nodeB = 2;
const server = {
  password: process.env.CHAINGRAPH_E2E_CLICKHOUSE_PASSWORD ?? '',
  url: e2eClickHouseUrl ?? '',
  username: process.env.CHAINGRAPH_E2E_CLICKHOUSE_USER ?? '',
};

const lease: CommitLease = { assertHeld: () => undefined, epoch: 1n };

/** A libauth-decoded fixture as a `ChaingraphTransaction`. */
const fromLibauth = (
  raw: string,
  isCoinbase = false
): { libauth: TransactionCommon; transaction: ChaingraphTransaction } => {
  const libauth = decodeTransaction(hexToBin(raw));
  if (typeof libauth === 'string') {
    // eslint-disable-next-line functional/no-throw-statement
    throw new Error(libauth);
  }
  const encoded = encodeTransaction(libauth);
  return {
    libauth,
    transaction: {
      hash: hashTransaction(encoded),
      inputs: libauth.inputs.map((input) => ({
        ...input,
        outpointTransactionHash: binToHex(input.outpointTransactionHash),
        unlockingBytecode: binToHex(input.unlockingBytecode),
      })),
      isCoinbase,
      locktime: libauth.locktime,
      outputs: libauth.outputs.map((output) => ({
        lockingBytecode: binToHex(output.lockingBytecode),
        valueSatoshis: output.valueSatoshis,
        ...(output.token === undefined
          ? {}
          : {
              fungibleTokenAmount: output.token.amount,
              tokenCategory: binToHex(output.token.category),
              ...(output.token.nft === undefined
                ? {}
                : {
                    nonfungibleTokenCapability: output.token.nft.capability,
                    nonfungibleTokenCommitment: binToHex(
                      output.token.nft.commitment
                    ),
                  }),
            }),
      })),
      sizeBytes: encoded.length,
      version: libauth.version,
    },
  };
};

/** Every spent output resolves to a fixed 1000-sat P2PKH-ish output. */
const anySpentOutput = (): SpentOutput => ({
  lockingBytecode: `76a914${'11'.repeat(20)}88ac`,
  valueSatoshis: 1000n,
});

interface Harness {
  checker: ClickHouseChecker;
  client: ClickHouseClient;
  log: CommitLog;
  publisher: VisibilityPublisher;
  /** Run `steps` under one commit for `nodeScope`; commit unless told not to. */
  commit: (
    nodeScope: number[],
    steps: (commit: OpenCommit) => Promise<void>,
    finish?: 'abort' | 'commit' | 'leave-open'
  ) => Promise<OpenCommit>;
}

const columnsOf = (table: keyof typeof rowBinaryTableColumns) =>
  rowBinaryTableColumns[table].map(([name]) => name);

const nodeTransactionColumns = [
  'node_internal_id',
  'transaction_internal_id',
  'transaction_hash',
  'validated_at',
  'sign',
  'version',
  'commit_seq',
];
const nodeBlockColumns = [
  'node_internal_id',
  'block_internal_id',
  'block_hash',
  'height',
  'accepted_at',
  'sign',
  'version',
  'commit_seq',
];
const txAcceptanceColumns = [
  'transaction_hash',
  'node_internal_id',
  'block_internal_id',
  'transaction_internal_id',
  'height',
  'accepted_at',
  'sign',
  'version',
  'commit_seq',
];
const utxoColumns = [
  'node_internal_id',
  'token_category',
  'transaction_hash',
  'output_index',
  'transaction_internal_id',
  'created_height',
  'value_satoshis',
  'locking_bytecode',
  'fungible_token_amount',
  'nonfungible_token_capability',
  'nonfungible_token_commitment',
  'sign',
  'version',
  'commit_seq',
];

const capabilityEnum = { minting: 3, mutable: 2, none: 1 } as const;
const byHex = (a: string, b: string) => (a < b ? -1 : Number(a > b));

const setUp = async (t: {
  teardown: (fn: () => Promise<void>) => void;
}): Promise<Harness> => {
  const database = `ch1_wp5b_${randomBytes(4).toString('hex')}`;
  await applyClickHouseDdl(server, database, { recreate: true });
  const client = new ClickHouseClient({
    database,
    password: server.password,
    requestTimeoutMs: 60_000,
    url: server.url,
    username: server.username,
  });
  t.teardown(async () => {
    await client.close();
    await dropClickHouseDatabase(server, database);
  });
  await client.command(
    `INSERT INTO node (internal_id, name, protocol_version, user_agent, first_connected_at, latest_connection_began_at, updated_at, commit_seq)
     VALUES (1, 'nodeA', 70016, '/a/', now64(3), now64(3), now64(3), 0),
            (2, 'nodeB', 70016, '/b/', now64(3), now64(3), now64(3), 0)`
  );
  const log = new CommitLog(client, lease);
  await log.init();
  const publisher = new VisibilityPublisher(client, log);
  await publisher.init();
  publisher.registerNode(nodeA);
  publisher.registerNode(nodeB);
  await publisher.publishWatermark();
  return {
    checker: new ClickHouseChecker(client, database),
    client,
    commit: async (nodeScope, steps, finish = 'commit') => {
      const commit = await log.beginCommit({
        kind: 'mempool_batch',
        nodeScope,
      });
      await steps(commit);
      if (finish === 'commit') {
        await log.markCommitted(commit.seq, {});
      } else if (finish === 'abort') {
        await log.markAborted(commit.seq, 'test abort');
      }
      return commit;
    },
    log,
    publisher,
  };
};

const insert = async (
  client: ClickHouseClient,
  commit: OpenCommit,
  table: string,
  columns: readonly string[],
  data: Uint8Array,
  chunk: number | string = 0
) =>
  client.insertRowBinary(table, columns, data, {
    deduplicationToken: commit.token(table, chunk),
  });

/** Node-agnostic rows of `transactions` (ids from `firstId`). */
const saveTransactions = async (
  client: ClickHouseClient,
  commit: OpenCommit,
  transactions: ChaingraphTransaction[],
  firstId: number
) => {
  const context = {
    commitSeq: commit.seq,
    transactionInternalIds: transactions.map((_, index) => firstId + index),
  };
  const table = columnsOf;
  await insert(
    client,
    commit,
    'transaction',
    table('transaction'),
    encodeTransactionRows(transactions, context).data
  );
  await insert(
    client,
    commit,
    'output',
    table('output'),
    encodeOutputRows(transactions, context).data
  );
  await insert(
    client,
    commit,
    'input',
    table('input'),
    encodeInputRows(transactions, context, anySpentOutput).data
  );
};

/** node_transaction + tx_acceptance (block 0) +1 rows: in `node`'s mempool. */
const acceptIntoMempool = async (
  client: ClickHouseClient,
  commit: OpenCommit,
  node: number,
  entries: { hash: string; id: number; validatedAt: Date }[],
  sign = 1,
  version = commit.seq
) => {
  const mempoolRows = new RowBinaryWriter();
  const acceptanceRows = new RowBinaryWriter();
  entries.forEach(({ hash, id, validatedAt }) => {
    mempoolRows
      .uint32(node)
      .uint64(id)
      .fixedString32(hash)
      .nullable(validatedAt, (w, date) => w.dateTime64(date))
      .int8(sign)
      .uint64(version)
      .uint64(commit.seq)
      .endRow();
    acceptanceRows
      .fixedString32(hash)
      .uint32(node)
      .uint64(0)
      .uint64(id)
      .uint32(0)
      .nullable(validatedAt, (w, date) => w.dateTime64(date))
      .int8(sign)
      .uint64(version)
      .uint64(commit.seq)
      .endRow();
  });
  await insert(
    client,
    commit,
    'node_transaction',
    nodeTransactionColumns,
    mempoolRows.finish(),
    `n${node}`
  );
  await insert(
    client,
    commit,
    'tx_acceptance',
    txAcceptanceColumns,
    acceptanceRows.finish(),
    `n${node}`
  );
};

/** utxo + utxo_by_script +1 rows for every output of `transaction`. */
const addUtxos = async (
  client: ClickHouseClient,
  commit: OpenCommit,
  node: number,
  transaction: ChaingraphTransaction,
  id: number
) => {
  const byCategory = new RowBinaryWriter();
  const byScript = new RowBinaryWriter();
  transaction.outputs.forEach((output, index) => {
    const category = output.tokenCategory ?? '00'.repeat(32);
    const writeRest = (writer: RowBinaryWriter) =>
      writer
        .uint32(index)
        .uint64(id)
        .uint32(0)
        .int64(output.valueSatoshis)
        .hexBytes(output.lockingBytecode);
    const writeToken = (writer: RowBinaryWriter) =>
      writer
        .nullable(output.fungibleTokenAmount, (w, amount) => w.int64(amount))
        .nullable(output.nonfungibleTokenCapability, (w, capability) =>
          w.enum8(capabilityEnum[capability])
        )
        .nullable(output.nonfungibleTokenCommitment, (w, commitment) =>
          w.hexBytes(commitment)
        )
        .int8(1)
        .uint64(1)
        .uint64(commit.seq)
        .endRow();
    writeToken(
      writeRest(
        byCategory
          .uint32(node)
          .fixedString32(category)
          .fixedString32(transaction.hash)
      )
    );
    writeToken(
      writeRest(byScript.uint32(node).fixedString32(transaction.hash))
        // utxo_by_script: token_category after locking_bytecode
        .fixedString32(category)
    );
  });
  await insert(
    client,
    commit,
    'utxo',
    utxoColumns,
    byCategory.finish(),
    `n${node}t${id}`
  );
  await insert(
    client,
    commit,
    'utxo_by_script',
    [
      'node_internal_id',
      'transaction_hash',
      'output_index',
      'transaction_internal_id',
      'created_height',
      'value_satoshis',
      'locking_bytecode',
      'token_category',
      'fungible_token_amount',
      'nonfungible_token_capability',
      'nonfungible_token_commitment',
      'sign',
      'version',
      'commit_seq',
    ],
    byScript.finish(),
    `n${node}t${id}`
  );
};

/** A block's node-agnostic rows plus `node`'s acceptance of it and its txs. */
const saveBlock = async (
  client: ClickHouseClient,
  commit: OpenCommit,
  block: ChaingraphBlock,
  blockId: number,
  firstTransactionId: number,
  acceptedBy: { acceptedAt: Date; node: number }[]
) => {
  const ids = block.transactions.map((_, index) => firstTransactionId + index);
  await saveTransactions(
    client,
    commit,
    block.transactions,
    firstTransactionId
  );
  await insert(
    client,
    commit,
    'block',
    columnsOf('block'),
    encodeBlockRows(
      [{ block, generatedValueSatoshis: 0n, internalId: blockId }],
      commit.seq
    ).data
  );
  await insert(
    client,
    commit,
    'block_transaction',
    columnsOf('block_transaction'),
    encodeBlockTransactionRows(block, blockId, {
      commitSeq: commit.seq,
      transactionInternalIds: ids,
    }).data
  );
  await acceptedBy.reduce<Promise<void>>(
    async (previous, { acceptedAt, node }) =>
      previous.then(async () => {
        const nodeBlock = new RowBinaryWriter()
          .uint32(node)
          .uint64(blockId)
          .fixedString32(block.hash)
          .uint32(block.height)
          .nullable(acceptedAt, (w, date) => w.dateTime64(date))
          .int8(1)
          .uint64(commit.seq)
          .uint64(commit.seq)
          .endRow()
          .finish();
        await insert(
          client,
          commit,
          'node_block',
          nodeBlockColumns,
          nodeBlock,
          `n${node}`
        );
        const acceptance = new RowBinaryWriter();
        block.transactions.forEach((transaction, index) => {
          acceptance
            .fixedString32(transaction.hash)
            .uint32(node)
            .uint64(blockId)
            .uint64(ids[index]!)
            .uint32(block.height)
            .nullable(acceptedAt, (w, date) => w.dateTime64(date))
            .int8(1)
            .uint64(commit.seq)
            .uint64(commit.seq)
            .endRow();
        });
        await insert(
          client,
          commit,
          'tx_acceptance',
          txAcceptanceColumns,
          acceptance.finish(),
          `b${blockId}n${node}`
        );
      }),
    Promise.resolve()
  );
};

const hal = fromLibauth(halTxRaw);
const tokens = fromLibauth(chipnetCashTokensTx);
const validatedAt = new Date('2026-01-15T00:00:00.123Z');
const categoryB = hashOf(0xb0b).toString('hex');

e2e(
  '[e2e] ClickHouseChecker: per-node mempool, acceptance and UTXOs never leak across nodes',
  async (t) => {
    const harness = await setUp(t);
    const { checker, client } = harness;
    await harness.commit([nodeA], async (commit) => {
      await saveTransactions(
        client,
        commit,
        [hal.transaction, tokens.transaction],
        10
      );
      await acceptIntoMempool(client, commit, nodeA, [
        { hash: hal.transaction.hash, id: 10, validatedAt },
        { hash: tokens.transaction.hash, id: 11, validatedAt },
      ]);
      await addUtxos(client, commit, nodeA, hal.transaction, 10);
      await addUtxos(client, commit, nodeA, tokens.transaction, 11);
    });
    await harness.commit([nodeB], async (commit) => {
      await Promise.all(
        testSaveSteps(client, commit, {
          category: Buffer.from(categoryB, 'hex'),
          nodeId: nodeB,
          outputs: 2,
          transactionId: 900,
        }).map(async (step) => step())
      );
    });
    await harness.publisher.publishWatermark();

    const halHash = hal.transaction.hash;
    const tokensHash = tokens.transaction.hash;
    const bHash = hashOf(900).toString('hex');
    t.deepEqual(await checker.nodeNamesOrdered(), [
      { internalId: 1, name: 'nodeA' },
      { internalId: 2, name: 'nodeB' },
    ]);
    t.is(await checker.nodeInternalId('nodeB'), 2);
    t.is(await checker.nodeInternalId('nobody'), undefined);

    t.deepEqual(
      await checker.mempool('nodeA'),
      [halHash, tokensHash].sort(byHex).map((hash) => ({ hash, validatedAt }))
    );
    t.deepEqual(await checker.mempool('nodeB'), []);
    t.deepEqual(
      [...(await checker.mempoolMembership('nodeA', [halHash, bHash]))],
      [halHash]
    );
    t.deepEqual(
      [...(await checker.mempoolMembership('nodeB', [halHash, bHash]))],
      []
    );
    t.deepEqual(await checker.validatingNodes(halHash), ['nodeA']);
    t.true(await checker.txAccepted('nodeA', halHash));
    t.false(await checker.txAccepted('nodeB', halHash));
    t.true(await checker.txAccepted('nodeB', bHash));
    t.false(await checker.txAccepted('nodeA', bHash));
    t.false(await checker.txAccepted('nobody', halHash));

    const halOutpoints = hal.transaction.outputs.map((_, outputIndex) => ({
      outputIndex,
      transactionHash: halHash,
    }));
    const tokenOutpoints = tokens.transaction.outputs.map((_, outputIndex) => ({
      outputIndex,
      transactionHash: tokensHash,
    }));
    t.deepEqual(
      await checker.unspent('nodeA', {}),
      [...halOutpoints, ...tokenOutpoints].sort((a, b) =>
        a.transactionHash === b.transactionHash
          ? a.outputIndex - b.outputIndex
          : a.transactionHash < b.transactionHash
          ? -1
          : 1
      )
    );
    const bOutpoints = [0, 1].map((outputIndex) => ({
      outputIndex,
      transactionHash: bHash,
    }));
    t.deepEqual(await checker.unspent('nodeB', {}), bOutpoints);
    // by category
    t.deepEqual(
      await checker.unspent('nodeB', { category: categoryB }),
      bOutpoints
    );
    t.deepEqual(await checker.unspent('nodeA', { category: categoryB }), []);
    const tokenCategory = tokens.transaction.outputs.find(
      (output) => output.tokenCategory !== undefined
    )!.tokenCategory!;
    t.deepEqual(
      await checker.unspent('nodeA', { category: tokenCategory }),
      tokens.transaction.outputs
        .map((output, outputIndex) => ({ output, outputIndex }))
        .filter(({ output }) => output.tokenCategory === tokenCategory)
        .map(({ outputIndex }) => ({
          outputIndex,
          transactionHash: tokensHash,
        }))
    );
    t.deepEqual(
      await checker.unspent('nodeB', { category: tokenCategory }),
      []
    );
    // by locking bytecode (test-support writes `script-<tx>-<index>`)
    const scriptB1 = Buffer.from('script-900-1').toString('hex');
    t.deepEqual(await checker.unspent('nodeB', { lockingBytecode: scriptB1 }), [
      { outputIndex: 1, transactionHash: bHash },
    ]);
    t.deepEqual(
      await checker.unspent('nodeA', { lockingBytecode: scriptB1 }),
      []
    );
    const halScript = hal.transaction.outputs[0]!.lockingBytecode;
    t.deepEqual(
      await checker.unspent('nodeA', { lockingBytecode: halScript }),
      hal.transaction.outputs
        .map((output, outputIndex) => ({ output, outputIndex }))
        .filter(({ output }) => output.lockingBytecode === halScript)
        .map(({ outputIndex }) => ({ outputIndex, transactionHash: halHash }))
    );
    t.deepEqual(
      await checker.unspent('nodeA', {
        category: categoryB,
        lockingBytecode: halScript,
      }),
      []
    );
    t.deepEqual(await checker.confirmedButInMempool('nodeA'), []);
    t.deepEqual(await checker.orphanMempoolDescendants('nodeA'), []);
  }
);

e2e(
  '[e2e] ClickHouseChecker: encodedTransactionHex round-trips libauth fixtures',
  async (t) => {
    const harness = await setUp(t);
    const { checker, client } = harness;
    await harness.commit([], async (commit) => {
      await saveTransactions(
        client,
        commit,
        [hal.transaction, tokens.transaction],
        1
      );
    });
    await harness.publisher.publishWatermark();
    t.is(binToHex(encodeTransaction(hal.libauth)), halTxRaw);
    t.is(await checker.encodedTransactionHex(hal.transaction.hash), halTxRaw);
    t.is(
      await checker.encodedTransactionHex(tokens.transaction.hash),
      binToHex(encodeTransaction(tokens.libauth))
    );
    t.is(
      await checker.encodedTransactionHex(tokens.transaction.hash),
      chipnetCashTokensTx
    );
    t.is(
      await checker.encodedTransactionHex(hashOf(404).toString('hex')),
      undefined
    );
    t.true(await checker.transactionExists(hal.transaction.hash));
    t.is(await checker.transactionRowCount(hal.transaction.hash), 1);
    t.false(await checker.transactionExists(hashOf(404).toString('hex')));

    const outputs = await checker.outputsOfTx(tokens.transaction.hash);
    t.deepEqual(
      outputs,
      tokens.transaction.outputs.map((output, outputIndex) => ({
        lockingBytecode: output.lockingBytecode,
        outputIndex,
        valueSatoshis: output.valueSatoshis,
        ...(output.tokenCategory === undefined
          ? {}
          : { tokenCategory: output.tokenCategory }),
        ...(output.fungibleTokenAmount === undefined
          ? {}
          : { fungibleTokenAmount: output.fungibleTokenAmount }),
        ...(output.nonfungibleTokenCapability === undefined
          ? {}
          : { nonfungibleTokenCapability: output.nonfungibleTokenCapability }),
        ...(output.nonfungibleTokenCommitment === undefined
          ? {}
          : { nonfungibleTokenCommitment: output.nonfungibleTokenCommitment }),
      }))
    );
    t.deepEqual(
      await checker.inputsOfTx(hal.transaction.hash),
      hal.transaction.inputs.map((input, inputIndex) => ({
        inputIndex,
        outpointIndex: input.outpointIndex,
        outpointTransactionHash: input.outpointTransactionHash,
        sequenceNumber: input.sequenceNumber,
        unlockingBytecode: input.unlockingBytecode,
      }))
    );
    const [firstInput] = hal.transaction.inputs;
    t.deepEqual(
      await checker.inputsSpending(
        firstInput!.outpointTransactionHash,
        firstInput!.outpointIndex
      ),
      [{ inputIndex: 0, txHash: hal.transaction.hash }]
    );
  }
);

e2e(
  '[e2e] ClickHouseChecker: rows of open, aborted and unpublished commits are invisible',
  async (t) => {
    const harness = await setUp(t);
    const { checker, client } = harness;
    const save = (transactionId: number) => async (commit: OpenCommit) => {
      await Promise.all(
        testSaveSteps(client, commit, {
          category: Buffer.from(categoryB, 'hex'),
          nodeId: nodeA,
          outputs: 1,
          transactionId,
        }).map(async (step) => step())
      );
    };
    const hashHex = (id: number) => hashOf(id).toString('hex');
    // open (never committed): it also holds the watermark of nodeA
    const open = await harness.commit([nodeA], save(901), 'leave-open');
    await harness.commit([nodeA], save(902), 'abort');
    await harness.publisher.publishWatermark();
    t.false(await checker.txAccepted('nodeA', hashHex(901)));
    t.false(await checker.txAccepted('nodeA', hashHex(902)));
    t.deepEqual(await checker.unspent('nodeA', {}), []);
    t.deepEqual(await checker.outputsOfTx(hashHex(901)), []);
    t.deepEqual(await checker.outputsOfTx(hashHex(902)), []);
    // abort the open one: its rows stay invisible after the watermark passes it
    await harness.log.markAborted(open.seq, 'test abort');
    /*
     * committed but not yet published: node facts hidden, node-agnostic rows
     * visible through the committed tail
     */
    await harness.commit([nodeA], save(903));
    t.false(await checker.txAccepted('nodeA', hashHex(903)));
    t.is((await checker.outputsOfTx(hashHex(903))).length, 1);
    await harness.publisher.publishWatermark();
    t.false(await checker.txAccepted('nodeA', hashHex(901)));
    t.false(await checker.txAccepted('nodeA', hashHex(902)));
    t.true(await checker.txAccepted('nodeA', hashHex(903)));
    t.false(await checker.txAccepted('nodeB', hashHex(903)));
    t.deepEqual(await checker.unspent('nodeA', {}), [
      { outputIndex: 0, transactionHash: hashHex(903) },
    ]);
    t.deepEqual(await checker.outputsOfTx(hashHex(901)), []);
  }
);

e2e(
  '[e2e] ClickHouseChecker: blocks, encodings, aggregates and per-node acceptance',
  async (t) => {
    const harness = await setUp(t);
    const { checker, client } = harness;
    const genesis = bitcoreBlockToChaingraphBlock(genesisBlock, 0);
    const acceptedAt = new Date('2026-02-01T12:34:56.789Z');
    await harness.commit([nodeA], async (commit) => {
      await saveBlock(client, commit, genesis, 1, 100, [
        { acceptedAt, node: nodeA },
      ]);
      // tx 0 is also still in nodeA's mempool: confirmedButInMempool
      await acceptIntoMempool(client, commit, nodeA, [
        { hash: genesis.transactions[0]!.hash, id: 100, validatedAt },
      ]);
    });
    await harness.publisher.publishWatermark();
    t.is(await checker.encodedBlockHex({ height: 0 }), genesisBlockRaw);
    t.is(
      await checker.encodedBlockHex({ hash: genesis.hash }),
      genesisBlockRaw
    );
    t.is(
      await checker.encodedBlockHeaderHex({ height: 0 }),
      genesisBlockRaw.slice(0, 160)
    );
    t.is(await checker.encodedBlockHex({ height: 1 }), undefined);
    t.deepEqual(await checker.blockValueAggregates({ height: 0 }), {
      fee: 0n,
      generated: 5000000000n,
      input: 0n,
      output: 5000000000n,
    });
    t.is(await checker.blockValueAggregates({ height: 7 }), undefined);
    t.deepEqual(await checker.allBlockHashes(), [genesis.hash]);
    t.is(await checker.blockTransactionCount(genesis.hash), 1);
    t.is(
      await checker.blockTransactionAt(genesis.hash, 0),
      genesis.transactions[0]!.hash
    );
    t.is(await checker.blockTransactionAt(genesis.hash, 1), undefined);
    t.deepEqual(await checker.acceptedBlocks('nodeA'), [
      { acceptedAt, hash: genesis.hash, height: 0 },
    ]);
    t.deepEqual(await checker.acceptedBlocks('nodeA', { height: 1 }), []);
    t.deepEqual(await checker.acceptedBlocks('nodeB'), []);
    t.is(await checker.acceptedBlockCount('nodeA', [genesis.hash]), 1);
    t.is(await checker.acceptedBlockCount('nodeB', [genesis.hash]), 0);
    t.true(await checker.txAccepted('nodeA', genesis.transactions[0]!.hash));
    t.false(await checker.txAccepted('nodeB', genesis.transactions[0]!.hash));
    t.deepEqual(await checker.confirmedButInMempool('nodeA'), [
      genesis.transactions[0]!.hash,
    ]);
    t.deepEqual(await checker.confirmedButInMempool('nodeB'), []);

    // a block with several transactions, accepted by nodeB only
    const multi = {
      ...genesis,
      hash: hashOf(0xb10c).toString('hex'),
      height: 1,
      previousBlockHash: genesis.hash,
      transactions: [
        { ...genesis.transactions[0]!, hash: hashOf(0xc0).toString('hex') },
        hal.transaction,
        tokens.transaction,
      ],
    };
    await harness.commit([nodeB], async (commit) => {
      await saveBlock(client, commit, multi, 2, 200, [
        { acceptedAt, node: nodeB },
      ]);
    });
    await harness.publisher.publishWatermark();
    const expectedMultiHex = [
      genesisBlockRaw.slice(0, 8),
      binToHex(hexToBin(genesis.hash).reverse()),
      genesisBlockRaw.slice(72, 160),
      '03',
      genesisBlockRaw.slice(162),
      halTxRaw,
      chipnetCashTokensTx,
    ].join('');
    t.is(await checker.encodedBlockHex({ hash: multi.hash }), expectedMultiHex);
    t.deepEqual(
      (await checker.acceptedBlocks('nodeB')).map((block) => block.hash),
      [multi.hash]
    );
    t.deepEqual(
      (await checker.acceptedBlocks('nodeA')).map((block) => block.hash),
      [genesis.hash]
    );
    const aggregates = await checker.blockValueAggregates({ hash: multi.hash });
    const output =
      5000000000n +
      [hal, tokens].reduce(
        (sum, { transaction }) =>
          sum +
          transaction.outputs.reduce(
            (total, { valueSatoshis }) => total + valueSatoshis,
            0n
          ),
        0n
      );
    const input = BigInt(
      (hal.transaction.inputs.length + tokens.transaction.inputs.length) * 1000
    );
    t.deepEqual(aggregates, {
      fee: input - (output - 5000000000n),
      generated: output - input,
      input,
      output,
    });

    // fault injection: lose one link; the block shows one transaction fewer
    await checker.dropBlockTransactionLink(multi.hash, 1);
    t.is(await checker.blockTransactionCount(multi.hash), 2);
    t.is(await checker.blockTransactionAt(multi.hash, 1), undefined);
    t.is(await checker.blockTransactionCount(genesis.hash), 1);

    const report = await checker.schemaReport();
    t.deepEqual(report.triggers, {});
    t.true(report.indexes.includes('table:utxo'));
    t.true(report.indexes.includes('projection:output.p_script'));
    t.true(report.indexes.includes('index:output.bf_locking_bytecode'));
    t.false(report.indexes.some((entry) => entry.endsWith('_at')));
  }
);

e2e(
  '[e2e] ClickHouseChecker: history, orphans and forgetNodeValidation per node',
  async (t) => {
    const harness = await setUp(t);
    const { checker, client } = harness;
    const hashHex = (id: number) => hashOf(id).toString('hex');
    /** tx `id` spending output 0 of `parent` */
    const child = (id: number, parent: string): ChaingraphTransaction => ({
      ...hal.transaction,
      hash: hashHex(id),
      inputs: [
        {
          ...hal.transaction.inputs[0]!,
          outpointIndex: 0,
          outpointTransactionHash: parent,
        },
      ],
    });
    const replacedParent = child(1, hashHex(999));
    const orphan = child(2, replacedParent.hash);
    const grandchild = child(3, orphan.hash);
    const unrelated = child(4, hashHex(998));
    const replacedAt = new Date('2026-03-01T00:00:00.000Z');
    await harness.commit([nodeA, nodeB], async (commit) => {
      await saveTransactions(
        client,
        commit,
        [replacedParent, orphan, grandchild, unrelated],
        1
      );
      const history = new RowBinaryWriter()
        .uint32(nodeA)
        .uint64(1)
        .uint64(1)
        .nullable(validatedAt, (w, date) => w.dateTime64(date))
        .nullable(replacedAt, (w, date) => w.dateTime64(date))
        .uint64(commit.seq)
        .endRow()
        .finish();
      await insert(
        client,
        commit,
        'node_transaction_history',
        [
          'node_internal_id',
          'transaction_internal_id',
          'internal_id',
          'validated_at',
          'replaced_at',
          'commit_seq',
        ],
        history
      );
      const blockHistory = new RowBinaryWriter()
        .uint32(nodeB)
        .dateTime64(replacedAt)
        .uint64(7)
        .uint64(1)
        .nullable(validatedAt, (w, date) => w.dateTime64(date))
        .uint64(commit.seq)
        .endRow()
        .finish();
      await insert(
        client,
        commit,
        'node_block_history',
        [
          'node_internal_id',
          'removed_at',
          'block_internal_id',
          'internal_id',
          'accepted_at',
          'commit_seq',
        ],
        blockHistory
      );
      await insert(
        client,
        commit,
        'block',
        columnsOf('block'),
        encodeBlockRows(
          [
            {
              block: { ...bitcoreBlockToChaingraphBlock(genesisBlock, 0) },
              generatedValueSatoshis: 0n,
              internalId: 7,
            },
          ],
          commit.seq
        ).data
      );
      const mempool = [orphan, grandchild, unrelated].map(
        (transaction, index) => ({
          hash: transaction.hash,
          id: index + 2,
          validatedAt,
        })
      );
      await acceptIntoMempool(client, commit, nodeA, mempool);
      await acceptIntoMempool(client, commit, nodeB, mempool);
    });
    await harness.publisher.publishWatermark();

    t.deepEqual(await checker.transactionHistory('nodeA'), [
      { hash: replacedParent.hash, replacedAt, validatedAt },
    ]);
    t.deepEqual(await checker.transactionHistory('nodeA', [orphan.hash]), []);
    t.deepEqual(await checker.transactionHistory('nodeB'), []);
    t.deepEqual(await checker.blockHistory('nodeA'), []);
    t.deepEqual(await checker.blockHistory('nodeB'), [
      {
        acceptedAt: validatedAt,
        hash: bitcoreBlockToChaingraphBlock(genesisBlock, 0).hash,
        removedAt: replacedAt,
      },
    ]);
    t.deepEqual(
      await checker.orphanMempoolDescendants('nodeA'),
      [orphan.hash, grandchild.hash].sort(byHex)
    );
    // nodeB never replaced the parent
    t.deepEqual(await checker.orphanMempoolDescendants('nodeB'), []);

    t.deepEqual(await checker.validatingNodes(orphan.hash), ['nodeA', 'nodeB']);
    await checker.forgetNodeValidation('nodeB', orphan.hash);
    t.deepEqual(await checker.validatingNodes(orphan.hash), ['nodeA']);
    t.false(await checker.txAccepted('nodeB', orphan.hash));
    t.true(await checker.txAccepted('nodeA', orphan.hash));
    t.deepEqual(
      (await checker.mempool('nodeB')).map((entry) => entry.hash),
      [grandchild.hash, unrelated.hash].sort(byHex)
    );
    // idempotent (deduplicated), and a no-op for an unknown node or tx
    await checker.forgetNodeValidation('nodeB', orphan.hash);
    await checker.forgetNodeValidation('nobody', orphan.hash);
    await checker.forgetNodeValidation('nodeB', hashHex(12345));
    t.deepEqual(await checker.validatingNodes(orphan.hash), ['nodeA']);
    t.is((await checker.mempool('nodeA')).length, 3);
  }
);
