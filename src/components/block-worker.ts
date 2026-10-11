/* eslint-disable @typescript-eslint/naming-convention, complexity, functional/no-let, functional/no-loop-statement */
// cspell:ignore Varint
/**
 * Block worker (a `worker_threads` worker of `BlockWorkerPool`): parses a raw
 * block and converts a range of its transactions to Chaingraph form with the
 * code the main thread uses (`bitcore.ts`), and optionally encodes their row
 * templates (`row-templates.ts`). The transactions go back in slices (each
 * slice is one message, so the main thread deserializes a large block in
 * several turns); the templates go back as transferred ArrayBuffers.
 */
import { parentPort } from 'node:worker_threads';

import type {
  BitcoreBlockHeader,
  BitcoreTransaction,
} from '@chaingraph/bitcore-p2p-cash';
import bitcoreP2pCash from '@chaingraph/bitcore-p2p-cash';

import {
  bitcoreHeaderToChaingraph,
  bitcoreTransactionToChaingraphTransaction,
  transactionEnd,
} from '../bitcore.js';
import {
  encodeTransactionRowTemplates,
  templateBuffers,
} from '../store/clickhouse/row-templates.js';

import type {
  BlockWorkerAck,
  BlockWorkerReply,
  BlockWorkerRequest,
} from './block-worker-protocol.js';

/** The bitcore parts the worker parses with (untyped in bitcore's typings). */
const bitcore = bitcoreP2pCash.internalBitcore as unknown as {
  encoding: {
    BufferReader: new (buffer: Buffer) => {
      pos: number;
      readVarintNum: () => number;
    };
  };
  BlockHeader: { fromBufferReader: (reader: unknown) => BitcoreBlockHeader };
  Transaction: new () => BitcoreTransaction & {
    fromBufferReader: (reader: unknown) => BitcoreTransaction;
  };
};

const port = parentPort;
if (port === null) {
  // eslint-disable-next-line functional/no-throw-statement
  throw new Error('block-worker.js must run as a worker thread.');
}

const post = (reply: BlockWorkerReply, transfer: ArrayBufferLike[] = []) => {
  port.postMessage(reply, transfer as ArrayBuffer[]);
};

/** Per conversion, the replies not sent yet (sent one per ack). */
const outbox = new Map<
  number,
  { reply: BlockWorkerReply; transfer: ArrayBufferLike[] }[]
>();

const sendNext = (id: number) => {
  const queue = outbox.get(id);
  const next = queue?.shift();
  if (next === undefined) return;
  if (queue!.length === 0) outbox.delete(id);
  post(next.reply, next.transfer);
};

const convert = (request: BlockWorkerRequest) => {
  const { count, first, header, height, id, raw, rowTemplates } = request;
  const replies: { reply: BlockWorkerReply; transfer: ArrayBufferLike[] }[] =
    [];
  // eslint-disable-next-line functional/no-try-statement
  try {
    /*
     * A copy of our own: bitcore's parse writes to the buffer it reads
     * (it reverses token categories in place), and the shared bytes are
     * read by every part's worker.
     */
    const bytes = Buffer.alloc(raw.byteLength);
    bytes.set(new Uint8Array(raw));
    /*
     * Parse what Block.fromBuffer parses (header, count, transactions with
     * Transaction#fromBufferReader), but only this part's transactions: the
     * ones before it are skipped by their serialized length.
     */
    const reader = new bitcore.encoding.BufferReader(bytes);
    const blockHeader = bitcore.BlockHeader.fromBufferReader(reader);
    const transactionCount = reader.readVarintNum();
    const end =
      count === undefined
        ? transactionCount
        : Math.min(first + count, transactionCount);
    let offset = reader.pos;
    for (let index = 0; index < first; index += 1) {
      offset = transactionEnd(bytes, offset);
    }
    reader.pos = offset;
    const transactions: ReturnType<
      typeof bitcoreTransactionToChaingraphTransaction
    >[] = [];
    for (let index = first; index < end; index += 1) {
      transactions.push(
        bitcoreTransactionToChaingraphTransaction(
          new bitcore.Transaction().fromBufferReader(reader)
        )
      );
    }
    /*
     * The block's size as bitcore re-serializes it (every length a canonical
     * CompactSize, as a node relays): the end of its last transaction.
     */
    const sizeBytes = (() => {
      if (!header) return 0;
      let blockEnd = reader.pos;
      for (let index = end; index < transactionCount; index += 1) {
        blockEnd = transactionEnd(bytes, blockEnd);
      }
      return blockEnd;
    })();
    const templates = rowTemplates
      ? encodeTransactionRowTemplates(transactions, first)
      : undefined;
    for (
      let start = 0;
      start < transactions.length;
      start += request.sliceTransactions
    ) {
      replies.push({
        reply: {
          id,
          transactions: transactions.slice(
            start,
            start + request.sliceTransactions
          ),
          type: 'slice',
        },
        transfer: [],
      });
    }
    replies.push({
      reply: {
        header: header
          ? bitcoreHeaderToChaingraph(blockHeader, height, sizeBytes)
          : undefined,
        id,
        templates,
        type: 'done',
      },
      transfer: templates === undefined ? [] : templateBuffers(templates),
    });
  } catch (error) {
    replies.length = 0;
    replies.push({
      reply: {
        id,
        message:
          error instanceof Error ? error.stack ?? error.message : String(error),
        type: 'error',
      },
      transfer: [],
    });
  }
  outbox.set(id, replies);
  sendNext(id);
};

port.on('message', (message: BlockWorkerAck | BlockWorkerRequest) => {
  if (message.type === 'ack') {
    sendNext(message.id);
    return;
  }
  convert(message);
});
