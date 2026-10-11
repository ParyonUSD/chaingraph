/* eslint-disable functional/no-let, functional/no-loop-statement, functional/no-throw-statement, functional/no-mixed-type, @typescript-eslint/member-ordering, @typescript-eslint/parameter-properties, max-classes-per-file, class-methods-use-this, max-params, complexity */
/**
 * A pool of block workers (`worker_threads`): block conversion (bitcore →
 * Chaingraph types) and the RowBinary encoding of a block's node-agnostic
 * rows (`row-templates.ts`) run off the agent's JS thread
 * (docs/clickhouse-port/pre-phase2-concurrency-mempool.md §1).
 *
 * The worker only computes: it returns the converted block (structured
 * clone, in slices of transactions) and the template bytes (transferred
 * ArrayBuffers). Everything that orders or commits stays on the main thread:
 * the agent still buffers blocks in arrival order and the store owns every
 * commit. `CHAINGRAPH_ENCODE_WORKERS=0` turns the pool off.
 */
import { Worker } from 'node:worker_threads';

import type { BlockRowTemplates } from '../store/clickhouse/row-templates.js';
import { blockRowTemplates } from '../store/clickhouse/row-templates.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

import type {
  BlockWorkerAck,
  BlockWorkerReply,
  BlockWorkerRequest,
} from './block-worker-protocol.js';

/** One part of a conversion (a range of the block's transactions). */
interface ConvertedPart {
  header: Omit<ChaingraphBlock, 'transactions'> | undefined;
  templates: BlockRowTemplates | undefined;
  transactions: ChaingraphTransaction[];
}

interface PendingConversion {
  resolve: (part: ConvertedPart) => void;
  reject: (error: Error) => void;
  transactions: ChaingraphTransaction[];
}

interface PoolMember {
  worker: Worker;
  pending: Map<number, PendingConversion>;
}

export class BlockWorkerError extends Error {}

const defaultSliceTransactions = 5_000;

/**
 * A block of at least twice this many transactions is split into parts of
 * about this size, one per worker (at most one part per worker).
 */
const defaultPartTransactions = 12_500;
const minimumPartsToSplit = 2;

/** The block's transaction count (the CompactSize after the 80-byte header). */
export const rawTransactionCount = (raw: Uint8Array): number | undefined => {
  /* eslint-disable @typescript-eslint/no-magic-numbers */
  const at = 80;
  const first = raw[at];
  if (first === undefined) return undefined;
  if (first < 0xfd) return first;
  const length = first === 0xfd ? 2 : first === 0xfe ? 4 : 8;
  if (raw.length < at + 1 + length) return undefined;
  let value = 0;
  for (let index = length - 1; index >= 0; index -= 1) {
    value = value * 256 + raw[at + 1 + index]!;
  }
  return value;
  /* eslint-enable @typescript-eslint/no-magic-numbers */
};

export class BlockWorkerPool {
  private readonly members: PoolMember[] = [];

  private nextId = 0;

  private closed = false;

  /** Conversions done by workers (tests and logs). */
  readonly stats = { blocks: 0, failures: 0, parts: 0, transactions: 0 };

  constructor(
    private readonly options: {
      size: number;
      /** Encode row templates (only the ClickHouse store uses them). */
      rowTemplates: boolean;
      sliceTransactions?: number;
      partTransactions?: number;
      onWorkerError?: (error: Error) => void;
    }
  ) {
    for (let index = 0; index < options.size; index += 1) {
      this.members.push(this.spawn());
    }
  }

  get size() {
    return this.members.length;
  }

  private spawn(): PoolMember {
    const worker = new Worker(new URL('./block-worker.js', import.meta.url));
    worker.unref();
    const member: PoolMember = { pending: new Map(), worker };
    worker.on('message', (reply: BlockWorkerReply) => {
      this.onReply(member, reply);
    });
    const fail = (error: Error) => {
      this.stats.failures += 1;
      member.pending.forEach((conversion) => {
        conversion.reject(error);
      });
      member.pending.clear();
      const index = this.members.indexOf(member);
      if (index !== -1 && !this.closed) {
        this.members[index] = this.spawn();
      }
      this.options.onWorkerError?.(error);
    };
    worker.on('error', (error) => {
      fail(new BlockWorkerError(`Block worker failed: ${error.message}`));
    });
    worker.on('exit', (code) => {
      if (this.closed) return;
      fail(new BlockWorkerError(`Block worker exited (code ${code}).`));
    });
    return member;
  }

  private onReply(member: PoolMember, reply: BlockWorkerReply) {
    const conversion = member.pending.get(reply.id);
    if (conversion === undefined) return;
    if (reply.type === 'slice') {
      reply.transactions.forEach((transaction) => {
        conversion.transactions.push(transaction);
      });
      const ack: BlockWorkerAck = { id: reply.id, type: 'ack' };
      member.worker.postMessage(ack);
      return;
    }
    member.pending.delete(reply.id);
    if (reply.type === 'error') {
      conversion.reject(new BlockWorkerError(reply.message));
      return;
    }
    conversion.resolve({
      header: reply.header,
      templates: reply.templates,
      transactions: conversion.transactions,
    });
  }

  private async convertPart(
    member: PoolMember,
    shared: SharedArrayBuffer,
    height: number,
    first: number,
    count: number | undefined
  ): Promise<ConvertedPart> {
    const id = this.nextId;
    this.nextId += 1;
    const request: BlockWorkerRequest = {
      count,
      first,
      header: first === 0,
      height,
      id,
      raw: shared,
      rowTemplates: this.options.rowTemplates,
      sliceTransactions:
        this.options.sliceTransactions ?? defaultSliceTransactions,
      type: 'convert',
    };
    return new Promise<ConvertedPart>((resolve, reject) => {
      member.pending.set(id, { reject, resolve, transactions: [] });
      member.worker.postMessage(request);
    });
  }

  /**
   * Convert the raw block `raw` at `height` (the bytes are copied once into
   * shared memory; `raw` is not changed). A large block is split into parts
   * converted by several workers at once; conversions of different blocks
   * run concurrently on the least busy workers, and the caller orders the
   * results.
   */
  async convert(raw: Uint8Array, height: number): Promise<ChaingraphBlock> {
    if (this.closed) {
      throw new BlockWorkerError('The block worker pool is closed.');
    }
    const shared = new SharedArrayBuffer(raw.byteLength);
    new Uint8Array(shared).set(raw);
    const partSize = this.options.partTransactions ?? defaultPartTransactions;
    const transactionCount = rawTransactionCount(raw) ?? 0;
    const partCount =
      transactionCount >= minimumPartsToSplit * partSize
        ? Math.min(this.members.length, Math.ceil(transactionCount / partSize))
        : 1;
    const members = [...this.members]
      .sort((a, b) => a.pending.size - b.pending.size)
      .slice(0, partCount);
    const perPart = Math.ceil(transactionCount / partCount);
    const parts = await Promise.all(
      members.map(async (member, index) =>
        partCount === 1
          ? this.convertPart(member, shared, height, 0, undefined)
          : this.convertPart(member, shared, height, index * perPart, perPart)
      )
    );
    const transactions: ChaingraphTransaction[] = [];
    parts.forEach((part) => {
      part.transactions.forEach((transaction) => {
        transactions.push(transaction);
      });
    });
    const block: ChaingraphBlock = { ...parts[0]!.header!, transactions };
    if (parts.every((part) => part.templates !== undefined)) {
      let first = 0;
      blockRowTemplates.set(block, {
        parts: parts.map((part) => {
          const entry = {
            count: part.transactions.length,
            first,
            templates: part.templates!,
          };
          first += part.transactions.length;
          return entry;
        }),
      });
    }
    this.stats.blocks += 1;
    this.stats.parts += parts.length;
    this.stats.transactions += transactions.length;
    return block;
  }

  async close() {
    this.closed = true;
    await Promise.all(
      this.members.map(async (member) => {
        member.pending.forEach((conversion) => {
          conversion.reject(new BlockWorkerError('Block worker pool closed.'));
        });
        member.pending.clear();
        await member.worker.terminate();
      })
    );
  }
}
