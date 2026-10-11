/** Messages between `BlockWorkerPool` and `block-worker.ts`. */
import type { BlockRowTemplates } from '../store/clickhouse/row-templates.js';
import type {
  ChaingraphBlock,
  ChaingraphTransaction,
} from '../types/chaingraph.js';

/**
 * Convert transactions `[first, first + count)` of a raw block (all of them
 * when `count` is undefined). Parts of one block go to several workers.
 */
export interface BlockWorkerRequest {
  type: 'convert';
  id: number;
  /** The raw block, shared by every part (each worker parses a copy). */
  raw: SharedArrayBuffer;
  height: number;
  first: number;
  count: number | undefined;
  /** Whether this part returns the block's other fields (the first part). */
  header: boolean;
  rowTemplates: boolean;
  sliceTransactions: number;
}

/**
 * The main thread took a slice: the worker sends the next one. One slice per
 * acknowledgement, so the main thread deserializes a large block over
 * several event-loop turns (a port's queued messages are otherwise
 * delivered in one turn).
 */
export interface BlockWorkerAck {
  type: 'ack';
  id: number;
}

export type BlockWorkerReply =
  | {
      id: number;
      type: 'done';
      header: Omit<ChaingraphBlock, 'transactions'> | undefined;
      templates: BlockRowTemplates | undefined;
    }
  | { id: number; type: 'error'; message: string }
  | { id: number; type: 'slice'; transactions: ChaingraphTransaction[] };
