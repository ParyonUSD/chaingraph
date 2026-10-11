/* eslint-disable func-names, functional/no-let, import/no-commonjs, @typescript-eslint/no-unsafe-assignment, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-explicit-any */
/**
 * Keep the raw payload of P2P `block` messages and parse the block only if
 * it is asked for. bitcore-p2p-cash parses every block message on arrival
 * (`Block.fromBuffer`, on the agent's one JS thread); with block workers the
 * agent hands the raw bytes to a worker instead and parses only the 80-byte
 * header here (docs/clickhouse-port/pre-phase2-concurrency-mempool.md §1).
 */
import { createRequire } from 'node:module';

import type {
  BitcoreBlock,
  BitcoreBlockHeader,
  BlockMessage,
} from '@chaingraph/bitcore-p2p-cash';
import bitcoreP2pCash from '@chaingraph/bitcore-p2p-cash';

const require = createRequire(import.meta.url);

/** The module bitcore-p2p-cash's message builder uses for `block`. */
const blockMessageModule =
  require('@chaingraph/bitcore-p2p-cash/lib/messages/commands/block.js') as {
    prototype: any;
  };

const blockHeaderBytes = 80;

let installed = false;

/**
 * From now on, `block` messages keep their payload (`rawPayloadOf`) and
 * parse it lazily on the first read of `message.block`. Idempotent.
 */
export const keepRawBlockPayloads = () => {
  if (installed) return;
  installed = true;
  const { prototype } = blockMessageModule;
  prototype.setPayload = function (payload: Buffer) {
    this.rawPayload = payload;
    this.parsedBlock = undefined;
  };
  Object.defineProperty(prototype, 'block', {
    configurable: true,
    get(this: any) {
      if (this.parsedBlock === undefined && this.rawPayload !== undefined) {
        this.parsedBlock = this.Block.fromBuffer(this.rawPayload);
      }
      return this.parsedBlock;
    },
    set(this: any, value: BitcoreBlock | undefined) {
      this.parsedBlock = value;
    },
  });
};

/** The raw payload of a received `block` message, if it was kept. */
export const rawPayloadOf = (message: BlockMessage): Buffer | undefined =>
  (message as unknown as { rawPayload?: Buffer }).rawPayload;

/** The header of a raw block (its first 80 bytes). */
export const rawBlockHeader = (raw: Buffer): BitcoreBlockHeader =>
  bitcoreP2pCash.internalBitcore.BlockHeader.fromBuffer(
    raw.subarray(0, blockHeaderBytes)
  );

/** Parse a raw block on this thread (the fallback when a worker fails). */
export const parseRawBlock = (raw: Buffer): BitcoreBlock =>
  bitcoreP2pCash.internalBitcore.Block.fromBuffer(raw);
