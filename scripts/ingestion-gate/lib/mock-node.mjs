/**
 * Minimal mock BCH node built on bitcore-p2p-cash (same approach as
 * src/e2e/e2e.spec.ts): serves headers/blocks/transactions from an in-memory
 * chain and lets scenarios announce new blocks or force reorgs. Blocks are
 * kept as raw buffers so 32 MB blocks are never re-serialized by bitcore.
 */
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const bitcoreP2pCash = require('@chaingraph/bitcore-p2p-cash');

const { Pool, internalBitcore } = bitcoreP2pCash;
const { Block, BlockHeader, Networks, Transaction } = internalBitcore;

const registeredNetworks = new Set();
const msgBlock = 2;
const msgTx = 1;
const headersPerMessage = 2000;

export const genesisBlockRaw =
  '0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000';
export const testnetGenesisBlockRaw =
  '0100000043497fd7f826957108f4a30fd9cec3aeba79972084e90ead01ea330900000000bac8b0fa927c0ac8234287e33c5f74d38d354820e24756ad709d7038fc5f31f020e7494dffff001d03e4b6720101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0e0420e7494d017f062f503253482fffffffff0100f2052a010000002321021aeaf2f8638a129a3156fbe7e5ef635226b0bafd495ff03afe2c843d7e3a4b51ac00000000';

/** A block wrapper that hands bitcore pre-serialized bytes. */
export const makeBlock = ({ header, raw, stats }) => {
  const bitcoreHeader = BlockHeader.fromBuffer(header);
  const block = Object.create(Block.prototype);
  block.header = bitcoreHeader;
  block.toBuffer = () => raw;
  return { bitcoreBlock: block, hash: bitcoreHeader.hash, header: bitcoreHeader, raw, stats };
};

export const genesisFromRaw = (rawHex) => {
  const raw = Buffer.from(rawHex, 'hex');
  return makeBlock({ header: raw.subarray(0, 80), raw, stats: { inputs: 1, outputs: 1, transactions: 1 } });
};

export class MockNode {
  /**
   * @param name - node name used in CHAINGRAPH_TRUSTED_NODES
   * @param magicHex - 4-byte network magic (hex)
   * @param port - listen port
   * @param genesis - block from `genesisFromRaw`
   */
  constructor({ name, magicHex, port, genesis, userAgent }) {
    this.name = name;
    this.magicHex = magicHex;
    this.port = port;
    this.genesis = genesis;
    this.chain = [genesis];
    this.blocksByHash = new Map([[genesis.hash, genesis]]);
    this.mempool = new Map();
    this.peer = undefined;
    this.readyWaiters = [];
    const networkName = `ingestion-gate-${name}-${port}`;
    if (!registeredNetworks.has(networkName)) {
      Networks.add({ name: networkName, networkMagic: parseInt(magicHex, 16), port });
      registeredNetworks.add(networkName);
    }
    this.pool = new Pool({
      dnsSeed: false,
      listenAddr: false,
      network: networkName,
      subversion: userAgent ?? `/ingestion-gate-${name}:0.0.0/`,
      version: 70016,
    });
    this.pool.on('peerready', (peer) => {
      if (peer.subversion.includes('tx-broadcast')) return;
      this.peer = peer;
      this.readyWaiters.splice(0).forEach((resolve) => resolve());
    });
    this.pool.on('peergetheaders', (peer, message) => {
      const starts = message.starts.map((hash) => Buffer.from(hash).reverse().toString('hex'));
      peer.sendMessage(new peer.messages.Headers(this.selectHeaders(starts)));
    });
    this.pool.on('peergetdata', (peer, message) => {
      message.inventory.forEach((inventory) => {
        const hash = Buffer.from(inventory.hash).reverse().toString('hex');
        if (inventory.type === msgBlock) {
          const block = this.blocksByHash.get(hash);
          if (block !== undefined) peer.sendMessage(new peer.messages.Block(block.bitcoreBlock));
        } else if (inventory.type === msgTx) {
          const raw = this.mempool.get(hash);
          if (raw !== undefined) peer.sendMessage(new peer.messages.Transaction(new Transaction(raw)));
        }
      });
    });
  }

  listen() {
    this.pool.listen();
  }

  async waitForPeer() {
    if (this.peer !== undefined) return;
    await new Promise((resolve) => this.readyWaiters.push(resolve));
  }

  close() {
    try {
      this.pool.disconnect();
      this.pool.server?.close();
    } catch {
      // ignore
    }
  }

  trustedNodeEntry() {
    return `${this.name}:127.0.0.1:${this.port}:${this.magicHex}`;
  }

  tip() {
    return this.chain[this.chain.length - 1];
  }

  /** Respond to a block locator like BCHN: headers after the first known start. */
  selectHeaders(starts) {
    let startIndex = 0;
    for (const start of starts) {
      const index = this.chain.findIndex((block) => block.hash === start);
      if (index !== -1) {
        startIndex = index + 1;
        break;
      }
    }
    return this.chain.slice(startIndex, startIndex + headersPerMessage).map((block) => block.header);
  }

  /** Append blocks to the chain without announcing them. */
  extend(blocks) {
    blocks.forEach((block) => {
      this.chain.push(block);
      this.blocksByHash.set(block.hash, block);
    });
  }

  /** Append and announce via a `headers` message (the normal live path). */
  announceViaHeaders(blocks) {
    this.extend(blocks);
    this.peer.sendMessage(new this.peer.messages.Headers(blocks.map((block) => block.header)));
  }

  /**
   * Replace the chain above `forkHeight` and announce the new tip via `inv`
   * (the agent then re-syncs headers with a locator) – used for deep reorgs
   * and long catch-ups, where real nodes only send an inventory.
   */
  reorgTo(forkHeight, newBlocks) {
    this.chain.splice(forkHeight + 1);
    this.extend(newBlocks);
    this.peer.sendMessage(this.peer.messages.Inventory.forBlock(this.tip().hash));
  }

  appendViaInventory(blocks) {
    this.reorgTo(this.chain.length - 1, blocks);
  }

  /** Push a raw transaction (Buffer) to the agent as an unsolicited `tx`. */
  sendTransaction(raw) {
    const transaction = new Transaction(raw.toString('hex'));
    this.mempool.set(transaction.hash, raw.toString('hex'));
    this.peer.sendMessage(new this.peer.messages.Transaction(transaction));
    return transaction.hash;
  }
}
