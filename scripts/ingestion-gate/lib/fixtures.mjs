/**
 * Deterministic block fixtures for the ingestion gate.
 *
 * Transactions are NOT valid (signatures are random bytes, inputs may spend
 * nonexistent outpoints) – the mock nodes never validate, and the agent only
 * stores what it is given. Shapes are chosen to resemble the dense "max-size
 * block" fixture used for the production-schema baseline (1-input / 2-output P2PKH-like
 * transactions of ~318 bytes, ~100k per 32 MB block), and dependent blocks
 * spend the previous block's outputs so input/output joins see real rows.
 *
 * Only the transaction payload (varint count + transactions) is cached on
 * disk. Headers are built at run time so blocks can carry a fresh timestamp
 * and attach to whatever base chain the scenario created.
 */
import { createCipheriv, createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

/** Bump when the generator output changes, so stale caches are ignored. */
export const fixtureGeneratorVersion = 1;

export const sha256 = (buffer) => createHash('sha256').update(buffer).digest();
export const doubleSha256 = (buffer) => sha256(sha256(buffer));

/** Seeded byte stream (AES-256-CTR over zeros, keyed by sha256(seed)). */
export class DeterministicBytes {
  constructor(seed) {
    this.cipher = createCipheriv(
      'aes-256-ctr',
      sha256(Buffer.from(String(seed))),
      Buffer.alloc(16)
    );
  }
  bytes(length) {
    return this.cipher.update(Buffer.alloc(length));
  }
  uint32() {
    return this.bytes(4).readUInt32LE(0);
  }
}

export const encodeVarInt = (value) => {
  if (value < 0xfd) return Buffer.from([value]);
  if (value <= 0xffff) {
    const buffer = Buffer.alloc(3);
    buffer[0] = 0xfd;
    buffer.writeUInt16LE(value, 1);
    return buffer;
  }
  const buffer = Buffer.alloc(5);
  buffer[0] = 0xfe;
  buffer.writeUInt32LE(value, 1);
  return buffer;
};

/**
 * Encode a transaction. `prevTxHash` is in internal (little-endian) byte order.
 */
export const encodeTransaction = ({ version = 2, inputs, outputs, locktime = 0 }) => {
  const parts = [];
  const versionBuffer = Buffer.alloc(4);
  versionBuffer.writeInt32LE(version, 0);
  parts.push(versionBuffer, encodeVarInt(inputs.length));
  for (const input of inputs) {
    const outpointIndex = Buffer.alloc(4);
    outpointIndex.writeUInt32LE(input.prevIndex, 0);
    const sequence = Buffer.alloc(4);
    sequence.writeUInt32LE(input.sequence ?? 0xffffffff, 0);
    parts.push(
      input.prevTxHash,
      outpointIndex,
      encodeVarInt(input.script.length),
      input.script,
      sequence
    );
  }
  parts.push(encodeVarInt(outputs.length));
  for (const output of outputs) {
    const satoshis = Buffer.alloc(8);
    satoshis.writeBigUInt64LE(BigInt(output.satoshis), 0);
    parts.push(satoshis, encodeVarInt(output.script.length), output.script);
  }
  const locktimeBuffer = Buffer.alloc(4);
  locktimeBuffer.writeUInt32LE(locktime, 0);
  parts.push(locktimeBuffer);
  return Buffer.concat(parts);
};

const p2pkhScript = (hash20) =>
  Buffer.concat([Buffer.from([0x76, 0xa9, 0x14]), hash20, Buffer.from([0x88, 0xac])]);

/** ~199 byte P2SH-style unlocking script: <sig72> <pubkey33> <redeem90>. */
const unlockingScript = (random) =>
  Buffer.concat([
    Buffer.from([0x48]),
    random.bytes(72),
    Buffer.from([0x21]),
    random.bytes(33),
    Buffer.from([0x4c, 0x5a]),
    random.bytes(90),
  ]);

/**
 * Generate a block's transaction payload.
 * @param seed - PRNG seed (string)
 * @param transactionCount - non-coinbase transactions
 * @param spendFromHashes - optional array of previous-block tx hashes (internal
 * byte order); tx i spends output 0 of spendFromHashes[i] when present.
 * @param mempoolTransactionCount - the first N non-coinbase transactions are
 * also returned as raw buffers so scenarios can broadcast them as mempool txs.
 */
export const generateTransactionPayload = ({
  seed,
  transactionCount,
  spendFromHashes = [],
  outputsPerTransaction = 2,
  mempoolTransactionCount = 0,
}) => {
  const random = new DeterministicBytes(seed);
  const coinbase = encodeTransaction({
    inputs: [
      {
        prevIndex: 0xffffffff,
        prevTxHash: Buffer.alloc(32),
        script: Buffer.concat([Buffer.from('ingestion-gate:'), Buffer.from(String(seed)), random.bytes(8)]),
      },
    ],
    outputs: [{ satoshis: 625_000_000, script: p2pkhScript(random.bytes(20)) }],
  });
  const transactions = [coinbase];
  const transactionHashes = [doubleSha256(coinbase)];
  for (let index = 0; index < transactionCount; index += 1) {
    const spentHash = spendFromHashes[index];
    const transaction = encodeTransaction({
      inputs: [
        {
          prevIndex: spentHash === undefined ? random.uint32() % 4 : 0,
          prevTxHash: spentHash ?? random.bytes(32),
          script: unlockingScript(random),
        },
      ],
      outputs: Array.from({ length: outputsPerTransaction }, () => ({
        satoshis: 546 + (random.uint32() % 100_000_000),
        script: p2pkhScript(random.bytes(20)),
      })),
    });
    transactions.push(transaction);
    transactionHashes.push(doubleSha256(transaction));
  }
  const payload = Buffer.concat([encodeVarInt(transactions.length), ...transactions]);
  return {
    mempoolTransactions: transactions.slice(1, 1 + mempoolTransactionCount),
    payload,
    stats: {
      inputs: transactionCount + 1,
      outputs: transactionCount * outputsPerTransaction + 1,
      transactions: transactionCount + 1,
    },
    transactionHashes,
  };
};

/**
 * Generate (or load from cache) a sequence of dependent block payloads: block
 * k+1 spends output 0 of each transaction in block k.
 */
export const loadOrGenerateBlockSequence = ({
  cacheDirectory,
  name,
  seed,
  blockCount,
  transactionsPerBlock,
  log = () => {},
}) => {
  const cacheKey = `${name}-v${fixtureGeneratorVersion}-seed${seed}-${blockCount}x${transactionsPerBlock}`;
  const cachePath = join(cacheDirectory, `${cacheKey}.bin`);
  const metaPath = join(cacheDirectory, `${cacheKey}.json`);
  if (existsSync(cachePath) && existsSync(metaPath)) {
    const meta = JSON.parse(readFileSync(metaPath, 'utf8'));
    const all = readFileSync(cachePath);
    let offset = 0;
    const blocks = meta.blocks.map((blockMeta) => {
      const payload = all.subarray(offset, offset + blockMeta.payloadBytes);
      offset += blockMeta.payloadBytes;
      return { payload, stats: blockMeta.stats };
    });
    log(`fixture ${cacheKey}: loaded from cache (${(all.length / 1e6).toFixed(2)} MB)`);
    return blocks;
  }
  const started = Date.now();
  const blocks = [];
  let previousHashes = [];
  for (let blockIndex = 0; blockIndex < blockCount; blockIndex += 1) {
    const generated = generateTransactionPayload({
      seed: `${seed}:${name}:${blockIndex}`,
      spendFromHashes: previousHashes.slice(1),
      transactionCount: transactionsPerBlock,
    });
    previousHashes = generated.transactionHashes;
    blocks.push({ payload: generated.payload, stats: generated.stats });
  }
  mkdirSync(cacheDirectory, { recursive: true });
  writeFileSync(cachePath, Buffer.concat(blocks.map((block) => block.payload)));
  writeFileSync(
    metaPath,
    JSON.stringify({
      blocks: blocks.map((block) => ({ payloadBytes: block.payload.length, stats: block.stats })),
      cacheKey,
      generatorVersion: fixtureGeneratorVersion,
    })
  );
  log(
    `fixture ${cacheKey}: generated in ${((Date.now() - started) / 1000).toFixed(1)}s and cached at ${cachePath}`
  );
  return blocks;
};

/**
 * Build an 80-byte header + payload. `previousHash` is big-endian hex (as
 * displayed); returns the raw block buffer and the header buffer.
 */
export const assembleBlock = ({ payload, previousHash, time, nonce }) => {
  const header = Buffer.alloc(80);
  header.writeInt32LE(0x20000000, 0);
  Buffer.from(previousHash, 'hex').reverse().copy(header, 4);
  sha256(payload).copy(header, 36); // pseudo merkle root – never validated
  header.writeUInt32LE(time, 68);
  header.writeUInt32LE(0x207fffff, 72);
  header.writeUInt32LE(nonce >>> 0, 76);
  return { header, raw: Buffer.concat([header, payload]) };
};
