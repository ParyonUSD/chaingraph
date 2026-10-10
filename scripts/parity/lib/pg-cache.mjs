// cspell:ignore clickhouse jsonl
/**
 * Postgres reference cache for the parity harness (`--pg-cache <dir>`).
 *
 * The Postgres side of a run (per node/table/chunk digests, including the F1g
 * `utxo` chunks, plus the sorted `s`+timestamp rows of the timestamp-tolerance
 * pass) is written once and read back by later runs, which then send no
 * Postgres query at all. A cache is only reused when the parameters that shape
 * the Postgres side match exactly (see `cacheParameters`).
 *
 * Layout: <dir>/manifest.json (parameters, Postgres nodes/tips/max height,
 * digests, row-file index; written last, so a crashed run leaves no usable
 * cache) and <dir>/rows/<md5 of key>.jsonl.gz (one JSON array per row, in
 * Postgres `ORDER BY s COLLATE "C"` order).
 */
import { createHash } from 'node:crypto';
import { once } from 'node:events';
import {
  createReadStream,
  createWriteStream,
  existsSync,
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { join } from 'node:path';
import { createInterface } from 'node:readline';
import { pipeline } from 'node:stream/promises';
import { createGunzip, createGzip } from 'node:zlib';

export const cacheFormatVersion = 1;
const manifestName = 'manifest.json';

/** Everything that changes which Postgres chunks exist or what their rows are. */
export const cacheParameters = (input) => ({
  atHeight: input.atHeight ?? null,
  chunkBlocks: input.chunkBlocks,
  every: input.every ?? null,
  formatVersion: cacheFormatVersion,
  from: input.from ?? null,
  hash: input.hash,
  hashChunks: input.hashChunks,
  includeMempool: input.includeMempool,
  nodes: input.nodes,
  tables: input.tables,
  timestamps: input.timestamps,
  to: input.to ?? null,
  utxoChunks: input.utxoChunks,
});

/** Names of parameters whose values differ (JSON comparison). */
export const parameterDifferences = (cached, wanted) =>
  [...new Set([...Object.keys(cached ?? {}), ...Object.keys(wanted)])]
    .filter(
      (name) =>
        JSON.stringify(cached?.[name] ?? null) !==
        JSON.stringify(wanted[name] ?? null)
    )
    .sort();

export const taskKey = (task) =>
  `${task.node?.name ?? '*'}\t${task.name}\t${task.chunk.label}`;

/** Digest of an empty chunk, as both engines report it. */
export const emptyDigest = (hash) =>
  hash === 'ordered'
    ? createHash('md5').update('').digest('hex')
    : '0'.repeat(32);

export const loadPgCache = (directory) => {
  const path = join(directory, manifestName);
  if (!existsSync(path)) return undefined;
  return JSON.parse(readFileSync(path, 'utf8'));
};

export async function* readCachedRows(directory, file) {
  const lines = createInterface({
    crlfDelay: Infinity,
    input: createReadStream(join(directory, file)).pipe(createGunzip()),
  });
  for await (const line of lines) if (line.length > 0) yield JSON.parse(line);
}

export class PgCacheWriter {
  constructor(directory) {
    this.directory = directory;
    this.digests = {};
    this.rows = {};
    mkdirSync(directory, { recursive: true });
    rmSync(join(directory, manifestName), { force: true });
    rmSync(join(directory, 'rows'), { force: true, recursive: true });
    mkdirSync(join(directory, 'rows'), { recursive: true });
  }

  setDigest(key, count, md5) {
    this.digests[key] = { count, md5 };
  }

  /** Pass rows through unchanged while writing them to the cache. */
  async *tee(key, rows) {
    const file = `rows/${createHash('md5').update(key).digest('hex')}.jsonl.gz`;
    const gzip = createGzip();
    const finished = pipeline(
      gzip,
      createWriteStream(join(this.directory, file))
    );
    finished.catch(() => undefined); // surfaced by `await finished` below
    for await (const row of rows) {
      if (!gzip.write(`${JSON.stringify(row)}\n`)) await once(gzip, 'drain');
      yield row;
    }
    gzip.end();
    await finished;
    this.rows[key] = file;
  }

  async store(key, rows) {
    // eslint-disable-next-line no-unused-vars
    for await (const _row of this.tee(key, rows));
  }

  /** Write the manifest (atomically); the cache is usable from here on. */
  finish(manifest) {
    const path = join(this.directory, manifestName);
    writeFileSync(
      `${path}.tmp`,
      `${JSON.stringify(
        { ...manifest, digests: this.digests, rows: this.rows },
        null,
        1
      )}\n`
    );
    renameSync(`${path}.tmp`, path);
  }
}
