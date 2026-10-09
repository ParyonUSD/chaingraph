#!/usr/bin/env node
// cspell:ignore bytewise clickhouse nullness Milli unhex prefilter denormalised selftest orged unnest
/**
 * Chaingraph Postgres ↔ ClickHouse parity harness (plan §5.2, Gate G1 item 1).
 * See docs/clickhouse-port/parity-harness.md.
 *
 *   node scripts/parity/compare.mjs --pg <postgres url> --ch <clickhouse http url> [--ch-db cg]
 *     --nodes <name>[,<name>…] --out <dir>
 *     [--at-height H] [--every N --from H0 --to H1] [--tables t1,t2,…] [--include-mempool]
 *     [--hash sum|ordered] [--timestamps tolerance|exact|exclude] [--ts-tolerance-ms 120000]
 *     [--chunk-blocks 10000] [--hash-chunks 16] [--utxo-chunks 16] [--parallel 4] [--diff]
 *
 * ClickHouse credentials: CH_USER / CH_PASSWORD (environment only).
 * A node is named as in both stores' `node.name`; a bare number that is not a
 * node name is taken as the Postgres `node.internal_id`.
 *
 * Writes <out>/parity.tsv (node, table, chunk, pg_count, ch_count, pg_md5,
 * ch_md5, match), <out>/summary.json and, with --diff, <out>/diff.txt.
 * Exit 0 = every row matches, 1 = at least one mismatch, 2 = error.
 */
import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { parseArgs } from 'node:util';

import { defaultTables, digestSql, rowsSql, tables } from './lib/canonical.mjs';
import { addDigests, digestFromSums } from './lib/digest.mjs';
import {
  ClickHouseHttp,
  PostgresSnapshotPool,
  readClickHouseSnapshot,
} from './lib/engines.mjs';

const diffRowLimit = 20;

const { values: options } = parseArgs({
  options: {
    ch: { type: 'string' },
    'ch-db': { default: 'cg', type: 'string' },
    'chunk-blocks': { default: '10000', type: 'string' },
    diff: { default: false, type: 'boolean' },
    every: { type: 'string' },
    from: { type: 'string' },
    hash: { default: 'sum', type: 'string' },
    'hash-chunks': { default: '16', type: 'string' },
    'at-height': { type: 'string' },
    'include-mempool': { default: false, type: 'boolean' },
    nodes: { type: 'string' },
    out: { type: 'string' },
    parallel: { default: '4', type: 'string' },
    pg: { type: 'string' },
    tables: { type: 'string' },
    timestamps: { default: 'tolerance', type: 'string' },
    to: { type: 'string' },
    'ts-tolerance-ms': { default: '120000', type: 'string' },
    'utxo-chunks': { default: '16', type: 'string' },
    quiet: { default: false, type: 'boolean' },
  },
});

const fail = (message) => {
  console.error(`compare: ${message}`);
  process.exit(2);
};

const integerOption = (name, minimum = 0) => {
  const raw = options[name];
  if (raw === undefined) return undefined;
  if (!/^\d+$/.test(raw) || Number(raw) < minimum)
    fail(`--${name} must be an integer >= ${minimum}`);
  return Number(raw);
};

/** Hash-prefix chunks: `count` must be 16^k (k = 1..4); returns [{ label, lo, hi? }]. */
const hashChunks = (count, labelPrefix) => {
  const nibbles = Math.round(Math.log(count) / Math.log(16));
  if (16 ** nibbles !== count || nibbles < 1 || nibbles > 4)
    fail(`chunk count ${count} must be 16, 256, 4096 or 65536`);
  return Array.from({ length: count }, (_, index) => {
    const prefix = index.toString(16).padStart(nibbles, '0');
    const next =
      index + 1 < count
        ? (index + 1).toString(16).padStart(nibbles, '0')
        : undefined;
    return {
      hi: next === undefined ? undefined : next.padEnd(64, '0'),
      kind: 'hash',
      label: `${labelPrefix}:${prefix}`,
      lo: prefix.padEnd(64, '0'),
    };
  });
};

const heightChunks = (from, to, size, kind) => {
  const chunks = [];
  for (let lo = from; lo <= to; lo += size) {
    const hi = Math.min(lo + size - 1, to);
    chunks.push({
      hi,
      kind,
      label: `${kind === 'window' ? 'w' : 'b'}:${lo}-${hi}`,
      lo,
    });
  }
  return chunks;
};

const runPool = async (items, size, work) => {
  let next = 0;
  const workers = Array.from(
    { length: Math.min(size, items.length) },
    async () => {
      while (next < items.length) {
        const index = next;
        next += 1;
        await work(items[index], index);
      }
    }
  );
  await Promise.all(workers);
};

/** Merge two sorted [s, …] streams; collect rows only on one side (up to `limit` each) and timestamp pairs. */
const mergeStreams = async (pgRows, chRows, { limit, onPair }) => {
  const onlyPg = [];
  const onlyCh = [];
  const pgIterator = pgRows[Symbol.asyncIterator]();
  const chIterator = chRows[Symbol.asyncIterator]();
  let pgNext = await pgIterator.next();
  let chNext = await chIterator.next();
  let onlyPgCount = 0;
  let onlyChCount = 0;
  const compare = (left, right) => (left < right ? -1 : left > right ? 1 : 0);
  while (!pgNext.done || !chNext.done) {
    // canonical strings are ASCII, so JS string order is the engines' bytewise order
    const order = pgNext.done
      ? 1
      : chNext.done
      ? -1
      : compare(pgNext.value[0], chNext.value[0]);
    if (order === 0) {
      onPair?.(pgNext.value, chNext.value);
      pgNext = await pgIterator.next();
      chNext = await chIterator.next();
    } else if (order < 0) {
      onlyPgCount += 1;
      if (onlyPg.length < limit) onlyPg.push(pgNext.value[0]);
      pgNext = await pgIterator.next();
    } else {
      onlyChCount += 1;
      if (onlyCh.length < limit) onlyCh.push(chNext.value[0]);
      chNext = await chIterator.next();
    }
  }
  return { onlyCh, onlyChCount, onlyPg, onlyPgCount };
};

const main = async () => {
  for (const required of ['pg', 'ch', 'nodes', 'out'])
    if (options[required] === undefined) fail(`--${required} is required`);
  if (!['sum', 'ordered'].includes(options.hash))
    fail('--hash must be sum or ordered');
  if (!['tolerance', 'exact', 'exclude'].includes(options.timestamps))
    fail('--timestamps must be tolerance, exact or exclude');
  const atHeight = integerOption('at-height');
  const every = integerOption('every', 1);
  const windowFrom = integerOption('from');
  const windowTo = integerOption('to');
  if (
    every !== undefined &&
    (windowFrom === undefined || windowTo === undefined)
  )
    fail('--every needs --from and --to');
  if (every !== undefined && atHeight !== undefined)
    fail('--every and --at-height are exclusive (windows are bounded by --to)');
  const chunkBlocks = integerOption('chunk-blocks', 1);
  const hashChunkCount = integerOption('hash-chunks', 16);
  const utxoChunkCount = integerOption('utxo-chunks', 16);
  const parallel = integerOption('parallel', 1);
  const toleranceMs = integerOption('ts-tolerance-ms');
  const tableNames =
    options.tables === undefined
      ? defaultTables
      : options.tables.split(',').map((name) => name.trim());
  for (const name of tableNames)
    if (tables[name] === undefined)
      fail(`unknown table ${name} (known: ${Object.keys(tables).join(', ')})`);
  const log = options.quiet
    ? () => undefined
    : (message) => console.error(message);

  mkdirSync(options.out, { recursive: true });
  const postgres = new PostgresSnapshotPool(options.pg, parallel);
  const clickhouse = new ClickHouseHttp(options.ch, options['ch-db']);
  await postgres.open();
  const started = Date.now();
  try {
    // ---- nodes (matched by name; ids differ between stores)
    const pgNodes = await postgres.query(
      'SELECT internal_id::text AS id, name FROM node'
    );
    const chNodes = await clickhouse.query(
      'SELECT toString(internal_id) AS id, name FROM node_v'
    );
    const nodes = options.nodes.split(',').map((token) => {
      const byName =
        pgNodes.find((node) => node.name === token) ??
        pgNodes.find((node) => /^\d+$/.test(token) && node.id === token);
      if (byName === undefined) fail(`node ${token} not found in Postgres`);
      const chNode = chNodes.find((node) => node.name === byName.name);
      if (chNode === undefined)
        fail(`node ${byName.name} not found in ClickHouse`);
      return { chId: chNode.id, name: byName.name, pgId: byName.id };
    });
    const snapshot = await readClickHouseSnapshot(
      clickhouse,
      nodes.map((node) => node.chId)
    );
    for (const node of nodes) node.visible = snapshot.visible.get(node.chId);
    const ctx = {
      atHeight,
      ch: { tail: snapshot.tail, visible0: snapshot.visible0 },
      historyHeight: every === undefined ? atHeight : windowTo,
      includeMempool: options['include-mempool'],
      nodes,
      timestamps: options.timestamps,
    };

    // ---- heights
    const [{ h: pgMax }] = await postgres.query(
      'SELECT coalesce(max(height), -1)::text AS h FROM block'
    );
    const [{ h: chMax }] = await clickhouse.query(
      `SELECT toString(if(count() = 0, -1, max(height))) AS h FROM block_at(visible0 = ${
        ctx.ch.visible0
      }, tail = [${ctx.ch.tail.join(', ')}])`
    );
    const maxHeight = atHeight ?? Math.max(Number(pgMax), Number(chMax));
    const tipOf = async (node) => {
      const [{ h: pgTip }] = await postgres.query(
        `SELECT coalesce(max(b.height), -1)::text AS h FROM node_block nb JOIN block b ON b.internal_id = nb.block_internal_id WHERE nb.node_internal_id = ${node.pgId}`
      );
      const [{ h: chTip }] = await clickhouse.query(
        `SELECT toString(if(count() = 0, -1, max(height))) AS h FROM node_block_at(node = ${node.chId}, visible = ${node.visible})`
      );
      return { chTip: Number(chTip), pgTip: Number(pgTip) };
    };
    for (const node of nodes) Object.assign(node, await tipOf(node));

    // ---- plan
    const windowMode = every !== undefined;
    const blockChunks = windowMode
      ? heightChunks(windowFrom, windowTo, every, 'window')
      : heightChunks(0, maxHeight, chunkBlocks, 'height');
    const txChunks = windowMode ? blockChunks : hashChunks(hashChunkCount, 'h');
    const tasks = [];
    const skipped = [];
    for (const name of tableNames) {
      const table = tables[name];
      switch (table.level) {
        case 'block':
          for (const chunk of blockChunks)
            tasks.push({ chunk, name, node: undefined });
          break;
        case 'tx':
          for (const chunk of txChunks)
            tasks.push({ chunk, name, node: undefined });
          break;
        case 'node-block':
          for (const node of nodes)
            for (const chunk of blockChunks) tasks.push({ chunk, name, node });
          break;
        case 'node-tx':
          for (const node of nodes) {
            for (const chunk of txChunks) tasks.push({ chunk, name, node });
            if (ctx.includeMempool)
              tasks.push({
                chunk: { kind: 'mempool', label: 'mempool' },
                name,
                node,
              });
          }
          break;
        case 'node-mempool':
          if (!ctx.includeMempool)
            skipped.push(`${name}: mempool excluded (use --include-mempool)`);
          else
            for (const node of nodes)
              tasks.push({
                chunk: { kind: 'mempool', label: 'mempool' },
                name,
                node,
              });
          break;
        case 'node-history':
          for (const node of nodes)
            tasks.push({ chunk: { kind: 'all', label: 'all' }, name, node });
          break;
        case 'utxo':
          for (const node of nodes) {
            if (windowMode) {
              skipped.push(
                `utxo ${node.name}: current state only, not compared per window`
              );
            } else if (
              atHeight !== undefined &&
              (node.pgTip !== atHeight || node.chTip !== atHeight)
            ) {
              skipped.push(
                `utxo ${node.name}: --at-height ${atHeight} but tips are pg ${node.pgTip} / ch ${node.chTip}`
              );
            } else {
              for (const chunk of hashChunks(utxoChunkCount, 'u'))
                tasks.push({ chunk, name, node });
            }
          }
          break;
        default:
          fail(`table ${name} has unknown level ${table.level}`);
      }
    }
    log(
      `compare: ${tasks.length} chunk tasks, ${nodes
        .map(
          (node) =>
            `${node.name} (pg ${node.pgId}/ch ${node.chId}, tips ${node.pgTip}/${node.chTip})`
        )
        .join(', ')}, max height ${maxHeight}, hash ${
        options.hash
      }, timestamps ${options.timestamps}`
    );

    // ---- run
    const results = new Array(tasks.length);
    const tsResults = [];
    const diffs = [];
    let done = 0;
    await runPool(tasks, parallel, async (task, index) => {
      const table = tables[task.name];
      const pgInner = table.pg(ctx, task.chunk, task.node).sql;
      const chInner = table.ch(ctx, task.chunk, task.node).sql;
      const [pgDigest, chDigest] = await Promise.all([
        postgres.query(digestSql.pg(pgInner, options.hash)),
        clickhouse.query(digestSql.ch(chInner, options.hash)),
      ]);
      const toDigest = (row) =>
        options.hash === 'ordered' ? row.m : digestFromSums(row.a, row.b);
      const result = {
        ch_count: Number(chDigest[0].n),
        ch_md5: toDigest(chDigest[0]),
        chunk: task.chunk.label,
        node: task.node?.name ?? '*',
        pg_count: Number(pgDigest[0].n),
        pg_md5: toDigest(pgDigest[0]),
        table: task.name,
      };
      result.match =
        result.pg_count === result.ch_count && result.pg_md5 === result.ch_md5;
      results[index] = result;

      const tsColumns =
        options.timestamps === 'tolerance' ? table.ts ?? [] : [];
      if (tsColumns.length > 0 || (options.diff && !result.match)) {
        let compared = 0;
        let over = 0;
        let maxDiff = 0;
        const { onlyCh, onlyChCount, onlyPg, onlyPgCount } = await mergeStreams(
          postgres.stream(rowsSql.pg(pgInner, tsColumns.length)),
          clickhouse.stream(rowsSql.ch(chInner, tsColumns.length)),
          {
            limit: diffRowLimit,
            onPair: (pgRow, chRow) => {
              for (let column = 1; column <= tsColumns.length; column += 1) {
                if (pgRow[column] === null || chRow[column] === null) continue; // NULL-ness is in s
                compared += 1;
                const difference = Math.abs(
                  Number(pgRow[column]) - Number(chRow[column])
                );
                maxDiff = Math.max(maxDiff, difference);
                if (difference > toleranceMs) over += 1;
              }
            },
          }
        );
        if (tsColumns.length > 0) {
          tsResults.push({
            ch_count: compared,
            ch_md5: `max_ms=${maxDiff};over=${over}`,
            chunk: task.chunk.label,
            match: over === 0,
            node: result.node,
            pg_count: compared,
            pg_md5: `tolerance_ms=${toleranceMs}`,
            table: `${task.name}:${tsColumns.join('+')}`,
          });
        }
        if (options.diff && !result.match) {
          diffs.push({ ...result, onlyCh, onlyChCount, onlyPg, onlyPgCount });
        }
      }
      done += 1;
      if (!result.match)
        log(
          `MISMATCH ${result.node} ${result.table} ${result.chunk}: pg ${result.pg_count} ${result.pg_md5} / ch ${result.ch_count} ${result.ch_md5}`
        );
      if (done % 50 === 0)
        log(
          `compare: ${done}/${tasks.length} chunks (${Math.round(
            (Date.now() - started) / 1000
          )} s)`
        );
    });

    // ---- roll-ups (per node and table, in chunk order) and cumulative window digests
    const rows = [...results];
    const groups = new Map();
    for (const result of results) {
      const key = `${result.node}\t${result.table}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(result);
    }
    for (const group of groups.values()) {
      const rollUp = (side) =>
        createHash('md5')
          .update(
            group
              .map(
                (row) =>
                  `${row.chunk}\t${row[`${side}_count`]}\t${
                    row[`${side}_md5`]
                  }\n`
              )
              .join('')
          )
          .digest('hex');
      const total = {
        ch_count: group.reduce((sum, row) => sum + row.ch_count, 0),
        ch_md5: rollUp('ch'),
        chunk: 'ALL',
        node: group[0].node,
        pg_count: group.reduce((sum, row) => sum + row.pg_count, 0),
        pg_md5: rollUp('pg'),
        table: group[0].table,
      };
      total.match = group.every((row) => row.match);
      if (
        windowMode &&
        options.hash === 'sum' &&
        group[0].chunk.startsWith('w:')
      ) {
        let pgRunning = '0'.repeat(32);
        let chRunning = '0'.repeat(32);
        let pgCount = 0;
        let chCount = 0;
        for (const row of group) {
          pgRunning = addDigests(pgRunning, row.pg_md5);
          chRunning = addDigests(chRunning, row.ch_md5);
          pgCount += row.pg_count;
          chCount += row.ch_count;
          rows.push({
            ch_count: chCount,
            ch_md5: chRunning,
            chunk: `cum:${windowFrom}-${row.chunk.split('-')[1]}`,
            match: pgCount === chCount && pgRunning === chRunning,
            node: row.node,
            pg_count: pgCount,
            pg_md5: pgRunning,
            table: row.table,
          });
        }
      }
      rows.push(total);
    }
    rows.push(...tsResults);

    const header = [
      'node',
      'table',
      'chunk',
      'pg_count',
      'ch_count',
      'pg_md5',
      'ch_md5',
      'match',
    ];
    const tsv = [
      header.join('\t'),
      ...rows.map((row) =>
        [
          ...header.slice(0, 7).map((key) => row[key]),
          row.match ? 'yes' : 'NO',
        ].join('\t')
      ),
    ].join('\n');
    writeFileSync(join(options.out, 'parity.tsv'), `${tsv}\n`);

    const mismatches = rows.filter(
      (row) =>
        !row.match && row.chunk !== 'ALL' && !row.chunk.startsWith('cum:')
    );
    const summary = {
      chunkTasks: tasks.length,
      durationSeconds: Math.round((Date.now() - started) / 100) / 10,
      hash: options.hash,
      maxHeight,
      mismatchedRows: mismatches.length,
      mismatches: mismatches.map(
        (row) => `${row.node}\t${row.table}\t${row.chunk}`
      ),
      mismatchedTables: [
        ...new Set(mismatches.map((row) => `${row.node}\t${row.table}`)),
      ],
      mode: windowMode
        ? `windows of ${every} in [${windowFrom}, ${windowTo}]`
        : atHeight === undefined
        ? 'current'
        : `at height ${atHeight}`,
      nodes,
      skipped,
      snapshot: {
        postgres: 'exported REPEATABLE READ snapshot',
        clickhouse: { tail: ctx.ch.tail, visible0: ctx.ch.visible0 },
      },
      timestamps: options.timestamps,
      toleranceMs,
    };
    writeFileSync(
      join(options.out, 'summary.json'),
      `${JSON.stringify(summary, null, 2)}\n`
    );
    if (options.diff) {
      const text = diffs
        .map(
          (diff) =>
            `== ${diff.node} ${diff.table} ${diff.chunk}: only in postgres ${diff.onlyPgCount}, only in clickhouse ${diff.onlyChCount}\n` +
            diff.onlyPg.map((line) => `- pg ${line}\n`).join('') +
            diff.onlyCh.map((line) => `+ ch ${line}\n`).join('')
        )
        .join('');
      writeFileSync(join(options.out, 'diff.txt'), text);
      if (text.length > 0) log(text.trimEnd());
    }
    for (const note of skipped) log(`skipped: ${note}`);
    log(
      `compare: ${
        mismatches.length === 0
          ? 'ALL MATCH'
          : `${mismatches.length} MISMATCHED chunk row(s) in ${summary.mismatchedTables.length} node/table(s)`
      } (${summary.durationSeconds} s) -> ${join(options.out, 'parity.tsv')}`
    );
    return mismatches.length === 0 ? 0 : 1;
  } finally {
    await postgres.close();
  }
};

main().then(
  (code) => process.exit(code),
  (error) => {
    console.error(error);
    process.exit(2);
  }
);
