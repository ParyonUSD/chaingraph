#!/usr/bin/env node
// cspell:ignore clickhouse chipnet
/**
 * API-side CPU of one document (fix-pass-9.md §4): the document's
 * ClickHouse responses are captured once (raw bytes), then replayed from
 * memory, so each timed run is only the API's own work: result decoding,
 * level assembly, GraphQL execution and serialisation (in process, through
 * `yoga.fetch`, as an HTTP request without the socket).
 *
 *   node scripts/measure/api-side.mjs --db <database> --doc <catalogue id | file.graphql>
 *     [--vars '{"limit":10000}'] [--runs 9] [--build <worktree>] [--cpu-prof <dir>]
 *
 * Prints JSON: median / min total ms, response bytes, ClickHouse result bytes.
 * --cpu-prof writes a V8 CPU profile of the timed runs (open in DevTools).
 */
import { readFileSync } from 'node:fs';
import { Session } from 'node:inspector/promises';
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const index = argv.indexOf(name);
  return index === -1 ? fallback : argv[index + 1];
};
const here = dirname(fileURLToPath(import.meta.url));
const root = opt('--build', join(here, '..', '..'));
const db = opt('--db');
const doc = opt('--doc');
const runs = Number(opt('--runs', '9'));
const over = JSON.parse(opt('--vars', '{}'));
const profDir = opt('--cpu-prof');
const imp = async (path) => import(pathToFileURL(join(root, path)).href);
const { ClickHouseClient } = await imp('build/store/clickhouse/client.js');
const { createApi } = await imp('build/api/server.js');
const catalogue = await imp('build/api/catalogue.js');

const entry = doc.endsWith('.graphql')
  ? undefined
  : catalogue.loadCatalogue().find((item) => item.id === doc);
const query = entry === undefined
  ? readFileSync(doc, 'utf8')
  : catalogue.documentText(entry.file);
const variables = { ...(entry?.v2?.variables ?? {}), ...over };
const client = new ClickHouseClient({
  database: db,
  password: '',
  requestTimeoutMs: 600_000,
  url: process.env.CH_URL ?? 'http://localhost:18123',
  username: 'default',
});
/* capture the raw responses, then replay them (the client's own decoding still runs) */
const captured = new Map();
let replay = false;
let resultBytes = 0;
const raw = client.raw;
const original = raw.query.bind(raw);
raw.query = async (params) => {
  const key = `${params.query}\n${JSON.stringify(params.query_params ?? {}, (_k, v) => (typeof v === 'bigint' ? v.toString() : v))}`;
  if (replay && captured.has(key)) {
    const { text, headers } = captured.get(key);
    resultBytes += text.length;
    return {
      json: async () => {
        /* as @clickhouse/client: one JSON.parse per line */
        const lines = text.split('\n');
        const rows = [];
        for (const line of lines) if (line !== '') rows.push(JSON.parse(line));
        return rows;
      },
      response_headers: headers,
    };
  }
  const result = await original(params);
  const text = await result.text();
  if (params.format === 'JSONEachRow' && !replay) {
    captured.set(key, { headers: result.response_headers, text });
  }
  return {
    json: async () => text.split('\n').filter((line) => line !== '').map((line) => JSON.parse(line)),
    response_headers: result.response_headers,
  };
};
const { disabledAdmissionOptions } = await imp('build/api/admission.js');
const api = createApi(client, { admission: disabledAdmissionOptions });
const sub = /^\s*subscription\b/u.test(query);
const body = JSON.stringify({
  query: sub ? query.replace(/^\s*subscription\b/u, 'query') : query,
  variables,
});
const once = async () => {
  const started = performance.now();
  const response = await api.yoga.fetch('http://localhost/graphql', {
    body,
    headers: { accept: 'application/json', 'content-type': 'application/json' },
    method: 'POST',
  });
  const bytes = (await response.arrayBuffer()).byteLength;
  return { bytes, ms: performance.now() - started, status: response.status };
};
const first = await once();
if (first.status !== 200) {
  console.error('status', first.status);
  process.exit(1);
}
replay = true;
await once();
const session = profDir === undefined ? undefined : new Session();
if (session !== undefined) {
  session.connect();
  await session.post('Profiler.enable');
  await session.post('Profiler.start');
}
const times = [];
resultBytes = 0;
for (let index = 0; index < runs; index += 1) times.push((await once()).ms);
if (session !== undefined) {
  const { profile } = await session.post('Profiler.stop');
  const file = join(profDir, `api-side-${doc.replace(/\W/gu, '_')}-${Date.now()}.cpuprofile`);
  writeFileSync(file, JSON.stringify(profile));
  console.error(`profile: ${file}`);
}
const sorted = [...times].sort((a, b) => a - b);
console.log(
  JSON.stringify({
    db,
    doc,
    minMs: Math.round(sorted[0]),
    medianMs: Math.round(sorted[Math.floor(sorted.length / 2)]),
    responseBytes: first.bytes,
    resultBytes: Math.round(resultBytes / runs),
    runs,
  })
);
await client.close();
process.exit(0);
