#!/usr/bin/env node
// cspell:ignore clickhouse chipnet paryon synth
/**
 * Local admission load test (docs/clickhouse-port/fix-pass-7.md §1): the
 * mainnet session's A1 (uncached soak) and A3 (overload B) shapes against an
 * API process on a synthetic mainnet-scale database
 * (docs/clickhouse-port/synthetic/), one arm per admission configuration.
 *
 *   node scripts/measure/admission-load.mjs --db ch1_fix7_syn --arm on [--build <worktree>] \
 *     [--shapes a1,a3] [--a1-qps 3,6,9] [--a3-light 10 --a3-heavy 2.5] [--seconds 60] [--timeout-s 30] \
 *     [--port 4417] [--out /tmp/load-on.json] [--env NAME=value ...] [--variables synthetic|catalogue]
 *
 * --arm off sets CHAINGRAPH_API_ADMISSION=off; any other arm is the build's
 * defaults plus --env overrides (repeatable). --build: the worktree whose
 * build/api/main.js serves (default this one; an older worktree measures
 * "before"). The API runs with the response cache off (every request reaches
 * ClickHouse), as the GKE A1/A3 runs.
 *
 * Shapes (open loop: a fixed send rate, a slow answer does not slow the
 * sender; mainnet rates scaled by the local server's cores, 14 vs 48):
 * - a1: the soak mix L40–L47, L12, L31, L35, X2 at each --a1-qps level
 *   (mainnet 10 / 20 / 30 QPS ≈ 3 / 6 / 9 here);
 * - a3: overload B: light L13 L20 L25 L40 L43 L46 X4 X8 at --a3-light QPS plus
 *   heavy L23 L30 L33 L35 X12 X13 at --a3-heavy QPS (mainnet 10 + 8).
 * Per level: ok / errors by class (OVERLOADED with its latency: refusals must
 * be fast), p50 / p95 / p99, per-document p95; light and heavy separately for
 * a3; ClickHouse CPU-s of the API's statements (query_log, cgapi-%).
 * Variables: the synthetic database's planted contracts (docs.json and the
 * generator's functions), mapped onto the chipnet catalogue's variables.
 *
 * Fix pass 8: per level also the API process's own CPU (ps, the primary and
 * its workers: a single event loop was the mainnet limit) and the admission
 * gate's state (`GET /admission`); --warm-rounds (default 2) warm-up rounds
 * of every document, then --warm-wait-s (default 10) for the CPU ledger
 * (system.query_log flushes every 7.5 s) before the first level.
 */
import { execFileSync } from 'node:child_process';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = join(here, '..', '..');
const argv = process.argv.slice(2);
const opt = (name, fallback) => {
  const index = argv.indexOf(name);
  return index === -1 ? fallback : argv[index + 1];
};
const opts = (name) =>
  argv.flatMap((arg, index) => (arg === name ? [argv[index + 1]] : []));
const db = opt('--db');
const arm = opt('--arm', 'on');
const build = opt('--build', root);
const shapes = opt('--shapes', 'a1,a3').split(',');
const a1Levels = opt('--a1-qps', '3,6,9').split(',').map(Number);
const a3Light = Number(opt('--a3-light', '10'));
const a3Heavy = Number(opt('--a3-heavy', '2.5'));
const seconds = Number(opt('--seconds', '60'));
const timeoutMs = Number(opt('--timeout-s', '30')) * 1000;
const port = Number(opt('--port', '4417'));
const chUrl = opt('--clickhouse', 'http://localhost:18123');
const out = opt('--out', `/tmp/admission-load-${arm}.json`);
/* catalogue: the chipnet catalogue's own variables (a chipnet database), else the synthetic database's */
const variableSource = opt('--variables', 'synthetic');
const warmRounds = Number(opt('--warm-rounds', '2'));
const warmWaitS = Number(opt('--warm-wait-s', '10'));
if (db === undefined) {
  console.error('usage: --db <database> [--arm on|off|<label>] …');
  process.exit(2);
}

/* ------------------------------------------------------------ documents */
const sha = (text) => createHash('sha256').update(text).digest('hex');
const paryon = sha('paryon');
const price = `aa20${sha('price')}87`;
const loan = `aa20${sha('loan')}87`;
const user = `76a914${sha('a5242').slice(0, 40)}88ac`;
const byName = {
  hashes: [sha('tx1000'), sha('tx2000')],
  interestManagerCommitment: '01',
  loanContractLockingBytecode: loan,
  loanKey: sha('cat9'),
  loanTokenId: sha('cat8'),
  lockingBytecodeLoan: loan,
  node: 'synth-node',
  paryonTokenId: paryon,
  poolTokenId: sha('cat7'),
  priceContractLockingBytecode: price,
  tokenId: paryon,
  userLockingBytecodes: [user],
  userLoanKeys: [sha('cat10'), sha('cat11')],
};
const documentsDir = join(root, 'docs', 'clickhouse-port', 'v2-documents');
const catalogue = JSON.parse(
  readFileSync(join(documentsDir, 'catalogue.json'), 'utf8')
);
const entries = Array.isArray(catalogue)
  ? catalogue
  : catalogue.entries ?? catalogue.documents;
const synthetic = JSON.parse(
  readFileSync(
    join(root, 'docs', 'clickhouse-port', 'synthetic', 'docs.json'),
    'utf8'
  )
);
const doc = (id) => {
  const entry = entries.find((e) => e.id === id);
  if (entry === undefined) throw new Error(`no catalogue entry ${id}`);
  const query = readFileSync(join(documentsDir, entry.file), 'utf8')
    .split('\n')
    .filter((line) => !line.startsWith('#'))
    .join('\n')
    .replace(/^\s*subscription\b/u, 'query');
  const variables =
    variableSource === 'catalogue'
      ? entry.variables ?? entry.v2?.variables
      : synthetic[id]?.variables ??
        Object.fromEntries(
          Object.entries(entry.variables ?? entry.v2?.variables ?? {}).map(
            ([name, value]) => [name, byName[name] ?? value]
          )
        );
  return { id, query, variables };
};
const a1Mix = [
  'L40',
  'L41',
  'L42',
  'L43',
  'L44',
  'L45',
  'L46',
  'L47',
  'L12',
  'L31',
  'L35',
  'X2',
].map(doc);
const lightMix = ['L13', 'L20', 'L25', 'L40', 'L43', 'L46', 'X4', 'X8'].map(
  doc
);
const heavyMix = ['L23', 'L30', 'L33', 'L35', 'X12', 'X13'].map(doc);

/* ------------------------------------------------------------ the API */
const extraEnv = Object.fromEntries(
  opts('--env').map((pair) => {
    const at = pair.indexOf('=');
    return [pair.slice(0, at), pair.slice(at + 1)];
  })
);
const apiUrl = `http://127.0.0.1:${port}/graphql`;
const startApi = async () => {
  const child = spawn(
    process.execPath,
    [join(build, 'build', 'api', 'main.js')],
    {
      cwd: '/tmp',
      env: {
        ...process.env,
        CHAINGRAPH_API_HOST: '127.0.0.1',
        CHAINGRAPH_API_PORT: String(port),
        CHAINGRAPH_API_RESPONSE_CACHE: 'off',
        CHAINGRAPH_CLICKHOUSE_DATABASE: db,
        CHAINGRAPH_CLICKHOUSE_URL: chUrl,
        ...(arm === 'off' ? { CHAINGRAPH_API_ADMISSION: 'off' } : {}),
        ...extraEnv,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    }
  );
  let log = '';
  child.stdout.on('data', (data) => {
    log += data;
  });
  child.stderr.on('data', (data) => {
    log += data;
  });
  for (let attempt = 0; attempt < 120; attempt += 1) {
    try {
      const res = await fetch(apiUrl, {
        body: JSON.stringify({ query: '{ __typename }' }),
        headers: { 'content-type': 'application/json' },
        method: 'POST',
      });
      if (res.status === 200) break;
    } catch {
      /* not up yet */
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  const admissionLine = log
    .split('\n')
    .find((line) => line.startsWith('admission:'));
  return { admissionLine, child };
};

/* ------------------------------------------------------------ load */
const pct = (xs, p) =>
  xs.length === 0
    ? null
    : [...xs].sort((a, b) => a - b)[
        Math.min(xs.length - 1, Math.floor((p / 100) * xs.length))
      ];
const classify = (status, body) => {
  const error = body?.errors?.[0];
  const code = error?.extensions?.code;
  if (status === 503 || code === 'OVERLOADED') return 'OVERLOADED';
  if (code === 'REQUEST_ABORTED' || code === 'QUERY_TIMEOUT') return code;
  if (/MEMORY_LIMIT|memory limit/iu.test(String(error?.message ?? '')))
    return 'MEMORY_LIMIT';
  return 'other';
};
const one = async (document, stream) => {
  const started = performance.now();
  try {
    const res = await fetch(apiUrl, {
      body: JSON.stringify({
        query: document.query,
        variables: document.variables,
      }),
      headers: { 'content-type': 'application/json' },
      method: 'POST',
      signal: AbortSignal.timeout(timeoutMs),
    });
    const body = await res.json().catch(() => undefined);
    const ok = res.status === 200 && body !== undefined && !body.errors?.length;
    return {
      err: ok
        ? undefined
        : String(body?.errors?.[0]?.message ?? `HTTP ${res.status}`).slice(
            0,
            160
          ),
      id: document.id,
      kind: ok ? 'ok' : classify(res.status, body),
      ms: performance.now() - started,
      ok,
      stream,
    };
  } catch (error) {
    const timeout = error?.name === 'TimeoutError';
    return {
      err: String(timeout ? 'timeout' : error?.message ?? error).slice(0, 160),
      id: document.id,
      kind: timeout ? 'timeout' : 'other',
      ms: performance.now() - started,
      ok: false,
      stream,
    };
  }
};
const summarise = (results, documents) => {
  const ok = results.filter((r) => r.ok).map((r) => r.ms);
  const classes = {};
  for (const r of results.filter((x) => !x.ok)) {
    (classes[r.kind] ??= []).push(r.ms);
  }
  const kinds = {};
  for (const r of results.filter((x) => !x.ok))
    kinds[r.err] = (kinds[r.err] ?? 0) + 1;
  return {
    errorClasses: Object.fromEntries(
      Object.entries(classes).map(([k, ms]) => [
        k,
        { n: ms.length, p50: pct(ms, 50), p95: pct(ms, 95) },
      ])
    ),
    errorKinds: kinds,
    errors: results.length - ok.length,
    ok: ok.length,
    p50: pct(ok, 50),
    p95: pct(ok, 95),
    p99: pct(ok, 99),
    p95PerDoc: Object.fromEntries(
      documents.map((d) => [
        d.id,
        pct(
          results.filter((r) => r.id === d.id && r.ok).map((r) => r.ms),
          95
        ),
      ])
    ),
    sent: results.length,
  };
};
const stream = async (documents, rate, name, pending) => {
  const interval = 1000 / rate;
  const started = performance.now();
  let index = 0;
  while (performance.now() - started < seconds * 1000) {
    pending.push(one(documents[index % documents.length], name));
    index += 1;
    const wait = started + index * interval - performance.now();
    if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
  }
};
const ch = async (sql) => {
  const res = await fetch(`${chUrl}/?default_format=JSONEachRow`, {
    body: sql,
    method: 'POST',
  });
  const text = await res.text();
  if (!res.ok) throw new Error(text);
  return text
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
};
/* ClickHouse CPU-s and statements of the API between two instants (query_log) */
const serverCost = async (fromMs, toMs) => {
  await ch('SYSTEM FLUSH LOGS');
  const [row] = await ch(`SELECT count() AS statements,
      round(sum(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']) / 1e6, 1) AS cpu_s,
      countIf(exception_code = 394) AS cancelled, countIf(exception_code = 241) AS memory_limit,
      max(query_duration_ms) AS max_ms
    FROM system.query_log WHERE type != 'QueryStart' AND query_id LIKE 'cgapi-%'
      AND current_database = '${db}'
      AND event_time_microseconds >= fromUnixTimestamp64Milli(${Math.floor(
        fromMs
      )})
      AND event_time_microseconds <= fromUnixTimestamp64Milli(${Math.ceil(
        toMs
      )})`);
  return row;
};
const settle = async () => {
  /* wait for the server to be quiet (no API statement running) before the next level */
  for (let attempt = 0; attempt < 120; attempt += 1) {
    const [row] = await ch(
      "SELECT count() AS n FROM system.processes WHERE query_id LIKE 'cgapi-%'"
    );
    if (Number(row.n) === 0) return;
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
};
/* CPU seconds of the API process and its workers (ps cputime) */
const apiCpuSeconds = (pid) => {
  try {
    let children = [];
    try {
      children = execFileSync('pgrep', ['-P', String(pid)], {
        encoding: 'utf8',
      })
        .split('\n')
        .filter(Boolean);
    } catch {
      /* no workers */
    }
    return [String(pid), ...children].reduce((sum, p) => {
      const text = execFileSync('ps', ['-o', 'time=', '-p', p], {
        encoding: 'utf8',
      }).trim();
      const parts = text.split(/[:.]/u).map(Number);
      /* [[dd-]hh:]mm:ss.cc */
      const [cc, ss, mm, hh = 0] = [
        parts.at(-1),
        parts.at(-2),
        parts.at(-3),
        parts.at(-4),
      ];
      return sum + (hh * 3600 + mm * 60 + ss + cc / 100);
    }, 0);
  } catch {
    return Number.NaN;
  }
};
const gateState = async () => {
  try {
    const res = await fetch(apiUrl.replace('/graphql', '/admission'));
    return res.ok ? await res.json() : undefined;
  } catch {
    return undefined;
  }
};
let apiPid;
const run = async (label, streams) => {
  await settle();
  const pending = [];
  const fromMs = Date.now();
  const cpuBefore = apiCpuSeconds(apiPid);
  await Promise.all(
    streams.map((s) => stream(s.documents, s.rate, s.name, pending))
  );
  const results = await Promise.all(pending);
  const toMs = Date.now();
  const all = [...new Set(streams.flatMap((s) => s.documents))];
  const row = {
    label,
    qps: streams.reduce((sum, s) => sum + s.rate, 0),
    ...summarise(results, all),
    ...(streams.length > 1
      ? Object.fromEntries(
          streams.map((s) => [
            s.name,
            summarise(
              results.filter((r) => r.stream === s.name),
              s.documents
            ),
          ])
        )
      : {}),
    server: await serverCost(fromMs, toMs),
    apiCpuSeconds: apiCpuSeconds(apiPid) - cpuBefore,
    gate: await gateState(),
  };
  const classes =
    Object.entries(row.errorClasses)
      .map(
        ([k, v]) =>
          `${k} ${v.n} (p50 ${v.p50?.toFixed(0)} p95 ${v.p95?.toFixed(0)} ms)`
      )
      .join(', ') || 'none';
  console.log(
    `${arm} ${label}: sent ${row.sent}, ok ${row.ok}, errors ${
      row.errors
    } [${classes}], p50 ${row.p50?.toFixed(0)} p95 ${row.p95?.toFixed(
      0
    )} p99 ${row.p99?.toFixed(0)} ms; server ${row.server.cpu_s} CPU-s, ${
      row.server.statements
    } statements, ${
      row.server.memory_limit
    } MEMORY_LIMIT; API process ${row.apiCpuSeconds.toFixed(1)} CPU-s`
  );
  for (const s of streams.length > 1 ? streams : []) {
    const part = row[s.name];
    console.log(
      `  ${s.name}: ok ${part.ok}/${part.sent} p50 ${part.p50?.toFixed(
        0
      )} p95 ${part.p95?.toFixed(0)} ms, errors ${JSON.stringify(
        part.errorClasses
      )}`
    );
  }
  return row;
};

const { admissionLine, child } = await startApi();
apiPid = child.pid;
console.log(
  `${arm}: ${admissionLine ?? '(no admission line)'} build ${build} db ${db}`
);
const report = {
  arm,
  admissionLine,
  build,
  db,
  env: extraEnv,
  rows: [],
  seconds,
  timeoutS: timeoutMs / 1000,
};
try {
  /* warm-up: every document once */
  for (let round = 0; round < warmRounds; round += 1) {
    for (const document of [...a1Mix, ...lightMix, ...heavyMix])
      await one(document, 'warm');
  }
  await new Promise((resolve) => setTimeout(resolve, warmWaitS * 1000));
  if (shapes.includes('a1')) {
    for (const rate of a1Levels) {
      report.rows.push(
        await run(`a1 ${rate} qps`, [{ documents: a1Mix, name: 'mix', rate }])
      );
    }
  }
  if (shapes.includes('a3')) {
    report.rows.push(
      await run(`a3 light ${a3Light} + heavy ${a3Heavy} qps`, [
        { documents: lightMix, name: 'light', rate: a3Light },
        { documents: heavyMix, name: 'heavy', rate: a3Heavy },
      ])
    );
  }
} finally {
  child.kill('SIGTERM');
}
writeFileSync(out, JSON.stringify(report, null, 1));
console.log(`results: ${out}`);
