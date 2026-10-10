#!/usr/bin/env node
/**
 * Chaingraph ingestion gate – see docs/ingestion-gate.md.
 *
 *   yarn ingestion-gate [--agent-dir <built chaingraph checkout>]
 *                       [--scenarios max-block,burst,reorg,concurrent,catch-up] (opt-in: replay)
 *                       [--quick] [--thresholds <file.json>] [--out <file.json>] [--keep-pg]
 *                       [--pg auto|docker|host] [--pg-image postgres:18] [--pg-bin <dir>]
 *                       [--pg-port 55432] [--pg-url <postgres://user:pass@host:port>]
 *                       [--store postgres|clickhouse] [--ch-url http://localhost:18123]
 *
 * `--store clickhouse` runs the agent with CHAINGRAPH_STORE=clickhouse against
 * an already-running ClickHouse server (`--ch-url`, credentials may be in the
 * URL; never Cloud for the gate), applies the agent's DDL per scenario, reads
 * correctness through the compiled ClickHouse checker (pinned gated views) and
 * reports part_log/events write metrics instead of WAL; thresholds default to
 * thresholds.clickhouse.json.
 *
 * Exits non-zero if any scenario errors, fails a correctness check or exceeds
 * a threshold.
 */
import { execFileSync } from 'node:child_process';
import { cpus, totalmem } from 'node:os';
import { existsSync, mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

import * as clickhouseBackend from './lib/clickhouse.mjs';
import * as postgresBackend from './lib/postgres.mjs';
import { dockerMemoryBytes, startDockerPostgres, startHostPostgres } from './lib/postgres.mjs';
import { optInScenarios, scenarios as defaultScenarios } from './lib/scenarios.mjs';

const scenarios = { ...defaultScenarios, ...optInScenarios };

const harnessDirectory = dirname(fileURLToPath(import.meta.url));
const repositoryRoot = resolve(harnessDirectory, '../..');

const { values: options } = parseArgs({
  options: {
    'agent-dir': { default: repositoryRoot, type: 'string' },
    'cache-dir': { default: join(repositoryRoot, 'data/ingestion-gate/fixtures'), type: 'string' },
    'ch-url': { default: process.env.INGESTION_GATE_CH_URL ?? 'http://localhost:18123', type: 'string' },
    quick: { default: false, type: 'boolean' },
    help: { default: false, type: 'boolean' },
    'keep-pg': { default: false, type: 'boolean' },
    label: { type: 'string' },
    out: { default: 'ingestion-gate.json', type: 'string' },
    pg: { default: process.env.INGESTION_GATE_PG ?? 'auto', type: 'string' },
    'pg-bin': { default: process.env.INGESTION_GATE_PG_BIN ?? '/opt/homebrew/opt/postgresql@18/bin', type: 'string' },
    'pg-image': { default: process.env.INGESTION_GATE_PG_IMAGE ?? 'postgres:18', type: 'string' },
    'pg-port': { default: process.env.INGESTION_GATE_PG_PORT ?? '55432', type: 'string' },
    'pg-url': { type: 'string' },
    scenarios: { default: Object.keys(defaultScenarios).join(','), type: 'string' },
    seed: { default: '1', type: 'string' },
    store: { default: process.env.INGESTION_GATE_STORE ?? 'postgres', type: 'string' },
    thresholds: { type: 'string' },
  },
});

if (options.help) {
  console.log(readFileSync(fileURLToPath(import.meta.url), 'utf8').split('*/')[0]);
  process.exit(0);
}

const log = (message) => console.log(`[ingestion-gate ${new Date().toISOString().slice(11, 19)}] ${message}`);
const agentDirectory = resolve(options['agent-dir']);
if (!['postgres', 'clickhouse'].includes(options.store)) throw new Error(`--store must be postgres or clickhouse, got ${options.store}`);
const isClickHouse = options.store === 'clickhouse';
const backend = isClickHouse ? clickhouseBackend : postgresBackend;
const thresholdsPath = options.thresholds ?? join(harnessDirectory, isClickHouse ? 'thresholds.clickhouse.json' : 'thresholds.json');
const thresholds = JSON.parse(readFileSync(thresholdsPath, 'utf8'));
const selectedScenarios = options.scenarios.split(',').map((name) => name.trim()).filter(Boolean);
selectedScenarios.forEach((name) => {
  if (scenarios[name] === undefined) throw new Error(`unknown scenario: ${name}`);
});

const gitRevision = (directory) => {
  try {
    return execFileSync('git', ['-C', directory, 'rev-parse', 'HEAD'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  } catch {
    const revisionFile = join(directory, 'REVISION');
    return existsSync(revisionFile) ? readFileSync(revisionFile, 'utf8').trim() : 'unknown';
  }
};

const settings = {
  blockBufferMb: 128,
  catchUpBlocks: options.quick ? 1_000 : 10_000,
  catchUpTransactionsPerBlock: 20,
  concurrentChipnetBlocks: 50,
  concurrentChipnetTransactionsPerBlock: 2_000,
  concurrentMainnetBlocks: 8,
  concurrentMainnetTransactionsPerBlock: 12_500,
  denseTransactionsPerBlock: 100_000,
  internalApiPort: Number(process.env.INGESTION_GATE_API_PORT ?? 3299),
  postgresMaxConnections: 8,
  reorgTransactionsPerBlock: 1_000,
};

const runId = new Date().toISOString().replace(/[:.]/g, '-');
const runDirectory = join(repositoryRoot, 'data/ingestion-gate/runs', runId);
mkdirSync(runDirectory, { recursive: true });

let postgresServer;
const cleanupAndExit = (code) => {
  if (postgresServer !== undefined && !options['keep-pg']) postgresServer.stop();
  process.exit(code);
};

/**
 * A 32 MB block save needs ~3 GB of Postgres backend memory, so the 3-block
 * burst needs ~10 GB. `auto` uses Docker when its VM has >= 14 GB, otherwise
 * a private host cluster if host binaries exist.
 */
const minimumDockerMemoryBytes = 14e9;
const startPostgres = async () => {
  if (isClickHouse) return { baseUrl: options['ch-url'], description: 'clickhouse (external)', stop: () => {} };
  if (options['pg-url'] !== undefined) return { baseUrl: options['pg-url'], description: 'external', stop: () => {} };
  let mode = options.pg;
  if (mode === 'auto') {
    const dockerMemory = dockerMemoryBytes();
    const hostAvailable = existsSync(join(options['pg-bin'], 'pg_ctl'));
    mode = dockerMemory >= minimumDockerMemoryBytes || !hostAvailable ? 'docker' : 'host';
    log(`--pg auto: Docker VM memory ${(dockerMemory / 1e9).toFixed(1)} GB, host binaries ${hostAvailable ? 'found' : 'not found'} -> ${mode}`);
    if (mode === 'docker' && dockerMemory < minimumDockerMemoryBytes) log('WARNING: Docker VM has < 14 GB; the burst scenario may be OOM-killed (see docs/ingestion-gate.md).');
  }
  const port = Number(options['pg-port']);
  return mode === 'host'
    ? startHostPostgres({ binDirectory: options['pg-bin'], dataDirectory: join(repositoryRoot, `data/ingestion-gate/pg-${process.pid}`), log, port })
    : startDockerPostgres({ image: options['pg-image'], log, name: `chaingraph-ingestion-gate-pg-${process.pid}`, port });
};
process.on('SIGINT', () => cleanupAndExit(130));
process.on('SIGTERM', () => cleanupAndExit(143));

/** ClickHouse write limits (null in thresholds.clickhouse.json until calibrated). */
const clickhouseWriteRules = [
  ['bytesWritten', 'maxBytesWritten', 'max'],
  ['partsCreated', 'maxPartsCreated', 'max'],
  ['mergeSeconds', 'maxMergeSeconds', 'max'],
  ['delayedInserts', 'maxDelayedInserts', 'max'],
  ['rejectedInserts', 'maxRejectedInserts', 'max'],
];

/** [metric path, threshold key, comparison] per scenario. */
const thresholdRules = {
  'max-block': [
    ['wallSeconds', 'maxWallSeconds', 'max'],
    ['walBytes', 'maxWalBytes', 'max'],
    ['peakHeapBytes', 'maxPeakHeapBytes', 'max'],
    ...clickhouseWriteRules,
  ],
  replay: [['drainSeconds', 'maxDrainSeconds', 'max'], ...clickhouseWriteRules, ['eventLoopMaxMs', 'maxEventLoopMs', 'max']],
  burst: [
    ['drainSeconds', 'maxDrainSeconds', 'max'],
    ['walBytes', 'maxWalBytes', 'max'],
    ...clickhouseWriteRules,
  ],
  reorg: [['convergeSeconds', 'maxConvergeSeconds', 'max']],
  concurrent: [['concurrencyRatio', 'minConcurrencyRatio', 'min']],
  'catch-up': [['blocksPerSecond', 'minBlocksPerSecond', 'min']],
};

const evaluate = (name, result) => {
  const scenarioThresholds = thresholds[name] ?? {};
  const failures = [];
  const knownFailingChecks = scenarioThresholds.knownFailingChecks ?? [];
  const isKnown = (check) => knownFailingChecks.some((pattern) => check.includes(pattern));
  const knownFailures = (result.failedChecks ?? []).filter(isKnown);
  const unexpectedFailedChecks = (result.failedChecks ?? []).filter((check) => !isKnown(check));
  if (knownFailures.length > 0) result.knownFailures = knownFailures;
  if (result.failedChecks !== undefined ? unexpectedFailedChecks.length > 0 : result.correct === false) {
    failures.push(`correctness check failed${unexpectedFailedChecks.length ? `: ${unexpectedFailedChecks.join('; ')}` : ''}`);
  }
  (thresholdRules[name] ?? []).forEach(([metric, key, comparison]) => {
    const limit = scenarioThresholds[key];
    const value = result[metric];
    if (limit === undefined || limit === null || value === undefined) return;
    if (comparison === 'max' ? value > limit : value < limit) {
      failures.push(`${metric} = ${formatValue(metric, value)} ${comparison === 'max' ? '>' : '<'} ${key} ${formatValue(metric, limit)}`);
    }
  });
  return failures;
};

const formatValue = (metric, value) => {
  if (typeof value !== 'number') return String(value);
  if (/Bytes$/i.test(metric)) return `${(value / 1e6).toFixed(1)} MB`;
  if (/Seconds$/.test(metric)) return `${value.toFixed(2)} s`;
  if (/Ratio$/.test(metric)) return value.toFixed(2);
  return value.toFixed(1);
};

const summaryColumns = (name, result) => {
  if (result === undefined) return {};
  const primary = {
    'max-block': `wall ${formatValue('wallSeconds', result.wallSeconds)}`,
    burst: `drain ${formatValue('drainSeconds', result.drainSeconds)}`,
    replay: `drain ${formatValue('drainSeconds', result.drainSeconds)}`,
    reorg: `converge ${formatValue('convergeSeconds', result.convergeSeconds)}`,
    concurrent: `ratio ${formatValue('concurrencyRatio', result.concurrencyRatio)} (${Math.round(result.together?.transactionsPerSecond ?? 0)} vs ${Math.round(result.mainnetAlone?.transactionsPerSecond ?? 0)}+${Math.round(result.chipnetAlone?.transactionsPerSecond ?? 0)} tx/s)`,
    'catch-up': `${result.blocksPerSecond?.toFixed(1)} blocks/s`,
  }[name];
  return {
    heap: result.peakHeapBytes ? formatValue('peakHeapBytes', result.peakHeapBytes) : '',
    primary: result.eventLoopMaxMs === undefined ? primary : `${primary}, loop max ${Math.round(result.eventLoopMaxMs)} ms`,
    txPerSecond: result.transactionsPerSecond ? Math.round(result.transactionsPerSecond).toString() : '',
    wal:
      result.walBytes !== undefined
        ? formatValue('walBytes', result.walBytes)
        : result.bytesWritten !== undefined
          ? `${formatValue('walBytes', result.bytesWritten)} (+${formatValue('walBytes', result.mergeBytesWritten)} merges, ${result.partsCreated} parts)`
          : '',
  };
};

/**
 * With CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS set (passed to the agent), the worst
 * event-loop delay any agent of the scenario logged (whole agent lifetime).
 */
const eventLoopSummary = (scenarioDirectory) => {
  if (!existsSync(scenarioDirectory)) return {};
  let eventLoopMaxMs;
  let eventLoopWorstP99Ms;
  let eventLoopSamples = 0;
  readdirSync(scenarioDirectory)
    .filter((file) => file.endsWith('.agent.ndjson'))
    .forEach((file) => {
      readFileSync(join(scenarioDirectory, file), 'utf8')
        .split('\n')
        .filter((line) => line.includes('"eventLoopDelay"'))
        .forEach((line) => {
          const { eventLoopDelay } = JSON.parse(line);
          eventLoopSamples += 1;
          eventLoopMaxMs = Math.max(eventLoopMaxMs ?? 0, eventLoopDelay.maxMs);
          eventLoopWorstP99Ms = Math.max(eventLoopWorstP99Ms ?? 0, eventLoopDelay.p99Ms);
        });
    });
  return eventLoopSamples === 0 ? {} : { eventLoopMaxMs, eventLoopSamples, eventLoopWorstP99Ms };
};

const main = async () => {
  const gateStarted = Date.now();
  postgresServer = await startPostgres();
  const { baseUrl } = postgresServer;
  const report = {
    agent: { directory: agentDirectory, revision: gitRevision(agentDirectory) },
    environment: {
      cpus: cpus().length,
      cpuModel: cpus()[0]?.model,
      node: process.version,
      platform: `${process.platform}-${process.arch}`,
      postgres: postgresServer.description,
      store: options.store,
      totalMemoryBytes: totalmem(),
    },
    harnessRevision: gitRevision(repositoryRoot),
    label: options.label ?? null,
    passed: true,
    runDirectory,
    scenarios: {},
    settings: { ...settings, quick: options.quick, seed: options.seed },
    startedAt: new Date(gateStarted).toISOString(),
    thresholds,
  };
  log(`agent under test: ${agentDirectory} @ ${report.agent.revision}`);
  for (const name of selectedScenarios) {
    log(`scenario ${name}: starting`);
    const started = Date.now();
    const context = {
      agentDirectory,
      backend,
      baseUrl,
      cacheDirectory: options['cache-dir'],
      log,
      runDirectory: join(runDirectory, name),
      seed: options.seed,
      settings,
    };
    let entry;
    try {
      const result = await scenarios[name](context);
      Object.assign(result, eventLoopSummary(context.runDirectory));
      const failures = evaluate(name, result);
      entry = { failures, passed: failures.length === 0, result, scenarioSeconds: (Date.now() - started) / 1000 };
    } catch (error) {
      entry = { error: String(error?.stack ?? error), failures: [`error: ${error?.message ?? error}`], passed: false, scenarioSeconds: (Date.now() - started) / 1000 };
    }
    report.scenarios[name] = entry;
    if (!entry.passed) report.passed = false;
    log(`scenario ${name}: ${entry.passed ? 'PASS' : 'FAIL'} (${entry.scenarioSeconds.toFixed(0)} s)${entry.failures.length ? ` – ${entry.failures.join('; ')}` : ''}`);
    writeFileSync(options.out, `${JSON.stringify(report, null, 2)}\n`);
  }
  if (options['keep-pg']) log(`--keep-pg: leaving Postgres running at ${baseUrl}/chaingraph_ingestion_gate (stop it yourself)`);
  else await backend.dropDatabase({ agentDirectory, baseUrl, databaseName: 'chaingraph_ingestion_gate' }).catch(() => {});
  report.totalSeconds = (Date.now() - gateStarted) / 1000;
  writeFileSync(options.out, `${JSON.stringify(report, null, 2)}\n`);

  const rows = selectedScenarios.map((name) => {
    const entry = report.scenarios[name];
    const columns = summaryColumns(name, entry.result);
    const status = entry.passed ? (entry.result?.knownFailures ? 'PASS*' : 'PASS') : 'FAIL';
    return [name, status, columns.primary ?? '-', columns.txPerSecond ?? '', columns.wal ?? '', columns.heap ?? '', entry.failures.join('; ')];
  });
  const header = ['scenario', 'status', 'headline', 'tx/s', isClickHouse ? 'written' : 'WAL', 'peak heap', 'failures'];
  const widths = header.map((title, index) => Math.max(title.length, ...rows.map((row) => String(row[index]).length)));
  const line = (cells) => cells.map((cell, index) => String(cell).padEnd(widths[index])).join(' | ');
  console.log(`\n${line(header)}\n${widths.map((width) => '-'.repeat(width)).join('-|-')}\n${rows.map(line).join('\n')}\n`);
  selectedScenarios.forEach((name) => (report.scenarios[name].result?.knownFailures ?? []).forEach((check) => log(`PASS* ${name}: known failing check (allow-listed in thresholds): ${check}`)));
  log(`total ${report.totalSeconds.toFixed(0)} s – report written to ${resolve(options.out)} – ${report.passed ? 'GATE PASSED' : 'GATE FAILED'}`);
  return report.passed ? 0 : 1;
};

main()
  .then((code) => cleanupAndExit(code))
  .catch((error) => {
    console.error(error);
    cleanupAndExit(2);
  });
