/**
 * Spawn a built chaingraph agent (any revision) against the gate's mock nodes.
 */
import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const heapSamplerUrl = pathToFileURL(fileURLToPath(new URL('./heap-sampler.mjs', import.meta.url))).href;

export class AgentProcess {
  constructor({ agentDirectory, runDirectory, label, connectionString, trustedNodes, genesisBlocks, settings }) {
    this.label = label;
    this.stdoutBuffer = '';
    this.waiters = [];
    mkdirSync(runDirectory, { recursive: true });
    // the agent's dotenv call requires a .env file in its cwd
    writeFileSync(join(runDirectory, '.env'), '');
    this.heapSamplePath = join(runDirectory, `${label}.heap.txt`);
    writeFileSync(this.heapSamplePath, '');
    this.logPath = join(runDirectory, `${label}.agent.ndjson`);
    const entry = join(agentDirectory, 'bin/chaingraph.js');
    if (!existsSync(join(agentDirectory, 'build/index.js'))) {
      throw new Error(`${agentDirectory} is not built (missing build/index.js) – run \`yarn build\` there first.`);
    }
    this.exited = false;
    this.child = spawn(process.execPath, ['--import', heapSamplerUrl, entry], {
      cwd: runDirectory,
      env: {
        CHAINGRAPH_BLOCK_BUFFER_TARGET_SIZE_MB: String(settings.blockBufferMb),
        CHAINGRAPH_GENESIS_BLOCKS: genesisBlocks,
        CHAINGRAPH_INTERNAL_API_PORT: String(settings.internalApiPort),
        CHAINGRAPH_LOG_FIREHOSE: 'false',
        CHAINGRAPH_LOG_LEVEL_PATH: 'debug',
        CHAINGRAPH_LOG_LEVEL_STDOUT: 'info',
        CHAINGRAPH_LOG_PATH: this.logPath,
        CHAINGRAPH_POSTGRES_CONNECTION_STRING: connectionString,
        CHAINGRAPH_POSTGRES_MAX_CONNECTIONS: String(settings.postgresMaxConnections),
        CHAINGRAPH_TRUSTED_NODES: trustedNodes,
        // experiment knobs passed through from the gate's environment (agent defaults otherwise)
        ...Object.fromEntries(
          ['CHAINGRAPH_UNSPENT_POST_COMMIT', 'CHAINGRAPH_UNSPENT_RESOLVE_NEW_OUTPUTS', 'CHAINGRAPH_UNSPENT_TRACKING', 'CHAINGRAPH_WRITE_PATH']
            .filter((key) => process.env[key] !== undefined)
            .map((key) => [key, process.env[key]])
        ),
        HOME: process.env.HOME,
        INGESTION_GATE_HEAP_SAMPLES: this.heapSamplePath,
        NODE_ENV: 'production',
        PATH: process.env.PATH,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const onData = (chunk) => {
      this.stdoutBuffer += chunk;
      if (this.stdoutBuffer.length > 4_000_000) this.stdoutBuffer = this.stdoutBuffer.slice(-2_000_000);
      this.checkWaiters();
    };
    this.child.stdout.on('data', onData);
    this.child.stderr.on('data', onData);
    this.exitPromise = new Promise((resolve) => {
      this.child.on('exit', (code) => {
        this.exited = true;
        this.exitCode = code;
        this.waiters.splice(0).forEach((waiter) => waiter.reject(new Error(`agent ${label} exited (code ${code}) while waiting for: ${waiter.pattern}\n--- last output ---\n${this.stdoutBuffer.slice(-3000)}`)));
        resolve(code);
      });
    });
  }

  checkWaiters() {
    this.waiters = this.waiters.filter((waiter) => {
      const matched = typeof waiter.pattern === 'string' ? this.stdoutBuffer.includes(waiter.pattern) : waiter.pattern.test(this.stdoutBuffer);
      if (matched) {
        clearTimeout(waiter.timer);
        waiter.resolve();
      }
      return !matched;
    });
  }

  waitForOutput(pattern, timeoutMs = 120_000) {
    return new Promise((resolve, reject) => {
      if (this.exited) {
        reject(new Error(`agent ${this.label} already exited`));
        return;
      }
      const waiter = { pattern, reject, resolve };
      waiter.timer = setTimeout(() => {
        this.waiters = this.waiters.filter((other) => other !== waiter);
        reject(new Error(`timed out waiting for agent output: ${pattern}\n--- last output ---\n${this.stdoutBuffer.slice(-3000)}`));
      }, timeoutMs);
      this.waiters.push(waiter);
      this.checkWaiters();
    });
  }

  /** Wait until initial sync, index creation and mempool tracking are done. */
  async waitForSteadyState(timeoutMs = 180_000) {
    await this.waitForOutput('Agent: initial sync is complete.', timeoutMs);
    await this.waitForOutput('Agent: enabled mempool tracking.', timeoutMs);
  }

  /** Observed peak heapUsed/rss between two epoch-ms timestamps. */
  heapPeak(fromMs, toMs) {
    let peakHeapUsed = 0;
    let peakRss = 0;
    let baselineHeapUsed;
    readFileSync(this.heapSamplePath, 'utf8')
      .split('\n')
      .forEach((line) => {
        const [time, heapUsed, rss] = line.split(' ').map(Number);
        if (!time) return;
        if (time <= fromMs) baselineHeapUsed = heapUsed;
        if (time >= fromMs && time <= toMs + 200) {
          peakHeapUsed = Math.max(peakHeapUsed, heapUsed);
          peakRss = Math.max(peakRss, rss);
        }
      });
    return { baselineHeapUsed: baselineHeapUsed ?? null, peakHeapUsed, peakRss };
  }

  async stop() {
    if (this.exited) return;
    this.child.kill('SIGINT');
    const timeout = new Promise((resolve) => setTimeout(() => resolve('timeout'), 15_000));
    if ((await Promise.race([this.exitPromise, timeout])) === 'timeout') {
      this.child.kill('SIGKILL');
      await this.exitPromise;
    }
  }
}
