/* eslint-disable @typescript-eslint/naming-convention, functional/no-try-statement */
/**
 * `config.ts` loads `.env` from the working directory if there is one: no
 * file means defaults plus the process environment (fix-pass-3.md §2); an
 * existing file behaves as before (its values over the defaults, the process
 * environment over both; an unreadable one still throws).
 */
import { execFile } from 'node:child_process';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { promisify } from 'node:util';

import test from 'ava';

const execFileAsync = promisify(execFile);

const configModule = pathToFileURL(
  join(process.cwd(), 'build', 'config.js')
).href;

const cleanEnvironment = () =>
  Object.fromEntries(
    Object.entries(process.env).filter(
      ([key]) => !key.startsWith('CHAINGRAPH_') && key !== 'NODE_ENV'
    )
  );

/** Import the compiled config with `cwd` as the working directory. */
const loadConfig = async (
  cwd: string,
  environment: { [key: string]: string } = {}
) => {
  const script = `const c = await import(${JSON.stringify(configModule)});
console.log(JSON.stringify({ userAgent: c.chaingraphUserAgent, store: c.chaingraphStore, nodes: c.trustedNodes.map((n) => n.name) }));`;
  const { stdout } = await execFileAsync(
    process.execPath,
    ['--input-type=module', '-e', script],
    { cwd, env: { ...cleanEnvironment(), ...environment } }
  );
  return JSON.parse(stdout.trim().split('\n').pop()!) as {
    userAgent: string;
    store: string;
    nodes: string[];
  };
};

const withDirectory = async (
  run: (directory: string) => Promise<void>
): Promise<void> => {
  const directory = await mkdtemp(join(tmpdir(), 'chaingraph-config-'));
  try {
    await run(directory);
  } finally {
    await rm(directory, { force: true, recursive: true });
  }
};

test('config: no .env in the working directory → defaults and process environment', async (t) => {
  await withDirectory(async (directory) => {
    const defaults = await loadConfig(directory);
    t.is(defaults.store, 'postgres');
    t.deepEqual(defaults.nodes, ['bchn-testnet', 'bchn-chipnet']);
    const fromEnvironment = await loadConfig(directory, {
      CHAINGRAPH_USER_AGENT: '/from-env/',
    });
    t.is(fromEnvironment.userAgent, '/from-env/');
  });
});

test('config: an existing .env is applied as before (over defaults, under the process environment)', async (t) => {
  await withDirectory(async (directory) => {
    await writeFile(
      join(directory, '.env'),
      'CHAINGRAPH_USER_AGENT=/from-dotenv/\nCHAINGRAPH_TRUSTED_NODES=lab:127.0.0.1:48333:chipnet\n'
    );
    const fromFile = await loadConfig(directory);
    t.is(fromFile.userAgent, '/from-dotenv/');
    t.deepEqual(fromFile.nodes, ['lab']);
    const overridden = await loadConfig(directory, {
      CHAINGRAPH_USER_AGENT: '/from-env/',
    });
    t.is(overridden.userAgent, '/from-env/');
    t.deepEqual(overridden.nodes, ['lab']);
  });
});

test('config: an unreadable .env still fails startup', async (t) => {
  await withDirectory(async (directory) => {
    await mkdir(join(directory, '.env'));
    await t.throwsAsync(loadConfig(directory), { message: /EISDIR/u });
  });
});
