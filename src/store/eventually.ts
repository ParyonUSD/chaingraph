import { isDeepStrictEqual } from 'util';

const defaultTimeoutMs = 3000;
const defaultIntervalMs = 50;

const sleep = async (ms: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

/**
 * Poll `read` until `isDone` accepts its result or `timeoutMs` elapses, then
 * return the last result (so the caller's assertion reports the real value on
 * timeout). Reads that follow an agent event must go through this: backends
 * may publish writes asynchronously (e.g. batched mempool commits).
 *
 * With no `isDone`, any result other than `undefined` is accepted.
 */
export const eventually = async <T>(
  read: () => Promise<T>,
  {
    intervalMs = defaultIntervalMs,
    isDone = (value: T) => value !== undefined,
    timeoutMs = defaultTimeoutMs,
  }: {
    intervalMs?: number;
    isDone?: (value: T) => boolean;
    timeoutMs?: number;
  } = {}
): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  const attempt = async (): Promise<T> => {
    const value = await read();
    if (isDone(value) || Date.now() >= deadline) {
      return value;
    }
    await sleep(intervalMs);
    return attempt();
  };
  return attempt();
};

/**
 * `eventually` until the result deep-equals `expected` (`isDeepStrictEqual`,
 * as AVA's `t.deepEqual`); returns the last result, so the caller still
 * asserts `t.deepEqual(result, expected)` and a timeout reports the real value.
 */
export const eventuallyEqual = async <T>(
  read: () => Promise<T>,
  expected: T,
  options: { intervalMs?: number; timeoutMs?: number } = {}
): Promise<T> =>
  eventually(read, {
    ...options,
    isDone: (value) => isDeepStrictEqual(value, expected),
  });

/**
 * For negative ("stays absent") checks: read, wait `gapMs`, read again, and
 * return both results. Asserting both against the expected value checks a
 * stable state instead of a single read after a fixed sleep.
 */
export const readTwice = async <T>(
  read: () => Promise<T>,
  gapMs: number
): Promise<[T, T]> => {
  const first = await read();
  await sleep(gapMs);
  return [first, await read()];
};
