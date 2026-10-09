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
