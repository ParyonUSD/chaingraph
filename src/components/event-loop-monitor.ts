/* eslint-disable @typescript-eslint/no-magic-numbers */
import { monitorEventLoopDelay } from 'perf_hooks';

/** One window of the event-loop diagnostic (all values in milliseconds). */
export interface EventLoopDelaySample {
  windowMs: number;
  p50Ms: number;
  p99Ms: number;
  maxMs: number;
  /** Heap in use at the end of the window (MB). */
  heapUsedMb: number;
}

const nsToMs = (ns: number) => Math.round(ns / 1e4) / 100;

/**
 * Env-gated diagnostic (`CHAINGRAPH_EVENT_LOOP_DIAGNOSTIC_MS`): every
 * `intervalMs`, report the event-loop delay of the agent's JS thread over the
 * window (p50 / p99 / max from `perf_hooks.monitorEventLoopDelay`). A long
 * synchronous stretch shows up as `maxMs` ≈ its length. Returns a stop
 * function. The timer is unref'd: it never keeps the process alive.
 */
export const startEventLoopMonitor = (
  intervalMs: number,
  report: (sample: EventLoopDelaySample) => void
) => {
  const histogram = monitorEventLoopDelay({ resolution: 10 });
  histogram.enable();
  // eslint-disable-next-line functional/no-let
  let windowStart = performance.now();
  const timer = setInterval(() => {
    const now = performance.now();
    report({
      heapUsedMb: Math.round(process.memoryUsage().heapUsed / 1e6),
      maxMs: nsToMs(histogram.max),
      p50Ms: nsToMs(histogram.percentile(50)),
      p99Ms: nsToMs(histogram.percentile(99)),
      windowMs: Math.round(now - windowStart),
    });
    windowStart = now;
    histogram.reset();
  }, intervalMs);
  timer.unref();
  return () => {
    clearInterval(timer);
    histogram.disable();
  };
};
