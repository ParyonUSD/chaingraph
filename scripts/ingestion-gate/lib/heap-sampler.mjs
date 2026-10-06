/**
 * Preloaded into the agent via `node --import`. Appends
 * `<epochMs> <heapUsed> <rss>` lines to $INGESTION_GATE_HEAP_SAMPLES every
 * 100 ms. Sampling pauses while the main thread is busy, so reported peaks are
 * observed (lower-bound) values, not guaranteed maxima.
 */
import { appendFileSync } from 'node:fs';

const samplePath = process.env.INGESTION_GATE_HEAP_SAMPLES;
if (samplePath) {
  const timer = setInterval(() => {
    const usage = process.memoryUsage();
    try {
      appendFileSync(samplePath, `${Date.now()} ${usage.heapUsed} ${usage.rss}\n`);
    } catch {
      // ignore
    }
  }, 100);
  timer.unref();
}
