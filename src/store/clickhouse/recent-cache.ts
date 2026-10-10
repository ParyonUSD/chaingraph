/**
 * A bounded insertion-order cache (FIFO; `set` moves a key to the newest
 * end) with O(1) amortized eviction.
 *
 * Why not `map.delete(map.keys().next().value)` per eviction: V8's ordered
 * hash table leaves a hole for every deleted entry until the next rehash, and
 * a fresh iterator skips every hole from the start, so once the cache is full
 * each eviction costs O(evictions since the last rehash): quadratic. With the
 * 500k-output cache that was the agent's ~30 s event-loop stall after every
 * large commit in the G1 lab (docs/clickhouse-port/g1-fix-pass-2.md §1). Here
 * one iterator is kept alive across calls: Map iterators continue across
 * insertions, deletions and rehashes, every entry before it has been evicted,
 * and re-set keys are appended after it, so each step skips each hole at most
 * once.
 */
export class RecentCache<Key, Value> {
  readonly capacity: number;

  private readonly entries = new Map<Key, Value>();

  private cursor: IterableIterator<Key>;

  constructor(capacity: number) {
    this.capacity = capacity;
    this.cursor = this.entries.keys();
  }

  get size() {
    return this.entries.size;
  }

  get(key: Key): Value | undefined {
    return this.entries.get(key);
  }

  has(key: Key) {
    return this.entries.has(key);
  }

  /** Insert or refresh `key` as the newest entry; evict the oldest beyond capacity. */
  set(key: Key, value: Value) {
    this.entries.delete(key);
    this.entries.set(key, value);
    // eslint-disable-next-line functional/no-loop-statement
    while (this.entries.size > this.capacity) {
      this.evictOldest();
    }
  }

  delete(key: Key) {
    return this.entries.delete(key);
  }

  private evictOldest() {
    const next = this.cursor.next();
    if (next.done !== true) {
      this.entries.delete(next.value);
      return;
    }
    // only after the cache was emptied: start over
    this.cursor = this.entries.keys();
    const first = this.cursor.next();
    if (first.done !== true) {
      this.entries.delete(first.value);
    }
  }
}
