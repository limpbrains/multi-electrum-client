// Microtask coalescer.
//
// Calls enqueued within the same microtask are flushed together on the next
// microtask boundary. The Manager uses this so `Promise.all([m.call(...),
// m.call(...)])` becomes a single JSON-RPC batch when the policy picks the
// same client for every item. Zero added latency (no setTimeout) and tiny
// surface — flush is invoked once with the buffered items.

export class MicrotaskBatcher<T> {
  private queue: T[] = [];
  private scheduled = false;
  private readonly flush: (items: T[]) => void;

  constructor(flush: (items: T[]) => void) {
    this.flush = flush;
  }

  enqueue(item: T): void {
    this.queue.push(item);
    if (!this.scheduled) {
      this.scheduled = true;
      queueMicrotask(() => this.run());
    }
  }

  /** Test/diagnostic only. Pending items remain queued. */
  pendingCount(): number {
    return this.queue.length;
  }

  private run(): void {
    const items = this.queue;
    this.queue = [];
    this.scheduled = false;
    this.flush(items);
  }
}
