import { describe, expect, it } from 'vitest';

import { MicrotaskBatcher } from '../../../src/util/microtask-batcher.js';

describe('MicrotaskBatcher', () => {
  it('flushes all items enqueued in the same microtask together', async () => {
    const flushed: number[][] = [];
    const b = new MicrotaskBatcher<number>((items) => flushed.push([...items]));
    b.enqueue(1);
    b.enqueue(2);
    b.enqueue(3);
    await Promise.resolve();
    expect(flushed).toEqual([[1, 2, 3]]);
  });

  it('separates flushes across microtask boundaries', async () => {
    const flushed: number[][] = [];
    const b = new MicrotaskBatcher<number>((items) => flushed.push([...items]));
    b.enqueue(1);
    await Promise.resolve();
    b.enqueue(2);
    b.enqueue(3);
    await Promise.resolve();
    expect(flushed).toEqual([[1], [2, 3]]);
  });

  it('schedules only one flush per pending burst', async () => {
    let flushes = 0;
    const b = new MicrotaskBatcher<number>(() => {
      flushes++;
    });
    for (let i = 0; i < 50; i++) b.enqueue(i);
    await Promise.resolve();
    expect(flushes).toBe(1);
  });
});
