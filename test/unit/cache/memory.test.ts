import { describe, expect, it, vi } from 'vitest';

import { MemoryCache } from '../../../src/cache/memory.js';

describe('MemoryCache', () => {
  it('round-trips a value', async () => {
    const c = new MemoryCache();
    await c.set('k', 'v');
    expect(await c.get('k')).toBe('v');
  });

  it('returns null for missing keys', async () => {
    const c = new MemoryCache();
    expect(await c.get('absent')).toBeNull();
  });

  it('expires entries past their ttl', async () => {
    vi.useFakeTimers();
    try {
      const c = new MemoryCache();
      await c.set('k', 'v', 100);
      expect(await c.get('k')).toBe('v');
      vi.advanceTimersByTime(101);
      expect(await c.get('k')).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it('del removes the entry', async () => {
    const c = new MemoryCache();
    await c.set('k', 'v');
    await c.del('k');
    expect(await c.get('k')).toBeNull();
  });
});
