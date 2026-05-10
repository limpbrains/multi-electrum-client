import { describe, expect, it, vi } from 'vitest';

import { MemoryCache } from '../../../src/cache/memory.js';

describe('MemoryCache', () => {
  it('set / get / del round-trips a value and reports null for absent / deleted keys', async () => {
    const c = new MemoryCache();
    expect(await c.get('absent')).toBeNull();
    await c.set('k', 'v');
    expect(await c.get('k')).toBe('v');
    await c.del('k');
    expect(await c.get('k')).toBeNull();
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
});
