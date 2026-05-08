import { describe, expect, it } from 'vitest';

import { deferred } from '../../../src/util/deferred.js';

describe('deferred', () => {
  it('resolves with the supplied value', async () => {
    const d = deferred<number>();
    d.resolve(42);
    expect(await d.promise).toBe(42);
  });

  it('rejects with the supplied reason', async () => {
    const d = deferred<number>();
    const cause = new Error('nope');
    d.reject(cause);
    await expect(d.promise).rejects.toBe(cause);
  });

  it('first settle wins', async () => {
    const d = deferred<number>();
    d.resolve(1);
    d.resolve(2);
    d.reject(new Error('late'));
    expect(await d.promise).toBe(1);
  });
});
