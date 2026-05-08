import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { MemoryCache } from '../../src/cache/memory.js';
import {
  ProtocolError,
  RpcError,
  SuspendedError,
  TimeoutError,
  TransportError,
} from '../../src/errors/types.js';

describe('MemoryCache', () => {
  it('round-trips a value', async () => {
    const cache = new MemoryCache();
    await cache.set('k', 'v');
    expect(await cache.get('k')).toBe('v');
  });

  it('returns null for missing keys', async () => {
    const cache = new MemoryCache();
    expect(await cache.get('missing')).toBeNull();
  });

  it('expires values past ttl', async () => {
    const cache = new MemoryCache();
    await cache.set('k', 'v', 1);
    await delay(5);
    expect(await cache.get('k')).toBeNull();
  });

  it('deletes', async () => {
    const cache = new MemoryCache();
    await cache.set('k', 'v');
    await cache.del('k');
    expect(await cache.get('k')).toBeNull();
  });
});

describe('error classes', () => {
  it('preserve name and message', () => {
    const cases = [new SuspendedError('s'), new TimeoutError('t'), new ProtocolError('p')] as const;
    for (const e of cases) {
      expect(e).toBeInstanceOf(Error);
      expect(e.message).toMatch(/.+/);
    }
  });

  it('TransportError carries cause', () => {
    const cause = new Error('inner');
    const err = new TransportError('outer', cause);
    expect(err.cause).toBe(cause);
    expect(err.name).toBe('TransportError');
  });

  it('RpcError carries code + data', () => {
    const err = new RpcError('boom', -32603, { detail: 'bad' });
    expect(err.code).toBe(-32603);
    expect(err.data).toEqual({ detail: 'bad' });
  });
});
