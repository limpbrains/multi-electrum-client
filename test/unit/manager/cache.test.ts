import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it, vi } from 'vitest';

import { MemoryCache } from '../../../src/cache/memory.js';
import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [{ id: 'a', host: 'a', port: 50001, protocol: 'ws' }];

/** Settle the internal headers subscription on first connect. */
async function startWithTip(
  h: ReturnType<typeof buildHarness>,
  manager: ElectrumManager,
  height: number,
): Promise<void> {
  // start() awaits the headers subscription wire round-trip — drive the
  // reply concurrently rather than serially.
  const startPromise = manager.start();
  // Wait for connect + subscribe wire send to land in `sent`.
  await delay(0);
  await delay(0);
  await delay(0);
  h.reply('a', (req: { id: number; method: string }) =>
    req.method === 'blockchain.headers.subscribe'
      ? { id: req.id, result: { height, hex: '00' } }
      : undefined,
  );
  await startPromise;
  await delay(0);
}

describe('Manager cache — finality-gated writes', () => {
  it('hits cache on second call to blockchain.block.header (finalized)', async () => {
    const h = buildHarness();
    const cache = new MemoryCache();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
      finalizedConfs: 6,
    });
    await startWithTip(h, manager, 100);

    // First call: miss → wire → write to cache.
    const p1 = manager.call('blockchain.block.header', [50]);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.block.header' ? { id: req.id, result: 'HEADER_HEX' } : undefined,
    );
    expect(await p1).toBe('HEADER_HEX');
    // Allow fire-and-forget cache write.
    await delay(0);
    expect(await cache.get('et:regtest:v1:hdr:32')).toBe('"HEADER_HEX"');

    // Second call: hit → no wire request.
    const sentBefore = h.transports.get('a')!.sent.length;
    const p2 = manager.call('blockchain.block.header', [50]);
    expect(await p2).toBe('HEADER_HEX');
    expect(h.transports.get('a')!.sent.length).toBe(sentBefore);

    await manager.stop();
  });

  it('does not write when the requested height is not yet finalized', async () => {
    const h = buildHarness();
    const cache = new MemoryCache();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
      finalizedConfs: 6,
    });
    await startWithTip(h, manager, 100);

    // tip=100, height=99: only 1 conf, < 6.
    const p1 = manager.call('blockchain.block.header', [99]);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.block.header' ? { id: req.id, result: 'NEAR_TIP' } : undefined,
    );
    expect(await p1).toBe('NEAR_TIP');
    await delay(0);
    expect(await cache.get('et:regtest:v1:hdr:63')).toBeNull();

    await manager.stop();
  });

  it('skips cache entirely when bypassCache is set', async () => {
    const h = buildHarness();
    const cache = new MemoryCache();
    // Pre-populate cache so a hit would be visible.
    await cache.set('et:regtest:v1:hdr:32', '"FROM_CACHE"');
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
      finalizedConfs: 6,
    });
    await startWithTip(h, manager, 100);

    const p = manager.call('blockchain.block.header', [50], { bypassCache: true });
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.block.header' ? { id: req.id, result: 'FROM_WIRE' } : undefined,
    );
    expect(await p).toBe('FROM_WIRE');
    // Cache was not written despite finality.
    await delay(0);
    expect(await cache.get('et:regtest:v1:hdr:32')).toBe('"FROM_CACHE"');

    await manager.stop();
  });

  it('caches blockchain.transaction.get_merkle when finalized', async () => {
    const h = buildHarness();
    const cache = new MemoryCache();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
      finalizedConfs: 6,
    });
    await startWithTip(h, manager, 100);

    const proof = { block_height: 50, pos: 3, merkle: ['aa', 'bb'] };
    const p = manager.call('blockchain.transaction.get_merkle', ['txid', 50]);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.transaction.get_merkle'
        ? { id: req.id, result: proof }
        : undefined,
    );
    expect(await p).toEqual(proof);
    await delay(0);
    expect(JSON.parse((await cache.get('et:regtest:v1:mrk:txid:32'))!)).toEqual(proof);

    await manager.stop();
  });

  it('does not cache non-allowlisted methods', async () => {
    const h = buildHarness();
    const cache = {
      get: vi.fn(async () => null),
      set: vi.fn(async () => undefined),
      del: vi.fn(async () => undefined),
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
      finalizedConfs: 6,
    });
    await startWithTip(h, manager, 100);
    cache.get.mockClear();
    cache.set.mockClear();

    const p = manager.call('blockchain.scripthash.get_balance', ['hash']);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.get_balance'
        ? { id: req.id, result: { confirmed: 0, unconfirmed: 0 } }
        : undefined,
    );
    await p;
    await delay(0);
    expect(cache.get).not.toHaveBeenCalled();
    expect(cache.set).not.toHaveBeenCalled();

    await manager.stop();
  });

  it('skips cache writes when tip is unknown (defensive default)', async () => {
    const h = buildHarness();
    const cache = new MemoryCache();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
      finalizedConfs: 6,
    });
    // Start without delivering an initial header — tip stays undefined.
    void manager.start();
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      // Reject the headers subscribe so tipHeight stays undefined.
      req.method === 'blockchain.headers.subscribe'
        ? { id: req.id, error: { code: -32603, message: 'no headers' } }
        : undefined,
    );
    await delay(0);

    const p = manager.call('blockchain.block.header', [50]);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.block.header' ? { id: req.id, result: 'X' } : undefined,
    );
    expect(await p).toBe('X');
    await delay(0);
    expect(await cache.get('et:regtest:v1:hdr:32')).toBeNull();

    await manager.stop();
  });
});
describe('Manager cache — tip install retry', () => {
  it('a trigger arriving during a failing install queues one retry', async () => {
    // Both servers connect during start(); each 'connected' transition
    // triggers installTipSubscription. The first becomes the leader; a
    // trigger landing while the leader's wire call is in flight must
    // not be discarded — if the leader then fails non-retryably, that
    // dropped trigger was the only retry, and with no further
    // connection transition the session runs forever with no tip and
    // every cache write silently disabled.
    const h = buildHarness();
    const cache = new MemoryCache();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [
        { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
        { id: 'b', host: 'b', port: 50001, protocol: 'ws' },
      ],
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
      finalizedConfs: 6,
    });
    let subAttempts = 0;
    const startPromise = manager.start();
    await delay(0);
    await delay(0);
    // Leader's subscribe fails non-retryably.
    h.reply('a', (req: { id: number; method: string }) => {
      if (req.method !== 'blockchain.headers.subscribe') return undefined;
      subAttempts++;
      return { id: req.id, error: { code: -32601, message: 'unknown method' } };
    });
    await delay(0);
    await delay(0);
    // The queued retry re-attempts; answer it with a real tip.
    h.reply('a', (req: { id: number; method: string }) => {
      if (req.method !== 'blockchain.headers.subscribe') return undefined;
      subAttempts++;
      return { id: req.id, result: { height: 100, hex: '00' } };
    });
    await startPromise;
    await delay(0);
    expect(subAttempts).toBeGreaterThanOrEqual(2);

    // The tip took: finalized writes are enabled.
    const p = manager.call('blockchain.block.header', [50]);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.block.header' ? { id: req.id, result: 'H' } : undefined,
    );
    expect(await p).toBe('H');
    await delay(0);
    expect(await cache.get('et:regtest:v1:hdr:32')).toBe('"H"');

    await manager.stop();
  });
});
