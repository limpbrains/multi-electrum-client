import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it, vi } from 'vitest';

import { ElectrumManager } from '../../../src/manager.js';
import { failover, roundRobin } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [
  { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
  { id: 'b', host: 'b', port: 50001, protocol: 'ws' },
];

describe('Manager subscriptions — happy path', () => {
  it('scripthash.subscribe fires handler with initial status and pushed updates', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const handler = vi.fn();
    const subPromise = manager.scripthash.subscribe('HASH', handler);
    await delay(0);
    // Server replies with the initial status.
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.subscribe'
        ? { id: req.id, result: 'STATUS_INITIAL' }
        : undefined,
    );

    const unsub = await subPromise;
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith('STATUS_INITIAL');

    // Server pushes a status update on the same wire.
    h.transports.get('a')!.pushFromServer(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'blockchain.scripthash.subscribe',
        params: ['HASH', 'STATUS_NEW'],
      }),
    );
    expect(handler).toHaveBeenCalledTimes(2);
    expect(handler).toHaveBeenLastCalledWith('STATUS_NEW');

    await unsub();
    // Last unsub triggers wire blockchain.scripthash.unsubscribe on the bound client.
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.unsubscribe' ? { id: req.id, result: true } : undefined,
    );
    await manager.stop();
  });

  it('headers.subscribe fires handler on initial tip and on every header push', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const handler = vi.fn();
    const subPromise = manager.headers.subscribe(handler);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.headers.subscribe'
        ? { id: req.id, result: { height: 1, hex: '00' } }
        : undefined,
    );
    const unsub = await subPromise;
    expect(handler).toHaveBeenCalledWith({ height: 1, hex: '00' });

    h.transports.get('a')!.pushFromServer(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'blockchain.headers.subscribe',
        params: [{ height: 2, hex: 'aa' }],
      }),
    );
    expect(handler).toHaveBeenLastCalledWith({ height: 2, hex: 'aa' });

    await unsub();
    await manager.stop();
  });

  it('multi-handler same scripthash shares one wire subscription', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const h1 = vi.fn();
    const h2 = vi.fn();

    const sub1 = manager.scripthash.subscribe('HASH', h1);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'INIT' }));
    const unsub1 = await sub1;

    // Second subscriber should NOT trigger a new wire subscribe call.
    const sentBefore = h.transports.get('a')!.sent.length;
    const sub2 = manager.scripthash.subscribe('HASH', h2);
    const unsub2 = await sub2;
    const sentAfter = h.transports.get('a')!.sent.length;

    expect(sentAfter - sentBefore).toBe(0); // dedup'd
    expect(h1).toHaveBeenCalledWith('INIT');
    expect(h2).toHaveBeenCalledWith('INIT'); // sync from lastKnownStatus

    // Push update — both handlers fire.
    h.transports.get('a')!.pushFromServer(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'blockchain.scripthash.subscribe',
        params: ['HASH', 'NEW'],
      }),
    );
    expect(h1).toHaveBeenLastCalledWith('NEW');
    expect(h2).toHaveBeenLastCalledWith('NEW');

    await unsub1();
    await unsub2();
    await manager.stop();
  });
});

describe('Manager subscriptions — restore on reconnect', () => {
  it('rebinds subscription to another server when bound server disconnects, fires drift notification', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: roundRobin(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    // Listener registered up front so we don't race with the rebind path.
    const restored: { method: string; params: readonly unknown[]; drift: boolean }[] = [];
    manager.on('subscription-restored', (p) => restored.push(p));

    // First subscribe lands on `a` (round-robin first eligible).
    const handler = vi.fn();
    const subPromise = manager.scripthash.subscribe('HASH', handler);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'INIT' }));
    const unsub = await subPromise;
    expect(handler).toHaveBeenCalledWith('INIT');

    // Server `a` drops the connection.
    handler.mockClear();
    h.transports.get('a')!.pushClose(1006, 'abnormal');
    // Let state-change listener fire and restoreOrphans send the rebind to `b`.
    await delay(0);
    await delay(0);
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: 'AFTER' }));
    // Settle the restore promise chain so the synthetic notify + emit run.
    await delay(10);

    expect(handler).toHaveBeenCalledWith('AFTER');
    expect(restored).toEqual([
      {
        method: 'blockchain.scripthash.subscribe',
        params: ['HASH'],
        drift: true,
      },
    ]);

    await unsub();
    await manager.stop();
  });
});
