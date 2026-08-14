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

describe('Manager subscriptions — failover binding', () => {
  it('binds to the server that actually answered when subscribe fails over, and unsubscribes there', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const handler = vi.fn();
    const subPromise = manager.scripthash.subscribe('HASH', handler);
    await delay(0);
    // Preferred server `a` rejects the subscribe with a retryable
    // (rate-limit-class) error → the retry pipeline fails over to `b`,
    // which answers. The record must bind to `b`.
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.subscribe'
        ? { id: req.id, error: { code: -32603, message: 'excessive resource usage' } }
        : undefined,
    );
    await delay(0);
    h.reply('b', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.subscribe'
        ? { id: req.id, result: 'STATUS_B' }
        : undefined,
    );
    const unsub = await subPromise;
    expect(handler).toHaveBeenCalledWith('STATUS_B');

    // A push from `b` (the real owner) must reach the handler.
    h.transports
      .get('b')!
      .pushFromServer(
        '{"jsonrpc":"2.0","method":"blockchain.scripthash.subscribe","params":["HASH","STATUS_B2"]}',
      );
    expect(handler).toHaveBeenCalledWith('STATUS_B2');

    // The wire unsubscribe must go to `b`, not the preferred `a`.
    await unsub();
    await delay(0);
    const bUnsubs = h.transports
      .get('b')!
      .sent.filter(
        (s) =>
          (JSON.parse(s) as { method?: string }).method === 'blockchain.scripthash.unsubscribe',
      );
    expect(bUnsubs).toHaveLength(1);
    const aUnsubs = h.transports
      .get('a')!
      .sent.filter(
        (s) =>
          (JSON.parse(s) as { method?: string }).method === 'blockchain.scripthash.unsubscribe',
      );
    expect(aUnsubs).toHaveLength(0);

    await manager.stop();
  });
});

describe('Manager subscriptions — ban vs socket ownership', () => {
  it('binds to the answering server even when a ban landed while the response was in flight', async () => {
    // A ban gates ROUTING of new calls; it does not disconnect the
    // socket or cancel a wire subscription the server already accepted.
    // Sequence: subscribe is in flight on `a`; a concurrent call gets a
    // rate-limit error, banning `a`; then the subscribe response lands.
    // The record must still bind to `a` — orphaning it silences a live
    // subscription (pushes only buffer for an orphan) for the whole ban,
    // and in a single-server pool nobody else can pick it up.
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [{ id: 'a', host: 'a', port: 50001, protocol: 'ws' }],
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const handler = vi.fn();
    const subPromise = manager.scripthash.subscribe('HASH', handler);
    await delay(0);
    // h.reply drains the whole sent-queue, so grab the subscribe
    // request's id now — its response is pushed by hand after the ban.
    const subReq = h.transports
      .get('a')!
      .sent.map((s) => JSON.parse(s) as { id: number; method: string })
      .find((r) => r.method === 'blockchain.scripthash.subscribe')!;

    const banned = vi.fn();
    manager.on('client-banned', banned);
    const ping = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.ping'
        ? { id: req.id, error: { code: -32603, message: 'excessive resource usage' } }
        : undefined,
    );
    await expect(ping).rejects.toThrow();
    // The guard is only exercising the fixed path if the error really
    // banned `a` — otherwise binding succeeds trivially.
    expect(banned).toHaveBeenCalledWith(expect.objectContaining({ clientId: 'a' }));

    // Ban is in force; now the subscribe response arrives.
    h.transports
      .get('a')!
      .pushFromServer(JSON.stringify({ jsonrpc: '2.0', id: subReq.id, result: 'S1' }));
    await subPromise;
    expect(handler).toHaveBeenCalledWith('S1');

    // Pushes from `a` reach the handler — the subscription is live.
    h.transports
      .get('a')!
      .pushFromServer(
        '{"jsonrpc":"2.0","method":"blockchain.scripthash.subscribe","params":["HASH","S2"]}',
      );
    expect(handler).toHaveBeenLastCalledWith('S2');

    await manager.stop();
  });
});

describe('Manager subscriptions — pinned unsubscribe under a ban', () => {
  it('sends the wire unsubscribe to the banned owner, never to another server', async () => {
    // The wire unsubscribe is strictly pinned: it addresses protocol
    // state on one specific connection. Falling through to policy.pick
    // would deliver it to a server with no such subscription — and if
    // the caller re-subscribes there, the misrouted unsubscribe can
    // land after the fresh subscribe and silently kill it. A ban must
    // not break the pin: it gates picking servers for new work, not
    // addressing the one that owns the subscription.
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const handler = vi.fn();
    const subPromise = manager.scripthash.subscribe('HASH', handler);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.subscribe' ? { id: req.id, result: 'S1' } : undefined,
    );
    const unsub = await subPromise;

    // Ban `a` (the owner) with a rate-limit error on an unrelated call.
    const banned = vi.fn();
    manager.on('client-banned', banned);
    const ping = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.ping'
        ? { id: req.id, error: { code: -32603, message: 'excessive resource usage' } }
        : undefined,
    );
    // Retry fails over to `b`, which also declines — the call fails,
    // but only `a` carries the rate-limit ban.
    await delay(0);
    h.reply('b', (req: { id: number; method: string }) =>
      req.method === 'server.ping'
        ? { id: req.id, error: { code: 1, message: 'unknown method' } }
        : undefined,
    );
    await expect(ping).rejects.toThrow();
    expect(banned).toHaveBeenCalledWith(expect.objectContaining({ clientId: 'a' }));

    await unsub();
    await delay(0);

    const unsubsTo = (host: string): string[] =>
      h.transports
        .get(host)!
        .sent.filter(
          (s) =>
            (JSON.parse(s) as { method?: string }).method === 'blockchain.scripthash.unsubscribe',
        );
    expect(unsubsTo('a')).toHaveLength(1);
    expect(unsubsTo('b')).toHaveLength(0);

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
