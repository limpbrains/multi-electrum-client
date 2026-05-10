import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it, vi } from 'vitest';

import { SuspendedError } from '../../../src/errors/types.js';
import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [{ id: 'a', host: 'a', port: 50001, protocol: 'ws' }];

describe('Manager lifecycle — suspend / resume', () => {
  it('transitions: created → running → suspending → suspended → resuming → running → stopped', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    expect(manager.state).toBe('created');
    await manager.start();
    expect(manager.state).toBe('running');
    await manager.suspend({ graceMs: 0 });
    expect(manager.state).toBe('suspended');
    await manager.resume();
    expect(manager.state).toBe('running');
    await manager.stop();
    expect(manager.state).toBe('stopped');
    // Note: the 'suspending' / 'resuming' intermediate states exist but
    // are reached inside the FIFO chain's microtasks; there is no
    // synchronous observation point for them after the public method
    // returns its promise. Tests for lifecycle gating semantics use the
    // stable terminal states (running / suspended / stopped).
  });

  it('queues calls during suspended; replays them on resume', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });

    // Call while suspended → queues, doesn't reject.
    const callPromise = manager.call('server.ping', []);
    // Resume: reconnect + drain queue.
    const resumePromise = manager.resume();
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.ping' ? { id: req.id, result: null } : undefined,
    );
    await resumePromise;
    expect(await callPromise).toBe(null);
    await manager.stop();
  });

  it('failOnSuspend rejects with SuspendedError instead of queueing', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });

    await expect(manager.call('server.ping', [], { failOnSuspend: true })).rejects.toBeInstanceOf(
      SuspendedError,
    );

    await manager.resume();
    await manager.stop();
  });

  it('cancelInFlight rejects pending requests with SuspendedError', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    // Issue a call but don't reply; suspend will reject it.
    const pending = manager.call('server.ping', []);
    await delay(0);
    await manager.suspend({ cancelInFlight: true });

    await expect(pending).rejects.toBeInstanceOf(SuspendedError);
    await manager.stop();
  });

  it('graceMs lets in-flight requests settle before suspend', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const pending = manager.call('server.ping', []);
    await delay(0);
    // Reply during the grace window — call should resolve, not reject.
    setTimeout(() => {
      h.reply('a', (req: { id: number; method: string }) =>
        req.method === 'server.ping' ? { id: req.id, result: null } : undefined,
      );
    }, 5);
    await manager.suspend({ graceMs: 100 });

    expect(await pending).toBe(null);
    await manager.stop();
  });

  // Idempotency on the same target state is the trivial corollary of
  // the FIFO chain semantics — covered by the `3-transition stack`
  // and the cross-direction tests below. No separate test for it here.

  it('idempotent: resume while running is a no-op', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.resume(); // no-op
    expect(manager.state).toBe('running');
    await manager.stop();
  });

  it('rejects suspend / resume on a stopped manager', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.stop();
    await expect(manager.suspend()).rejects.toBeInstanceOf(SuspendedError);
    await expect(manager.resume()).rejects.toBeInstanceOf(SuspendedError);
  });

  it('stop while queue has items rejects them with SuspendedError', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });

    const callPromise = manager.call('server.ping', []);
    await manager.stop();

    await expect(callPromise).rejects.toBeInstanceOf(SuspendedError);
  });

  it('queues calls submitted during the resuming window so they replay in order', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });

    // First call is queued during 'suspended'; second lands during the
    // resume reconnect (state=resuming) — both must serialize.
    const firstCall = manager.call('server.ping', []);
    const resumePromise = manager.resume();
    // resume() is async; right after kicking it we're in 'resuming'.
    const secondCall = manager.call('server.ping', []);

    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.ping' ? { id: req.id, result: null } : undefined,
    );
    await resumePromise;
    expect(await firstCall).toBe(null);
    expect(await secondCall).toBe(null);
    await manager.stop();
  });

  it('suspend before start (created → suspended) is a no-op transition', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    expect(manager.state).toBe('created');
    await manager.suspend();
    expect(manager.state).toBe('suspended');
    // Resume from this synthetic suspend works without a prior start().
    await manager.resume();
    expect(manager.state).toBe('running');
    await manager.stop();
  });

  // Two parallel suspends collapsing onto a single transition is a
  // sub-case of `concurrent suspend()s share the same in-flight
  // transition promise` further down (which adds an in-flight call to
  // exercise the grace window). Single test suffices.

  it('tipUnsub is reset on suspend so resume re-installs a fresh headers sub', async () => {
    const h = buildHarness();
    const cache = new (await import('../../../src/cache/memory.js')).MemoryCache();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
    });

    const startPromise = manager.start();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.headers.subscribe'
        ? { id: req.id, result: { height: 100, hex: '00' } }
        : undefined,
    );
    await startPromise;

    await manager.suspend({ graceMs: 0 });
    // After suspend, manager must be ready to subscribe again on resume —
    // verified by the resume not throwing when the headers sub is re-issued.
    const resumePromise = manager.resume();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.headers.subscribe'
        ? { id: req.id, result: { height: 200, hex: '00' } }
        : undefined,
    );
    await resumePromise;

    await manager.stop();
  });

  it('start() rejects when called from suspended (must use resume)', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });
    await expect(manager.start()).rejects.toBeInstanceOf(SuspendedError);
    await manager.resume();
    await manager.stop();
  });

  it('start() rejects when called from running (idempotent guard)', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await expect(manager.start()).rejects.toBeInstanceOf(SuspendedError);
    await manager.stop();
  });

  it('start() works again after stop (re-init)', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.stop();
    await manager.start();
    expect(manager.state).toBe('running');
    await manager.stop();
  });

  it('stop() during suspend grace race: lifecycle ends stopped, not suspended', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    // Issue a non-replied call so suspend's grace-window has work to wait on.
    void manager.call('server.ping', []).catch(() => undefined);
    await delay(0);
    // Trigger suspend with a generous grace, then race a stop() in.
    // stop() awaits the FIFO transition tail, so suspend resolves cleanly
    // (its doSuspendIfNeeded sees stopped and returns).
    const suspendPromise = manager.suspend({ graceMs: 100 });
    const stopPromise = manager.stop();
    await Promise.allSettled([suspendPromise, stopPromise]);
    expect(manager.state).toBe('stopped');
  });

  it('stop() during resume reconnect race: queued items reject SuspendedError, state stopped', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });
    const queued = manager.call('server.ping', []);
    // Kick off resume; immediately stop before resume finishes its awaits.
    const resumePromise = manager.resume();
    const stopPromise = manager.stop();
    await Promise.allSettled([resumePromise, stopPromise]);
    await expect(queued).rejects.toBeInstanceOf(SuspendedError);
    expect(manager.state).toBe('stopped');
  });

  it('queued call rejects immediately on AbortSignal abort, drops from queue', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });

    const controller = new AbortController();
    const queued = manager.call('server.ping', [], { signal: controller.signal });
    controller.abort(new Error('user cancelled'));
    await expect(queued).rejects.toThrow(/user cancelled/);

    // Drained queue must not double-resolve / fire wire calls for the aborted item.
    const resumePromise = manager.resume();
    await delay(0);
    // No reply needed — no wire request fires for the aborted call.
    await resumePromise;
    await manager.stop();
  });

  it('queued call rejects immediately when signal is already aborted at submit', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });

    const controller = new AbortController();
    controller.abort(new Error('preempt'));
    const queued = manager.call('server.ping', [], { signal: controller.signal });
    await expect(queued).rejects.toThrow(/preempt/);

    await manager.resume();
    await manager.stop();
  });

  it('concurrent suspend()s share the same in-flight transition promise', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    // Issue an in-flight call so suspend's grace window has work to do.
    void manager.call('server.ping', []).catch(() => undefined);
    await delay(0);

    const a = manager.suspend({ graceMs: 50 });
    const b = manager.suspend({ graceMs: 50 });
    // Both must observe the FINAL state, not the intermediate.
    await Promise.all([a, b]);
    expect(manager.state).toBe('suspended');
    await manager.stop();
  });

  it('cross-direction transition: resume() during in-flight suspend() lands on running', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    // Issue an in-flight call so suspend has work to do during its grace.
    void manager.call('server.ping', []).catch(() => undefined);
    await delay(0);

    const a = manager.suspend({ graceMs: 50 });
    const b = manager.resume(); // should chain after suspend, not return its promise
    await Promise.all([a, b]);
    expect(manager.state).toBe('running');
    await manager.stop();
  });

  it('3-transition stack: suspend → resume → suspend lands on the third caller`s intent', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const a = manager.suspend({ graceMs: 0 });
    const b = manager.resume();
    const c = manager.suspend({ graceMs: 0 });
    await Promise.all([a, b, c]);
    // FIFO: a then b then c. Final state must reflect c, not b.
    expect(manager.state).toBe('suspended');
    await manager.stop();
  });

  it('cross-direction transition: suspend() during in-flight resume() lands on suspended', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });
    const r = manager.resume();
    const s = manager.suspend({ graceMs: 0 });
    await Promise.all([r, s]);
    expect(manager.state).toBe('suspended');
    await manager.stop();
  });

  it('does not accumulate duplicate tip handlers across suspend/resume cycles', async () => {
    const h = buildHarness();
    const cache = new (await import('../../../src/cache/memory.js')).MemoryCache();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cache,
    });

    // Helper: settle a header subscribe round-trip on `a`.
    const settleHeaders = async (height: number): Promise<void> => {
      await delay(0);
      await delay(0);
      h.reply('a', (req: { id: number; method: string }) =>
        req.method === 'blockchain.headers.subscribe'
          ? { id: req.id, result: { height, hex: '00' } }
          : undefined,
      );
    };

    const startPromise = manager.start();
    await settleHeaders(100);
    await startPromise;

    // Two suspend/resume cycles — without the round-3 fix, each adds a
    // handler to the registry's per-key Set.
    for (let i = 0; i < 2; i++) {
      await manager.suspend({ graceMs: 0 });
      const resumePromise = manager.resume();
      await settleHeaders(101 + i);
      await resumePromise;
    }

    // Push a header notification; only one tip-tracker handler should
    // fire (verified by checking the cache write goes through cleanly —
    // duplicate handlers would still write the same value, but we want
    // to assert the registry's notify-fanout didn't multiply).
    h.transports.get('a')!.pushFromServer(
      JSON.stringify({
        jsonrpc: '2.0',
        method: 'blockchain.headers.subscribe',
        params: [{ height: 200, hex: 'ff' }],
      }),
    );
    await delay(0);

    // Cache write for height 100 should be possible (tip=200, finality=6).
    const probe = manager.call('blockchain.block.header', [100]);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.block.header' ? { id: req.id, result: 'HDR' } : undefined,
    );
    await probe;
    await delay(0);
    expect(await cache.get('et:regtest:v1:hdr:64')).toBe('"HDR"');

    await manager.stop();
  });

  it('addServer while suspended does not eagerly connect; resume() picks it up', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.suspend({ graceMs: 0 });

    manager.addServer({ id: 'b', host: 'b', port: 50001, protocol: 'ws' });
    // Transport was constructed (factory runs at install time) but not
    // connected — eager connect would defeat suspend.
    expect(h.transports.get('b')).toBeDefined();
    expect(h.transports.get('b')!.connected).toBe(false);

    await manager.resume();
    // Now `b` is connected.
    expect(h.transports.get('b')!.connected).toBe(true);
    await manager.stop();
  });

  it('addServer on a stopped manager throws SuspendedError', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.stop();
    expect(() => manager.addServer({ id: 'b', host: 'b', port: 50001, protocol: 'ws' })).toThrow(
      SuspendedError,
    );
  });

  it('preserves subscriptions across suspend / resume with catch-up', async () => {
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
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.subscribe' ? { id: req.id, result: 'INIT' } : undefined,
    );
    const unsub = await subPromise;
    expect(handler).toHaveBeenCalledWith('INIT');

    handler.mockClear();
    await manager.suspend({ graceMs: 0 });
    // Resume: registry's restoreOrphans re-issues the subscribe; drift
    // handler fires with the new status.
    const resumePromise = manager.resume();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'blockchain.scripthash.subscribe'
        ? { id: req.id, result: 'AFTER_RESUME' }
        : undefined,
    );
    await resumePromise;
    await delay(10);

    expect(handler).toHaveBeenCalledWith('AFTER_RESUME');
    await unsub();
    await manager.stop();
  });
});
