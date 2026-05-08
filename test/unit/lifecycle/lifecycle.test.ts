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
    const resumePromise = manager.resume();
    // Briefly observable as 'resuming' before reconnect resolves.
    expect(manager.state).toBe('resuming');
    await resumePromise;
    expect(manager.state).toBe('running');
    await manager.stop();
    expect(manager.state).toBe('stopped');
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

  it('idempotent: suspend while suspended is a no-op', async () => {
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
    expect(manager.state).toBe('suspended');
    await manager.suspend({ graceMs: 0 });
    expect(manager.state).toBe('suspended');
    await manager.resume();
    await manager.stop();
  });

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
