import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it, vi } from 'vitest';

import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [{ id: 'a', host: 'a', port: 50001, protocol: 'ws' }];

describe('Manager peer discovery', () => {
  it('does not probe a banned peer — discovery is new work, not cleanup', async () => {
    // The probe is strictly addressed (results must be attributed to
    // the probed peer), but strict addressing is not permission to
    // bypass a rate-limit ban: server.peers.subscribe is new work, and
    // probing a peer that just throttled us prolongs the throttling.
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: { enabled: true, intervalMs: 20 },
    });
    await manager.start();
    await delay(0);
    await delay(0);
    let probes = 0;
    h.reply('a', (req: { id: number; method: string }) => {
      if (req.method !== 'server.peers.subscribe') return undefined;
      probes++;
      return { id: req.id, result: [] };
    });
    expect(probes).toBe(1);

    // Rate-limit ban lands on `a`.
    const ping = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.ping'
        ? { id: req.id, error: { code: -32603, message: 'excessive resource usage' } }
        : undefined,
    );
    await expect(ping).rejects.toThrow();

    // Let the re-poll timer fire during the cooldown.
    await delay(60);
    h.reply('a', (req: { id: number; method: string }) => {
      if (req.method !== 'server.peers.subscribe') return undefined;
      probes++;
      return { id: req.id, result: [] };
    });
    expect(probes).toBe(1);

    await manager.stop();
  });

  it('a probe refused during a cooldown does not end discovery — it resumes after the ban', async () => {
    // The refused probe must still re-arm the poll timer: the only
    // other re-kick is a fresh 'connected' transition, and a ban does
    // not disconnect — without the re-arm, one unlucky poll during a
    // cooldown would silently end discovery for the life of the socket.
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      cooldownMs: 50,
      discover: { enabled: true, intervalMs: 20 },
    });
    await manager.start();
    await delay(0);
    await delay(0);
    let probes = 0;
    const answerProbe = (req: { id: number; method: string }) => {
      if (req.method !== 'server.peers.subscribe') return undefined;
      probes++;
      return { id: req.id, result: [] };
    };
    h.reply('a', answerProbe);
    expect(probes).toBe(1);

    const ping = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.ping'
        ? { id: req.id, error: { code: -32603, message: 'excessive resource usage' } }
        : undefined,
    );
    await expect(ping).rejects.toThrow();

    // Poll timers fire during the 50ms cooldown (refused), then after
    // it expires the next poll must go out.
    for (let i = 0; i < 30 && probes < 2; i++) {
      await delay(10);
      h.reply('a', answerProbe);
    }
    expect(probes).toBeGreaterThanOrEqual(2);

    await manager.stop();
  });

  it('admits ws/wss peers from server.peers.subscribe', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: { enabled: true, intervalMs: 0 },
    });
    await manager.start();
    // Wait for state-change → discoverFromClient to send the wire request.
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.peers.subscribe'
        ? {
            id: req.id,
            result: [
              ['peer1.example.com', '1.1.1.1', ['v1.4', 'ws:50001']],
              ['peer2.example.com', '2.2.2.2', ['v1.4', 'wss:50002']],
              // No ws/wss features — must be skipped.
              ['skipme.example.com', '3.3.3.3', ['v1.4', 's50002', 't50001']],
            ],
          }
        : undefined,
    );
    await delay(10);

    // Both peers admitted; addServer fired connect attempts. Harness keys
    // transports by endpoint.host, not spec id.
    expect(h.transports.get('peer1.example.com')).toBeDefined();
    expect(h.transports.get('peer2.example.com')).toBeDefined();
    expect(h.transports.get('skipme.example.com')).toBeUndefined();

    await manager.stop();
  });

  it('runs onDiscover and skips false returns', async () => {
    const h = buildHarness();
    const onDiscover = vi.fn(async (peer: ServerSpec) => peer.host === 'allowed.example.com');
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: { enabled: true, intervalMs: 0, onDiscover },
    });
    await manager.start();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.peers.subscribe'
        ? {
            id: req.id,
            result: [
              ['allowed.example.com', '', ['ws:5001']],
              ['blocked.example.com', '', ['ws:5002']],
            ],
          }
        : undefined,
    );
    await delay(10);

    expect(onDiscover).toHaveBeenCalledTimes(2);
    expect(h.transports.get('allowed.example.com')).toBeDefined();
    expect(h.transports.get('blocked.example.com')).toBeUndefined();

    await manager.stop();
  });

  it('thrown onDiscover surfaces on error event and skips the candidate', async () => {
    const h = buildHarness();
    const errors: unknown[] = [];
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: {
        enabled: true,
        intervalMs: 0,
        onDiscover: () => {
          throw new Error('callback boom');
        },
      },
    });
    manager.on('error', (e) => errors.push(e));
    await manager.start();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.peers.subscribe'
        ? { id: req.id, result: [['boom.example.com', '', ['ws:5001']]] }
        : undefined,
    );
    await delay(10);

    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe('callback boom');
    expect(h.transports.get('boom.example.com')).toBeUndefined();

    await manager.stop();
  });

  it('skips peers already in the pool', async () => {
    const h = buildHarness();
    const onDiscover = vi.fn(async () => true);
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [
        { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
        // Pre-existing peer with the same id we'd build for the discovered one.
        { id: 'peer1.example.com:5001', host: 'peer1.example.com', port: 5001, protocol: 'ws' },
      ],
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: { enabled: true, intervalMs: 0, onDiscover },
    });
    await manager.start();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.peers.subscribe'
        ? { id: req.id, result: [['peer1.example.com', '', ['ws:5001']]] }
        : undefined,
    );
    await delay(10);

    // onDiscover never invoked for already-known peer.
    expect(onDiscover).not.toHaveBeenCalled();

    await manager.stop();
  });

  it('swallows server.peers.subscribe rpc errors silently', async () => {
    const h = buildHarness();
    const errors: unknown[] = [];
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: { enabled: true, intervalMs: 0 },
    });
    manager.on('error', (e) => errors.push(e));
    await manager.start();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.peers.subscribe'
        ? { id: req.id, error: { code: -32601, message: 'method not found' } }
        : undefined,
    );
    await delay(10);

    // discovery is best-effort — rpc errors don't surface.
    expect(errors).toHaveLength(0);

    await manager.stop();
  });

  it('does not poll when discover.enabled is false', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: { enabled: false },
    });
    await manager.start();
    await delay(0);
    await delay(0);
    // No server.peers.subscribe in the queue.
    const sent = h.transports.get('a')!.sent;
    expect(sent.some((s) => s.includes('server.peers.subscribe'))).toBe(false);
    await manager.stop();
  });

  it('a peer approved after suspend() is not admitted', async () => {
    // The post-await guard only asked whether the manager was STOPPED,
    // which is false during a suspend — so a slow onDiscover resolving
    // after `await suspend()` returned added a server to a pool that was
    // supposed to be quiescent, and resume() then connected it.
    const h = buildHarness();
    let approve: (() => void) | undefined;
    const onDiscover = async (): Promise<boolean> => {
      await new Promise<void>((r) => {
        approve = r;
      });
      return true;
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      discover: { enabled: true, intervalMs: 0, onDiscover },
    });
    await manager.start();
    await delay(0);
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.peers.subscribe'
        ? { id: req.id, result: [['late.example.com', '', ['ws:5001']]] }
        : undefined,
    );
    await delay(10);
    expect(approve).toBeDefined();

    await manager.suspend();
    approve!();
    await delay(10);

    expect(h.transports.get('late.example.com')).toBeUndefined();
    await manager.resume();
    await delay(10);
    expect(h.transports.get('late.example.com')).toBeUndefined();
    await manager.stop();
  });
});
