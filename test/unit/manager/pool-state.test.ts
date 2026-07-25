// Aggregate pool-state events: online / degraded / offline transitions,
// the post-start baseline guarantee, ban-expiry recovery via the internal
// timer, and suspend/resume suppression semantics.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ElectrumManager, type PoolState } from '../../../src/manager.js';
import { roundRobin } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [
  { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
  { id: 'b', host: 'b', port: 50001, protocol: 'ws' },
];

/** Run all queued microtasks. Loops so chained `then` settles too. */
async function flush(): Promise<void> {
  for (let i = 0; i < 8; i++) await Promise.resolve();
}

function build(servers: ServerSpec[] = SERVERS) {
  const h = buildHarness();
  const manager = new ElectrumManager({
    network: 'regtest',
    servers,
    policy: roundRobin(),
    transportFactory: h.factory,
    autoBatch: false,
    cooldownMs: 5000,
    reconnectBackoff: { minMs: 100, maxMs: 1000, factor: 2, jitter: 0 },
  });
  const events: PoolState[] = [];
  manager.on('pool-state', (s) => events.push(s));
  return { h, manager, events };
}

describe('ElectrumManager — pool-state events', () => {
  beforeEach(() => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('emits exactly one online baseline after a successful start()', async () => {
    const { manager, events } = build();
    const startP = manager.start();
    await flush();
    await startP;
    expect(events).toEqual([{ status: 'online', usable: 2, connected: 2, total: 2 }]);
    await manager.stop();
  });

  it('emits one offline baseline when every initial connect fails', async () => {
    const { h, manager, events } = build();
    manager.on('error', () => {});
    // Both transports refuse the first connect.
    // (Harness transports are created inside installServer — at
    // construction time — so they exist before start().)
    h.transports.get('a')!.nextConnectError = new Error('down');
    h.transports.get('b')!.nextConnectError = new Error('down');
    const startP = manager.start();
    await flush();
    await startP;
    expect(events).toEqual([{ status: 'offline', usable: 0, connected: 0, total: 2 }]);
    await manager.stop();
  });

  it('walks online → degraded → offline as servers drop, and recovers', async () => {
    const { h, manager, events } = build();
    manager.on('error', () => {});
    const startP = manager.start();
    await flush();
    await startP;
    events.length = 0;

    h.transports.get('a')!.pushClose(1006);
    await flush();
    expect(events.map((e) => e.status)).toEqual(['degraded']);

    h.transports.get('b')!.pushClose(1006);
    await flush();
    expect(events.map((e) => e.status)).toEqual(['degraded', 'offline']);

    // Backoff reconnect brings them back: 100ms each (attempt 0).
    await vi.advanceTimersByTimeAsync(150);
    await flush();
    expect(events.at(-1)!.status).toBe('online');
    await manager.stop();
  });

  it('ban of the last usable client goes offline; expiry re-emits without traffic', async () => {
    const { h, manager, events } = build([SERVERS[0]!]);
    const startP = manager.start();
    await flush();
    await startP;
    events.length = 0;

    // One rate-limit-classified RPC error → ban → offline.
    const p = manager.call('server.ping', []).catch(() => undefined);
    await flush();
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage' },
    }));
    await flush();
    await p;
    expect(events.map((e) => e.status)).toEqual(['offline']);
    expect(events[0]).toMatchObject({ usable: 0, connected: 1, total: 1 });

    // cooldownMs = 5000: the internal timer flips the pool back with no calls.
    await vi.advanceTimersByTimeAsync(5100);
    await flush();
    expect(events.map((e) => e.status)).toEqual(['offline', 'online']);
    await manager.stop();
  });

  it('suppresses events across suspend and emits one baseline on resume', async () => {
    const { manager, events } = build();
    const startP = manager.start();
    await flush();
    await startP;
    events.length = 0;

    const suspendP = manager.suspend({ graceMs: 0 });
    await flush();
    await suspendP;
    // Sockets were closed deliberately — no offline noise.
    expect(events).toEqual([]);

    const resumeP = manager.resume();
    await flush();
    await resumeP;
    expect(events.map((e) => e.status)).toEqual(['online']);
    await manager.stop();
  });

  it('does not emit duplicates for same-status transitions', async () => {
    const { h, manager, events } = build();
    manager.on('error', () => {});
    const startP = manager.start();
    await flush();
    await startP;
    events.length = 0;

    // Two drops in a row: degraded once, offline once — the second close
    // of an already-counted client must not re-emit.
    h.transports.get('a')!.pushClose(1006);
    await flush();
    h.transports.get('a')!.pushClose(1006);
    await flush();
    expect(events.map((e) => e.status)).toEqual(['degraded']);
    await manager.stop();
  });

  it('emits a baseline on resume() even when the manager was suspended before start()', async () => {
    const { manager, events } = build();
    // Legal path: suspend straight from `created`, then resume — the
    // manager reaches `running` without start() ever being called, so
    // resume owns the baseline.
    const suspendP = manager.suspend();
    await flush();
    await suspendP;
    const resumeP = manager.resume();
    await flush();
    await resumeP;
    expect(events.map((e) => e.status)).toEqual(['online']);
    await manager.stop();
  });

  it('removing a healthy server from a healthy pool emits nothing (no false degraded)', async () => {
    const { manager, events } = build();
    const startP = manager.start();
    await flush();
    await startP;
    events.length = 0;

    // The client's own `disconnected` transition fires while it is
    // still pooled — suppression must swallow the interim degraded
    // snapshot; online→online after removal means zero events.
    await manager.removeServer('b');
    await flush();
    expect(events).toEqual([]);
    expect(manager.poolState).toEqual({ status: 'online', usable: 1, connected: 1, total: 1 });
    await manager.stop();
  });

  it('an unrelated outage during a slow removeServer() still emits immediately', async () => {
    const { h, manager, events } = build([
      SERVERS[0]!,
      SERVERS[1]!,
      { id: 'c', host: 'c', port: 50001, protocol: 'ws' },
    ]);
    manager.on('error', () => {});
    const startP = manager.start();
    await flush();
    await startP;
    events.length = 0;

    // Make b's transport close hang: removeServer('b') stays in flight.
    const bT = h.transports.get('b')!;
    let releaseClose!: () => void;
    bT.close = () =>
      new Promise<void>((r) => {
        releaseClose = r;
      });
    const removeP = manager.removeServer('b');
    await flush();
    expect(events).toEqual([]); // removal itself is silent (online→online)

    // While removal is pending, an UNRELATED server dies — must emit
    // right away, not after the slow close resolves.
    h.transports.get('a')!.pushClose(1006);
    await flush();
    expect(events.map((e) => e.status)).toEqual(['degraded']);
    // b is excluded from the counts while mid-removal.
    expect(events[0]).toMatchObject({ usable: 1, connected: 1, total: 2 });

    releaseClose();
    await removeP;
    await flush();
    // Post-removal recompute: same degraded status, no duplicate event.
    expect(events.map((e) => e.status)).toEqual(['degraded']);
    expect(manager.poolState).toMatchObject({ status: 'degraded', total: 2 });
    await manager.stop();
  });

  it('survives cooldowns longer than the setTimeout clamp (2^31−1 ms)', async () => {
    const THIRTY_DAYS = 30 * 24 * 3600 * 1000; // > 2^31−1 ≈ 24.8 days
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [SERVERS[0]!],
      policy: roundRobin(),
      transportFactory: h.factory,
      autoBatch: false,
      cooldownMs: THIRTY_DAYS,
    });
    const events: PoolState[] = [];
    manager.on('pool-state', (s) => events.push(s));
    const startP = manager.start();
    await flush();
    await startP;
    events.length = 0;

    const p = manager.call('server.ping', []).catch(() => undefined);
    await flush();
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage' },
    }));
    await flush();
    await p;
    expect(events.map((e) => e.status)).toEqual(['offline']);

    // Without the delay cap this would spin fire→rearm ~1ms steps for
    // the whole advance and hang the test. With the cap: one capped
    // wake-up (~24.8d), one re-arm for the remainder, then recovery.
    await vi.advanceTimersByTimeAsync(THIRTY_DAYS + 1000);
    await flush();
    expect(events.map((e) => e.status)).toEqual(['offline', 'online']);
    await manager.stop();
  });

  it('poolState getter reflects state without any subscription', async () => {
    const { h, manager } = build();
    expect(manager.poolState).toEqual({ status: 'offline', usable: 0, connected: 0, total: 2 });
    const startP = manager.start();
    await flush();
    await startP;
    expect(manager.poolState.status).toBe('online');
    h.transports.get('a')!.pushClose(1006);
    await flush();
    expect(manager.poolState).toMatchObject({ status: 'degraded', usable: 1 });
    await manager.stop();
  });
});
