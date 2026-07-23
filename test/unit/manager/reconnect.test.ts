// Manager auto-reconnect — backoff schedule, cancellation, and lifecycle gates.
//
// Drives the timer with `vi.useFakeTimers()`, the transport with `MockTransport`
// helpers (`pushClose`, `nextConnectError`). MockTransport.connect is sync past
// a microtask so each timer advance pairs with a `flush()` to settle the
// resulting `client.connect()` promise chain before assertions.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [{ id: 'a', host: 'a', port: 50001, protocol: 'ws' }];

/** Run all queued microtasks. Loops a few times so chained `then` settles too. */
async function flush(): Promise<void> {
  for (let i = 0; i < 8; i++) await Promise.resolve();
}

describe('ElectrumManager — auto-reconnect', () => {
  beforeEach(() => {
    // Math.random is used for jitter; pin it so backoff is deterministic.
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('schedules a reconnect when the transport closes mid-session', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      reconnectBackoff: { minMs: 100, maxMs: 5000, factor: 2, jitter: 0 },
    });
    const startP = manager.start();
    await flush();
    await startP;
    const t = h.transports.get('a')!;
    expect(t.connectCalls).toBe(1);

    t.pushClose(1006);
    await flush();
    // Reconnect timer is armed but not yet fired.
    expect(t.connectCalls).toBe(1);

    await vi.advanceTimersByTimeAsync(100);
    await flush();
    expect(t.connectCalls).toBe(2);

    await manager.stop();
  });

  it('doubles the delay on consecutive failed reconnects', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      reconnectBackoff: { minMs: 100, maxMs: 10_000, factor: 2, jitter: 0 },
    });
    manager.on('error', () => {
      // Swallow — failed reconnects emit `error` so vitest unhandled-rejection
      // detection doesn't see them; we assert via connectCalls.
    });
    const startP = manager.start();
    await flush();
    await startP;
    const t = h.transports.get('a')!;
    expect(t.connectCalls).toBe(1);

    // Drop the link, then make the next two reconnect attempts fail.
    t.pushClose(1006);
    t.nextConnectError = new Error('boom-1');
    await vi.advanceTimersByTimeAsync(100);
    await flush();
    expect(t.connectCalls).toBe(2);

    // Second attempt: 100 * 2^1 = 200ms.
    t.nextConnectError = new Error('boom-2');
    await vi.advanceTimersByTimeAsync(199);
    await flush();
    expect(t.connectCalls).toBe(2);
    await vi.advanceTimersByTimeAsync(1);
    await flush();
    expect(t.connectCalls).toBe(3);

    // Third: 100 * 2^2 = 400ms — succeeds this time.
    await vi.advanceTimersByTimeAsync(399);
    await flush();
    expect(t.connectCalls).toBe(3);
    await vi.advanceTimersByTimeAsync(1);
    await flush();
    expect(t.connectCalls).toBe(4);

    await manager.stop();
  });

  it('clamps the delay to maxMs', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      reconnectBackoff: { minMs: 100, maxMs: 250, factor: 10, jitter: 0 },
    });
    manager.on('error', () => {});
    const startP = manager.start();
    await flush();
    await startP;
    const t = h.transports.get('a')!;

    t.pushClose(1006);
    // attempt 0 → 100ms (would be 100 * 10^0)
    t.nextConnectError = new Error('e1');
    await vi.advanceTimersByTimeAsync(100);
    await flush();
    expect(t.connectCalls).toBe(2);

    // attempt 1 → 100 * 10 = 1000ms, clamped to 250ms.
    t.nextConnectError = new Error('e2');
    await vi.advanceTimersByTimeAsync(250);
    await flush();
    expect(t.connectCalls).toBe(3);

    // attempt 2 → also clamped to 250ms.
    await vi.advanceTimersByTimeAsync(250);
    await flush();
    expect(t.connectCalls).toBe(4);

    await manager.stop();
  });

  it('resets the backoff to minMs after a successful reconnect', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      reconnectBackoff: { minMs: 100, maxMs: 10_000, factor: 2, jitter: 0 },
    });
    manager.on('error', () => {});
    const startP = manager.start();
    await flush();
    await startP;
    const t = h.transports.get('a')!;

    // Fail twice, then succeed.
    t.pushClose(1006);
    t.nextConnectError = new Error('e1');
    await vi.advanceTimersByTimeAsync(100);
    await flush();
    t.nextConnectError = new Error('e2');
    await vi.advanceTimersByTimeAsync(200);
    await flush();
    // 3rd attempt succeeds (no nextConnectError).
    await vi.advanceTimersByTimeAsync(400);
    await flush();
    expect(t.connectCalls).toBe(4);

    // Drop again — next delay should be back to 100ms (not 800ms).
    t.pushClose(1006);
    await vi.advanceTimersByTimeAsync(99);
    await flush();
    expect(t.connectCalls).toBe(4);
    await vi.advanceTimersByTimeAsync(1);
    await flush();
    expect(t.connectCalls).toBe(5);

    await manager.stop();
  });

  it('cancels pending reconnects on stop()', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      reconnectBackoff: { minMs: 1000, maxMs: 5000, factor: 2, jitter: 0 },
    });
    const startP = manager.start();
    await flush();
    await startP;
    const t = h.transports.get('a')!;

    t.pushClose(1006);
    await flush();
    // Timer is armed for 1000ms.
    const stopP = manager.stop();
    await flush();
    await stopP;

    // Even after the would-have-fired window, no extra connect.
    await vi.advanceTimersByTimeAsync(2000);
    await flush();
    expect(t.connectCalls).toBe(1);
  });

  it('cancels pending reconnects on removeServer()', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [
        { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
        { id: 'b', host: 'b', port: 50001, protocol: 'ws' },
      ],
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
      reconnectBackoff: { minMs: 500, maxMs: 5000, factor: 2, jitter: 0 },
    });
    const startP = manager.start();
    await flush();
    await startP;
    const ta = h.transports.get('a')!;

    ta.pushClose(1006);
    await flush();
    const rmP = manager.removeServer('a');
    await flush();
    await rmP;

    await vi.advanceTimersByTimeAsync(2000);
    await flush();
    expect(ta.connectCalls).toBe(1);

    await manager.stop();
  });

  it('does not schedule reconnects while suspended', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      reconnectBackoff: { minMs: 100, maxMs: 5000, factor: 2, jitter: 0 },
    });
    const startP = manager.start();
    await flush();
    await startP;
    const t = h.transports.get('a')!;

    // suspend triggers an internal disconnect; the resulting `disconnected`
    // event must NOT schedule a reconnect because resume() owns that path.
    const suspendP = manager.suspend({ graceMs: 0 });
    await flush();
    await suspendP;

    await vi.advanceTimersByTimeAsync(5000);
    await flush();
    // Only the original connect on start().
    expect(t.connectCalls).toBe(1);

    // resume() reconnects.
    const resumeP = manager.resume();
    await flush();
    await resumeP;
    expect(t.connectCalls).toBe(2);

    await manager.stop();
  });

});
