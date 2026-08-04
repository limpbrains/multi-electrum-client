// ensureConnected(): BlueWallet-style connection guard — wait-for-usable
// with a wall budget, probe policy ('auto' / true / false), lifecycle
// guards, single-flight coalescing, and stop/suspend/abort unwinding.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { NoClientAvailableError, SuspendedError } from '../../../src/errors/types.js';
import { ElectrumManager } from '../../../src/manager.js';
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
    requestTimeoutMs: 2000,
    reconnectBackoff: { minMs: 100, maxMs: 1000, factor: 2, jitter: 0 },
  });
  manager.on('error', () => {});
  return { h, manager };
}

/** Total wire pings currently recorded across both harness transports. */
function pingsSent(h: ReturnType<typeof buildHarness>): number {
  let n = 0;
  for (const t of h.transports.values()) {
    n += t.sent.filter(
      (s) => (JSON.parse(s) as { method?: string }).method === 'server.ping',
    ).length;
  }
  return n;
}

async function start(manager: ElectrumManager): Promise<void> {
  const p = manager.start();
  await flush();
  await p;
}

describe('ElectrumManager.ensureConnected', () => {
  beforeEach(() => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("probe 'auto' skips the ping when a usable client answered recently", async () => {
    const { h, manager } = build();
    await start(manager);
    // Produce a fresh success so lastSuccessAt is now.
    const p = manager.call('server.ping', []);
    await flush();
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    await p;
    const before = pingsSent(h);

    const ensured = manager.ensureConnected();
    await flush();
    await ensured;
    expect(pingsSent(h)).toBe(before); // no extra wire traffic
    await manager.stop();
  });

  it("probe 'auto' pings after the pool has been idle past probeStaleMs", async () => {
    const { h, manager } = build();
    await start(manager);
    const p = manager.call('server.ping', []);
    await flush();
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    await p;
    const before = pingsSent(h);

    // Idle past the freshness window.
    await vi.advanceTimersByTimeAsync(11_000);
    const ensured = manager.ensureConnected();
    await flush();
    expect(pingsSent(h)).toBe(before + 1);
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    await ensured;
    await manager.stop();
  });

  it('probe: true always pings; probe: false never does', async () => {
    const { h, manager } = build();
    await start(manager);
    const warm = manager.call('server.ping', []);
    await flush();
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    await warm;
    const before = pingsSent(h);

    const never = manager.ensureConnected({ probe: false });
    await flush();
    await never;
    expect(pingsSent(h)).toBe(before);

    const always = manager.ensureConnected({ probe: true });
    await flush();
    expect(pingsSent(h)).toBe(before + 1);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    await always;
    await manager.stop();
  });

  it('a half-open socket fails the probe on one server and retries on the other', async () => {
    const { h, manager } = build();
    await start(manager);

    const ensured = manager.ensureConnected({ probe: true, timeoutMs: 10_000 });
    await flush();
    // First pick (roundRobin → a) never answers: half-open. Its request
    // times out (2s), retry routes to b, which answers.
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    await vi.advanceTimersByTimeAsync(2100);
    await flush();
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    await flush();
    await ensured;
    await manager.stop();
  });

  it('waits out a total outage and resolves once reconnect restores a client', async () => {
    const { h, manager } = build();
    await start(manager);
    h.transports.get('a')!.pushClose(1006);
    h.transports.get('b')!.pushClose(1006);
    await flush();
    expect(manager.poolState.status).toBe('offline');

    const ensured = manager.ensureConnected({ probe: false, timeoutMs: 30_000 });
    await flush();
    // Backoff reconnect (100ms) brings the clients back.
    await vi.advanceTimersByTimeAsync(150);
    await flush();
    await ensured;
    expect(manager.poolState.status).toBe('online');
    await manager.stop();
  });

  it('rejects with NoClientAvailableError when the outage outlasts the budget', async () => {
    const { h, manager } = build();
    await start(manager);
    // Make reconnects fail forever.
    for (const t of h.transports.values()) {
      t.connect = async () => {
        throw new Error('down');
      };
      t.pushClose(1006);
    }
    await flush();

    const ensured = manager.ensureConnected({ timeoutMs: 5000 });
    const assertion = expect(ensured).rejects.toBeInstanceOf(NoClientAvailableError);
    await vi.advanceTimersByTimeAsync(5100);
    await flush();
    await assertion;
    await manager.stop();
  });

  it('lifecycle guards: created and stopped throw SuspendedError', async () => {
    const { manager } = build();
    await expect(manager.ensureConnected()).rejects.toThrow(/call start\(\) first/);
    await start(manager);
    await manager.stop();
    await expect(manager.ensureConnected()).rejects.toBeInstanceOf(SuspendedError);
  });

  it('suspended: throws by default, resumes with resumeIfSuspended', async () => {
    const { manager } = build();
    await start(manager);
    const suspendP = manager.suspend({ graceMs: 0 });
    await flush();
    await suspendP;

    await expect(manager.ensureConnected()).rejects.toThrow(/resumeIfSuspended/);

    const ensured = manager.ensureConnected({ resumeIfSuspended: true, probe: false });
    await flush();
    await ensured;
    expect(manager.state).toBe('running');
    await manager.stop();
  });

  it('stop() during the wait rejects immediately, not at the budget', async () => {
    const { h, manager } = build();
    await start(manager);
    h.transports.get('a')!.pushClose(1006);
    h.transports.get('b')!.pushClose(1006);
    // Keep them down so the waiter stays parked.
    for (const t of h.transports.values()) {
      t.connect = async () => {
        throw new Error('down');
      };
    }
    await flush();

    const ensured = manager.ensureConnected({ timeoutMs: 60_000 });
    const assertion = expect(ensured).rejects.toThrow(/manager stopped/);
    await flush();
    const stopP = manager.stop();
    await flush();
    await stopP;
    await assertion;
  });

  it('abort signal rejects the wait with the signal reason', async () => {
    const { h, manager } = build();
    await start(manager);
    h.transports.get('a')!.pushClose(1006);
    h.transports.get('b')!.pushClose(1006);
    for (const t of h.transports.values()) {
      t.connect = async () => {
        throw new Error('down');
      };
    }
    await flush();

    const ctrl = new AbortController();
    const ensured = manager.ensureConnected({ timeoutMs: 60_000, signal: ctrl.signal });
    const assertion = expect(ensured).rejects.toThrow(/nope/);
    await flush();
    ctrl.abort(new Error('nope'));
    await flush();
    await assertion;
    await manager.stop();
  });

  it('budget covers a resumeIfSuspended resume that hangs', async () => {
    const { h, manager } = build();
    await start(manager);
    const suspendP = manager.suspend({ graceMs: 0 });
    await flush();
    await suspendP;
    // Resume's reconnect wave stalls until we release it.
    const releases: Array<() => void> = [];
    for (const t of h.transports.values()) {
      t.connect = () =>
        new Promise<void>((r) => {
          releases.push(r);
        });
    }
    const ensured = manager.ensureConnected({ resumeIfSuspended: true, timeoutMs: 1000 });
    const assertion = expect(ensured).rejects.toBeInstanceOf(NoClientAvailableError);
    await vi.advanceTimersByTimeAsync(1100);
    await flush();
    await assertion;
    // Release the stalled connects and finish the transition cleanly so
    // no pending promises leak past the test.
    for (const r of releases) r();
    await flush();
    await manager.stop();
  });

  it('a probe that outlives the budget rejects at the budget, not at the ping timeout', async () => {
    const { manager } = build();
    await start(manager);
    // requestTimeoutMs is 2000; nobody replies to the ping. Budget 500ms
    // must win the race.
    const startedAt = Date.now();
    const ensured = manager.ensureConnected({ probe: true, timeoutMs: 500 });
    const assertion = expect(ensured).rejects.toThrow(/timed out after 500ms/);
    await vi.advanceTimersByTimeAsync(600);
    await flush();
    await assertion;
    expect(Date.now() - startedAt).toBeLessThan(1000);
    await manager.stop();
  });

  it("one caller's abort does not poison a concurrent caller", async () => {
    const { h, manager } = build();
    await start(manager);
    const ctrl = new AbortController();
    const first = manager.ensureConnected({ probe: true, signal: ctrl.signal });
    const second = manager.ensureConnected({ probe: true });
    await flush();
    const firstAssertion = expect(first).rejects.toThrow(/mine only/);
    ctrl.abort(new Error('mine only'));
    await flush();
    await firstAssertion;
    // Second caller keeps waiting and succeeds on the shared ping reply.
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    await flush();
    await second;
    await manager.stop();
  });

  it('suspend() during a probe-retry loop surfaces SuspendedError, not the budget', async () => {
    const { manager } = build();
    await start(manager);
    // probe: true, nobody replies — ping will time out (2s) and the loop
    // would normally retry until the 60s budget.
    const ensured = manager.ensureConnected({ probe: true, timeoutMs: 60_000 });
    const assertion = expect(ensured).rejects.toThrow(/manager suspend/);
    await flush();
    const suspendP = manager.suspend({ graceMs: 0, cancelInFlight: true });
    await flush();
    await suspendP;
    // Ping fails via failInFlight; next loop iteration hits the
    // lifecycle check — long before the 60s budget.
    await vi.advanceTimersByTimeAsync(1000);
    await flush();
    await assertion;
    await manager.stop();
  });

  it('stop() interrupts a hanging resumeIfSuspended immediately', async () => {
    const { h, manager } = build();
    await start(manager);
    const suspendP = manager.suspend({ graceMs: 0 });
    await flush();
    await suspendP;
    const releases: Array<() => void> = [];
    for (const t of h.transports.values()) {
      t.connect = () =>
        new Promise<void>((r) => {
          releases.push(r);
        }); // resume stalls until released
    }
    const ensured = manager.ensureConnected({ resumeIfSuspended: true, timeoutMs: 60_000 });
    const assertion = expect(ensured).rejects.toThrow(/manager stopped/);
    await flush();
    const stopP = manager.stop(); // parks behind the stalled resume
    await flush();
    await assertion; // rejected NOW, not at the 60s budget
    // Release the resume so the transition chain and stop() can finish.
    for (const r of releases) r();
    await flush();
    await stopP;
  });

  it('suspend() submitted behind a hanging resume still rejects waiters immediately', async () => {
    const { h, manager } = build();
    await start(manager);
    const suspendP = manager.suspend({ graceMs: 0 });
    await flush();
    await suspendP;
    const releases: Array<() => void> = [];
    for (const t of h.transports.values()) {
      t.connect = () =>
        new Promise<void>((r) => {
          releases.push(r);
        });
    }
    const ensured = manager.ensureConnected({ resumeIfSuspended: true, timeoutMs: 60_000 });
    const assertion = expect(ensured).rejects.toThrow(/manager suspending/);
    await flush();
    // The queued suspend parks behind the stalled resume in the FIFO
    // chain — the waiter must be failed at intent time regardless.
    const secondSuspend = manager.suspend({ graceMs: 0 });
    await flush();
    await assertion; // rejected NOW, not at the 60s budget
    for (const r of releases) r();
    await flush();
    await secondSuspend;
    await manager.stop();
  });

  it('a new ensureConnected after suspend() intent rejects even while lifecycle still reads running', async () => {
    const { manager } = build();
    await start(manager);
    // Intent submitted; the queued transition has not run yet — the
    // lifecycle is still 'running' in this tick.
    const suspendP = manager.suspend({ graceMs: 0 });
    expect(manager.state).toBe('running');
    // probe: false would otherwise resolve instantly off the live pool.
    await expect(manager.ensureConnected({ probe: false })).rejects.toThrow(/manager suspending/);
    await flush();
    await suspendP;

    // Intent clears once the transition settles: after resume, the guard
    // works again.
    const resumeP = manager.resume();
    await flush();
    await resumeP;
    const ensured = manager.ensureConnected({ probe: false });
    await flush();
    await ensured;
    await manager.stop();
  });

  it('a ban expiring wakes a parked waiter — recovery with zero traffic', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [SERVERS[0]!],
      policy: roundRobin(),
      transportFactory: h.factory,
      autoBatch: false,
      cooldownMs: 3000,
    });
    manager.on('error', () => {});
    await start(manager);
    // Ban the only client via a rate-limit-classified error → pool offline.
    const p = manager.call('server.ping', []).catch(() => undefined);
    await flush();
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage' },
    }));
    await flush();
    await p;
    expect(manager.poolState.status).toBe('offline');

    const ensured = manager.ensureConnected({ probe: false, timeoutMs: 30_000 });
    await flush();
    // No calls, no socket events — only the internal ban-expiry timer.
    await vi.advanceTimersByTimeAsync(3100);
    await flush();
    await ensured;
    await manager.stop();
  });

  it("during 'resuming', resumeIfSuspended parks on the in-flight resume instead of throwing", async () => {
    const { h, manager } = build();
    await start(manager);
    const suspendP = manager.suspend({ graceMs: 0 });
    await flush();
    await suspendP;
    // Stall the resume so lifecycle sits in 'resuming'.
    const releases: Array<() => void> = [];
    for (const t of h.transports.values()) {
      t.connect = () =>
        new Promise<void>((r) => {
          releases.push(r);
        });
    }
    const resumeP = manager.resume();
    await flush();
    expect(manager.state).toBe('resuming');

    const ensured = manager.ensureConnected({ resumeIfSuspended: true, probe: false });
    await flush();
    for (const r of releases) r();
    await flush();
    await resumeP;
    await ensured;
    expect(manager.state).toBe('running');
    await manager.stop();
  });

  it('honors a custom probeStaleMs window', async () => {
    const { h, manager } = build();
    await start(manager);
    const warm = manager.call('server.ping', []);
    await flush();
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    await warm;
    const before = pingsSent(h);

    // 2s idle: stale under a 1s window → pings; fresh under the 10s default.
    await vi.advanceTimersByTimeAsync(2000);
    const ensured = manager.ensureConnected({ probeStaleMs: 1000 });
    await flush();
    expect(pingsSent(h)).toBe(before + 1);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    await ensured;
    await manager.stop();
  });

  it('rejects a non-finite or clamp-overflowing timeoutMs synchronously', async () => {
    const { manager } = build();
    await start(manager);
    await expect(manager.ensureConnected({ timeoutMs: Infinity })).rejects.toBeInstanceOf(
      RangeError,
    );
    await expect(manager.ensureConnected({ timeoutMs: 0 })).rejects.toBeInstanceOf(RangeError);
    await expect(manager.ensureConnected({ timeoutMs: 2 ** 31 })).rejects.toBeInstanceOf(
      RangeError,
    );
    await expect(manager.ensureConnected({ probeStaleMs: Infinity })).rejects.toBeInstanceOf(
      RangeError,
    );
    await expect(manager.ensureConnected({ probeStaleMs: -1 })).rejects.toBeInstanceOf(RangeError);
    await manager.stop();
  });

  it('suspend() during an in-flight ping rejects the caller immediately, grace window notwithstanding', async () => {
    const { h, manager } = build();
    await start(manager);
    const ensured = manager.ensureConnected({ probe: true, timeoutMs: 60_000 });
    await flush(); // ping now in flight
    // Grace window lets the ping SUCCEED on the wire during 'suspending';
    // the caller must still be rejected right away — a paused manager
    // must never be reported as live.
    const suspendP = manager.suspend({ graceMs: 5000 });
    const assertion = expect(ensured).rejects.toThrow(/manager suspend/);
    await flush();
    await assertion; // rejected immediately, not after grace/budget
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    await vi.advanceTimersByTimeAsync(200);
    await flush();
    await suspendP;
    await manager.stop();
  });

  it('budget expiry during a retry surfaces the last REAL ping failure', async () => {
    const { h, manager } = build();
    await start(manager);
    const ensured = manager.ensureConnected({ probe: true, timeoutMs: 600 });
    const assertion = expect(ensured).rejects.toThrow(/boom/);
    await flush();
    // Both servers answer the ping with a non-retryable RPC error — the
    // loop records it and cools down; the budget expires during/after
    // the cooldown and must surface 'boom', not the synthetic timeout.
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      error: { code: 1, message: 'boom' },
    }));
    h.reply('b', (req: { id: number }) => ({
      id: req.id,
      error: { code: 1, message: 'boom' },
    }));
    await vi.advanceTimersByTimeAsync(700);
    await flush();
    await assertion;
    await manager.stop();
  });

  it('abort during the retry cooldown wakes immediately', async () => {
    const { h, manager } = build();
    await start(manager);
    const ctrl = new AbortController();
    const ensured = manager.ensureConnected({
      probe: true,
      timeoutMs: 60_000,
      signal: ctrl.signal,
    });
    const assertion = expect(ensured).rejects.toThrow(/now/);
    await flush();
    // Fail the ping fast (non-retryable) → loop enters its 500ms cooldown.
    h.reply('a', (req: { id: number }) => ({ id: req.id, error: { code: 1, message: 'x' } }));
    h.reply('b', (req: { id: number }) => ({ id: req.id, error: { code: 1, message: 'x' } }));
    await flush();
    // Abort 100ms into the cooldown — must not wait out the remaining 400ms.
    await vi.advanceTimersByTimeAsync(100);
    ctrl.abort(new Error('now'));
    await flush();
    await assertion;
    await manager.stop();
  });

  it('concurrent calls coalesce onto one wire ping', async () => {
    const { h, manager } = build();
    await start(manager);
    const warm = manager.call('server.ping', []);
    await flush();
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    await warm;
    const before = pingsSent(h);

    const first = manager.ensureConnected({ probe: true });
    const second = manager.ensureConnected({ probe: true });
    await flush();
    expect(pingsSent(h)).toBe(before + 1); // single shared probe
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: null }));
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: null }));
    await Promise.all([first, second]);
    await manager.stop();
  });
});
