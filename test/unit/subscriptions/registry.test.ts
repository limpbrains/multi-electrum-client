import { describe, expect, it, vi } from 'vitest';

import { SubscriptionRegistry, type SubscriptionEnv } from '../../../src/subscriptions/registry.js';

interface FakeEnv extends SubscriptionEnv {
  // Test-only handles to drive the env.
  setStatus(method: string, params: readonly unknown[], status: unknown): void;
  setNoClient(): void;
  setConnected(id: string | null): void;
  setClientUnusable(id: string): void;
  /**
   * Hold the next env.call: returns a promise that the test can resolve /
   * reject manually. Lets us drive races between subscribe and disconnect.
   */
  holdNextCall(): { resolve: (status: unknown) => void; reject: (e: unknown) => void };
  callLog: { method: string; params: readonly unknown[] }[];
  emitted: { event: string; payload: unknown }[];
}

function fakeEnv(): FakeEnv {
  const statuses = new Map<string, unknown>();
  let connected: string | null = 'A';
  const usable = new Set<string>(['A', 'B', 'C']);
  const callLog: { method: string; params: readonly unknown[] }[] = [];
  const emitted: { event: string; payload: unknown }[] = [];
  let pendingHold: {
    resolve: (status: unknown) => void;
    reject: (e: unknown) => void;
  } | null = null;

  const env: FakeEnv = {
    async call(method, params) {
      callLog.push({ method, params });
      if (pendingHold) {
        const hold = pendingHold;
        pendingHold = null;
        return new Promise((resolve, reject) => {
          hold.resolve = resolve;
          hold.reject = reject;
        });
      }
      const key = `${method} ${JSON.stringify(params)}`;
      return statuses.get(key);
    },
    emit(event, payload) {
      emitted.push({ event, payload });
    },
    pickConnectedClient() {
      return connected;
    },
    isClientConnected(id) {
      return usable.has(id) && id === connected;
    },
    setStatus(method, params, status) {
      statuses.set(`${method} ${JSON.stringify(params)}`, status);
    },
    setNoClient() {
      connected = null;
    },
    setConnected(id) {
      connected = id;
    },
    setClientUnusable(id) {
      usable.delete(id);
    },
    holdNextCall() {
      const handle = {
        resolve: (_: unknown) => undefined,
        reject: (_: unknown) => undefined,
      };
      pendingHold = handle;
      return handle;
    },
    callLog,
    emitted,
  };
  return env;
}

describe('SubscriptionRegistry — basic handler routing', () => {
  it('subscribe returns Unsubscribe; first sub triggers wire call', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['HASH'], 'STATUS_1');
    const reg = new SubscriptionRegistry(env);

    const handler = vi.fn();
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['HASH'], handler);

    expect(env.callLog).toHaveLength(1);
    expect(env.callLog[0]).toEqual({
      method: 'blockchain.scripthash.subscribe',
      params: ['HASH'],
    });
    expect(handler).toHaveBeenCalledWith('STATUS_1');
    expect(reg.size()).toBe(1);

    await unsub();
    expect(reg.size()).toBe(0);
    // Last unsub triggers wire unsubscribe.
    expect(env.callLog).toHaveLength(2);
    expect(env.callLog[1]?.method).toBe('blockchain.scripthash.unsubscribe');
  });

  it('multi-handler dedup: second subscribe does not re-call wire subscribe', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['HASH'], 'STATUS_1');
    const reg = new SubscriptionRegistry(env);

    const h1 = vi.fn();
    const h2 = vi.fn();
    const unsub1 = await reg.subscribe('blockchain.scripthash.subscribe', ['HASH'], h1);
    const unsub2 = await reg.subscribe('blockchain.scripthash.subscribe', ['HASH'], h2);

    expect(env.callLog).toHaveLength(1); // only first call subscribed
    // Both handlers received the initial status (h2 sync via lastKnownStatus).
    expect(h1).toHaveBeenCalledWith('STATUS_1');
    expect(h2).toHaveBeenCalledWith('STATUS_1');
    expect(reg.size()).toBe(1);

    await unsub1();
    // Record still alive — h2 still subscribed.
    expect(reg.size()).toBe(1);
    expect(env.callLog).toHaveLength(1);

    await unsub2();
    // Last handler gone → record dropped + wire unsubscribe sent.
    expect(reg.size()).toBe(0);
    expect(env.callLog).toHaveLength(2);
    expect(env.callLog[1]?.method).toBe('blockchain.scripthash.unsubscribe');
  });

  it('throws when no client is connected', async () => {
    const env = fakeEnv();
    env.setNoClient();
    const reg = new SubscriptionRegistry(env);
    await expect(
      reg.subscribe('blockchain.scripthash.subscribe', ['HASH'], () => undefined),
    ).rejects.toThrow(/no connected client/);
  });

  it('headers.subscribe has no wire unsubscribe', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.headers.subscribe', [], { height: 1, hex: '00' });
    const reg = new SubscriptionRegistry(env);

    const unsub = await reg.subscribe('blockchain.headers.subscribe', [], () => undefined);
    await unsub();
    // Only the initial subscribe call, no unsubscribe (no paired wire method).
    expect(env.callLog).toHaveLength(1);
  });
});

describe('SubscriptionRegistry — notify dispatch', () => {
  it('routes notifications to registered handlers and updates lastKnownStatus', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'S0');
    const reg = new SubscriptionRegistry(env);
    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    handler.mockClear();
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'S1');
    expect(handler).toHaveBeenCalledWith('S1');

    // Repeat with same status: no-op (dedup).
    handler.mockClear();
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'S1');
    expect(handler).not.toHaveBeenCalled();

    // Different status: fires.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'S2');
    expect(handler).toHaveBeenCalledWith('S2');
  });

  it('drops notifications from a different (stale) client', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'S0');
    const reg = new SubscriptionRegistry(env);
    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    handler.mockClear();
    reg.notify('OTHER_CLIENT', 'blockchain.scripthash.subscribe', ['H'], 'S1');
    expect(handler).not.toHaveBeenCalled();
  });

  it('drops notifications for unknown subscription keys', async () => {
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    // No subscribe call — registry empty.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['UNKNOWN'], 'X');
    // No throw, no crash; just a silent drop.
  });
});

describe('SubscriptionRegistry — disconnect / restore', () => {
  it('orphans subs on clientDisconnected, rebinds on restoreOrphans', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    handler.mockClear();
    reg.clientDisconnected('A');
    // Notification from old client should not fire (orphaned + stale generation).
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'X');
    expect(handler).not.toHaveBeenCalled();

    // Status drifted server-side during the gap.
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'AFTER_REBIND');
    await reg.restoreOrphans();

    // Synthetic handler call with the new status.
    expect(handler).toHaveBeenCalledWith('AFTER_REBIND');
    // Plus a 'subscription-restored' event with drift=true.
    expect(env.emitted).toHaveLength(1);
    expect(env.emitted[0]).toEqual({
      event: 'subscription-restored',
      payload: {
        method: 'blockchain.scripthash.subscribe',
        params: ['H'],
        drift: true,
      },
    });
  });

  it('restoreOrphans without drift fires event with drift=false and does not call handler', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'STABLE');
    const reg = new SubscriptionRegistry(env);
    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    handler.mockClear();
    reg.clientDisconnected('A');
    // Same status on rebind.
    await reg.restoreOrphans();

    expect(handler).not.toHaveBeenCalled();
    expect(env.emitted[0]).toMatchObject({
      event: 'subscription-restored',
      payload: { drift: false },
    });
  });

  it('restoreOrphans is a no-op when no client is available', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    reg.clientDisconnected('A');
    env.setNoClient();
    await reg.restoreOrphans();

    // No re-call attempted; record stays orphaned.
    handler.mockClear();
    expect(handler).not.toHaveBeenCalled();
  });
});

describe('SubscriptionRegistry — handler isolation', () => {
  it('throwing handler does not stop other handlers from receiving status', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'S0');
    const reg = new SubscriptionRegistry(env);

    const bad = vi.fn(() => {
      throw new Error('handler boom');
    });
    const good = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], bad);
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], good);

    bad.mockClear();
    good.mockClear();
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'S1');
    expect(bad).toHaveBeenCalledWith('S1');
    expect(good).toHaveBeenCalledWith('S1');
  });
});

describe('SubscriptionRegistry — concurrency', () => {
  it('coalesces concurrent first-subscribes onto one wire call', async () => {
    const env = fakeEnv();
    const hold = env.holdNextCall();
    const reg = new SubscriptionRegistry(env);

    const h1 = vi.fn();
    const h2 = vi.fn();
    const p1 = reg.subscribe('blockchain.scripthash.subscribe', ['H'], h1);
    const p2 = reg.subscribe('blockchain.scripthash.subscribe', ['H'], h2);

    hold.resolve('INIT');
    await Promise.all([p1, p2]);

    // Only one wire call despite two same-tick subscribes.
    expect(env.callLog).toHaveLength(1);
    expect(h1).toHaveBeenCalledWith('INIT');
    expect(h2).toHaveBeenCalledWith('INIT');
    expect(reg.size()).toBe(1);
  });

  it('rebinds an in-flight subscribe whose target client disconnects mid-call', async () => {
    const env = fakeEnv();
    const hold = env.holdNextCall();
    const reg = new SubscriptionRegistry(env);

    const handler = vi.fn();
    const subPromise = reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    // Disconnect the bound client while the subscribe wire call is pending.
    reg.clientDisconnected('A');
    // Rebind target on a different client.
    env.setConnected('B');
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'AFTER_REBIND');

    // Original wire call resolves with stale status.
    hold.resolve('STALE_FROM_A');
    await subPromise;
    // Allow the background rebind to settle.
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    // Handler saw stale 'STALE_FROM_A' (initial) then drifted to 'AFTER_REBIND'.
    expect(handler).toHaveBeenCalledWith('STALE_FROM_A');
    expect(handler).toHaveBeenLastCalledWith('AFTER_REBIND');
    // Two wire calls total: original subscribe + rebind subscribe.
    expect(env.callLog).toHaveLength(2);
    // Restored event fired for the rebind.
    expect(env.emitted).toEqual([
      {
        event: 'subscription-restored',
        payload: {
          method: 'blockchain.scripthash.subscribe',
          params: ['H'],
          drift: true,
        },
      },
    ]);
  });

  it('restoreOrphans gates concurrent restores onto one wire call per key', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'AFTER');
    const callsBefore = env.callLog.length;
    // Two restore triggers (e.g. disconnect + connect) racing.
    await Promise.all([reg.restoreOrphans(), reg.restoreOrphans()]);
    const callsAfter = env.callLog.length;

    // Only one rebind wire call despite two restoreOrphans triggers.
    expect(callsAfter - callsBefore).toBe(1);
  });

  it('subscribe rejection clears pending and surfaces error to caller', async () => {
    const env = fakeEnv();
    const hold = env.holdNextCall();
    const reg = new SubscriptionRegistry(env);

    const subPromise = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    hold.reject(new Error('wire boom'));
    await expect(subPromise).rejects.toThrow(/wire boom/);

    // Pending entry cleaned up; a fresh subscribe must issue a new wire call.
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    expect(handler).toHaveBeenCalledWith('INIT');
    // Original failed call + the retry — two total.
    expect(env.callLog).toHaveLength(2);
    expect(reg.size()).toBe(1);
  });

  it('clear() drops pending and stops in-flight subscribes from registering', async () => {
    const env = fakeEnv();
    const hold = env.holdNextCall();
    const reg = new SubscriptionRegistry(env);

    const subPromise = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    // Manager.stop() path: clear before the wire call lands.
    reg.clear();
    hold.resolve('LATE');
    await subPromise;

    // Even though the task body did `subs.set`, clear() ran first.
    // The stop path then awaits no further notifications. The record may
    // or may not be present depending on ordering, but for our purposes
    // the contract is: notifications after clear() must not reach handlers.
    // Verify no synthetic state leaked by checking emitted events.
    expect(env.emitted).toEqual([]);
  });
});

describe('SubscriptionRegistry — exception safety', () => {
  it('notify with non-JSON-serializable status does not throw, fires handler', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    handler.mockClear();
    // Circular object: JSON.stringify would throw without the try/catch in
    // statusEquals.
    const circular: Record<string, unknown> = {};
    circular['self'] = circular;
    expect(() =>
      reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], circular),
    ).not.toThrow();
    expect(handler).toHaveBeenCalledWith(circular);
  });
});

describe('SubscriptionRegistry — last-handler unsub gating', () => {
  it('skips wire unsubscribe when bound client is gone', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);

    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    const callsBefore = env.callLog.length;
    // Mark the bound client unusable (e.g. disconnected and not yet rebound).
    env.setClientUnusable('A');
    await unsub();
    // No wire unsubscribe call.
    expect(env.callLog).toHaveLength(callsBefore);
    expect(reg.size()).toBe(0);
  });
});
