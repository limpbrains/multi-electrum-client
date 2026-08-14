import { describe, expect, it, vi } from 'vitest';

import { NoClientAvailableError, RpcError, SuspendedError } from '../../../src/errors/types.js';
import { SubscriptionRegistry, type SubscriptionEnv } from '../../../src/subscriptions/registry.js';

interface FakeEnv extends SubscriptionEnv {
  // Test-only handles to drive the env.
  setStatus(method: string, params: readonly unknown[], status: unknown): void;
  setNoClient(): void;
  setConnected(id: string | null): void;
  setClientGone(id: string): void;
  /** Client ids handed to env.retireClient, in order. */
  retired: string[];
  /** Test hook: runs synchronously inside retireClient. */
  onRetire: (() => void) | undefined;
  /** Bump the session counter for `id` (simulates a reconnect). */
  bumpSession(id: string): void;
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
  const sessionSeqs = new Map<string, number>();
  const callLog: { method: string; params: readonly unknown[] }[] = [];
  const emitted: { event: string; payload: unknown }[] = [];
  let pendingHold: {
    resolve: (status: unknown) => void;
    reject: (e: unknown) => void;
    arm(real: { resolve: (v: unknown) => void; reject: (e: unknown) => void }): void;
  } | null = null;

  const env: FakeEnv = {
    async call(method, params) {
      callLog.push({ method, params });
      // The manager reports which client actually served the call; the
      // fake serves with whichever client is connected at dispatch time.
      const servedBy = connected ?? 'A';
      if (pendingHold) {
        const hold = pendingHold;
        pendingHold = null;
        return new Promise<{ value: unknown; servedBy: string }>((resolve, reject) => {
          hold.arm({
            resolve: (status: unknown) => resolve({ value: status, servedBy }),
            reject,
          });
        });
      }
      const key = `${method} ${JSON.stringify(params)}`;
      return { value: statuses.get(key), servedBy };
    },
    emit(event, payload) {
      emitted.push({ event, payload });
    },
    pickConnectedClient() {
      return connected;
    },
    retired: [],
    onRetire: undefined,
    retireClient(id: string) {
      this.retired.push(id);
      this.onRetire?.();
    },
    sessionSeq(id: string) {
      return sessionSeqs.get(id) ?? 0;
    },
    bumpSession(id: string) {
      sessionSeqs.set(id, (sessionSeqs.get(id) ?? 0) + 1);
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
    setClientGone(id) {
      usable.delete(id);
    },
    holdNextCall() {
      // Buffer an early settlement: the registry defers its wire call by
      // one microtask (leader registration must be visible before any
      // synchronous side effect), so a test may settle the hold before
      // env.call arms the real resolvers.
      let armed: { resolve: (v: unknown) => void; reject: (e: unknown) => void } | null = null;
      let early: { kind: 'resolve' | 'reject'; value: unknown } | null = null;
      const handle = {
        resolve: (v: unknown) => {
          if (armed) armed.resolve(v);
          // First settlement wins, like a real promise — a later
          // resolve must not overwrite a buffered reject (or vice
          // versa).
          else early ??= { kind: 'resolve', value: v };
        },
        reject: (e: unknown) => {
          if (armed) armed.reject(e);
          else early ??= { kind: 'reject', value: e };
        },
        arm(real: { resolve: (v: unknown) => void; reject: (e: unknown) => void }) {
          armed = real;
          if (early) {
            if (early.kind === 'resolve') real.resolve(early.value);
            else real.reject(early.value);
          }
        },
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

    // CONTRACT CHANGE (protocol-mandated): a repeated equal status from
    // the bound server DELIVERS. The spec says the client MAY be
    // notified without a status change and MUST be notified on a
    // same-height reorg — where the blockhash changes but the status
    // hash does not. Suppressing the equal push swallowed exactly that
    // mandated notification, leaving a consumer on a merkle proof tied
    // to the orphaned block. Handlers resync on every callback, so the
    // duplicate costs one refetch.
    handler.mockClear();
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'S1');
    expect(handler).toHaveBeenCalledWith('S1');

    // Different status: fires.
    handler.mockClear();
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

// Drain the microtask queue so promise chains (e.g. a rejected wire call
// propagating through rebind's catch into its backoff setTimeout) settle
// before the test advances fake timers. Plain awaits, no timers — works
// identically under vitest's timer mock (node) and the on-device
// @sinonjs/fake-timers shim, whose tickAsync would otherwise start before
// the retry timer exists and advance an empty clock.
const flushMicrotasks = async (rounds = 20): Promise<void> => {
  for (let i = 0; i < rounds; i++) await Promise.resolve();
};

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

  it('retries a failed rebind with backoff while a client stays connected', async () => {
    vi.useFakeTimers();
    try {
      const env = fakeEnv();
      env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
      const reg = new SubscriptionRegistry(env);
      const handler = vi.fn();
      await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
      handler.mockClear();

      reg.clientDisconnected('A');
      // Rebind lands on a connected client but the wire subscribe fails
      // (e.g. request timeout on an established-but-dead reconnect).
      const hold = env.holdNextCall();
      const restore = reg.restoreOrphans();
      await flushMicrotasks(); // let the held call be issued
      hold.reject(new Error('subscribe timed out'));
      await flushMicrotasks(); // let the failure schedule the backoff timer

      // The retry fires after the 1s backoff and succeeds.
      env.setStatus('blockchain.scripthash.subscribe', ['H'], 'AFTER_RETRY');
      await vi.advanceTimersByTimeAsync(1_000);
      await restore;

      expect(
        env.callLog.filter((c) => c.method === 'blockchain.scripthash.subscribe'),
      ).toHaveLength(
        3, // initial subscribe + failed rebind + successful retry
      );
      expect(handler).toHaveBeenCalledWith('AFTER_RETRY');
      expect(env.emitted[env.emitted.length - 1]).toMatchObject({
        event: 'subscription-restored',
        payload: { drift: true },
      });
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops retrying a failed rebind once no client is connected', async () => {
    vi.useFakeTimers();
    try {
      const env = fakeEnv();
      env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
      const reg = new SubscriptionRegistry(env);
      await reg.subscribe('blockchain.scripthash.subscribe', ['H'], vi.fn());

      reg.clientDisconnected('A');
      const hold = env.holdNextCall();
      const restore = reg.restoreOrphans();
      await flushMicrotasks();
      hold.reject(new Error('subscribe timed out'));
      await flushMicrotasks();

      // The client drops during the backoff pause — the loop must end
      // without another wire call; the next connect owns the retry.
      env.setNoClient();
      await vi.advanceTimersByTimeAsync(1_000);
      await restore;

      expect(
        env.callLog.filter((c) => c.method === 'blockchain.scripthash.subscribe'),
      ).toHaveLength(
        2, // initial subscribe + the single failed rebind
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it('stops retrying a failed rebind after the last handler unsubscribes', async () => {
    vi.useFakeTimers();
    try {
      const env = fakeEnv();
      env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
      const reg = new SubscriptionRegistry(env);
      const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], vi.fn());

      reg.clientDisconnected('A');
      const hold = env.holdNextCall();
      const restore = reg.restoreOrphans();
      await flushMicrotasks();
      hold.reject(new Error('subscribe timed out'));
      await flushMicrotasks();

      // Caller gives up during the backoff pause.
      await unsub();
      await vi.advanceTimersByTimeAsync(1_000);
      await restore;

      expect(
        env.callLog.filter((c) => c.method === 'blockchain.scripthash.subscribe'),
      ).toHaveLength(
        2, // initial subscribe + the single failed rebind; no retry after unsub
      );
    } finally {
      vi.useRealTimers();
    }
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
    // Let the (microtask-deferred) wire call dispatch on A first — the
    // scenario under test is a call already in flight when A dies.
    await flushMicrotasks();

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

    // The subscribe's own status came from a server that was already
    // gone, so it is never presented as current: the handler's first —
    // and only — callback is the status of the binding that actually
    // holds.
    expect(handler).not.toHaveBeenCalledWith('STALE_FROM_A');
    expect(handler.mock.calls).toEqual([['AFTER_REBIND']]);
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

    // The subscription never existed as far as the caller is concerned:
    // the response arrives after the registry was torn down, so the call
    // fails rather than registering a record on a stopped manager.
    await expect(subPromise).rejects.toThrow(/registry cleared/);
    expect(reg.size()).toBe(0);
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
    expect(() => reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], circular)).not.toThrow();
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
    env.setClientGone('A');
    await unsub();
    // No wire unsubscribe call.
    expect(env.callLog).toHaveLength(callsBefore);
    expect(reg.size()).toBe(0);
  });
});

describe('SubscriptionRegistry — same-ref handler dedup (Set semantics)', () => {
  it('subscribing the same function reference twice yields one slot; first unsub kills both', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);

    const handler = vi.fn();
    const unsubA = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    const unsubB = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    // Set, not multiset — second subscribe is a no-op for the handler set.
    expect(env.callLog).toHaveLength(1);

    handler.mockClear();
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'NEW');
    expect(handler).toHaveBeenCalledTimes(1); // not 2

    // First unsub removes the handler entirely; record drops + wire unsub fires.
    await unsubA();
    expect(reg.size()).toBe(0);
    // Second unsub is a no-op (record already gone).
    await unsubB();
    expect(reg.size()).toBe(0);
  });
});

describe('SubscriptionRegistry — removeServer-style flow', () => {
  it('orphan + restoreOrphans rebinds onto a remaining connected client', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);

    const handler = vi.fn();
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    // Simulate manager.removeServer('A'): orphan + flip pickConnectedClient
    // to a different client. restoreOrphans should bind on the new client.
    handler.mockClear();
    reg.clientDisconnected('A');
    env.setConnected('B');
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'AFTER');
    await reg.restoreOrphans();

    expect(handler).toHaveBeenCalledWith('AFTER');
    // Notification on the new client now dispatches; old client's would not.
    handler.mockClear();
    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'NEXT');
    expect(handler).toHaveBeenCalledWith('NEXT');
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'STALE');
    expect(handler).not.toHaveBeenCalledWith('STALE');
  });
});

describe('SubscriptionRegistry — notifications racing the wire call', () => {
  // A server may answer `subscribe` and push the first status change in
  // the same TCP chunk / WebSocket frame. The transport frames and
  // dispatches both lines synchronously, while resolving the wire call
  // only schedules the registry's continuation — so `notify` lands before
  // the record exists. Dropping it there left the caller on the status
  // the subscribe returned until the next change: for a wallet, a missed
  // transaction.
  it('delivers a notification that arrived before the record was published', async () => {
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();

    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    hold.resolve('S0');
    // Same turn as the response — the continuation has not run yet.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'S1');
    // A real server that pushed S1 answers S1 when asked again; the
    // registry asks precisely because the two cannot be ordered.
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'S1');
    await p;
    await flushMicrotasks();

    // The subscribe's own status first, then the change.
    expect(seen).toEqual(['S0', 'S1']);
  });

  it('delivers a racing notification even from another server', async () => {
    // A server only pushes keys it believes we subscribed on — a push
    // from a different pooled server during our subscribe means a leaked
    // or parallel subscription saw the address change. A status is a
    // change signal, and a signal from any server we pool is worth one
    // resync; dropping it can hide the change until the next push.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();

    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    hold.resolve('S0');
    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'FROM_OTHER_SERVER');
    await p;
    await flushMicrotasks();

    expect(seen).toEqual(['S0', 'FROM_OTHER_SERVER']);
  });

  it('does not buffer notifications for keys we never subscribed to', async () => {
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];

    reg.notify('A', 'blockchain.scripthash.subscribe', ['UNKNOWN'], 'X');
    // Subscribing afterwards must report the server's status, not the
    // stale push that predates the call.
    env.setStatus('blockchain.scripthash.subscribe', ['UNKNOWN'], 'S0');
    await reg.subscribe('blockchain.scripthash.subscribe', ['UNKNOWN'], (s) => seen.push(s));

    expect(seen).toEqual(['S0']);
  });

  it('delivers a notification that raced a rebind response', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));

    reg.clientDisconnected('A');
    env.setConnected('B');
    const hold = env.holdNextCall();
    const restored = reg.restoreOrphans();
    await flushMicrotasks();

    hold.resolve('R0');
    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'R1');
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'R1');
    await restored;
    await flushMicrotasks();

    expect(seen).toEqual(['INIT', 'R0', 'R1']);
  });

  it('delivers both the response and the racing push, in wire order', async () => {
    // The two cannot be ordered — both landed in one synchronously
    // dispatched chunk — and a status is a change signal, not data:
    // whichever was really newer, the consumer's resync after the last
    // callback fetches the server's current truth. Delivering both is
    // therefore correct in either ordering, and needs no machinery. (A
    // wire-refetch tie-break lived here once; it caused more defects
    // than the ambiguity it resolved.)
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));

    reg.clientDisconnected('A');
    env.setConnected('B');
    const hold = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();

    hold.resolve('S1');
    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'S2');
    env.setNoClient();
    await restoring;
    await flushMicrotasks();

    expect(seen).toEqual(['INIT', 'S1', 'S2']);
  });

  it('delivers an unorderable pre-response push after the response', async () => {
    // The push arrived before the response here, so the response is the
    // newer of the two — but nothing in the buffer can know that. Both
    // are delivered, response first; the handler's final resync settles
    // the truth either way.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'OLD');
    hold.resolve('NEW');
    await p;
    await flushMicrotasks();

    expect(seen).toEqual(['NEW', 'OLD']);
  });
});

describe('SubscriptionRegistry — late continuations must not resurrect state', () => {
  it('a subscribe whose response lands after clear() installs nothing', async () => {
    // stop() clears the registry, but a first-subscribe already waiting on
    // its wire call had its continuation queued: it used to run afterwards
    // and re-install a record (plus call the handler) on a stopped manager.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();

    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    reg.clear();
    hold.resolve('LATE');
    await p.catch(() => undefined);
    await flushMicrotasks();

    expect(reg.size()).toBe(0);
    expect(seen).toEqual([]);
  });

  it('a rebind that resolves after its record was replaced touches nothing', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const first: unknown[] = [];
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) =>
      first.push(s),
    );

    // Orphan it and start a rebind we can hold mid-flight.
    reg.clientDisconnected('A');
    env.setConnected('B');
    const hold = env.holdNextCall();
    const rebinding = reg.restoreOrphans();
    await flushMicrotasks();

    // The old record goes away and a fresh subscription takes the key.
    await unsub();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'NEW');
    const second: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => second.push(s));
    const pushed = env.emitted.length;

    // Now the stale rebind's wire call finally answers.
    hold.resolve('STALE');
    await rebinding;
    await flushMicrotasks();

    // The replacement keeps its own status, and the dead record's rebind
    // announces nothing.
    expect(second).toEqual(['NEW']);
    expect(first).toEqual(['INIT']);
    expect(env.emitted.length).toBe(pushed);
  });
});

describe('SubscriptionRegistry — handler attached during delivery', () => {
  it('receives the current status exactly once', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const second: unknown[] = [];
    let attached = false;

    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => {
      // A handler that subscribes another handler while it is being
      // called: `deliver` used to iterate the live Set, so the new
      // handler got the same snapshot twice — once from attach(), once
      // from the iterator that reached it. Attach only on the push, so
      // the initial replay is not what we are measuring.
      if (attached || s !== 'PUSH') return;
      attached = true;
      void reg.subscribe('blockchain.scripthash.subscribe', ['H'], (x) => second.push(x));
    });

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'PUSH');
    await flushMicrotasks();

    expect(second).toEqual(['PUSH']);
  });
});

describe('SubscriptionRegistry — teardown must actually finish', () => {
  it('clear() ends a rebind sitting in its backoff sleep', async () => {
    // The retry sleep used to be a plain setTimeout: after stop() the task
    // stayed alive and the timer kept the event loop referenced for up to
    // 10s, so `await stop()` returned before teardown was really over.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    const failed = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();
    failed.reject(new Error('wire subscribe failed'));
    await flushMicrotasks();

    reg.clear();
    const outcome = await Promise.race([
      restoring.then(() => 'settled'),
      new Promise((r) => setTimeout(() => r('still sleeping'), 100)),
    ]);
    expect(outcome).toBe('settled');
  });

  it('unsubscribing during a rebind still unsubscribes on the wire', async () => {
    // The record was orphaned when the last handler left, so makeUnsub had
    // no server to tell. The rebind then succeeded and created a live
    // subscription on the replacement — which nobody would ever cancel.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setConnected('B');
    const hold = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();

    await unsub();
    hold.resolve('LATE');
    await restoring;
    await flushMicrotasks();

    expect(env.callLog.map((c) => c.method)).toContain('blockchain.scripthash.unsubscribe');
  });
});

describe('SubscriptionRegistry — same-key rebind ownership', () => {
  it('a stale rebind does not strand the record that replaced it', async () => {
    // Single-flight is keyed by canonical key, so a replacement record
    // asking for a rebind used to be handed its predecessor's in-flight
    // task. That task exits on the identity check, clears the map, and
    // nothing ever recovers the replacement: it stays orphaned — no
    // notifications — until some unrelated client transition happens.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    // R1 orphaned, its rebind held mid-flight, then unsubscribed.
    reg.clientDisconnected('A');
    env.setConnected('B');
    const stale = env.holdNextCall();
    const staleTask = reg.restoreOrphans();
    await flushMicrotasks();
    await unsub();

    // R2 takes the key, then loses its server while another stays up.
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'SECOND');
    const seen: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    reg.clientDisconnected('B');
    env.setConnected('C');

    const recovery = reg.restoreOrphans();
    stale.resolve('STALE');
    await staleTask;
    await recovery;
    await flushMicrotasks();

    // R2 is bound again and its status was refreshed from the new server.
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'AFTER');
    reg.notify('C', 'blockchain.scripthash.subscribe', ['H'], 'AFTER');
    expect(seen).toEqual(['SECOND', 'AFTER']);
  });
});

describe('SubscriptionRegistry — joining a record with no live binding', () => {
  it('waits for a binding instead of replaying a snapshot of unknown age', async () => {
    // Joining an existing key attached immediately and replayed
    // `lastKnownStatus`. Whether that value was current depended on
    // whether the record happened to be bound: suspended, offline or
    // mid-rebind, the joiner was handed an hours-old status as its
    // "current" first callback — while subscribing to a NEW key at the
    // same moment threw. Same key, same instant, two different answers.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'BEFORE');
    const reg = new SubscriptionRegistry(env);
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    // The pool goes away — records survive, bindings do not.
    reg.clientDisconnected('A');
    env.setNoClient();

    const joined: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => joined.push(s));
    expect(joined).toEqual([]);

    // A binding holds again: the joiner gets its first callback then,
    // even though the status never changed.
    env.setConnected('B');
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'BEFORE');
    await reg.restoreOrphans();
    await flushMicrotasks();

    expect(joined).toEqual(['BEFORE']);
  });
});

describe('SubscriptionRegistry — a handler that stops listening mid-fan-out', () => {
  it('does not receive the push whose delivery is already under way', async () => {
    // Fan-out iterates a snapshot so a callback can mutate the handler
    // set safely. That protects the loop, not the callers: a handler
    // dropped by an earlier callback in the SAME push must not still be
    // called.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const second = vi.fn();
    let dropSecond: (() => Promise<void>) | undefined;

    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => {
      if (s === 'PUSH') void dropSecond?.();
    });
    dropSecond = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], second);
    second.mockClear();

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'PUSH');
    await flushMicrotasks();

    expect(second).not.toHaveBeenCalled();
  });

  it('does not receive it once an earlier callback tore the registry down', async () => {
    // `clear()` empties `subs` but leaves each record's handler set
    // intact, so membership alone still reads as "subscribed" — and by
    // then every unsubscribe handle is a silent no-op, so the caller has
    // no way to stop the callback it is about to get.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const second = vi.fn();

    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => {
      if (s === 'PUSH') reg.clear();
    });
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], second);
    second.mockClear();

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'PUSH');
    await flushMicrotasks();

    expect(second).not.toHaveBeenCalled();
  });

  it('does not receive a baseline it unsubscribed from before the binding arrived', async () => {
    // A handler that joins an unbound record waits for a binding. If it
    // unsubscribes before that binding arrives, the wait has to go with
    // it — otherwise the caller gets a callback after it explicitly
    // stopped listening.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'SAME');
    const reg = new SubscriptionRegistry(env);
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setNoClient();
    const late = vi.fn();
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], late);
    await unsub();

    // The rebind returns the status we already had: the no-drift path is
    // exactly where baseline waiters are paid.
    env.setConnected('B');
    await reg.restoreOrphans();
    await flushMicrotasks();

    expect(late).not.toHaveBeenCalled();
  });
});

describe('SubscriptionRegistry — drift includes the unorderable push', () => {
  it('reports drift when the rebind answer matched but a raced push did not', async () => {
    // Consumers use `drift` to decide whether to refetch history. The
    // rebind's own answer said "unchanged", but a push we could not
    // order carried a change — it is delivered synchronously before the
    // announcement, so the drift measurement sees it.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'SAME');
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));

    reg.clientDisconnected('A');
    env.setConnected('B');
    const rebindCall = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();

    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'CHANGED');
    rebindCall.resolve('SAME');
    await restoring;
    await flushMicrotasks();

    expect(seen.at(-1)).toBe('CHANGED');
    expect(env.emitted).toEqual([
      {
        event: 'subscription-restored',
        payload: { method: 'blockchain.scripthash.subscribe', params: ['H'], drift: true },
      },
    ]);
  });
});

describe('SubscriptionRegistry — a handler removed and re-added mid-fan-out', () => {
  it('treats the re-registration as new, and the old handle as spent', async () => {
    // Handler identity is the function reference, so a remove + re-add of
    // the SAME function is a new registration that nothing can tell from
    // the old one: the fan-out snapshot still holds it and calls it
    // again, and the old unsubscribe handle then tears down the
    // replacement.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const handler = (s: unknown): void => {
      seen.push(s);
    };

    // The swapper is registered FIRST, so the swap happens while the
    // fan-out snapshot still holds the handler's old registration.
    let oldHandle: (() => Promise<void>) | undefined;
    let swapped = false;
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => {
      if (s !== 'PUSH' || swapped) return;
      swapped = true;
      void oldHandle?.();
      void reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    });
    oldHandle = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    seen.length = 0;

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'PUSH');
    await flushMicrotasks();

    // Once: from the replacement's own attach. The snapshot entry for
    // the registration that was just removed must not fire as well.
    expect(seen).toEqual(['PUSH']);

    // The spent handle belongs to the previous registration and must not
    // remove the new one.
    await oldHandle();
    seen.length = 0;
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'NEXT');
    await flushMicrotasks();

    expect(seen).toEqual(['NEXT']);
  });
});

describe('SubscriptionRegistry — an unorderable push we cannot ask about', () => {
  it('keeps a push from the server the record is still bound to', async () => {
    // The tie-break needs the answering server, and one whose socket
    // has since DROPPED cannot be asked — but it is still ours: the
    // record stays bound to it, and dropping the buffered status would
    // lose a change outright.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'PUSHED');
    hold.resolve('RESPONSE');
    // Let the record install (bound to A, whose socket is up at that
    // moment), then take the socket down before the continuation
    // reaches the flush: gone at flush time, while the record stays
    // bound to it.
    for (let i = 0; i < 50 && reg.size() === 0; i++) await Promise.resolve();
    env.setClientGone('A');
    await p;
    await flushMicrotasks();

    expect(seen).toEqual(['RESPONSE', 'PUSHED']);
  });
});

describe('SubscriptionRegistry — unsubscribe/resubscribe are serialized per key', () => {
  it('holds a replacement subscribe until the prior wire unsubscribe settles', async () => {
    // Electrum servers may process a session's requests concurrently
    // (ElectrumX INITIAL_CONCURRENT), so "U written before S" does not
    // imply "U executed before S": a late unsubscribe can cancel the
    // replacement's live subscription server-side, silently. A settled
    // unsubscribe response is the only proof the server executed it, so
    // a replacement subscribe for the same key must wait for it.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    const hold = env.holdNextCall();
    await unsub();
    expect(env.callLog.map((c) => c.method)).toEqual([
      'blockchain.scripthash.subscribe',
      'blockchain.scripthash.unsubscribe',
    ]);

    // Replacement while the unsubscribe is still in flight.
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    await flushMicrotasks();
    // No wire subscribe yet — the unsubscribe has not settled.
    expect(env.callLog).toHaveLength(2);

    hold.resolve(true);
    await p;
    expect(env.callLog.map((c) => c.method)).toEqual([
      'blockchain.scripthash.subscribe',
      'blockchain.scripthash.unsubscribe',
      'blockchain.scripthash.subscribe',
    ]);
  });
});

describe('SubscriptionRegistry — the unsubscribe barrier stays safe', () => {
  it('clear() during the barrier wait rejects the subscribe and sends no wire call', async () => {
    // The barrier is an await before the wire subscribe; clear() landing
    // in that window must not let a stray wire subscribe go out on
    // behalf of a registry that no longer exists — the server would hold
    // a subscription with no local record and nobody to unsubscribe it.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    const hold = env.holdNextCall();
    await unsub();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    await flushMicrotasks();
    reg.clear();
    hold.resolve(true);
    await expect(p).rejects.toThrow(/registry cleared/);
    // subscribe, unsubscribe — and nothing after the clear.
    expect(env.callLog).toHaveLength(2);
  });

  it('drops a push buffered during the barrier wait — it predates the wire call', async () => {
    // A push received before the subscribe request is even written
    // cannot be newer than the response; flushing it after the response
    // is the exact spurious delivery the pre-call buffer purge exists
    // to prevent.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'FRESH');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    const hold = env.holdNextCall();
    await unsub();
    const seen: unknown[] = [];
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();
    // Arrives while the replacement is parked on the barrier — before
    // its wire subscribe exists.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'STALE');
    hold.resolve(true);
    await p;
    await flushMicrotasks();
    expect(seen).toEqual(['FRESH']);
  });

  it('clear() releases the barrier for the next epoch', async () => {
    // A subscribe on a reset registry must not block on a wire call
    // from the previous epoch.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    const hold = env.holdNextCall();
    await unsub();
    expect(env.callLog).toHaveLength(2);
    reg.clear();

    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    await flushMicrotasks();
    // Dispatched immediately — the stale barrier is gone.
    expect(env.callLog).toHaveLength(3);
    hold.resolve(true);
    await p;
  });
});

describe('SubscriptionRegistry — unsubscribe is bound to the subscribing session', () => {
  it('skips the wire unsubscribe when the bound session was replaced', async () => {
    // The record remembers which SESSION its subscribe ran on. If that
    // server dropped and reconnected, the fresh session never held the
    // subscription — writing an unsubscribe there is spurious, and its
    // ambiguous timeout would poison (and later retire) a healthy
    // socket.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    // The socket drops and reconnects: same client id, new session.
    reg.clientDisconnected('A');
    env.bumpSession('A');
    // (isClientConnected('A') is true again — the reconnect completed.)

    const callsBefore = env.callLog.length;
    await unsub();
    expect(env.callLog).toHaveLength(callsBefore);
    expect(env.retired).toEqual([]);
  });
});

describe('SubscriptionRegistry — the leader always gets its own response', () => {
  it('replays the initial status even when the serving client died mid-call', async () => {
    // The status is not a stale snapshot — this very call fetched it.
    // Parking the FIRST subscriber in awaitingBaseline because the
    // socket dropped before the continuation ran means subscribe()
    // resolves but the handler never fires until a rebind holds — in an
    // offline single-server pool, never.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    // Server answers, then the socket drops before our continuation —
    // and the pool is empty, so no rebind can deliver anything.
    env.setClientGone('A');
    env.setNoClient();
    hold.resolve('S');
    await p;
    await flushMicrotasks();

    expect(seen).toEqual(['S']);
  });
});

describe('SubscriptionRegistry — coalesced joiners share the leader fallback', () => {
  it('a joiner of the same wire call also gets the fetched status when the pool is empty', async () => {
    // The joiner's status is exactly as fresh as the leader's — the
    // same wire call fetched it. Settling only the leader left
    // subscriber #2 parked in awaitingBaseline, silent until a
    // reconnect that offline never brings.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seenL: unknown[] = [];
    const seenJ: unknown[] = [];
    const hold = env.holdNextCall();
    const p1 = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seenL.push(s));
    const p2 = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seenJ.push(s));
    await flushMicrotasks();

    env.setClientGone('A');
    env.setNoClient();
    hold.resolve('S');
    await Promise.all([p1, p2]);
    await flushMicrotasks();

    expect(seenL).toEqual(['S']);
    expect(seenJ).toEqual(['S']);
  });
});

describe('SubscriptionRegistry — leader dedup survives synchronous re-entry', () => {
  it('a subscribe re-entered from inside retireClient joins the leader instead of forking', async () => {
    // retireClient runs synchronously inside the leader's prefix; in
    // the manager it fires 'client-state' synchronously, and a listener
    // may subscribe the same key re-entrantly. If the leader has not
    // yet registered itself in `pending`, the re-entrant call becomes a
    // SECOND leader: two wire subscribes, and the loser throws a
    // spurious 'registry cleared while subscribing'.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    const hold = env.holdNextCall();
    await unsub();
    hold.reject(new Error('request timed out'));
    await flushMicrotasks();
    // Key is poisoned; the next subscribe will retire the session.

    let reentrant: Promise<unknown> | null = null;
    env.onRetire = () => {
      reentrant = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    };
    const callsBefore = env.callLog.length;
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    await expect(reentrant!).resolves.toBeDefined();
    // One wire subscribe, shared by both callers.
    const subs = env.callLog
      .slice(callsBefore)
      .filter((c) => c.method === 'blockchain.scripthash.subscribe');
    expect(subs).toHaveLength(1);
  });
});

describe('SubscriptionRegistry — an ambiguous unsubscribe poisons its session', () => {
  // A timed-out unsubscribe was written to the session and may still
  // execute later — after a replacement subscribe on that same session,
  // silently cancelling it. Subscriptions are session-scoped, so
  // retiring the socket kills the stray unsubscribe with its session —
  // but retiring EAGERLY would cycle a merely-slow server's connection
  // on every unsubscribe, subscription or not. The hazard only exists
  // when the same key is re-subscribed while the ambiguity stands, so
  // that is the moment the retire fires.
  async function ambiguousUnsub(reason: Error) {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    const hold = env.holdNextCall();
    await unsub();
    hold.reject(reason);
    await flushMicrotasks();
    return { env, reg };
  }

  it('does not retire on the failure alone — no replacement, no hazard', async () => {
    const { env } = await ambiguousUnsub(new Error('request timed out'));
    expect(env.retired).toEqual([]);
  });

  it('retires the poisoned session when the key is re-subscribed', async () => {
    const { env, reg } = await ambiguousUnsub(new Error('request timed out'));
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    expect(env.retired).toEqual(['A']);
  });

  it('a different key does not touch the poisoned session', async () => {
    const { env, reg } = await ambiguousUnsub(new Error('request timed out'));
    await reg.subscribe('blockchain.scripthash.subscribe', ['OTHER'], () => undefined);
    expect(env.retired).toEqual([]);
  });

  it('a disconnect clears the poison — the session died with its stray unsubscribe', async () => {
    const { env, reg } = await ambiguousUnsub(new Error('request timed out'));
    reg.clientDisconnected('A');
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    expect(env.retired).toEqual([]);
  });

  it('does not poison when the session died before the failure settled', async () => {
    // On socket loss the client rejects in-flight calls and THEN
    // publishes 'disconnected'; the rejection's catch runs as a later
    // microtask. Poisoning there would record the DEAD session's client
    // id after clientDisconnected already cleared it — and retire the
    // fresh session that reconnects under the same id. A client that is
    // no longer connected at catch time proves the session (and its
    // stray unsubscribe) died; there is nothing left to race.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    const hold = env.holdNextCall();
    await unsub();
    // The socket drops: disconnect publishes first…
    env.setClientGone('A');
    reg.clientDisconnected('A');
    // …then the in-flight unsubscribe's rejection lands.
    hold.reject(new Error('socket closed'));
    await flushMicrotasks();

    // The server reconnects under the same id; re-subscribing must not
    // retire the fresh session.
    env.setConnected('A');
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    expect(env.retired).toEqual([]);
  });

  it('does not poison when a NEW session connected before the failure settled', async () => {
    // The rejection's catch resumes several microtask hops after the
    // failure; an embedding with a synchronously-connecting transport
    // can have the replacement session up before it runs. "Still
    // connected" alone would then tag the FRESH session — the poison
    // must prove the session is the very one the unsubscribe was
    // written to.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    const hold = env.holdNextCall();
    await unsub();
    // Old session dies and a new one connects before the catch runs.
    env.bumpSession('A');
    hold.reject(new Error('socket closed'));
    await flushMicrotasks();

    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    expect(env.retired).toEqual([]);
  });

  it('does not poison on a settled unsubscribe response', async () => {
    const { env, reg } = await ambiguousUnsub(new RpcError('unknown method', 1));
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    expect(env.retired).toEqual([]);
  });

  it('does not poison when the pin found no connection — nothing was sent', async () => {
    const { env, reg } = await ambiguousUnsub(
      new NoClientAvailableError('pinned client for x is not connected'),
    );
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    expect(env.retired).toEqual([]);
  });

  it('does not poison on a pre-dispatch SuspendedError — nothing was sent', async () => {
    // Teardown intent rejects the call before dispatch; retiring here
    // would force-close a socket mid-grace-drain, failing the very
    // in-flight requests suspend() promised to let finish.
    const { env, reg } = await ambiguousUnsub(
      new SuspendedError('suspending before the request was dispatched'),
    );
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    expect(env.retired).toEqual([]);
  });
});

describe('SubscriptionRegistry — foreign payloads carry no authority', () => {
  it('a header buffered from the answering server itself is stale — the response wins', async () => {
    // The buffered push's order against the response is unknowable (a
    // push coalesced into the response's chunk buffers too), so for a
    // data-bearing status one of two errors is unavoidable — and
    // replaying a stale header REGRESSES the tip the finality gate
    // reads, while dropping a newer one merely waits for the next
    // block. Never-regress wins: buffered headers are dropped, the
    // response is the baseline.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.headers.subscribe', [], (s) => seen.push(s));
    await flushMicrotasks();

    // The answering server ('A') pushes an older tip mid-call — a ghost
    // or duplicate subscription on the same session.
    reg.notify('A', 'blockchain.headers.subscribe', [], { height: 5, hex: 'aa' });
    hold.resolve({ height: 9, hex: 'bb' });
    await p;
    await flushMicrotasks();

    expect(seen).toEqual([{ height: 9, hex: 'bb' }]);

    // And the stored view is the response, not the stale push.
    const joined: unknown[] = [];
    await reg.subscribe('blockchain.headers.subscribe', [], (s) => joined.push(s));
    expect(joined).toEqual([{ height: 9, hex: 'bb' }]);
  });

  it('a purged differing header still forces drift on the rebind', async () => {
    // The purge drops the payload, not the SIGNAL: a differing buffered
    // header proves the tip may have moved while the rebind was in
    // flight, and a consumer trusting drift:false would skip the resync
    // that would catch it — one block of staleness the next push may
    // take many minutes to correct.
    const env = fakeEnv();
    env.setStatus('blockchain.headers.subscribe', [], { height: 7, hex: 'aa' });
    const reg = new SubscriptionRegistry(env);
    await reg.subscribe('blockchain.headers.subscribe', [], () => undefined);

    reg.clientDisconnected('A');
    env.setConnected('C');
    const hold = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();
    // A newer tip races the rebind; the response then matches the old
    // baseline exactly.
    reg.notify('C', 'blockchain.headers.subscribe', [], { height: 8, hex: 'bb' });
    hold.resolve({ height: 7, hex: 'aa' });
    await restoring;
    await flushMicrotasks();

    const restored = env.emitted.filter((e) => e.event === 'subscription-restored');
    expect(restored).toHaveLength(1);
    expect((restored[0]!.payload as { drift: boolean }).drift).toBe(true);
  });

  it('drops a foreign buffered header instead of delivering it', async () => {
    // Electrum subscriptions are connection-scoped: the tip that binds
    // this record is the one its own server reports. A header is not an
    // opaque change signal — the payload IS the data, and the manager's
    // finality gate consumes its height directly. Delivering a buffered
    // header from an unrelated pooled server would let one bad server
    // inflate the tip during any subscribe race and poison finalized
    // cache writes.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.headers.subscribe', [], (s) => seen.push(s));
    await flushMicrotasks();

    // Foreign server B races the subscribe with an inflated tip.
    reg.notify('B', 'blockchain.headers.subscribe', [], { height: 9_999_999, hex: 'ff' });
    hold.resolve({ height: 100, hex: 'aa' });
    await p;
    await flushMicrotasks();

    expect(seen).toEqual([{ height: 100, hex: 'aa' }]);
  });

  it('still delivers a foreign buffered scripthash status — it is an opaque resync signal', async () => {
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'FOREIGN');
    hold.resolve('RESPONSE');
    await p;
    await flushMicrotasks();

    expect(seen).toEqual(['RESPONSE', 'FOREIGN']);
  });

  it('a rebind that flushed a foreign change signal reports drift', async () => {
    // The flushed delivery IS the change signal; an event-driven
    // consumer trusting drift:false would skip the very resync it asked
    // for. Storage keeping the serving view must not erase that signal
    // from the drift computation.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'B');
    const reg = new SubscriptionRegistry(env);
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setConnected('C');
    const hold = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();
    // Old server pushes a change while the rebind call is in flight.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'F');
    // The new server's answer matches the pre-rebind baseline exactly.
    hold.resolve('B');
    await restoring;
    await flushMicrotasks();

    const restored = env.emitted.filter((e) => e.event === 'subscription-restored');
    expect(restored).toHaveLength(1);
    expect((restored[0]!.payload as { drift: boolean }).drift).toBe(true);
  });

  it('a handler subscribed from inside a foreign flush replays the serving view', async () => {
    // Re-entrant attach happens DURING the fan-out; the record's stored
    // status must already be the serving server's at that instant — a
    // restore that runs after the loop hands the joiner the foreign
    // payload as its baseline.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const joined: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => {
      if (s === 'FOREIGN' && joined.length === 0) {
        void reg.subscribe('blockchain.scripthash.subscribe', ['H'], (j) => joined.push(j));
      }
    });
    await flushMicrotasks();

    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'FOREIGN');
    hold.resolve('RESPONSE');
    await p;
    await flushMicrotasks();

    expect(joined[0]).toBe('RESPONSE');
  });

  it('a handler still awaiting its first status is not fed a foreign payload', async () => {
    // awaitingBaseline handlers were promised a status from a live
    // binding; a foreign change signal is not one. They sit out the
    // fan-out and settle on the next authoritative delivery.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'B');
    const reg = new SubscriptionRegistry(env);
    const h1: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => h1.push(s));
    h1.length = 0;

    reg.clientDisconnected('A');
    // Joins while orphaned — no live binding, so its first status waits.
    const h2: unknown[] = [];
    void reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => h2.push(s));
    await flushMicrotasks();

    env.setConnected('C');
    const hold = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();
    reg.notify('X', 'blockchain.scripthash.subscribe', ['H'], 'F');
    // The answering server is already gone when the response lands, so
    // the record stays orphaned and the waiting handler stays waiting.
    env.setClientGone('C');
    hold.resolve('B');
    await flushMicrotasks();

    // Established handler heard the change signal; the waiting one did
    // not have a foreign payload passed off as its first status.
    expect(h1).toEqual(['F']);
    expect(h2).toEqual([]);

    // The record is still orphaned, so the rebind retry loop is live —
    // end it before the test returns.
    reg.clear();
    await restoring.catch(() => undefined);
  });

  it("the stored status stays the serving server's — a joiner is not handed a foreign value", async () => {
    // Deliveries are change signals, but STORAGE is authoritative: the
    // record's lastKnownStatus feeds every later joiner's replay and
    // the dedup baseline, and only the socket that served the
    // subscription owns that view. A foreign flush must not leave its
    // payload as the record's state.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    await flushMicrotasks();

    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'FOREIGN');
    hold.resolve('RESPONSE');
    await p;
    await flushMicrotasks();

    const joined: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => joined.push(s));
    expect(joined).toEqual(['RESPONSE']);

    // Live pushes are never deduplicated, so the serving server
    // confirming its own equal status reaches the handler.
    const seen: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    seen.length = 0;
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'RESPONSE');
    expect(seen).toEqual(['RESPONSE']);
  });
});

describe('SubscriptionRegistry — a tie-break that never answers', () => {
  it('does not report "nothing changed" for an unresolved reconciliation', async () => {
    // The rebind's own answer matched, but a buffered push made the
    // ordering ambiguous and the tie-break asked to settle it — and then
    // failed. Nothing reconciled anything, so reporting drift:false
    // tells a consumer it need not refetch history, when the push it
    // could not order may be exactly the change it would have found.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'SAME');
    const reg = new SubscriptionRegistry(env);
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setConnected('B');
    const rebindCall = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();

    reg.notify('B', 'blockchain.scripthash.subscribe', ['H'], 'NEWER_PUSH');
    const refetch = env.holdNextCall();
    rebindCall.resolve('SAME');
    await restoring;
    await flushMicrotasks();

    refetch.reject(new Error('tie-break failed'));
    await flushMicrotasks();

    expect(env.emitted).toEqual([
      {
        event: 'subscription-restored',
        payload: { method: 'blockchain.scripthash.subscribe', params: ['H'], drift: true },
      },
    ]);
  });
});

describe('SubscriptionRegistry — stale-rebind cleanup with a pending successor', () => {
  it('never risks unsubscribing a server the successor may end up on', async () => {
    // Ownership of the old server's subscription is undecidable while a
    // successor exists or is mid-subscribe: it may land — or later
    // rebind — exactly there, and a deferred re-check acts on a snapshot
    // the next transition can invalidate (one such deferred cleanup was
    // observed unsubscribing the very server a successor had just
    // rebound to). The cleanup is deliberately conservative: a leaked
    // subscription pushes into the void until its connection closes,
    // while a wrong unsubscribe silently kills a live one — missed
    // transactions with nothing to correct them.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setConnected('B');
    const staleRebind = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();

    await unsub();
    env.setConnected('C');
    const successorCall = env.holdNextCall();
    const successor = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    await flushMicrotasks();

    // B reachable again BEFORE the stale rebind settles: the cleanup's
    // wire unsubscribe would go to B and would not be skipped as
    // unreachable — so restraint is the only thing preventing it.
    env.setConnected('B');
    staleRebind.resolve('STALE');
    await restoring;
    await flushMicrotasks();

    successorCall.resolve('FROM_C');
    await successor;
    await flushMicrotasks();

    const unsubs = env.callLog.filter((c2) => c2.method === 'blockchain.scripthash.unsubscribe');
    expect(unsubs).toHaveLength(0);
  });

  it('does unsubscribe when nobody could own the old subscription', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setConnected('B');
    const staleRebind = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();

    // Dropped with no successor at all: the stale rebind's subscription
    // on B provably has no owner.
    await unsub();
    staleRebind.resolve('STALE');
    await restoring;
    await flushMicrotasks();

    const unsubs = env.callLog.filter((c2) => c2.method === 'blockchain.scripthash.unsubscribe');
    expect(unsubs).toHaveLength(1);
  });
});

describe('SubscriptionRegistry — a repeated status after an unorderable flush', () => {
  it('delivers every equal push — live pushes are never deduplicated', async () => {
    // Scripthash statuses REPEAT without any reorg: a mempool tx dropped
    // (RBF, expiry, conflict) restores the exact prior history and the
    // server pushes the old status again. If an unorderable buffered
    // push left `lastKnownStatus` on that value, the dedup swallowed the
    // rollback and the consumer stayed on the newer state indefinitely.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    // Push H1 and response H2 land unorderably; both are delivered.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'H1');
    hold.resolve('H2');
    await p;
    await flushMicrotasks();
    expect(seen).toEqual(['H2', 'H1']);

    // Mempool rollback: the server genuinely pushes H1 again — reaches
    // the handler.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'H1');
    expect(seen).toEqual(['H2', 'H1', 'H1']);

    // Live pushes are NEVER deduplicated (the protocol mandates
    // delivery even when the status is unchanged — same-height reorg);
    // a further equal push delivers too.
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'H1');
    expect(seen).toEqual(['H2', 'H1', 'H1', 'H1']);
  });

  it('lets a rebind answer equal to the uncertain baseline through, as drift', async () => {
    // The other dedup site: after a reconnect, the rebind's answer may
    // equal the flushed unorderable value. Suppressing it would leave a
    // consumer parked on the other candidate with no synthetic
    // notification — and the restore event must say drift, because the
    // forced delivery IS the change signal an event-driven consumer
    // resyncs on.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'H1');
    hold.resolve('H2');
    await p;
    await flushMicrotasks();
    expect(seen).toEqual(['H2', 'H1']);

    // Reconnect; the rebind answers the same H1 the flush left behind.
    reg.clientDisconnected('A');
    env.setConnected('B');
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'H1');
    await reg.restoreOrphans();
    await flushMicrotasks();

    expect(seen).toEqual(['H2', 'H1', 'H1']);
    expect(env.emitted).toEqual([
      {
        event: 'subscription-restored',
        payload: { method: 'blockchain.scripthash.subscribe', params: ['H'], drift: true },
      },
    ]);
  });
});

describe('SubscriptionRegistry — record identity across replacements', () => {
  it('does not attach or resolve when clear() lands after the record is installed', async () => {
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    const hold = env.holdNextCall();
    const p = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));
    await flushMicrotasks();

    // Resolve the wire call and let the registry's own task install the
    // record, but clear before the subscribe() continuation that attaches
    // the handler resumes — three microtask hops lands exactly there.
    // Drive to the seam by observation, not by counting microtasks: the
    // record becomes visible when the registry's own task installs it,
    // and the handler is only called once subscribe()'s continuation
    // resumes. Microtask scheduling differs between engines (this suite
    // also runs on-device under Hermes), so watch for the state instead
    // of assuming the hop count that produces it.
    hold.resolve('LATE');
    for (let i = 0; i < 50 && reg.size() === 0; i++) await Promise.resolve();
    const insideWindow = reg.size() === 1 && seen.length === 0;
    reg.clear();

    if (insideWindow) {
      await expect(p).rejects.toThrow(/registry cleared/);
      expect(seen).toEqual([]);
    } else {
      // The continuation beat us to it: the subscribe completed before
      // the teardown, which is the ordinary ordering and not this test's
      // subject. `clear()` still has to leave nothing behind.
      await p.catch(() => undefined);
    }
    expect(reg.size()).toBe(0);
  });

  it('a handle from a previous registration does not tear down its replacement', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const handler = vi.fn();

    const first = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    const alsoFirst = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    await first();
    expect(reg.size()).toBe(0);

    // Same key, same handler reference — a new registration entirely.
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
    await alsoFirst();

    expect(reg.size()).toBe(1);
    handler.mockClear();
    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'STILL_LIVE');
    expect(handler).toHaveBeenCalledWith('STILL_LIVE');
  });

  it('re-subscribing the same handler from inside its own callback does not recurse', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const calls: unknown[] = [];
    const handler = (s: unknown): void => {
      calls.push(s);
      if (s === 'PUSH' && calls.length < 10) {
        void reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);
      }
    };
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], handler);

    reg.notify('A', 'blockchain.scripthash.subscribe', ['H'], 'PUSH');
    await flushMicrotasks();

    expect(calls).toEqual(['INIT', 'PUSH']);
  });

  it('a stale rebind does not unsubscribe a successor that owns the same server', async () => {
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const unsub = await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);

    reg.clientDisconnected('A');
    env.setConnected('B');
    const stale = env.holdNextCall();
    const staleTask = reg.restoreOrphans();
    await flushMicrotasks();
    await unsub();

    // A replacement subscribes on the very server the stale rebind used.
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => undefined);
    const before = env.callLog.length;

    stale.resolve('STALE');
    await staleTask;
    await flushMicrotasks();

    // Electrum keys subscriptions by method+params per connection, so an
    // unsubscribe here would silence the live replacement on B.
    expect(env.callLog.slice(before).map((c) => c.method)).not.toContain(
      'blockchain.scripthash.unsubscribe',
    );
  });
});

describe('SubscriptionRegistry — coalesced subscribers and teardown', () => {
  it('a coalesced subscriber does not attach after clear()', async () => {
    // Two callers share one wire subscribe. The first one's handler tears
    // the manager down from inside its own replay; the second caller's
    // continuation then ran against a record the registry had already
    // dropped — its handler fired after shutdown and it received an
    // unsubscribe handle owning nothing.
    const env = fakeEnv();
    const reg = new SubscriptionRegistry(env);
    const hold = env.holdNextCall();
    const second: unknown[] = [];

    const first = reg.subscribe('blockchain.scripthash.subscribe', ['H'], () => {
      reg.clear();
    });
    const joiner = reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => second.push(s));
    await flushMicrotasks();

    hold.resolve('S0');
    await first.catch(() => undefined);
    await expect(joiner).rejects.toThrow(/registry cleared/);

    expect(second).toEqual([]);
    expect(reg.size()).toBe(0);
  });
});

describe('SubscriptionRegistry — a rebind that lands on a dead server', () => {
  it('delivers the status but does not announce a restore while still orphaned', async () => {
    // B answers the subscribe and stops being usable before our
    // continuation runs, so the record cannot bind to it. The status it
    // returned is real and still gets delivered — dropping it would lose
    // the only fresh view we got, and on a single-server pool nothing
    // would fetch it again — but announcing `subscription-restored`
    // would tell consumers the subscription is live while nothing is
    // pushing to it.
    const env = fakeEnv();
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'INIT');
    const reg = new SubscriptionRegistry(env);
    const seen: unknown[] = [];
    await reg.subscribe('blockchain.scripthash.subscribe', ['H'], (s) => seen.push(s));

    reg.clientDisconnected('A');
    env.setConnected('B');
    const hold = env.holdNextCall();
    const restoring = reg.restoreOrphans();
    await flushMicrotasks();
    // The rebind really went out — the assertions below are about what
    // its answer does, not about it never happening.
    expect(env.callLog.map((c) => c.method)).toContain('blockchain.scripthash.subscribe');

    // B answers, then `setNoClient` makes `isClientConnected('B')` false:
    // the record has nowhere to bind when we resume.
    hold.resolve('AFTER');
    env.setNoClient();
    await restoring;
    await flushMicrotasks();

    expect(seen).toEqual(['INIT', 'AFTER']);
    expect(env.emitted).toEqual([]);

    // Deferred, not dropped: once a bind holds, the restore is announced.
    env.setConnected('C');
    env.setStatus('blockchain.scripthash.subscribe', ['H'], 'LATER');
    await reg.restoreOrphans();
    await flushMicrotasks();

    expect(seen).toEqual(['INIT', 'AFTER', 'LATER']);
    expect(env.emitted.map((e) => e.event)).toEqual(['subscription-restored']);
  });
});
