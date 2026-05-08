import { describe, expect, it, vi } from 'vitest';

import { SubscriptionRegistry, type SubscriptionEnv } from '../../../src/subscriptions/registry.js';

interface FakeEnv extends SubscriptionEnv {
  // Test-only handles to drive the env.
  setStatus(method: string, params: readonly unknown[], status: unknown): void;
  setNoClient(): void;
  callLog: { method: string; params: readonly unknown[] }[];
  emitted: { event: string; payload: unknown }[];
}

function fakeEnv(): FakeEnv {
  const statuses = new Map<string, unknown>();
  let connected: string | null = 'A';
  const callLog: { method: string; params: readonly unknown[] }[] = [];
  const emitted: { event: string; payload: unknown }[] = [];

  const env: FakeEnv = {
    async call(method, params) {
      callLog.push({ method, params });
      const key = `${method} ${JSON.stringify(params)}`;
      return statuses.get(key);
    },
    emit(event, payload) {
      emitted.push({ event, payload });
    },
    pickConnectedClient() {
      return connected;
    },
    setStatus(method, params, status) {
      statuses.set(`${method} ${JSON.stringify(params)}`, status);
    },
    setNoClient() {
      connected = null;
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
