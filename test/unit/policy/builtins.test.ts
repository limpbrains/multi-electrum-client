import { describe, expect, it } from 'vitest';

import type { ClientView } from '../../../src/client.js';
import { failover, preferFastest, roundRobin, withSticky } from '../../../src/policy/builtins.js';
import type { PickContext } from '../../../src/policy/types.js';

function view(id: string, overrides: Partial<ClientView> = {}): ClientView {
  return {
    id,
    endpoint: { host: id, port: 50001, protocol: 'ws' },
    state: 'connected',
    capabilities: {},
    telemetry: {
      latency: { ema: 100, p50: 100, p95: 100, samples: 1 },
      errors: { rate: 0, consecutive: 0 },
      success: { count: 0 },
      inFlight: 0,
    },
    ...overrides,
  };
}

function ctx(candidates: ClientView[], overrides: Partial<PickContext> = {}): PickContext {
  return {
    request: { method: 'server.ping', params: [] },
    attempt: 0,
    excluded: new Set(),
    candidates,
    now: Date.now(),
    ...overrides,
  };
}

describe('roundRobin', () => {
  it('cycles through eligible clients', () => {
    const policy = roundRobin();
    const cs = [view('a'), view('b'), view('c')];
    const seen: string[] = [];
    for (let i = 0; i < 6; i++) {
      seen.push(policy.pick(ctx(cs))!);
    }
    expect(seen).toEqual(['a', 'b', 'c', 'a', 'b', 'c']);
  });

  it('skips disconnected and banned clients', () => {
    const policy = roundRobin();
    const cs = [
      view('a', { state: 'disconnected' }),
      view('b'),
      view('c', { state: 'banned' }),
      view('d'),
    ];
    const seen: string[] = [];
    for (let i = 0; i < 4; i++) {
      seen.push(policy.pick(ctx(cs))!);
    }
    expect(seen).toEqual(['b', 'd', 'b', 'd']);
  });

  it('respects excluded set', () => {
    const policy = roundRobin();
    const cs = [view('a'), view('b'), view('c')];
    const result = policy.pick(ctx(cs, { excluded: new Set(['a', 'b']) }));
    expect(result).toBe('c');
  });

  it('returns null when no eligible client', () => {
    const policy = roundRobin();
    const cs = [view('a', { state: 'disconnected' })];
    expect(policy.pick(ctx(cs))).toBeNull();
  });

  it('skips clients banned via bannedUntil > now', () => {
    const policy = roundRobin();
    const future = Date.now() + 60_000;
    const cs = [view('a', { bannedUntil: future }), view('b')];
    expect(policy.pick(ctx(cs))).toBe('b');
    expect(policy.pick(ctx(cs))).toBe('b');
  });
});

describe('failover', () => {
  it('picks first eligible in orderHint', () => {
    const policy = failover(['c', 'a', 'b']);
    const cs = [view('a'), view('b'), view('c')];
    expect(policy.pick(ctx(cs))).toBe('c');
  });

  it('falls back to next hint when first is ineligible', () => {
    const policy = failover(['a', 'b']);
    const cs = [view('a', { state: 'disconnected' }), view('b')];
    expect(policy.pick(ctx(cs))).toBe('b');
  });

  it('without orderHint, picks first eligible candidate', () => {
    const policy = failover();
    const cs = [view('a'), view('b')];
    expect(policy.pick(ctx(cs))).toBe('a');
    expect(policy.pick(ctx(cs))).toBe('a');
  });
});

describe('preferFastest', () => {
  it('picks the lowest EMA', () => {
    const policy = preferFastest();
    const cs = [
      view('a', {
        telemetry: {
          ...view('a').telemetry,
          latency: { ema: 200, p50: 200, p95: 200, samples: 5 },
        },
      }),
      view('b', {
        telemetry: { ...view('b').telemetry, latency: { ema: 50, p50: 50, p95: 50, samples: 5 } },
      }),
      view('c', {
        telemetry: {
          ...view('c').telemetry,
          latency: { ema: 100, p50: 100, p95: 100, samples: 5 },
        },
      }),
    ];
    expect(policy.pick(ctx(cs))).toBe('b');
  });

  it('breaks ties by leastInFlight by default', () => {
    const policy = preferFastest();
    const cs = [
      view('a', {
        telemetry: {
          ...view('a').telemetry,
          latency: { ema: 100, p50: 100, p95: 100, samples: 5 },
          inFlight: 3,
        },
      }),
      view('b', {
        telemetry: {
          ...view('b').telemetry,
          latency: { ema: 100, p50: 100, p95: 100, samples: 5 },
          inFlight: 1,
        },
      }),
    ];
    expect(policy.pick(ctx(cs))).toBe('b');
  });

  it('admits untested (samples=0) clients to the tied set without monopolizing', () => {
    const policy = preferFastest();
    // Untested b should not pull the threshold to 0 (which would have hidden a
    // entirely under the old logic). Both clients land in the tied set; the
    // leastInFlight tiebreak (both 0) returns the first.
    const cs = [
      view('a', {
        telemetry: { ...view('a').telemetry, latency: { ema: 50, p50: 50, p95: 50, samples: 5 } },
      }),
      view('b', {
        telemetry: { ...view('b').telemetry, latency: { ema: 0, p50: 0, p95: 0, samples: 0 } },
      }),
    ];
    expect(policy.pick(ctx(cs))).toBe('a');
  });

  it('untested wins via leastInFlight when a tested client is busy', () => {
    const policy = preferFastest();
    const cs = [
      view('a', {
        telemetry: {
          ...view('a').telemetry,
          latency: { ema: 50, p50: 50, p95: 50, samples: 5 },
          inFlight: 5,
        },
      }),
      view('b', {
        telemetry: {
          ...view('b').telemetry,
          latency: { ema: 0, p50: 0, p95: 0, samples: 0 },
          inFlight: 0,
        },
      }),
    ];
    expect(policy.pick(ctx(cs))).toBe('b');
  });

  it('all-untested falls back to leastInFlight / first-wins', () => {
    const policy = preferFastest();
    const cs = [
      view('a', {
        telemetry: {
          ...view('a').telemetry,
          latency: { ema: 0, p50: 0, p95: 0, samples: 0 },
          inFlight: 0,
        },
      }),
      view('b', {
        telemetry: {
          ...view('b').telemetry,
          latency: { ema: 0, p50: 0, p95: 0, samples: 0 },
          inFlight: 0,
        },
      }),
    ];
    expect(policy.pick(ctx(cs))).toBe('a');
  });

  it('withinPct widens the tied set', () => {
    const policy = preferFastest({ withinPct: 50 });
    const cs = [
      view('a', {
        telemetry: {
          ...view('a').telemetry,
          latency: { ema: 100, p50: 100, p95: 100, samples: 5 },
          inFlight: 0,
        },
      }),
      view('b', {
        telemetry: {
          ...view('b').telemetry,
          latency: { ema: 140, p50: 140, p95: 140, samples: 5 },
          inFlight: 5,
        },
      }),
    ];
    // a has 0 inflight, both within 50%, leastInFlight tiebreak -> a.
    expect(policy.pick(ctx(cs))).toBe('a');
  });
});

describe('withSticky', () => {
  it('routes same scripthash key to the same client', () => {
    const inner = roundRobin();
    const policy = withSticky(inner, 'scripthash');
    const cs = [view('a'), view('b')];
    const reqA: PickContext = ctx(cs, {
      request: { method: 'blockchain.scripthash.get_balance', params: ['HASH1'] },
    });
    const reqB: PickContext = ctx(cs, {
      request: { method: 'blockchain.scripthash.get_balance', params: ['HASH2'] },
    });
    const reqAagain: PickContext = ctx(cs, {
      request: { method: 'blockchain.scripthash.subscribe', params: ['HASH1'] },
    });

    const first = policy.pick(reqA)!;
    const second = policy.pick(reqB)!;
    const repeat = policy.pick(reqAagain)!;
    expect(repeat).toBe(first);
    expect(first).not.toBe(second);
  });

  it('re-pins when the pinned client becomes ineligible', () => {
    const inner = roundRobin();
    const policy = withSticky(inner, 'scripthash');
    const cs1 = [view('a'), view('b')];
    const reqWith = (cs: ClientView[]): PickContext =>
      ctx(cs, {
        request: { method: 'blockchain.scripthash.get_balance', params: ['HASH'] },
      });

    const initial = policy.pick(reqWith(cs1))!;
    // Make initial pick disconnected on next call.
    const cs2 = cs1.map((c) => (c.id === initial ? { ...c, state: 'disconnected' as const } : c));
    const next = policy.pick(reqWith(cs2))!;
    expect(next).not.toBe(initial);
  });

  it('passes through unrelated methods to inner without pinning', () => {
    const inner = roundRobin();
    const policy = withSticky(inner, 'scripthash');
    const cs = [view('a'), view('b')];
    const r: PickContext = ctx(cs, { request: { method: 'server.ping', params: [] } });
    const a = policy.pick(r);
    const b = policy.pick(r);
    expect(a).not.toBe(b); // RR alternates, no pin to interfere
  });
});
