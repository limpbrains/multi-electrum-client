// Property-based tests for the built-in routing policies.
//
// Concrete cases live in `builtins.test.ts`; these check invariants
// that must hold for *any* candidate set / excluded mask / latency mix:
//
//   1. `pick(ctx)` never returns a candidate in `excluded`.
//   2. `pick(ctx)` never returns a candidate that's `banned` /
//      `disconnected` / has an unexpired `bannedUntil`.
//   3. `pick(ctx)` returns either `null` or an `id` that *exists* in
//      `candidates`.
//   4. `withSticky` is sticky: once a key has a pinned client and that
//      client stays usable, the same key returns the same id.

import fc from 'fast-check';
import { describe, expect, it } from 'vitest';

import type { ClientId, ClientView } from '../../../src/client.js';
import { failover, preferFastest, roundRobin, withSticky } from '../../../src/policy/builtins.js';
import type { PickContext, RoutingPolicy } from '../../../src/policy/types.js';

const NOW = 1_000_000;

const arbState = fc.constantFrom<ClientView['state']>(
  'connecting',
  'connected',
  'disconnected',
  'banned',
);

const arbView = (id: string): fc.Arbitrary<ClientView> =>
  fc
    .record({
      state: arbState,
      // bannedUntil either undefined, expired, or active.
      bannedUntil: fc.option(
        fc.oneof(
          fc.integer({ min: 0, max: NOW - 1 }), // expired
          fc.integer({ min: NOW + 1, max: NOW + 1_000_000 }), // active
        ),
        { nil: undefined },
      ),
      ema: fc.float({ min: 1, max: 1_000, noNaN: true }),
      samples: fc.integer({ min: 0, max: 100 }),
      inFlight: fc.integer({ min: 0, max: 50 }),
    })
    .map(({ state, bannedUntil, ema, samples, inFlight }) => ({
      id,
      endpoint: { host: id, port: 50001, protocol: 'ws' },
      state,
      ...(bannedUntil !== undefined ? { bannedUntil } : {}),
      capabilities: {},
      telemetry: {
        latency: { ema, p50: ema, p95: ema, samples },
        errors: { rate: 0, consecutive: 0 },
        success: { count: samples },
        inFlight,
      },
    }));

const arbCandidates = (): fc.Arbitrary<ClientView[]> =>
  fc
    .integer({ min: 1, max: 8 })
    .chain((n) =>
      fc.tuple(...Array.from({ length: n }, (_, i) => arbView(`c${i}`))).map((arr) => [...arr]),
    );

const arbExcluded = (cs: ClientView[]): fc.Arbitrary<ReadonlySet<ClientId>> =>
  fc.subarray(cs.map((c) => c.id)).map((ids) => new Set(ids));

function makeCtx(candidates: ClientView[], excluded: ReadonlySet<ClientId>): PickContext {
  return {
    request: { method: 'server.ping', params: [] },
    attempt: 0,
    excluded,
    candidates,
    now: NOW,
  };
}

function isUsable(c: ClientView, excluded: ReadonlySet<ClientId>): boolean {
  if (excluded.has(c.id)) return false;
  if (c.state !== 'connected') return false;
  if (c.bannedUntil !== undefined && c.bannedUntil > NOW) return false;
  return true;
}

const POLICIES: { name: string; build: () => RoutingPolicy }[] = [
  { name: 'roundRobin', build: () => roundRobin() },
  { name: 'failover (no order)', build: () => failover() },
  {
    name: 'failover (reverse)',
    build: () => failover(['c7', 'c6', 'c5', 'c4', 'c3', 'c2', 'c1', 'c0']),
  },
  { name: 'preferFastest (strict)', build: () => preferFastest() },
  { name: 'preferFastest (within 20%)', build: () => preferFastest({ withinPct: 20 }) },
  { name: 'preferFastest (rr tiebreak)', build: () => preferFastest({ tiebreak: 'rr' }) },
];

describe.each(POLICIES)(
  'property: $name pick respects excluded / banned / disconnected',
  ({ build }) => {
    it('never picks an excluded, disconnected, or banned candidate', () => {
      fc.assert(
        fc.property(
          arbCandidates().chain((cs) => arbExcluded(cs).map((excluded) => ({ cs, excluded }))),
          ({ cs, excluded }) => {
            const policy = build();
            // Drive a few picks so internal cursors get exercised.
            for (let i = 0; i < 5; i++) {
              const id = policy.pick(makeCtx(cs, excluded));
              if (id === null) {
                // null is only valid when no candidate is usable.
                expect(cs.every((c) => !isUsable(c, excluded))).toBe(true);
                continue;
              }
              const cv = cs.find((c) => c.id === id);
              expect(cv).toBeDefined();
              expect(isUsable(cv!, excluded)).toBe(true);
            }
          },
        ),
        { numRuns: 100 },
      );
    });
  },
);

describe('property: withSticky pins by key', () => {
  it('returns the same id for repeated picks with the same scripthash key', () => {
    fc.assert(
      fc.property(
        arbCandidates(),
        fc.string({ minLength: 1, maxLength: 32 }).filter((s) => s.length > 0),
        (cs, hash) => {
          // Force at least one usable candidate so the test isn't trivially
          // satisfied by `null`. fast-check shrinkers will still hit edge
          // cases via the predicate filter.
          if (!cs.some((c) => isUsable(c, new Set()))) return;

          const sticky = withSticky(roundRobin(), 'scripthash');
          const ctx = (): PickContext => ({
            request: { method: 'blockchain.scripthash.subscribe', params: [hash] },
            attempt: 0,
            excluded: new Set(),
            candidates: cs,
            now: NOW,
          });
          const first = sticky.pick(ctx());
          if (first === null) return; // no usable client, nothing to pin
          for (let i = 0; i < 5; i++) {
            expect(sticky.pick(ctx())).toBe(first);
          }
        },
      ),
      { numRuns: 50 },
    );
  });

  it('drops the pin if the pinned client becomes unusable, then re-pins', () => {
    fc.assert(
      fc.property(arbCandidates(), (cs) => {
        // Need at least two usable candidates so we have somewhere to fail
        // over to.
        const usable = cs.filter((c) => isUsable(c, new Set()));
        if (usable.length < 2) return;

        const sticky = withSticky(roundRobin(), 'scripthash');
        const hash = 'aabbccdd';
        const baseCtx = (overrides: Partial<PickContext> = {}): PickContext => ({
          request: { method: 'blockchain.scripthash.subscribe', params: [hash] },
          attempt: 0,
          excluded: new Set(),
          candidates: cs,
          now: NOW,
          ...overrides,
        });
        const first = sticky.pick(baseCtx())!;
        // Exclude `first` — sticky should pick a different usable client.
        const second = sticky.pick(baseCtx({ excluded: new Set([first]) }));
        expect(second).not.toBe(first);
        expect(second).not.toBeNull();
      }),
      { numRuns: 50 },
    );
  });
});

describe('property: failover with orderHint prefers earlier ids', () => {
  it('returns the earliest usable id from the hint', () => {
    fc.assert(
      fc.property(arbCandidates(), (cs) => {
        const ids = cs.map((c) => c.id);
        const policy = failover(ids);
        const id = policy.pick(makeCtx(cs, new Set()));
        if (id === null) {
          expect(cs.every((c) => !isUsable(c, new Set()))).toBe(true);
          return;
        }
        // Must be the FIRST usable id in `ids` order.
        const expected = ids.find((cid) => isUsable(cs.find((c) => c.id === cid)!, new Set()));
        expect(id).toBe(expected);
      }),
      { numRuns: 100 },
    );
  });
});
