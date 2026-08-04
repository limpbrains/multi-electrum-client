// Built-in RoutingPolicy factories.
//
// Each factory returns a fresh `RoutingPolicy` (so internal cursors / sticky
// maps are not shared between Manager instances). Custom policies are plain
// objects — see RoutingPolicy in `./types.ts`. We deliberately do not ship a
// `compose()` DSL: function composition in JS already covers everything users
// reasonably need.

import type { ClientId, ClientView } from '../client.js';
import type { PickContext, RoutingPolicy } from './types.js';

/** Cycle through eligible (connected, non-banned, non-excluded) clients. */
export function roundRobin(): RoutingPolicy {
  let cursor = 0;
  return {
    pick(ctx) {
      const { probe } = ctx;
      const eligible = usableCandidates(ctx);
      if (eligible.length === 0) return null;
      const picked = eligible[cursor % eligible.length]!;
      // Probe picks are side-effect-free: a discarded hedge probe must not
      // shift future routing (see PickContext.probe).
      if (!probe) cursor++;
      return picked.id;
    },
  };
}

/**
 * Try clients in `orderHint` order; first eligible wins. Without an order hint
 * the manager's own pool order is used. Designed for "primary + fallbacks"
 * setups where one server is preferred until it goes down.
 */
export function failover(orderHint?: readonly ClientId[]): RoutingPolicy {
  return {
    pick(ctx) {
      const eligible = usableCandidates(ctx);
      if (eligible.length === 0) return null;
      if (orderHint) {
        for (const id of orderHint) {
          const c = eligible.find((e) => e.id === id);
          if (c) return c.id;
        }
      }
      return eligible[0]!.id;
    },
  };
}

export interface PreferFastestOpts {
  /**
   * Clients within this percent of the best EMA are treated as tied. 0 = strict
   * argmin. Example: 20 → all clients with EMA ≤ 1.2 × bestEMA tie.
   */
  withinPct?: number;
  /** How to break ties between clients with comparable latency. */
  tiebreak?: 'rr' | 'leastInFlight';
}

/**
 * Pick the client with the lowest latency EMA among clients that have at least
 * one sample. Untested clients (samples=0) are *also* admitted to the tied set
 * so a fresh server gets a real chance to be picked, but they do not pull the
 * threshold to zero — that would let an untested server monopolize the pool
 * forever and hide a known-fast tested server. Ties are broken by
 * `leastInFlight` (default) or `rr`.
 */
export function preferFastest(opts: PreferFastestOpts = {}): RoutingPolicy {
  const withinPct = opts.withinPct ?? 0;
  const tiebreak = opts.tiebreak ?? 'leastInFlight';
  let cursor = 0;
  return {
    pick(ctx) {
      const { probe } = ctx;
      const eligible = usableCandidates(ctx);
      if (eligible.length === 0) return null;

      // Single pass over the per-request hot loop: min tested EMA without
      // the filter/map/spread allocations.
      let minTested = Number.POSITIVE_INFINITY;
      for (const c of eligible) {
        const { samples, ema } = c.telemetry.latency;
        if (samples > 0 && ema < minTested) minTested = ema;
      }
      const threshold =
        minTested === Number.POSITIVE_INFINITY
          ? Number.POSITIVE_INFINITY
          : minTested * (1 + withinPct / 100);

      const tied = eligible.filter((c) => {
        if (c.telemetry.latency.samples === 0) return true; // fresh: always eligible
        return c.telemetry.latency.ema <= threshold;
      });

      if (tied.length === 1) return tied[0]!.id;
      if (tiebreak === 'rr') {
        const picked = tied[cursor % tied.length]!;
        // Same probe rule as roundRobin: no cursor movement on probes.
        if (!probe) cursor++;
        return picked.id;
      }
      // leastInFlight
      let best = tied[0]!;
      for (let i = 1; i < tied.length; i++) {
        if (tied[i]!.telemetry.inFlight < best.telemetry.inFlight) best = tied[i]!;
      }
      return best.id;
    },
  };
}

export type StickyKeyFn = (req: {
  method: string;
  params: readonly unknown[];
}) => string | undefined;

/**
 * Wrap an inner policy so that requests sharing a derived key route to the
 * same client. Useful for subscriptions: a `blockchain.scripthash.subscribe`
 * for a given scripthash must keep landing on the same server so notifications
 * stay glued to that server's session.
 *
 * Pass `'scripthash'` for the common case (extracts the first param of any
 * `blockchain.scripthash.*` method) or a custom function for everything else.
 *
 * Pin lifecycle. A pin moves in two cases: the pinned client is genuinely
 * unusable (removed from the pool, not connected, or banned), or a REAL
 * failover retry excludes it — the pinned server just failed this key's
 * request, and keeping the pin would send every future call back into the
 * same timeout. In both cases the fresh pick becomes the new pin. Probe
 * picks (`ctx.probe` — the manager's hedge picks, which may be discarded
 * on divergence or lose the race to a live primary) route elsewhere for
 * that pick WITHOUT touching the pin, and never create one: re-pinning
 * there would silently glue the key to a server that may never have seen
 * the request.
 */
export function withSticky(inner: RoutingPolicy, key: 'scripthash' | StickyKeyFn): RoutingPolicy {
  const keyFn: StickyKeyFn = key === 'scripthash' ? scripthashKey : key;
  const pins = new Map<string, ClientId>();
  const wrapped: RoutingPolicy = {
    pick(ctx) {
      const k = keyFn(ctx.request);
      if (k !== undefined) {
        const pinned = pins.get(k);
        if (pinned !== undefined) {
          const cv = ctx.candidates.find((c) => c.id === pinned);
          if (cv && isUsable(cv, ctx.excluded, ctx.now)) return cv.id;
          if (ctx.probe) {
            // Probe picks are side-effect-free WHATEVER state the pinned
            // client is in — excluded, banned, disconnected, or gone. A
            // possibly-discarded hedge probe must not delete the pin; if
            // the client is genuinely unusable the next REAL pick will
            // drop and re-home it.
            return inner.pick(ctx);
          }
          // Pinned client is unusable, or a real failover retry excluded
          // it after a failure: drop the pin and re-pin to the fresh pick.
          pins.delete(k);
        }
      }
      const next = inner.pick(ctx);
      // Probe picks never create or move pins.
      if (next !== null && k !== undefined && !ctx.probe) pins.set(k, next);
      return next;
    },
  };
  if (inner.onOutcome) {
    wrapped.onOutcome = (o) => inner.onOutcome!(o);
  }
  return wrapped;
}

function scripthashKey(req: { method: string; params: readonly unknown[] }): string | undefined {
  if (!req.method.startsWith('blockchain.scripthash.')) return undefined;
  const first = req.params[0];
  return typeof first === 'string' ? first : undefined;
}

/** Shared eligibility filter: connected, non-banned, non-excluded. */
function usableCandidates({
  candidates,
  excluded,
  now,
}: Pick<PickContext, 'candidates' | 'excluded' | 'now'>): ClientView[] {
  return candidates.filter((c) => isUsable(c, excluded, now));
}

function isUsable(c: ClientView, excluded: ReadonlySet<ClientId>, now: number): boolean {
  if (excluded.has(c.id)) return false;
  if (c.state !== 'connected') return false;
  if (c.bannedUntil !== undefined && c.bannedUntil > now) return false;
  return true;
}
