// Built-in RoutingPolicy factories.
//
// Each factory returns a fresh `RoutingPolicy` (so internal cursors / sticky
// maps are not shared between Manager instances). Custom policies are plain
// objects — see RoutingPolicy in `./types.ts`. We deliberately do not ship a
// `compose()` DSL: function composition in JS already covers everything users
// reasonably need.

import type { ClientId, ClientView } from '../client.js';
import type { RoutingPolicy } from './types.js';

/** Cycle through eligible (connected, non-banned, non-excluded) clients. */
export function roundRobin(): RoutingPolicy {
  let cursor = 0;
  return {
    pick({ candidates, excluded, now }) {
      const eligible = candidates.filter((c) => isUsable(c, excluded, now));
      if (eligible.length === 0) return null;
      const picked = eligible[cursor % eligible.length]!;
      cursor++;
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
    pick({ candidates, excluded, now }) {
      const eligible = candidates.filter((c) => isUsable(c, excluded, now));
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
    pick({ candidates, excluded, now }) {
      const eligible = candidates.filter((c) => isUsable(c, excluded, now));
      if (eligible.length === 0) return null;

      const tested = eligible.filter((c) => c.telemetry.latency.samples > 0);
      const minTested =
        tested.length > 0
          ? Math.min(...tested.map((c) => c.telemetry.latency.ema))
          : Number.POSITIVE_INFINITY;
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
        cursor++;
        return picked.id;
      }
      // leastInFlight
      let best = tied[0]!;
      for (const c of tied.slice(1)) {
        if (c.telemetry.inFlight < best.telemetry.inFlight) best = c;
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
          // Pinned client is unusable; drop the pin and re-pick below.
          pins.delete(k);
        }
      }
      const next = inner.pick(ctx);
      if (next !== null && k !== undefined) pins.set(k, next);
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

function isUsable(c: ClientView, excluded: ReadonlySet<ClientId>, now: number): boolean {
  if (excluded.has(c.id)) return false;
  if (c.state !== 'connected') return false;
  if (c.bannedUntil !== undefined && c.bannedUntil > now) return false;
  return true;
}
