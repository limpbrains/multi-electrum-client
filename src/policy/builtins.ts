// Built-in RoutingPolicy factories. M0: signatures only; real bodies land in M2.

import type { ClientId } from '../client.js';
import type { RoutingPolicy } from './types.js';

export function roundRobin(): RoutingPolicy {
  throw new Error('roundRobin: not implemented (M2)');
}

export function failover(_orderHint?: readonly ClientId[]): RoutingPolicy {
  throw new Error('failover: not implemented (M2)');
}

export interface PreferFastestOpts {
  withinPct?: number;
  tiebreak?: 'rr' | 'leastInFlight';
}

export function preferFastest(_opts?: PreferFastestOpts): RoutingPolicy {
  throw new Error('preferFastest: not implemented (M2)');
}

export type StickyKeyFn = (req: {
  method: string;
  params: readonly unknown[];
}) => string | undefined;

export function withSticky(_inner: RoutingPolicy, _key: 'scripthash' | StickyKeyFn): RoutingPolicy {
  throw new Error('withSticky: not implemented (M2)');
}
