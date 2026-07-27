import type { ClientId, ClientView } from '../client.js';
import type { ErrorKind } from '../errors/types.js';

export interface PickContext {
  request: { method: string; params: readonly unknown[] };
  attempt: number;
  excluded: ReadonlySet<ClientId>;
  candidates: readonly ClientView[];
  stickyKey?: string;
  now: number;
  /**
   * `true` when the manager asks speculatively — a hedge pick whose result
   * may never be dispatched (group probes can be discarded on divergence)
   * or is a duplicate of a still-live primary. Stateful policies should
   * make probe picks side-effect-free: don't advance rotation cursors,
   * don't move sticky pins. Failover retries after a REAL failure arrive
   * without this flag and may re-home state. Absent = normal pick.
   */
  probe?: boolean;
}

export type Outcome =
  | { kind: 'success'; clientId: ClientId; method: string; latencyMs: number }
  | {
      kind: 'error';
      clientId: ClientId;
      method: string;
      error: ErrorKind;
      latencyMs: number;
    }
  | { kind: 'connect-state'; clientId: ClientId; state: ClientView['state'] };

export interface RoutingPolicy {
  pick(ctx: PickContext): ClientId | null;
  onOutcome?(o: Outcome): void;
}
