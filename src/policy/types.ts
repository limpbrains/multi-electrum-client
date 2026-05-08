import type { ClientId, ClientView } from '../client.js';
import type { ErrorKind } from '../errors/types.js';

export interface PickContext {
  request: { method: string; params: readonly unknown[] };
  attempt: number;
  excluded: ReadonlySet<ClientId>;
  candidates: readonly ClientView[];
  stickyKey?: string;
  now: number;
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
