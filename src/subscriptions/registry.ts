// SubscriptionRegistry — internal state shared between Manager and the
// per-client notification stream.
//
// Responsibilities:
//
//  - Multi-handler dedup. Two callers subscribing to the same (method,
//    params) pair share one wire subscription; both handlers fan out from
//    the same incoming notification.
//  - Disconnect / rebind. When a client transitions away from 'connected'
//    we mark every subscription bound to that client as orphaned. When any
//    client (re-)enters 'connected' we walk the orphans and re-issue their
//    `subscribe` wire calls. The new server's response is the *current*
//    status — if it differs from the last one we delivered to handlers, we
//    fire a synthetic notification so callers don't miss state changes
//    that happened during the gap.
//  - Last-unsub bookkeeping. When the last handler for a record is removed
//    we send the wire-level unsubscribe to the bound server (only for
//    methods where one exists — `blockchain.headers.subscribe` has no
//    counterpart, so we just stop dispatching).
//  - Survives `removeServer`. Records orphaned by removal stay in the
//    registry; the next `start` / connect on any other client re-binds
//    them. Manager owns server lifecycle, registry only owns handlers.

import type { ClientId } from '../client.js';
import type { CallOpts } from '../protocol/types.js';

import { canonicalKey } from './canonicalKey.js';
import type { SubscriptionHandler, Unsubscribe } from './types.js';

/**
 * Bridge to Manager. Registry doesn't import Manager directly (cycle), but
 * needs three things from it:
 *  - to issue `subscribe` / `unsubscribe` wire calls on a *specific* client
 *    (or any client, when re-binding an orphan after rebind);
 *  - to emit `subscription-restored` events;
 *  - to know which clients are currently connected.
 */
export interface SubscriptionEnv {
  /**
   * Issue a wire JSON-RPC call. The registry uses this only for the
   * subscribe/unsubscribe wire methods; the manager applies its full
   * routing/retry/telemetry pipeline as if the call had come from the
   * caller directly. `preferClient` (in `opts`) lets the registry hint
   * "use this client if possible" so the unsubscribe goes to the same
   * server we subscribed on.
   */
  call(method: string, params: readonly unknown[], opts?: CallOpts): Promise<unknown>;

  /** Emit a manager-level event (observability hook). */
  emit(event: 'subscription-restored', payload: SubscriptionRestoredEvent): void;

  /**
   * Resolves the id of any currently 'connected' client, or `null` when no
   * client is available right now. Used during rebind / initial subscribe.
   */
  pickConnectedClient(): ClientId | null;
}

export interface SubscriptionRestoredEvent {
  method: string;
  params: readonly unknown[];
  drift: boolean;
}

interface SubscriptionRecord {
  /** Stable string built from method + canonical(params). */
  key: string;
  method: string;
  params: readonly unknown[];
  handlers: Set<SubscriptionHandler>;
  /** Client we last subscribed on; `null` if orphaned. */
  clientId: ClientId | null;
  /** Last status we delivered to handlers (for catch-up diff). */
  lastKnownStatus: unknown;
  /**
   * Bumped every time we (re-)bind. Used to ignore stale notifications that
   * arrive after a rebind from the previous client.
   */
  generation: number;
}

/**
 * Methods that have a paired wire-level unsubscribe. Subscriptions on other
 * methods (e.g. `blockchain.headers.subscribe`) just stop dispatching when
 * the last handler is removed; we cannot tell the server to stop pushing.
 */
const UNSUB_METHOD: Record<string, string> = {
  'blockchain.scripthash.subscribe': 'blockchain.scripthash.unsubscribe',
};

export class SubscriptionRegistry {
  private readonly env: SubscriptionEnv;
  private readonly subs = new Map<string, SubscriptionRecord>();

  constructor(env: SubscriptionEnv) {
    this.env = env;
  }

  // --- Public API used by Manager ----------------------------------------

  /**
   * Register `handler` for `(method, params)`. Sends the wire `subscribe`
   * call if this is the first handler for that key. Returns an unsubscribe
   * function. The handler is invoked for the initial status returned by the
   * subscribe call AND for every subsequent server-pushed notification on
   * the same key.
   */
  async subscribe(
    method: string,
    params: readonly unknown[],
    handler: SubscriptionHandler,
  ): Promise<Unsubscribe> {
    const key = canonicalKey(method, params);
    const existing = this.subs.get(key);

    if (existing) {
      existing.handlers.add(handler);
      // New handler joining a live subscription gets the most recently seen
      // status synchronously, so it doesn't have to wait for the next event.
      if (existing.lastKnownStatus !== undefined) {
        this.invokeHandler(handler, existing.lastKnownStatus);
      }
      return this.makeUnsub(key, handler);
    }

    const clientId = this.env.pickConnectedClient();
    if (clientId === null) {
      throw new Error(`subscribe(${method}): no connected client to bind to`);
    }

    const status = await this.env.call(method, params, { preferClient: clientId });
    const record: SubscriptionRecord = {
      key,
      method,
      params,
      handlers: new Set([handler]),
      clientId,
      lastKnownStatus: status,
      generation: 1,
    };
    this.subs.set(key, record);
    this.invokeHandler(handler, status);
    return this.makeUnsub(key, handler);
  }

  /**
   * Dispatch a server-pushed notification to all registered handlers for
   * the key. Updates `lastKnownStatus` so future joins see the current
   * value and rebind catch-up uses the right baseline. Notifications from
   * a generation we no longer track are dropped silently.
   */
  notify(clientId: ClientId, method: string, params: readonly unknown[], status: unknown): void {
    const key = canonicalKey(method, params);
    const record = this.subs.get(key);
    if (!record) return; // we never subscribed to this key
    if (record.clientId !== clientId) return; // came from a stale generation

    if (statusEquals(record.lastKnownStatus, status)) return; // dedup
    record.lastKnownStatus = status;
    for (const h of record.handlers) this.invokeHandler(h, status);
  }

  /** Mark every subscription bound to `clientId` as orphaned. */
  clientDisconnected(clientId: ClientId): void {
    for (const record of this.subs.values()) {
      if (record.clientId === clientId) {
        record.clientId = null;
      }
    }
  }

  /**
   * Replay every orphaned subscription. Safe to call on every connect — non-
   * orphaned records are skipped. Each replay re-subscribes against the
   * configured policy (which may pick the same client we just connected, or
   * any other connected client) and fires synthetic notifications when the
   * returned status drifts from `lastKnownStatus`.
   */
  async restoreOrphans(): Promise<void> {
    const tasks: Promise<void>[] = [];
    for (const record of this.subs.values()) {
      if (record.clientId !== null) continue;
      tasks.push(this.rebind(record));
    }
    await Promise.all(tasks);
  }

  /** Drop every subscription. Does NOT send wire unsubscribes (called from manager.stop). */
  clear(): void {
    this.subs.clear();
  }

  /** Test / diagnostic helper. */
  size(): number {
    return this.subs.size;
  }

  // --- Internals ---------------------------------------------------------

  private makeUnsub(key: string, handler: SubscriptionHandler): Unsubscribe {
    return async () => {
      const record = this.subs.get(key);
      if (!record) return;
      record.handlers.delete(handler);
      if (record.handlers.size > 0) return;
      // Last handler gone — drop the record and best-effort tell the
      // server to stop pushing. We delete locally first so concurrent
      // notifications that arrive after this point are ignored.
      this.subs.delete(key);
      const unsubMethod = UNSUB_METHOD[record.method];
      if (unsubMethod && record.clientId !== null) {
        // Fire-and-forget. Local dispatch is already torn down; blocking
        // unsub() on a wire round-trip would let server-side weirdness
        // (slow servers, dropped connections) hang the caller's cleanup.
        // Errors are swallowed for the same reason.
        void this.env
          .call(unsubMethod, record.params, {
            preferClient: record.clientId,
            retry: 'none',
          })
          .catch(() => undefined);
      }
    };
  }

  private async rebind(record: SubscriptionRecord): Promise<void> {
    const clientId = this.env.pickConnectedClient();
    if (clientId === null) {
      // Stay orphaned; next connect re-tries.
      return;
    }
    let status: unknown;
    try {
      status = await this.env.call(record.method, record.params, { preferClient: clientId });
    } catch {
      // Rebind failed (e.g. server doesn't support the method). Leave
      // orphaned for the next connect to retry. Manager 'error' event has
      // already surfaced the underlying failure via routeAttempts.
      return;
    }
    record.clientId = clientId;
    record.generation++;
    const drift = !statusEquals(record.lastKnownStatus, status);
    if (drift) {
      record.lastKnownStatus = status;
      for (const h of record.handlers) this.invokeHandler(h, status);
    }
    this.env.emit('subscription-restored', {
      method: record.method,
      params: record.params,
      drift,
    });
  }

  private invokeHandler(h: SubscriptionHandler, status: unknown): void {
    try {
      h(status);
    } catch {
      // Handler errors are caller's bug and must not corrupt our internal
      // state. Manager's `error` event surfaces them via the same path used
      // by `policy.onOutcome` failures.
    }
  }
}

function statusEquals(a: unknown, b: unknown): boolean {
  // Subscription statuses are JSON-serializable scalars or small objects
  // (BlockHeader, ScripthashStatus). Stringify is acceptable; if a server
  // ever returns key order non-deterministically we'll need a deeper compare.
  if (a === b) return true;
  return JSON.stringify(a) === JSON.stringify(b);
}
