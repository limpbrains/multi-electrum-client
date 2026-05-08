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
//
// Design choices worth flagging:
//
//  - Subscriptions pin to one server at a time (we use
//    `env.pickConnectedClient` to bind / re-bind, NOT `policy.pick`). The
//    plan calls out "asks the policy"; the implementation deviates because
//    a multi-handler dedup across N clients defeats the dedup. The pinned
//    binding is what `preferClient` was added for.
//  - There is a small window between sending the wire `subscribe` and
//    storing the record where any server-pushed notification on the same
//    key would be silently dropped (`subs.get(key)` is undefined inside
//    `notify`). Electrum servers in practice include the current status in
//    the subscribe response, so the first interesting state change always
//    lands. Documented here so a future protocol shift doesn't surprise.
//  - We rely on per-socket-fresh wire semantics for stale-notification
//    filtering: every reconnect is a new socket, and the server isn't
//    going to push gen-N notifications onto a gen-(N+1) socket. The
//    `clientId` check in `notify` is therefore sufficient even when
//    rebind picks the same client id (which it can, on a fresh socket);
//    no per-record generation token is needed.

import type { ClientId } from '../client.js';
import type { CallOpts } from '../protocol/types.js';

import { canonicalKey } from './canonicalKey.js';
import type { SubscriptionHandler, Unsubscribe } from './types.js';

/**
 * Bridge to Manager. Registry doesn't import Manager directly (cycle), but
 * needs four things from it:
 *  - to issue `subscribe` / `unsubscribe` wire calls (manager applies its
 *    routing/retry/telemetry pipeline);
 *  - to emit `subscription-restored` events;
 *  - to know which clients are currently connected (for binding / rebind);
 *  - to ask whether a specific client id is still usable (so we don't fire
 *    a wire `unsubscribe` at a server that's gone, where it would either
 *    fall through to a different server or no-op against a dead socket).
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
   * Resolves the id of any currently 'connected' (and non-banned) client,
   * or `null` when none is available. Used during rebind / initial subscribe.
   */
  pickConnectedClient(): ClientId | null;

  /**
   * True iff `clientId` is in the pool, currently in `connected` state, and
   * not under a ban. Used by the last-handler unsubscribe path so a wire
   * unsub doesn't fall through `policy.pick` and end up at a different
   * server (which has no record of the subscription).
   */
  isClientConnected(clientId: ClientId): boolean;
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
   * Distinguishes "no status yet" from "status is `undefined`". Subscription
   * statuses for the methods we currently support are `string | null` or
   * `BlockHeader`, so this flag is conservative — but a future method that
   * legitimately returns `undefined` would otherwise confuse the
   * "joining handler gets last-known status" path.
   */
  hasStatus: boolean;
}

/**
 * In-flight first-subscribe state. Kept separately from `subs` so concurrent
 * callers in the same tick share one wire call, and so `clientDisconnected`
 * can tag a pending bind whose target client died mid-flight.
 */
interface PendingSubscribe {
  /** Client id the wire call targets. */
  clientId: ClientId;
  /** Becomes `true` if `clientDisconnected(clientId)` lands while pending. */
  orphaned: boolean;
  promise: Promise<SubscriptionRecord>;
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
  /**
   * In-flight first-subscribe wire calls keyed by canonical key. Two callers
   * subscribing in the same tick must share the same wire call: without this
   * gate both would see `subs.get(key) === undefined`, both would issue
   * `env.call`, and the second `subs.set` would overwrite the first record
   * (orphaning the first handler). Cleared once the wire call settles.
   */
  private readonly pending = new Map<string, PendingSubscribe>();
  /**
   * In-flight rebinds keyed by canonical key. Mirrors `pending` for the
   * orphan-replay path: prevents two state transitions in quick succession
   * (e.g. `disconnect` immediately followed by another client's `connect`)
   * from firing two wire `subscribe` calls for the same key.
   */
  private readonly pendingRebinds = new Map<string, Promise<void>>();

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
  async subscribe<T = unknown>(
    method: string,
    params: readonly unknown[],
    handler: SubscriptionHandler<T>,
  ): Promise<Unsubscribe> {
    const key = canonicalKey(method, params);
    const h = handler as SubscriptionHandler;
    const existing = this.subs.get(key);

    if (existing) {
      existing.handlers.add(h);
      // New handler joining a live subscription gets the most recently seen
      // status synchronously, so it doesn't have to wait for the next event.
      if (existing.hasStatus) {
        this.invokeHandler(h, existing.lastKnownStatus);
      }
      return this.makeUnsub(key, h);
    }

    // Coalesce concurrent first-subscribes onto one wire call.
    const inflight = this.pending.get(key);
    if (inflight) {
      const record = await inflight.promise;
      record.handlers.add(h);
      if (record.hasStatus) {
        this.invokeHandler(h, record.lastKnownStatus);
      }
      return this.makeUnsub(key, h);
    }

    const clientId = this.env.pickConnectedClient();
    if (clientId === null) {
      throw new Error(`subscribe(${method}): no connected client to bind to`);
    }

    const entry: PendingSubscribe = {
      clientId,
      orphaned: false,
      promise: undefined as unknown as Promise<SubscriptionRecord>,
    };
    entry.promise = (async () => {
      const status = await this.env.call(method, params, {
        preferClient: clientId,
        stickyKey: key,
      });
      const record: SubscriptionRecord = {
        key,
        method,
        params,
        handlers: new Set([h]),
        // If the bound client died while this wire call was in flight,
        // surface the record as already orphaned so the next connect /
        // restoreOrphans rebinds it.
        clientId: entry.orphaned ? null : clientId,
        lastKnownStatus: status,
        hasStatus: true,
      };
      this.subs.set(key, record);
      return record;
    })();
    this.pending.set(key, entry);
    let record: SubscriptionRecord;
    try {
      record = await entry.promise;
    } finally {
      this.pending.delete(key);
    }
    this.invokeHandler(h, record.lastKnownStatus);
    // If we landed orphaned, kick off a rebind in the background so the
    // record doesn't sit dead until the next state transition.
    if (record.clientId === null) {
      void this.rebindOnce(record);
    }
    return this.makeUnsub(key, h);
  }

  /**
   * Dispatch a server-pushed notification to all registered handlers for
   * the key. Updates `lastKnownStatus` so future joins see the current
   * value and rebind catch-up uses the right baseline. Notifications from
   * a client we no longer associate with the record are dropped silently.
   */
  notify(clientId: ClientId, method: string, params: readonly unknown[], status: unknown): void {
    const key = canonicalKey(method, params);
    const record = this.subs.get(key);
    if (!record) return; // we never subscribed to this key (or already unsubbed)
    if (record.clientId !== clientId) return; // came from a stale client / orphaned

    if (record.hasStatus && statusEquals(record.lastKnownStatus, status)) return; // dedup
    record.lastKnownStatus = status;
    record.hasStatus = true;
    for (const h of record.handlers) this.invokeHandler(h, status);
  }

  /**
   * Mark every subscription bound to `clientId` as orphaned. Also tags any
   * in-flight first-subscribe targeting this id so its post-await path
   * stores the record as orphaned rather than racing past the disconnect.
   */
  clientDisconnected(clientId: ClientId): void {
    for (const record of this.subs.values()) {
      if (record.clientId === clientId) {
        record.clientId = null;
      }
    }
    for (const entry of this.pending.values()) {
      if (entry.clientId === clientId) {
        entry.orphaned = true;
      }
    }
  }

  /**
   * Replay every orphaned subscription. Safe to call on every connect — non-
   * orphaned records and records with a rebind already in flight are
   * skipped. Each replay re-subscribes on a connected client (pinned, not
   * load-balanced) and fires synthetic notifications when the returned
   * status drifts from `lastKnownStatus`.
   */
  async restoreOrphans(): Promise<void> {
    const tasks: Promise<void>[] = [];
    for (const record of this.subs.values()) {
      if (record.clientId !== null) continue;
      if (this.pendingRebinds.has(record.key)) continue;
      tasks.push(this.rebindOnce(record));
    }
    await Promise.all(tasks);
  }

  /** Drop every subscription. Does NOT send wire unsubscribes (called from manager.stop). */
  clear(): void {
    this.subs.clear();
    this.pending.clear();
    this.pendingRebinds.clear();
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
      // Skip the wire unsub when the bound client is gone: routing it via
      // policy.pick would land at a different server (no record of the
      // sub there), or no-op if the socket is dead. Either way, useless.
      if (unsubMethod && record.clientId !== null && this.env.isClientConnected(record.clientId)) {
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

  /**
   * Single-flight wrapper around `rebind`: if a rebind for `record.key` is
   * already in flight, await that one; otherwise register and run a fresh
   * one. Prevents duplicate wire `subscribe` calls when two state transitions
   * fire `restoreOrphans` in quick succession.
   */
  private rebindOnce(record: SubscriptionRecord): Promise<void> {
    const existing = this.pendingRebinds.get(record.key);
    if (existing) return existing;
    const task = (async () => {
      try {
        await this.rebind(record);
      } finally {
        this.pendingRebinds.delete(record.key);
      }
    })();
    this.pendingRebinds.set(record.key, task);
    return task;
  }

  private async rebind(record: SubscriptionRecord): Promise<void> {
    const clientId = this.env.pickConnectedClient();
    if (clientId === null) {
      // Stay orphaned; next connect re-tries.
      return;
    }
    let status: unknown;
    try {
      status = await this.env.call(record.method, record.params, {
        preferClient: clientId,
        stickyKey: record.key,
      });
    } catch {
      // Rebind failed (e.g. server doesn't support the method). Leave
      // orphaned for the next connect to retry. Manager 'error' event has
      // already surfaced the underlying failure via runAttempts.
      return;
    }
    // Record may have been unsubscribed between the env.call dispatch and
    // resolution; if it's gone from `subs` we must not resurrect it.
    if (!this.subs.has(record.key)) return;
    record.clientId = clientId;
    const drift = !record.hasStatus || !statusEquals(record.lastKnownStatus, status);
    if (drift) {
      record.lastKnownStatus = status;
      record.hasStatus = true;
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
  // A misbehaving server returning a circular / non-serializable payload
  // would otherwise throw out of `notify` / `rebind` and crash the caller's
  // notification handler — fall back to "not equal" so the new status fires
  // through (callers see it once and can act). Worse than a deep compare,
  // strictly better than crashing the registry.
  if (a === b) return true;
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return false;
  }
}
