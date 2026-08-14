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
//  - A scripthash status is an opaque CHANGE SIGNAL, not data: consumers
//    react to a callback by resyncing from a server; they do not
//    interpret the value. That is what makes the race between a subscribe
//    response and a push buffered during the call tractable: the two
//    cannot be ordered (the transport dispatches both lines synchronously
//    before our continuation runs, and an Electrum status carries no
//    ordering of its own), so the registry delivers BOTH, response first.
//    This does NOT generalize: a header's payload IS the data (its height
//    feeds the finality gate directly), and Electrum subscriptions are
//    connection-scoped, so only statuses in `OPAQUE_STATUS_METHODS` get
//    the deliver-both treatment — everything else never delivers a
//    buffered status at all: the flush purges the payloads and keeps
//    only the uncertainty signal (see `flushEarlyNotification`).
//    Whichever was really newer, the consumer's final resync fetches the
//    server's current truth. The residual — `lastKnownStatus` possibly
//    ending on the older value — is handled by marking the baseline
//    UNCERTAIN: statuses repeat without any reorg (a mempool tx dropped
//    by RBF, expiry or conflict restores the exact prior history), so an
//    uncertain baseline makes the REBIND path deliver an equal answer
//    and report drift (live pushes are never deduplicated at all — the
//    protocol mandates delivering even an unchanged status). A wire-refetch tie-break was tried
//    here and removed: it produced more defects than it closed.
//  - Continuation validity is layered by lifetime, one mechanism per
//    lifetime: `epoch` (registry, bumped by clear()), record identity
//    (`subs.get(key) === record`), and registration tokens (handler).
//    Anything that survives an `await` re-checks the lifetimes it
//    depends on — and nothing else.

import type { ClientId } from '../client.js';
import type { CallOpts } from '../protocol/types.js';

import { setUnrefTimeout } from '../util/timers.js';
import { NoClientAvailableError, RpcError, SuspendedError } from '../errors/types.js';
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
   * "use this client if possible" — but the retry pipeline may fail
   * over, so the result reports `servedBy`: the client that actually
   * answered. Records MUST bind to `servedBy`, never to the preferred
   * pick — binding to the wrong client silently drops the
   * subscription's future notifications and misroutes its unsubscribe.
   */
  call(
    method: string,
    params: readonly unknown[],
    opts?: CallOpts,
  ): Promise<{ value: unknown; servedBy: ClientId }>;

  /** Emit a manager-level event (observability hook). */
  emit(event: 'subscription-restored', payload: SubscriptionRestoredEvent): void;

  /**
   * Resolves the id of any currently 'connected' (and non-banned) client,
   * or `null` when none is available. Used during rebind / initial subscribe.
   */
  pickConnectedClient(): ClientId | null;

  /**
   * True iff `clientId` is in the pool and currently in `connected`
   * state. Deliberately NOT ban-aware: subscription ownership follows
   * the socket. A ban gates routing of new calls; it neither closes the
   * connection nor cancels a wire subscription the server already
   * accepted, so a banned-but-connected server still owns its
   * subscriptions and its pushes must still be delivered.
   */
  isClientConnected(clientId: ClientId): boolean;

  /**
   * Force-close `clientId`'s connection (a no-op if it is not
   * connected). Used when a wire unsubscribe fails AMBIGUOUSLY — timed
   * out or died in transport — so the server may still execute it
   * later, after a replacement subscribe on the same session, silently
   * cancelling it. Electrum subscriptions are session-scoped: retiring
   * the socket guarantees the stray unsubscribe dies with the session,
   * and the replacement binds on a fresh one. The manager's reconnect
   * machinery restores the connection.
   */
  retireClient(clientId: ClientId): void;

  /**
   * Monotonic per-client session counter, bumped on every `connected`
   * transition. Lets a continuation that captured it before an await
   * prove the session it wrote to is the one still connected — a bare
   * "is connected" check cannot distinguish the same session from a
   * replacement that connected while the continuation was queued.
   */
  sessionSeq(clientId: ClientId): number;
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
  /**
   * Set, not multiset: subscribing the *same function reference* twice
   * occupies one slot, so the first `unsub` removes it for both callers.
   * This matches `EventTarget.addEventListener` semantics. Callers who
   * truly want two independent subscriptions should pass two distinct
   * handler functions (e.g. wrap with arrow functions).
   */
  handlers: Map<SubscriptionHandler, number>;
  /** Client we last subscribed on; `null` if orphaned. */
  clientId: ClientId | null;
  /**
   * Client that ANSWERED that subscribe, even if it has since become
   * unusable. Ownership of a status buffered during the call belongs to
   * it, not to the current binding: a server can push a newer status in
   * the same chunk as its response and disconnect right after, and that
   * push is still the most recent thing it told us.
   */
  servedBy: ClientId;
  /**
   * Handlers that joined while the record was unbound and so were not
   * given the stale snapshot. They get the status from the first binding
   * that holds, even when it matches what we already had.
   */
  awaitingBaseline: Map<SubscriptionHandler, number>;
  /**
   * True while `lastKnownStatus` came from a push whose order against
   * the response could not be established (or a differing data-bearing
   * push was purged). Live pushes are NEVER deduplicated, so this flag
   * does not gate them; its two consumers are the REBIND path — an
   * answer equal to an uncertain baseline still delivers (scripthash
   * statuses repeat without a reorg: a dropped mempool tx restores the
   * exact prior history) and an uncertain baseline counts as drift —
   * and nothing else. The next delivery resolves the ambiguity and
   * clears it.
   */
  baselineUncertain: boolean;
  /**
   * `env.sessionSeq(servedBy)` at the moment this record bound (initial
   * subscribe or rebind). A wire unsubscribe is only valid against the
   * SESSION that accepted the subscribe — the server reconnecting under
   * the same client id starts a fresh session that never held it, and
   * writing there is spurious cleanup whose ambiguous timeout would
   * poison a healthy socket.
   */
  boundSessionSeq: number;
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
 * Methods that have a paired wire-level unsubscribe. Subscriptions on other
 * methods (e.g. `blockchain.headers.subscribe`) just stop dispatching when
 * the last handler is removed; we cannot tell the server to stop pushing.
 */
const UNSUB_METHOD: Record<string, string> = {
  'blockchain.scripthash.subscribe': 'blockchain.scripthash.unsubscribe',
};

/**
 * Methods whose status is an OPAQUE change signal: the payload carries no
 * data a consumer acts on directly — it only tells them to resync from a
 * server, so delivering one from a foreign pooled server costs at most a
 * spurious refetch. Everything else's payload IS the data (a header's
 * height feeds the manager's finality gate directly), and Electrum
 * subscriptions are connection-scoped: an unrelated socket has no
 * authority over this binding's view, and delivering its buffered payload
 * would let one bad pooled server inject state (an inflated tip poisons
 * finalized cache writes) during any subscribe/rebind race.
 */
const OPAQUE_STATUS_METHODS = new Set(['blockchain.scripthash.subscribe']);

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
  private readonly pending = new Map<string, Promise<SubscriptionRecord>>();
  /**
   * In-flight wire unsubscribes by canonical key. A replacement
   * subscribe for the same key must wait for the prior unsubscribe to
   * SETTLE: servers may process a session's requests concurrently
   * (ElectrumX `INITIAL_CONCURRENT`), so writing U before S does not
   * order their execution — a late U silently cancels S's live
   * subscription server-side. A settled response is the only proof the
   * server executed U. (A timed-out U stays ambiguous — the server may
   * still execute it later; nothing client-side can close that, so the
   * barrier waits for settlement, not success.) This deliberately
   * couples a replacement subscribe's latency to the unsubscribe's
   * settlement, bounded by the request timeout: the uncoupled
   * alternative is a subscription that looks live and receives nothing.
   */
  private readonly pendingWireUnsubs = new Map<string, Promise<void>>();
  /**
   * Sessions holding an unsubscribe whose outcome is AMBIGUOUS (timed
   * out / died in transport): the server may still execute it later,
   * after a replacement subscribe on the same session — silently
   * cancelling it. Keyed by canonical key, value = the session's
   * client. The retire is DEFERRED to the moment a replacement
   * subscribe for that key actually appears: retiring eagerly would
   * cycle a merely-slow server's whole connection on every unsubscribe,
   * hazard or not, while a wallet that never re-subscribes the key
   * needs no retire at all. A disconnect clears the entry — the stray
   * unsubscribe died with its session.
   */
  private readonly poisonedSessions = new Map<string, ClientId>();
  /**
   * In-flight rebinds keyed by canonical key. Mirrors `pending` for the
   * orphan-replay path: prevents two state transitions in quick succession
   * (e.g. `disconnect` immediately followed by another client's `connect`)
   * from firing two wire `subscribe` calls for the same key.
   */
  private readonly pendingRebinds = new Map<
    string,
    { record: SubscriptionRecord; task: Promise<void> }
  >();
  /**
   * Notifications that arrived for a key whose subscribe / rebind wire
   * call was still in flight, keyed by canonical key.
   *
   * A server may answer `subscribe` and push the first status change in
   * the SAME chunk. The transport frames and dispatches both lines
   * synchronously, while the wire call's resolution only schedules our
   * continuation as a microtask — so the notification reaches `notify`
   * before the record exists (first subscribe) or while it is still
   * orphaned (rebind), and used to be dropped. The caller would then sit
   * on the status the subscribe returned until the NEXT change, which
   * for a wallet means a missed transaction.
   *
   * Buffered per key (a status is a snapshot, not a delta, so the last
   * one wins) and cleared when the wire call starts, so anything found
   * at flush time arrived during that call. Its order against the
   * response is NOT knowable — the push may sit in an earlier chunk than
   * the response or a later one — which is why the flush marks the
   * baseline uncertain after delivering (see `baselineUncertain`).
   */
  private readonly earlyNotifications = new Map<string, Map<ClientId, unknown>>();
  /**
   * Bumped by `clear()`. Every wire call captures the value it started
   * under and abandons its continuation if it no longer matches: a
   * subscribe waiting on its response when `stop()` runs had its
   * continuation already queued, and it used to re-install a record and
   * call the handler on a manager that had shut down.
   */
  private epoch = 0;
  /**
   * Source of registration tokens. A handler is identified by its
   * function reference, so removing and re-adding the SAME function is a
   * new registration that nothing else can distinguish from the old one:
   * a fan-out snapshot taken before the swap would call it again, and
   * the spent unsubscribe handle would tear the replacement down.
   */
  private nextRegistration = 1;
  /**
   * Armed rebind backoff sleeps. A plain `setTimeout` here kept the event
   * loop referenced for up to 10s after `stop()` resolved, and the task
   * behind it stayed alive with nothing left to do. `clear()` cancels
   * them so teardown finishes when the caller is told it has.
   */
  private readonly rebindSleeps = new Set<{
    timer: ReturnType<typeof setTimeout>;
    wake: () => void;
  }>();

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
      return this.attach(existing, h);
    }

    // Coalesce concurrent first-subscribes onto one wire call.
    const inflight = this.pending.get(key);
    if (inflight) {
      const joinedEpoch = this.epoch;
      const shared = await inflight;
      // Same guard the leader applies after ITS await, and for the same
      // reason: `clear()` can land while this continuation is queued —
      // including from the leading subscriber's own handler, which runs
      // during its replay. Without it a joiner attached to a retired
      // record, its handler fired after teardown, and it was handed an
      // unsubscribe owning nothing.
      if (joinedEpoch !== this.epoch || this.subs.get(key) !== shared) {
        throw new Error(`subscribe(${method}): registry cleared while subscribing`);
      }
      const joined = this.attach(shared, h);
      // The joiner's status is exactly as fresh as the leader's — the
      // same wire call fetched it — so it shares the leader's
      // empty-pool fallback: parked with no rebind even possible, it
      // would otherwise stay silent until a reconnect that offline
      // never brings.
      if (shared.clientId === null && this.env.pickConnectedClient() === null) {
        this.settleAwaitingWithOwnResponse(shared, h);
      }
      return joined;
    }

    const clientId = this.env.pickConnectedClient();
    if (clientId === null) {
      throw new Error(`subscribe(${method}): no connected client to bind to`);
    }

    const epoch = this.epoch;
    // The leader's registration in `pending` must be visible before ANY
    // of its side effects run — `retireClient` fires 'client-state'
    // synchronously, and a listener subscribing this same key
    // re-entrantly must join this leader instead of forking a second
    // wire subscribe. A deferred promise keeps the WIRE DISPATCH
    // synchronous with the subscribe() call itself (a one-microtask
    // delay let a same-tick suspend() win the race and reject a call
    // that used to complete under the grace period).
    let settleLeader!: {
      resolve: (r: SubscriptionRecord) => void;
      reject: (e: unknown) => void;
    };
    const inflightPromise = new Promise<SubscriptionRecord>((resolve, reject) => {
      settleLeader = { resolve, reject };
    });
    this.pending.set(key, inflightPromise);
    void (async () => {
      // Serialize behind a wire unsubscribe still in flight for this key
      // (see `pendingWireUnsubs`), and re-check the epoch afterwards:
      // clear() landing while we are parked here must not let a stray
      // wire subscribe go out for a registry that no longer exists —
      // the server would hold a subscription with no local record and
      // nobody to ever unsubscribe it.
      const unsubBarrier = this.pendingWireUnsubs.get(key);
      if (unsubBarrier) {
        await unsubBarrier;
        if (epoch !== this.epoch) {
          throw new Error(`subscribe(${method}): registry cleared while subscribing`);
        }
      }
      // The prior unsubscribe for this key failed AMBIGUOUSLY: it may
      // still execute on that session, after our subscribe, silently
      // cancelling it. This is the moment the hazard becomes real —
      // retire the session so the stray unsubscribe dies with it; the
      // routing layer then binds us elsewhere (or the manager's
      // reconnect restores the server first).
      const poisoned = this.poisonedSessions.get(key);
      if (poisoned !== undefined) {
        this.poisonedSessions.delete(key);
        this.env.retireClient(poisoned);
      }
      // Anything buffered for this key predates the call we are about
      // to make, so it cannot be newer than the status that call
      // returns. Purged AFTER the barrier — a push received while we
      // were parked also predates the call.
      this.earlyNotifications.delete(key);
      // Retryable on purpose: failover to a server that can answer is
      // how a subscribe survives a flaky first pick. ACCEPTED COST: an
      // attempt that timed out after the server installed the
      // subscription leaves it live and untracked on that session —
      // pushes into the void until that connection recycles. Bounded
      // and self-healing (same class as the conservative stale-rebind
      // cleanup below); the registry never sees per-attempt outcomes,
      // and the correctness-affecting variants are closed elsewhere
      // (storage authority, session poisoning).
      const { value: status, servedBy } = await this.env.call(method, params, {
        preferClient: clientId,
        stickyKey: key,
      });
      // Session identity FIRST, before anything else can interleave:
      // the response arrived on servedBy's session, and every microtask
      // between the resolve and this line is a window in which a
      // synchronously-reconnecting embedding can replace it.
      const servedSessionSeq = this.env.sessionSeq(servedBy);
      if (epoch !== this.epoch) {
        // clear() ran while this was in flight — the registry this record
        // belonged to is gone.
        throw new Error(`subscribe(${method}): registry cleared while subscribing`);
      }
      const record: SubscriptionRecord = {
        key,
        method,
        params,
        // Empty: `attach` below registers the handler and replays the
        // status to it, so every handler joins through exactly one path.
        handlers: new Map<SubscriptionHandler, number>(),
        awaitingBaseline: new Map<SubscriptionHandler, number>(),
        // Bind to the client that actually answered (retry may have
        // failed over past our preferred pick). If that client is
        // already gone — it died while the call was in flight — store
        // the record as orphaned so restoreOrphans / the rebind kick
        // below picks it up.
        clientId: this.env.isClientConnected(servedBy) ? servedBy : null,
        servedBy,
        boundSessionSeq: servedSessionSeq,
        baselineUncertain: false,
        lastKnownStatus: status,
        hasStatus: true,
      };
      this.subs.set(key, record);
      return record;
    })().then(
      (r) => settleLeader.resolve(r),
      (e: unknown) => settleLeader.reject(e),
    );
    let record: SubscriptionRecord;
    try {
      record = await inflightPromise;
    } finally {
      this.pending.delete(key);
      // Failed subscribe: nothing will ever flush what raced it.
      if (!this.subs.has(key)) this.earlyNotifications.delete(key);
    }
    // The epoch check inside the task covers the wire call; this covers
    // the gap between the task installing the record and THIS
    // continuation resuming — `clear()` can land in between, and the
    // caller would otherwise get a handler call and a live unsubscribe
    // handle for a registry that no longer exists.
    if (epoch !== this.epoch || this.subs.get(key) !== record) {
      throw new Error(`subscribe(${method}): registry cleared while subscribing`);
    }
    // Attach (and replay the initial status) BEFORE kicking a rebind:
    // the caller's handler must see the status this subscribe returned
    // ahead of any side effects of the recovery path.
    const unsub = this.attach(record, h);
    // A status pushed while the subscribe was in flight has been held
    // until now. Opaque statuses deliver after the handler has seen the
    // response's own status; data-bearing ones are purged to an
    // uncertainty mark instead (see `flushEarlyNotification`).
    this.flushEarlyNotification(record);
    // If we landed orphaned, kick off a rebind in the background so the
    // record doesn't sit dead until the next state transition. When no
    // rebind can even START (the pool is empty — mobile offline right
    // after the answer), the LEADER must not be left silent: its status
    // is not a snapshot of unknown age, this very call fetched it, and
    // the documented contract promises the handler its initial status.
    // With a rebind available the parked handler is settled by the
    // binding that actually holds — fresher than an answer from a
    // socket that already died.
    if (record.clientId === null) {
      if (this.env.pickConnectedClient() === null) {
        this.settleAwaitingWithOwnResponse(record, h);
      } else {
        void this.rebindOnce(record);
      }
    }
    return unsub;
  }

  /**
   * Settle one handler's awaitingBaseline debt with the record's own
   * just-fetched status. Used by the leader and its coalesced joiners
   * when the record landed orphaned AND the pool is empty: the status
   * is not a snapshot of unknown age — the wire call these subscribers
   * share fetched it — and with no rebind even possible, parking would
   * leave them silent until a reconnect that offline never brings.
   */
  private settleAwaitingWithOwnResponse(record: SubscriptionRecord, h: SubscriptionHandler): void {
    if (!record.hasStatus) return;
    const token = record.handlers.get(h);
    if (token !== undefined && record.awaitingBaseline.has(h)) {
      record.awaitingBaseline.delete(h);
      this.invokeHandler(record, h, token, record.lastKnownStatus);
    }
  }

  /**
   * Register a handler on a live record: add, replay the last-known
   * status synchronously (a joiner must not wait for the next push), and
   * hand back the unsubscriber. Every subscribe path funnels through here.
   */
  private attach(record: SubscriptionRecord, h: SubscriptionHandler): Unsubscribe {
    // Registering the SAME handler reference twice is idempotent — the
    // map holds one entry per function. Replaying to it again would
    // re-enter a handler that is mid-call if it subscribed itself from
    // its own callback, and each re-entry replays again: unbounded
    // recursion. A registration that is still live keeps its token, so
    // handles taken earlier stay valid.
    const existing = record.handlers.get(h);
    const token = existing ?? this.nextRegistration++;
    const added = existing === undefined;
    record.handlers.set(h, token);
    if (added && record.hasStatus) {
      if (record.clientId === null) {
        // Nothing is pushing to this record right now (suspended, offline
        // or waiting on a rebind), so `lastKnownStatus` is a snapshot of
        // unknown age. Handing it over as the joiner's first callback
        // reads as "this is current" and it may be hours old. Wait for a
        // binding instead — `restoreOrphans` delivers to these handlers
        // once one holds, whether or not the status changed meanwhile.
        record.awaitingBaseline.set(h, token);
      } else {
        this.invokeHandler(record, h, token, record.lastKnownStatus);
      }
    }
    return this.makeUnsub(record, h, token);
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
    if (!record) {
      // No record yet: buffer only while OUR first subscribe for this key
      // is in flight — an unknown key stays dropped, so a rogue server
      // cannot grow this map by pushing keys we never asked for.
      if (this.pending.has(key)) this.bufferEarly(key, clientId, status);
      return;
    }
    if (record.clientId !== clientId) {
      // Orphaned record: a rebind may be in flight and about to bind to
      // the very client this push came from. Buffer for that flush; a
      // push from any other client is dropped there.
      if (record.clientId === null) this.bufferEarly(key, clientId, status);
      return; // came from a stale client / orphaned
    }

    // NO deduplication of live pushes from the bound server: the
    // protocol says the client MAY be notified without a status change
    // and MUST be notified on a same-height reorg — where the blockhash
    // changes but the status hash does not. Suppressing an equal push
    // swallowed exactly that mandated notification, leaving a consumer
    // on a merkle proof tied to the orphaned block. Handlers resync on
    // every callback (the documented contract), so a duplicate costs
    // one refetch. Equality suppression exists only on SYNTHETIC paths
    // (attach replay, rebind response, flush) — those are our replays,
    // not server notifications. (A hostile bound server can drive
    // resyncs by flooding pushes either way — DISTINCT statuses were
    // never deduplicated — so suppression bought no DoS bound; pool
    // policies and bans are the systemic answer.)
    this.deliver(record, status);
  }

  /** Store the status, then fan out — stored-before-handlers is invariant. */
  private deliver(record: SubscriptionRecord, status: unknown): void {
    record.lastKnownStatus = status;
    record.hasStatus = true;
    record.baselineUncertain = false;
    // Everyone about to be called has now seen a status from a live
    // binding.
    record.awaitingBaseline.clear();
    // Snapshot: a Set iterator visits entries added while it runs, so a
    // handler that subscribed another handler from inside its callback
    // saw that new handler receive the same status twice — once from
    // `attach`'s replay, once from this loop catching up with it.
    for (const [h, token] of [...record.handlers]) this.invokeHandler(record, h, token, status);
  }

  /**
   * Deliver the notifications that raced this record's subscribe /
   * rebind, if any did. Call AFTER the record's own response status has
   * been replayed, so handlers see response-then-push order.
   *
   * For OPAQUE-status methods every differing buffered status is
   * delivered — a change signal from a server we subscribed on is worth
   * one resync, and dropping one can lose a change with no recovery
   * path (a push from the previously bound server, buffered while
   * orphaned, describes a transaction the new binding's lagging answer
   * may not know yet). The answering server's push goes LAST, so
   * `lastKnownStatus` settles on the bound server's view. Ordering
   * against the response is unknowable either way; see the header notes
   * for why delivering both is the correct resolution.
   *
   * For everything else — data-bearing statuses (see
   * `OPAQUE_STATUS_METHODS`) — the flush is a pure PURGE: no buffered
   * payload reaches handlers or storage, from any server, the
   * answering one included. A differing dropped status still marks the
   * baseline uncertain, so the change signal survives as drift and
   * relaxed dedup even though the payload does not.
   */
  private flushEarlyNotification(record: SubscriptionRecord): void {
    const byClient = this.earlyNotifications.get(record.key);
    if (byClient === undefined) return;
    this.earlyNotifications.delete(record.key);
    // The answering server's status goes last, so `lastKnownStatus`
    // settles on the bound server's view.
    // For DATA-bearing statuses (headers), buffered pushes are dropped
    // outright — even the answering server's own. Their order against
    // the response is unknowable (a push in the same chunk as the
    // response is buffered too, because this continuation runs a
    // microtask later), so one of two errors is unavoidable: replaying
    // a pre-response push REGRESSES the tip below the response the
    // server just gave (and the finality gate reads it), while
    // dropping a post-response push merely leaves the tip one block
    // behind until the next push — which the next block delivers
    // anyway. Never-regress wins. (A pinned re-subscribe "refresh" to
    // settle the order was considered and rejected: the wire-refetch
    // tie-break class was implemented on this branch once and produced
    // more defects than it closed; the staleness this trades away is
    // bounded by the next block.) The SIGNAL survives the purge: a
    // differing dropped status proves the tip may have moved, so the
    // baseline is marked uncertain — the rebind reports drift and
    // delivers even an equal answer. (That is what the
    // buffer is FOR on these methods: uncertainty evidence, never
    // delivery.) Handlers still awaiting their first status keep
    // waiting for a live binding, per their contract — the rebind
    // retry loop settles them. Opaque statuses (scripthash) keep the
    // deliver-both change-signal path below: consumers resync, so
    // order cannot mislead them, and dropping could lose the only
    // notice of a change.
    if (!OPAQUE_STATUS_METHODS.has(record.method)) {
      for (const status of byClient.values()) {
        if (!record.hasStatus || !statusEquals(record.lastKnownStatus, status)) {
          record.baselineUncertain = true;
          break;
        }
      }
      return;
    }
    const servedByStatus = byClient.get(record.servedBy);
    const hadServedBy = byClient.has(record.servedBy);
    byClient.delete(record.servedBy);
    const foreign = [...byClient.values()];
    // Foreign statuses are CHANGE SIGNALS, never state: they fan out to
    // established handlers but bypass `deliver()`, so the record's
    // stored view — which feeds every joiner's replay (including one
    // attached re-entrantly from inside this very fan-out) and the
    // dedup baseline — is only ever what the serving socket reported.
    // Handlers still awaiting their first status are skipped: they were
    // promised a status from a live binding, and a foreign payload is
    // not one — the serving delivery below (or the next authoritative
    // one) settles their debt.
    let delivered = false;
    for (const status of foreign) {
      if (record.hasStatus && statusEquals(record.lastKnownStatus, status)) continue;
      for (const [h, token] of [...record.handlers]) {
        if (record.awaitingBaseline.has(h)) continue;
        this.invokeHandler(record, h, token, status);
      }
      delivered = true;
    }
    // The serving server's own buffered push IS its view: stored via
    // `deliver`, exactly like a live push from it.
    if (
      hadServedBy &&
      (!record.hasStatus || !statusEquals(record.lastKnownStatus, servedByStatus))
    ) {
      this.deliver(record, servedByStatus);
      delivered = true;
    }
    // Anything delivered here has unknown order against the response,
    // so the baseline it left behind is ambiguous: the REBIND path must
    // deliver even an answer equal to it (statuses repeat without a
    // reorg: a dropped mempool tx restores the exact prior history) and
    // must report drift. Set AFTER the deliveries: `deliver` resets the
    // flag.
    if (delivered) record.baselineUncertain = true;
  }

  /** Last write wins per (key, client): a status is a snapshot. */
  private bufferEarly(key: string, clientId: ClientId, status: unknown): void {
    const byClient = this.earlyNotifications.get(key) ?? new Map<ClientId, unknown>();
    byClient.set(clientId, status);
    this.earlyNotifications.set(key, byClient);
  }

  /**
   * Mark every subscription bound to `clientId` as orphaned. An in-flight
   * first-subscribe needs no tagging: its continuation binds via
   * `isClientConnected(servedBy)`, which is already false by then.
   */
  clientDisconnected(clientId: ClientId): void {
    // The session died and its stray unsubscribe with it.
    for (const [key, owner] of this.poisonedSessions) {
      if (owner === clientId) this.poisonedSessions.delete(key);
    }
    for (const record of this.subs.values()) {
      if (record.clientId === clientId) {
        record.clientId = null;
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
    // A connection came back (or the pool changed): anything sitting in
    // its retry backoff should try again NOW rather than sleep out a
    // delay that was chosen while the pool was down — at later stages
    // that is up to 10 seconds of a subscription staying dead.
    this.wakeBackoffSleeps();
    const tasks: Promise<void>[] = [];
    for (const record of this.subs.values()) {
      if (record.clientId !== null) continue;
      // Skip only if the in-flight rebind is OURS: a task left over from a
      // predecessor at this key recovers nothing for us, so treating the
      // key as busy would leave this record orphaned indefinitely.
      if (this.pendingRebinds.get(record.key)?.record === record) continue;
      tasks.push(this.rebindOnce(record));
    }
    await Promise.all(tasks);
  }

  /** Drop every subscription. Does NOT send wire unsubscribes (called from manager.stop). */
  clear(): void {
    this.epoch++;
    this.subs.clear();
    this.pending.clear();
    this.pendingRebinds.clear();
    this.earlyNotifications.clear();
    // A subscribe on the next epoch must not park on a wire unsubscribe
    // from this one; the in-flight call itself settles on its own.
    this.pendingWireUnsubs.clear();
    this.poisonedSessions.clear();
    // Wake every backoff sleep so its task can observe the new epoch and
    // finish, instead of holding a timer past the end of the manager.
    this.wakeBackoffSleeps();
  }

  /** End every backoff sleep early; each task re-checks its own state. */
  private wakeBackoffSleeps(): void {
    for (const s of this.rebindSleeps) {
      clearTimeout(s.timer);
      s.wake();
    }
    this.rebindSleeps.clear();
  }

  /** Test / diagnostic helper. */
  size(): number {
    return this.subs.size;
  }

  // --- Internals ---------------------------------------------------------

  private makeUnsub(
    owner: SubscriptionRecord,
    handler: SubscriptionHandler,
    token: number,
  ): Unsubscribe {
    const key = owner.key;
    return async () => {
      // Identity, not key: a handle from a PREVIOUS registration of this
      // key must not remove a handler from — or tear down — the record
      // that replaced it. (Same handler reference, resubscribed: the key
      // and handler alone cannot tell the two registrations apart.)
      const record = this.subs.get(key);
      if (record !== owner) return;
      // Token, not just the function: this handle belongs to ONE
      // registration. If that one was removed and the same function
      // subscribed again, the handle is spent and must not remove the
      // registration that replaced it.
      if (record.handlers.get(handler) !== token) return;
      const boundTo = record.clientId ?? record.servedBy;
      record.handlers.delete(handler);
      // Drop the pending baseline debt too. `invokeHandler` would refuse
      // to call a handler that is no longer in `handlers` anyway — this
      // is hygiene, so a long-lived orphaned record does not accumulate
      // dead references in a second set.
      record.awaitingBaseline.delete(handler);
      if (record.handlers.size > 0) return;
      // Last handler gone — drop the record and best-effort tell the
      // server to stop pushing. We delete locally first so concurrent
      // notifications that arrive after this point are ignored.
      this.subs.delete(key);
      this.earlyNotifications.delete(key);
      // `clientId ?? servedBy`: an orphaned record still remembers the
      // server that answered its subscribe. If that connection is alive
      // when the last handler leaves, the server may still hold the
      // subscription — telling nobody would leave it pushing with no
      // consumer for the life of the connection. `wireUnsubscribe`
      // itself skips a client that is gone — then there is nobody left
      // to tell.
      this.wireUnsubscribe(record.method, record.params, boundTo, record.boundSessionSeq);
    };
  }

  /**
   * Best-effort "stop pushing this" to one specific server.
   *
   * Fire-and-forget: local dispatch is already torn down, and blocking on
   * a wire round-trip would let server-side weirdness (slow servers,
   * dropped connections) hang the caller's cleanup. Errors are swallowed
   * for the same reason. Addressed with `pinStrict` (plus
   * `retry: 'none'`) — that exact connection or nobody; routing it via
   * policy.pick would land at a server that has no such subscription.
   */
  private wireUnsubscribe(
    method: string,
    params: readonly unknown[],
    clientId: ClientId,
    bindSeq: number,
  ): void {
    const unsubMethod = UNSUB_METHOD[method];
    if (!unsubMethod) return;
    // Valid only against the session that accepted the subscribe: a
    // reconnected client (same id, new session) never held it, and a
    // spurious unsubscribe there could time out ambiguously and poison
    // a healthy socket.
    if (!this.env.isClientConnected(clientId)) return;
    if (this.env.sessionSeq(clientId) !== bindSeq) return;
    const key = canonicalKey(method, params);
    // Chain behind any unsubscribe already in flight for this key so the
    // barrier a replacement subscribe awaits covers every one of them.
    const prior = this.pendingWireUnsubs.get(key);
    const task = (async () => {
      if (prior) await prior;
      // The chain can outlive a session (the client recycles while the
      // prior call is in flight). A session-scoped subscription died
      // with its session, so there is nothing left to clean: dispatching
      // anyway would write a spurious unsubscribe to a FRESH session
      // that never held the subscription — and its ambiguous timeout
      // would poison, and later retire, a healthy socket. Re-checked
      // here, after the wait and immediately before dispatch; past this
      // point the seq is the session we are writing to.
      if (!this.env.isClientConnected(clientId) || this.env.sessionSeq(clientId) !== bindSeq) {
        return;
      }

      try {
        // A rejection here must also never poison the barrier a
        // replacement subscribe awaits. try/catch rather than .catch():
        // an env.call implementation that throws SYNCHRONOUSLY would
        // reject the task before .catch ever attached.
        await this.env.call(unsubMethod, params, {
          preferClient: clientId,
          retry: 'none',
          pinStrict: true,
          // Cleanup of state this session already holds: the
          // unsubscribe must reach the owning connection even while
          // that server cools down. The only ban-exempt strict pin.
          pinBanExempt: true,
        });
      } catch (e) {
        // Any settled RESPONSE (success or RPC error) proves the server
        // executed the unsubscribe. A failure here is AMBIGUOUS — a
        // timeout or transport death leaves an unsubscribe the server
        // may still execute later, after a replacement subscribe on the
        // same session, silently cancelling it. Subscriptions are
        // session-scoped, so retire the socket: the stray unsubscribe
        // dies with the session and the replacement binds on a fresh
        // one — deferred until a replacement subscribe appears (see
        // `poisonedSessions`). Three exceptions, none ambiguous: the
        // strict pin found no connection and a pre-dispatch
        // SuspendedError both mean nothing was written; an RPC ERROR
        // response means the server answered, which proves it executed
        // the request — nothing is left in flight to race a successor.
        // …and only when the SESSION THE UNSUBSCRIBE WAS WRITTEN TO is
        // still the connected one. On socket loss the client rejects
        // in-flight calls and then publishes 'disconnected' — this
        // catch runs microtask hops later, after clientDisconnected
        // already cleared the key; and an embedding with a
        // synchronously-connecting transport can even have a FRESH
        // session up by now. A bare "is connected" check cannot tell
        // the two apart; the session seq captured before the call can:
        // unchanged seq + connected = the very session we wrote to,
        // anything else means that session (and its stray unsubscribe)
        // died.
        if (
          !(e instanceof NoClientAvailableError) &&
          !(e instanceof RpcError) &&
          !(e instanceof SuspendedError) &&
          this.env.isClientConnected(clientId) &&
          this.env.sessionSeq(clientId) === bindSeq
        ) {
          this.poisonedSessions.set(key, clientId);
        }
      }
    })();
    this.pendingWireUnsubs.set(key, task);
    void task.finally(() => {
      if (this.pendingWireUnsubs.get(key) === task) this.pendingWireUnsubs.delete(key);
    });
  }

  /**
   * Single-flight wrapper around `rebind`: if a rebind for `record.key` is
   * already in flight, await that one; otherwise register and run a fresh
   * one. Prevents duplicate wire `subscribe` calls when two state transitions
   * fire `restoreOrphans` in quick succession.
   */
  private rebindOnce(record: SubscriptionRecord): Promise<void> {
    const existing = this.pendingRebinds.get(record.key);
    if (existing) {
      if (existing.record === record) return existing.task;
      // The in-flight task belongs to a PREDECESSOR at this key — an
      // unsubscribed record whose rebind is still settling. It will exit
      // on its identity check and recover nothing, so handing it back
      // would leave us orphaned with no recovery pending at all. Wait for
      // it to clear the slot, then rebind on our own behalf.
      return existing.task.then(() => {
        if (this.subs.get(record.key) !== record || record.clientId !== null) return;
        if (this.pendingRebinds.has(record.key)) return; // someone else got there first
        return this.rebindOnce(record);
      });
    }
    const epoch = this.epoch;
    const task = (async () => {
      try {
        // A failed wire subscribe must not strand the record until the next
        // client state transition — on flaky links (mobile networks, the
        // Android emulator's NAT) a freshly reconnected socket can accept
        // the connection yet drop the first request. Retry with backoff for
        // as long as some client is connected; a disconnect ends the loop
        // and the next connect starts a fresh one.
        let delayMs = 1_000;
        for (;;) {
          await this.rebind(record);
          if (this.subs.get(record.key) !== record) return; // unsubscribed / replaced meanwhile
          if (record.clientId !== null) return; // bound — done
          if (this.env.pickConnectedClient() === null) return; // next connect retries
          // Woken early (a connection returned): retry at once and start
          // the ladder over — the old delay was chosen for a pool that
          // no longer looks the way it did.
          const sleptFully = await this.backoffSleep(delayMs);
          delayMs = sleptFully ? Math.min(delayMs * 2, 10_000) : 1_000;
          // Re-check after the pause: an unsubscribe, a concurrent rebind
          // or a clear() may have settled things while we slept.
          if (epoch !== this.epoch) return;
          if (this.subs.get(record.key) !== record || record.clientId !== null) return;
        }
      } finally {
        // Only our own entry: a successor may already own the slot.
        if (this.pendingRebinds.get(record.key)?.record === record) {
          this.pendingRebinds.delete(record.key);
        }
      }
    })();
    this.pendingRebinds.set(record.key, { record, task });
    return task;
  }

  /** Cancellable, unreferenced sleep: see `rebindSleeps`. */
  private backoffSleep(ms: number): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const entry = {
        timer: setUnrefTimeout(() => {
          this.rebindSleeps.delete(entry);
          resolve(true);
        }, ms),
        wake: () => resolve(false),
      };
      this.rebindSleeps.add(entry);
    });
  }

  private async rebind(record: SubscriptionRecord): Promise<void> {
    const clientId = this.env.pickConnectedClient();
    if (clientId === null) {
      // Stay orphaned; next connect re-tries.
      return;
    }
    let status: unknown;
    let servedBy: ClientId;
    let servedSessionSeq: number;
    this.earlyNotifications.delete(record.key);
    try {
      ({ value: status, servedBy } = await this.env.call(record.method, record.params, {
        preferClient: clientId,
        stickyKey: record.key,
      }));
      // Session identity FIRST — same reasoning as the initial bind:
      // this rebind's subscription lives on the session that answered,
      // and any later sampling can observe a replacement session.
      servedSessionSeq = this.env.sessionSeq(servedBy);
    } catch {
      // Rebind failed (e.g. server doesn't support the method, or the
      // request timed out on a bad link). Leave orphaned — rebindOnce's
      // retry loop / the next connect will try again. Manager 'error'
      // events have already surfaced the underlying failure via
      // runAttempts.
      return;
    }
    // The record may have been unsubscribed between the env.call dispatch
    // and its resolution — and the key may since have been taken by a
    // NEW record. Identity, not key presence: a stale rebind that only
    // checked `has(key)` went on to mutate the dead record, consume the
    // replacement's buffered notification and announce a restore for a
    // subscription nobody holds.
    if (this.subs.get(record.key) !== record) {
      // We just created a subscription on `servedBy` for a record nobody
      // holds any more. `makeUnsub` could not have cleaned it up: the
      // record was orphaned (clientId null) when it was dropped, so it
      // had no server to send the unsubscribe to. Do it here, or that
      // server keeps pushing for a subscription with no consumer.
      // Electrum keys a subscription by method+params on a connection —
      // NOT by our local record. If a successor at this key is already
      // bound to (or is subscribing on) the same server, our cleanup
      // would cancel ITS subscription and the server would stop pushing
      // to a live subscriber.
      const successor = this.subs.get(record.key);
      // Deliberately conservative: with ANY successor record present —
      // whichever server it points at, because it may yet land (or
      // later rebind) exactly here, and any deferred re-check acts on a
      // snapshot the next transition can invalidate; one such deferred
      // cleanup was observed unsubscribing the very server a successor
      // had just rebound to — or a subscribe still in flight, ownership
      // of this server's subscription cannot be decided, and the
      // unsubscribe is sent only when NOBODY could own it. The cost of
      // restraint is a subscription the server pushes into the void
      // until that connection closes — bounded and self-healing; the
      // cost of a wrong unsubscribe is a live subscription silently
      // killed, which for a wallet is missed transactions with nothing
      // to correct them.
      if (successor === undefined && !this.pending.has(record.key)) {
        // Bind-time seq, captured right after the response: passing the
        // CURRENT seq made wireUnsubscribe's session guard a tautology,
        // and a servedBy that recycled while this continuation was
        // queued received spurious cleanup on a fresh session.
        this.wireUnsubscribe(record.method, record.params, servedBy, servedSessionSeq);
      }
      return;
    }
    // Bind to the actual server (retry may have failed over); if it died
    // while the call was in flight, stay orphaned for the next round.
    record.clientId = this.env.isClientConnected(servedBy) ? servedBy : null;
    record.servedBy = servedBy;
    record.boundSessionSeq = servedSessionSeq;
    const hadStatus = record.hasStatus;
    const baseline = record.lastKnownStatus;
    const wasUncertain = record.baselineUncertain;
    if (
      !record.hasStatus ||
      record.baselineUncertain ||
      !statusEquals(record.lastKnownStatus, status)
    ) {
      // Real data from the server that answered, delivered even if that
      // server has since become unusable — dropping it would lose the
      // only fresh view we got, and for a single-server pool (or one
      // where the answering client is merely banned) nothing would come
      // along to fetch it again.
      this.deliver(record, status);
    } else if (record.clientId !== null && record.awaitingBaseline.size > 0) {
      // No change, but handlers that joined while this record was unbound
      // are still waiting for their first callback — the binding they
      // were promised now exists.
      const waiting = [...record.awaitingBaseline];
      record.awaitingBaseline.clear();
      for (const [h, token] of waiting) this.invokeHandler(record, h, token, status);
    }
    // Pushes that raced this rebind's response flush here — delivered
    // for opaque statuses, purged-to-uncertainty for data-bearing ones
    // — synchronously, so the drift below reflects them either way.
    this.flushEarlyNotification(record);
    // Handlers ran synchronously above; one may have dropped the last
    // handle (record gone) or torn the binding down (removeServer from
    // inside a callback). Announcing a restore for either would send a
    // consumer refetching history for an address it no longer watches,
    // or tell it the subscription is live while nothing is delivering to
    // it — the retry loop / next rebind reports once a bind holds.
    if (this.subs.get(record.key) !== record) return;
    if (record.clientId === null) return;
    this.env.emit('subscription-restored', {
      method: record.method,
      params: record.params,
      // Measured against the status held BEFORE this rebind, so the
      // flushed pushes — and anything a handler did meanwhile — are
      // included. An uncertain baseline also counts as drift even when
      // the values compare equal: the delivery it forced IS the change
      // signal, and an event-driven consumer trusting drift:false would
      // skip the very resync that delivery asked for. A false positive
      // costs one resync, a false negative a missed change.
      // `baselineUncertain` (set by a flush that delivered) counts too:
      // storage keeps the serving view, so a flushed foreign change
      // signal is invisible to the value comparison — but the delivery
      // happened, and it IS the drift.
      drift:
        !hadStatus ||
        wasUncertain ||
        record.baselineUncertain ||
        !statusEquals(baseline, record.lastKnownStatus),
    });
  }

  /**
   * Call one handler, unless it stopped being ours since the fan-out
   * started.
   *
   * Every fan-out iterates a snapshot, because a callback may add or
   * remove handlers while it runs — that protects the loop, not the
   * callers. Two things can happen inside an earlier callback in the same
   * push: it can unsubscribe a sibling (gone from `handlers`), or it can
   * tear the whole registry down (`clear()` drops the record from `subs`
   * but leaves handler sets intact, and by then every unsubscribe handle
   * is a no-op, so the caller has no way to stop what is coming). Both
   * are checked here, once, rather than at each of the four call sites.
   */
  private invokeHandler(
    record: SubscriptionRecord,
    h: SubscriptionHandler,
    token: number,
    status: unknown,
  ): void {
    // The token catches the case the reference alone cannot: an earlier
    // callback in this same fan-out unsubscribed this handler and
    // subscribed the same function again, which is a NEW registration —
    // and it has already been given the status by its own attach.
    if (record.handlers.get(h) !== token) return;
    if (this.subs.get(record.key) !== record) return;
    this.invoke(h, status);
  }

  private invoke(h: SubscriptionHandler, status: unknown): void {
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
  // Two unequal strings can never be JSON-equal — skip the stringify on
  // the dominant scripthash-status path. Deliberately string-only: a
  // broader primitive fast path would flip the `0` vs `-0` result, and
  // objects with `toJSON` could stringify-equal a string.
  if (typeof a === 'string' && typeof b === 'string') return false;
  try {
    return JSON.stringify(a) === JSON.stringify(b);
  } catch {
    return false;
  }
}
