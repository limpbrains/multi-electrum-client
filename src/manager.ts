// ElectrumManager — orchestrates a pool of ElectrumClient instances behind a
// RoutingPolicy. Single entry point for callers; transparent failover, partial-
// batch retry, microtask auto-batch coalescing, per-client telemetry, typed
// method registry + namespace API (M3). Subscriptions + classifier + peer
// discovery (M4), cache (also M4 — needs the headers subscription to track
// finality), lifecycle suspend/resume (M5), and TCP/TLS transports (M6) plug
// in here in subsequent milestones.

import { findCacheSpec, isFinalized, MISS, readFromCache, writeToCache } from './cache/finality.js';
import type { CacheStore } from './cache/types.js';
import type { ClientId, ClientView, ConnectionState, Endpoint, Telemetry } from './client.js';
import { ElectrumClient } from './client.js';
import { PeerDiscoveryRunner } from './discovery.js';
import { defaultClassifier } from './errors/classifier.js';
import {
  NoClientAvailableError,
  ProtocolError,
  SuspendedError,
  type ErrorClassifier,
  type ErrorKind,
} from './errors/types.js';
import type { LifecycleState, SuspendOptions } from './lifecycle/types.js';
import type { PickContext, RoutingPolicy } from './policy/types.js';
import type { MethodName, ParamsOf, ResultOf } from './protocol/methods.js';
import type {
  BatchRequest,
  BlockHeader,
  CallOpts,
  ManagerOptions,
  Network,
  RawTxHex,
  Scripthash,
  ScripthashStatus,
  ServerSpec,
  TxId,
  TxVerbose,
} from './protocol/types.js';
import { ReconnectRunner } from './reconnect.js';
import { SubscriptionRegistry } from './subscriptions/registry.js';
import type { SubscriptionHandler } from './subscriptions/types.js';
import { defaultTransportFactory } from './transport/factory.js';
import type { Transport } from './transport/types.js';
import { deferred, type Deferred } from './util/deferred.js';
import { MicrotaskBatcher } from './util/microtask-batcher.js';
import { ok, err, type Result } from './util/result.js';
import { TelemetryAccumulator } from './util/telemetry.js';

export type { ManagerOptions, BatchRequest } from './protocol/types.js';

interface ClientMeta {
  bannedUntil: number | undefined;
  capabilities: { serverSoftware?: string; protocolVersion?: string };
  telemetry: TelemetryAccumulator;
}

interface BatchItem {
  method: string;
  params: readonly unknown[];
  opts: CallOpts | undefined;
  def: Deferred<unknown>;
  attempt: number;
  excluded: Set<ClientId>;
}

export interface ManagerEvents {
  'client-state': { clientId: ClientId; state: ConnectionState };
  'client-banned': { clientId: ClientId; until: number; reason: ErrorKind };
  'subscription-restored': { method: string; params: readonly unknown[]; drift: boolean };
  error: unknown;
}

type AttemptOutcome =
  | { kind: 'success'; clientId: ClientId; value: unknown }
  | { kind: 'error'; clientId: ClientId; error: Error; errorKind: ErrorKind }
  // policy returned null → no eligible client at all; bail.
  | { kind: 'no-pick'; error: Error }
  // policy named a client we no longer have (race with removeServer or a
  // stale id from a custom policy) → exclude + retry.
  | { kind: 'client-missing'; clientId: ClientId; error: Error };

/**
 * Trailing-args shape for `Manager.call`. Methods whose registry entry has an
 * empty params tuple (e.g. `server.ping`) accept `params` as optional;
 * everything else requires it. Unknown methods fall back to `readonly
 * unknown[]`. Conditional rest tuple is the only way to express "second arg
 * is required for some literal first args, optional for others" in TS.
 */
type CallArgs<M extends string> = M extends MethodName
  ? ParamsOf<M> extends readonly []
    ? [params?: readonly [], opts?: CallOpts]
    : [params: ParamsOf<M>, opts?: CallOpts]
  : [params: readonly unknown[], opts?: CallOpts];

export class ElectrumManager {
  readonly network: Network;
  private readonly clients = new Map<ClientId, ElectrumClient>();
  private readonly meta = new Map<ClientId, ClientMeta>();
  private readonly policy: RoutingPolicy;
  private readonly classifier: ErrorClassifier;
  private readonly autoBatchEnabled: boolean;
  private readonly cooldownMs: number;
  private readonly requestTimeoutMs: number | undefined;
  private readonly transportFactory: (endpoint: Endpoint) => Transport;
  private readonly batcher: MicrotaskBatcher<BatchItem>;
  private readonly registry: SubscriptionRegistry;
  private readonly cache: CacheStore | undefined;
  private readonly finalizedConfs: number;
  /**
   * Latest tip height, populated from the internal headers subscription.
   * `undefined` until the first header arrives — manager treats that as
   * "no tip known", which means cache writes are skipped (safer than
   * caching potentially-non-final data).
   */
  private tipHeight: number | undefined;
  /** Disposer for the internal headers subscription. */
  private tipUnsub: (() => Promise<void>) | null = null;
  private readonly discovery: PeerDiscoveryRunner | undefined;
  /**
   * Auto-handshake `server.version` on every connect. Default `true`;
   * disabled when the caller wants to drive `server.version` directly
   * (e.g. ElectrumX 1.16+ rejects a duplicate version call with
   * `"server.version already sent"`).
   */
  private readonly handshakeOnConnect: boolean;
  /**
   * Per-client backoff reconnect loop. See `./reconnect.ts` for the
   * lifecycle / cancellation contract; the manager just installs /
   * removes / cancels through the runner's API and forwards
   * connect-failure errors to the `error` event.
   */
  private readonly reconnect: ReconnectRunner;
  /** True while we're tearing down — guards async tasks against post-stop work. */
  private stopped = false;
  /**
   * Lifecycle state. `created → running → suspending → suspended →
   * resuming → running → stopped`. See lifecycle/types.ts.
   */
  private lifecycle: LifecycleState = 'created';
  /**
   * FIFO tail of suspend/resume transitions. Each `suspend()` / `resume()`
   * call appends its own task; the tail is the (catch-wrapped) promise
   * that settles when the most-recently-queued transition finishes.
   *
   * Replaces a previous tagged-singleton scheme that broke under 3+
   * overlapping calls: with that scheme, `suspend → resume → suspend`
   * could collapse onto the head, leaving the manager in whatever state
   * the second call landed on rather than the third caller's intent.
   * The FIFO chain runs each call's task in submission order; each task
   * re-evaluates lifecycle and is idempotent on its target state.
   */
  private transitionTail: Promise<void> = Promise.resolve();
  /**
   * Calls submitted while `suspended` — replayed in order during `resume()`.
   * Each entry's deferred is resolved with the eventual wire result (or
   * rejected with `SuspendedError` on `stop()` mid-suspend).
   */
  private readonly suspendQueue: Array<{
    method: string;
    params: readonly unknown[];
    opts: CallOpts | undefined;
    def: Deferred<unknown>;
  }> = [];
  // Standard typed-event-emitter pattern: store opaquely, cast at the API edge.
  private readonly listeners = new Map<keyof ManagerEvents, Set<(p: unknown) => void>>();

  constructor(opts: ManagerOptions & { transportFactory?: (e: Endpoint) => Transport }) {
    this.network = opts.network;
    this.policy = opts.policy;
    this.classifier = opts.classifier ?? defaultClassifier;
    this.autoBatchEnabled = opts.autoBatch ?? true;
    this.cooldownMs = opts.cooldownMs ?? 60_000;
    this.requestTimeoutMs = opts.requestTimeoutMs;
    this.transportFactory = opts.transportFactory ?? defaultTransportFactory;
    this.cache = opts.cache;
    this.finalizedConfs = opts.finalizedConfs ?? 6;
    this.discovery = opts.discover
      ? new PeerDiscoveryRunner(opts.discover, {
          call: (clientId, method, params) =>
            this.callDirect(method, params, { preferClient: clientId, retry: 'none' }),
          hasClient: (id) => this.clients.has(id),
          addServer: (spec) => this.addServer(spec),
          isStopped: () => this.stopped,
          onError: (e) => this.emit('error', e),
        })
      : undefined;
    this.handshakeOnConnect = opts.handshakeOnConnect ?? true;
    this.reconnect = new ReconnectRunner(
      opts.reconnectBackoff ?? { minMs: 500, maxMs: 30_000, factor: 2, jitter: 0.2 },
      {
        getClient: (id) => this.clients.get(id),
        isRunning: () => this.lifecycle === 'running' || this.lifecycle === 'created',
        onError: (e) => this.emit('error', e),
      },
    );
    this.batcher = new MicrotaskBatcher<BatchItem>((items) => {
      this.flushBatch(items).catch((e) => this.emit('error', e));
    });
    this.registry = new SubscriptionRegistry({
      call: (method, params, callOpts) =>
        // Subscribe wire calls bypass auto-batch — they need an immediate
        // round-trip so we have the initial status to compare against.
        this.callDirect(method, params, callOpts),
      emit: (event, payload) => this.emit(event, payload),
      pickConnectedClient: () => this.firstConnectedClient(),
      isClientConnected: (id) => this.isClientUsable(id),
    });
    for (const spec of opts.servers) {
      this.installServer(spec);
    }
  }

  /** Current lifecycle state. */
  get state(): LifecycleState {
    return this.lifecycle;
  }

  /**
   * Connect every server in parallel. Errors do not throw; they fire
   * `error` events. Only valid from `created` (fresh manager) or
   * `stopped` (re-init after a terminal stop). To wake from `suspended`
   * use `resume()`; calling `start()` instead would re-install the
   * tip subscription on top of a live one and skip queue draining.
   */
  async start(): Promise<void> {
    if (this.lifecycle !== 'created' && this.lifecycle !== 'stopped') {
      throw new SuspendedError(
        `cannot start from ${this.lifecycle}; use resume() to wake a suspended manager`,
      );
    }
    this.stopped = false;
    this.lifecycle = 'running';
    const tasks = [...this.clients.values()].map(async (c) => {
      try {
        await c.connect();
      } catch (e) {
        this.emit('error', e);
      }
    });
    await Promise.all(tasks);
    // If a cache is configured we need a tip to gate finalized writes.
    // Wire an internal headers subscription whose handler updates
    // `tipHeight`. Best-effort: errors surface as `error` events, the
    // cache simply remains read-only until the tip is known.
    if (this.cache && this.tipUnsub === null) {
      try {
        this.tipUnsub = await this.registry.subscribe<BlockHeader>(
          'blockchain.headers.subscribe',
          [],
          (h) => {
            // Validate at the boundary; an arbitrary registry handler
            // shape shouldn't be trusted to be a BlockHeader.
            if (h && typeof (h as BlockHeader).height === 'number') {
              this.tipHeight = (h as BlockHeader).height;
            }
          },
        );
      } catch (e) {
        this.emit('error', e);
      }
    }
  }

  /**
   * Disconnect every server and drop all subscriptions. Terminal.
   *
   * Awaits the FIFO transition tail before tearing down — any
   * already-queued suspend / resume calls observe `lifecycle === 'stopped'`
   * via their own re-evaluation and reject cleanly. Without this, a
   * runSuspend's tail (e.g. `await tipUnsub()`) or a chained transition
   * could fire after `await m.stop()` returned, surfacing as ghost
   * `error` events to a caller who already considers the manager dead.
   *
   * Note on `state` observability: `manager.state` flips to `'stopped'`
   * synchronously on entry — that flag is the *terminal intent*, not a
   * "teardown finished" signal. Sockets and tip-subscription cleanup
   * still run async; only after `await m.stop()` returns is teardown
   * fully complete. Code reading `m.state === 'stopped'` to gate UI
   * teardown should rely on the awaited resolve, not the field alone.
   */
  async stop(): Promise<void> {
    this.stopped = true;
    this.lifecycle = 'stopped';
    // Tail is catch-wrapped at append time — never rejects, so we can
    // safely `await` without try/catch.
    await this.transitionTail;
    // Reject anything queued during a prior suspend so callers don't dangle.
    while (this.suspendQueue.length > 0) {
      const item = this.suspendQueue.shift()!;
      item.def.reject(new SuspendedError('manager stopped before resume'));
    }
    this.discovery?.cancelAll();
    // Auto-reconnect off — clear timers + intent flags so a `disconnected`
    // event triggered by our own `c.disconnect()` below doesn't reschedule.
    this.reconnect.clear();
    if (this.tipUnsub) {
      try {
        await this.tipUnsub();
      } catch (e) {
        this.emit('error', e);
      }
      this.tipUnsub = null;
    }
    this.tipHeight = undefined;
    this.registry.clear();
    await Promise.all(
      [...this.clients.values()].map(async (c) => {
        try {
          await c.disconnect();
        } catch (e) {
          this.emit('error', e);
        }
      }),
    );
    // Drop event listeners after the last possible emit (`error` paths
    // above) so closures captured by user listeners don't keep the manager
    // / its dependencies pinned across a discarded reference.
    this.listeners.clear();
  }

  /**
   * Drain in-flight, close every socket, and enter `suspended`. The
   * subscription registry is preserved across suspend so `resume()` can
   * replay subscriptions with catch-up. Calls submitted while suspended
   * queue (or reject if `failOnSuspend` is set on the call). Idempotent —
   * calling `suspend` while already suspended / suspending is a no-op.
   *
   * `graceMs` (default 2000) bounds how long we wait for in-flight requests
   * to settle before forcibly rejecting them with `SuspendedError`.
   * `cancelInFlight: true` skips the wait entirely.
   */
  async suspend(opts: SuspendOptions = {}): Promise<void> {
    // Submit-time guard: caller invoking suspend on an already-stopped
    // manager gets a synchronous rejection (matches the documented
    // contract). Race with a concurrent stop — see doSuspendIfNeeded.
    if (this.lifecycle === 'stopped') {
      throw new SuspendedError('cannot suspend a stopped manager');
    }
    // Append to the FIFO chain: our task waits for any prior transition
    // (success or failure — outcome of the prior call is independent of
    // ours) and then re-evaluates lifecycle. The catch on the tail
    // assignment keeps it never-rejecting so subsequent appenders and
    // `stop()` can `await transitionTail` without seeing throws.
    const task = this.transitionTail
      .catch(() => undefined)
      .then(() => this.doSuspendIfNeeded(opts));
    this.transitionTail = task.catch(() => undefined);
    return task;
  }

  /**
   * Idempotent body of a queued suspend. Re-evaluates lifecycle at run
   * time so chained transitions land on the right action regardless of
   * what earlier transitions did. `created` flips to `suspended`
   * synchronously (no sockets to drain). Already-suspended or stopped is
   * a no-op — for the latter, the user must have called stop() while we
   * were queued; honor stop's terminal intent rather than re-throwing.
   */
  private async doSuspendIfNeeded(opts: SuspendOptions): Promise<void> {
    if (this.lifecycle === 'suspended' || this.lifecycle === 'stopped') return;
    if (this.lifecycle === 'created') {
      this.lifecycle = 'suspended';
      return;
    }
    await this.runSuspend(opts);
  }

  private async runSuspend(opts: SuspendOptions): Promise<void> {
    this.lifecycle = 'suspending';
    const graceMs = opts.graceMs ?? 2000;
    const cancelInFlight = opts.cancelInFlight ?? false;

    if (!cancelInFlight && graceMs > 0) {
      // Best-effort drain. Poll inFlightCount across all clients; bail out
      // when zero or grace elapses. Polling beats hooking each client's
      // resolve callback because in-flight may finish during the await.
      const deadline = Date.now() + graceMs;
      while (Date.now() < deadline) {
        if (this.totalInFlight() === 0) break;
        await sleep(20);
      }
    }
    // Anything left over: reject with `SuspendedError` so the caller sees
    // the right cause (they didn't lose the link, the manager paused).
    // We do this before `disconnect` because disconnect would otherwise
    // surface them as `TransportError("disconnected by client")`.
    for (const c of this.clients.values()) {
      if (c.inFlightCount > 0) {
        c.failInFlight(new SuspendedError('manager suspending'));
      }
    }
    await Promise.all(
      [...this.clients.values()].map(async (c) => {
        try {
          await c.disconnect();
        } catch (e) {
          this.emit('error', e);
        }
      }),
    );
    // Clear discover timers — they'd fire against dead sockets and route
    // via policy.pick to a different server. The next `resume()` re-arms
    // them on every `connected` transition. Caveat: a client that fails
    // to reconnect on resume() never fires `connected` and therefore
    // never re-arms its discover timer; peer discovery for that endpoint
    // dies until the user manually `removeServer` + `addServer`s, or the
    // client eventually reconnects via the underlying transport's
    // backoff.
    this.discovery?.cancelAll();
    // Cancel any pending reconnects — sockets we just closed would
    // otherwise auto-reconnect during suspend. resume() drives the
    // explicit reconnect path. `cancelAllTimers` preserves the
    // per-client `wants` flags so a disconnect-fault during `running`
    // after resume() resumes the backoff loop.
    this.reconnect.cancelAllTimers();
    // Tip becomes stale across suspend; cache writes are gated until
    // `resume()` re-establishes the headers subscription. We INVOKE the
    // tipUnsub (not just null it) — otherwise the registry's per-key
    // record retains the previous handler in its Set, and the next
    // resume's subscribe call adds a *second* handler to the same record.
    // After N suspend/resume cycles a long-lived RN session would have N
    // copies of the tip-tracker handler all firing on every header push.
    this.tipHeight = undefined;
    if (this.tipUnsub) {
      try {
        await this.tipUnsub();
      } catch (e) {
        this.emit('error', e);
      }
      this.tipUnsub = null;
    }
    // Note: registry / suspendQueue / cache state preserved.
    // Race: a concurrent `stop()` may have flipped lifecycle to 'stopped'
    // while we were awaiting the disconnect. Don't clobber that — the
    // stopped flag is terminal. Cast widens the TS-narrowed literal so the
    // runtime check survives the type system's view.
    if ((this.lifecycle as LifecycleState) !== 'stopped') {
      this.lifecycle = 'suspended';
    }
  }

  /**
   * Reconnect all clients, replay subscriptions with catch-up, and drain
   * the suspend queue. Idempotent on `running`. Calls during `resuming`
   * see the `running` gate path once the state flips.
   */
  async resume(): Promise<void> {
    if (this.lifecycle === 'stopped') {
      throw new SuspendedError('cannot resume a stopped manager');
    }
    const task = this.transitionTail.catch(() => undefined).then(() => this.doResumeIfNeeded());
    this.transitionTail = task.catch(() => undefined);
    return task;
  }

  /**
   * Idempotent body of a queued resume. Re-evaluates lifecycle at run
   * time. Already-running or stopped is a no-op (stopped honors the
   * user's terminal intent if a concurrent stop landed while queued).
   * `created` throws with a `start()` hint; only `suspended` proceeds.
   */
  private async doResumeIfNeeded(): Promise<void> {
    if (this.lifecycle === 'running' || this.lifecycle === 'stopped') return;
    if (this.lifecycle !== 'suspended') {
      throw new SuspendedError(
        this.lifecycle === 'created'
          ? 'cannot resume from created — use start() on a fresh manager'
          : `cannot resume from ${this.lifecycle}`,
      );
    }
    await this.runResume();
  }

  private async runResume(): Promise<void> {
    this.lifecycle = 'resuming';
    // Reconnect: each client's `connected` transition fires
    // `onStateChange` which in turn calls `restoreOrphans()`. We don't
    // have to drive subscription replay manually.
    await Promise.all(
      [...this.clients.values()].map(async (c) => {
        try {
          await c.connect();
        } catch (e) {
          this.emit('error', e);
        }
      }),
    );
    // Race check: a concurrent `stop()` may have flipped lifecycle to
    // 'stopped' while we awaited reconnects. Bail before re-installing
    // the headers subscription / draining the queue (which would
    // dispatch through call() against soon-to-be-disconnected clients).
    // Cast through LifecycleState because TS narrows `lifecycle` from the
    // earlier `=== 'suspended'` check and doesn't know a concurrent stop()
    // can mutate it across awaits.
    if ((this.lifecycle as LifecycleState) === 'stopped') {
      while (this.suspendQueue.length > 0) {
        const item = this.suspendQueue.shift()!;
        item.def.reject(new SuspendedError('manager stopped during resume'));
      }
      return;
    }
    // Re-install the tip subscription if a cache was configured. `tipUnsub`
    // was cleared inside `suspend()`; it should always be `null` here.
    if (this.cache && this.tipUnsub === null) {
      try {
        this.tipUnsub = await this.registry.subscribe<BlockHeader>(
          'blockchain.headers.subscribe',
          [],
          (h) => {
            if (h && typeof (h as BlockHeader).height === 'number') {
              this.tipHeight = (h as BlockHeader).height;
            }
          },
        );
      } catch (e) {
        this.emit('error', e);
      }
    }
    // Re-check after the second await for the same race window.
    // Cast through LifecycleState because TS narrows `lifecycle` from the
    // earlier `=== 'suspended'` check and doesn't know a concurrent stop()
    // can mutate it across awaits.
    if ((this.lifecycle as LifecycleState) === 'stopped') {
      while (this.suspendQueue.length > 0) {
        const item = this.suspendQueue.shift()!;
        item.def.reject(new SuspendedError('manager stopped during resume'));
      }
      return;
    }
    // Drain order matters: splice the queue while we're still `resuming`
    // (so any call() that lands between this line and the lifecycle flip
    // joins the queue rather than dispatching ahead of the drained
    // items), then flip to `running`, then re-issue. The for-loop is
    // synchronous, so all drained calls hit the lifecycle gate (now
    // `running`) and dispatch in arrival order before any subsequent
    // microtask-scheduled caller can sneak in.
    const queued = this.suspendQueue.splice(0);
    this.lifecycle = 'running';
    for (const item of queued) {
      this.call(item.method, item.params, item.opts).then(
        (v) => item.def.resolve(v),
        (e) => item.def.reject(e),
      );
    }
  }

  /**
   * Add a server to the pool. Coordinated with lifecycle:
   *  - `running`: connect immediately.
   *  - `created`: install only; `start()` will connect.
   *  - `suspending` / `suspended`: install only; the next `resume()` will
   *    connect along with the rest of the pool. An eager connect here
   *    would defeat suspend (a fresh socket goes live while the manager
   *    is supposed to be paused).
   *  - `resuming`: connect immediately. resume()'s reconnect snapshot was
   *    already captured, so we wire this one up ourselves.
   *  - `stopped`: throws — terminal state.
   */
  addServer(spec: ServerSpec): void {
    if (this.lifecycle === 'stopped') {
      throw new SuspendedError('cannot addServer on a stopped manager');
    }
    this.installServer(spec);
    const client = this.clients.get(spec.id);
    if (!client) return;
    if (this.lifecycle === 'created') return;
    if (this.lifecycle === 'suspending' || this.lifecycle === 'suspended') return;
    client.connect().catch((e) => this.emit('error', e));
  }

  async removeServer(id: ClientId): Promise<void> {
    const c = this.clients.get(id);
    if (!c) return;
    // Clear reconnect intent BEFORE disconnect so the resulting
    // `disconnected` event doesn't reschedule the backoff loop.
    this.reconnect.unregister(id);
    // Disconnect first so any in-flight retry that races sees the client in
    // its 'disconnected' state via the candidate snapshot rather than missing
    // entirely (the latter would surface as a misleading "client disappeared"
    // error). Only after the socket is closed do we drop the entry.
    try {
      await c.disconnect();
    } catch (e) {
      this.emit('error', e);
    }
    this.clients.delete(id);
    this.meta.delete(id);
  }

  /**
   * Typed call. The single conditional signature picks param/result types from
   * the method registry when `method` is a known wire name and falls back to
   * `readonly unknown[]` / `unknown` otherwise. The trailing rest tuple lets
   * us drop `params` for empty-tuple methods (`m.call('server.ping')`) while
   * keeping it required for everything else.
   *
   *   const bal = await manager.call('blockchain.scripthash.get_balance', [hash]);
   *   //    ^? Balance
   *   await manager.call('server.ping');                  // params optional
   *   const x = (await manager.call('vendor.specific', [1, 2])) as MyType;
   */
  call<M extends string>(
    method: M,
    ...args: CallArgs<M>
  ): Promise<M extends MethodName ? ResultOf<M> : unknown>;
  async call(method: string, params: readonly unknown[] = [], opts?: CallOpts): Promise<unknown> {
    // Lifecycle gate: while the manager is in (or transitioning through)
    // a non-running state, calls either reject (when `failOnSuspend` is
    // set) or queue and replay on `resume()`. `resuming` is included so
    // calls submitted between the state flip to `resuming` and the final
    // flip to `running` don't race ahead of items already in the queue —
    // ordering would otherwise break.
    //
    // `failOnSuspend` is consumed at queue-time only: a call that joins
    // the queue without it set will not be re-checked at drain time
    // (lifecycle is `running` then anyway). For mid-suspend cancel, pass
    // `opts.signal` — abort drops the queue entry and rejects locally.
    if (
      this.lifecycle === 'suspending' ||
      this.lifecycle === 'suspended' ||
      this.lifecycle === 'resuming'
    ) {
      if (opts?.failOnSuspend) {
        throw new SuspendedError(`manager is ${this.lifecycle}`);
      }
      const def = deferred<unknown>();
      const item = { method, params, opts, def };
      this.suspendQueue.push(item);
      // Honor a pre-supplied AbortSignal: a caller who aborts while their
      // call is queued must not hang until resume(). On abort we drop the
      // queue entry and reject locally; the eventual drain skips it. If
      // the signal is already aborted, reject + skip enqueue immediately.
      const signal = opts?.signal;
      if (signal) {
        if (signal.aborted) {
          this.dropQueueItem(item);
          def.reject(signalAbortReason(signal));
        } else {
          const onAbort = (): void => {
            this.dropQueueItem(item);
            def.reject(signalAbortReason(signal));
          };
          signal.addEventListener('abort', onAbort, { once: true });
          // Best-effort cleanup of the listener once the deferred settles
          // (success or failure) so we don't pin the signal across long
          // suspends. AbortSignals are caller-owned; we err on the side of
          // unbinding eagerly. Catch + ignore: this branch must not
          // observe rejections (the caller's `await` is the owner).
          def.promise.then(
            () => signal.removeEventListener('abort', onAbort),
            () => signal.removeEventListener('abort', onAbort),
          );
        }
      }
      return def.promise;
    }
    if (this.lifecycle === 'stopped') {
      throw new SuspendedError('manager is stopped');
    }

    // Cache lookup happens up-front: if the method is on the cacheable
    // allow-list and we already have a value, return it without ever
    // touching the wire. Cache writes happen after a successful wire
    // call. `bypassCache` opts out of both the read and the write.
    const cache = this.cache;
    const spec = cache && !opts?.bypassCache ? findCacheSpec(method, params) : null;
    const onCacheError = (e: unknown): void => this.emit('error', e);
    if (cache && spec) {
      const hit = await readFromCache(cache, this.network, spec, onCacheError);
      if (hit !== MISS) return hit;
    }

    const value = await this.callInner(method, params, opts);
    if (cache && spec && isFinalized(spec.finalityHeight, this.tipHeight, this.finalizedConfs)) {
      // Fire-and-forget: a slow cache adapter must not block the
      // caller. `writeToCache` catches every failure internally and
      // routes through `onCacheError`, so no outer `.catch` is needed.
      void writeToCache(cache, this.network, spec, value, onCacheError);
    }
    return value;
  }

  /**
   * The pre-cache call path. Same logic as the public `call` body but
   * without the cache layer — used internally by the cache wrapper above
   * and by the namespace API when a method needs to bypass the cache
   * (e.g. mempool-sensitive lookups).
   */
  private async callInner(
    method: string,
    params: readonly unknown[],
    opts?: CallOpts,
  ): Promise<unknown> {
    // `preferClient` pins to a specific server; batching groups by policy.pick
    // and would lose the pin, so always take the direct path when set.
    const useBatch = opts?.preferClient === undefined && (opts?.autoBatch ?? this.autoBatchEnabled);
    if (useBatch) {
      const def = deferred<unknown>();
      this.batcher.enqueue({
        method,
        params,
        opts,
        def,
        attempt: 0,
        excluded: new Set(),
      });
      return def.promise;
    }
    return this.runAttempts(
      method,
      params,
      new Set<ClientId>(),
      this.maxAttemptsFor(opts),
      0,
      opts,
    );
  }

  /**
   * Friendly camelCase namespace API generated from the method registry.
   * Each member is a thin `call` wrapper with the right param tuple typed in.
   * Both arrow functions and the underlying `call` resolve through the same
   * routing pipeline (auto-batch coalescing, retry, telemetry).
   */
  readonly scripthash = {
    getBalance: (hash: Scripthash, opts?: CallOpts) =>
      this.call('blockchain.scripthash.get_balance', [hash], opts),
    getHistory: (hash: Scripthash, opts?: CallOpts) =>
      this.call('blockchain.scripthash.get_history', [hash], opts),
    listUnspent: (hash: Scripthash, opts?: CallOpts) =>
      this.call('blockchain.scripthash.listunspent', [hash], opts),
    /**
     * Subscribe to scripthash status changes. The handler is invoked once
     * with the initial status and then on every server-pushed change. The
     * returned `Unsubscribe` removes this handler; when the last handler for
     * a given scripthash is gone the manager sends
     * `blockchain.scripthash.unsubscribe` to the bound server.
     *
     * Multiple callers asking for the same scripthash share one wire
     * subscription — handlers fan out from a single notification stream.
     *
     * Lifecycle: throws synchronously when called outside `running` (no
     * connected client to bind to). Unlike `call()`, subscriptions are
     * not queued across suspend — wait for `resume()` and re-subscribe.
     * Existing subscriptions are preserved across suspend / resume and
     * replayed with catch-up automatically.
     */
    subscribe: (hash: Scripthash, handler: SubscriptionHandler<ScripthashStatus>) =>
      this.registry.subscribe<ScripthashStatus>('blockchain.scripthash.subscribe', [hash], handler),
    /**
     * Direct wire `blockchain.scripthash.unsubscribe`. Bypasses the
     * SubscriptionRegistry; use the `Unsubscribe` returned from
     * `subscribe(...)` for the registry-managed path.
     */
    unsubscribe: (hash: Scripthash, opts?: CallOpts) =>
      this.call('blockchain.scripthash.unsubscribe', [hash], opts),
  };

  readonly transaction = {
    get: (txid: TxId, opts?: CallOpts) => this.call('blockchain.transaction.get', [txid], opts),
    /**
     * Verbose form of `blockchain.transaction.get` — returns the server-
     * decoded tx shape (`TxVerbose`) instead of raw hex.
     *
     * **Caveat:** the result is `as`-cast unchecked. The wire method has two
     * response shapes keyed on a second `verbose=true` param; the registry
     * can't elegantly express that without forcing every plain caller to
     * narrow `string | TxVerbose`, so we route via the unknown-method
     * overload and trust the cast. The post-M4 decoder pass will validate
     * the shape at runtime.
     */
    getVerbose: async (txid: TxId, opts?: CallOpts): Promise<TxVerbose> => {
      const verboseMethod: string = 'blockchain.transaction.get';
      return (await this.call(verboseMethod, [txid, true], opts)) as TxVerbose;
    },
    broadcast: (rawTx: RawTxHex, opts?: CallOpts) =>
      this.call('blockchain.transaction.broadcast', [rawTx], opts),
    getMerkle: (txid: TxId, height: number, opts?: CallOpts) =>
      this.call('blockchain.transaction.get_merkle', [txid, height], opts),
    /**
     * Reverse lookup: txid at `txPos` in the block at `height`. Useful for
     * chasing the txid of a position-encoded reference (e.g. some SPV
     * proof formats). Non-merkle form — for the merkle-bundled variant
     * call `manager.call(..., [height, pos, true])` directly.
     *
     * Wire-shape quirks normalized here:
     *  - electrs rejects the 2-arg form as `"invalid params"`, so we
     *    always send `merkle=false` as the 3rd arg.
     *  - electrs ≥ 0.11 returns `{tx_id: string}` instead of a bare
     *    string for the non-merkle form. We unwrap so callers across
     *    every server impl see `Promise<TxId>`.
     */
    idFromPos: async (height: number, txPos: number, opts?: CallOpts): Promise<TxId> => {
      const r = await this.call('blockchain.transaction.id_from_pos', [height, txPos, false], opts);
      if (typeof r === 'string') return r;
      // electrs envelope: { tx_id: "..." }
      if (r && typeof r === 'object' && 'tx_id' in r && typeof r.tx_id === 'string') {
        return r.tx_id;
      }
      throw new ProtocolError(
        `blockchain.transaction.id_from_pos: unexpected result shape ${JSON.stringify(r)}`,
      );
    },
  };

  readonly headers = {
    /**
     * Subscribe to new chain tips. Handler fires immediately with the
     * current tip and on every header notification. Returns `Unsubscribe`
     * (last handler removed → manager stops dispatching; the wire
     * `blockchain.headers.subscribe` has no paired unsubscribe so the
     * server keeps pushing for the session — documented quirk).
     *
     * Same lifecycle contract as `scripthash.subscribe`: throws when
     * called outside `running`; existing subscriptions survive
     * suspend / resume.
     */
    subscribe: (handler: SubscriptionHandler<BlockHeader>) =>
      this.registry.subscribe<BlockHeader>('blockchain.headers.subscribe', [], handler),
    /**
     * Fetch the current tip. Electrum has no separate "get tip" wire method
     * — the only way to read it is to call `blockchain.headers.subscribe`,
     * whose response includes the current header. The server treats this
     * as a real subscription and will start pushing header notifications
     * on the same connection; the registry has no record for them so they
     * are dropped silently. If you also use `headers.subscribe(handler)`
     * the server may produce two pushes per block (one for each
     * registration). Use this only when you don't intend to subscribe.
     */
    getTip: (opts?: CallOpts) => this.call('blockchain.headers.subscribe', [], opts),
    getHeader: (height: number, opts?: CallOpts) =>
      this.call('blockchain.block.header', [height], opts),
  };

  readonly server = {
    ping: (opts?: CallOpts) => this.call('server.ping', [], opts),
    version: (clientName: string, protocolVersion: string, opts?: CallOpts) =>
      this.call('server.version', [clientName, protocolVersion], opts),
    banner: (opts?: CallOpts) => this.call('server.banner', [], opts),
  };

  estimateFee(confirmationTarget: number, opts?: CallOpts) {
    return this.call('blockchain.estimatefee', [confirmationTarget], opts);
  }

  readonly mempool = {
    /**
     * Fetch the server's mempool fee histogram. Returns `[[feeSatVb,
     * vsize], ...]` in **descending** fee order; an empty mempool is
     * `[]`. Wallets walk the array to derive a "next-block" fee from
     * cumulative `vsize`. Not finality-gated and not cached — mempool
     * state changes on every block and every broadcast.
     */
    getFeeHistogram: (opts?: CallOpts) => this.call('mempool.get_fee_histogram', [], opts),
  };

  /**
   * Run a list of requests; each gets its own routing decision. Returns one
   * Result per request in input order. Per-item failures do not reject the
   * outer promise — caller inspects `Result.ok`. Auto-batch coalescing groups
   * sub-requests by chosen client into wire-level JSON-RPC batches.
   */
  async batch<T = unknown>(reqs: readonly BatchRequest[], opts?: CallOpts): Promise<Result<T>[]> {
    return Promise.all(
      reqs.map(async (r): Promise<Result<T>> => {
        try {
          // r.method is a runtime string (not a literal), so call resolves to
          // Promise<unknown> via the conditional return type. Caller asserts
          // the per-request result via the batch's `<T>`.
          const v = (await this.call(r.method, r.params, opts)) as T;
          return ok(v);
        } catch (e) {
          return err(e as Error);
        }
      }),
    );
  }

  /** Subscribe to a manager event. Returns an unsubscribe function. */
  on<K extends keyof ManagerEvents>(event: K, listener: (p: ManagerEvents[K]) => void): () => void {
    let set = this.listeners.get(event);
    if (!set) {
      set = new Set();
      this.listeners.set(event, set);
    }
    const fn = listener as (p: unknown) => void;
    set.add(fn);
    return () => {
      set!.delete(fn);
    };
  }

  /** Snapshot of all clients (test/diagnostic helper). */
  getClientViews(): ClientView[] {
    return this.buildCandidates();
  }

  // --- Internals -----------------------------------------------------------

  private installServer(spec: ServerSpec): void {
    if (this.clients.has(spec.id)) {
      throw new ProtocolError(`duplicate server id: ${spec.id}`);
    }
    const endpoint: Endpoint = {
      host: spec.host,
      port: spec.port,
      protocol: spec.protocol,
      ...(spec.path !== undefined ? { path: spec.path } : {}),
    };
    const transport = this.transportFactory(endpoint);
    const client = new ElectrumClient({
      id: spec.id,
      endpoint: transport.endpoint,
      transport,
      ...(this.requestTimeoutMs !== undefined ? { requestTimeoutMs: this.requestTimeoutMs } : {}),
    });
    client.onNotification((notif) => {
      // Notifications fire on the transport's read path. An uncaught throw
      // here propagates into the transport handler — at best the client's
      // dispatch loop tears down, at worst the WS layer drops the socket.
      // Surface the failure as an `error` event and keep the stream alive.
      try {
        const { subParams, status } = decodeNotification(notif.method, notif.params);
        this.registry.notify(spec.id, notif.method, subParams, status);
      } catch (e) {
        this.emit('error', e);
      }
    });
    client.onStateChange((state) => {
      this.emit('client-state', { clientId: spec.id, state });
      if (state === 'connected') {
        // Fresh connection — reset reconnect backoff; the next disconnect
        // will start over from `minMs`.
        this.reconnect.resetAttempts(spec.id);
        // Identify the server software so the per-software classifier
        // tables actually run instead of falling through to the generic
        // table. Fire-and-forget; failures surface as `error` events.
        if (this.handshakeOnConnect) {
          this.handshakeVersion(spec.id, client).catch((e) => this.emit('error', e));
        }
        // Fire-and-forget: rebind any orphaned subs onto the new connection.
        // Errors surface through the manager `error` event via runAttempts.
        this.registry.restoreOrphans().catch((e) => this.emit('error', e));
        // Kick off a peer-discovery probe on this fresh connection.
        // `runFor` self-no-ops when discovery is disabled; we don't
        // gate at the call site so the runner stays the single source
        // of truth for "is discovery active right now".
        this.discovery?.runFor(spec.id).catch((e) => this.emit('error', e));
      } else if (state === 'disconnected') {
        this.registry.clientDisconnected(spec.id);
        // Subs bound to this client are now orphaned — immediately try to
        // re-bind them onto any other already-connected client without
        // waiting for the next state transition.
        this.registry.restoreOrphans().catch((e) => this.emit('error', e));
        // Cancel any scheduled re-poll: it would fire against a dead
        // client and route via policy.pick to a different server, which
        // is fine but wasteful. The next `connected` re-installs it.
        this.discovery?.cancelFor(spec.id);
        // Schedule a backoff reconnect. Skipped while suspending /
        // suspended (those states close sockets deliberately and resume()
        // drives the reconnect) and when the user has explicitly removed
        // / stopped the manager.
        this.reconnect.schedule(spec.id);
      }
    });
    this.clients.set(spec.id, client);
    this.reconnect.register(spec.id);
    this.meta.set(spec.id, {
      bannedUntil: undefined,
      capabilities: {},
      telemetry: new TelemetryAccumulator(),
    });
  }

  /**
   * Loop attemptOnce until success, non-retryable error, no-pick, or budget
   * exhausted. Single source of routing logic for both the direct call path
   * and the partial-batch retry path. `seed` lets the retry path carry the
   * original failure cause through to a final NoClientAvailableError.
   *
   * `client-missing` outcomes (policy named a client we no longer have) are
   * recovered transparently — exclude that id and try again without burning
   * a real attempt. Retries are reserved for actual failures.
   */
  private async runAttempts(
    method: string,
    params: readonly unknown[],
    excluded: Set<ClientId>,
    maxAttempts: number,
    initialAttempt: number,
    opts?: CallOpts,
    seed?: Error,
  ): Promise<unknown> {
    let lastErr: Error | undefined = seed;
    let attempt = initialAttempt;
    // Bound on `client-missing` recoveries: every real client can plausibly
    // be missing once (race with removeServer), plus a small slack. A buggy
    // policy that ignores `excluded` and keeps returning the same stale id
    // would otherwise spin forever — `attempt` doesn't move on missing.
    let missCount = 0;
    const missCap = this.clients.size + 4;
    while (attempt < maxAttempts) {
      const outcome = await this.attemptOnce(method, params, excluded, attempt, opts);
      if (outcome.kind === 'success') return outcome.value;
      if (outcome.kind === 'no-pick') throw lastErr ?? outcome.error;
      if (outcome.kind === 'client-missing') {
        excluded.add(outcome.clientId);
        if (++missCount > missCap) {
          throw (
            lastErr ??
            new NoClientAvailableError(
              `policy kept returning stale client ids (${missCount} consecutive); aborting`,
            )
          );
        }
        // Don't bump `attempt`: missing-client doesn't count as a real try.
        continue;
      }
      // 'error'
      lastErr = outcome.error;
      excluded.add(outcome.clientId);
      attempt++;
      if (!isRetryable(outcome.errorKind)) throw outcome.error;
    }
    throw lastErr ?? new NoClientAvailableError('exhausted retry budget');
  }

  /**
   * Direct-path call (no auto-batch). Used by SubscriptionRegistry for
   * subscribe/unsubscribe wire calls — those need a guaranteed round-trip
   * on a known client so we have the initial status to compare against,
   * which auto-batch coalescing would obscure.
   */
  private callDirect(
    method: string,
    params: readonly unknown[],
    opts?: CallOpts,
  ): Promise<unknown> {
    return this.runAttempts(
      method,
      params,
      new Set<ClientId>(),
      this.maxAttemptsFor(opts),
      0,
      opts,
    );
  }

  /**
   * Return the id of any client currently in `connected` state, or `null`.
   * SubscriptionRegistry uses this to bind / re-bind subs without consulting
   * the routing policy (subs are pinned, not load-balanced).
   */
  private firstConnectedClient(): ClientId | null {
    const now = Date.now();
    for (const [id, client] of this.clients) {
      if (client.getState() !== 'connected') continue;
      const meta = this.meta.get(id);
      if (meta?.bannedUntil !== undefined && meta.bannedUntil > now) continue;
      return id;
    }
    return null;
  }

  /**
   * True iff the client is in the pool, in `connected` state, and not
   * currently banned. Used by the subscription registry to gate a wire
   * `unsubscribe` at the bound server (a fall-through to a different
   * server would no-op or surface a misleading rpc-error).
   */
  private isClientUsable(id: ClientId): boolean {
    const client = this.clients.get(id);
    if (!client || client.getState() !== 'connected') return false;
    const meta = this.meta.get(id);
    if (meta?.bannedUntil !== undefined && meta.bannedUntil > Date.now()) return false;
    return true;
  }

  /**
   * Issue `server.version` on a freshly connected client and stash the
   * returned `[softwareName, protocolVersion]` pair into the client's
   * `capabilities`. The `ErrorClassifier` consults `serverSoftware` to
   * pick its per-software substring table; without this handshake every
   * client would always fall through to the generic table and the
   * vendor-specific tables would be dead code. Fire-and-forget — a
   * server that refuses `server.version` (or returns a non-tuple
   * shape) just keeps `serverSoftware` undefined; the classifier still
   * works via the generic path.
   *
   * Skipped if `capabilities.serverSoftware` is already populated —
   * server software doesn't change between disconnects on the same
   * `ServerSpec`, and ElectrumX 1.16+ rejects a duplicate
   * `server.version` call with `"server.version already sent"` if the
   * server-side session somehow persisted (rare but possible). This
   * also keeps `resume()` cheap: no extra round-trip per client when
   * we already know the software.
   */
  private async handshakeVersion(id: ClientId, client: ElectrumClient): Promise<void> {
    const existing = this.meta.get(id);
    if (existing?.capabilities.serverSoftware !== undefined) return;
    let v: unknown;
    try {
      v = await client.call('server.version', ['multi-electrum-client', '1.4']);
    } catch {
      return; // Server doesn't speak version, or we raced a disconnect.
    }
    if (!Array.isArray(v) || v.length < 2) return;
    const [software, protocolVersion] = v;
    if (typeof software !== 'string' || typeof protocolVersion !== 'string') return;
    const meta = this.meta.get(id);
    if (!meta) return; // Client removed mid-handshake.
    meta.capabilities = { serverSoftware: software, protocolVersion };
  }

  // Reconnect loop is implemented as `ReconnectRunner` in
  // `./reconnect.ts`; manager's onStateChange / lifecycle / removeServer
  // paths drive it through `register` / `schedule` / `resetAttempts` /
  // `cancelAllTimers` / `clear`.

  /** Sum of in-flight requests across every connected client. */
  private totalInFlight(): number {
    let n = 0;
    for (const c of this.clients.values()) n += c.inFlightCount;
    return n;
  }

  /** Remove a queued suspend item by reference. No-op if already drained. */
  private dropQueueItem(target: { def: Deferred<unknown> }): void {
    const i = this.suspendQueue.findIndex((q) => q.def === target.def);
    if (i >= 0) this.suspendQueue.splice(i, 1);
  }

  // Cache layer is implemented as pure functions in `./cache/finality.ts`;
  // peer discovery is implemented as `PeerDiscoveryRunner` in
  // `./discovery.ts`. Manager delegates to both so this file stays
  // focused on routing, lifecycle, and telemetry.

  /**
   * One pick-and-call cycle: ask the policy, send the request, classify the
   * outcome, record telemetry, ban on rate-limit. Caller decides whether to
   * loop based on the returned outcome kind. `attempt` is forwarded into
   * `PickContext.attempt` verbatim so user policies see one consistent value
   * regardless of which call path drove the pick.
   */
  private async attemptOnce(
    method: string,
    params: readonly unknown[],
    excluded: ReadonlySet<ClientId>,
    attempt: number,
    opts?: CallOpts,
  ): Promise<AttemptOutcome> {
    const candidates = this.buildCandidates();
    const now = Date.now();

    // `preferClient` lets the registry route an unsubscribe (or any other
    // pinned call) at the exact server we're targeting without consulting
    // the policy. Honored only when the client is in the pool, connected,
    // not banned, and not in `excluded`. Falls through to `policy.pick`
    // otherwise.
    const preferred = opts?.preferClient;
    let clientId: ClientId | null = null;
    if (preferred !== undefined && !excluded.has(preferred)) {
      const view = candidates.find((c) => c.id === preferred);
      if (view && view.state === 'connected' && (view.bannedUntil ?? 0) <= now) {
        clientId = preferred;
      }
    }
    if (clientId === null) {
      const ctx: PickContext = {
        request: { method, params },
        attempt,
        excluded,
        candidates,
        now,
        ...(opts?.stickyKey !== undefined ? { stickyKey: opts.stickyKey } : {}),
      };
      clientId = this.policy.pick(ctx);
    }
    if (clientId === null) {
      return {
        kind: 'no-pick',
        error: new NoClientAvailableError(`no eligible client for ${method}`),
      };
    }
    const client = this.clients.get(clientId);
    if (!client) {
      return {
        kind: 'client-missing',
        clientId,
        error: new NoClientAvailableError(`client ${clientId} no longer in pool`),
      };
    }
    const start = Date.now();
    try {
      const value = await client.call(method, params);
      this.recordSuccess(clientId, method, Date.now() - start);
      return { kind: 'success', clientId, value };
    } catch (e) {
      const elapsed = Date.now() - start;
      const errorKind = this.classifyFor(clientId, method, e, elapsed);
      this.recordError(clientId, method, errorKind, elapsed);
      return { kind: 'error', clientId, error: e as Error, errorKind };
    }
  }

  private async flushBatch(items: BatchItem[]): Promise<void> {
    // One candidate snapshot per flush; the policy sees a stable view across
    // every item it picks for. N items × M clients allocations would otherwise
    // blow up here.
    const candidates = this.buildCandidates();
    const now = Date.now();
    const groups = new Map<ClientId, BatchItem[]>();
    const unroutable: BatchItem[] = [];

    for (const item of items) {
      const ctx: PickContext = {
        request: { method: item.method, params: item.params },
        attempt: item.attempt,
        excluded: item.excluded,
        candidates,
        now,
        ...(item.opts?.stickyKey !== undefined ? { stickyKey: item.opts.stickyKey } : {}),
      };
      const clientId = this.policy.pick(ctx);
      if (clientId === null) {
        unroutable.push(item);
        continue;
      }
      const grp = groups.get(clientId) ?? [];
      grp.push(item);
      groups.set(clientId, grp);
    }

    for (const item of unroutable) {
      item.def.reject(new NoClientAvailableError(`no eligible client for ${item.method}`));
    }

    await Promise.all(
      [...groups.entries()].map(async ([clientId, grp]) => this.dispatchGroup(clientId, grp)),
    );
  }

  private async dispatchGroup(clientId: ClientId, grp: BatchItem[]): Promise<void> {
    const client = this.clients.get(clientId);
    if (!client) {
      // Initial-flush race with removeServer. Treat as `client-missing`
      // (consistent with runAttempts): exclude the id and re-route without
      // burning a real attempt against the actual server.
      for (const item of grp) {
        item.excluded.add(clientId);
        this.runAttempts(
          item.method,
          item.params,
          item.excluded,
          this.maxAttemptsFor(item.opts),
          item.attempt,
          item.opts,
        ).then(
          (v) => item.def.resolve(v),
          (e) => item.def.reject(e),
        );
      }
      return;
    }

    const start = Date.now();
    let results: Result<unknown>[];
    try {
      results = await client.batchCall(grp.map((i) => ({ method: i.method, params: i.params })));
    } catch (e) {
      // Whole-batch transport failure. Treat every item as failed and retry.
      const elapsed = Date.now() - start;
      const kind = this.classifyFor(clientId, '<batch>', e, elapsed);
      this.recordError(clientId, '<batch>', kind, elapsed);
      for (const item of grp) {
        item.excluded.add(clientId);
        if (isRetryable(kind)) {
          this.maybeRetry(item, e as Error);
        } else {
          item.def.reject(e);
        }
      }
      return;
    }

    const elapsed = Date.now() - start;
    for (let i = 0; i < grp.length; i++) {
      const item = grp[i]!;
      const r = results[i]!;
      if (r.ok) {
        this.recordSuccess(clientId, item.method, elapsed);
        item.def.resolve(r.value);
      } else {
        const kind = this.classifyFor(clientId, item.method, r.error, elapsed);
        this.recordError(clientId, item.method, kind, elapsed);
        item.excluded.add(clientId);
        if (isRetryable(kind)) {
          this.maybeRetry(item, r.error);
        } else {
          item.def.reject(r.error);
        }
      }
    }
  }

  private maybeRetry(item: BatchItem, lastError: Error): void {
    item.attempt++;
    if (item.attempt >= this.maxAttemptsFor(item.opts)) {
      item.def.reject(lastError);
      return;
    }
    // Retries take the direct (single-shot) path. Re-batching them across the
    // next microtask boundary buys little and complicates ordering guarantees.
    // Seed `runAttempts` with the original error so a final
    // NoClientAvailableError surfaces the actual cause.
    this.runAttempts(
      item.method,
      item.params,
      item.excluded,
      this.maxAttemptsFor(item.opts),
      item.attempt,
      item.opts,
      lastError,
    ).then(
      (v) => item.def.resolve(v),
      (e) => item.def.reject(e),
    );
  }

  private buildCandidates(): ClientView[] {
    const now = Date.now();
    const out: ClientView[] = [];
    for (const [id, client] of this.clients) {
      const meta = this.meta.get(id);
      if (!meta) continue;
      let state = client.getState();
      if (meta.bannedUntil !== undefined && meta.bannedUntil > now && state === 'connected') {
        state = 'banned';
      }
      const telemetry: Telemetry = {
        latency: meta.telemetry.latency(),
        errors: meta.telemetry.errorsSnapshot(),
        success: meta.telemetry.successSnapshot(),
        inFlight: client.inFlightCount,
        ...(client.connectedSince !== undefined ? { connectedSince: client.connectedSince } : {}),
      };
      const view: ClientView = {
        id,
        endpoint: client.endpoint,
        state,
        capabilities: { ...meta.capabilities },
        telemetry,
        ...(meta.bannedUntil !== undefined ? { bannedUntil: meta.bannedUntil } : {}),
      };
      out.push(view);
    }
    return out;
  }

  private classifyFor(
    clientId: ClientId,
    method: string,
    e: unknown,
    durationMs: number,
  ): ErrorKind {
    const sw = this.meta.get(clientId)?.capabilities.serverSoftware;
    return this.classifier.classify(e, {
      method,
      durationMs,
      ...(sw !== undefined ? { serverSoftware: sw } : {}),
    });
  }

  private recordSuccess(id: ClientId, method: string, latencyMs: number): void {
    const m = this.meta.get(id);
    if (!m) return;
    m.telemetry.recordSuccess(latencyMs, Date.now());
    this.invokeOnOutcome({ kind: 'success', clientId: id, method, latencyMs });
  }

  private recordError(id: ClientId, method: string, kind: ErrorKind, latencyMs: number): void {
    const m = this.meta.get(id);
    if (!m) return;
    m.telemetry.recordError(kind, latencyMs, Date.now());
    if (kind === 'rate-limit') {
      const now = Date.now();
      // Coalesce: only the LEADING edge of a ban window emits an event.
      // Without this, a burst of N parallel calls that all return a
      // rate-limit RPC error would emit N `client-banned` events and
      // ratchet `bannedUntil` forward by `cooldownMs` per error,
      // potentially extending the ban indefinitely while late-arriving
      // responses keep classifying. Once a client is banned within the
      // current cooldown window, further rate-limit errors only
      // contribute to telemetry / `onOutcome` — not a re-ban.
      if (m.bannedUntil === undefined || m.bannedUntil <= now) {
        m.bannedUntil = now + this.cooldownMs;
        this.emit('client-banned', {
          clientId: id,
          until: m.bannedUntil,
          reason: kind,
        });
      }
    }
    this.invokeOnOutcome({ kind: 'error', clientId: id, method, error: kind, latencyMs });
  }

  private invokeOnOutcome(o: Parameters<NonNullable<RoutingPolicy['onOutcome']>>[0]): void {
    const fn = this.policy.onOutcome;
    if (!fn) return;
    try {
      fn(o);
    } catch (e) {
      // Buggy user policy must not corrupt the hot path.
      this.emit('error', e);
    }
  }

  private emit<K extends keyof ManagerEvents>(event: K, payload: ManagerEvents[K]): void {
    const set = this.listeners.get(event);
    if (!set) return;
    for (const l of set) {
      try {
        l(payload);
      } catch {
        // listener errors swallowed; observability hook only.
      }
    }
  }

  private maxAttemptsFor(opts: CallOpts | undefined): number {
    const r = opts?.retry ?? 'auto';
    if (r === 'none') return 1;
    if (r === 'auto') return Math.max(1, this.clients.size);
    return Math.max(1, r.maxAttempts);
  }
}

function isRetryable(kind: ErrorKind): boolean {
  // Server-availability-class errors get a re-pick. Caller-owned errors
  // (rpc-error, protocol) are bubbled — the request itself is wrong.
  return kind === 'transport' || kind === 'timeout' || kind === 'rate-limit';
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Best-effort extraction of an AbortSignal's `reason`. Falls back to an
 * `Error` whose `name === 'AbortError'` so callers writing the idiomatic
 * `if (err.name === 'AbortError')` check still hit on older runtimes
 * that don't populate `signal.reason`.
 */
function signalAbortReason(signal: AbortSignal): unknown {
  const reason: unknown = (signal as { reason?: unknown }).reason;
  if (reason !== undefined) return reason;
  const e = new Error('aborted');
  e.name = 'AbortError';
  return e;
}

/**
 * Map a server-pushed notification's wire shape onto the (subParams, status)
 * pair that SubscriptionRegistry indexes by.
 *
 * Wire shapes are method-specific:
 *
 *  - `blockchain.scripthash.subscribe` notification: `params = [scripthash, status]`
 *    → registry key params = [scripthash], status payload = the second element.
 *  - `blockchain.headers.subscribe` notification: `params = [BlockHeader]`
 *    → registry key params = [], status payload = the first element.
 *
 * For unrecognized notification methods we fall through to `[]` key + first-
 * element status; nothing matches in the registry so the notification is
 * silently dropped (intentional — only methods we know how to subscribe to
 * have records).
 */
function decodeNotification(
  method: string,
  params: readonly unknown[],
): { subParams: readonly unknown[]; status: unknown } {
  switch (method) {
    case 'blockchain.scripthash.subscribe':
      return { subParams: [params[0]], status: params[1] };
    case 'blockchain.headers.subscribe':
      return { subParams: [], status: params[0] };
    default:
      return { subParams: [], status: params[0] };
  }
}
