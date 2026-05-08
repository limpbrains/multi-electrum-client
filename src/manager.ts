// ElectrumManager — orchestrates a pool of ElectrumClient instances behind a
// RoutingPolicy. Single entry point for callers; transparent failover, partial-
// batch retry, microtask auto-batch coalescing, per-client telemetry, typed
// method registry + namespace API (M3). Subscriptions + classifier + peer
// discovery (M4), cache (also M4 — needs the headers subscription to track
// finality), lifecycle suspend/resume (M5), and TCP/TLS transports (M6) plug
// in here in subsequent milestones.

import { buildKey, cacheSpec } from './cache/keys.js';
import type { CacheStore } from './cache/types.js';
import type { ClientId, ClientView, ConnectionState, Endpoint, Telemetry } from './client.js';
import { ElectrumClient } from './client.js';
import { DEFAULT_DISCOVER_INTERVAL_MS, parsePeerList, type DiscoverOptions } from './discovery.js';
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
import { SubscriptionRegistry } from './subscriptions/registry.js';
import type { SubscriptionHandler } from './subscriptions/types.js';
import type { Transport } from './transport/types.js';
import { WsTransport } from './transport/ws.js';
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
  private readonly discoverOptions: DiscoverOptions | undefined;
  /** Per-client re-poll timers for peer discovery. */
  private readonly discoverTimers = new Map<ClientId, ReturnType<typeof setTimeout>>();
  /** True while we're tearing down — guards async tasks against post-stop work. */
  private stopped = false;
  /**
   * Lifecycle state. `created → running → suspending → suspended →
   * resuming → running → stopped`. See lifecycle/types.ts.
   */
  private lifecycle: LifecycleState = 'created';
  /**
   * Tagged in-flight transition. Same-kind concurrent callers share the
   * promise; opposite-kind callers chain after the current transition
   * completes (so `m.resume()` issued during a `m.suspend()` actually
   * runs the resume after the suspend lands, rather than returning the
   * suspend's promise verbatim).
   */
  private inFlightTransition: { kind: 'suspend' | 'resume'; promise: Promise<void> } | null = null;
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
    this.discoverOptions = opts.discover;
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

  /** Disconnect every server and drop all subscriptions. Terminal. */
  async stop(): Promise<void> {
    // If a suspend / resume transition is in flight, let it observe the
    // stopped flag and unwind cleanly before we rip down the rest of the
    // manager. Without this, runSuspend's tail (e.g. `await tipUnsub()`)
    // could still fire after `await m.stop()` returns to the caller —
    // observable to anyone listening on the `error` event.
    this.stopped = true;
    this.lifecycle = 'stopped';
    const pending = this.inFlightTransition?.promise;
    if (pending) {
      try {
        await pending;
      } catch {
        // Swallow — the transition's own race-checks short-circuit on
        // stopped; any rejection is purely informational and already
        // surfaced via the manager `error` event.
      }
    }
    // Reject anything queued during a prior suspend so callers don't dangle.
    while (this.suspendQueue.length > 0) {
      const item = this.suspendQueue.shift()!;
      item.def.reject(new SuspendedError('manager stopped before resume'));
    }
    for (const t of this.discoverTimers.values()) clearTimeout(t);
    this.discoverTimers.clear();
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
    // Same-kind re-entry: share the in-flight promise so `await` reflects
    // the actual completion, not the intermediate state.
    if (this.inFlightTransition?.kind === 'suspend') {
      return this.inFlightTransition.promise;
    }
    // Opposite-kind re-entry: chain after the running resume completes,
    // then run our own suspend. Without this, calling suspend() during
    // resume() would observe the resume's promise and return as if the
    // suspend had run — leaving the manager 'running' instead of
    // 'suspended'.
    if (this.inFlightTransition) {
      const prev = this.inFlightTransition.promise;
      return prev.then(
        () => this.suspend(opts),
        () => this.suspend(opts),
      );
    }
    if (this.lifecycle === 'suspended') return;
    if (this.lifecycle === 'stopped') {
      throw new SuspendedError('cannot suspend a stopped manager');
    }
    // `created` is reachable when bindAppState fires before start() — app
    // launches in the background and the AppState listener races ahead of
    // the user's start() call. No sockets / no in-flight to drain; just
    // record the state flip so resume() works as expected.
    if (this.lifecycle === 'created') {
      this.lifecycle = 'suspended';
      return;
    }
    const task = this.runSuspend(opts);
    this.inFlightTransition = { kind: 'suspend', promise: task };
    try {
      await task;
    } finally {
      this.inFlightTransition = null;
    }
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
    for (const t of this.discoverTimers.values()) clearTimeout(t);
    this.discoverTimers.clear();
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
    if (this.inFlightTransition?.kind === 'resume') {
      return this.inFlightTransition.promise;
    }
    if (this.inFlightTransition) {
      const prev = this.inFlightTransition.promise;
      return prev.then(
        () => this.resume(),
        () => this.resume(),
      );
    }
    if (this.lifecycle === 'running') return;
    if (this.lifecycle === 'stopped') {
      throw new SuspendedError('cannot resume a stopped manager');
    }
    if (this.lifecycle !== 'suspended') {
      throw new SuspendedError(
        this.lifecycle === 'created'
          ? 'cannot resume from created — use start() on a fresh manager'
          : `cannot resume from ${this.lifecycle}`,
      );
    }
    const task = this.runResume();
    this.inFlightTransition = { kind: 'resume', promise: task };
    try {
      await task;
    } finally {
      this.inFlightTransition = null;
    }
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

  addServer(spec: ServerSpec): void {
    this.installServer(spec);
    const client = this.clients.get(spec.id);
    if (!client) return;
    client.connect().catch((e) => this.emit('error', e));
  }

  async removeServer(id: ClientId): Promise<void> {
    const c = this.clients.get(id);
    if (!c) return;
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
    // touching the wire. Cache writes happen after a successful wire call
    // (see `runAttempts` resolution + `dispatchGroup`). `bypassCache` opts
    // out of both the read and the write.
    const spec = opts?.bypassCache ? null : this.cacheSpecFor(method, params);
    if (spec && this.cache) {
      const hit = await this.readCache(spec);
      if (hit !== undefined) return hit;
    }

    const value = await this.callInner(method, params, opts);
    if (spec && this.cache && this.isFinalized(spec.finalityHeight) && !opts?.bypassCache) {
      // Fire-and-forget: a slow cache adapter must not block the caller.
      // A failed cache write just means the next call refetches.
      void this.writeCache(spec, value).catch((e) => this.emit('error', e));
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
  };

  readonly headers = {
    /**
     * Subscribe to new chain tips. Handler fires immediately with the
     * current tip and on every header notification. Returns `Unsubscribe`
     * (last handler removed → manager stops dispatching; the wire
     * `blockchain.headers.subscribe` has no paired unsubscribe so the
     * server keeps pushing for the session — documented quirk).
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
        // Fire-and-forget: rebind any orphaned subs onto the new connection.
        // Errors surface through the manager `error` event via runAttempts.
        this.registry.restoreOrphans().catch((e) => this.emit('error', e));
        // Kick off a peer-discovery probe on this fresh connection. Idempotent:
        // already-known peers are skipped without consulting onDiscover.
        if (this.discoverOptions?.enabled) {
          this.discoverFromClient(spec.id).catch((e) => this.emit('error', e));
        }
      } else if (state === 'disconnected') {
        this.registry.clientDisconnected(spec.id);
        // Subs bound to this client are now orphaned — immediately try to
        // re-bind them onto any other already-connected client without
        // waiting for the next state transition.
        this.registry.restoreOrphans().catch((e) => this.emit('error', e));
        // Cancel any scheduled re-poll: it would fire against a dead
        // client and route via policy.pick to a different server, which
        // is fine but wasteful. The next `connected` re-installs it.
        const t = this.discoverTimers.get(spec.id);
        if (t !== undefined) {
          clearTimeout(t);
          this.discoverTimers.delete(spec.id);
        }
      }
    });
    this.clients.set(spec.id, client);
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

  // --- Cache helpers -----------------------------------------------------

  /** Public wrapper kept stable so the cache module remains side-effect-free. */
  private cacheSpecFor(method: string, params: readonly unknown[]): ReturnType<typeof cacheSpec> {
    if (!this.cache) return null;
    return cacheSpec(method, params);
  }

  /** True iff the cache has been populated past `finalizedConfs` for this height. */
  private isFinalized(height: number): boolean {
    if (this.tipHeight === undefined) return false;
    return this.tipHeight - height >= this.finalizedConfs;
  }

  private async readCache(
    spec: NonNullable<ReturnType<typeof cacheSpec>>,
  ): Promise<unknown | undefined> {
    if (!this.cache) return undefined;
    const key = buildKey(this.network, spec.bucket, spec.id);
    let raw: string | null;
    try {
      raw = await this.cache.get(key);
    } catch (e) {
      // Cache failure must not break the request — caller injected the
      // store, manager just falls back to a wire call. Surface for visibility.
      this.emit('error', e);
      return undefined;
    }
    if (raw === null) return undefined;
    try {
      return JSON.parse(raw);
    } catch (e) {
      // Corrupted entry. Remove it best-effort and miss the read.
      this.emit('error', e);
      void this.cache.del(key).catch(() => undefined);
      return undefined;
    }
  }

  private async writeCache(
    spec: NonNullable<ReturnType<typeof cacheSpec>>,
    value: unknown,
  ): Promise<void> {
    if (!this.cache) return;
    const key = buildKey(this.network, spec.bucket, spec.id);
    let serialized: string;
    try {
      serialized = JSON.stringify(value);
    } catch (e) {
      // Non-serializable response (circular ref, BigInt) — skip the
      // write rather than throwing out of the post-success path.
      this.emit('error', e);
      return;
    }
    await this.cache.set(key, serialized);
  }

  // --- Peer discovery ----------------------------------------------------

  /**
   * Probe a freshly-connected client for `server.peers.subscribe`. Parses
   * the response, runs each candidate through `onDiscover`, and admits via
   * `addServer`. Servers that don't support peer discovery typically emit
   * an RPC error which we swallow — discovery is best-effort.
   *
   * Schedules the next re-poll on success (and only on success — a
   * permanently-failing server doesn't waste timer slots forever).
   */
  private async discoverFromClient(clientId: ClientId): Promise<void> {
    const opts = this.discoverOptions;
    if (!opts?.enabled) return;
    if (this.stopped) return;

    let response: unknown;
    try {
      response = await this.callDirect('server.peers.subscribe', [], {
        preferClient: clientId,
        retry: 'none',
      });
    } catch {
      // Server doesn't support peer discovery, or transient failure — drop
      // silently. Manager `error` event already surfaced the underlying
      // failure via runAttempts if it's something the caller cares about.
      return;
    }

    const candidates = parsePeerList(response);
    for (const cand of candidates) {
      // Dedup against the existing pool (id is `host:port`).
      if (this.clients.has(cand.id)) continue;
      let admit: boolean;
      if (opts.onDiscover) {
        try {
          admit = await opts.onDiscover(cand);
        } catch (e) {
          this.emit('error', e);
          continue;
        }
      } else {
        admit = true;
      }
      if (!admit) continue;
      // Re-check after the await: the pool may have grown / shrunk while
      // the user's callback ran (their callback might also have called
      // addServer / removeServer).
      if (this.stopped) return;
      if (this.clients.has(cand.id)) continue;
      try {
        this.addServer(cand);
      } catch (e) {
        // Likely a duplicate id race; surface and move on.
        this.emit('error', e);
      }
    }

    const interval = opts.intervalMs ?? DEFAULT_DISCOVER_INTERVAL_MS;
    if (interval <= 0) return;
    if (this.stopped) return;
    // Replace any prior timer (defensive; onStateChange already cleared on
    // disconnect, but a manual addServer-during-poll race is possible).
    const prev = this.discoverTimers.get(clientId);
    if (prev !== undefined) clearTimeout(prev);
    const t = setTimeout(() => {
      this.discoverTimers.delete(clientId);
      this.discoverFromClient(clientId).catch((e) => this.emit('error', e));
    }, interval);
    // Don't keep the Node event loop alive on this timer alone.
    if (typeof t === 'object' && t !== null && 'unref' in t) {
      (t as { unref: () => void }).unref();
    }
    this.discoverTimers.set(clientId, t);
  }

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
      m.bannedUntil = Date.now() + this.cooldownMs;
      this.emit('client-banned', {
        clientId: id,
        until: m.bannedUntil,
        reason: kind,
      });
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

function defaultTransportFactory(endpoint: Endpoint): Transport {
  return new WsTransport({ endpoint });
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
