// ElectrumManager — orchestrates a pool of ElectrumClient instances behind a
// RoutingPolicy. Single entry point for callers; transparent failover, partial-
// batch retry, microtask auto-batch coalescing, per-client telemetry, typed
// method registry + namespace API (M3). Subscriptions + classifier + peer
// discovery (M4), cache (also M4 — needs the headers subscription to track
// finality), lifecycle suspend/resume (M5), and TCP/TLS transports (M6) plug
// in here in subsequent milestones.

import type { ClientId, ClientView, ConnectionState, Endpoint, Telemetry } from './client.js';
import { ElectrumClient } from './client.js';
import { defaultClassifier } from './errors/classifier.js';
import {
  NoClientAvailableError,
  ProtocolError,
  type ErrorClassifier,
  type ErrorKind,
} from './errors/types.js';
import type { PickContext, RoutingPolicy } from './policy/types.js';
import type { MethodName, ParamsOf, ResultOf } from './protocol/methods.js';
import type {
  BatchRequest,
  CallOpts,
  ManagerOptions,
  Network,
  RawTxHex,
  Scripthash,
  ServerSpec,
  TxId,
  TxVerbose,
} from './protocol/types.js';
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
    this.batcher = new MicrotaskBatcher<BatchItem>((items) => {
      this.flushBatch(items).catch((e) => this.emit('error', e));
    });
    for (const spec of opts.servers) {
      this.installServer(spec);
    }
  }

  /** Connect every server in parallel. Errors do not throw; they fire `error` events. */
  async start(): Promise<void> {
    const tasks = [...this.clients.values()].map(async (c) => {
      try {
        await c.connect();
      } catch (e) {
        this.emit('error', e);
      }
    });
    await Promise.all(tasks);
  }

  /** Disconnect every server. */
  async stop(): Promise<void> {
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
    const useBatch = opts?.autoBatch ?? this.autoBatchEnabled;
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
    return this.runAttempts(method, params, new Set<ClientId>(), this.maxAttemptsFor(opts), 0);
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
    subscribe: (hash: Scripthash, opts?: CallOpts) =>
      this.call('blockchain.scripthash.subscribe', [hash], opts),
    unsubscribe: (hash: Scripthash, opts?: CallOpts) =>
      this.call('blockchain.scripthash.unsubscribe', [hash], opts),
  };

  readonly transaction = {
    get: (txid: TxId, opts?: CallOpts) => this.call('blockchain.transaction.get', [txid], opts),
    /**
     * Verbose form of `blockchain.transaction.get` — server-decoded tx shape.
     *
     * Routed deliberately via the unknown-method overload because the wire
     * method has *two* legitimate response shapes (`RawTxHex` for `[txid]`,
     * `TxVerbose` for `[txid, true]`) keyed on the second param. Encoding
     * that as a registry param-union would force every plain
     * `manager.call('blockchain.transaction.get', [txid])` caller to narrow
     * `string | TxVerbose`, defeating the whole point of typed `call`. The
     * `verboseMethod: string` cast is the price; the `as TxVerbose` is
     * unchecked at runtime (same caveat as every other shape we type — the
     * post-M4 decoder pass will validate it).
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
     * Fetches the current tip and registers a wire-level subscription with
     * the chosen server. Returns the initial header. Notifications fired by
     * the server after subscription are silently dropped until M4 wires the
     * SubscriptionRegistry with handler routing — at which point this method
     * is replaced by `subscribe(handler)` and a separate `getTip()` shorthand.
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
      const outcome = await this.attemptOnce(method, params, excluded, attempt);
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
  ): Promise<AttemptOutcome> {
    const ctx: PickContext = {
      request: { method, params },
      attempt,
      excluded,
      candidates: this.buildCandidates(),
      now: Date.now(),
    };
    const clientId = this.policy.pick(ctx);
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

function defaultTransportFactory(endpoint: Endpoint): Transport {
  return new WsTransport({ endpoint });
}
