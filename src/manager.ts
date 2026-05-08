// ElectrumManager — orchestrates a pool of ElectrumClient instances behind a
// RoutingPolicy. Single entry point for callers; transparent failover, partial-
// batch retry, microtask auto-batch coalescing, per-client telemetry. M2 ships
// the orchestration core; subscriptions (M4), cache (M3), lifecycle suspend/
// resume (M5), and TCP/TLS transports (M6) plug in here in subsequent
// milestones.

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
import type {
  BatchRequest,
  CallOpts,
  ManagerOptions,
  Network,
  ServerSpec,
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
  // Variance on a discriminated mapped type doesn't carry through `[K]`
  // lookups in strict TS, so we store opaquely and cast at the API edge.
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
      void this.flushBatch(items);
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
    this.clients.delete(id);
    this.meta.delete(id);
    try {
      await c.disconnect();
    } catch (e) {
      this.emit('error', e);
    }
  }

  /** Direct or auto-batched single call. */
  async call<T = unknown>(
    method: string,
    params: readonly unknown[] = [],
    opts?: CallOpts,
  ): Promise<T> {
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
      return def.promise as Promise<T>;
    }
    return this.routeSingle(method, params, opts) as Promise<T>;
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
          const v = await this.call<T>(r.method, r.params, opts);
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

  private async routeSingle(
    method: string,
    params: readonly unknown[],
    opts: CallOpts | undefined,
  ): Promise<unknown> {
    const maxAttempts = this.maxAttemptsFor(opts);
    const excluded = new Set<ClientId>();
    let lastErr: Error | undefined;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const ctx: PickContext = {
        request: { method, params },
        attempt,
        excluded,
        candidates: this.buildCandidates(),
        now: Date.now(),
      };
      const clientId = this.policy.pick(ctx);
      if (clientId === null) {
        throw lastErr ?? new NoClientAvailableError(`no eligible client for ${method}`);
      }
      const client = this.clients.get(clientId);
      if (!client) {
        excluded.add(clientId);
        continue;
      }
      const start = Date.now();
      try {
        const value = await client.call(method, params);
        this.recordSuccess(clientId, method, Date.now() - start);
        return value;
      } catch (e) {
        const elapsed = Date.now() - start;
        const kind = this.classifier.classify(e, {
          ...(this.meta.get(clientId)?.capabilities.serverSoftware !== undefined
            ? { serverSoftware: this.meta.get(clientId)!.capabilities.serverSoftware! }
            : {}),
          method,
          durationMs: elapsed,
        });
        this.recordError(clientId, method, kind, elapsed);
        excluded.add(clientId);
        lastErr = e as Error;
        if (!isRetryable(kind)) throw e;
      }
    }

    throw lastErr ?? new NoClientAvailableError('exhausted retry budget');
  }

  private async flushBatch(items: BatchItem[]): Promise<void> {
    // Per-item picks at flush time; group same-client items into one batch.
    const groups = new Map<ClientId, BatchItem[]>();
    const unroutable: BatchItem[] = [];

    for (const item of items) {
      const ctx: PickContext = {
        request: { method: item.method, params: item.params },
        attempt: item.attempt,
        excluded: item.excluded,
        candidates: this.buildCandidates(),
        now: Date.now(),
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
      for (const item of grp) {
        item.excluded.add(clientId);
        this.maybeRetry(item, new NoClientAvailableError(`client ${clientId} disappeared`));
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
      const kind = this.classifier.classify(e, {
        ...(this.meta.get(clientId)?.capabilities.serverSoftware !== undefined
          ? { serverSoftware: this.meta.get(clientId)!.capabilities.serverSoftware! }
          : {}),
        method: '<batch>',
        durationMs: elapsed,
      });
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
        const kind = this.classifier.classify(r.error, {
          ...(this.meta.get(clientId)?.capabilities.serverSoftware !== undefined
            ? { serverSoftware: this.meta.get(clientId)!.capabilities.serverSoftware! }
            : {}),
          method: item.method,
          durationMs: elapsed,
        });
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
    this.routeFromItem(item).then(
      (v) => item.def.resolve(v),
      (e) => item.def.reject(e),
    );
  }

  private async routeFromItem(item: BatchItem): Promise<unknown> {
    const maxAttempts = this.maxAttemptsFor(item.opts);
    let lastErr: Error | undefined;
    while (item.attempt < maxAttempts) {
      const ctx: PickContext = {
        request: { method: item.method, params: item.params },
        attempt: item.attempt,
        excluded: item.excluded,
        candidates: this.buildCandidates(),
        now: Date.now(),
      };
      const clientId = this.policy.pick(ctx);
      if (clientId === null) {
        throw lastErr ?? new NoClientAvailableError(`no eligible client for ${item.method}`);
      }
      const client = this.clients.get(clientId);
      if (!client) {
        item.excluded.add(clientId);
        item.attempt++;
        continue;
      }
      const start = Date.now();
      try {
        const value = await client.call(item.method, item.params);
        this.recordSuccess(clientId, item.method, Date.now() - start);
        return value;
      } catch (e) {
        const elapsed = Date.now() - start;
        const kind = this.classifier.classify(e, {
          ...(this.meta.get(clientId)?.capabilities.serverSoftware !== undefined
            ? { serverSoftware: this.meta.get(clientId)!.capabilities.serverSoftware! }
            : {}),
          method: item.method,
          durationMs: elapsed,
        });
        this.recordError(clientId, item.method, kind, elapsed);
        item.excluded.add(clientId);
        item.attempt++;
        lastErr = e as Error;
        if (!isRetryable(kind)) throw e;
      }
    }
    throw lastErr ?? new NoClientAvailableError('exhausted retry budget');
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

  private recordSuccess(id: ClientId, method: string, latencyMs: number): void {
    const m = this.meta.get(id);
    if (!m) return;
    m.telemetry.recordSuccess(latencyMs, Date.now());
    this.policy.onOutcome?.({ kind: 'success', clientId: id, method, latencyMs });
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
    this.policy.onOutcome?.({ kind: 'error', clientId: id, method, error: kind, latencyMs });
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
