// ElectrumClient — single connection to one Electrum server.
//
// Drives one Transport, owns the in-flight JSON-RPC request map, allocates
// request ids, dispatches responses back to caller promises, and routes
// server-initiated notifications to a single subscribed listener (the
// SubscriptionRegistry, in M4).
//
// Telemetry (latency / success / error / ban / classified-kind) lives on the
// Manager — Client only exposes raw inFlight count and connectedSince so the
// Manager can compose a ClientView snapshot for the RoutingPolicy.

import type { ErrorKind } from './errors/types.js';
import { ProtocolError, RpcError, TimeoutError, TransportError } from './errors/types.js';
import {
  decodeMessage,
  encodeBatch,
  encodeRequest,
  type JsonRpcId,
  type JsonRpcMessage,
  type JsonRpcNotification,
  type JsonRpcRequest,
} from './protocol/framing.js';
import type { Transport, TransportEvent } from './transport/types.js';
import { deferred, type Deferred } from './util/deferred.js';
import { err, ok, type Result } from './util/result.js';

export type ClientId = string;

export type Protocol = 'ws' | 'wss' | 'tcp' | 'tls';

export interface Endpoint {
  host: string;
  port: number;
  protocol: Protocol;
  /** Optional URL path for WebSocket endpoints, e.g. `/ws` or `/electrum`. */
  path?: string;
}

export type ConnectionState = 'connecting' | 'connected' | 'disconnected' | 'banned';

export interface Telemetry {
  latency: { ema: number; p50: number; p95: number; samples: number };
  errors: {
    rate: number;
    lastKind?: ErrorKind;
    lastAt?: number;
    consecutive: number;
  };
  success: { count: number; lastAt?: number };
  inFlight: number;
  connectedSince?: number;
}

/** Read-only snapshot of a Client's state, given to RoutingPolicy. */
export interface ClientView {
  id: ClientId;
  endpoint: Endpoint;
  state: ConnectionState;
  bannedUntil?: number;
  capabilities: { serverSoftware?: string; protocolVersion?: string };
  telemetry: Telemetry;
}

export interface ElectrumClientOpts {
  id: ClientId;
  endpoint: Endpoint;
  transport: Transport;
  /** Per-call timeout, default 10_000ms. */
  requestTimeoutMs?: number;
}

export interface BatchCallItem {
  method: string;
  params: readonly unknown[];
}

interface InFlight {
  def: Deferred<unknown>;
  method: string;
  timer: ReturnType<typeof setTimeout>;
}

export class ElectrumClient {
  readonly id: ClientId;
  readonly endpoint: Endpoint;
  private readonly transport: Transport;
  private readonly requestTimeoutMs: number;
  private readonly inFlight = new Map<JsonRpcId, InFlight>();
  /**
   * Id-sets of wire batches currently awaiting responses, in send order.
   * Some servers (Fulcrum: `"Batch limit exceeded"`, code 4) reject an
   * oversized batch with a SINGLE error object whose `id` is `null`
   * instead of a response array — per the JSON-RPC 2.0 spec for an
   * invalid batch. Without this bookkeeping that reply matches no
   * in-flight id and every item would hang until its timeout.
   */
  private readonly openBatches: JsonRpcId[][] = [];
  private nextId = 1;
  private state: ConnectionState = 'disconnected';
  private connectedAt: number | undefined;
  private notifListener: ((n: JsonRpcNotification) => void) | undefined;
  private stateListener: ((state: ConnectionState) => void) | undefined;
  private protocolErrorListener: ((e: ProtocolError) => void) | undefined;
  private detachTransport: (() => void) | undefined;

  constructor(opts: ElectrumClientOpts) {
    this.id = opts.id;
    this.endpoint = opts.endpoint;
    this.transport = opts.transport;
    this.requestTimeoutMs = opts.requestTimeoutMs ?? 10_000;
  }

  getState(): ConnectionState {
    return this.state;
  }

  /** Number of currently outstanding JSON-RPC requests. */
  get inFlightCount(): number {
    return this.inFlight.size;
  }

  /** Monotonic ms timestamp of the most recent successful connect. */
  get connectedSince(): number | undefined {
    return this.connectedAt;
  }

  async connect(): Promise<void> {
    if (this.state === 'connected') return;
    if (this.state === 'connecting') {
      throw new TransportError('connect already in progress');
    }
    this.setState('connecting');
    this.detachTransport = this.transport.on((ev) => this.handle(ev));
    try {
      await this.transport.connect();
      this.connectedAt = Date.now();
      this.setState('connected');
    } catch (e) {
      this.detach();
      this.setState('disconnected');
      throw e;
    }
  }

  async disconnect(): Promise<void> {
    this.connectedAt = undefined;
    this.detach();
    this.failInFlight(new TransportError('disconnected by client'));
    this.setState('disconnected');
    await this.transport.close();
  }

  /** Unbind from the transport; never leaves a stale detach fn behind. */
  private detach(): void {
    this.detachTransport?.();
    this.detachTransport = undefined;
  }

  async call<T = unknown>(method: string, params: readonly unknown[] = []): Promise<T> {
    if (this.state !== 'connected') {
      throw new TransportError(`cannot call ${method}: state is ${this.state}`);
    }
    const { id, def, req } = this.openRequest(method, params);
    await this.sendOrCancel(encodeRequest(req), [id]);
    return def.promise as Promise<T>;
  }

  /**
   * Send N requests as a single JSON-RPC batch and resolve with one Result per
   * item, in the original order. Per-item failures (including timeouts) are
   * surfaced as `{ ok: false, error }`. A whole-batch failure (transport send
   * error, connection drop) rejects the outer promise.
   */
  async batchCall(reqs: readonly BatchCallItem[]): Promise<Array<Result<unknown>>> {
    if (this.state !== 'connected') {
      throw new TransportError(`cannot batchCall: state is ${this.state}`);
    }
    if (reqs.length === 0) return [];

    const ids: JsonRpcId[] = [];
    const promises: Promise<Result<unknown>>[] = [];
    const jsonReqs: JsonRpcRequest[] = [];

    for (const r of reqs) {
      const { id, def, req } = this.openRequest(r.method, r.params);
      ids.push(id);
      jsonReqs.push(req);
      promises.push(def.promise.then(ok, err));
    }

    await this.sendOrCancel(encodeBatch(jsonReqs), ids);

    // Track the batch so a batch-level `id: null` error reply can be
    // mapped back to these items (see `dispatch`). Entry is dropped once
    // every item has settled, however it settled.
    this.openBatches.push(ids);
    const settled = Promise.all(promises);
    void settled.then(() => {
      const i = this.openBatches.indexOf(ids);
      if (i >= 0) this.openBatches.splice(i, 1);
    });
    return settled;
  }

  /** Set the listener for server-initiated notifications (one per client). */
  onNotification(listener: (n: JsonRpcNotification) => void): void {
    this.notifListener = listener;
  }

  /**
   * Set the listener for malformed inbound frames (one per client). Fired
   * when a line from the server fails JSON-RPC decoding. The connection
   * stays up — framing is line-delimited, so the next line usually parses
   * fine; the Manager uses this to record a `protocol` error against the
   * client's telemetry so routing policies see the real cause instead of
   * the eventual request timeout.
   */
  onProtocolError(listener: (e: ProtocolError) => void): void {
    this.protocolErrorListener = listener;
  }

  /**
   * Set a listener fired on every state transition. Used by Manager to drive
   * SubscriptionRegistry rebinds when a client connects / disconnects.
   * Replaces the previous listener if called twice.
   */
  onStateChange(listener: (state: ConnectionState) => void): void {
    this.stateListener = listener;
  }

  private setState(s: ConnectionState): void {
    if (this.state === s) return;
    this.state = s;
    try {
      this.stateListener?.(s);
    } catch {
      // Listener errors are observable through Manager's `error` event, but
      // for clients we swallow to keep state-machine progress unblocked.
    }
  }

  /** Allocate an id, register the in-flight entry, and build the wire request. */
  private openRequest(
    method: string,
    params: readonly unknown[],
  ): { id: JsonRpcId; def: Deferred<unknown>; req: JsonRpcRequest } {
    const id = this.nextId++;
    const def = deferred<unknown>();
    this.registerInFlight(id, def, method);
    return { id, def, req: { jsonrpc: '2.0', method, params, id } };
  }

  /** Send, unwinding every listed in-flight entry if the transport throws. */
  private async sendOrCancel(text: string, ids: readonly JsonRpcId[]): Promise<void> {
    try {
      await this.transport.send(text);
    } catch (e) {
      for (const id of ids) this.takeInFlight(id);
      throw e;
    }
  }

  private registerInFlight(id: JsonRpcId, def: Deferred<unknown>, method: string): void {
    const timer = setTimeout(() => {
      const inflight = this.takeInFlight(id);
      inflight?.def.reject(
        new TimeoutError(`${method} timed out after ${this.requestTimeoutMs}ms`),
      );
    }, this.requestTimeoutMs);
    this.inFlight.set(id, { def, method, timer });
  }

  /**
   * Atomically remove an in-flight entry: the entry leaves the map exactly
   * once with its timer dead. Every settlement path goes through here.
   */
  private takeInFlight(id: JsonRpcId): InFlight | undefined {
    const inflight = this.inFlight.get(id);
    if (!inflight) return undefined;
    this.inFlight.delete(id);
    clearTimeout(inflight.timer);
    return inflight;
  }

  private handle(ev: TransportEvent): void {
    if (ev.type === 'close') {
      this.connectedAt = undefined;
      // Detach from the transport now: the next connect() attaches a fresh
      // listener, and leaving this one bound would accumulate one duplicate
      // handler per reconnect cycle (notifications dispatched N times).
      this.detachTransport?.();
      this.detachTransport = undefined;
      const reason =
        ev.code !== undefined
          ? `socket closed (code=${ev.code}${ev.reason ? `, reason=${ev.reason}` : ''})`
          : 'socket closed';
      this.failInFlight(new TransportError(reason));
      this.setState('disconnected');
      return;
    }
    if (ev.type === 'error') {
      // Surface errors via the close path; raw 'error' events alone don't fail
      // pending requests — the WS spec follows error with close shortly after.
      return;
    }
    let msgs: JsonRpcMessage | JsonRpcMessage[];
    try {
      msgs = decodeMessage(ev.text);
    } catch (e) {
      // Malformed frame. Keep the connection alive — a misbehaving server
      // must not take the whole client down, and the next line usually
      // parses fine. Surface to the listener instead of dropping silently;
      // the in-flight request this frame answered (if any) falls back to
      // its per-request timeout.
      const protoErr = e instanceof ProtocolError ? e : new ProtocolError(String(e));
      try {
        this.protocolErrorListener?.(protoErr);
      } catch {
        // Listener errors must not break the read path.
      }
      return;
    }
    if (Array.isArray(msgs)) {
      for (const m of msgs) this.dispatch(m);
    } else {
      this.dispatch(msgs);
    }
  }

  private dispatch(msg: JsonRpcMessage): void {
    if ('method' in msg) {
      this.notifListener?.(msg);
      return;
    }
    if (msg.id === null && 'error' in msg) {
      // Batch-level rejection (e.g. Fulcrum's "Batch limit exceeded"):
      // the server answered a whole batch with one id-less error object.
      // Batches are answered in send order on a single connection, so
      // map it onto the oldest open batch. With no open batch there is
      // nothing to attribute it to — drop it (a lone id-less error can
      // also be a reply to garbage we never sent).
      this.failOldestOpenBatch(msg.error);
      return;
    }
    const inflight = this.takeInFlight(msg.id);
    if (!inflight) return; // unknown id (late response after timeout)
    if ('error' in msg) {
      inflight.def.reject(toRpcError(msg.error));
    } else {
      inflight.def.resolve(msg.result);
    }
  }

  /**
   * Reject every item of the oldest open batch with the given wire error.
   * Items already settled (timeout raced us) are skipped naturally — their
   * ids are gone from `inFlight`.
   */
  private failOldestOpenBatch(e: { code: number; message: string; data?: unknown }): void {
    const ids = this.openBatches.shift();
    if (!ids) return;
    const rpcError = toRpcError(e);
    for (const id of ids) {
      this.takeInFlight(id)?.def.reject(rpcError);
    }
  }

  /**
   * Reject every in-flight request with `e`, clear the timers, and drop
   * the table. Used by the manager's `suspend()` path so callers see a
   * `SuspendedError` rather than the `TransportError` that `disconnect`
   * would surface (their request didn't fail because the link died, it
   * failed because the manager went to sleep).
   *
   * Safe to call before `disconnect()`: the inFlight Map is cleared here,
   * so `disconnect`'s own `failAllInFlight(TransportError)` walks an
   * empty Map and produces no second rejection per request.
   */
  failInFlight(e: Error): void {
    for (const inflight of this.inFlight.values()) {
      clearTimeout(inflight.timer);
      inflight.def.reject(e);
    }
    this.inFlight.clear();
  }
}

/** Map a wire error object onto RpcError (the ctor drops undefined `data`). */
function toRpcError(e: { code: number; message: string; data?: unknown }): RpcError {
  return new RpcError(e.message, e.code, e.data);
}
