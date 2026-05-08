// ElectrumClient — single connection to one Electrum server.
//
// Drives one Transport, owns the in-flight JSON-RPC request map, allocates
// request ids, dispatches responses back to caller promises, and routes
// server-initiated notifications to a single subscribed listener (the
// SubscriptionRegistry, in M4).

import type { ErrorKind } from './errors/types.js';
import { RpcError, TimeoutError, TransportError } from './errors/types.js';
import {
  decodeMessage,
  encodeRequest,
  type JsonRpcId,
  type JsonRpcMessage,
  type JsonRpcNotification,
  type JsonRpcRequest,
} from './protocol/framing.js';
import type { Transport, TransportEvent } from './transport/types.js';
import { deferred, type Deferred } from './util/deferred.js';

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
  private nextId = 1;
  private state: ConnectionState = 'disconnected';
  private notifListener: ((n: JsonRpcNotification) => void) | undefined;
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

  async connect(): Promise<void> {
    if (this.state === 'connected') return;
    if (this.state === 'connecting') {
      throw new TransportError('connect already in progress');
    }
    this.state = 'connecting';
    this.detachTransport = this.transport.on((ev) => this.handle(ev));
    try {
      await this.transport.connect();
      this.state = 'connected';
    } catch (err) {
      this.state = 'disconnected';
      this.detachTransport?.();
      this.detachTransport = undefined;
      throw err;
    }
  }

  async disconnect(): Promise<void> {
    this.state = 'disconnected';
    this.detachTransport?.();
    this.detachTransport = undefined;
    this.failAllInFlight(new TransportError('disconnected by client'));
    await this.transport.close();
  }

  async call<T = unknown>(method: string, params: readonly unknown[] = []): Promise<T> {
    if (this.state !== 'connected') {
      throw new TransportError(`cannot call ${method}: state is ${this.state}`);
    }
    const id = this.nextId++;
    const req: JsonRpcRequest = { jsonrpc: '2.0', method, params, id };
    const text = encodeRequest(req);
    const def = deferred<T>();

    const timer = setTimeout(() => {
      const inflight = this.inFlight.get(id);
      if (!inflight) return;
      this.inFlight.delete(id);
      inflight.def.reject(new TimeoutError(`${method} timed out after ${this.requestTimeoutMs}ms`));
    }, this.requestTimeoutMs);

    this.inFlight.set(id, { def: def as Deferred<unknown>, method, timer });

    try {
      await this.transport.send(text);
    } catch (err) {
      clearTimeout(timer);
      this.inFlight.delete(id);
      throw err;
    }

    return def.promise;
  }

  /** Set the listener for server-initiated notifications (one per client). */
  onNotification(listener: (n: JsonRpcNotification) => void): void {
    this.notifListener = listener;
  }

  private handle(ev: TransportEvent): void {
    if (ev.type === 'close') {
      this.state = 'disconnected';
      const reason =
        ev.code !== undefined
          ? `socket closed (code=${ev.code}${ev.reason ? `, reason=${ev.reason}` : ''})`
          : 'socket closed';
      this.failAllInFlight(new TransportError(reason));
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
    } catch {
      // TODO(M4): forward to ErrorClassifier as a 'protocol' error against
      // this client. For M1 we silently drop so a misbehaving server can't
      // take the whole client down.
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
    const inflight = this.inFlight.get(msg.id);
    if (!inflight) return; // unknown id (late response after timeout)
    this.inFlight.delete(msg.id);
    clearTimeout(inflight.timer);
    if ('error' in msg) {
      const err = msg.error;
      inflight.def.reject(
        err.data !== undefined
          ? new RpcError(err.message, err.code, err.data)
          : new RpcError(err.message, err.code),
      );
    } else {
      inflight.def.resolve(msg.result);
    }
  }

  private failAllInFlight(err: Error): void {
    for (const inflight of this.inFlight.values()) {
      clearTimeout(inflight.timer);
      inflight.def.reject(err);
    }
    this.inFlight.clear();
  }
}
