// WebSocket transport. Universal: uses globalThis.WebSocket where available
// (Node 22+, browser, RN, Bun); on Node 20 the caller passes the `ws` package's
// WebSocket constructor via `opts.WebSocket`.
//
// Framing: Electrum servers that expose WebSocket commonly bridge to the
// underlying TCP protocol's newline-delimited JSON-RPC framing. We follow that
// convention: outbound text is appended with `\n`; inbound text is split on
// `\r?\n` and emitted one logical message per `data` event. A LineFramer
// buffers across frames so partial lines reassemble correctly if a proxy
// splits messages.
//
// close() during an in-flight connect() aborts it: the pending WebSocket
// is closed and the connect promise rejects with
// `TransportError('closed during connect')`.

import type { Endpoint } from '../client.js';
import { TransportError } from '../errors/types.js';
import { registerTransport } from './factory.js';
import { LineFramer } from './lineFramer.js';
import type { Transport, TransportEvent, TransportListener } from './types.js';

type WebSocketCtor = new (url: string) => WebSocket;

export interface WsTransportOpts {
  endpoint: Endpoint;
  /**
   * WebSocket constructor. Defaults to globalThis.WebSocket. Pass `ws.WebSocket`
   * from the `ws` package to support Node 20 (where the global is gated behind
   * --experimental-websocket).
   */
  WebSocket?: WebSocketCtor;
  /** Connect timeout, default 10_000ms. */
  connectTimeoutMs?: number;
}

export class WsTransport implements Transport {
  readonly endpoint: Endpoint;
  private ws: WebSocket | null = null;
  private readonly listeners = new Set<TransportListener>();
  private readonly Ctor: WebSocketCtor;
  private readonly connectTimeoutMs: number;
  private readonly framer = new LineFramer();
  /**
   * Created lazily on the first binary frame: Hermes (React Native) has no
   * global TextDecoder, and constructing one in the field initializer made
   * `new WsTransport(...)` throw there — even though Electrum servers speak
   * text frames and the decoder is never needed on the normal path.
   */
  private decoder: TextDecoder | undefined;
  /**
   * Set only while a `connect()` is in flight. Invoking it closes the
   * pending WebSocket and rejects the connect promise — `close()` calls
   * it so a close-during-connect doesn't leak a half-open socket.
   */
  private abortConnect: (() => void) | null = null;

  constructor(opts: WsTransportOpts) {
    if (opts.endpoint.protocol !== 'ws' && opts.endpoint.protocol !== 'wss') {
      throw new TransportError(
        `WsTransport requires protocol 'ws' or 'wss', got '${opts.endpoint.protocol}'`,
      );
    }
    this.endpoint = opts.endpoint;
    const Ctor =
      opts.WebSocket ?? (globalThis as unknown as { WebSocket?: WebSocketCtor }).WebSocket;
    if (!Ctor) {
      throw new TransportError(
        "no WebSocket constructor available; pass opts.WebSocket (e.g. from 'ws' on Node 20) or use Node 22+",
      );
    }
    this.Ctor = Ctor;
    this.connectTimeoutMs = opts.connectTimeoutMs ?? 10_000;
  }

  async connect(): Promise<void> {
    // Drop any partial line buffered from a previous connection — stale
    // bytes would otherwise prepend to the first message on this socket.
    // (TcpTransport resets in its close handler; WS resets here.)
    this.framer.reset();
    const scheme = this.endpoint.protocol === 'wss' ? 'wss' : 'ws';
    const path = this.endpoint.path ?? '';
    const url = `${scheme}://${this.endpoint.host}:${this.endpoint.port}${path}`;

    const ws = new this.Ctor(url);
    ws.binaryType = 'arraybuffer';

    // Attach data handlers synchronously, BEFORE awaiting open, so we don't
    // drop frames the server may send immediately on its connection handler.
    ws.addEventListener('message', (ev) => {
      const text = this.toText((ev as MessageEvent).data);
      if (text === undefined) {
        // Undecodable frame (unknown data type, or binary data on a
        // runtime without TextDecoder) — surface it instead of silently
        // dropping; a dropped frame otherwise shows up as a hung request.
        this.emit({
          type: 'error',
          cause: new TransportError('undecodable message data from socket'),
        });
        return;
      }
      for (const line of this.framer.push(text)) {
        this.emit({ type: 'data', text: line });
      }
    });

    let connected = false;
    ws.addEventListener('close', (ev) => {
      // Suppress close events from a connect that never completed
      // (timeout / abort / handshake error): the connect promise has
      // already rejected, a stray 'close' would surface to listeners as
      // an event on a connection they never saw open.
      if (!connected) return;
      const ce = ev as CloseEvent;
      const out: TransportEvent = {
        type: 'close',
        ...(typeof ce.code === 'number' ? { code: ce.code } : {}),
        ...(typeof ce.reason === 'string' && ce.reason.length > 0 ? { reason: ce.reason } : {}),
      };
      this.emit(out);
    });

    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.abortConnect = null;
        reject(new TransportError(`connect timeout after ${this.connectTimeoutMs}ms`));
        try {
          ws.close();
        } catch {
          // ignore
        }
      }, this.connectTimeoutMs);
      this.abortConnect = () => {
        this.abortConnect = null;
        clearTimeout(timer);
        reject(new TransportError('closed during connect'));
        try {
          ws.close();
        } catch {
          // ignore
        }
      };
      ws.addEventListener(
        'open',
        () => {
          this.abortConnect = null;
          clearTimeout(timer);
          connected = true;
          resolve();
        },
        { once: true },
      );
      ws.addEventListener('error', (ev) => {
        if (!connected) {
          this.abortConnect = null;
          clearTimeout(timer);
          reject(new TransportError('connect error', ev));
        } else {
          // TODO(M4): wrap with readyState + any platform-specific detail
          // before forwarding so the classifier has more to work with.
          this.emit({ type: 'error', cause: ev });
        }
      });
    });

    this.ws = ws;
  }

  async send(text: string): Promise<void> {
    if (!this.ws) throw new TransportError('not connected');
    // We append '\n' to delimit messages on the wire. An embedded newline in
    // the payload would corrupt framing on the server side. JSON.stringify
    // (the only intended caller via ElectrumClient) escapes newlines, so this
    // guard only fires for misuse.
    if (text.includes('\n')) {
      throw new TransportError('payload must not contain embedded newline');
    }
    this.ws.send(text + '\n');
  }

  async close(): Promise<void> {
    // Close during an in-flight connect: abort it (closes the pending
    // WebSocket, rejects the connect promise) instead of leaking it.
    if (this.abortConnect) {
      this.abortConnect();
      return;
    }
    const ws = this.ws;
    if (!ws) return;
    if (ws.readyState === ws.CLOSED) {
      this.ws = null;
      return;
    }
    await new Promise<void>((resolve) => {
      const onClose = () => {
        ws.removeEventListener('close', onClose);
        resolve();
      };
      ws.addEventListener('close', onClose);
      try {
        ws.close();
      } catch {
        ws.removeEventListener('close', onClose);
        resolve();
      }
    });
    this.ws = null;
  }

  on(listener: TransportListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private emit(ev: TransportEvent): void {
    for (const l of this.listeners) l(ev);
  }

  private toText(data: unknown): string | undefined {
    if (typeof data === 'string') return data;
    // Buffer check comes BEFORE the generic view check: Buffer IS a
    // Uint8Array, and its own utf-8 decoding needs no TextDecoder (which
    // Hermes lacks).
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(data)) {
      return data.toString('utf-8');
    }
    if (typeof TextDecoder === 'undefined') return undefined;
    if (data instanceof ArrayBuffer) {
      this.decoder ??= new TextDecoder();
      return this.decoder.decode(new Uint8Array(data));
    }
    if (ArrayBuffer.isView(data)) {
      this.decoder ??= new TextDecoder();
      return this.decoder.decode(data as ArrayBufferView as Uint8Array);
    }
    return undefined;
  }
}

// Self-register so `defaultTransportFactory` finds us by protocol.
registerTransport('ws', (endpoint) => new WsTransport({ endpoint }));
registerTransport('wss', (endpoint) => new WsTransport({ endpoint }));
