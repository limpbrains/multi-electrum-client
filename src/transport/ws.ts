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
// TODO(M2): close() during in-flight connect() leaves the ws to finish
// opening into this.ws — needs a coordinated abort path before reconnect
// logic in M2 starts retrying connects.

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
  private readonly decoder = new TextDecoder();

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
    const scheme = this.endpoint.protocol === 'wss' ? 'wss' : 'ws';
    const path = this.endpoint.path ?? '';
    const url = `${scheme}://${this.endpoint.host}:${this.endpoint.port}${path}`;

    const ws = new this.Ctor(url);
    ws.binaryType = 'arraybuffer';

    // Attach data handlers synchronously, BEFORE awaiting open, so we don't
    // drop frames the server may send immediately on its connection handler.
    ws.addEventListener('message', (ev) => {
      const text = this.toText((ev as MessageEvent).data);
      if (text === undefined) return;
      for (const line of this.framer.push(text)) {
        this.emit({ type: 'data', text: line });
      }
    });

    ws.addEventListener('close', (ev) => {
      const ce = ev as CloseEvent;
      const out: TransportEvent = {
        type: 'close',
        ...(typeof ce.code === 'number' ? { code: ce.code } : {}),
        ...(typeof ce.reason === 'string' && ce.reason.length > 0 ? { reason: ce.reason } : {}),
      };
      this.emit(out);
    });

    let connected = false;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new TransportError(`connect timeout after ${this.connectTimeoutMs}ms`));
        try {
          ws.close();
        } catch {
          // ignore
        }
      }, this.connectTimeoutMs);
      ws.addEventListener(
        'open',
        () => {
          clearTimeout(timer);
          connected = true;
          resolve();
        },
        { once: true },
      );
      ws.addEventListener('error', (ev) => {
        if (!connected) {
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
    if (data instanceof ArrayBuffer) return this.decoder.decode(new Uint8Array(data));
    if (ArrayBuffer.isView(data)) {
      return this.decoder.decode(data as ArrayBufferView as Uint8Array);
    }
    if (typeof Buffer !== 'undefined' && Buffer.isBuffer(data)) {
      return data.toString('utf-8');
    }
    return undefined;
  }
}

// Self-register so `defaultTransportFactory` finds us by protocol.
registerTransport('ws', (endpoint) => new WsTransport({ endpoint }));
registerTransport('wss', (endpoint) => new WsTransport({ endpoint }));
