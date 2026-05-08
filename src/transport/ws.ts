// WebSocket transport. Universal: uses globalThis.WebSocket where available
// (Node 22+, browser, RN, Bun); on Node 20 the caller passes the `ws` package's
// WebSocket constructor via `opts.WebSocket`.
//
// Framing: Electrum servers that expose WebSocket commonly bridge to the
// underlying TCP protocol's newline-delimited JSON-RPC framing. We follow that
// convention: outbound bytes are appended with `\n`; inbound text is split on
// `\r?\n` and emitted one logical message per `data` event. A LineFramer
// buffers across frames so partial lines (very rare with WS, but possible if a
// proxy splits messages) reassemble correctly.

import type { Endpoint } from '../client.js';
import { TransportError } from '../errors/types.js';
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
  private readonly encoder = new TextEncoder();
  private readonly decoder = new TextDecoder();
  private readonly framer = new LineFramer();

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
    if ('binaryType' in ws) {
      (ws as unknown as { binaryType: BinaryType }).binaryType = 'arraybuffer';
    }

    // Attach data handlers synchronously, BEFORE awaiting open, so we don't
    // drop frames the server may send immediately on its connection handler.
    ws.addEventListener('message', (ev) => {
      const text = this.toText((ev as MessageEvent).data);
      if (text === undefined) return;
      for (const line of this.framer.push(text)) {
        this.emit({ type: 'data', bytes: this.encoder.encode(line) });
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
          this.emit({ type: 'error', cause: ev });
        }
      });
    });

    this.ws = ws;
  }

  async send(bytes: Uint8Array): Promise<void> {
    if (!this.ws) throw new TransportError('not connected');
    const text = this.decoder.decode(bytes) + '\n';
    this.ws.send(text);
  }

  async close(): Promise<void> {
    const ws = this.ws;
    if (!ws) return;
    if (ws.readyState === ws.CLOSED) return;
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

class LineFramer {
  private buf = '';
  push(chunk: string): string[] {
    this.buf += chunk;
    const parts = this.buf.split(/\r?\n/);
    this.buf = parts.pop() ?? '';
    return parts.filter((p) => p.length > 0);
  }
}
