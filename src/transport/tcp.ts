// TCP transport for Electrum's plain JSON-RPC over a raw socket.
//
// Imports node:net statically. RN consumers point metro at
// `react-native-tcp-socket` via the standard alias — its API is a 1:1
// emulation of node:net's `Socket`. Browser / Bun callers should use
// WsTransport (this file is not bundled into browser builds — see
// package.json conditional exports).
//
// Framing: newline-delimited messages via the shared `LineFramer`. Outgoing
// payloads are appended `\n`; incoming chunks are split on `\r?\n` with
// partial lines buffered across reads.

import type { Socket as NodeSocket } from 'node:net';
import { connect as netConnect } from 'node:net';

import type { Endpoint } from '../client.js';
import { TransportError } from '../errors/types.js';
import { LineFramer } from './lineFramer.js';
import type { Transport, TransportEvent, TransportListener } from './types.js';

/**
 * Subset of `net.Socket` we actually use. Lets RN's
 * `react-native-tcp-socket` Socket sub in via metro alias without import
 * gymnastics.
 */
export interface TcpSocketLike {
  on(event: 'data', listener: (chunk: Buffer | string) => void): this;
  on(event: 'close', listener: (hadError: boolean) => void): this;
  on(event: 'error', listener: (err: Error) => void): this;
  on(event: 'connect' | 'ready', listener: () => void): this;
  off?(event: string, listener: (...args: unknown[]) => void): this;
  setEncoding(encoding: BufferEncoding): this;
  write(chunk: string | Uint8Array): boolean;
  end(): void;
  destroy(err?: Error): void;
}

export interface TcpTransportOpts {
  endpoint: Endpoint;
  /**
   * Override socket factory. Defaults to `net.connect`. Tests pass an
   * in-memory socket; RN wallets that want explicit control over the
   * react-native-tcp-socket constructor can also wire it here.
   */
  connect?: (host: string, port: number) => TcpSocketLike;
  /** Connect timeout, default 10_000ms. */
  connectTimeoutMs?: number;
}

export class TcpTransport implements Transport {
  readonly endpoint: Endpoint;
  private socket: TcpSocketLike | null = null;
  private readonly listeners = new Set<TransportListener>();
  private readonly framer = new LineFramer();
  private readonly connectFn: (host: string, port: number) => TcpSocketLike;
  private readonly connectTimeoutMs: number;
  private closing = false;

  constructor(opts: TcpTransportOpts & { allowProtocols?: readonly string[] }) {
    const allowed = opts.allowProtocols ?? ['tcp'];
    if (!allowed.includes(opts.endpoint.protocol)) {
      throw new TransportError(
        `TcpTransport requires protocol in [${allowed.join(', ')}], got '${opts.endpoint.protocol}'`,
      );
    }
    this.endpoint = opts.endpoint;
    this.connectFn =
      opts.connect ?? ((host, port) => netConnect({ host, port }) as unknown as TcpSocketLike);
    this.connectTimeoutMs = opts.connectTimeoutMs ?? 10_000;
  }

  async connect(): Promise<void> {
    const socket = this.connectFn(this.endpoint.host, this.endpoint.port);
    socket.setEncoding('utf-8');

    // Wire data / close / error handlers BEFORE awaiting connect so we
    // don't drop bytes the server may push as soon as the socket opens.
    socket.on('data', (chunk) => {
      const text = typeof chunk === 'string' ? chunk : chunk.toString('utf-8');
      for (const line of this.framer.push(text)) {
        this.emit({ type: 'data', text: line });
      }
    });
    socket.on('close', (hadError) => {
      this.framer.reset();
      this.emit({
        type: 'close',
        ...(hadError ? { reason: 'transport error' } : {}),
      });
    });

    let connected = false;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new TransportError(`connect timeout after ${this.connectTimeoutMs}ms`));
        try {
          socket.destroy();
        } catch {
          // ignore
        }
      }, this.connectTimeoutMs);
      socket.on('connect', () => {
        clearTimeout(timer);
        connected = true;
        resolve();
      });
      socket.on('error', (err) => {
        if (!connected) {
          clearTimeout(timer);
          reject(new TransportError('connect error', err));
        } else {
          this.emit({ type: 'error', cause: err });
        }
      });
    });

    this.socket = socket;
  }

  async send(text: string): Promise<void> {
    if (!this.socket) throw new TransportError('not connected');
    if (text.includes('\n')) {
      throw new TransportError('payload must not contain embedded newline');
    }
    this.socket.write(text + '\n');
  }

  async close(): Promise<void> {
    const socket = this.socket;
    if (!socket) return;
    this.closing = true;
    await new Promise<void>((resolve) => {
      const onClose = (): void => resolve();
      socket.on('close', onClose);
      try {
        socket.end();
      } catch {
        try {
          socket.destroy();
        } catch {
          // ignore
        }
        resolve();
      }
    });
    this.socket = null;
    this.closing = false;
  }

  on(listener: TransportListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private emit(ev: TransportEvent): void {
    // Suppress emit while we're cooperatively closing — the caller
    // initiated the disconnect; surfacing the resulting `close` event
    // again would cause Manager's onStateChange to fire twice.
    if (this.closing && ev.type !== 'close') return;
    for (const l of this.listeners) l(ev);
  }
}

// Type alias re-export for ergonomics if someone wants the precise node type.
export type { NodeSocket };
