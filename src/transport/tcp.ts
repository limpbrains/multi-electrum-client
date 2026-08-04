// TCP transport for Electrum's plain JSON-RPC over a raw socket.
//
// Imports node:net statically. RN consumers point metro at
// `react-native-tcp-socket` via the standard alias — its API is a 1:1
// emulation of node:net's `Socket`.
//
// Browser builds: this module is currently re-exported from the package
// root (so its top-level `import 'node:net'` is reachable from a
// browser bundler's resolution graph even when the consumer never
// instantiates `TcpTransport`). Until the package ships a separate
// browser entry (M7), browser users must add a bundler alias /
// fallback:
//
//     // webpack
//     resolve.fallback = { 'node:net': false, 'node:tls': false };
//     // vite
//     resolve.alias = { 'node:net': false, 'node:tls': false };
//
// or restrict their package import to `multi-electrum-client/transport/ws`.
// Bun and Node use the modules directly.
//
// Framing: newline-delimited messages via the shared `LineFramer`. Outgoing
// payloads are appended `\n`; incoming chunks are split on `\r?\n` with
// partial lines buffered across reads.
//
// close() during an in-flight connect() aborts it: the pending socket is
// destroyed and the connect promise rejects with
// `TransportError('closed during connect')`.

import type { Socket as NodeSocket } from 'node:net';
import { connect as netConnect } from 'node:net';

import type { Endpoint } from '../client.js';
import { TransportError } from '../errors/types.js';
import { registerTransport } from './factory.js';
import { LineFramer } from './lineFramer.js';
import type { Transport, TransportEvent, TransportListener } from './types.js';

/**
 * Subset of `net.Socket` we actually use. Lets RN's
 * `react-native-tcp-socket` Socket and `tls.TLSSocket` (which extends
 * `net.Socket`) sub in via metro alias / TLS factory without import
 * gymnastics. `off` is declared optional and informational — currently
 * unused; transport instances are one-shot, so listener removal is
 * handled by socket destruction rather than explicit unbind.
 */
export interface TcpSocketLike {
  on(event: 'data', listener: (chunk: Buffer | string) => void): this;
  on(event: 'close', listener: (hadError: boolean) => void): this;
  on(event: 'error', listener: (err: Error) => void): this;
  on(event: 'connect' | 'ready' | 'secureConnect', listener: () => void): this;
  once?(event: string, listener: (...args: unknown[]) => void): this;
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

/**
 * Internal extension used by `TlsTransport` to retarget the protocol guard
 * and switch the connect-resolution event. NOT exported — the public
 * surface is `TcpTransportOpts`.
 */
interface InternalOpts extends TcpTransportOpts {
  /** Allowed `endpoint.protocol` values. Default: `['tcp']`. */
  allowProtocols?: readonly string[];
  /**
   * Socket event to await for "connect succeeded". `'connect'` for raw
   * TCP; `'secureConnect'` for TLS (fires after the handshake completes,
   * not just the underlying TCP setup).
   */
  readyEvent?: 'connect' | 'secureConnect';
}

export class TcpTransport implements Transport {
  readonly endpoint: Endpoint;
  private socket: TcpSocketLike | null = null;
  private readonly listeners = new Set<TransportListener>();
  private readonly framer = new LineFramer();
  private readonly connectFn: (host: string, port: number) => TcpSocketLike;
  private readonly connectTimeoutMs: number;
  private readonly readyEvent: 'connect' | 'secureConnect';
  /** True after `connect()` resolves. Gates emit() so pre-connect / post-timeout close events don't surface. */
  private opened = false;
  /**
   * Set only while a `connect()` is in flight. Invoking it destroys the
   * pending socket and rejects the connect promise — `close()` calls it
   * so a close-during-connect doesn't leak a half-open socket.
   */
  private abortConnect: (() => void) | null = null;

  constructor(opts: TcpTransportOpts) {
    const internal = opts as InternalOpts;
    const allowed = internal.allowProtocols ?? ['tcp'];
    if (!allowed.includes(opts.endpoint.protocol)) {
      throw new TransportError(
        `TcpTransport requires protocol in [${allowed.join(', ')}], got '${opts.endpoint.protocol}'`,
      );
    }
    this.endpoint = opts.endpoint;
    this.connectFn =
      opts.connect ?? ((host, port) => netConnect({ host, port }) as unknown as TcpSocketLike);
    this.connectTimeoutMs = opts.connectTimeoutMs ?? 10_000;
    this.readyEvent = internal.readyEvent ?? 'connect';
  }

  async connect(): Promise<void> {
    // A connect always starts with an empty framer (the close handler
    // resets it too, but state the invariant where it matters).
    this.framer.reset();
    const socket = this.connectFn(this.endpoint.host, this.endpoint.port);
    socket.setEncoding('utf-8');

    // Wire data / close / error handlers BEFORE awaiting connect so we
    // don't drop bytes the server may push as soon as the socket opens.
    // `setEncoding('utf-8')` above means `chunk` arrives as a string from
    // both `node:net` and `react-native-tcp-socket`. If a future shim
    // ignores the encoding hint and emits a Buffer, `Buffer.toString`
    // decodes correctly; a raw `Uint8Array` would not (its `toString`
    // returns comma-decimals), so we route those through `TextDecoder`.
    //
    // The decoder is created lazily INSIDE that branch: Hermes (React
    // Native) has no global TextDecoder, and constructing it eagerly here
    // made every connect() throw on stock RN even though the normal
    // string / Buffer paths never need it.
    let decoder: TextDecoder | undefined;
    socket.on('data', (chunk) => {
      let text: string;
      if (typeof chunk === 'string') {
        text = chunk;
      } else if (typeof Buffer !== 'undefined' && Buffer.isBuffer(chunk)) {
        text = chunk.toString('utf-8');
      } else if (chunk instanceof Uint8Array && typeof TextDecoder !== 'undefined') {
        decoder ??= new TextDecoder();
        text = decoder.decode(chunk);
      } else {
        // Unknown shape — surface as an error rather than silently
        // corrupting the framer's input.
        this.emit({
          type: 'error',
          cause: new TransportError('unexpected data chunk type from socket'),
        });
        return;
      }
      for (const line of this.framer.push(text)) {
        this.emit({ type: 'data', text: line });
      }
    });
    socket.on('close', (hadError) => {
      this.framer.reset();
      // Pre-connect / post-timeout close: the connect promise has already
      // been rejected (or is about to be). Emitting `close` here would
      // surface as a stray transport event to a caller that already
      // failed the connect attempt.
      if (!this.opened) return;
      this.emit({
        type: 'close',
        ...(hadError ? { reason: 'transport error' } : {}),
      });
    });

    let connected = false;
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.abortConnect = null;
        reject(new TransportError(`connect timeout after ${this.connectTimeoutMs}ms`));
        try {
          socket.destroy();
        } catch {
          // ignore
        }
      }, this.connectTimeoutMs);
      this.abortConnect = () => {
        this.abortConnect = null;
        clearTimeout(timer);
        reject(new TransportError('closed during connect'));
        try {
          socket.destroy();
        } catch {
          // ignore
        }
      };
      socket.on(this.readyEvent, () => {
        this.abortConnect = null;
        clearTimeout(timer);
        connected = true;
        resolve();
      });
      socket.on('error', (err) => {
        if (!connected) {
          this.abortConnect = null;
          clearTimeout(timer);
          reject(new TransportError('connect error', err));
        } else {
          this.emit({ type: 'error', cause: err });
        }
      });
    });

    this.socket = socket;
    this.opened = true;
  }

  async send(text: string): Promise<void> {
    if (!this.socket) throw new TransportError('not connected');
    if (text.includes('\n')) {
      throw new TransportError('payload must not contain embedded newline');
    }
    this.socket.write(text + '\n');
  }

  async close(): Promise<void> {
    // Close during an in-flight connect: abort it (destroys the pending
    // socket, rejects the connect promise) instead of leaking the socket.
    if (this.abortConnect) {
      this.abortConnect();
      return;
    }
    const socket = this.socket;
    if (!socket) return;
    this.socket = null;
    this.opened = false;
    await new Promise<void>((resolve) => {
      // Use `once` if the socket implementation supports it so calling
      // close() repeatedly doesn't stack listeners. Falls back to `on`
      // for socket shims that don't expose `once`.
      let settled = false;
      const settle = (): void => {
        if (settled) return;
        settled = true;
        clearTimeout(backstop);
        resolve();
      };
      // Backstop: a slow / dead peer may never send FIN-ACK in response
      // to our `end()`. Wait at most 500ms before forcibly destroying
      // the socket so callers (manager.stop, suspend) don't hang on
      // teardown.
      const backstop = setTimeout(() => {
        try {
          socket.destroy();
        } catch {
          // ignore
        }
        settle();
      }, 500);
      // Prefer `once` so the listener detaches after firing; shims
      // without it fall back to `on` — safe either way, `settle` is
      // idempotent and a second close() returns early (socket nulled).
      if (typeof socket.once === 'function') {
        socket.once('close', settle);
      } else {
        socket.on('close', settle);
      }
      try {
        socket.end();
      } catch {
        try {
          socket.destroy();
        } catch {
          // ignore
        }
        settle();
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
}

// Type alias re-export for ergonomics if someone wants the precise node type.
export type { NodeSocket };
// Internal options type — exported for `TlsTransport` only. Not part of the
// public surface; do not document in README.
export type { InternalOpts as InternalTcpTransportOpts };

// Self-register so `defaultTransportFactory` finds us by protocol. Importing
// this module pulls in `node:net` — the browser entry intentionally skips it.
registerTransport('tcp', (endpoint) => new TcpTransport({ endpoint }));
