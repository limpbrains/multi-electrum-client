// TLS transport for Electrum's JSON-RPC over an encrypted socket.
//
// Wraps the same socket-handling pipeline as `TcpTransport` — only the
// connect call and the ready-event differ. `tls.TLSSocket` extends
// `net.Socket`, but its connect-completed signal is `'secureConnect'`
// (post-handshake), not `'connect'` (which fires after the underlying
// TCP setup but BEFORE the TLS handshake). Resolving on `'connect'`
// would clear the connect-timeout and let the caller proceed before the
// cert is verified — a self-signed test server would appear to connect.
//
// Imports node:tls statically; RN consumers point metro at
// `react-native-tcp-socket` (its `tls` shim mirrors the same API
// surface).

import type { TLSSocket, ConnectionOptions } from 'node:tls';
import { connect as tlsConnect } from 'node:tls';

import type { Endpoint } from '../client.js';
import { TransportError } from '../errors/types.js';
import { registerTransport } from './factory.js';
import { TcpTransport, type TcpSocketLike, type InternalTcpTransportOpts } from './tcp.js';
import type { Transport, TransportListener } from './types.js';

export interface TlsTransportOpts {
  endpoint: Endpoint;
  /**
   * Override TLS-connect factory. Defaults to `tls.connect`. Tests pass an
   * in-memory socket; production callers may want to pass extra
   * `ConnectionOptions` (`rejectUnauthorized`, `ca`, etc.) via this hook.
   */
  connect?: (host: string, port: number) => TcpSocketLike;
  /** Connect timeout, default 10_000ms. */
  connectTimeoutMs?: number;
  /** Cap on one logical line (default 32 MiB) — see `TcpTransportOpts`. */
  maxLineLength?: number;
  /**
   * Optional `tls.ConnectionOptions` merged into the default connect call.
   * Ignored when `connect` is provided. Use it to pin a CA bundle or relax
   * `rejectUnauthorized` against self-signed test servers.
   */
  tlsOptions?: ConnectionOptions;
}

/**
 * Convenience factory: implements the same `Transport` shape as
 * `TcpTransport` but routes through `tls.connect` and awaits
 * `'secureConnect'` (TLS-handshake-complete) before resolving the connect
 * promise. Constructed via a delegated `TcpTransport` so all the framing /
 * error / close logic stays in one place.
 */
export class TlsTransport implements Transport {
  readonly endpoint: Endpoint;
  private readonly inner: TcpTransport;

  constructor(opts: TlsTransportOpts) {
    if (opts.endpoint.protocol !== 'tls') {
      // `wss` belongs to WsTransport; this transport handles raw
      // JSON-RPC-over-TLS only. (TcpTransport's protocol guard would
      // also reject this, but a TLS-specific message is friendlier.)
      throw new TransportError(
        `TlsTransport requires protocol 'tls', got '${opts.endpoint.protocol}'`,
      );
    }
    this.endpoint = opts.endpoint;
    const { tlsOptions } = opts;
    const innerOpts: InternalTcpTransportOpts = {
      endpoint: opts.endpoint,
      allowProtocols: ['tls'],
      readyEvent: 'secureConnect',
      connect:
        opts.connect ??
        ((host, port) => tlsConnect({ host, port, ...tlsOptions }) as unknown as TcpSocketLike),
      ...(opts.connectTimeoutMs !== undefined ? { connectTimeoutMs: opts.connectTimeoutMs } : {}),
      ...(opts.maxLineLength !== undefined ? { maxLineLength: opts.maxLineLength } : {}),
    };
    this.inner = new TcpTransport(innerOpts);
  }

  connect(): Promise<void> {
    return this.inner.connect();
  }

  send(text: string): Promise<void> {
    return this.inner.send(text);
  }

  close(): Promise<void> {
    return this.inner.close();
  }

  on(listener: TransportListener): () => void {
    return this.inner.on(listener);
  }
}

// Re-export the precise node TLS types for callers who want them.
export type { TLSSocket, ConnectionOptions };

// Self-register so `defaultTransportFactory` finds us by protocol. Importing
// this module pulls in `node:tls` — the browser entry intentionally skips it.
registerTransport('tls', (endpoint) => new TlsTransport({ endpoint }));
