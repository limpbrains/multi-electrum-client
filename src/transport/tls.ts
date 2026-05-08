// TLS transport for Electrum's JSON-RPC over an encrypted socket.
//
// Wraps the same socket-handling pipeline as `TcpTransport` — only the
// connect call differs. Imports node:tls statically; RN consumers point
// metro at `react-native-tcp-socket` (its `tls` shim mirrors the same
// API surface). The shared `LineFramer` handles message delimiting; the
// shared `TcpSocketLike` interface covers both `net.Socket` and
// `tls.TLSSocket` since the latter extends the former.

import type { TLSSocket, ConnectionOptions } from 'node:tls';
import { connect as tlsConnect } from 'node:tls';

import type { Endpoint } from '../client.js';
import { TransportError } from '../errors/types.js';
import { TcpTransport, type TcpSocketLike } from './tcp.js';
import type { Transport } from './types.js';

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
  /**
   * Optional `tls.ConnectionOptions` merged into the default connect call.
   * Ignored when `connect` is provided. Use it to pin a CA bundle or relax
   * `rejectUnauthorized` against self-signed test servers.
   */
  tlsOptions?: ConnectionOptions;
}

/**
 * Convenience factory: implements the same `Transport` shape as
 * `TcpTransport` but routes through `tls.connect`. Constructed via a
 * delegated `TcpTransport` so all the framing / error / close logic stays
 * in one place.
 */
export class TlsTransport implements Transport {
  readonly endpoint: Endpoint;
  private readonly inner: TcpTransport;

  constructor(opts: TlsTransportOpts) {
    if (opts.endpoint.protocol !== 'tls') {
      // `wss` belongs to WsTransport; this transport handles raw
      // JSON-RPC-over-TLS only.
      throw new TransportError(
        `TlsTransport requires protocol 'tls', got '${opts.endpoint.protocol}'`,
      );
    }
    this.endpoint = opts.endpoint;
    const { tlsOptions } = opts;
    const innerOpts: ConstructorParameters<typeof TcpTransport>[0] = {
      endpoint: opts.endpoint,
      allowProtocols: ['tls'],
      connect:
        opts.connect ??
        ((host, port) => {
          const socket = tlsConnect({
            host,
            port,
            ...(tlsOptions ?? {}),
          });
          return socket as unknown as TcpSocketLike;
        }),
      ...(opts.connectTimeoutMs !== undefined ? { connectTimeoutMs: opts.connectTimeoutMs } : {}),
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

  on(listener: Parameters<Transport['on']>[0]): () => void {
    return this.inner.on(listener);
  }
}

// Re-export the precise node TLS types for callers who want them.
export type { TLSSocket, ConnectionOptions };
