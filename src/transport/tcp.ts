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
import { TransportLifecycle } from './lifecycle.js';
import { assertMaxLineLength, LineFramer } from './lineFramer.js';
import type { Transport, TransportEvent, TransportListener } from './types.js';
import { Utf8Stream } from './utf8.js';

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
  /**
   * Cap on one logical line / the retained partial buffer (default
   * 32 MiB — sized to the protocol's largest valid response, see
   * `DEFAULT_MAX_LINE_LENGTH`). Bounds a malicious or broken server
   * streaming newline-free data; on overflow the transport emits an
   * `error` event and closes. `endpoint.maxLineLength` (the per-server
   * declaration) takes precedence over this construction-wide value.
   */
  maxLineLength?: number;
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
  /**
   * Serializes connect / close and stamps each attempt with a generation
   * so a superseded socket's handlers go inert. See ./lifecycle.ts.
   */
  private readonly lifecycle = new TransportLifecycle();
  private readonly maxLineLength: number | undefined;
  private readonly connectFn: (host: string, port: number) => TcpSocketLike;
  private readonly connectTimeoutMs: number;
  private readonly readyEvent: 'connect' | 'secureConnect';
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
    // The framer is per-connect (see `connect`), so validate the option
    // eagerly instead of surfacing it on the first connection.
    // The per-server endpoint declaration outranks the construction
    // opt: a custom factory's opts value is a fleet-wide default, while
    // `endpoint.maxLineLength` is the operator sizing ONE server (say,
    // the only one allowed to serve verbose multi-MB transactions).
    // The other precedence silently turns the per-server knob into a
    // no-op for every custom-factory deployment.
    const maxLineLength = opts.endpoint.maxLineLength ?? opts.maxLineLength;
    if (maxLineLength !== undefined) assertMaxLineLength(maxLineLength);
    this.maxLineLength = maxLineLength;
  }

  async connect(): Promise<void> {
    return this.lifecycle.connect(
      () => this.socket !== null,
      (gen) => this.connectOnce(gen),
    );
  }

  private async connectOnce(gen: number): Promise<void> {
    /** True once this attempt has been superseded, closed, or retired. */
    const stale = (): boolean => this.lifecycle.isStale(gen);
    // One framer per attempt: a shared instance let a stale socket's
    // bytes (or its reset) corrupt the partial line of the connection
    // that replaced it.
    const framer = new LineFramer(this.maxLineLength);
    const socket = this.connectFn(this.endpoint.host, this.endpoint.port);

    // Wire data / close / error handlers BEFORE awaiting connect so we
    // don't drop bytes the server may push as soon as the socket opens.
    //
    // The socket is deliberately left in binary mode. `setEncoding('utf-8')`
    // hands over a string that Node's own decoder has already "repaired" —
    // a malformed byte arrives as U+FFFD, inside JSON that still parses, so
    // the corruption reaches the caller as data and our validator never
    // sees the bytes that caused it. Decoding happens here instead, where
    // invalid input can end the connection.
    let decoder: Utf8Stream | undefined;
    // Per-attempt state. The handlers below outlive their connect() call
    // and can fire for a socket generation that is no longer active, so
    // they close over these instead of only consulting shared fields.
    let terminal: TransportError | undefined;
    let closeEmitted = false;
    /** Set by the connect promise; fails this attempt from a handler. */
    let failAttempt: ((e: TransportError) => void) | undefined;
    /**
     * End this connection on unusable protocol data. The close event is
     * emitted here explicitly: a self-initiated close() suppresses its
     * own event, but the caller's in-flight requests must fail over NOW,
     * not wait out their timeouts. (A duplicate close event is harmless
     * — the client detaches its transport listener on the first one.)
     * A socket connect() never published emits nothing and fails the
     * attempt instead: nobody holds it, and the rejection is the report.
     */
    const failStream = (cause: TransportError, reason: string): void => {
      terminal = cause;
      closeEmitted = true;
      const published = this.socket === socket;
      // Order matters: retire, unpublish and physically tear down the
      // socket BEFORE calling any listener. Emitting first would let a
      // throwing consumer callback strand a retired-but-still-stored
      // socket — no native event could finish the cleanup (they are all
      // stale by then) and connect() would refuse to replace it.
      this.lifecycle.retire();
      if (published) {
        this.socket = null;
      }
      // Destroy THIS socket directly instead of via close(): the data can
      // arrive between the ready event and the `this.socket = socket`
      // assignment below, where close() would see a null field, no-op,
      // and leave the socket open. `terminal` makes the post-await code
      // reject that attempt instead of reviving the destroyed socket.
      try {
        socket.destroy();
      } catch {
        // ignore
      }
      if (!published) {
        // Never handed out, so there is nothing to report and nobody to
        // report it to — but `closeEmitted` has just muted the native
        // close, so this is also the only thing left that can settle
        // `connect()`. Without it the caller waited out the whole connect
        // timeout on a socket the transport had already destroyed.
        failAttempt?.(cause);
        return;
      }
      this.emit({ type: 'error', cause });
      this.emit({ type: 'close', reason });
    };
    socket.on('data', (chunk) => {
      // Stale generation: decoding, framing, or emitting here would
      // attribute an old socket's bytes to the current connection.
      if (stale()) return;
      let text: string;
      if (chunk instanceof Uint8Array) {
        // One decoder per attempt, carrying any half-received character
        // across chunks: TCP splits at arbitrary byte offsets, so a
        // multi-byte character (a server banner is free-form UTF-8) can
        // straddle two of them.
        decoder ??= new Utf8Stream();
        try {
          text = decoder.decode(chunk);
        } catch (e) {
          failStream(new TransportError('undecodable data from socket', e), 'undecodable data');
          return;
        }
      } else if (typeof chunk === 'string') {
        // A socket shim that decodes for us (some React Native ones do).
        // Nothing to validate here — we were handed characters, not bytes.
        // But a shim that emits both shapes must not interrupt a byte
        // sequence: appending this string while a character is half
        // received would place it BEFORE that character, turning a
        // malformed stream into valid-looking, reordered JSON.
        if (chunk.length > 0 && decoder?.hasPending()) {
          failStream(
            new TransportError('decoded chunk interrupted a partial character'),
            'undecodable data',
          );
          return;
        }
        text = chunk;
      } else {
        // Unknown shape: the bytes it carried are lost, so the framer's
        // view of the stream now has a hole and every later line is
        // suspect. Terminal, like an undecodable WebSocket frame —
        // emitting only `error` would leave the socket published, and
        // ElectrumClient deliberately ignores a standalone transport
        // error, so in-flight requests would hang to their timeouts with
        // no reconnect scheduled.
        failStream(
          new TransportError('unexpected data chunk type from socket'),
          'undecodable data',
        );
        return;
      }
      try {
        // Streamed, not collected: a frame full of short lines would
        // otherwise be held as one string per line before any of them
        // reached a listener.
        framer.pushEach(text, (line) => {
          // A listener may close() on the first line of a multi-line
          // chunk. Nothing from a retired socket may surface afterwards,
          // so re-check per line rather than once per chunk.
          if (stale()) return;
          this.emit({ type: 'data', text: line });
        });
      } catch (e) {
        // Line-length cap exceeded (hostile / broken peer): the stream
        // can't be trusted past this point.
        failStream(
          new TransportError('line length limit exceeded', e),
          'line length limit exceeded',
        );
        return;
      }
    });
    socket.on('close', (hadError) => {
      // Scope to THIS socket generation FIRST: resetting the framer or
      // emitting for a superseded socket would corrupt / disconnect the
      // connection that replaced it.
      if (stale()) return;
      framer.reset();
      if (this.socket !== socket) {
        // The socket died before this attempt published it — including
        // the window between the ready event and the continuation that
        // stores it. Fail the attempt instead of handing the caller a
        // dead connection, and emit nothing: a connection that connect()
        // never exposed has no close to report.
        terminal ??= new TransportError('socket closed during connect');
        this.lifecycle.retire();
        failAttempt?.(terminal);
        return;
      }
      // At most one terminal event per generation — the overflow path
      // already emitted this socket's close.
      if (closeEmitted) return;
      closeEmitted = true;
      // A peer-initiated close ends this socket's life: drop it so the
      // instance is free to connect again (the connect guard treats a
      // stored socket as live, and reconnect must not need close()), and
      // retire the generation so nothing else from this socket surfaces.
      this.socket = null;
      this.lifecycle.retire();
      this.emit({
        type: 'close',
        ...(hadError ? { reason: 'transport error' } : {}),
      });
    });

    let connected = false;
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => {
          this.lifecycle.setAbort(gen, null);
          // Retire BEFORE destroying, for the same reason the connect-error
          // path does: `destroy()` on an injected socket can synchronously
          // flush buffered `data`, which would otherwise still find this
          // generation current and surface as bytes from a connection the
          // caller was about to be told had timed out.
          terminal ??= new TransportError(`connect timeout after ${this.connectTimeoutMs}ms`);
          if (!stale()) this.lifecycle.retire();
          try {
            socket.destroy();
          } catch {
            // ignore
          }
          reject(terminal);
        }, this.connectTimeoutMs);
        this.lifecycle.setAbort(gen, () => {
          this.lifecycle.setAbort(gen, null);
          clearTimeout(timer);
          reject(new TransportError('closed during connect'));
          try {
            socket.destroy();
          } catch {
            // ignore
          }
        });
        failAttempt = (e: TransportError) => {
          this.lifecycle.setAbort(gen, null);
          clearTimeout(timer);
          reject(e);
        };
        socket.on(this.readyEvent, () => {
          this.lifecycle.setAbort(gen, null);
          clearTimeout(timer);
          connected = true;
          resolve();
        });
        socket.on('error', (err) => {
          if (!connected) {
            this.lifecycle.setAbort(gen, null);
            clearTimeout(timer);
            // Retire and destroy HERE, not in the catch that follows the
            // rejection: `reject` only schedules, while an injected
            // socket (the RN / custom `socketFactory` case this type
            // exists for) can emit `data` later in the same turn. That
            // data would still find its generation current and surface
            // as a live connection's bytes after connect() had failed.
            terminal ??= new TransportError('connect error', err);
            // Only if this attempt is still the current one: a late error
            // from a candidate that was already superseded would
            // otherwise retire the generation of the connection that
            // replaced it.
            if (!stale()) this.lifecycle.retire();
            try {
              socket.destroy();
            } catch {
              // ignore
            }
            reject(terminal);
          } else if (!stale()) {
            // A superseded socket's error is not the current connection's.
            this.emit({ type: 'error', cause: err });
          }
        });
      });

      // The attempt may have died while the ready-event continuation was
      // queued (an oversized flood delivered between ready and here, or a
      // close()): publishing it would report a live connection over a
      // destroyed one, and send() would silently write into the void.
      if (terminal) throw terminal;
      if (stale()) throw new TransportError('connect superseded');
      this.socket = socket;
    } catch (e) {
      // A failed attempt must leave nothing behind: kill the candidate and
      // retire its generation so its handlers cannot emit for a caller who
      // was told the connect failed. A close() that superseded us already
      // retired this generation — don't retire the next one on its behalf.
      try {
        socket.destroy();
      } catch {
        // ignore
      }
      if (!stale()) this.lifecycle.retire();
      throw e;
    }
  }

  async send(text: string): Promise<void> {
    if (!this.socket) throw new TransportError('not connected');
    if (text.includes('\n')) {
      throw new TransportError('payload must not contain embedded newline');
    }
    this.socket.write(text + '\n');
  }

  async close(): Promise<void> {
    return this.lifecycle.close(async () => {
      const socket = this.socket;
      if (!socket) return;
      this.socket = null;
      await new Promise<void>((resolve) => {
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
        // idempotent.
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
    });
  }

  on(listener: TransportListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  private emit(ev: TransportEvent): void {
    // Snapshot: a Set iterator revisits an entry removed and re-added
    // while it runs, so a listener that unsubscribes and resubscribes
    // itself from inside its own callback was called again, forever.
    for (const l of [...this.listeners]) {
      try {
        l(ev);
      } catch {
        // A consumer callback must not break the transport: swallowing
        // here keeps the remaining listeners (and the socket's own
        // teardown) running. Listener bugs surface through the
        // manager's `error` event, which wraps its own callbacks.
      }
    }
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
