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
import { setUnrefTimeout } from '../util/timers.js';
import { TransportLifecycle } from './lifecycle.js';
import { ListenerSet } from '../util/listeners.js';
import {
  assertMaxLineLength,
  DEFAULT_MAX_LINE_LENGTH,
  LineFramer,
  LineTooLongError,
} from './lineFramer.js';
import type { Transport, TransportEvent, TransportListener } from './types.js';
import { asBytes, Utf8Stream } from './utf8.js';

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
  /**
   * Cap on one logical line / the retained partial buffer (default
   * 32 MiB — sized to the protocol's largest valid response, see
   * `DEFAULT_MAX_LINE_LENGTH`). Bounds a malicious or broken server
   * streaming newline-free data; on overflow the transport emits an
   * `error` event and closes. `endpoint.maxLineLength` (the per-server
   * declaration) takes precedence over this construction-wide value.
   *
   * Caveat: this bounds what the FRAMER retains, not what the platform
   * receives — the WebSocket API materializes each complete message
   * before delivering it, so a single oversized message is allocated
   * once by the runtime regardless; the cap then fails it loudly
   * (before the UTF-8 decode would allocate a second copy) instead of
   * accumulating further. For a pre-delivery bound, configure the
   * injected WebSocket implementation itself (e.g. `maxPayload` in the
   * `ws` package).
   */
  maxLineLength?: number;
}

export class WsTransport implements Transport {
  readonly endpoint: Endpoint;
  private ws: WebSocket | null = null;
  private readonly listeners = new ListenerSet<TransportEvent>();
  private readonly Ctor: WebSocketCtor;
  private readonly connectTimeoutMs: number;
  /**
   * Serializes connect / close and stamps each attempt with a generation
   * so a superseded socket's handlers go inert. See ./lifecycle.ts.
   */
  private readonly lifecycle = new TransportLifecycle();
  private readonly maxLineLength: number;
  /** Newline-mode aggregate message bound; see `Endpoint.maxMessageLength`. */
  private readonly maxMessageLength: number;
  /**
   * Chars of edge-terminator padding tolerated on top of the cap in
   * message mode — room for a payload's own terminator plus a few
   * coalesced CRLF-CRLF keepalives from a sloppy peer, small enough
   * that padding cannot become a size exemption.
   */
  private static readonly PADDING_ALLOWANCE = 16;
  /**
   * Floor of the DEFAULT newline-mode aggregate message bound (the
   * default itself scales as 4x the line cap). RFC 6455 ties messages
   * to no size and an aggregating bridge may legally coalesce several
   * near-cap responses into one message — hence the scaling — while
   * the bound cannot be absent: native browser WebSocket exposes no
   * `maxPayload`, so without it a browser wallet would decode and
   * dispatch an arbitrarily large flood of tiny lines in one
   * synchronous storm. `Endpoint.maxMessageLength` overrides the
   * default outright for bridges whose coalescing exceeds it.
   */
  private static readonly NEWLINE_MESSAGE_BOUND_FLOOR = 8 * 1024 * 1024;
  /** See `Endpoint.wsFraming`. */
  private readonly framing: 'message' | 'newline';
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
    this.framing = opts.endpoint.wsFraming ?? 'message';
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
    this.maxLineLength = maxLineLength ?? DEFAULT_MAX_LINE_LENGTH;
    if (opts.endpoint.maxMessageLength !== undefined) {
      assertMaxLineLength(opts.endpoint.maxMessageLength, 'maxMessageLength');
      // An aggregate bound below the per-line cap would tear the
      // connection down on every legal single-line response — a
      // misconfiguration, rejected where it is written (the +1 leaves
      // room for the line's terminator).
      if (opts.endpoint.maxMessageLength < this.maxLineLength + 1) {
        throw new RangeError(
          `maxMessageLength (${opts.endpoint.maxMessageLength}) must be at least maxLineLength + 1 (${this.maxLineLength + 1})`,
        );
      }
    }
    // Default: four full-cap lines WITH their terminators (the padding
    // allowance doubles as per-line terminator headroom), floored at
    // 8 MiB — ~128 MiB at the default 32 MiB cap. Memory-sensitive
    // deployments lower it per server via `maxMessageLength`.
    this.maxMessageLength =
      opts.endpoint.maxMessageLength ??
      Math.max(
        4 * (this.maxLineLength + WsTransport.PADDING_ALLOWANCE),
        WsTransport.NEWLINE_MESSAGE_BOUND_FLOOR,
      );
  }

  async connect(): Promise<void> {
    return this.lifecycle.connect(
      () => this.ws !== null,
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
    const scheme = this.endpoint.protocol === 'wss' ? 'wss' : 'ws';
    const path = this.endpoint.path ?? '';
    const url = `${scheme}://${this.endpoint.host}:${this.endpoint.port}${path}`;

    const ws = new this.Ctor(url);
    ws.binaryType = 'arraybuffer';

    // Per-attempt state. These handlers outlive their connect() call and
    // can fire for a socket generation that is no longer active, so they
    // close over these instead of only consulting shared fields.
    let connected = false;
    let closeEmitted = false;
    let terminal: TransportError | undefined;
    // One decoder per attempt, created lazily on the first binary frame:
    // Hermes (React Native) has no global TextDecoder, and constructing
    // one eagerly made `connect()` throw there even though Electrum
    // servers speak text frames. Decoding runs in streaming mode — a
    // multi-byte character split across two binary frames would
    // otherwise decode to replacement characters in both halves and
    // corrupt the JSON line carrying it.
    let decoder: Utf8Stream | undefined;
    const toText = (data: unknown): string | undefined => {
      if (typeof data === 'string') {
        // An empty text frame (a keepalive, say) carries nothing and so
        // cannot reorder anything — it must not end a binary sequence
        // that is legitimately split across the frames around it.
        if (data === '') return data;
        // A non-empty text frame ends whatever byte stream the decoder
        // was in the middle of: WebSocket permits mixing frame types,
        // and the held bytes must not later merge with a subsequent
        // binary frame — that would place the completed character AFTER
        // text that came earlier on the wire. A half-received character
        // at this point means the byte stream was truncated.
        if (decoder?.hasPending()) return undefined;
        decoder = undefined;
        return data;
      }
      const bytes = asBytes(data);
      if (bytes === undefined) return undefined;
      decoder ??= new Utf8Stream();
      try {
        return decoder.decode(bytes);
      } catch {
        return undefined;
      }
    };
    /** Set by the connect promise; fails this attempt from a handler. */
    let failAttempt: ((e: TransportError) => void) | undefined;

    /**
     * Physical teardown of THIS socket, never via `close()`: a failure can
     * arrive between the 'open' event and the `this.ws = ws` assignment
     * below, where `close()` would see a null field, no-op, and leave the
     * socket open — exactly the connection a hostile peer wants kept
     * alive. The backstop matters because `ws.close()` is a cooperative
     * handshake: a peer that never answers it (or a shim whose close is a
     * no-op before the connection is established) would otherwise leave
     * the socket alive, and a reconnect loop leaks one per attempt.
     */
    const destroySocket = (): void => {
      try {
        ws.close();
      } catch {
        // ignore
      }
      setUnrefTimeout(() => {
        try {
          (ws as { terminate?: () => void }).terminate?.();
        } catch {
          // ignore
        }
      }, 500);
    };

    /**
     * End this connection on unusable protocol data.
     *
     * For a PUBLISHED socket the close event is emitted here explicitly so
     * the caller's in-flight requests fail over NOW rather than waiting
     * out their timeouts; `closeEmitted` then suppresses the native
     * handshake's own close so listeners see exactly one terminal event
     * (WsTransport is a public export — its contract must not depend on a
     * consumer detaching after the first one). For a socket connect()
     * never handed out, the attempt is failed and nothing is emitted.
     */
    const failStream = (cause: TransportError, reason: string): void => {
      terminal = cause;
      closeEmitted = true;
      const published = this.ws === ws;
      // Order matters: retire, unpublish and start the physical teardown
      // BEFORE calling any listener. Emitting first would let a throwing
      // consumer callback strand a retired-but-still-stored socket — no
      // native event could finish the cleanup (they are all stale by
      // then) and connect() would refuse to replace it.
      this.lifecycle.retire();
      if (published) this.ws = null;
      destroySocket();
      if (!published) {
        // The attempt never became a connection anyone holds. Fail it —
        // `closeEmitted` has just muted the native close, so nothing else
        // would ever settle `connect()`, and it would sit out its whole
        // timeout while the transport had already declared the socket
        // dead. Emit nothing, for the same reason the native close
        // listener stays silent here: listeners never saw this open.
        failAttempt?.(cause);
        return;
      }
      this.emit({ type: 'error', cause });
      this.emit({ type: 'close', reason });
    };

    // Attach data handlers synchronously, BEFORE awaiting open, so we don't
    // drop frames the server may send immediately on its connection handler.
    ws.addEventListener('message', (ev) => {
      // Stale generation: decoding, framing, or emitting here would
      // attribute an old socket's bytes to the current connection.
      if (stale()) return;
      const raw = (ev as MessageEvent).data;
      // One failure shape for every size rejection, wherever it trips —
      // consumers classify on the LineTooLongError cause. The cause
      // names the bound that was actually violated: the aggregate
      // message bound and the per-line cap are separate options, and
      // blaming the wrong one sends an operator tuning a knob that
      // will not help.
      const failLineTooLong = (
        detail: string,
        name: 'maxLineLength' | 'maxMessageLength' = 'maxLineLength',
        bound = this.maxLineLength,
      ): void => {
        failStream(
          new TransportError(
            'line length limit exceeded',
            new LineTooLongError(`${detail} exceeds ${name} (${bound})`),
          ),
          'line length limit exceeded',
        );
      };
      const failMessageBound = (detail: string): void => {
        if (this.framing === 'message') failLineTooLong(detail);
        else failLineTooLong(detail, 'maxMessageLength', this.maxMessageLength);
      };
      // Pre-decode gates. The platform has already materialized the
      // message (RFC 6455 allows arbitrarily large reassembled
      // messages, and the WebSocket API constructs the DOMString /
      // ArrayBuffer before firing 'message' — the pre-delivery bound on
      // THAT is the injected WebSocket implementation's own maxPayload,
      // e.g. in the `ws` package). What we can still avoid is the UTF-8
      // decode's second full copy, when the payload is provably
      // hopeless. A binary payload of N bytes decodes to at least
      // ceil(N/3) UTF-16 units (worst case: 3-byte characters):
      //  - message mode: one payload per message, so the cap (plus the
      //    padding allowance) bounds the whole message;
      //  - newline mode: the message aggregates lines and RFC 6455 ties
      //    it to no size (an aggregating bridge may legally pack many
      //    near-cap responses into one message), so only a payload with
      //    NO newline byte — one line by construction, 0x0a never
      //    occurs inside a UTF-8 multi-byte sequence — can be judged
      //    here, against the per-line cap (less one unit for a
      //    possible trailing CR). Everything else is the framer's job,
      //    line by line.
      const messageBound =
        this.framing === 'message'
          ? this.maxLineLength + WsTransport.PADDING_ALLOWANCE
          : this.maxMessageLength;
      if (typeof raw === 'string') {
        if (raw.length > messageBound) {
          failMessageBound(`message of ${raw.length} chars`);
          return;
        }
      } else {
        const gateBytes = asBytes(raw);
        if (gateBytes !== undefined) {
          const minUnits = Math.ceil(gateBytes.byteLength / 3);
          if (minUnits > messageBound) {
            failMessageBound(`message of at least ${minUnits} chars`);
            return;
          }
          if (this.framing === 'newline' && minUnits - 1 > this.maxLineLength) {
            // Only worth the O(N) newline scan once the size test says a
            // single line of this payload would be hopeless.
            if (!gateBytes.includes(0x0a)) {
              failLineTooLong(`single-line message of at least ${minUnits - 1} chars`);
              return;
            }
          }
        }
      }
      const text = toText(raw);
      if (text === undefined) {
        // Undecodable frame (unknown data type, or binary data on a
        // runtime without TextDecoder). This is terminal, not an
        // observation: a frame we cannot read may have carried a
        // response, and the framer's line boundaries are now unknowable
        // either way. Emitting only `error` would leave the socket open,
        // and ElectrumClient deliberately ignores a standalone transport
        // error — so every in-flight request would hang to its timeout
        // with no reconnect scheduled.
        failStream(new TransportError('undecodable message data from socket'), 'undecodable data');
        return;
      }
      // Re-check the bound on the DECODED length: ceil(N/3) is only
      // the minimum a binary payload can decode to — mostly-ASCII
      // bytes decode 1:1, so a binary message up to 3x the bound
      // passes the pre-decode gate; without this an all-padding binary
      // flood was silently swallowed after a full char-by-char scan.
      if (text.length > messageBound) {
        failMessageBound(`message of ${text.length} chars`);
        return;
      }
      try {
        // Streamed, not collected: a frame full of short lines would
        // otherwise be held as one string per line before any of them
        // reached a listener.
        const onLine = (line: string): void => {
          // A listener may close() on the first line of a multi-line
          // chunk. Nothing from a retired socket may surface afterwards,
          // so re-check per line rather than once per chunk.
          if (stale()) return;
          this.emit({ type: 'data', text: line });
        };
        if (this.framing === 'message') {
          // Native Electrum-over-WebSocket sends one complete JSON-RPC
          // payload per message with NO trailing newline — the message
          // is the framing unit, so the payload is taken whole and the
          // cap applies to the MESSAGE. It must not go through the
          // newline framer: splitting on interior newlines would accept
          // framing the protocol forbids AND unbound the cap — one huge
          // message of millions of tiny newline-joined lines passes
          // every per-line check while the runtime holds the whole
          // message. (A byte tunnel — 'newline' framing — is the
          // opposite: its boundaries are arbitrary TCP fragmentation
          // and ONLY the framer can reassemble them. Declared per
          // endpoint; traffic cannot tell the two apart.)
          //
          // A message-framed peer also never splits a CHARACTER across
          // messages — bytes still held by the decoder here mean the
          // payload is truncated, and flushing would emit the broken
          // prefix as data while the tail leaked into the next payload.
          // Fail loudly instead, as the text-frame path already does.
          if (decoder?.hasPending()) {
            failStream(
              new TransportError('binary message ended mid-character'),
              'undecodable data',
            );
            return;
          }
          // The raw message is already bounded by the gate above (cap
          // plus padding allowance), so the trims below scan a bounded
          // run. Tolerate terminator RUNS at either edge, the way the
          // wire framer would: empty lines are keepalives there, and
          // their equivalent here is padding — '{...}\n\n' or a
          // CRLF-CRLF keepalive message is one payload (or none).
          let end = text.length;
          while (end > 0) {
            const cc = text.charCodeAt(end - 1);
            if (cc === 10 || cc === 13) end--;
            else break;
          }
          let startIdx = 0;
          while (startIdx < end) {
            const cc = text.charCodeAt(startIdx);
            if (cc === 10 || cc === 13) startIdx++;
            else break;
          }
          const payload = text.slice(startIdx, end);
          if (payload.length > this.maxLineLength) {
            failLineTooLong(`message of ${payload.length} chars`);
            return;
          }
          // Emitted WHOLE, interior newlines and all: RFC 8259 permits
          // LF as insignificant whitespace, so a compliant server may
          // pretty-print a response. The client's JSON decoder is the
          // validator — a message packing several newline-separated
          // roots fails there as one malformed value, surfacing as a
          // ProtocolError (telemetry-visible; routing deprioritizes the
          // peer) while requests to it run out their timeouts. The
          // connection is deliberately NOT torn down here: the
          // transport cannot tell a pretty-printed payload from a
          // multi-root one without parsing JSON, which is not its job.
          if (payload.length > 0) onLine(payload);
        } else {
          framer.pushEach(text, onLine);
        }
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

    ws.addEventListener('close', (ev) => {
      // Suppress close events from a connect that never completed
      // (timeout / abort / handshake error): the connect promise has
      // already rejected, a stray 'close' would surface to listeners as
      // an event on a connection they never saw open. `closeEmitted`
      // keeps it to one terminal event per generation (the overflow path
      // emits its own before starting the handshake).
      if (closeEmitted) return;
      // Scope to THIS socket generation. Identity against `this.ws` is
      // not enough: after close() hits its backstop `this.ws` is null,
      // and it stays null while a replacement connect is in flight — a
      // late close from the dead socket would land in that window and
      // read as a terminal event for the new connection.
      if (stale()) return;
      if (this.ws !== ws) {
        // Not published (yet): a close before `open`, or in the window
        // between `open` and the continuation that stores the socket.
        // Fail the attempt rather than hand back a dead connection, and
        // emit nothing — connect() never exposed this socket.
        terminal ??= new TransportError(
          connected ? 'socket closed during connect' : 'closed before open',
        );
        this.lifecycle.retire();
        failAttempt?.(terminal);
        return;
      }
      closeEmitted = true;
      // A peer-initiated close ends this socket's life: drop it so the
      // instance is free to connect again (the connect guard treats a
      // stored socket as live, and reconnect must not need close()), and
      // retire the generation so nothing else from this socket surfaces.
      this.ws = null;
      this.lifecycle.retire();
      const ce = ev as CloseEvent;
      const out: TransportEvent = {
        type: 'close',
        ...(typeof ce.code === 'number' ? { code: ce.code } : {}),
        ...(typeof ce.reason === 'string' && ce.reason.length > 0 ? { reason: ce.reason } : {}),
      };
      this.emit(out);
    });

    try {
      await new Promise<void>((resolve, reject) => {
        let settled = false;
        /**
         * End this attempt: one path for the timeout, the abort hook and a
         * pre-open error, because three hand-written copies is how the
         * teardown steps drifted apart in the first place.
         *
         * `settled` also makes it re-entrant-safe: tearing the socket down
         * can make a shim emit `error` synchronously, and the `error`
         * listener cannot be `once` (it also forwards post-open errors),
         * so without the flag that would recurse until the stack ran out.
         */
        const failConnect = (cause: TransportError, teardown: boolean): void => {
          if (settled) return;
          settled = true;
          this.lifecycle.setAbort(gen, null);
          clearTimeout(timer);
          terminal ??= cause;
          // Retire before tearing down: a socket that delivers a queued
          // frame as it goes down would otherwise still find this
          // generation current, and its bytes would surface as data from
          // a connection the caller is being told it never got. Only if
          // the attempt is still current — a close() that superseded us
          // already retired it, and retiring again would take the
          // generation of whatever replaced us.
          if (!stale()) this.lifecycle.retire();
          if (teardown) destroySocket();
          reject(terminal);
        };
        const timer = setTimeout(() => {
          failConnect(new TransportError(`connect timeout after ${this.connectTimeoutMs}ms`), true);
        }, this.connectTimeoutMs);
        this.lifecycle.setAbort(gen, () => {
          failConnect(new TransportError('closed during connect'), true);
        });
        failAttempt = (e: TransportError) => {
          // The caller (a handler that already tore the socket down)
          // owns the teardown here; we only settle the promise.
          failConnect(e, false);
        };
        ws.addEventListener(
          'open',
          () => {
            this.lifecycle.setAbort(gen, null);
            clearTimeout(timer);
            settled = true;
            connected = true;
            resolve();
          },
          { once: true },
        );
        ws.addEventListener('error', (ev) => {
          if (!connected) {
            failConnect(new TransportError('connect error', ev), true);
          } else if (!stale()) {
            // A superseded socket's error is not the current connection's.
            // TODO(M4): wrap with readyState + any platform-specific detail
            // before forwarding so the classifier has more to work with.
            this.emit({ type: 'error', cause: ev });
          }
        });
      });

      // The attempt may have died while the open-event continuation was
      // queued (an oversized flood delivered between open and here, or a
      // close()): publishing it would report a live connection over a
      // closed one, and send() would write into the void.
      if (terminal) throw terminal;
      if (stale()) throw new TransportError('connect superseded');
      this.ws = ws;
    } catch (e) {
      // A failed attempt must leave nothing behind: close the candidate
      // and retire its generation so its handlers cannot emit for a caller
      // who was told the connect failed. A close() that superseded us
      // already retired this generation — don't retire the next one.
      try {
        ws.close();
      } catch {
        // ignore
      }
      if (!stale()) this.lifecycle.retire();
      throw e;
    }
  }

  async send(text: string): Promise<void> {
    if (!this.ws) throw new TransportError('not connected');
    // An embedded newline in the payload would corrupt framing on the
    // server side. JSON.stringify (the only intended caller via
    // ElectrumClient) escapes newlines, so this guard only fires for
    // misuse.
    if (text.includes('\n')) {
      throw new TransportError('payload must not contain embedded newline');
    }
    // Outbound framing mirrors inbound: a native message-framed server
    // speaks one bare JSON payload per message (a strict per-message
    // parser may reject trailing bytes), while a byte tunnel needs the
    // newline the TCP server behind it delimits with.
    this.ws.send(this.framing === 'message' ? text : text + '\n');
  }

  async close(): Promise<void> {
    return this.lifecycle.close(async () => {
      const ws = this.ws;
      if (!ws) return;
      this.ws = null;
      if (ws.readyState === ws.CLOSED) return;
      await new Promise<void>((resolve) => {
        let settled = false;
        const settle = (): void => {
          if (settled) return;
          settled = true;
          clearTimeout(backstop);
          ws.removeEventListener('close', onClose);
          resolve();
        };
        const onClose = () => {
          settle();
        };
        // Backstop: a dead or non-compliant peer may never complete the
        // close handshake, and stop()/suspend() await this — one bad WSS
        // peer must not park the whole lifecycle. Mirror TcpTransport's
        // 500ms bound; force-terminate where the runtime supports it
        // (the `ws` package), otherwise just abandon the socket.
        const backstop = setTimeout(() => {
          try {
            (ws as { terminate?: () => void }).terminate?.();
          } catch {
            // ignore
          }
          settle();
        }, 500);
        ws.addEventListener('close', onClose);
        try {
          ws.close();
        } catch {
          // Route through settle(): resolving directly would leave the
          // backstop armed to terminate() the socket half a second after
          // the caller believes teardown finished.
          settle();
        }
      });
    });
  }

  on(listener: TransportListener): () => void {
    return this.listeners.add(listener);
  }

  private emit(ev: TransportEvent): void {
    // Revisit safety, throwing-listener isolation and copy-on-write
    // snapshots all live in ListenerSet — shared with the other
    // transport so the next emitter cannot fork the semantics.
    this.listeners.emit(ev);
  }
}

// Self-register so `defaultTransportFactory` finds us by protocol.
registerTransport('ws', (endpoint) => new WsTransport({ endpoint }));
registerTransport('wss', (endpoint) => new WsTransport({ endpoint }));
