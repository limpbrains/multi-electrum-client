// Transport abstraction.
//
// Electrum is a text JSON-RPC protocol, so the transport layer carries strings.
// Each `data` event carries one logical message: WS transports split incoming
// frames on `\r?\n` and emit one event per line; TCP/TLS (M6) will use the
// same line-framing layer.

import type { Endpoint } from '../client.js';

export type TransportEvent =
  | { type: 'data'; text: string }
  | { type: 'close'; code?: number; reason?: string }
  | { type: 'error'; cause: unknown };

export type TransportListener = (event: TransportEvent) => void;

/**
 * One transport instance owns at most one live socket.
 *
 *  - `connect()` rejects while another connect is in flight or a socket
 *    is already open; it waits out a close that is already underway, so
 *    `await close(); connect()` is always valid.
 *  - `close()` is idempotent — concurrent callers share one teardown —
 *    and aborts an in-flight connect before tearing down.
 *  - A socket is retired when it is replaced, closed, or its connect
 *    fails: nothing it does can surface afterwards. A self-initiated
 *    `close()` therefore emits no `close` event (the caller knows); a
 *    peer-initiated close does, and frees the instance to connect again.
 *  - A failure emits `error` then `close`, in that order, to every
 *    listener. `close` is the TERMINAL signal: drive teardown and
 *    reconnects from it, not from `error` — `ElectrumClient` itself
 *    deliberately ignores a standalone `error`. Reconnecting from
 *    inside the `error` handler is out of contract: the failure's own
 *    `close` still follows and would read as the replacement dying.
 */
export interface Transport {
  endpoint: Endpoint;
  connect(): Promise<void>;
  /** Send one logical message. Implementations append framing (e.g. `\n`) as needed. */
  send(text: string): Promise<void>;
  close(): Promise<void>;
  on(listener: TransportListener): () => void;
}
