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

export interface Transport {
  endpoint: Endpoint;
  connect(): Promise<void>;
  /** Send one logical message. Implementations append framing (e.g. `\n`) as needed. */
  send(text: string): Promise<void>;
  close(): Promise<void>;
  on(listener: TransportListener): () => void;
}
