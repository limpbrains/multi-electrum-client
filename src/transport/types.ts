// Transport abstraction. M0: signatures only.

import type { Endpoint } from '../client.js';

export type TransportEvent =
  | { type: 'data'; bytes: Uint8Array }
  | { type: 'close'; code?: number; reason?: string }
  | { type: 'error'; cause: unknown };

export type TransportListener = (event: TransportEvent) => void;

export interface Transport {
  endpoint: Endpoint;
  connect(): Promise<void>;
  send(bytes: Uint8Array): Promise<void>;
  close(): Promise<void>;
  on(listener: TransportListener): () => void;
}
