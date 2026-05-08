// Synchronous in-process Transport for ElectrumClient unit tests.
// Test code drives the conversation with `pushFromServer`, `pushClose`, etc.

import type { Endpoint } from '../../src/client.js';
import type { Transport, TransportEvent, TransportListener } from '../../src/transport/types.js';

export class MockTransport implements Transport {
  readonly endpoint: Endpoint;
  readonly sent: string[] = [];
  connected = false;
  private readonly listeners = new Set<TransportListener>();
  private readonly enc = new TextEncoder();
  private readonly dec = new TextDecoder();

  constructor(endpoint: Endpoint = { host: 'mock', port: 0, protocol: 'ws' }) {
    this.endpoint = endpoint;
  }

  async connect(): Promise<void> {
    this.connected = true;
  }

  async send(bytes: Uint8Array): Promise<void> {
    this.sent.push(this.dec.decode(bytes));
  }

  async close(): Promise<void> {
    this.connected = false;
  }

  on(listener: TransportListener): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  // --- Test helpers ---

  pushFromServer(text: string): void {
    this.emit({ type: 'data', bytes: this.enc.encode(text) });
  }

  pushClose(code = 1000, reason = ''): void {
    const ev: TransportEvent = {
      type: 'close',
      code,
      ...(reason ? { reason } : {}),
    };
    this.emit(ev);
  }

  pushError(cause: unknown): void {
    this.emit({ type: 'error', cause });
  }

  private emit(ev: TransportEvent): void {
    for (const l of this.listeners) l(ev);
  }
}
