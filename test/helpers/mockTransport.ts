// Synchronous in-process Transport for ElectrumClient unit tests.
// Test code drives the conversation with `pushFromServer`, `pushClose`, etc.

import type { Endpoint } from '../../src/client.js';
import type { Transport, TransportEvent, TransportListener } from '../../src/transport/types.js';

export class MockTransport implements Transport {
  readonly endpoint: Endpoint;
  readonly sent: string[] = [];
  connected = false;
  /** Total `connect()` calls across transport lifetime (incl. reconnects). */
  connectCalls = 0;
  /** Override next `connect()` to throw — simulate failed reconnect. */
  nextConnectError: Error | null = null;
  private readonly listeners = new Set<TransportListener>();

  constructor(endpoint: Endpoint = { host: 'mock', port: 0, protocol: 'ws' }) {
    this.endpoint = endpoint;
  }

  async connect(): Promise<void> {
    this.connectCalls++;
    if (this.nextConnectError) {
      const e = this.nextConnectError;
      this.nextConnectError = null;
      throw e;
    }
    this.connected = true;
  }

  async send(text: string): Promise<void> {
    this.sent.push(text);
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
    this.emit({ type: 'data', text });
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
