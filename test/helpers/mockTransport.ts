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
  /**
   * Auto-reply to `server.version` requests with a synthetic version
   * tuple instead of enqueueing them in `sent[]`. Default true so
   * Manager-level tests don't have to reserve a slot for the
   * implicit handshake `installServer` issues on every connect. Set
   * to false in tests that drive `server.version` directly.
   */
  autoReplyVersion = true;
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
    if (this.autoReplyVersion) {
      // Manager issues a `server.version` handshake on every connect
      // (see `installServer`). Auto-reply with a synthetic version
      // tuple and DON'T enqueue the request in `sent[]` — tests
      // assert on the wire calls they actually drive without having
      // to reserve a slot for the handshake. Tests that exercise
      // version directly set `autoReplyVersion = false`.
      try {
        const parsed = JSON.parse(text) as { id?: number; method?: string };
        if (parsed && parsed.method === 'server.version' && typeof parsed.id === 'number') {
          const reply = JSON.stringify({ id: parsed.id, result: ['MockServer 0.0.0', '1.4'] });
          // Microtask reply so client.send().then() resolves before
          // the response handler runs.
          queueMicrotask(() => this.emit({ type: 'data', text: reply }));
          return;
        }
      } catch {
        // Fall through and treat as a normal send.
      }
    }
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
