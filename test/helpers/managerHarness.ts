// Helpers for Manager unit tests. Builds a transportFactory that registers a
// MockTransport per ServerSpec.id and lets tests drive each server's wire I/O.

import type { Endpoint } from '../../src/client.js';
import type { Transport } from '../../src/transport/types.js';

import { MockTransport } from './mockTransport.js';

export interface Harness {
  transports: Map<string, MockTransport>;
  factory: (endpoint: Endpoint) => Transport;
  /** Resolve all queued sends for one server with a typed JSON-RPC response array or single. */
  reply(host: string, build: (req: unknown) => unknown): void;
}

export function buildHarness(): Harness {
  const transports = new Map<string, MockTransport>();
  const factory = (endpoint: Endpoint): Transport => {
    const t = new MockTransport(endpoint);
    transports.set(endpoint.host, t);
    return t;
  };
  const reply = (host: string, build: (req: unknown) => unknown): void => {
    const t = transports.get(host);
    if (!t) throw new Error(`no transport for host=${host}`);
    while (t.sent.length > 0) {
      const text = t.sent.shift()!;
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        const responses = parsed.map((r) => build(r)).filter((r): r is unknown => r !== undefined);
        if (responses.length > 0) t.pushFromServer(JSON.stringify(responses));
      } else {
        const out = build(parsed);
        if (out !== undefined) t.pushFromServer(JSON.stringify(out));
      }
    }
  };
  return { transports, factory, reply };
}
