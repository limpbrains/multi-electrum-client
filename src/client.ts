// ElectrumClient — single connection to one server.
// M0: type stubs only. Implementation in M1.

import type { ErrorKind } from './errors/types.js';

export type ClientId = string;

export type Protocol = 'ws' | 'tcp' | 'tls';

export interface Endpoint {
  host: string;
  port: number;
  protocol: Protocol;
}

export type ConnectionState = 'connecting' | 'connected' | 'disconnected' | 'banned';

export interface Telemetry {
  latency: { ema: number; p50: number; p95: number; samples: number };
  errors: {
    rate: number;
    lastKind?: ErrorKind;
    lastAt?: number;
    consecutive: number;
  };
  success: { count: number; lastAt?: number };
  inFlight: number;
  connectedSince?: number;
}

/** Read-only snapshot of a Client's state, given to RoutingPolicy. */
export interface ClientView {
  id: ClientId;
  endpoint: Endpoint;
  state: ConnectionState;
  bannedUntil?: number;
  capabilities: { serverSoftware?: string; protocolVersion?: string };
  telemetry: Telemetry;
}
