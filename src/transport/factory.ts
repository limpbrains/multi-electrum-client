// Transport registry. Each `Transport` implementation self-registers on
// import; `ElectrumManager`'s default factory looks up by `endpoint.protocol`
// without importing transport modules itself.
//
// Why a registry: it lets the package ship two entry points (Node / RN /
// Bun via `src/index.ts`, browser via `src/index.browser.ts`) that import
// different subsets of the transport modules. The browser entry registers
// only `ws` / `wss`, so a browser bundler never has to resolve `node:net`
// or `node:tls`. Manager.ts depends only on `factory.ts`, not on any
// concrete transport, so adding or removing a transport is a one-line
// import change in the entry file.

import type { Endpoint } from '../client.js';
import { ProtocolError } from '../errors/types.js';

import type { Transport } from './types.js';

type TransportCtor = (endpoint: Endpoint) => Transport;
const registry = new Map<string, TransportCtor>();

/**
 * Register a transport for a given protocol scheme. Subsequent calls with
 * the same protocol overwrite the prior entry; tests and embedders may
 * override registered transports for fault injection.
 */
export function registerTransport(protocol: string, ctor: TransportCtor): void {
  registry.set(protocol, ctor);
}

/** Currently-registered protocol schemes. Useful for debug logging. */
export function registeredProtocols(): readonly string[] {
  return [...registry.keys()];
}

/**
 * Manager's default transport factory. Throws `ProtocolError` if the
 * server's protocol is not registered. Browser bundles register only
 * `ws` / `wss`; calling with `'tcp'` or `'tls'` throws clearly rather
 * than failing at module-resolve time.
 */
export function defaultTransportFactory(endpoint: Endpoint): Transport {
  const ctor = registry.get(endpoint.protocol);
  if (!ctor) {
    throw new ProtocolError(
      `no transport registered for protocol '${endpoint.protocol}'; registered: [${registeredProtocols().join(', ')}]`,
    );
  }
  return ctor(endpoint);
}
