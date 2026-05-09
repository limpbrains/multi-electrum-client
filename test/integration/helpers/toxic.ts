// Thin wrapper around toxiproxy's admin HTTP API. We only need the bits
// that the integration suite uses: enable / disable a proxy, attach /
// remove toxics, and reset state between tests. Toxiproxy's REST API is
// stable, so we hand-roll rather than pulling in a client library.
//
// Reference: https://github.com/Shopify/toxiproxy#http-api

import { INTEGRATION_HOST, PORTS } from './config.js';

const ADMIN = `http://${INTEGRATION_HOST}:${PORTS.toxiproxyAdmin}`;

export interface ToxicSpec {
  /** Toxic name. Tests use stable names so we can remove by name later. */
  name: string;
  /** Toxic type — see toxiproxy docs. Common: 'latency', 'bandwidth', 'down', 'timeout'. */
  type: string;
  /** Direction. 'downstream' = server→client, 'upstream' = client→server. Default 'downstream'. */
  stream?: 'upstream' | 'downstream';
  /** Probability the toxic applies to a given connection. Default 1.0. */
  toxicity?: number;
  /** Type-specific knobs (e.g. `{ latency: 1000, jitter: 100 }`). */
  attributes?: Record<string, number | string>;
}

async function api(
  method: 'GET' | 'POST' | 'DELETE',
  path: string,
  body?: unknown,
): Promise<unknown> {
  const res = await fetch(`${ADMIN}${path}`, {
    method,
    headers: body !== undefined ? { 'content-type': 'application/json' } : {},
    body: body !== undefined ? JSON.stringify(body) : null,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`toxiproxy ${method} ${path} → ${res.status}: ${text}`);
  }
  // 204 etc. — no body.
  const text = await res.text();
  return text.length > 0 ? JSON.parse(text) : undefined;
}

/** Disable a proxy entirely. Existing connections are dropped. */
export async function disable(proxyName: string): Promise<void> {
  await api('POST', `/proxies/${proxyName}`, { enabled: false });
}

/** Re-enable a proxy. Idempotent on already-enabled. */
export async function enable(proxyName: string): Promise<void> {
  await api('POST', `/proxies/${proxyName}`, { enabled: true });
}

/** Add a toxic to a proxy. */
export async function addToxic(proxyName: string, spec: ToxicSpec): Promise<void> {
  await api('POST', `/proxies/${proxyName}/toxics`, {
    name: spec.name,
    type: spec.type,
    stream: spec.stream ?? 'downstream',
    toxicity: spec.toxicity ?? 1.0,
    attributes: spec.attributes ?? {},
  });
}

/** Remove a toxic from a proxy. Idempotent — 404 is treated as already-gone. */
export async function removeToxic(proxyName: string, toxicName: string): Promise<void> {
  try {
    await api('DELETE', `/proxies/${proxyName}/toxics/${toxicName}`);
  } catch (e) {
    // Toxic may already be gone (parallel cleanup, prior failed run).
    if (e instanceof Error && e.message.includes('404')) return;
    throw e;
  }
}

/** Reset every proxy + remove all toxics. Tests call this in `beforeEach`. */
export async function reset(): Promise<void> {
  await api('POST', '/reset');
}
