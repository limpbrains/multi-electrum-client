// Peer discovery — parses `server.peers.subscribe` responses into ServerSpec
// candidates the manager can decide to admit. The wire shape is loose
// (Electrum's protocol doc only sketches it), so the parser is permissive
// and skips entries we don't understand.

import type { ClientId, Protocol } from './client.js';
import type { ServerSpec } from './protocol/types.js';

/**
 * Wire shape: `[host, ip, [feature, ...]]`. Some servers omit `ip` or pad
 * with extra elements; we only read the first and third positions. Features
 * are strings like:
 *  - `"v1.4"` — protocol version
 *  - `"s50002"` — SSL/TLS port
 *  - `"t50001"` — plain TCP port
 *  - `"ws:50001"` / `"wss:50002"` — WebSocket / WebSocket-TLS (rare,
 *    non-standard, but some servers expose them)
 *
 * The MVP transport surface is `ws` only, with `wss` added in M6 alongside
 * TCP/TLS. Until then we admit `wss` peers (browsers / RN / Bun ship a
 * global WebSocket that handles both schemes via `WsTransport`) and skip
 * everything else. The caller's `onDiscover` callback can override.
 */

const WS_FEATURE_RX = /^(wss?):([0-9]+)$/;

/**
 * Parse one wire entry into zero or more `ServerSpec`s. Returns an empty
 * array for malformed entries. A single peer record can yield multiple
 * specs if the server advertises both `ws` and `wss` ports.
 */
export function parsePeerEntry(entry: unknown): ServerSpec[] {
  if (!Array.isArray(entry)) return [];
  const host = entry[0];
  const features = entry[2];
  if (typeof host !== 'string' || host.length === 0) return [];
  if (!Array.isArray(features)) return [];

  const out: ServerSpec[] = [];
  for (const f of features) {
    if (typeof f !== 'string') continue;
    const m = WS_FEATURE_RX.exec(f);
    if (!m) continue;
    const protocol = m[1] as Protocol;
    const port = Number(m[2]);
    if (!Number.isInteger(port) || port <= 0 || port > 65535) continue;
    out.push({
      // Stable, deterministic id so dedup against the existing pool works.
      id: `${host}:${port}` as ClientId,
      host,
      port,
      protocol,
    });
  }
  return out;
}

/** Parse a full `server.peers.subscribe` response into ServerSpec candidates. */
export function parsePeerList(response: unknown): ServerSpec[] {
  if (!Array.isArray(response)) return [];
  const out: ServerSpec[] = [];
  for (const entry of response) {
    out.push(...parsePeerEntry(entry));
  }
  return out;
}

/** Caller-provided veto / persist hook. */
export type OnDiscoverCallback = (peer: ServerSpec) => boolean | Promise<boolean>;

export interface DiscoverOptions {
  /** Master switch. Default: `false`. */
  enabled: boolean;
  /**
   * Called once per candidate. Returning `false` skips the peer; returning
   * `true` admits it via `manager.addServer`. A throw is treated as
   * `false`, but the thrown value also surfaces on the manager `error`
   * event so a buggy callback is observable rather than silently dropping
   * every candidate. Omitted callback = "admit everything".
   */
  onDiscover?: OnDiscoverCallback;
  /**
   * Re-poll interval. Default: 6h. Set to `0` to poll once per connect
   * and never again. Per-server timer; servers that don't support peer
   * discovery still get retried on every reconnect.
   */
  intervalMs?: number;
}

export const DEFAULT_DISCOVER_INTERVAL_MS = 6 * 60 * 60 * 1000; // 6h
