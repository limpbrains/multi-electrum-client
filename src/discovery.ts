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

/**
 * Dependencies the runner needs from the manager. Kept narrow so this
 * module can't reach into manager internals — every interaction goes
 * through one of these callbacks.
 */
export interface PeerDiscoveryDeps {
  /** Make a non-retried wire call pinned to `clientId`. */
  call(clientId: ClientId, method: string, params: readonly unknown[]): Promise<unknown>;
  /** Already in pool? Used to dedup admitted peers. */
  hasClient(id: ClientId): boolean;
  /** Admit a peer; runner catch-wraps any throw and forwards via `onError`. */
  addServer(spec: ServerSpec): void;
  /**
   * True once the manager is tearing down — runner short-circuits between
   * awaits so a late timer fire doesn't poke a dead manager.
   */
  isStopped(): boolean;
  /** Surface a recoverable error to the manager's `error` event. */
  onError(e: unknown): void;
}

/**
 * Periodic `server.peers.subscribe` poller. One instance per manager;
 * the manager calls `runFor(clientId)` on every fresh connect (the
 * runner internally schedules the re-poll) and `cancelFor(clientId)`
 * on every disconnect / removeServer / suspend / stop.
 *
 * Errors mid-poll are swallowed — most servers don't support peer
 * discovery and emit an RPC error, which we treat as "this server
 * can't be polled" rather than a manager-level failure. User
 * callbacks (`onDiscover`) that throw are surfaced via `onError` so
 * a buggy callback is observable.
 */
export class PeerDiscoveryRunner {
  private readonly options: DiscoverOptions;
  private readonly deps: PeerDiscoveryDeps;
  private readonly timers = new Map<ClientId, ReturnType<typeof setTimeout>>();

  constructor(options: DiscoverOptions, deps: PeerDiscoveryDeps) {
    this.options = options;
    this.deps = deps;
  }

  /**
   * Probe `clientId` for peers and admit any that pass the user's
   * filter. Schedules the next re-poll on success. No-op when
   * disabled, after the manager is stopped, or once the client itself
   * is no longer in the pool.
   */
  async runFor(clientId: ClientId): Promise<void> {
    if (!this.options.enabled) return;
    if (this.deps.isStopped()) return;

    let response: unknown;
    try {
      response = await this.deps.call(clientId, 'server.peers.subscribe', []);
    } catch {
      // Server doesn't support discovery or transient failure — runAttempts
      // already surfaced anything callers care about. Drop silently.
      return;
    }

    const candidates = parsePeerList(response);
    for (const cand of candidates) {
      // Pre-await dedup: skip peers already in the pool. The re-check
      // post-`onDiscover` below is a separate guard against the user's
      // callback racing the pool (e.g. calling `addServer` on this
      // same id from inside its own handler).
      if (this.deps.hasClient(cand.id)) continue;
      let admit: boolean;
      if (this.options.onDiscover) {
        try {
          admit = await this.options.onDiscover(cand);
        } catch (e) {
          this.deps.onError(e);
          continue;
        }
      } else {
        admit = true;
      }
      if (!admit) continue;
      // Post-await re-check: lifecycle and pool may have changed while
      // `onDiscover` ran. The user's callback can synchronously call
      // `addServer` / `removeServer`, so this isn't redundant with the
      // pre-await dedup above.
      if (this.deps.isStopped()) return;
      if (this.deps.hasClient(cand.id)) continue;
      try {
        this.deps.addServer(cand);
      } catch (e) {
        // Likely a duplicate-id race; surface and move on.
        this.deps.onError(e);
      }
    }

    const interval = this.options.intervalMs ?? DEFAULT_DISCOVER_INTERVAL_MS;
    if (interval <= 0) return;
    if (this.deps.isStopped()) return;
    // The probe `await`s above may have outlasted the client itself
    // (manager `cancelFor` + `removeServer` on disconnect), so a re-poll
    // timer for an id that's no longer in the pool would just retry
    // against a stale endpoint. Skip when the pool no longer has us.
    if (!this.deps.hasClient(clientId)) return;
    const prev = this.timers.get(clientId);
    if (prev !== undefined) clearTimeout(prev);
    const t = setTimeout(() => {
      this.timers.delete(clientId);
      this.runFor(clientId).catch((e) => this.deps.onError(e));
    }, interval);
    if (typeof t === 'object' && t !== null && 'unref' in t) {
      (t as { unref: () => void }).unref();
    }
    this.timers.set(clientId, t);
  }

  /** Cancel any pending re-poll timer for `clientId`. */
  cancelFor(clientId: ClientId): void {
    const t = this.timers.get(clientId);
    if (t !== undefined) {
      clearTimeout(t);
      this.timers.delete(clientId);
    }
  }

  /** Cancel every pending timer. Used by manager on `stop` / `suspend`. */
  cancelAll(): void {
    for (const t of this.timers.values()) clearTimeout(t);
    this.timers.clear();
  }
}
