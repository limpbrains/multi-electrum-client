// Ban / rate-limit detection on a strict-config Fulcrum.
//
// Compose ships a `fulcrum-strict` variant whose `max_subs_per_ip = 500`
// (Fulcrum's hard minimum). A burst of 501 scripthash subscribes from
// a single IP trips `RPC::Code_App_LimitExceeded`; Fulcrum replies
// with `RpcError("Subscription limit reached")` per
// `impl_generic_subscribe` in `src/Servers.cpp`. The default
// classifier maps that string to `rate-limit`, sets `meta.bannedUntil`,
// and emits `client-banned`. Subsequent calls route around `strict` to
// the healthy `default` lane.
//
// We chose Fulcrum here rather than ElectrumX because aiorpcx's
// `ExcessiveSessionCostError` path drops the socket without delivering
// the RPC payload to the client (the throttle queue collapses tasks
// before the response is sent). Fulcrum's per-IP-subscribe path
// produces a real RPC error that the manager can observe and ban on
// without us needing to invent a transport-level rate-limit heuristic.

import { describe, expect, it } from 'vitest';

import {
  ElectrumManager,
  failover,
  type ClientId,
  type ErrorKind,
  type ServerSpec,
} from '../../src/index.js';

import { INTEGRATION_HOST, PORTS } from './helpers/config.js';

// 32-byte hex scripthash. Fulcrum costs the subscribe before knowing
// whether the script is interesting, so unique random hashes work fine.
function randomScripthash(): string {
  let out = '';
  for (let i = 0; i < 64; i++) {
    out += Math.floor(Math.random() * 16).toString(16);
  }
  return out;
}

const SERVERS: ServerSpec[] = [
  // Strict lane first so the failover policy hands every request here
  // until it's banned.
  { id: 'strict', host: INTEGRATION_HOST, port: PORTS.fulcrumStrictTcp, protocol: 'tcp' },
  { id: 'default', host: INTEGRATION_HOST, port: PORTS.fulcrumTcp, protocol: 'tcp' },
];

describe('integration: ban detection on Fulcrum with tight max_subs_per_ip', () => {
  it('bans the strict client on rate-limit and routes future calls to the healthy lane', async () => {
    const banned: { clientId: ClientId; reason: ErrorKind; until: number }[] = [];
    const errors: unknown[] = [];

    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      // failover with strict first — every subscribe lands on strict
      // until the manager bans it, then falls through to default.
      policy: failover(['strict', 'default']),
      autoBatch: false,
      requestTimeoutMs: 4000,
      // Generous cooldown so the `until` we capture below stays in
      // the future across the rest of the test no matter how slow CI
      // runs. We don't assert on expiry; only on detection.
      cooldownMs: 60_000,
      // Disable rapid reconnect for the duration so a transport blip
      // doesn't reset Fulcrum's per-IP subscribe counter mid-burst.
      // Use 120s — decoupled from the 60s test timeout so the timer
      // CAN'T fire near the deadline (Fulcrum closes the socket on
      // some over-cap responses; a reconnect timer firing right at
      // 60s would race the test's cleanup). `manager.stop()` clears
      // the pending timer in the finally block so this doesn't leak.
      reconnectBackoff: { minMs: 120_000, maxMs: 120_000, factor: 2, jitter: 0 },
    });
    manager.on('client-banned', (e) =>
      banned.push({ clientId: e.clientId, reason: e.reason, until: e.until }),
    );
    manager.on('error', (e) => errors.push(e));

    try {
      await manager.start();

      // Burst-fire ~600 subscribes in parallel against the strict lane.
      // Fulcrum is fast (~5-10ms per subscribe) so this completes in a
      // couple of seconds. Errors per-call are expected once the cap
      // is reached; we only need ONE ban event.
      const hashes = Array.from({ length: 600 }, () => randomScripthash());
      await Promise.all(
        hashes.map(async (h) => {
          try {
            await manager.scripthash.subscribe(h, () => {
              // Notification handler not exercised by this test.
            });
          } catch {
            // Expected on the over-cap calls; ban observed via the
            // `client-banned` event.
          }
        }),
      );

      // Exactly ONE `client-banned` event for the strict client.
      // Pre-fix this was ~100 (one per over-cap response in the
      // burst) — `recordError` re-extended `bannedUntil` and
      // re-emitted on every rate-limit error. The leading-edge guard
      // in `manager.ts` collapses subsequent rate-limit errors during
      // the cooldown window into telemetry without re-emitting.
      const strictBans = banned.filter((b) => b.clientId === 'strict' && b.reason === 'rate-limit');
      expect(strictBans).toHaveLength(1);

      const firstBan = strictBans[0]!;
      const strictView = manager.getClientViews().find((v) => v.id === 'strict');
      // The view's bannedUntil reflects the same value we observed in
      // the event, not a fresh `Date.now()` — racy CI runs could
      // otherwise see the ban expire between assertion lines.
      expect(strictView?.bannedUntil).toBe(firstBan.until);

      // A fresh call resolves: failover routes around the banned client
      // to `default`, which is healthy.
      expect(await manager.server.ping()).toBeNull();
    } finally {
      await manager.stop();
    }
  }, 60_000);
});
