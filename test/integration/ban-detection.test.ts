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

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

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
  beforeAll(async () => {
    // Both lanes are direct compose ports; nothing to reset.
  });

  afterAll(async () => {
    // Nothing to tear down beyond the manager itself.
  });

  it('bans the strict client on rate-limit and routes future calls to the healthy lane', async () => {
    const banned: { clientId: ClientId; reason: ErrorKind }[] = [];
    const errors: unknown[] = [];

    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      // failover with strict first — every subscribe lands on strict
      // until the manager bans it, then falls through to default.
      policy: failover(['strict', 'default']),
      autoBatch: false,
      requestTimeoutMs: 4000,
      // Short cooldown — we don't assert on expiry, but a long ban
      // could leak into later tests if state were shared.
      cooldownMs: 5_000,
      // Disable rapid reconnect during the test so a transport blip
      // doesn't reset Fulcrum's per-IP counter mid-burst. The ban
      // event fires on the first RPC error well before any reconnect
      // would be relevant.
      reconnectBackoff: { minMs: 60_000, maxMs: 60_000, factor: 2, jitter: 0 },
    });
    manager.on('client-banned', (e) => banned.push({ clientId: e.clientId, reason: e.reason }));
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

      expect(banned.some((b) => b.clientId === 'strict' && b.reason === 'rate-limit')).toBe(true);

      const strictView = manager.getClientViews().find((v) => v.id === 'strict');
      expect(strictView?.bannedUntil).toBeDefined();
      expect(strictView?.bannedUntil).toBeGreaterThan(Date.now());

      // A fresh call resolves: failover routes around the banned client
      // to `default`, which is healthy.
      expect(await manager.server.ping()).toBeNull();
    } finally {
      await manager.stop();
    }
  }, 60_000);
});
