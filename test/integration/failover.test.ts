// Failover under live transport faults. The compose stack exposes the same
// ElectrumX backend on two routes: a direct lane (:50001) and a toxiproxy
// lane (:52001). Register both as separate clients, kill the toxiproxy
// lane mid-test via the admin API, and verify the manager transparently
// routes around it.
//
// Note on timing: ElectrumX 1.18 coalesces JSON-RPC responses with ~1s
// padding (constant-size response anti-side-channel). Every successful
// call therefore takes ~1s on the wire. We size timeouts accordingly —
// the test isn't measuring raw latency, just observing that the manager
// excludes the dead lane from future picks.

import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { ElectrumManager, roundRobin, type ServerSpec } from '../../src/index.js';

import { lane } from './helpers/config.js';
import * as toxic from './helpers/toxic.js';

const PROXY = lane.proxy('electrumx');

const SERVERS: ServerSpec[] = [
  { id: 'direct', ...lane.spec('electrumx') },
  { id: 'proxy', ...lane.spec('electrumx', { via: 'proxy' }) },
];

describe('integration: failover under toxiproxy faults', () => {
  beforeAll(async () => {
    await toxic.reset();
  });

  afterAll(async () => {
    await toxic.reset();
  });

  afterEach(async () => {
    await toxic.enable(PROXY);
  });

  it('marks the proxy lane disconnected once toxiproxy drops it; routes future calls to direct', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: roundRobin(),
      autoBatch: false,
      requestTimeoutMs: 4000,
    });
    try {
      await manager.start();
      // Pre-fault sanity — both lanes alive.
      expect(await manager.server.ping()).toBeNull();

      // Kill the proxy lane. toxiproxy drops existing connections on disable.
      await toxic.disable(PROXY);

      // Drive enough traffic that any in-flight on `proxy` finishes (success
      // or timeout) and the manager observes the disconnect. Timing-loose
      // because of ElectrumX's ~1s response padding.
      for (let i = 0; i < 4; i++) {
        expect(await manager.server.ping()).toBeNull();
      }

      // Every ping above resolved, so retries routed around the dead
      // lane. Assert the ROUTING outcome, not the raw socket state: on
      // Android-emulator NAT (10.0.2.2) the RST from toxiproxy's
      // disable may never reach the app, leaving the proxy socket
      // half-open in 'connected' while its calls time out. On hosts
      // that do see the RST the state flips to 'disconnected'/'banned'.
      //
      // The dead-lane check must POLL, not snapshot: the same NAT
      // phantom-accepts reconnects to the closed host port (the guest-side
      // handshake completes before the host connect is attempted), so at
      // any single instant the proxy client can look freshly 'connected'
      // with clean telemetry until the next call lands on it and times
      // out. Keep driving traffic until the manager has evidence.
      const proxyLooksDead = (): boolean => {
        const proxy = manager.getClientViews().find((v) => v.id === 'proxy');
        return (
          proxy?.state !== 'connected' ||
          (proxy.telemetry.errors.consecutive >= 1 &&
            (proxy.telemetry.errors.lastKind === 'timeout' ||
              proxy.telemetry.errors.lastKind === 'transport'))
        );
      };
      const deadline = Date.now() + 20_000;
      while (!proxyLooksDead() && Date.now() < deadline) {
        expect(await manager.server.ping()).toBeNull();
      }
      expect(proxyLooksDead()).toBe(true);

      const direct = manager.getClientViews().find((v) => v.id === 'direct');
      expect(direct?.state).toBe('connected');
      // 5 fixed pings, at most the pre-fault one can have succeeded on
      // `proxy` — everything else (plus any polling pings) landed on
      // `direct`.
      expect(direct?.telemetry.success.count ?? 0).toBeGreaterThanOrEqual(4);
    } finally {
      await manager.stop();
    }
  }, 60_000);
});
