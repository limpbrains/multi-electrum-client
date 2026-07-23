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

import { INTEGRATION_HOST, PORTS } from './helpers/config.js';
import * as toxic from './helpers/toxic.js';

const SERVERS: ServerSpec[] = [
  { id: 'direct', host: INTEGRATION_HOST, port: PORTS.electrumxTcp, protocol: 'tcp' },
  { id: 'proxy', host: INTEGRATION_HOST, port: PORTS.toxiproxyElectrumxTcp, protocol: 'tcp' },
];

describe('integration: failover under toxiproxy faults', () => {
  beforeAll(async () => {
    await toxic.reset();
  });

  afterAll(async () => {
    await toxic.reset();
  });

  afterEach(async () => {
    await toxic.enable('electrumx-tcp');
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
      await toxic.disable('electrumx-tcp');

      // Drive enough traffic that any in-flight on `proxy` finishes (success
      // or timeout) and the manager observes the disconnect. Timing-loose
      // because of ElectrumX's ~1s response padding.
      for (let i = 0; i < 4; i++) {
        expect(await manager.server.ping()).toBeNull();
      }

      // After this much traffic the proxy view should be either
      // 'disconnected' (close observed) or 'banned' (cooldown after error)
      // and `direct` should still be 'connected'.
      const views = manager.getClientViews();
      const proxy = views.find((v) => v.id === 'proxy');
      const direct = views.find((v) => v.id === 'direct');
      expect(direct?.state).toBe('connected');
      expect(proxy?.state).not.toBe('connected');
    } finally {
      await manager.stop();
    }
  }, 30_000);
});
