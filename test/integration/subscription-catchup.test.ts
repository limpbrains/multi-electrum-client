// Subscription catch-up across a suspend / resume cycle.
//
// Plot:
//   1. Subscribe to `blockchain.headers.subscribe`. Initial tip handler fires.
//   2. `manager.suspend()` — sockets close; subscription is orphaned in the
//      registry but preserved across suspend.
//   3. Mine N blocks via bitcoind RPC while the manager is suspended.
//      ElectrumX learns the new tip server-side.
//   4. `manager.resume()` — reconnect, registry's `restoreOrphans` re-issues
//      `headers.subscribe`, handler fires synthetically with the drifted tip.
//
// (We don't auto-reconnect on transport errors yet — that's deferred. This
// test exercises the lifecycle-driven reconnect path which IS shipped, and
// covers the same registry / catch-up code as the eventual auto-reconnect.)

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { ElectrumManager, failover, type BlockHeader, type ServerSpec } from '../../src/index.js';

import { INTEGRATION_HOST, PORTS } from './helpers/config.js';
import { getBlockCount, mineBlocks } from './helpers/regtestRpc.js';
import * as toxic from './helpers/toxic.js';
import { waitFor } from './helpers/wait.js';

const SERVERS: ServerSpec[] = [
  { id: 'ex', host: INTEGRATION_HOST, port: PORTS.electrumxTcp, protocol: 'tcp' },
];

describe('integration: subscription catch-up across suspend / resume', () => {
  beforeAll(async () => {
    await toxic.reset();
    if ((await getBlockCount()) === 0) await mineBlocks(1);
  });

  afterAll(async () => {
    await toxic.reset();
  });

  it('replays headers.subscribe on resume; handler fires with drifted tip', async () => {
    const headers: BlockHeader[] = [];
    const restored: { method: string; drift: boolean }[] = [];
    const errors: unknown[] = [];
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['ex']),
      autoBatch: false,
      requestTimeoutMs: 4000,
    });
    manager.on('subscription-restored', (e) => restored.push({ method: e.method, drift: e.drift }));
    manager.on('error', (e) => errors.push(e));

    try {
      await manager.start();
      const unsub = await manager.headers.subscribe((h) => headers.push(h));
      expect(headers.length).toBeGreaterThanOrEqual(1);
      const heightBefore = headers[headers.length - 1]!.height;

      // Suspend. Sockets close; subscription registry preserves the record
      // but flags it orphaned.
      await manager.suspend({ graceMs: 0 });
      expect(manager.state).toBe('suspended');

      // Mine while suspended. ElectrumX scans bitcoind on a poll interval,
      // so the new tip may not be reflected on the wire until ~1s after
      // mining; resume() may even race and re-subscribe before ElectrumX
      // catches up. Either way the handler eventually receives the new
      // tip — via the immediate subscribe response if the server is
      // current, or via a follow-up server-pushed notification.
      await mineBlocks(3);

      await manager.resume();
      expect(manager.state).toBe('running');

      await waitFor(() => headers.some((h) => h.height >= heightBefore + 3), {
        label: 'headers caught up',
        timeoutMs: 15_000,
      });

      // The restore event always fires (with drift true OR false depending
      // on the race), so just assert the manager surfaced one for our key.
      expect(restored.some((r) => r.method === 'blockchain.headers.subscribe')).toBe(true);

      await unsub();
    } finally {
      await manager.stop();
    }
  }, 30_000);
});
