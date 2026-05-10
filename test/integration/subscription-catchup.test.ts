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
// This test covers the lifecycle path; the transport-fault path (auto-
// reconnect after toxiproxy drops the link) is exercised by
// `auto-reconnect.test.ts`. Both end up driving the same registry /
// catch-up code, but the entry points are independent.

import { createConnection } from 'node:net';

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { ElectrumManager, failover, type BlockHeader, type ServerSpec } from '../../src/index.js';

import { INTEGRATION_HOST, PORTS } from './helpers/config.js';
import { getBlockCount, mineBlocks } from './helpers/regtestRpc.js';
import * as toxic from './helpers/toxic.js';
import { waitFor } from './helpers/wait.js';

/**
 * Open a one-shot TCP socket to ElectrumX, send `headers.subscribe`,
 * read one reply, close. Used as a side-channel poll while the manager
 * is suspended — the suspended manager has no open socket and we don't
 * want to wake it just to peek at ElectrumX's view of the tip.
 */
async function electrumxKnowsHeight(targetHeight: number): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const sock = createConnection(PORTS.electrumxTcp, INTEGRATION_HOST);
    let buf = '';
    const cleanup = (result: boolean): void => {
      sock.destroy();
      resolve(result);
    };
    sock.setTimeout(3000);
    sock.on('connect', () => {
      sock.write(
        JSON.stringify({ id: 1, method: 'blockchain.headers.subscribe', params: [] }) + '\n',
      );
    });
    sock.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      const nl = buf.indexOf('\n');
      if (nl < 0) return;
      try {
        const msg = JSON.parse(buf.slice(0, nl));
        const height = msg?.result?.height;
        cleanup(typeof height === 'number' && height >= targetHeight);
      } catch {
        cleanup(false);
      }
    });
    sock.on('error', () => cleanup(false));
    sock.on('timeout', () => cleanup(false));
  });
}

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

      // Mine while suspended. ElectrumX scans bitcoind on a poll
      // interval (~5s default), so we explicitly wait for ElectrumX
      // to learn the new tip BEFORE resuming. Without this, the
      // resume's `restoreOrphans` may issue `headers.subscribe`
      // before ElectrumX has caught up; the immediate subscribe
      // response would then contain the OLD tip and the test would
      // depend on ElectrumX subsequently pushing — racy on CI.
      await mineBlocks(3);

      // Poll ElectrumX directly via a one-shot raw socket so the
      // suspended manager isn't disturbed.
      await waitFor(() => electrumxKnowsHeight(heightBefore + 3), {
        label: 'electrumx caught up to mined tip',
        timeoutMs: 30_000,
        intervalMs: 500,
      });

      await manager.resume();
      expect(manager.state).toBe('running');

      // Now ElectrumX is up-to-date; restoreOrphans's subscribe call
      // returns the new tip in its immediate response. The handler
      // fires synthetically inside the manager's restore path with
      // the drifted tip.
      await waitFor(() => headers.some((h) => h.height >= heightBefore + 3), {
        label: 'subscription handler fired with drifted tip',
        timeoutMs: 15_000,
      });

      // The restore event always fires (with drift true OR false depending
      // on the race), so just assert the manager surfaced one for our key.
      expect(restored.some((r) => r.method === 'blockchain.headers.subscribe')).toBe(true);

      await unsub();
    } finally {
      await manager.stop();
    }
  }, 60_000);
});
