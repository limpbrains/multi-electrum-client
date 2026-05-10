// Auto-reconnect on transport faults.
//
// Plot:
//   1. Connect through the toxiproxy lane to ElectrumX.
//   2. `toxic.disable` the proxy — every existing connection is dropped.
//      The client surfaces a `close` event; manager schedules a backoff
//      reconnect and emits `client-state: disconnected`.
//   3. `toxic.enable` — reconnect timer fires, transport reconnects, the
//      manager re-attaches the orphaned `headers.subscribe` and the
//      handler fires synthetically with the (drifted) tip.
//
// Differs from `subscription-catchup.test.ts` in that NO lifecycle
// suspend / resume is involved — recovery is driven entirely by the
// transport-fault → backoff → reconnect path.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import {
  ElectrumManager,
  failover,
  RpcError,
  TransportError,
  type BlockHeader,
  type ServerSpec,
} from '../../src/index.js';

import { INTEGRATION_HOST, PORTS } from './helpers/config.js';
import { getBlockCount, mineBlocks } from './helpers/regtestRpc.js';
import * as toxic from './helpers/toxic.js';
import { waitFor } from './helpers/wait.js';

const PROXY = 'electrumx-tcp';
const SERVERS: ServerSpec[] = [
  { id: 'ex', host: INTEGRATION_HOST, port: PORTS.toxiproxyElectrumxTcp, protocol: 'tcp' },
];

describe('integration: auto-reconnect on transport fault', () => {
  beforeAll(async () => {
    await toxic.reset();
    if ((await getBlockCount()) === 0) await mineBlocks(1);
  });

  afterAll(async () => {
    await toxic.reset();
  });

  it('reconnects after the proxy drops the link and replays the subscription', async () => {
    const headers: BlockHeader[] = [];
    const states: string[] = [];
    const restored: string[] = [];
    const errors: unknown[] = [];

    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['ex']),
      autoBatch: false,
      requestTimeoutMs: 4000,
      // Small floor so the test isn't gated on the default 500ms.
      reconnectBackoff: { minMs: 100, maxMs: 2000, factor: 2, jitter: 0 },
    });
    manager.on('client-state', (e) => states.push(`${e.clientId}:${e.state}`));
    manager.on('subscription-restored', (e) => restored.push(e.method));
    manager.on('error', (e) => errors.push(e));

    try {
      await manager.start();
      const unsub = await manager.headers.subscribe((h) => headers.push(h));
      expect(headers.length).toBeGreaterThanOrEqual(1);
      const heightBefore = headers[headers.length - 1]!.height;

      // Drop the link via toxiproxy. Manager observes a transport close.
      await toxic.disable(PROXY);
      await waitFor(() => states.includes('ex:disconnected'), {
        label: 'manager observes ex disconnect',
        timeoutMs: 5_000,
      });

      // Mine while the link is down — server learns a new tip.
      await mineBlocks(2);

      // Re-enable and let the backoff loop reconnect.
      await toxic.enable(PROXY);

      await waitFor(() => states.lastIndexOf('ex:connected') > states.indexOf('ex:disconnected'), {
        label: 'auto-reconnect lands a fresh ex:connected',
        timeoutMs: 15_000,
      });

      await waitFor(() => headers.some((h) => h.height >= heightBefore + 2), {
        label: 'subscription replayed with drifted tip',
        timeoutMs: 15_000,
      });

      expect(restored).toContain('blockchain.headers.subscribe');

      // Errors during a forced disconnect are expected (TransportError
      // for the dropped in-flight, RpcError if a reconnect attempt
      // races the proxy re-enable). What we DON'T want is anything
      // else surfacing as a manager `error` event during the cycle.
      const unexpected = errors.filter(
        (e) => !(e instanceof TransportError) && !(e instanceof RpcError),
      );
      expect(unexpected).toEqual([]);

      await unsub();
    } finally {
      await manager.stop();
    }
  }, 30_000);
});
