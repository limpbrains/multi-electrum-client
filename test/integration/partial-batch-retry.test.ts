// Partial-batch retry under live transport faults. Manager's auto-batch
// coalesces sub-requests by chosen client into wire-level JSON-RPC
// batches; when one of the chosen clients dies mid-flight, the failed
// items must retry on a different client — without losing the others
// that succeeded on a healthy client in the same batch.
//
// The compose stack gives us two routes to the same ElectrumX backend
// (direct + toxiproxy). With roundRobin policy and autoBatch enabled,
// items are split between the two clients. We disable the toxiproxy
// lane mid-batch; manager observes the close on the proxy client and
// retries the affected items on direct.

import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';

import { ElectrumManager, roundRobin, type ServerSpec } from '../../src/index.js';

import { lane } from './helpers/config.js';
import * as toxic from './helpers/toxic.js';

const PROXY = lane.proxy('electrumx');

const SERVERS: ServerSpec[] = [
  { id: 'direct', ...lane.spec('electrumx') },
  { id: 'proxy', ...lane.spec('electrumx', { via: 'proxy' }) },
];

describe('integration: partial-batch retry under toxiproxy faults', () => {
  beforeAll(async () => {
    await toxic.reset();
  });

  afterAll(async () => {
    await toxic.reset();
  });

  afterEach(async () => {
    await toxic.enable(PROXY);
  });

  it('survives the proxy lane dying mid-batch — every item resolves', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: roundRobin(),
      autoBatch: true,
      requestTimeoutMs: 4000,
    });
    try {
      await manager.start();

      // Issue a batch of 6 pings. With roundRobin + autoBatch, manager
      // groups them by chosen client — 3 items go to `proxy`, 3 to
      // `direct`. We schedule a `disable` on the proxy lane right after
      // the batch is enqueued so the in-flight wire batch dies before it
      // returns; manager's classifier should mark the failed items as
      // transport, retry on `direct`, and resolve every input.
      const batchPromise = manager.batch(
        Array.from({ length: 6 }, () => ({ method: 'server.ping', params: [] })),
      );
      // Drop the proxy lane while the batch is being dispatched. Tiny
      // delay so the items have a chance to enqueue; toxiproxy disable
      // closes existing connections.
      setTimeout(() => {
        toxic.disable(PROXY).catch(() => undefined);
      }, 5);

      const results = await batchPromise;
      expect(results).toHaveLength(6);
      // Every item resolved successfully (null for server.ping).
      for (const r of results) {
        expect(r.ok).toBe(true);
        if (r.ok) expect(r.value).toBeNull();
      }
    } finally {
      await manager.stop();
    }
  }, 30_000);
});
