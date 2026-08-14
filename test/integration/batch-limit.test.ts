// Real-server batch-limit handling. Fulcrum caps wire batches at 345
// items (v1.11 defaults, measured empirically); an oversized batch is
// rejected with a SINGLE `{error: {code: 4, message: "Batch limit
// exceeded"}, id: null}` object instead of a response array.
//
// The client must map that id-less reply onto the batch's items (no
// timeout stall), the classifier must mark it rate-limit, and the
// manager must cool Fulcrum down and re-route every item to the other
// server — the caller sees 400/400 successes.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { ElectrumManager, failover, type ServerSpec } from '../../src/index.js';

import { lane } from './helpers/config.js';

const SERVERS: ServerSpec[] = [
  { id: 'fulcrum', ...lane.spec('fulcrum') },
  { id: 'electrumx', ...lane.spec('electrumx') },
];

// Comfortably above Fulcrum's 345-item cap.
const BATCH_SIZE = 400;

describe('integration: whole-batch rejection at Fulcrum batch limit', () => {
  let manager: ElectrumManager;

  beforeAll(async () => {
    manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      // Everything routes to fulcrum until it fails.
      policy: failover(['fulcrum', 'electrumx']),
      autoBatch: true,
      // ElectrumX 1.18 pads responses (~1s anti-side-channel); give the
      // rerouted singles room.
      requestTimeoutMs: 30_000,
    });
    await manager.start();
  });

  afterAll(async () => {
    await manager.stop();
  });

  it('re-routes all items of an over-limit batch; every item resolves', async () => {
    const banned: string[] = [];
    manager.on('client-banned', ({ clientId }) => banned.push(clientId));

    const started = Date.now();
    const results = await manager.batch(
      Array.from({ length: BATCH_SIZE }, () => ({ method: 'server.ping', params: [] })),
    );
    const elapsed = Date.now() - started;

    expect(results).toHaveLength(BATCH_SIZE);
    expect(results.every((r) => r.ok)).toBe(true);

    // Fulcrum's rejection was classified rate-limit → cooldown ban.
    expect(banned).toContain('fulcrum');

    // The batch-level error must be mapped immediately — if the items
    // had waited out their 30s timeout instead, elapsed would blow past
    // it. Generous bound: reroute + ElectrumX response padding.
    expect(elapsed).toBeLessThan(25_000);
  });
});
