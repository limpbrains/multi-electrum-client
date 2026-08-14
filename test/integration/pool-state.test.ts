// Aggregate pool-state against live servers: cut every toxiproxy lane →
// `offline`; restore → auto-reconnect brings the pool back → `online`.

import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { ElectrumManager, roundRobin, type PoolState, type ServerSpec } from '../../src/index.js';

import { lane } from './helpers/config.js';
import * as toxic from './helpers/toxic.js';
import { waitFor } from './helpers/wait.js';

const EX_PROXY = lane.proxy('electrumx');
const F_PROXY = lane.proxy('fulcrum');

const SERVERS: ServerSpec[] = [
  { id: 'ex-proxy', ...lane.spec('electrumx', { via: 'proxy' }) },
  { id: 'f-proxy', ...lane.spec('fulcrum', { via: 'proxy' }) },
];

describe('integration: pool-state across total outage and recovery', () => {
  beforeAll(async () => {
    await toxic.reset();
  });

  afterAll(async () => {
    await toxic.reset();
  });

  it('goes offline when every lane dies, back online after restore', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: roundRobin(),
      autoBatch: false,
      requestTimeoutMs: 5000,
      reconnectBackoff: { minMs: 300, maxMs: 2000, factor: 2, jitter: 0.1 },
    });
    const events: PoolState[] = [];
    manager.on('pool-state', (s) => events.push(s));
    manager.on('error', () => {});
    try {
      await manager.start();
      expect(events.at(-1)?.status).toBe('online');

      await toxic.disable(EX_PROXY);
      await toxic.disable(F_PROXY);
      await waitFor(() => events.at(-1)?.status === 'offline', {
        timeoutMs: 15_000,
        label: 'pool offline',
      });

      // BlueWallet-style guard across the outage: starts while the pool
      // is fully down, must resolve once the lanes come back (real
      // reconnect + wire ping through the routing pipeline).
      const ensured = manager.ensureConnected({ timeoutMs: 30_000 });

      await toxic.enable(EX_PROXY);
      await toxic.enable(F_PROXY);
      await ensured;
      await waitFor(() => events.at(-1)?.status === 'online', {
        timeoutMs: 30_000,
        label: 'pool online again',
      });

      const statuses = events.map((e) => e.status);
      expect(statuses[0]).toBe('online');
      expect(statuses).toContain('offline');
      expect(statuses.at(-1)).toBe('online');
    } finally {
      await manager.stop();
    }
  });
});
