// Integration smoke test. Runs against the `slim` compose profile:
//
//   docker compose -f docker/compose.yml --profile slim up -d
//
// Pins the contract that the built library can talk to a real ElectrumX
// instance over TCP — server.version handshake, server.ping, and a basic
// blockchain query. Other test buckets (failover, partial-batch retry,
// subscription catch-up, ban detection, cross-impl parity) land in
// follow-up PRs as the compose stack grows to include Fulcrum / electrs /
// strict variants and toxiproxy fault injection.

import { describe, expect, it } from 'vitest';

// Import via the package index so the transport modules self-register
// in the factory (side-effect imports inside `src/index.ts`).
import { ElectrumManager, failover, type ServerSpec, type ServerVersion } from '../../src/index.js';

import { lane } from './helpers/config.js';

const SERVERS: ServerSpec[] = [{ id: 'ex', ...lane.spec('electrumx') }];

describe('integration: smoke against ElectrumX over TCP', () => {
  it('handshake: server.version returns [softwareName, protocolVersion]', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['ex']),
      autoBatch: false,
      // We drive `server.version` directly below; ElectrumX 1.16+
      // rejects a duplicate version call on the same session
      // (`"server.version already sent"`).
      handshakeOnConnect: false,
    });
    try {
      await manager.start();
      const v: ServerVersion = await manager.server.version('multi-electrum-client', '1.4');
      // ElectrumX returns its software banner first, then the negotiated
      // protocol version. We don't pin the exact ElectrumX revision — just
      // verify shape and substring.
      expect(Array.isArray(v)).toBe(true);
      expect(v).toHaveLength(2);
      expect(v[0]).toMatch(/^ElectrumX\b/);
      expect(v[1]).toMatch(/^1\.4/);
    } finally {
      await manager.stop();
    }
  });

  it('server.ping resolves with null', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['ex']),
      autoBatch: false,
    });
    try {
      await manager.start();
      const pong = await manager.server.ping();
      expect(pong).toBeNull();
    } finally {
      await manager.stop();
    }
  });

  it('blockchain.headers.subscribe returns the current tip', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['ex']),
      autoBatch: false,
    });
    try {
      await manager.start();
      const tip = await manager.headers.getTip();
      // Regtest starts at height 0 (genesis only). Allow >= 0 to tolerate
      // a developer running miner regtest blocks against the same stack.
      expect(typeof tip.height).toBe('number');
      expect(tip.height).toBeGreaterThanOrEqual(0);
      expect(typeof tip.hex).toBe('string');
      // Block headers are exactly 80 bytes = 160 hex chars.
      expect(tip.hex).toHaveLength(160);
    } finally {
      await manager.stop();
    }
  });
});
