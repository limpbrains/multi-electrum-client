// Cross-impl parity. The MVP method set must return shape-compatible
// results from every Electrum implementation we support. This bucket
// runs the same assertions against ElectrumX (1.18), Fulcrum (1.11),
// and electrs (0.11) — not byte-identical responses (server software
// stamps differ) but matching shape and basic invariants.

import { describe, expect, it } from 'vitest';

import { ElectrumManager, failover, type ServerSpec, type ServerVersion } from '../../src/index.js';

import { lane } from './helpers/config.js';

interface Impl {
  name: string;
  software: RegExp;
  spec: ServerSpec;
}

const IMPLS: Impl[] = [
  {
    name: 'ElectrumX',
    software: /^ElectrumX\b/,
    spec: { id: 'ex', ...lane.spec('electrumx') },
  },
  {
    name: 'Fulcrum',
    software: /^Fulcrum\b/,
    spec: { id: 'fc', ...lane.spec('fulcrum') },
  },
  {
    name: 'electrs',
    software: /^electrs/i,
    spec: { id: 'el', ...lane.spec('electrs') },
  },
];

describe.each(IMPLS)('integration: cross-impl parity — $name', (impl) => {
  it('server.version returns [softwareName, protocolVersion]', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [impl.spec],
      policy: failover([impl.spec.id]),
      autoBatch: false,
      requestTimeoutMs: 4000,
      // We drive `server.version` directly below; ElectrumX rejects a
      // duplicate version call on the same session.
      handshakeOnConnect: false,
    });
    try {
      await manager.start();
      const v: ServerVersion = await manager.server.version('multi-electrum-client', '1.4');
      expect(Array.isArray(v)).toBe(true);
      expect(v).toHaveLength(2);
      expect(v[0]).toMatch(impl.software);
      expect(v[1]).toMatch(/^1\.4/);
    } finally {
      await manager.stop();
    }
  });

  // server.ping per impl is redundant with the smoke test against
  // ElectrumX + every other parity test reaching `manager.start()`
  // against Fulcrum/electrs. Dropped to keep the integration suite
  // tight.

  it('headers.getTip returns a 80-byte block header', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [impl.spec],
      policy: failover([impl.spec.id]),
      autoBatch: false,
      requestTimeoutMs: 4000,
    });
    try {
      await manager.start();
      const tip = await manager.headers.getTip();
      expect(typeof tip.height).toBe('number');
      expect(tip.height).toBeGreaterThanOrEqual(0);
      expect(tip.hex).toHaveLength(160); // 80 bytes hex-encoded
    } finally {
      await manager.stop();
    }
  });

  it('transaction.idFromPos returns a 64-char hex txid for the regtest coinbase', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [impl.spec],
      policy: failover([impl.spec.id]),
      autoBatch: false,
      requestTimeoutMs: 4000,
    });
    try {
      await manager.start();
      // `bitcoind-init` mines block 1 with one coinbase tx; pos 0
      // always resolves on regtest. Use height 1 not 0 — genesis is a
      // special case in some indexers (electrs).
      const txid = await manager.transaction.idFromPos(1, 0);
      expect(typeof txid).toBe('string');
      expect(txid).toMatch(/^[0-9a-f]{64}$/);
    } finally {
      await manager.stop();
    }
  });

  it('mempool.getFeeHistogram returns an array of [fee, vsize] pairs', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [impl.spec],
      policy: failover([impl.spec.id]),
      autoBatch: false,
      requestTimeoutMs: 4000,
    });
    try {
      await manager.start();
      const hist = await manager.mempool.getFeeHistogram();
      // Empty mempool on a fresh regtest is `[]`. With txs, every entry
      // is a 2-tuple of finite numbers.
      expect(Array.isArray(hist)).toBe(true);
      for (const entry of hist) {
        expect(Array.isArray(entry)).toBe(true);
        expect(entry).toHaveLength(2);
        expect(typeof entry[0]).toBe('number');
        expect(typeof entry[1]).toBe('number');
        expect(Number.isFinite(entry[0])).toBe(true);
        expect(Number.isFinite(entry[1])).toBe(true);
      }
    } finally {
      await manager.stop();
    }
  });

  it('scripthash.subscribe returns initial status (null for never-seen) and unsubscribe resolves', async () => {
    // Subscriptions are the most divergent surface across server impls
    // (initial-status timing, scriptpubkey indexing, retention), so
    // exercising the full registry+wire round-trip per impl is worth
    // its own test. We pick a never-seen scripthash so initial status
    // is reliably `null` on every impl.
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [impl.spec],
      policy: failover([impl.spec.id]),
      autoBatch: false,
      requestTimeoutMs: 4000,
    });
    try {
      await manager.start();
      const SCRIPTHASH = 'a'.repeat(64);
      const seen: Array<unknown> = [];
      const unsub = await manager.scripthash.subscribe(SCRIPTHASH, (status) => {
        seen.push(status);
      });
      // Initial-status handler fires synchronously inside `subscribe`.
      expect(seen).toHaveLength(1);
      expect(seen[0]).toBeNull(); // never-seen address → null status across all impls
      await unsub(); // last-handler unsubscribe must resolve cleanly
    } finally {
      await manager.stop();
    }
  });
});
