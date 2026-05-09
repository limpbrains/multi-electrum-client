// Cross-impl parity. The MVP method set must return shape-compatible
// results from every Electrum implementation we support. This bucket
// runs the same assertions against ElectrumX (1.18) and Fulcrum (1.11) —
// not byte-identical responses (server software stamps differ; Fulcrum
// emits a different software banner; tip hex is the same regtest header)
// but matching shape and basic invariants.

import { describe, expect, it } from 'vitest';

import { ElectrumManager, failover, type ServerSpec, type ServerVersion } from '../../src/index.js';

import { INTEGRATION_HOST, PORTS } from './helpers/config.js';

interface Impl {
  name: string;
  software: RegExp;
  spec: ServerSpec;
}

const IMPLS: Impl[] = [
  {
    name: 'ElectrumX',
    software: /^ElectrumX\b/,
    spec: { id: 'ex', host: INTEGRATION_HOST, port: PORTS.electrumxTcp, protocol: 'tcp' },
  },
  {
    name: 'Fulcrum',
    software: /^Fulcrum\b/,
    spec: { id: 'fc', host: INTEGRATION_HOST, port: PORTS.fulcrumTcp, protocol: 'tcp' },
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

  it('server.ping resolves with null', async () => {
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [impl.spec],
      policy: failover([impl.spec.id]),
      autoBatch: false,
      requestTimeoutMs: 4000,
    });
    try {
      await manager.start();
      expect(await manager.server.ping()).toBeNull();
    } finally {
      await manager.stop();
    }
  });

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
});
