// Bun example. Bun ships its own WebSocket / TCP / TLS implementations
// that are API-compatible with Node's, so the same code path Node uses
// works unchanged.
//
// Run: bun examples/bun-basic.ts

import { ElectrumManager, failover, MemoryCache } from 'multi-electrum-client';

const manager = new ElectrumManager({
  network: 'mainnet',
  servers: [{ id: 'a', host: 'electrum.blockstream.info', port: 50002, protocol: 'tls' }],
  policy: failover(['a']),
  cache: new MemoryCache(),
});

manager.on('client-state', (e) => console.log('[state]', e.clientId, '→', e.state));

await manager.start();
console.log('tip:', await manager.headers.getTip());
console.log('ping:', await manager.server.ping());
await manager.stop();
