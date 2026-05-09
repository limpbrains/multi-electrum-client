// Node.js example — runs against public Electrum servers.
// Requires Node 22+ (global WebSocket) or Node 20 with `--experimental-websocket`.
//
// Run: pnpm tsx examples/node-basic.ts

import { ElectrumManager, failover, MemoryCache, type Scripthash } from 'multi-electrum-client';

async function main(): Promise<void> {
  const manager = new ElectrumManager({
    network: 'mainnet',
    servers: [
      // Mix of transports — the default factory dispatches on protocol.
      { id: 'a', host: 'electrum.blockstream.info', port: 50002, protocol: 'tls' },
      { id: 'b', host: 'electrum.blockstream.info', port: 50001, protocol: 'tcp' },
    ],
    policy: failover(['a', 'b']),
    cache: new MemoryCache(),
  });

  manager.on('client-state', (e) => console.log('[state]', e.clientId, '→', e.state));
  manager.on('error', (e) => console.error('[error]', e));

  await manager.start();

  // Fetch the current tip (one-shot — does not subscribe).
  const tip = await manager.headers.getTip();
  console.log('tip:', tip);

  // Scripthash query (caller computes the scripthash).
  const hash = 'a3c2b3a90e2c34f7c2a01a0a16ac8d6b8c9a07b2f1f2e9d3a4b5c6d7e8f90a1b' as Scripthash;
  const balance = await manager.scripthash.getBalance(hash);
  console.log('balance:', balance);

  await manager.stop();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
