import { defineConfig } from 'vitest/config';

// Separate config so the integration suite never runs as part of `pnpm test`.
// Boots are slow; failures are coupled to Docker / network / server software,
// which is exactly what we want for `pnpm test:integration` and CI's
// integration job, but not for unit-only iteration.
export default defineConfig({
  test: {
    include: ['test/integration/**/*.test.ts'],
    environment: 'node',
    globals: false,
    // Real network + bitcoind RPC + electrum handshake takes a while.
    testTimeout: 60_000,
    hookTimeout: 60_000,
    // Run files sequentially: every integration test shares the same
    // toxiproxy admin and bitcoind regtest chain, so parallel files
    // would race on toxic state and chain height. Vitest's default
    // worker-per-file setup happily lets `failover` and `auto-reconnect`
    // both poke `electrumx-tcp` at the same time and one undoes the
    // other's `disable`.
    fileParallelism: false,
  },
});
