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
  },
});
