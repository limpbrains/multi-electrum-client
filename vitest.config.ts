import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    // `test/types/**` is type-only — its assertions are validated by `tsc`
    // (run via `pnpm typecheck`). Vitest must not execute it because the
    // tests reference `declare const m: ElectrumManager`, which is undefined
    // at runtime.
    exclude: [
      'test/**/*.test-d.ts',
      'test/types/**',
      'test/integration/**',
      'test/rn/**',
      'node_modules/**',
    ],
    environment: 'node',
    globals: false,
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      include: ['src/**/*.ts'],
      exclude: ['src/**/types.ts', 'src/**/*.d.ts'],
    },
  },
});
