import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.ts'],
    exclude: ['test/**/*.test-d.ts', 'test/integration/**', 'test/rn/**', 'node_modules/**'],
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
