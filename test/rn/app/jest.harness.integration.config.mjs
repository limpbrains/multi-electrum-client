// Runs the repo's integration suite (test/integration/**) on-device against
// the docker compose stack on the host machine. Bring the stack up first:
//   docker compose -f docker/compose.yml --profile slim up -d --wait
// Wrappers in suite-integration/ bootstrap INTEGRATION_HOST per platform
// (iOS simulator: 127.0.0.1, Android emulator: 10.0.2.2). Tests bound their
// own waits, so no per-test timeout is imposed here; harness runs files
// serially, matching the node config's fileParallelism: false.
export default {
  preset: 'react-native-harness',
  rootDir: '.',
  roots: ['<rootDir>/suite-integration'],
  testMatch: ['**/*.test.ts'],
};
