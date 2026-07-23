// Runs the repo's node unit suite (test/unit/**) on-device. Metro can only
// serve bundle entries from inside the app root, so suite/ holds one
// wrapper per real test file (see suite/README.md); each wrapper re-imports
// its test/unit counterpart, which then runs unmodified through the
// 'vitest' Metro alias (harness/vitest-shim.ts). The two files without
// wrappers — transport/ws.test.ts and client/electrum-client.ws.test.ts —
// spin up a node `ws` WebSocketServer on the host, which cannot exist
// inside the React Native runtime.
export default {
  preset: 'react-native-harness',
  rootDir: '.',
  roots: ['<rootDir>/suite'],
  testMatch: ['**/*.test.ts'],
};
