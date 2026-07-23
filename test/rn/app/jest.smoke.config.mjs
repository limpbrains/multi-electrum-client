// Milestone-1 probe config: runs only the pipeline smoke test.
export default {
  preset: 'react-native-harness',
  rootDir: '.',
  roots: ['<rootDir>/smoke'],
  testMatch: ['**/*.test.ts'],
};
