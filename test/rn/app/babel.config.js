module.exports = {
  presets: ['module:@react-native/babel-preset'],
  // A few unit tests use dynamic `await import(...)`. Metro's async-require
  // machinery isn't available inside harness per-test-file bundles, so
  // rewrite dynamic imports to synchronous requires at transform time.
  plugins: ['babel-plugin-dynamic-import-node'],
};
