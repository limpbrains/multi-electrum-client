const path = require('node:path');
const { getDefaultConfig, mergeConfig } = require('@react-native/metro-config');

const appRoot = __dirname;
// Repo root: the test files under test/unit import ../../src/**/*.ts
// relatively, so Metro must be able to watch and resolve outside the app.
const repoRoot = path.resolve(appRoot, '..', '..', '..');

// Bare-specifier remaps applied before default resolution.
//  - vitest: the unit suite imports its test API from 'vitest'; on-device
//    the same surface is provided by harness/vitest-shim.ts.
//  - node:net / node:tls: satisfied by react-native-tcp-socket via thin
//    adapters (same alias the README documents for RN consumers).
//  - node:timers/promises: only `setTimeout as delay` is used.
//  - node:events: the npm 'events' package is API-compatible.
const ALIASES = {
  vitest: path.join(appRoot, 'harness', 'vitest-shim.ts'),
  // The ws-backed tests start a WebSocketServer inside the test body; the
  // shim implements the needed RFC 6455 slice on react-native-tcp-socket.
  ws: path.join(appRoot, 'harness', 'ws-shim.ts'),
  'node:net': path.join(appRoot, 'harness', 'node-net.ts'),
  'node:tls': path.join(appRoot, 'harness', 'node-tls.ts'),
  'node:timers/promises': path.join(appRoot, 'harness', 'node-timers-promises.ts'),
  // @sinonjs/fake-timers statically requires these node builtins (its
  // runtime guards don't stop Metro from resolving them).
  timers: path.join(appRoot, 'harness', 'node-timers.ts'),
  'timers/promises': path.join(appRoot, 'harness', 'node-timers-promises.ts'),
  // NOTE: require.resolve('events') would return the node builtin
  // specifier, not a file path — resolve the npm package's entry file.
  'node:events': require.resolve('events/events.js'),
};

const defaultConfig = getDefaultConfig(appRoot);

/** @type {import('@react-native/metro-config').MetroConfig} */
const config = {
  watchFolders: [repoRoot],
  resolver: {
    // Resolve everything from the app's own node_modules. Without this,
    // Metro walks up from test/unit/** into the repo-root node_modules and
    // finds the real 'vitest' / 'ws'.
    nodeModulesPaths: [path.join(appRoot, 'node_modules')],
    disableHierarchicalLookup: true,
    resolveRequest: (context, moduleName, platform) => {
      if (ALIASES[moduleName]) {
        return { type: 'sourceFile', filePath: ALIASES[moduleName] };
      }
      // The library and its tests use ESM-style relative specifiers with a
      // .js extension pointing at .ts sources (../foo.js -> ../foo.ts).
      // Metro resolves the literal path only, so retry without the
      // extension when the literal lookup fails.
      try {
        return context.resolveRequest(context, moduleName, platform);
      } catch (error) {
        if (moduleName.startsWith('.') && moduleName.endsWith('.js')) {
          return context.resolveRequest(context, moduleName.slice(0, -3), platform);
        }
        throw error;
      }
    },
  },
};

module.exports = mergeConfig(defaultConfig, config);
