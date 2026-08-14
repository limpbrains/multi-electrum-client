// Host bootstrap for running the integration suite on-device. The suite's
// helpers/config.ts reads process.env at module init; this module is
// imported first by every suite-integration/ wrapper so the docker stack on
// the development machine is reachable from the device:
//  - iOS simulator shares the host network -> 127.0.0.1
//  - Android emulator reaches the host via the magic 10.0.2.2 alias
import { Platform } from 'react-native';

declare const global: typeof globalThis & {
  process?: { env: Record<string, string | undefined> };
};

global.process ??= { env: {} };
global.process.env ??= {};
global.process.env['INTEGRATION_HOST'] ??= Platform.OS === 'android' ? '10.0.2.2' : '127.0.0.1';
// On-device integration stays on the TCP lane: the rn job is already
// the longest in CI, a second harness pass would re-bundle everything
// through Metro, and the ws transport gets its real-server coverage
// from the node ws lane (RN WebSocket quirks are covered on-device by
// the unit suite's ws shim).
global.process.env['INTEGRATION_LANE'] ??= 'tcp';
