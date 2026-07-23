import { androidPlatform, androidEmulator } from '@react-native-harness/platform-android';
import { applePlatform, appleSimulator } from '@react-native-harness/platform-apple';

// Device pins default to what GitHub-hosted runners provide; override
// locally via env when your simulator/AVD differs, e.g.
//   HARNESS_IOS_SIM='iPhone 17 Pro' HARNESS_IOS_VERSION='26.3' pnpm harness:ios
const IOS_SIM = process.env.HARNESS_IOS_SIM ?? 'iPhone 16';
const IOS_VERSION = process.env.HARNESS_IOS_VERSION ?? '18.4';
const ANDROID_AVD = process.env.HARNESS_ANDROID_AVD ?? 'rn_harness_avd';

export default {
  entryPoint: './index.js',
  appRegistryComponentName: 'HarnessApp',
  runners: [
    applePlatform({
      name: 'ios',
      device: appleSimulator(IOS_SIM, IOS_VERSION),
      bundleId: 'org.reactjs.native.example.HarnessApp',
    }),
    androidPlatform({
      name: 'android',
      device: androidEmulator(ANDROID_AVD, { apiLevel: 35, profile: 'pixel_6' }),
      bundleId: 'com.harnessapp',
    }),
  ],
  defaultRunner: 'ios',
  // Without this, every per-test-file bundle re-includes the entire RN +
  // harness graph (~6.4 MB) and RN's fetch on iOS truncates responses at
  // ~6.35 MB, which surfaces as MalformedModuleError('No __r function
  // found'). Skipping modules already shipped in the host-app bundle keeps
  // test bundles small and fast.
  unstable__skipAlreadyIncludedModules: true,
};
