// On-device stand-in for the 'vitest' module, wired up via a Metro alias
// (see metro.config.js). The unit suite imports its whole test API from
// 'vitest'; react-native-harness already speaks the same dialect for most
// of it — its `expect` is built on @vitest/expect and its mocks ARE
// @vitest/spy — so this shim only fills the two real gaps: fake timers
// (@sinonjs/fake-timers) and `describe.each`.
import './setup';

import {
  describe as harnessDescribe,
  it,
  test,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
  fn,
  spyOn,
  restoreAllMocks,
} from 'react-native-harness';
import { withGlobal, type InstalledClock } from '@sinonjs/fake-timers';

let clock: InstalledClock | null = null;

// Fake only the timer surface the suite relies on. Faking sinon's full
// default list would also stub performance/requestAnimationFrame, which
// the harness UI itself uses while tests run.
const FAKED = [
  'setTimeout',
  'clearTimeout',
  'setInterval',
  'clearInterval',
  'Date',
] as const;

export const vi = {
  fn,
  spyOn,
  restoreAllMocks,
  useFakeTimers(): void {
    if (clock) return;
    clock = withGlobal(globalThis).install({
      toFake: [...FAKED],
      shouldClearNativeTimers: true,
    });
  },
  useRealTimers(): void {
    clock?.uninstall();
    clock = null;
  },
  advanceTimersByTime(ms: number): void {
    if (!clock) throw new Error('vi.advanceTimersByTime: fake timers are not installed');
    clock.tick(ms);
  },
  async advanceTimersByTimeAsync(ms: number): Promise<void> {
    if (!clock) throw new Error('vi.advanceTimersByTimeAsync: fake timers are not installed');
    await clock.tickAsync(ms);
  },
};

// vitest-style title interpolation for describe.each: `$prop` reads a
// property off an object case; printf-ish `%s`/`%i`/`%d` consume the case
// positionally. The suite's single call site uses the `$name` form.
const formatTitle = (template: string, testCase: unknown, index: number): string => {
  let title = template.replace(/\$([A-Za-z_][A-Za-z0-9_.]*)/g, (match, prop: string) => {
    if (prop === '#') return String(index);
    if (testCase !== null && typeof testCase === 'object') {
      const value = prop
        .split('.')
        .reduce<unknown>((obj, key) => (obj as Record<string, unknown> | undefined)?.[key], testCase);
      return value === undefined ? match : String(value);
    }
    return match;
  });
  const positional = Array.isArray(testCase) ? testCase : [testCase];
  let i = 0;
  title = title.replace(/%[sdifjo#%]/g, (spec) => {
    if (spec === '%%') return '%';
    if (spec === '%#') return String(index);
    const value = positional[i++];
    return spec === '%j' || spec === '%o' ? JSON.stringify(value) : String(value);
  });
  return title;
};

type SuiteFn = (name: string, body: () => void) => void;

export const describe: SuiteFn & {
  each: (cases: readonly unknown[]) => (template: string, body: (...args: never[]) => void) => void;
} = Object.assign(
  (name: string, body: () => void) => harnessDescribe(name, body),
  {
    each:
      (cases: readonly unknown[]) =>
      (template: string, body: (...args: never[]) => void): void => {
        cases.forEach((testCase, index) => {
          const args = (Array.isArray(testCase) ? testCase : [testCase]) as never[];
          harnessDescribe(formatTitle(template, testCase, index), () => body(...args));
        });
      },
  },
);

export { it, test, expect, beforeAll, afterAll, beforeEach, afterEach };
