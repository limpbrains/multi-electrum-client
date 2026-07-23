// Minimal node:timers stand-in for @sinonjs/fake-timers, which probes it
// for the global timer functions. RN provides all of these on global.
declare const global: typeof globalThis & {
  setImmediate?: (...args: never[]) => unknown;
  clearImmediate?: (...args: never[]) => unknown;
};

export const setTimeout = global.setTimeout.bind(global);
export const clearTimeout = global.clearTimeout.bind(global);
export const setInterval = global.setInterval.bind(global);
export const clearInterval = global.clearInterval.bind(global);
export const setImmediate = global.setImmediate?.bind(global);
export const clearImmediate = global.clearImmediate?.bind(global);

export default {
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
  setImmediate,
  clearImmediate,
};
