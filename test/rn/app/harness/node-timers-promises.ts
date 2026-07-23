// Minimal node:timers/promises stand-in. The suite only uses
// `setTimeout as delay` (promise form, no AbortSignal).
export const setTimeout = <T = void>(ms?: number, value?: T): Promise<T> =>
  new Promise<T>((resolve) => {
    globalThis.setTimeout(() => resolve(value as T), ms);
  });
