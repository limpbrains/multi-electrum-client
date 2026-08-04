// Environment-agnostic timer helper.

/**
 * `setTimeout` that unrefs the returned handle where the runtime supports
 * it (Node/Bun), so a pending timer never keeps the process alive.
 * Browsers and React Native return a number — no-op there.
 */
export function setUnrefTimeout(fn: () => void, ms: number): ReturnType<typeof setTimeout> {
  const t = setTimeout(fn, ms);
  if (typeof t === 'object' && t !== null && 'unref' in t) {
    (t as { unref: () => void }).unref();
  }
  return t;
}
