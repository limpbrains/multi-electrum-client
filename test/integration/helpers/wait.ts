// Poll-and-wait utility for integration tests. Real servers need a few
// hundred ms to settle on regtest after `docker compose up`; tests use this
// to gate operations that depend on a server being ready.

export interface WaitForOpts {
  /** Total budget. Default: 20s. */
  timeoutMs?: number;
  /** Sleep between probes. Default: 100ms. */
  intervalMs?: number;
  /** Optional human-readable label — surfaced in the timeout error. */
  label?: string;
}

export async function waitFor(
  predicate: () => Promise<boolean> | boolean,
  opts: WaitForOpts = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 20_000;
  const intervalMs = opts.intervalMs ?? 100;
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown = null;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (e) {
      lastErr = e;
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  const label = opts.label ?? 'predicate';
  const reason = lastErr ? ` (last error: ${String(lastErr)})` : '';
  throw new Error(`waitFor(${label}) timed out after ${timeoutMs}ms${reason}`);
}
