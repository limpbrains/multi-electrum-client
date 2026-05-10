// Per-client backoff reconnect loop.
//
// Manager registers each `installServer`'d client; the runner owns the
// per-client `wantsReconnect` flag, the attempt counter, and the
// pending timer. When a client transitions to `disconnected` the manager
// calls `schedule(id)`; on `connected` it calls `resetAttempts(id)`.
// Lifecycle (suspend / resume / stop) is gated through `isRunning` so a
// timer firing during `suspended` is a no-op — `resume()` is what
// actually drives reconnect during the suspend cycle.
//
// Backoff: `delay = clamp(minMs * factor^attempt, minMs, maxMs)` with
// `±jitter * delay` randomization. `attempt` resets on every successful
// `connected` transition.
//
// Error policy: a failed `client.connect()` surfaces via `onError` and
// schedules the next attempt. The state-change handler ALSO fires
// `disconnected → schedule`, but only on transitions; if connect
// rejects without ever reaching `connected`, that path doesn't fire and
// the inner-rejection re-arm is the only way the loop continues.

import type { ClientId } from './client.js';
import type { ReconnectBackoff } from './protocol/types.js';

export interface ReconnectDeps {
  /**
   * Look up the client by id. Returns the underlying object that
   * exposes `connect()` and `getState()`. The runner doesn't import
   * `ElectrumClient` directly — keeps the module independently
   * testable with a mock client surface.
   */
  getClient(id: ClientId): ReconnectClient | undefined;
  /**
   * True iff the manager is in a state where reconnects should fire
   * (`running` or `created`). Suspended / suspending / resuming /
   * stopped collapse the answer to `false` and the lifecycle path
   * drives reconnect on resume.
   */
  isRunning(): boolean;
  /** Surface a connect failure to the manager's `error` event. */
  onError(e: unknown): void;
}

/**
 * Minimal client surface the reconnect loop needs. `getState()` lets
 * us short-circuit when an in-flight connect has already landed
 * `connected` between schedule and fire (race with state-change
 * handler).
 */
export interface ReconnectClient {
  connect(): Promise<void>;
  getState(): 'disconnected' | 'connecting' | 'connected' | 'banned';
}

export class ReconnectRunner {
  private readonly backoff: ReconnectBackoff;
  private readonly deps: ReconnectDeps;
  /**
   * "User wants this client connected." Set on `register`, cleared on
   * `unregister` / `clear`. Reconnect loop only fires while true.
   * Suspend leaves the flag set so the loop can resume after `running`.
   */
  private readonly wants = new Map<ClientId, boolean>();
  /** Pending timers per client. Cleared on cancel / unregister / clear. */
  private readonly timers = new Map<ClientId, ReturnType<typeof setTimeout>>();
  /**
   * Per-client attempt count. Reset to 0 on every successful connect
   * (manager calls `resetAttempts`); incremented on every fired
   * reconnect attempt to drive backoff growth.
   */
  private readonly attempts = new Map<ClientId, number>();

  constructor(backoff: ReconnectBackoff, deps: ReconnectDeps) {
    this.backoff = backoff;
    this.deps = deps;
  }

  /** Manager calls this once per `installServer`. */
  register(id: ClientId): void {
    this.wants.set(id, true);
    this.attempts.set(id, 0);
  }

  /**
   * Manager calls this on `removeServer` BEFORE disconnecting the
   * client, so the resulting `disconnected` event doesn't reschedule
   * the loop.
   */
  unregister(id: ClientId): void {
    this.wants.delete(id);
    this.cancel(id);
    this.attempts.delete(id);
  }

  /** Manager calls this on every successful `connected` transition. */
  resetAttempts(id: ClientId): void {
    this.attempts.set(id, 0);
  }

  /**
   * Schedule a backoff reconnect for `id`. Idempotent: a pending timer
   * is left in place if one is already scheduled. Skipped when:
   *  - the user has unregistered / stopped (`wants` cleared);
   *  - the manager is suspending / suspended / resuming (`isRunning()`
   *    returns false; lifecycle path drives reconnect on resume);
   *  - the client has already reconnected by the time the timer fires
   *    (state check at fire time).
   */
  schedule(id: ClientId): void {
    if (!this.wants.get(id)) return;
    if (!this.deps.isRunning()) return;
    if (this.timers.has(id)) return;
    const client = this.deps.getClient(id);
    if (!client) return;
    const attempt = this.attempts.get(id) ?? 0;
    const delay = computeBackoff(this.backoff, attempt);
    const timer = setTimeout(() => {
      this.timers.delete(id);
      // Re-check at fire time: caller may have unregistered or the
      // manager may have suspended between schedule and fire.
      if (!this.wants.get(id)) return;
      if (!this.deps.isRunning()) return;
      const c = this.deps.getClient(id);
      if (!c) return;
      if (c.getState() === 'connected' || c.getState() === 'connecting') return;
      this.attempts.set(id, attempt + 1);
      c.connect().catch((e) => {
        this.deps.onError(e);
        // Re-arm. The state-change handler also fires `disconnected →
        // schedule`, but only on transitions; if `connect` rejected
        // without ever reaching `connected`, the inner-rejection
        // re-arm is the only path that keeps the loop alive.
        this.schedule(id);
      });
    }, delay);
    if (typeof timer === 'object' && timer !== null && 'unref' in timer) {
      (timer as { unref: () => void }).unref();
    }
    this.timers.set(id, timer);
  }

  /** Cancel a single client's pending timer (does NOT clear `wants`). */
  cancel(id: ClientId): void {
    const t = this.timers.get(id);
    if (t !== undefined) {
      clearTimeout(t);
      this.timers.delete(id);
    }
  }

  /**
   * Suspend path: cancel every pending timer, but leave the `wants`
   * flags set so the loop can re-arm after resume.
   */
  cancelAllTimers(): void {
    for (const t of this.timers.values()) clearTimeout(t);
    this.timers.clear();
  }

  /**
   * Stop path: clear every piece of state. After this, `register` is
   * required to drive the loop again for any client.
   */
  clear(): void {
    this.wants.clear();
    this.cancelAllTimers();
    this.attempts.clear();
  }
}

/**
 * Pure backoff calculation. Exported for unit testing of edge cases
 * (max clamp, jitter range) without spinning up a runner.
 */
export function computeBackoff(backoff: ReconnectBackoff, attempt: number): number {
  const { minMs, maxMs, factor, jitter } = backoff;
  const base = Math.min(maxMs, minMs * Math.pow(factor, attempt));
  const jitterRange = base * jitter;
  return Math.max(minMs, base + (Math.random() * 2 - 1) * jitterRange);
}
