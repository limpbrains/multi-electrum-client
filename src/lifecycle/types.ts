// Lifecycle state machine for ElectrumManager.
//
// `created` → `running` (after start) → `suspending` → `suspended` →
// `resuming` → `running` → `stopped` (after stop). `stop` is terminal —
// to use the manager again, construct a new instance. (Round-trips via
// `start` after `stop` re-enable the running state, but subscriptions
// were cleared and the cache tip is gone; treat it as a fresh boot.)

export type LifecycleState =
  | 'created'
  | 'running'
  | 'suspending'
  | 'suspended'
  | 'resuming'
  | 'stopped';

export interface SuspendOptions {
  /**
   * How long to wait for in-flight requests to settle before forcibly
   * rejecting them with `SuspendedError`. Default: 2000 ms.
   */
  graceMs?: number;
  /**
   * If `true`, in-flight requests are rejected immediately rather than
   * given a grace window. Default: `false`.
   */
  cancelInFlight?: boolean;
}
