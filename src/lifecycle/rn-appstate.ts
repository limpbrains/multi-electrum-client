// `bindAppState` — small helper that wires React Native's AppState events to
// `manager.suspend()` / `manager.resume()`. The library does not import
// `react-native`; the caller passes their `AppState` reference, which keeps
// this file safe to bundle in Node / browser / Bun where react-native is
// absent. Returns a disposer the caller can call to unbind.

import type { ElectrumManager } from '../manager.js';

/** RN's `AppStateStatus`. Inlined to avoid the react-native import. */
export type AppStateStatus = 'active' | 'background' | 'inactive' | 'unknown' | 'extension';

/**
 * Minimal subset of RN's `AppState` we use. The real type returns an
 * `EmitterSubscription` from `addEventListener`; we only need `.remove()`.
 */
export interface AppStateLike {
  addEventListener(
    event: 'change',
    listener: (state: AppStateStatus) => void,
  ): { remove: () => void };
}

export interface BindAppStateOptions {
  /** App states that should trigger `manager.suspend()`. Default: `['background', 'inactive']`. */
  suspendOn?: ReadonlyArray<AppStateStatus>;
  /** App states that should trigger `manager.resume()`. Default: `['active']`. */
  resumeOn?: ReadonlyArray<AppStateStatus>;
}

/**
 * Bind manager lifecycle to RN AppState transitions. Returns a disposer.
 *
 * Errors from `suspend`/`resume` are swallowed inside the listener so a
 * flaky transition can't crash the AppState pipeline; the manager surfaces
 * the underlying cause on its own `error` event.
 *
 * The listener auto-disposes itself the first time it observes
 * `manager.state === 'stopped'`. Without this, a stopped manager paired
 * with a long-lived AppState (the user forgot to `dispose()`) would
 * receive a spam of `SuspendedError('cannot suspend a stopped manager')`
 * on every foreground/background flip for the rest of the process.
 *
 * Caveat: RN can emit `inactive` then `background` within ~50 ms on iOS,
 * and a quick app-switcher glance can produce
 * `active → inactive → active`. Both `suspend()` and `resume()` are
 * idempotent on their target states; rapid back-to-back transitions
 * settle on the last one. There is no internal debounce — if you need
 * one for telemetry / battery reasons, wrap your own.
 */
export function bindAppState(
  manager: ElectrumManager,
  appState: AppStateLike,
  opts: BindAppStateOptions = {},
): () => void {
  const suspendOn = opts.suspendOn ?? ['background', 'inactive'];
  const resumeOn = opts.resumeOn ?? ['active'];
  let disposed = false;
  const sub = appState.addEventListener('change', (state) => {
    if (disposed) return;
    if (manager.state === 'stopped') {
      disposed = true;
      sub.remove();
      return;
    }
    if (suspendOn.includes(state)) {
      manager.suspend().catch(() => undefined);
    } else if (resumeOn.includes(state)) {
      manager.resume().catch(() => undefined);
    }
  });
  return () => {
    if (disposed) return;
    disposed = true;
    sub.remove();
  };
}
