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
 * Errors from `suspend`/`resume` are swallowed (manager already emits them
 * via `error`); we don't want a flaky transition to crash the listener.
 */
export function bindAppState(
  manager: ElectrumManager,
  appState: AppStateLike,
  opts: BindAppStateOptions = {},
): () => void {
  const suspendOn = opts.suspendOn ?? ['background', 'inactive'];
  const resumeOn = opts.resumeOn ?? ['active'];
  const sub = appState.addEventListener('change', (state) => {
    if (suspendOn.includes(state)) {
      manager.suspend().catch(() => undefined);
    } else if (resumeOn.includes(state)) {
      manager.resume().catch(() => undefined);
    }
  });
  return () => sub.remove();
}
