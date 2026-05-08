import { describe, expect, it, vi } from 'vitest';

import {
  bindAppState,
  type AppStateLike,
  type AppStateStatus,
} from '../../../src/lifecycle/rn-appstate.js';
import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [{ id: 'a', host: 'a', port: 50001, protocol: 'ws' }];

/** Test double for RN's AppState. */
function fakeAppState(): AppStateLike & {
  fire: (s: AppStateStatus) => void;
  listenerCount: () => number;
} {
  const listeners: Array<(s: AppStateStatus) => void> = [];
  return {
    addEventListener(_event, listener) {
      listeners.push(listener);
      return {
        remove() {
          const i = listeners.indexOf(listener);
          if (i >= 0) listeners.splice(i, 1);
        },
      };
    },
    fire(s) {
      for (const l of [...listeners]) l(s);
    },
    listenerCount() {
      return listeners.length;
    },
  };
}

describe('bindAppState', () => {
  it('calls suspend on background and resume on active', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const suspendSpy = vi.spyOn(manager, 'suspend');
    const resumeSpy = vi.spyOn(manager, 'resume');

    const appState = fakeAppState();
    const dispose = bindAppState(manager, appState);

    appState.fire('background');
    expect(suspendSpy).toHaveBeenCalledTimes(1);
    appState.fire('active');
    expect(resumeSpy).toHaveBeenCalledTimes(1);

    dispose();
    await manager.stop();
  });

  it('honors custom suspendOn / resumeOn', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    const suspendSpy = vi.spyOn(manager, 'suspend');

    const appState = fakeAppState();
    bindAppState(manager, appState, { suspendOn: ['inactive'], resumeOn: ['active'] });

    appState.fire('background'); // not in suspendOn → no-op
    expect(suspendSpy).not.toHaveBeenCalled();
    appState.fire('inactive');
    expect(suspendSpy).toHaveBeenCalled();

    await manager.stop();
  });

  it('disposer removes the listener', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const appState = fakeAppState();
    const dispose = bindAppState(manager, appState);
    expect(appState.listenerCount()).toBe(1);
    dispose();
    expect(appState.listenerCount()).toBe(0);

    await manager.stop();
  });

  it('swallows errors from suspend/resume so the listener stays alive', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.stop();

    // suspend on a stopped manager rejects — listener must not propagate.
    const appState = fakeAppState();
    bindAppState(manager, appState);
    expect(() => appState.fire('background')).not.toThrow();
  });

  it('auto-disposes the listener when the manager hits stopped', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    const appState = fakeAppState();
    bindAppState(manager, appState);
    expect(appState.listenerCount()).toBe(1);

    await manager.stop();
    // First fire after stop: listener observes 'stopped' and removes itself.
    appState.fire('background');
    expect(appState.listenerCount()).toBe(0);
  });
});
