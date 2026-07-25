// Hedged requests (opt-in via ManagerOptions.hedging). A single-path call
// that hasn't settled within `afterMs` fires the same request on a second
// eligible client without cancelling the first; first success wins. Timers
// use real (short) delays — the repo's manager tests drive MockTransport with
// real timers throughout.

import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [
  { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
  { id: 'b', host: 'b', port: 50001, protocol: 'ws' },
];

const SERVERS3: ServerSpec[] = [...SERVERS, { id: 'c', host: 'c', port: 50001, protocol: 'ws' }];

interface WireReq {
  id: number;
  method: string;
  params: readonly unknown[];
}

describe('ElectrumManager — hedged requests', () => {
  it('fires a hedge after afterMs; the fast second server wins and the late primary reply is swallowed', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 25 },
    });
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(0);
    const aT = h.transports.get('a')!;
    const bT = h.transports.get('b')!;
    expect(aT.sent).toHaveLength(1);
    expect(bT.sent).toHaveLength(0);

    // a sits on the request past afterMs → the same call goes out on b.
    await delay(80);
    expect(bT.sent).toHaveLength(1);
    const wireB = JSON.parse(bT.sent[0]!) as WireReq;
    expect(wireB.method).toBe('blockchain.transaction.get');
    expect(wireB.params).toEqual(['tx1']);

    h.reply<WireReq>('b', (req) => ({ id: req.id, result: 'from-b' }));
    expect(await p).toBe('from-b');

    // The slow primary answers late: result is swallowed (caller already
    // settled) but still lands in a's telemetry.
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'from-a-late' }));
    await delay(0);
    const a = manager.getClientViews().find((v) => v.id === 'a')!;
    expect(a.telemetry.success.count).toBe(1);

    await manager.stop();
  });

  it('does not hedge when ManagerOptions.hedging is absent', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(60);
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'from-a' }));
    expect(await p).toBe('from-a');

    await manager.stop();
  });

  it('never hedges broadcast or subscribe methods, even when forced per-call', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 10 },
    });
    await manager.start();

    const pBroadcast = manager.call('blockchain.transaction.broadcast', ['00'], { hedge: true });
    const pSub = manager.call('blockchain.scripthash.subscribe', ['ab'.repeat(32)], {
      hedge: true,
    });
    await delay(50);
    // Well past afterMs and still nothing on b — hard exclusion beats overrides.
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      result: req.method === 'blockchain.transaction.broadcast' ? 'txid' : null,
    }));
    expect(await pBroadcast).toBe('txid');
    expect(await pSub).toBeNull();

    await manager.stop();
  });

  it('per-call hedge:false suppresses a globally enabled hedge', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 10 },
    });
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1'], { hedge: false });
    await delay(50);
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'from-a' }));
    expect(await p).toBe('from-a');

    await manager.stop();
  });

  it('keeps waiting on the primary when no eligible second client exists', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [SERVERS[0]!], // pool of one — the hedge pick can never succeed
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 10 },
    });
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(50);
    const aT = h.transports.get('a')!;
    // No duplicate dispatch on the same (or any) client.
    expect(aT.sent).toHaveLength(1);
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'from-a' }));
    expect(await p).toBe('from-a');

    await manager.stop();
  });

  it('continues the retry loop with both clients excluded when primary and hedge both fail', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(0);
    expect(h.transports.get('a')!.sent).toHaveLength(1);

    // Hedge fires on b...
    await delay(50);
    expect(h.transports.get('b')!.sent).toHaveLength(1);

    // ...then both connections die. Retry must move to c, excluding a AND b.
    h.transports.get('a')!.pushClose(1006, 'cut-a');
    h.transports.get('b')!.pushClose(1006, 'cut-b');
    await delay(0);

    const cT = h.transports.get('c')!;
    expect(cT.sent).toHaveLength(1);
    h.reply<WireReq>('c', (req) => ({ id: req.id, result: 'from-c' }));
    expect(await p).toBe('from-c');

    await manager.stop();
  });

  it('stop() during the hedge window settles the call cleanly (no leaked timer)', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 5_000 }, // longer than the test — must be cleaned up
    });
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    const rejected = expect(p).rejects.toMatchObject({ name: 'TransportError' });
    await delay(0);
    expect(h.transports.get('a')!.sent).toHaveLength(1);

    await manager.stop();
    await rejected;
  });
});
