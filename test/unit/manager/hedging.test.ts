// Hedged requests (opt-in via ManagerOptions.hedging). A call that hasn't
// settled within `afterMs` fires the same request on a second eligible client
// without cancelling the first; first success wins. Covers both the direct
// path (`autoBatch: false`) and the auto-batch path, where the hedge races
// the whole wire batch at the group level. Timers use real (short) delays —
// the repo's manager tests drive MockTransport with real timers throughout.

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

const SERVERS4: ServerSpec[] = [...SERVERS3, { id: 'd', host: 'd', port: 50001, protocol: 'ws' }];

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

  // --- F1: hedging on the auto-batch path (group-level hedge) --------------

  it('hedges a single call under default autoBatch: the group of one races on a second server', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      // autoBatch defaults to true — this is the path every namespace call takes.
      hedging: { afterMs: 25 },
    });
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(0);
    const aT = h.transports.get('a')!;
    const bT = h.transports.get('b')!;
    expect(aT.sent).toHaveLength(1);
    expect(bT.sent).toHaveLength(0);

    await delay(80);
    expect(bT.sent).toHaveLength(1);
    const wireB = JSON.parse(bT.sent[0]!) as WireReq[];
    expect(wireB.map((r) => r.method)).toEqual(['blockchain.transaction.get']);
    expect(wireB.map((r) => r.params)).toEqual([['tx1']]);

    h.reply<WireReq>('b', (req) => ({ id: req.id, result: 'from-b' }));
    expect(await p).toBe('from-b');

    await manager.stop();
  });

  it('hedges a coalesced batch group: the same wire batch races on a second server and its results win', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      hedging: { afterMs: 25 },
    });
    await manager.start();

    const promises = [
      manager.call('blockchain.transaction.get', ['tx0']),
      manager.call('blockchain.transaction.get', ['tx1']),
    ];
    await delay(0);
    const aT = h.transports.get('a')!;
    const bT = h.transports.get('b')!;
    expect(aT.sent).toHaveLength(1);
    expect(JSON.parse(aT.sent[0]!)).toHaveLength(2);
    expect(bT.sent).toHaveLength(0);

    // a sits on the batch past afterMs → the SAME wire batch goes out on b.
    await delay(80);
    expect(bT.sent).toHaveLength(1);
    const wireB = JSON.parse(bT.sent[0]!) as WireReq[];
    expect(wireB.map((r) => r.params)).toEqual([['tx0'], ['tx1']]);

    h.reply<WireReq>('b', (req) => ({
      id: req.id,
      result: `b-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await Promise.all(promises)).toEqual(['b-tx0', 'b-tx1']);

    // The slow primary answers late: results are swallowed (callers already
    // settled) but still land in a's per-item telemetry.
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      result: `a-${(req.params as unknown[])[0] as string}`,
    }));
    await delay(0);
    const a = manager.getClientViews().find((v) => v.id === 'a')!;
    expect(a.telemetry.success.count).toBe(2);

    await manager.stop();
  });

  it('does not hedge a batch group when any item in it is not hedge-eligible', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      hedging: { afterMs: 10 },
    });
    await manager.start();

    const promises = [
      manager.call('blockchain.transaction.get', ['tx0']),
      // Mixed eligibility: this item opts out, so the whole group must not hedge.
      manager.call('blockchain.transaction.get', ['tx1'], { hedge: false }),
    ];
    await delay(50);
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      result: `a-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await Promise.all(promises)).toEqual(['a-tx0', 'a-tx1']);

    await manager.stop();
  });

  it('batch hedge: when both dispatches fail at transport level, each item burns two attempts and both clients are excluded', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS4,
      policy: failover(['a', 'b', 'c', 'd']),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const promises = [
      manager.call('blockchain.transaction.get', ['tx0'], { retry: { maxAttempts: 3 } }),
      manager.call('blockchain.transaction.get', ['tx1'], { retry: { maxAttempts: 3 } }),
    ];
    const settledP = Promise.allSettled(promises);
    await delay(0);
    expect(h.transports.get('a')!.sent).toHaveLength(1);

    // Hedge fires on b...
    await delay(50);
    expect(h.transports.get('b')!.sent).toHaveLength(1);

    // ...then both connections die: attempt advances by 2 (both dispatches
    // burned distinct clients) and the survivors re-batch onto c.
    h.transports.get('a')!.pushClose(1006, 'cut-a');
    h.transports.get('b')!.pushClose(1006, 'cut-b');
    await delay(0);
    const cT = h.transports.get('c')!;
    expect(cT.sent).toHaveLength(1);
    expect(JSON.parse(cT.sent[0]!)).toHaveLength(2);
    // c gets no hedge: only one attempt-budget unit remains per item.
    await delay(50);
    expect(h.transports.get('d')!.sent).toHaveLength(0);

    // c dies too — budget (3) is spent after a+b (2) and c (1); d must never
    // be contacted. If the hedge only advanced attempt by 1, items would
    // wrongly get a fourth dispatch on d.
    cT.pushClose(1006, 'cut-c');
    const settled = await settledP;
    for (const r of settled) {
      expect(r.status).toBe('rejected');
      const err = (r as PromiseRejectedResult).reason as Error;
      expect(err.name).toBe('TransportError');
      expect(err.message).toContain('cut-c');
    }
    expect(h.transports.get('d')!.sent).toHaveLength(0);

    await manager.stop();
  });

  it('batch hedge: keeps waiting on the primary when no eligible second client exists', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [SERVERS[0]!], // pool of one — the hedge pick can never succeed
      policy: failover(['a']),
      transportFactory: h.factory,
      hedging: { afterMs: 10 },
    });
    await manager.start();

    // maxAttempts must leave hedge room (pool-size default would be 1).
    const p = manager.call('blockchain.transaction.get', ['tx1'], {
      retry: { maxAttempts: 3 },
    });
    await delay(50);
    const aT = h.transports.get('a')!;
    expect(aT.sent).toHaveLength(1); // no duplicate dispatch anywhere
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'from-a' }));
    expect(await p).toBe('from-a');

    await manager.stop();
  });

  // --- F2: hedging never exceeds the retry budget --------------------------

  it("retry:'none' + hedging on → exactly one dispatch (direct path)", async () => {
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

    const p = manager.call('blockchain.transaction.get', ['tx1'], { retry: 'none' });
    await delay(50);
    // maxAttempts is 1: a hedge would be a second wire request the caller's
    // contract forbids.
    expect(h.transports.get('a')!.sent).toHaveLength(1);
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'from-a' }));
    expect(await p).toBe('from-a');

    await manager.stop();
  });

  it("retry:'none' + hedging on → exactly one dispatch (batch path)", async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      hedging: { afterMs: 10 },
    });
    await manager.start();

    const promises = [
      manager.call('blockchain.transaction.get', ['tx0'], { retry: 'none' }),
      manager.call('blockchain.transaction.get', ['tx1'], { retry: 'none' }),
    ];
    await delay(50);
    expect(h.transports.get('a')!.sent).toHaveLength(1);
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      result: `a-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await Promise.all(promises)).toEqual(['a-tx0', 'a-tx1']);

    await manager.stop();
  });

  // --- F5: idempotency allowlist --------------------------------------------

  it('does not hedge an unknown/vendor method by default, even with manager hedging on', async () => {
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

    const p = manager.call('vendor.specific.thing', [1]);
    await delay(50);
    // Not on the idempotent-read allowlist → no hedge without an explicit opt-in.
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'vendor-a' }));
    expect(await p).toBe('vendor-a');

    await manager.stop();
  });

  it('hedges a vendor method when the caller asserts idempotency with hedge: true', async () => {
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

    const p = manager.call('vendor.specific.thing', [1], { hedge: true });
    await delay(50);
    const bT = h.transports.get('b')!;
    expect(bT.sent).toHaveLength(1);
    const wireB = JSON.parse(bT.sent[0]!) as WireReq;
    expect(wireB.method).toBe('vendor.specific.thing');
    h.reply<WireReq>('b', (req) => ({ id: req.id, result: 'vendor-b' }));
    expect(await p).toBe('vendor-b');
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'vendor-a-late' }));

    await manager.stop();
  });

  it('hedges an allowlisted untyped method (server.features) by default', async () => {
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

    const p = manager.call('server.features', []);
    await delay(50);
    expect(h.transports.get('b')!.sent).toHaveLength(1);
    h.reply<WireReq>('b', (req) => ({ id: req.id, result: { genesis: 'x' } }));
    expect(await p).toEqual({ genesis: 'x' });
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: { genesis: 'x' } }));

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
