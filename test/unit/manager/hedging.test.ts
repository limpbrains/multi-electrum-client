// Hedged requests (opt-in via ManagerOptions.hedging). A call that hasn't
// settled within `afterMs` fires the same request on a second eligible client
// without cancelling the first; first success wins. Covers both the direct
// path (`autoBatch: false`) and the auto-batch path, where the hedge races
// the whole wire batch at the group level. Timers use real (short) delays —
// the repo's manager tests drive MockTransport with real timers throughout.

import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { RpcError } from '../../../src/errors/types.js';
import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { RoutingPolicy } from '../../../src/policy/types.js';
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
    // server.version is session negotiation — a second call on the same
    // session is rejected by ElectrumX and another server's answer doesn't
    // describe this session. Hard-excluded like broadcast/subscribe.
    const pVersion = manager.call('server.version', ['test', '1.4'], { hedge: true });
    await delay(50);
    // Well past afterMs and still nothing on b — hard exclusion beats overrides.
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      result: req.method === 'blockchain.transaction.broadcast' ? 'txid' : null,
    }));
    expect(await pBroadcast).toBe('txid');
    expect(await pSub).toBeNull();
    // Answered by MockTransport's handshake auto-reply — never left for b.
    expect(await pVersion).toEqual(['MockServer 0.0.0', '1.4']);

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

  // --- R1: per-item accounting across both hedge branches ------------------

  it('mixed winner defers retryable items to the in-flight sibling: a late sibling success resolves them with no extra dispatch', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0']);
    const p1 = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(0);
    const aT = h.transports.get('a')!;
    const bT = h.transports.get('b')!;
    const cT = h.transports.get('c')!;
    expect(aT.sent).toHaveLength(1);

    // Hedge fires on b: the same 2-item wire batch.
    await delay(50);
    expect(bT.sent).toHaveLength(1);

    // Primary answers MIXED: tx0 succeeds, tx1 fails retryably. The winner's
    // success must settle immediately (the latency win)...
    h.reply<WireReq>('a', (req) =>
      (req.params as unknown[])[0] === 'tx0'
        ? { id: req.id, result: 'a-tx0' }
        : { id: req.id, error: { code: -32603, message: 'excessive resource usage' } },
    );
    expect(await p0).toBe('a-tx0');

    // ...but tx1 must NOT be retried anywhere while b still holds it in
    // flight: no duplicate dispatch on b, nothing on c.
    await delay(20);
    expect(bT.sent).toHaveLength(1);
    expect(cT.sent).toHaveLength(0);

    // The sibling answers late with a SUCCESS for tx1 — a valid answer that
    // must resolve the caller instead of being discarded.
    h.reply<WireReq>('b', (req) => ({
      id: req.id,
      result: `b-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await p1).toBe('b-tx1');
    await delay(20);
    // No third-server dispatch for tx1, ever.
    expect(cT.sent).toHaveLength(0);

    await manager.stop();
  });

  it('mixed winner + sibling failure: the retry excludes BOTH hedge clients and never exceeds the attempt budget', async () => {
    const h = buildHarness();
    // Pool of four so an accounting bug has somewhere visible to overflow
    // into: budget 3 = a (primary) + b (hedge) + c (retry); d must stay
    // silent. If the sibling dispatch weren't counted (or b not excluded),
    // tx1 would get a fourth wire dispatch.
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS4,
      policy: failover(['a', 'b', 'c', 'd']),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0'], { retry: { maxAttempts: 3 } });
    const p1 = manager.call('blockchain.transaction.get', ['tx1'], { retry: { maxAttempts: 3 } });
    // Attach the rejection expectation before driving the wire.
    const p1Rejected = expect(p1).rejects.toMatchObject({
      message: expect.stringContaining('(from-c)') as string,
    });
    await delay(0);
    const aT = h.transports.get('a')!;
    const bT = h.transports.get('b')!;
    const cT = h.transports.get('c')!;
    const dT = h.transports.get('d')!;
    expect(aT.sent).toHaveLength(1);

    await delay(50);
    expect(bT.sent).toHaveLength(1);

    // Primary answers MIXED: tx0 ok, tx1 retryable.
    h.reply<WireReq>('a', (req) =>
      (req.params as unknown[])[0] === 'tx0'
        ? { id: req.id, result: 'a-tx0' }
        : { id: req.id, error: { code: -32603, message: 'excessive resource usage (from-a)' } },
    );
    expect(await p0).toBe('a-tx0');

    // Deferred: nothing new dispatched while the sibling is in flight.
    await delay(20);
    expect(bT.sent).toHaveLength(1);
    expect(cT.sent).toHaveLength(0);

    // Sibling also fails tx1 retryably → NOW the retry may fire, on c, with
    // both a and b excluded.
    h.reply<WireReq>('b', (req) =>
      (req.params as unknown[])[0] === 'tx0'
        ? { id: req.id, result: 'b-tx0' } // swallowed: tx0 already settled
        : { id: req.id, error: { code: -32603, message: 'excessive resource usage (from-b)' } },
    );
    await delay(20);
    expect(cT.sent).toHaveLength(1);
    // The retry re-pick must NOT have selected the sibling client again.
    expect(bT.sent).toHaveLength(0);
    expect(aT.sent).toHaveLength(0);

    // c fails too: budget (3) is spent after a + b (2) and c (1) — tx1
    // rejects with c's OWN error and d is never contacted.
    h.reply<WireReq>('c', (req) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage (from-c)' },
    }));
    await p1Rejected;
    await delay(20);
    expect(dT.sent).toHaveLength(0);

    await manager.stop();
  });

  // --- R2: per-item error preservation on all-retryable results arrays -----

  it("both branches all-retryable: each item rejects with ITS OWN error at budget exhaustion, not the last item's", async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS, // pool of 2 → default budget 2 = primary + hedge
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0']);
    const p1 = manager.call('blockchain.transaction.get', ['tx1']);
    const settledP = Promise.allSettled([p0, p1]);
    await delay(0);
    expect(h.transports.get('a')!.sent).toHaveLength(1);

    await delay(50);
    expect(h.transports.get('b')!.sent).toHaveLength(1);

    // Every item fails retryably on BOTH branches, each with a distinct
    // per-item error. Budget (2) is spent by the two dispatches.
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      error: {
        code: -32603,
        message: `excessive resource usage (${(req.params as unknown[])[0] as string}-a)`,
      },
    }));
    await delay(10);
    h.reply<WireReq>('b', (req) => ({
      id: req.id,
      error: {
        code: -32603,
        message: `excessive resource usage (${(req.params as unknown[])[0] as string}-b)`,
      },
    }));

    const settled = await settledP;
    const reasons = settled.map((r) => {
      expect(r.status).toBe('rejected');
      return ((r as PromiseRejectedResult).reason as Error).message;
    });
    // Each item carries its own cause (from the last-settled branch, b) —
    // not a collapsed copy of the last item's error.
    expect(reasons[0]).toContain('tx0-b');
    expect(reasons[1]).toContain('tx1-b');

    await manager.stop();
  });

  // --- R3: per-item hedge routing under request-dependent policies ----------

  /**
   * Sticky-style policy: routing depends on the request's stickyKey, so
   * per-item hedge picks can diverge once the primary is excluded.
   * Preference orders diverge after 'a': k1 → b, k2 → c.
   */
  const stickyStylePolicy = (): RoutingPolicy => ({
    pick(ctx) {
      const order = ctx.stickyKey === 'k2' ? ['a', 'c', 'b'] : ['a', 'b', 'c'];
      for (const id of order) {
        const c = ctx.candidates.find((v) => v.id === id);
        if (c && c.state === 'connected' && !ctx.excluded.has(id)) return id;
      }
      return null;
    },
  });

  it('skips the group hedge when per-item picks diverge under a sticky policy, waiting out the primary', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: stickyStylePolicy(),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p1 = manager.call('blockchain.transaction.get', ['tx-k1'], { stickyKey: 'k1' });
    const p2 = manager.call('blockchain.transaction.get', ['tx-k2'], { stickyKey: 'k2' });
    await delay(0);
    const aT = h.transports.get('a')!;
    expect(aT.sent).toHaveLength(1);
    expect(JSON.parse(aT.sent[0]!)).toHaveLength(2);

    // Past afterMs the hedge probe runs: k1 → b, k2 → c. Divergent picks
    // must NOT dispatch a hedge anywhere (splitting the batch would
    // multiply the race bookkeeping; grp[0]'s key must not decide for k2).
    await delay(50);
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    expect(h.transports.get('c')!.sent).toHaveLength(0);

    // The primary is waited out and still wins.
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      result: `a-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await p1).toBe('a-tx-k1');
    expect(await p2).toBe('a-tx-k2');

    await manager.stop();
  });

  it('dispatches the group hedge when per-item sticky picks converge on the same client', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: stickyStylePolicy(),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    // Same key on both items → per-item hedge picks agree on b.
    const p1 = manager.call('blockchain.transaction.get', ['tx0'], { stickyKey: 'k1' });
    const p2 = manager.call('blockchain.transaction.get', ['tx1'], { stickyKey: 'k1' });
    await delay(0);
    expect(h.transports.get('a')!.sent).toHaveLength(1);

    await delay(50);
    const bT = h.transports.get('b')!;
    expect(bT.sent).toHaveLength(1);
    expect(JSON.parse(bT.sent[0]!)).toHaveLength(2);
    expect(h.transports.get('c')!.sent).toHaveLength(0);

    h.reply<WireReq>('b', (req) => ({
      id: req.id,
      result: `b-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await p1).toBe('b-tx0');
    expect(await p2).toBe('b-tx1');

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

  it('winner non-retryable failure defers to the sibling: a late sibling success still wins', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0']);
    const p1 = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(0);
    const bT = h.transports.get('b')!;
    const cT = h.transports.get('c')!;
    await delay(50); // hedge fires on b
    expect(bT.sent).toHaveLength(1);

    // Winner answers MIXED with a NON-retryable failure for tx1. Unlike the
    // pre-fix behavior, tx1 must NOT reject yet — the sibling holds a live
    // dispatch that can still produce a valid answer (a non-retryable error
    // can be server-specific).
    h.reply<WireReq>('a', (req) =>
      (req.params as unknown[])[0] === 'tx0'
        ? { id: req.id, result: 'a-tx0' }
        : { id: req.id, error: { code: 2, message: 'no such tx' } },
    );
    expect(await p0).toBe('a-tx0');

    h.reply<WireReq>('b', (req) => ({
      id: req.id,
      result: `b-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await p1).toBe('b-tx1');
    await delay(20);
    expect(cT.sent).toHaveLength(0); // never retried anywhere

    await manager.stop();
  });

  it('winner non-retryable failure rejects with the WINNER error when the sibling also fails, without retrying', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0']);
    const p1 = manager.call('blockchain.transaction.get', ['tx1']);
    const rejected = expect(p1).rejects.toMatchObject({ code: 2, message: 'no such tx' });
    await delay(0);
    const bT = h.transports.get('b')!;
    const cT = h.transports.get('c')!;
    await delay(50); // hedge fires on b

    h.reply<WireReq>('a', (req) =>
      (req.params as unknown[])[0] === 'tx0'
        ? { id: req.id, result: 'a-tx0' }
        : { id: req.id, error: { code: 2, message: 'no such tx' } },
    );
    expect(await p0).toBe('a-tx0');

    // Sibling fails tx1 retryably — the winner's non-retryable answer must
    // dominate: reject with it, no third-server retry.
    h.reply<WireReq>('b', (req) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage' },
    }));
    await rejected;
    await delay(20);
    // reply() drained b's queue; the point is no retry landed anywhere.
    expect(bT.sent).toHaveLength(0);
    expect(cT.sent).toHaveLength(0);

    await manager.stop();
  });

  it('a policy that throws on the hedge probe degrades to no-hedge: primary still settles the batch', async () => {
    const h = buildHarness();
    const inner = failover(['a', 'b']);
    const throwing: RoutingPolicy = {
      pick(ctx) {
        if (ctx.probe) throw new Error('policy exploded on probe');
        return inner.pick(ctx);
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: throwing,
      transportFactory: h.factory,
      hedging: { afterMs: 10 },
    });
    const errors: unknown[] = [];
    manager.on('error', (e) => errors.push(e));
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0']);
    const p1 = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(40); // hedge timer fires, probe throws
    expect(errors.some((e) => (e as Error).message === 'policy exploded on probe')).toBe(true);
    expect(h.transports.get('b')!.sent).toHaveLength(0);

    // The primary is still live and must settle the callers.
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      result: `a-${(req.params as unknown[])[0] as string}`,
    }));
    expect(await p0).toBe('a-tx0');
    expect(await p1).toBe('a-tx1');

    await manager.stop();
  });

  it('a policy that throws on the single-path hedge probe degrades to no-hedge', async () => {
    const h = buildHarness();
    const inner = failover(['a', 'b']);
    const throwing: RoutingPolicy = {
      pick(ctx) {
        if (ctx.probe) throw new Error('policy exploded on probe');
        return inner.pick(ctx);
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: throwing,
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 10 },
    });
    const errors: unknown[] = [];
    manager.on('error', (e) => errors.push(e));
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(40);
    expect(errors).toHaveLength(1);
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    h.reply<WireReq>('a', (req) => ({ id: req.id, result: 'a-tx1' }));
    expect(await p).toBe('a-tx1');

    await manager.stop();
  });

  it('a policy that throws at flush time rejects the items instead of stranding them', async () => {
    const h = buildHarness();
    const throwing: RoutingPolicy = {
      pick() {
        throw new Error('policy exploded on flush');
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: throwing,
      transportFactory: h.factory,
    });
    const errors: unknown[] = [];
    manager.on('error', (e) => errors.push(e));
    await manager.start();

    const p = manager.call('blockchain.transaction.get', ['tx1']);
    await expect(p).rejects.toThrow('policy exploded on flush');
    expect(errors).toHaveLength(1);

    await manager.stop();
  });

  it('non-retryable sibling batch-error rejects deferred items instead of retrying them', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      hedging: { afterMs: 15 },
    });
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0']);
    const p1 = manager.call('blockchain.transaction.get', ['tx1']);
    const rejected = expect(p1).rejects.toMatchObject({
      name: 'RpcError',
      message: 'verbose transactions are currently unsupported',
    });
    await delay(0);
    const bT = h.transports.get('b')!;
    const cT = h.transports.get('c')!;
    // The hedge dispatch on b fails AT SEND TIME with a non-retryable
    // cause — the only way batchCall rejects as a whole.
    bT.nextSendError = new RpcError('verbose transactions are currently unsupported', 1);
    await delay(50); // hedge fires on b and dies at send

    // Winner answers mixed: tx1's retryable failure defers to the sibling,
    // which already failed fatally at batch level → reject with the
    // sibling's cause, never retry on c.
    h.reply<WireReq>('a', (req) =>
      (req.params as unknown[])[0] === 'tx0'
        ? { id: req.id, result: 'a-tx0' }
        : { id: req.id, error: { code: -32603, message: 'excessive resource usage' } },
    );
    expect(await p0).toBe('a-tx0');
    await rejected;
    await delay(20);
    expect(bT.sent).toHaveLength(0); // send died before enqueueing
    expect(cT.sent).toHaveLength(0);

    await manager.stop();
  });
});
