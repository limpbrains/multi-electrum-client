// Re-batched retries: when a wire batch dies (whole-batch transport cut or
// per-item retryable errors), the surviving items must retry on the next
// server as ONE wire batch — not degrade into N sequential single calls.
// Assertions here are on the fake transport's observed wire traffic (send
// count + JSON array length per send), not just on promise outcomes.

import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { SuspendedError } from '../../../src/errors/types.js';
import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
import type { RoutingPolicy } from '../../../src/policy/types.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS3: ServerSpec[] = [
  { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
  { id: 'b', host: 'b', port: 50001, protocol: 'ws' },
  { id: 'c', host: 'c', port: 50001, protocol: 'ws' },
];

interface WireReq {
  id: number;
  method: string;
  params: [string];
}

describe('ElectrumManager — re-batched retries', () => {
  it('retries the survivors of a dead batch as ONE wire batch on the next server', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const promises = Array.from({ length: 5 }, (_, i) =>
      manager.call('blockchain.transaction.get', [`tx${i}`]),
    );
    await delay(0);

    const aT = h.transports.get('a')!;
    expect(aT.sent).toHaveLength(1);
    expect(JSON.parse(aT.sent[0]!)).toHaveLength(5);

    // Transport cut mid-flight: every item fails with a retryable TransportError.
    aT.pushClose(1006, 'cut');
    await delay(0);

    // All 5 survivors move to b as a single JSON-RPC batch — not 5 singles.
    const bT = h.transports.get('b')!;
    expect(bT.sent).toHaveLength(1);
    const wireB = JSON.parse(bT.sent[0]!) as WireReq[];
    expect(Array.isArray(wireB)).toBe(true);
    expect(wireB).toHaveLength(5);

    h.reply<WireReq>('b', (req) => ({ id: req.id, result: `raw-${req.params[0]}` }));
    expect(await Promise.all(promises)).toEqual([
      'raw-tx0',
      'raw-tx1',
      'raw-tx2',
      'raw-tx3',
      'raw-tx4',
    ]);
    // Third server never contacted — one re-pick served the whole group.
    expect(h.transports.get('c')!.sent).toHaveLength(0);

    await manager.stop();
  });

  it('settles a mixed batch correctly: success, fatal reject, retryables re-batched', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const promises = Array.from({ length: 4 }, (_, i) =>
      manager.call('blockchain.transaction.get', [`tx${i}`]),
    );
    // Attach the rejection expectation before driving the wire so the fatal
    // item's reject never races an unhandled-rejection report.
    const fatal = expect(promises[1]).rejects.toMatchObject({ code: 2, message: 'no such tx' });
    await delay(0);

    const aT = h.transports.get('a')!;
    const wireA = JSON.parse(aT.sent[0]!) as WireReq[];
    expect(wireA).toHaveLength(4);
    aT.pushFromServer(
      JSON.stringify([
        { id: wireA[0]!.id, result: 'ok-tx0' },
        // rpc-error → non-retryable, rejects immediately.
        { id: wireA[1]!.id, error: { code: 2, message: 'no such tx' } },
        // rate-limit → retryable.
        { id: wireA[2]!.id, error: { code: -32603, message: 'excessive resource usage' } },
        { id: wireA[3]!.id, error: { code: -32603, message: 'excessive resource usage' } },
      ]),
    );
    await delay(0);

    // Only the two retryable items move on, together, in one wire batch,
    // with their original params intact (id/order mapping survived).
    const bT = h.transports.get('b')!;
    expect(bT.sent).toHaveLength(1);
    const wireB = JSON.parse(bT.sent[0]!) as WireReq[];
    expect(wireB.map((r) => r.params[0])).toEqual(['tx2', 'tx3']);

    h.reply<WireReq>('b', (req) => ({ id: req.id, result: `ok-${req.params[0]}` }));

    expect(await promises[0]).toBe('ok-tx0');
    await fatal;
    expect(await promises[2]).toBe('ok-tx2');
    expect(await promises[3]).toBe('ok-tx3');

    await manager.stop();
  });

  it('rejects retryable items with the last error when the retry budget exhausts', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3.slice(0, 2), // pool of 2 → max 2 attempts per item
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const promises = Array.from({ length: 3 }, (_, i) =>
      manager.call('blockchain.transaction.get', [`tx${i}`]),
    );
    // Attach handlers up front — the final rejection must never sit
    // handler-less across a macrotask boundary.
    const settledP = Promise.allSettled(promises);
    await delay(0);

    h.transports.get('a')!.pushClose(1006, 'cut-a');
    await delay(0);

    // Survivors re-batched onto b...
    const bT = h.transports.get('b')!;
    expect(bT.sent).toHaveLength(1);
    expect(JSON.parse(bT.sent[0]!)).toHaveLength(3);

    // ...which also dies. Budget (pool size = 2) is now spent: every item
    // rejects with the LAST error (b's transport cut).
    bT.pushClose(1006, 'cut-b');
    const settled = await settledP;
    for (const r of settled) {
      expect(r.status).toBe('rejected');
      const err = (r as PromiseRejectedResult).reason as Error;
      expect(err.name).toBe('TransportError');
      expect(err.message).toContain('cut-b');
    }

    await manager.stop();
  });

  it('re-picks retry routing PER ITEM: items with different sticky keys land on their own targets', async () => {
    const h = buildHarness();
    // Deterministic sticky-style policy: routing depends on the request's
    // stickyKey, so a group-level representative pick would send BOTH items
    // to the first item's target. Preference orders diverge after 'a' dies.
    const policy: RoutingPolicy = {
      pick(ctx) {
        const order = ctx.stickyKey === 'k2' ? ['a', 'c', 'b'] : ['a', 'b', 'c'];
        for (const id of order) {
          const c = ctx.candidates.find((v) => v.id === id);
          if (c && c.state === 'connected' && !ctx.excluded.has(id)) return id;
        }
        return null;
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy,
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const p1 = manager.call('blockchain.transaction.get', ['tx-k1'], { stickyKey: 'k1' });
    const p2 = manager.call('blockchain.transaction.get', ['tx-k2'], { stickyKey: 'k2' });
    await delay(0);

    // Both stickies prefer 'a' initially → one coalesced batch of 2.
    const aT = h.transports.get('a')!;
    expect(aT.sent).toHaveLength(1);
    expect(JSON.parse(aT.sent[0]!)).toHaveLength(2);

    aT.pushClose(1006, 'cut-a');
    await delay(0);

    // The retry must route per item: k1 → b, k2 → c. A representative-based
    // group pick would put both on b and leave c untouched.
    const bT = h.transports.get('b')!;
    const cT = h.transports.get('c')!;
    expect(bT.sent).toHaveLength(1);
    expect(cT.sent).toHaveLength(1);
    const wireB = JSON.parse(bT.sent[0]!) as WireReq[];
    const wireC = JSON.parse(cT.sent[0]!) as WireReq[];
    expect(wireB.map((r) => r.params[0])).toEqual(['tx-k1']);
    expect(wireC.map((r) => r.params[0])).toEqual(['tx-k2']);

    h.reply<WireReq>('b', (req) => ({ id: req.id, result: `b-${req.params[0]}` }));
    h.reply<WireReq>('c', (req) => ({ id: req.id, result: `c-${req.params[0]}` }));
    expect(await p1).toBe('b-tx-k1');
    expect(await p2).toBe('c-tx-k2');

    await manager.stop();
  });

  it('rejects every pending item (instead of hanging) when the redispatch path throws unexpectedly', async () => {
    const h = buildHarness();
    const boom = new Error('boom-policy');
    // First flush routes normally; the retry re-pick (attempt > 0) throws —
    // the shape of a buggy user policy blowing up inside redispatch.
    const policy: RoutingPolicy = {
      pick(ctx) {
        if (ctx.attempt > 0) throw boom;
        const c = ctx.candidates.find((v) => v.state === 'connected' && !ctx.excluded.has(v.id));
        return c ? c.id : null;
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy,
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();
    const errors: unknown[] = [];
    manager.on('error', (e) => errors.push(e));

    const promises = Array.from({ length: 3 }, (_, i) =>
      manager.call('blockchain.transaction.get', [`tx${i}`]),
    );
    const settledP = Promise.allSettled(promises);
    await delay(0);

    const aT = h.transports.get('a')!;
    expect(aT.sent).toHaveLength(1);
    aT.pushClose(1006, 'cut-a');

    // Every item's promise must reject with the thrown error — not time out.
    const settled = await settledP;
    for (const r of settled) {
      expect(r.status).toBe('rejected');
      expect((r as PromiseRejectedResult).reason).toBe(boom);
    }
    // The failure is still surfaced for observability.
    expect(errors).toContain(boom);

    await manager.stop();
  });

  it('rejects retryable items with the last error when no eligible client remains', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3, // pool of 3 → budget not the limiting factor here
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    // b and c go away before the retry — the re-pick must find nothing.
    h.transports.get('b')!.pushClose(1006, 'gone-b');
    h.transports.get('c')!.pushClose(1006, 'gone-c');

    const promises = Array.from({ length: 3 }, (_, i) =>
      manager.call('blockchain.transaction.get', [`tx${i}`]),
    );
    const settledP = Promise.allSettled(promises);
    await delay(0);

    const aT = h.transports.get('a')!;
    expect(aT.sent).toHaveLength(1);
    aT.pushClose(1006, 'cut-a');
    await delay(0);

    const settled = await settledP;
    for (const r of settled) {
      expect(r.status).toBe('rejected');
      const err = (r as PromiseRejectedResult).reason as Error;
      // Each item carries its own last real failure (a's cut), matching the
      // single path's `throw lastErr` on pool exhaustion.
      expect(err.name).toBe('TransportError');
      expect(err.message).toContain('cut-a');
    }

    await manager.stop();
  });

  it('a throwing custom classifier settles auto-batched items via fallback classification', async () => {
    const h = buildHarness();
    const throwingClassifier = {
      classify(): never {
        throw new Error('classifier exploded');
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      classifier: throwingClassifier,
    });
    const errors: unknown[] = [];
    manager.on('error', (e) => errors.push(e));
    await manager.start();

    const p0 = manager.call('blockchain.transaction.get', ['tx0']);
    const p1 = manager.call('blockchain.transaction.get', ['tx1']);
    await delay(0);

    // Both items fail with a rate-limit-shaped error; the custom classifier
    // explodes on every classification. sendGroup must keep its
    // never-rejects contract: fallback classification (built-in default →
    // retryable) re-routes the batch to b instead of stranding the callers.
    h.reply<WireReq>('a', (req) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage' },
    }));
    await delay(10);
    expect(errors.some((e) => (e as Error).message === 'classifier exploded')).toBe(true);

    h.reply<WireReq>('b', (req) => ({ id: req.id, result: `b-${req.params[0]}` }));
    expect(await p0).toBe('b-tx0');
    expect(await p1).toBe('b-tx1');

    await manager.stop();
  });
});

describe('ElectrumManager — stop() and queued batch work', () => {
  it('does not put a microtask-batched call on the wire after stop()', async () => {
    // `call()` only queues; the flush runs on a microtask. stop() marked
    // the manager stopped and yielded, so the already-queued flush ran
    // afterwards and dispatched to a socket that was still connected —
    // a wire side effect after the caller asked for teardown, and a
    // transport rejection instead of SuspendedError.
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();
    const aT = h.transports.get('a')!;
    const sentBefore = aT.sent.length;

    const call = manager.call('blockchain.transaction.get', ['tx0']);
    const stopping = manager.stop();

    await expect(call).rejects.toThrow(SuspendedError);
    await stopping;
    expect(aT.sent).toHaveLength(sentBefore);
  });
});

describe('ElectrumManager — teardown intent stops new wire work', () => {
  it('does not dispatch a retry after suspend was requested', async () => {
    // Letting an already-dispatched request finish during the suspend
    // grace is deliberate. Starting a NEW attempt is not: it goes out on
    // a socket suspend is closing, and its answer arrives for a caller
    // the manager is about to fail anyway.
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    const call = manager.call('blockchain.transaction.get', ['tx0']);
    await delay(0);
    expect(h.transports.get('a')!.sent).toHaveLength(1);

    const suspending = manager.suspend({ graceMs: 0 });
    h.transports.get('a')!.pushClose(1006, 'cut');

    await expect(call).rejects.toMatchObject({ name: 'SuspendedError' });
    await suspending;
    // b was never asked: the failover retry would have been new traffic.
    expect(h.transports.get('b')?.sent ?? []).toHaveLength(0);
    await manager.stop();
  });

  it('rejects a non-finite suspend grace instead of hanging teardown', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    // An unbounded grace would park the drain loop forever, and stop()
    // waits on that very transition. NaN is the opposite failure: every
    // deadline comparison is false, so the drain the caller asked for is
    // silently skipped.
    await expect(manager.suspend({ graceMs: Number.POSITIVE_INFINITY })).rejects.toThrow(
      RangeError,
    );
    await expect(manager.suspend({ graceMs: Number.NaN })).rejects.toThrow(RangeError);

    // The rejection leaves the manager where it was: validation runs
    // before the transition mutates anything, so a bad argument cannot
    // strand it in `suspending` with calls queueing behind it.
    expect(manager.state).toBe('running');
    const ping = manager.call('server.ping', [], { retry: 'none' });
    await delay(0);
    h.reply('a', (req: { id: number; method: string }) =>
      req.method === 'server.ping' ? { id: req.id, result: null } : undefined,
    );
    await expect(ping).resolves.toBeNull();

    // `null` means "unset" for an optional option, the way `?? 2000`
    // reads it — validating before that default would turn it into an
    // error for anyone forwarding a config value.
    await manager.suspend({ graceMs: null as unknown as number });
    expect(manager.state).toBe('suspended');
    await manager.stop();
  });
});

describe('ElectrumManager — teardown intent covers every dispatch site', () => {
  it('does not send a first request that was queued behind an await', async () => {
    // The entry check passes, then the call waits (a cache read, a
    // rebind continuation). By the time it wants the wire, teardown has
    // been requested — this dispatch had not gone out yet, so it must not.
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();
    const aT = h.transports.get('a')!;
    const sentBefore = aT.sent.length;

    const call = manager.call('blockchain.transaction.get', ['tx0']);
    const suspending = manager.suspend({ graceMs: 0 });

    await expect(call).rejects.toMatchObject({ name: 'SuspendedError' });
    await suspending;
    expect(aT.sent).toHaveLength(sentBefore);
    await manager.stop();
  });

  it('does not re-batch survivors after suspend was requested', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy: failover(['a', 'b', 'c']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const calls = Array.from({ length: 3 }, (_, i) =>
      manager.call('blockchain.transaction.get', [`tx${i}`]),
    );
    await delay(0);
    expect(h.transports.get('a')!.sent).toHaveLength(1);

    // Suspend enters its grace, then the server rate-limits every item:
    // those are retryable, and the re-batch must not travel to b.
    const suspending = manager.suspend({ graceMs: 50 });
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage' },
    }));

    const reasons = await Promise.all(
      calls.map((p) =>
        p.then(
          () => 'ok',
          (e: Error) => e.name,
        ),
      ),
    );
    await suspending;
    // Rejected because the manager is going down, not because the retry
    // that would have gone to b failed on its own.
    expect(reasons).toEqual(['SuspendedError', 'SuspendedError', 'SuspendedError']);
    expect(h.transports.get('b')?.sent ?? []).toHaveLength(0);
    await manager.stop();
  });
});

describe('ElectrumManager — policy re-entrancy cannot outrun teardown', () => {
  it('a policy that suspends from inside pick() does not get its request sent', async () => {
    // The routing policy is user code and it runs between the caller's
    // teardown check and the dispatch. Without a check at the wire, the
    // request went out on a manager the policy itself had just suspended:
    // the caller saw SuspendedError (suspend fails everything in flight)
    // while the server had already been asked to do the work.
    const h = buildHarness();
    let manager!: ElectrumManager;
    let suspended: Promise<void> | undefined;
    const policy: RoutingPolicy = {
      pick(ctx) {
        // Only for the call under test: start()'s own handshake routes
        // through here too.
        if (ctx.request.method === 'blockchain.transaction.get') {
          suspended ??= manager.suspend({ graceMs: 0 });
        }
        return ctx.candidates[0]?.id ?? null;
      },
    };
    manager = new ElectrumManager({
      network: 'regtest',
      servers: [SERVERS3[0]!],
      policy,
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    const aT = h.transports.get('a')!;
    const sentBefore = aT.sent.length;

    await expect(
      manager.call('blockchain.transaction.get', ['tx0'], { retry: 'none' }),
    ).rejects.toMatchObject({ name: 'SuspendedError' });
    await suspended;

    expect(aT.sent).toHaveLength(sentBefore);
    await manager.stop();
  });

  it('holds for the hedged path too, and settles the caller', async () => {
    // The hedge races two dispatches and attaches no rejection handler to
    // the second one, so the guard must report through the routing
    // result rather than by throwing: a rejection here left the caller's
    // promise pending forever and raised an unhandled rejection.
    const h = buildHarness();
    let manager!: ElectrumManager;
    let suspended: Promise<void> | undefined;
    const policy: RoutingPolicy = {
      pick(ctx) {
        if (ctx.request.method === 'blockchain.transaction.get') {
          suspended ??= manager.suspend({ graceMs: 0 });
        }
        return ctx.candidates[0]?.id ?? null;
      },
    };
    manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS3,
      policy,
      transportFactory: h.factory,
      autoBatch: false,
      hedging: { afterMs: 5 },
    });
    await manager.start();
    const sentBefore = SERVERS3.map((s) => h.transports.get(s.host)?.sent.length ?? 0);

    const settled = await Promise.race([
      manager.call('blockchain.transaction.get', ['tx0']).then(
        () => 'resolved',
        (e: Error) => e.name,
      ),
      delay(300).then(() => 'never settled'),
    ]);
    await suspended;

    expect(settled).toBe('SuspendedError');
    expect(SERVERS3.map((s) => h.transports.get(s.host)?.sent.length ?? 0)).toEqual(sentBefore);
    await manager.stop();
  });

  it('holds for the auto-batch path too', async () => {
    const h = buildHarness();
    let manager!: ElectrumManager;
    let suspended: Promise<void> | undefined;
    const policy: RoutingPolicy = {
      pick(ctx) {
        if (ctx.request.method === 'blockchain.transaction.get') {
          suspended ??= manager.suspend({ graceMs: 0 });
        }
        return ctx.candidates[0]?.id ?? null;
      },
    };
    manager = new ElectrumManager({
      network: 'regtest',
      servers: [SERVERS3[0]!],
      policy,
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();
    const aT = h.transports.get('a')!;
    const sentBefore = aT.sent.length;

    const outcome = await Promise.race([
      manager.call('blockchain.transaction.get', ['tx0']).then(
        () => 'resolved',
        (e: Error) => e.name,
      ),
      delay(300).then(() => 'never settled'),
    ]);
    await suspended;

    expect(outcome).toBe('SuspendedError');
    expect(aT.sent).toHaveLength(sentBefore);
    await manager.stop();
  });
});
