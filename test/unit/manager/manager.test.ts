import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { ElectrumManager } from '../../../src/manager.js';
import { failover, preferFastest, roundRobin } from '../../../src/policy/builtins.js';
import type { PickContext } from '../../../src/policy/types.js';
import type { ServerSpec } from '../../../src/protocol/types.js';
import { buildHarness } from '../../helpers/managerHarness.js';

const SERVERS: ServerSpec[] = [
  { id: 'a', host: 'a', port: 50001, protocol: 'ws' },
  { id: 'b', host: 'b', port: 50001, protocol: 'ws' },
];

describe('ElectrumManager — basic call routing', () => {
  it('routes a single call through the policy and returns the result', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const promise = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'pong' }));
    expect(await promise).toBe('pong');

    expect(h.transports.get('b')!.sent).toHaveLength(0);
    await manager.stop();
  });

  it('records success in telemetry visible via getClientViews', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'pong' }));
    await p;

    const views = manager.getClientViews();
    const a = views.find((v) => v.id === 'a')!;
    expect(a.telemetry.success.count).toBe(1);
    expect(a.telemetry.errors.consecutive).toBe(0);

    await manager.stop();
  });

  it('records a protocol error in telemetry when a server sends a malformed frame', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    const errors: unknown[] = [];
    manager.on('error', (e) => errors.push(e));
    await manager.start();

    h.transports.get('a')!.pushFromServer('garbage not json');

    const a = manager.getClientViews().find((v) => v.id === 'a')!;
    expect(a.telemetry.errors.lastKind).toBe('protocol');
    expect(a.telemetry.errors.consecutive).toBe(1);
    // Latency stats untouched — no zero sample dragging the EMA down.
    expect(a.telemetry.latency.samples).toBe(0);
    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).name).toBe('ProtocolError');

    await manager.stop();
  });

  it('recovers when policy returns a stale id (client no longer in pool)', async () => {
    const h = buildHarness();
    let calls = 0;
    const stalePolicy = {
      pick({ excluded }: { excluded: ReadonlySet<string> }) {
        calls++;
        if (calls === 1) return 'gone'; // not in pool
        if (excluded.has('gone')) return 'a';
        return null;
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [{ id: 'a', host: 'a', port: 1, protocol: 'ws' }],
      policy: stalePolicy,
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'pong' }));
    expect(await p).toBe('pong');
    expect(calls).toBeGreaterThanOrEqual(2);

    await manager.stop();
  });

  it('aborts when a buggy policy keeps returning the same stale id forever', async () => {
    const h = buildHarness();
    const stuckPolicy = {
      pick() {
        return 'gone'; // ignores `excluded`
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: stuckPolicy,
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await expect(manager.call('server.ping', [])).rejects.toThrow(/stale client ids/);
    await manager.stop();
  });

  it('forwards a consistent ctx.attempt to the policy across call paths', async () => {
    const h = buildHarness();
    const seen: number[] = [];
    const recordingPolicy = {
      pick({ candidates, excluded, now, attempt }: PickContext) {
        seen.push(attempt);
        const usable = candidates.find(
          (c) => c.state === 'connected' && !excluded.has(c.id) && (c.bannedUntil ?? 0) <= now,
        );
        return usable ? usable.id : null;
      },
    };
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: recordingPolicy,
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const p = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'pong' }));
    await p;

    expect(seen[0]).toBe(0);

    await manager.stop();
  });

  it('rejects with NoClientAvailableError when policy returns null', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [{ id: 'a', host: 'a', port: 1, protocol: 'ws' }],
      policy: { pick: () => null },
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await expect(manager.call('server.ping', [])).rejects.toThrow();
    await manager.stop();
  });
});

describe('ElectrumManager — failover and retry', () => {
  it('retries on a different client when first reports a transport error', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: roundRobin(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    const promise = manager.call('server.ping', []);
    await delay(0);
    // Server a closes — pending requests rejected with TransportError.
    const aT = h.transports.get('a')!;
    aT.pushClose(1006, 'abnormal');
    await delay(0);
    // Manager retries on b.
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: 'pong-b' }));
    expect(await promise).toBe('pong-b');

    await manager.stop();
  });

  it('rate-limit error bans the client and emits client-banned', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: false,
      cooldownMs: 5_000,
    });
    await manager.start();

    const banned: Array<{ clientId: string; until: number; reason: string }> = [];
    manager.on('client-banned', (p) => banned.push(p));

    const p = manager.call('server.ping', []);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      error: { code: -32603, message: 'excessive resource usage' },
    }));
    await delay(0);
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: 'pong' }));
    expect(await p).toBe('pong');

    expect(banned).toHaveLength(1);
    expect(banned[0]!.clientId).toBe('a');
    expect(banned[0]!.reason).toBe('rate-limit');
    const a = manager.getClientViews().find((v) => v.id === 'a')!;
    expect(a.bannedUntil).toBeGreaterThan(Date.now());
    expect(a.state).toBe('banned');

    await manager.stop();
  });

  it('does not retry on rpc-error (caller-owned)', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    const p = manager.call('blockchain.transaction.get', ['bad']);
    await delay(0);
    h.reply('a', (req: { id: number }) => ({
      id: req.id,
      error: { code: 2, message: 'no such tx' },
    }));
    await expect(p).rejects.toMatchObject({ code: 2, message: 'no such tx' });
    expect(h.transports.get('b')!.sent).toHaveLength(0);
    await manager.stop();
  });
});

describe('ElectrumManager — auto-batch coalescing', () => {
  it('packs same-microtask calls for one client into a single JSON-RPC array', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const p1 = manager.call('server.ping', []);
    const p2 = manager.call('server.ping', []);
    const p3 = manager.call('server.version', ['x', '1.4']);
    await delay(0);

    const aT = h.transports.get('a')!;
    expect(aT.sent).toHaveLength(1);
    const wire = JSON.parse(aT.sent[0]!);
    expect(Array.isArray(wire)).toBe(true);
    expect(wire).toHaveLength(3);

    aT.pushFromServer(
      JSON.stringify([
        { id: wire[0].id, result: 'pong-1' },
        { id: wire[1].id, result: 'pong-2' },
        { id: wire[2].id, result: ['srv', '1.4'] },
      ]),
    );

    expect(await Promise.all([p1, p2, p3])).toEqual(['pong-1', 'pong-2', ['srv', '1.4']]);
    await manager.stop();
  });

  it('routes per-item — different calls go to different clients in parallel batches', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: roundRobin(),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const p1 = manager.call('server.ping', []);
    const p2 = manager.call('server.ping', []);
    await delay(0);

    expect(h.transports.get('a')!.sent).toHaveLength(1);
    expect(h.transports.get('b')!.sent).toHaveLength(1);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'a' }));
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: 'b' }));

    const [r1, r2] = await Promise.all([p1, p2]);
    // Round-robin orders as a then b.
    expect(r1).toBe('a');
    expect(r2).toBe('b');
    await manager.stop();
  });

  it('redirects per-item failures within a batch to a different server', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const p1 = manager.call('server.ping', []);
    const p2 = manager.call('server.ping', []);
    await delay(0);

    // a returns: first ok, second a rate-limit-class error (also retryable).
    const aT = h.transports.get('a')!;
    const wireA = JSON.parse(aT.sent[0]!);
    aT.pushFromServer(
      JSON.stringify([
        { id: wireA[0].id, result: 'ok-a' },
        { id: wireA[1].id, error: { code: -32603, message: 'excessive resource usage' } },
      ]),
    );
    // a got banned for the second item; retry routes to b.
    await delay(0);
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: 'ok-b' }));

    expect(await p1).toBe('ok-a');
    expect(await p2).toBe('ok-b');

    await manager.stop();
  });

  it('reroutes a whole batch rejected with an id:null error (Fulcrum batch limit)', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: failover(['a', 'b']),
      transportFactory: h.factory,
      autoBatch: true,
    });
    await manager.start();

    const promises = Array.from({ length: 4 }, () => manager.call('server.ping', []));
    await delay(0);

    // a rejects the entire batch Fulcrum-style: single error, id null.
    const aT = h.transports.get('a')!;
    expect(JSON.parse(aT.sent[0]!)).toHaveLength(4);
    aT.pushFromServer(
      '{"jsonrpc":"2.0","id":null,"error":{"code":4,"message":"Batch limit exceeded"}}',
    );
    await delay(0);

    // Classifier maps "batch limit exceeded" → rate-limit: `a` gets a
    // cooldown ban and every item retries on `b`.
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: 'ok-b' }));
    expect(await Promise.all(promises)).toEqual(['ok-b', 'ok-b', 'ok-b', 'ok-b']);

    const a = manager.getClientViews().find((v) => v.id === 'a')!;
    expect(a.state).toBe('banned');
    expect(a.telemetry.errors.lastKind).toBe('rate-limit');

    await manager.stop();
  });
});

describe('ElectrumManager — pool mutation', () => {
  it('addServer brings a new server into the rotation', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: [SERVERS[0]!],
      policy: roundRobin(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    manager.addServer(SERVERS[1]!);
    await delay(0);
    expect(manager.getClientViews()).toHaveLength(2);
    await manager.stop();
  });

  it('removeServer drops the client and disconnects it', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: roundRobin(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();
    await manager.removeServer('a');
    expect(manager.getClientViews().map((v) => v.id)).toEqual(['b']);
    expect(h.transports.get('a')!.connected).toBe(false);
    await manager.stop();
  });
});

describe('ElectrumManager — telemetry feeds preferFastest', () => {
  it('routes subsequent calls to the lowest-latency tested client', async () => {
    const h = buildHarness();
    const manager = new ElectrumManager({
      network: 'regtest',
      servers: SERVERS,
      policy: preferFastest(),
      transportFactory: h.factory,
      autoBatch: false,
    });
    await manager.start();

    // Warm both in parallel so each gets a sample. With leastInFlight tiebreak
    // and untested-clients-treated-as-eligible-but-not-monopolizing, the first
    // call lands on a (both 0 inFlight; first wins) and the second on b (a now
    // has 1 inFlight, b has 0).
    const w1 = manager.call('server.ping', []);
    const w2 = manager.call('server.ping', []);
    await delay(0);
    // a replies fast.
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'a' }));
    // b replies slowly so its EMA inflates past a's.
    await delay(30);
    h.reply('b', (req: { id: number }) => ({ id: req.id, result: 'b' }));
    await Promise.all([w1, w2]);

    const warm = manager.getClientViews();
    const aWarm = warm.find((v) => v.id === 'a')!;
    const bWarm = warm.find((v) => v.id === 'b')!;
    expect(aWarm.telemetry.latency.samples).toBeGreaterThanOrEqual(1);
    expect(bWarm.telemetry.latency.samples).toBeGreaterThanOrEqual(1);
    expect(bWarm.telemetry.latency.ema).toBeGreaterThan(aWarm.telemetry.latency.ema);

    // Third call must go to a (lower EMA), not b.
    const w3 = manager.call('server.ping', []);
    await delay(0);
    expect(h.transports.get('a')!.sent.length).toBeGreaterThan(0);
    expect(h.transports.get('b')!.sent.length).toBe(0);
    h.reply('a', (req: { id: number }) => ({ id: req.id, result: 'a-3' }));
    expect(await w3).toBe('a-3');

    await manager.stop();
  });
});
