// Re-batched retries: when a wire batch dies (whole-batch transport cut or
// per-item retryable errors), the surviving items must retry on the next
// server as ONE wire batch — not degrade into N sequential single calls.
// Assertions here are on the fake transport's observed wire traffic (send
// count + JSON array length per send), not just on promise outcomes.

import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { ElectrumManager } from '../../../src/manager.js';
import { failover } from '../../../src/policy/builtins.js';
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
});
