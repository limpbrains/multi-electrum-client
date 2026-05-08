import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { ElectrumClient } from '../../../src/client.js';
import { TransportError } from '../../../src/errors/types.js';
import { MockTransport } from '../../helpers/mockTransport.js';

describe('ElectrumClient.batchCall', () => {
  it('sends one JSON-RPC array and resolves per-item Results in order', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const promise = client.batchCall([
      { method: 'server.ping', params: [] },
      { method: 'blockchain.transaction.get', params: ['txid1'] },
      { method: 'server.ping', params: [] },
    ]);

    await delay(0);
    expect(transport.sent).toHaveLength(1);
    const wire = JSON.parse(transport.sent[0]!);
    expect(Array.isArray(wire)).toBe(true);
    expect(wire).toHaveLength(3);
    const [a, b, c] = wire;
    transport.pushFromServer(
      JSON.stringify([
        { id: a.id, result: 'pong' },
        { id: b.id, error: { code: 2, message: 'no such tx' } },
        { id: c.id, result: 'pong' },
      ]),
    );

    const results = await promise;
    expect(results).toHaveLength(3);
    expect(results[0]).toEqual({ ok: true, value: 'pong' });
    const second = results[1]!;
    expect(second.ok).toBe(false);
    if (!second.ok) {
      expect(second.error.message).toBe('no such tx');
    }
    expect(results[2]).toEqual({ ok: true, value: 'pong' });
  });

  it('handles out-of-order responses', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const promise = client.batchCall([
      { method: 'server.ping', params: [] },
      { method: 'server.ping', params: [] },
    ]);
    await delay(0);
    const wire = JSON.parse(transport.sent[0]!);

    transport.pushFromServer(
      JSON.stringify([
        { id: wire[1].id, result: 'second' },
        { id: wire[0].id, result: 'first' },
      ]),
    );

    const results = await promise;
    expect(results[0]).toEqual({ ok: true, value: 'first' });
    expect(results[1]).toEqual({ ok: true, value: 'second' });
  });

  it('returns [] for an empty batch without sending anything', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();
    expect(await client.batchCall([])).toEqual([]);
    expect(transport.sent).toHaveLength(0);
  });

  it('rejects entire promise when transport.send fails', async () => {
    const transport = new MockTransport();
    transport.send = async () => {
      throw new TransportError('send failed');
    };
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();
    await expect(client.batchCall([{ method: 'server.ping', params: [] }])).rejects.toBeInstanceOf(
      TransportError,
    );
  });

  it('refuses batch when not connected', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await expect(client.batchCall([{ method: 'server.ping', params: [] }])).rejects.toBeInstanceOf(
      TransportError,
    );
  });
});
