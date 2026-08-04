import { setTimeout as delay } from 'node:timers/promises';

import { describe, expect, it } from 'vitest';

import { ElectrumClient } from '../../../src/client.js';
import { RpcError, TimeoutError, TransportError } from '../../../src/errors/types.js';
import { MockTransport } from '../../helpers/mockTransport.js';

describe('ElectrumClient', () => {
  it('round-trips a successful call', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const promise = client.call<{ confirmed: number; unconfirmed: number }>(
      'blockchain.scripthash.get_balance',
      ['ab'],
    );
    // Allow the send to flush through transport.
    await delay(0);
    expect(transport.sent).toHaveLength(1);
    const sentReq = JSON.parse(transport.sent[0]!);
    expect(sentReq).toMatchObject({
      jsonrpc: '2.0',
      method: 'blockchain.scripthash.get_balance',
      params: ['ab'],
    });
    const id: number = sentReq.id;

    transport.pushFromServer(
      `{"jsonrpc":"2.0","id":${id},"result":{"confirmed":42,"unconfirmed":0}}`,
    );

    expect(await promise).toEqual({ confirmed: 42, unconfirmed: 0 });
  });

  it('rejects with RpcError when server returns an error response', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const promise = client.call('blockchain.transaction.get', ['deadbeef']);
    await delay(0);
    const id = JSON.parse(transport.sent[0]!).id;

    transport.pushFromServer(
      `{"jsonrpc":"2.0","id":${id},"error":{"code":2,"message":"missing tx"}}`,
    );

    await expect(promise).rejects.toBeInstanceOf(RpcError);
    await expect(promise).rejects.toMatchObject({ code: 2, message: 'missing tx' });
  });

  it('times out a request that gets no reply', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({
      id: 'a',
      endpoint: transport.endpoint,
      transport,
      requestTimeoutMs: 30,
    });
    await client.connect();

    await expect(client.call('server.ping', [])).rejects.toBeInstanceOf(TimeoutError);
  });

  it('uses unique sequential ids', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    void client.call('server.ping', []);
    void client.call('server.ping', []);
    void client.call('server.ping', []);
    await delay(0);

    const ids = transport.sent.map((s) => JSON.parse(s).id);
    expect(new Set(ids).size).toBe(3);
  });

  it('routes a server notification to the registered listener', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const seen: unknown[] = [];
    client.onNotification((n) => seen.push(n));

    transport.pushFromServer(
      '{"jsonrpc":"2.0","method":"blockchain.headers.subscribe","params":[{"height":7,"hex":"00"}]}',
    );

    expect(seen).toHaveLength(1);
    expect(seen[0]).toEqual({
      method: 'blockchain.headers.subscribe',
      params: [{ height: 7, hex: '00' }],
    });
  });

  it('drops malformed inbound frames without crashing', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    transport.pushFromServer('not json');
    transport.pushFromServer('{"id":1,"result":"orphan"}'); // unknown id

    // No error thrown, no in-flight to reject. Issue a real call afterwards to
    // confirm the client is still healthy.
    const p = client.call('server.ping', []);
    await delay(0);
    const id = JSON.parse(transport.sent[0]!).id;
    transport.pushFromServer(`{"id":${id},"result":null}`);
    expect(await p).toBeNull();
  });

  it('rejects all in-flight requests when the transport closes unexpectedly', async () => {
    const transport = new MockTransport();
    // This test exercises raw client.call('server.version', ...) and
    // expects it to be in-flight when the close arrives — opt out of
    // the harness's auto-reply.
    transport.autoReplyVersion = false;
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const a = client.call('server.ping', []);
    const b = client.call('server.version', ['x', '1.4']);
    await delay(0);

    transport.pushClose(1006, 'abnormal');

    await expect(a).rejects.toBeInstanceOf(TransportError);
    await expect(b).rejects.toBeInstanceOf(TransportError);
    expect(client.getState()).toBe('disconnected');
  });

  it('refuses calls before connect', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await expect(client.call('server.ping', [])).rejects.toBeInstanceOf(TransportError);
  });

  it('handles a batch response array', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const a = client.call('server.ping', []);
    const b = client.call('server.ping', []);
    await delay(0);
    const sentA = JSON.parse(transport.sent[0]!);
    const sentB = JSON.parse(transport.sent[1]!);

    transport.pushFromServer(`[{"id":${sentA.id},"result":1},{"id":${sentB.id},"result":2}]`);

    expect(await a).toBe(1);
    expect(await b).toBe(2);
  });

  it('refuses concurrent connect', async () => {
    const transport = new MockTransport();
    // Make connect() wait so the second call lands while the first is in flight.
    let release!: () => void;
    transport.connect = () =>
      new Promise<void>((r) => {
        release = r;
      });
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });

    const first = client.connect();
    await expect(client.connect()).rejects.toBeInstanceOf(TransportError);
    release();
    await first;
  });

  it('does not accumulate transport listeners across close/reconnect cycles', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    let notifications = 0;
    client.onNotification(() => notifications++);

    await client.connect();
    transport.pushClose(1006);
    await client.connect();
    transport.pushClose(1006);
    await client.connect();

    // With a leaked listener per reconnect, this notification would be
    // dispatched once per surviving listener instead of exactly once.
    transport.pushFromServer('{"method":"blockchain.headers.subscribe","params":[{"height":1}]}');
    expect(notifications).toBe(1);
  });

  it('reports malformed frames via onProtocolError and keeps the stream alive', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    const seen: Error[] = [];
    client.onProtocolError((e) => seen.push(e));
    await client.connect();

    transport.pushFromServer('this is not json');
    expect(seen).toHaveLength(1);
    expect(seen[0]!.name).toBe('ProtocolError');

    // Stream survives: a valid response after the garbage still resolves.
    const p = client.call('server.ping', []);
    await delay(0);
    const id = JSON.parse(transport.sent[0]!).id;
    transport.pushFromServer(`{"jsonrpc":"2.0","id":${id},"result":null}`);
    expect(await p).toBe(null);
  });

  it('a serialization failure leaves no in-flight entry behind', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    // Circular params make JSON.stringify (encodeRequest) throw AFTER the
    // in-flight entry is registered. The entry must be unwound — a leaked
    // entry would fire its timeout against a promise nobody holds.
    const circular: Record<string, unknown> = {};
    circular['self'] = circular;
    await expect(client.call('server.ping', [circular])).rejects.toThrow();
    expect(client.inFlightCount).toBe(0);

    await expect(
      client.batchCall([{ method: 'server.ping', params: [circular] }]),
    ).rejects.toThrow();
    expect(client.inFlightCount).toBe(0);
  });

  it('disconnect closes transport and rejects pending', async () => {
    const transport = new MockTransport();
    const client = new ElectrumClient({ id: 'a', endpoint: transport.endpoint, transport });
    await client.connect();

    const p = client.call('server.ping', []);
    await delay(0);
    await client.disconnect();

    await expect(p).rejects.toBeInstanceOf(TransportError);
    expect(transport.connected).toBe(false);
  });
});
