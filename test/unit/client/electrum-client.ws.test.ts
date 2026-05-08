// Integration-style test that wires ElectrumClient on top of a real WsTransport
// against a local `ws` server faking an Electrum endpoint. This exercises the
// full M1 happy path (framing + transport + client) without Docker.

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { WebSocket as WsWebSocket } from 'ws';

import { ElectrumClient } from '../../../src/client.js';
import { WsTransport } from '../../../src/transport/ws.js';
import { startTestWsServer, type TestWsServer } from '../../helpers/wsTestServer.js';

const WebSocketCtor = WsWebSocket as unknown as new (url: string) => WebSocket;

describe('ElectrumClient over WsTransport (M1 happy path)', () => {
  let srv: TestWsServer;

  beforeEach(async () => {
    srv = await startTestWsServer();
  });

  afterEach(async () => {
    await srv.close();
  });

  it('does server.ping and server.version against a faked server', async () => {
    srv.server.on('connection', (sock) => {
      sock.on('message', (raw) => {
        const text = raw.toString('utf-8').trim();
        const req = JSON.parse(text);
        if (req.method === 'server.ping') {
          sock.send(JSON.stringify({ jsonrpc: '2.0', id: req.id, result: null }) + '\n');
        } else if (req.method === 'server.version') {
          sock.send(
            JSON.stringify({
              jsonrpc: '2.0',
              id: req.id,
              result: ['ElectrumX 1.16.0', '1.4.2'],
            }) + '\n',
          );
        }
      });
    });

    const transport = new WsTransport({
      endpoint: { host: 'localhost', port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });
    const client = new ElectrumClient({
      id: 'a',
      endpoint: transport.endpoint,
      transport,
    });

    await client.connect();

    expect(await client.call('server.ping', [])).toBeNull();

    const version = await client.call<[string, string]>('server.version', [
      'multi-electrum-client',
      '1.4',
    ]);
    expect(version).toEqual(['ElectrumX 1.16.0', '1.4.2']);

    await client.disconnect();
  });

  it('receives a server-pushed notification', async () => {
    srv.server.on('connection', (sock) => {
      sock.on('message', (raw) => {
        const req = JSON.parse(raw.toString('utf-8').trim());
        // First reply with a real response, then push a notification.
        sock.send(JSON.stringify({ id: req.id, result: { height: 1 } }) + '\n');
        setTimeout(() => {
          sock.send(
            JSON.stringify({
              jsonrpc: '2.0',
              method: 'blockchain.headers.subscribe',
              params: [{ height: 2, hex: 'aa' }],
            }) + '\n',
          );
        }, 5);
      });
    });

    const transport = new WsTransport({
      endpoint: { host: 'localhost', port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });
    const client = new ElectrumClient({
      id: 'a',
      endpoint: transport.endpoint,
      transport,
    });

    const notifs: unknown[] = [];
    client.onNotification((n) => notifs.push(n));

    await client.connect();
    const sub = await client.call<{ height: number }>('blockchain.headers.subscribe', []);
    expect(sub.height).toBe(1);

    // Wait for the pushed notification.
    await new Promise((r) => setTimeout(r, 30));

    expect(notifs).toHaveLength(1);
    expect(notifs[0]).toMatchObject({ method: 'blockchain.headers.subscribe' });

    await client.disconnect();
  });
});
