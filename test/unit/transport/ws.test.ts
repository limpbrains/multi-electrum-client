import { setTimeout as delay } from 'node:timers/promises';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { WebSocket as WsWebSocket } from 'ws';

import { TransportError } from '../../../src/errors/types.js';
import type { TransportEvent } from '../../../src/transport/types.js';
import { WsTransport } from '../../../src/transport/ws.js';
import { startTestWsServer, type TestWsServer } from '../../helpers/wsTestServer.js';

const WebSocketCtor = WsWebSocket as unknown as new (url: string) => WebSocket;
const HOST = '127.0.0.1';

describe('WsTransport', () => {
  let srv: TestWsServer;

  beforeEach(async () => {
    srv = await startTestWsServer();
  });

  afterEach(async () => {
    await srv.close();
  });

  it('connects, sends newline-terminated, receives newline-split, closes', async () => {
    const received: string[] = [];
    srv.server.on('connection', (sock) => {
      sock.on('message', (data) => {
        received.push(data.toString('utf-8'));
        sock.send('{"id":1,"result":"ok"}\n');
      });
    });

    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });

    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    await transport.connect();
    await transport.send('{"id":1,"method":"server.ping","params":[]}');

    await delay(40);

    const dataEvents = events.filter((e) => e.type === 'data');
    expect(dataEvents).toHaveLength(1);
    expect(dataEvents[0]!.text).toBe('{"id":1,"result":"ok"}');

    expect(received[0]).toBe('{"id":1,"method":"server.ping","params":[]}\n');

    await transport.close();
    expect(events.some((e) => e.type === 'close')).toBe(true);
  });

  it('emits one data event per newline-terminated message even when packed in one frame', async () => {
    srv.server.on('connection', (sock) => {
      sock.send('{"id":1,"result":1}\n{"id":2,"result":2}\n');
    });

    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });

    const datas: string[] = [];
    transport.on((ev) => {
      if (ev.type === 'data') datas.push(ev.text);
    });

    await transport.connect();
    await delay(30);
    await transport.close();

    expect(datas).toEqual(['{"id":1,"result":1}', '{"id":2,"result":2}']);
  });

  it('buffers a partial line across frames', async () => {
    srv.server.on('connection', (sock) => {
      sock.send('{"id":1,"resu');
      setTimeout(() => sock.send('lt":"ok"}\n'), 10);
    });

    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });

    const datas: string[] = [];
    transport.on((ev) => {
      if (ev.type === 'data') datas.push(ev.text);
    });

    await transport.connect();
    await delay(50);
    await transport.close();

    expect(datas).toEqual(['{"id":1,"result":"ok"}']);
  });

  it('rejects send before connect', async () => {
    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });
    await expect(transport.send('x')).rejects.toBeInstanceOf(TransportError);
  });

  it('rejects send when payload contains embedded newline', async () => {
    srv.server.on('connection', () => {
      // accept silently
    });
    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });
    await transport.connect();
    await expect(transport.send('a\nb')).rejects.toBeInstanceOf(TransportError);
    await transport.close();
  });

  it('rejects send after close (ws is nulled)', async () => {
    srv.server.on('connection', () => {
      // accept silently
    });
    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });
    await transport.connect();
    await transport.close();
    await expect(transport.send('x')).rejects.toBeInstanceOf(TransportError);
  });

  it('rejects unsupported protocol', () => {
    expect(
      () =>
        new WsTransport({
          endpoint: { host: HOST, port: 1, protocol: 'tcp' },
          WebSocket: WebSocketCtor,
        }),
    ).toThrow(TransportError);
  });

  it('throws TransportError when connect fails (server not listening)', async () => {
    const port = srv.port;
    await srv.close();

    const transport = new WsTransport({
      endpoint: { host: HOST, port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
      connectTimeoutMs: 500,
    });
    await expect(transport.connect()).rejects.toBeInstanceOf(TransportError);
  });

  it('builds the URL with optional path', async () => {
    let connectedPath: string | undefined;
    srv.server.on('connection', (_sock, req) => {
      connectedPath = req.url ?? undefined;
    });

    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws', path: '/electrum' },
      WebSocket: WebSocketCtor,
    });
    await transport.connect();
    await delay(20);
    await transport.close();

    expect(connectedPath).toBe('/electrum');
  });
});
