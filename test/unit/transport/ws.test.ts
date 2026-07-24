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

  it('drops a stale partial line when reconnecting on the same transport', async () => {
    let connections = 0;
    srv.server.on('connection', (sock) => {
      connections++;
      if (connections === 1) {
        // Partial line, never completed — must not leak into the next connection.
        sock.send('{"id":1,"resu');
      } else {
        sock.send('{"id":2,"result":"fresh"}\n');
      }
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
    await transport.connect();
    await delay(30);
    await transport.close();

    expect(datas).toEqual(['{"id":2,"result":"fresh"}']);
  });

  it('close() during in-flight connect aborts it and rejects the connect promise', async () => {
    srv.server.on('connection', () => {
      // accept silently
    });
    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws' },
      WebSocket: WebSocketCtor,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const pending = transport.connect();
    await transport.close();
    await expect(pending).rejects.toThrow('closed during connect');

    // No stray close event from the aborted connection.
    await delay(30);
    expect(events).toEqual([]);

    // Transport stays usable: a fresh connect succeeds.
    await transport.connect();
    await transport.close();
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

  it('works without a global TextDecoder (Hermes): construct + text frames flow, binary surfaces an error', async () => {
    // Stock React Native Hermes has no TextDecoder. Constructing the
    // transport must not require one (the field initializer used to throw),
    // text frames must flow, and a binary frame must surface as a transport
    // error instead of being dropped silently.
    const g = globalThis as { TextDecoder?: unknown };
    const saved = g.TextDecoder;
    g.TextDecoder = undefined;
    try {
      type Listener = (ev: unknown) => void;
      class FakeWs {
        static last: FakeWs;
        readonly listeners = new Map<string, Listener[]>();
        binaryType = '';
        readyState = 0;
        CLOSED = 3;
        constructor(_url: string) {
          FakeWs.last = this;
        }
        addEventListener(type: string, cb: Listener): void {
          const arr = this.listeners.get(type) ?? [];
          arr.push(cb);
          this.listeners.set(type, arr);
        }
        removeEventListener(type: string, cb: Listener): void {
          this.listeners.set(
            type,
            (this.listeners.get(type) ?? []).filter((l) => l !== cb),
          );
        }
        dispatch(type: string, ev: unknown): void {
          for (const cb of this.listeners.get(type) ?? []) cb(ev);
        }
        send(_data: string): void {}
        close(): void {
          this.readyState = this.CLOSED;
          this.dispatch('close', { code: 1000 });
        }
      }

      const transport = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: FakeWs as unknown as new (url: string) => WebSocket,
      });
      const events: TransportEvent[] = [];
      transport.on((ev) => events.push(ev));

      const pending = transport.connect();
      FakeWs.last.dispatch('open', {});
      await pending;

      FakeWs.last.dispatch('message', { data: '{"id":1,"result":"ok"}\n' });
      expect(events).toContainEqual({ type: 'data', text: '{"id":1,"result":"ok"}' });

      FakeWs.last.dispatch('message', { data: new Uint8Array([0x7b, 0x7d, 0x0a]).buffer });
      expect(
        events.some(
          (e) => e.type === 'error' && String(e.cause).includes('undecodable message data'),
        ),
      ).toBe(true);

      await transport.close();
    } finally {
      g.TextDecoder = saved;
    }
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
