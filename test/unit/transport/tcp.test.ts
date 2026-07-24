import net, { type Server, type Socket } from 'node:net';

import { afterEach, describe, expect, it } from 'vitest';

import { TransportError } from '../../../src/errors/types.js';
import { TcpTransport, type TcpSocketLike } from '../../../src/transport/tcp.js';
import type { TransportEvent } from '../../../src/transport/types.js';

interface TestTcpServer {
  server: Server;
  port: number;
  close(): Promise<void>;
}

async function startTestTcpServer(
  onConn: (socket: Socket) => void = () => undefined,
): Promise<TestTcpServer> {
  return new Promise((resolve, reject) => {
    const server = net.createServer((sock) => {
      sock.setEncoding('utf-8');
      onConn(sock);
    });
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      if (!addr || typeof addr === 'string') {
        reject(new Error('no address'));
        return;
      }
      resolve({
        server,
        port: addr.port,
        close: () =>
          new Promise<void>((r) => {
            server.close(() => r());
          }),
      });
    });
  });
}

describe('TcpTransport', () => {
  let srv: TestTcpServer;

  afterEach(async () => {
    if (srv) await srv.close();
  });

  it('connects, sends newline-terminated, receives newline-split, closes', async () => {
    const received: string[] = [];
    srv = await startTestTcpServer((sock) => {
      sock.on('data', (chunk) => {
        received.push(typeof chunk === 'string' ? chunk : chunk.toString('utf-8'));
        sock.write('{"id":1,"result":"ok"}\n');
      });
    });

    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp' },
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    await t.connect();
    await t.send('{"id":1}');
    // Wait for the round-trip.
    for (let i = 0; i < 50; i++) {
      if (events.find((e) => e.type === 'data')) break;
      await new Promise((r) => setTimeout(r, 10));
    }
    expect(received).toContain('{"id":1}\n');
    expect(events.find((e) => e.type === 'data')).toEqual({
      type: 'data',
      text: '{"id":1,"result":"ok"}',
    });

    await t.close();
  });

  it('reassembles split frames across multiple data chunks', async () => {
    srv = await startTestTcpServer((sock) => {
      sock.on('data', () => {
        // Send a single message split into two writes.
        sock.write('{"id":');
        setTimeout(() => sock.write('1,"result":"ok"}\n'), 5);
      });
    });

    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp' },
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });

    await t.connect();
    await t.send('{"id":1}');
    for (let i = 0; i < 50; i++) {
      if (data.length > 0) break;
      await new Promise((r) => setTimeout(r, 10));
    }
    expect(data).toEqual(['{"id":1,"result":"ok"}']);

    await t.close();
  });

  it('emits one data event per line when chunks contain multiple frames', async () => {
    srv = await startTestTcpServer((sock) => {
      sock.on('data', () => {
        sock.write('{"id":1}\n{"id":2}\n');
      });
    });

    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp' },
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });

    await t.connect();
    await t.send('go');
    for (let i = 0; i < 50; i++) {
      if (data.length >= 2) break;
      await new Promise((r) => setTimeout(r, 10));
    }
    expect(data).toEqual(['{"id":1}', '{"id":2}']);

    await t.close();
  });

  it('emits close event when the server drops the socket', async () => {
    srv = await startTestTcpServer((sock) => {
      sock.end();
    });

    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp' },
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    await t.connect();
    for (let i = 0; i < 50; i++) {
      if (events.find((e) => e.type === 'close')) break;
      await new Promise((r) => setTimeout(r, 10));
    }
    expect(events.find((e) => e.type === 'close')).toBeDefined();
  });

  it('rejects connect on a port nothing is listening on', async () => {
    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: 1, protocol: 'tcp' },
      connectTimeoutMs: 500,
    });
    await expect(t.connect()).rejects.toBeInstanceOf(TransportError);
  });

  it('connect timeout: a transport.on listener attached before connect does not see a stray close', async () => {
    // Wire a listener BEFORE connect (matches Client's order). With the
    // pre-fix code, the timeout-driven destroy would emit { type: 'close' }
    // on top of the connect rejection.
    const t = new TcpTransport({
      endpoint: { host: '10.255.255.1', port: 1, protocol: 'tcp' },
      connectTimeoutMs: 50,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));
    await expect(t.connect()).rejects.toBeInstanceOf(TransportError);
    // Allow any post-rejection async events to settle.
    await new Promise((r) => setTimeout(r, 20));
    expect(events.find((e) => e.type === 'close')).toBeUndefined();
  });

  it('rejects send before connect', async () => {
    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: 1, protocol: 'tcp' },
    });
    await expect(t.send('x')).rejects.toBeInstanceOf(TransportError);
  });

  it('rejects send with an embedded newline', async () => {
    srv = await startTestTcpServer();
    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp' },
    });
    await t.connect();
    await expect(t.send('a\nb')).rejects.toBeInstanceOf(TransportError);
    await t.close();
  });

  it('decodes Uint8Array data chunks via TextDecoder (defensive shim path)', async () => {
    // Inject a fake socket that emits a raw Uint8Array (mimicking a shim
    // that ignores setEncoding). The naive `chunk.toString('utf-8')`
    // fallback would corrupt this into comma-decimals.
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => ee.emit('close', false),
      destroy: () => ee.emit('close', true),
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });
    const p = t.connect();
    queueMicrotask(() => ee.emit('connect'));
    await p;

    ee.emit('data', new TextEncoder().encode('{"id":1}\n'));
    expect(data).toEqual(['{"id":1}']);

    await t.close();
  });

  it('works without a global TextDecoder (Hermes): strings flow, raw bytes surface an error', async () => {
    // Stock React Native Hermes has no TextDecoder. connect() must not
    // require one (it used to construct one eagerly and threw), string
    // chunks must flow normally, and a raw Uint8Array chunk must surface
    // as a transport error instead of crashing.
    const g = globalThis as { TextDecoder?: unknown };
    const saved = g.TextDecoder;
    g.TextDecoder = undefined;
    try {
      const { EventEmitter } = await import('node:events');
      const ee = new EventEmitter();
      const fakeSocket = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        emit: ee.emit.bind(ee),
        setEncoding: () => fakeSocket,
        write: () => true,
        end: () => ee.emit('close', false),
        destroy: () => ee.emit('close', true),
      };
      const t = new TcpTransport({
        endpoint: { host: 'h', port: 1, protocol: 'tcp' },
        connect: () => fakeSocket as unknown as TcpSocketLike,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      queueMicrotask(() => ee.emit('connect'));
      await p;

      ee.emit('data', '{"id":1}\n');
      expect(events).toContainEqual({ type: 'data', text: '{"id":1}' });

      ee.emit('data', new Uint8Array([0x7b, 0x7d, 0x0a]));
      expect(
        events.some(
          (e) => e.type === 'error' && String(e.cause).includes('unexpected data chunk type'),
        ),
      ).toBe(true);

      await t.close();
    } finally {
      g.TextDecoder = saved;
    }
  });

  it('refuses non-tcp protocol', () => {
    expect(
      () =>
        new TcpTransport({
          endpoint: { host: 'h', port: 1, protocol: 'ws' },
        }),
    ).toThrow(TransportError);
  });

  it('close() during in-flight connect destroys the pending socket', async () => {
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    let destroyed = false;
    // Never emits 'connect' — the connect() stays pending until aborted.
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => {
        destroyed = true;
      },
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const pending = t.connect();
    await t.close();
    await expect(pending).rejects.toThrow('closed during connect');
    expect(destroyed).toBe(true);
    // No stray close event from the aborted connection.
    expect(events).toEqual([]);
  });
});
