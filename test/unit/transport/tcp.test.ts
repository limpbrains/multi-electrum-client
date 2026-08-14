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

/** Poll until `predicate` holds — engine-independent microtask draining. */
async function waitFor(predicate: () => boolean, tries = 100): Promise<void> {
  for (let i = 0; i < tries; i++) {
    if (predicate()) return;
    await new Promise((r) => setTimeout(r, 5));
  }
  throw new Error('waitFor: condition never became true');
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

  it('honors the endpoint-declared line cap', async () => {
    // `ServerSpec.maxLineLength` reaches the transport as
    // `endpoint.maxLineLength`; without this fallback the manager has no
    // way to size the cap per server at all.
    srv = await startTestTcpServer((sock) => {
      sock.on('data', () => {
        sock.write('way more than eight characters\n');
      });
    });

    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp', maxLineLength: 8 },
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    await t.connect();
    await t.send('{"id":1}');
    await waitFor(() => events.some((e) => e.type === 'close'));
    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
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

  it('decodes raw bytes without a global TextDecoder (Hermes)', async () => {
    // Stock React Native Hermes has no TextDecoder. connect() must not
    // require one (it used to construct one eagerly and threw), and bytes
    // must still decode: validation and assembly are ours, so the
    // platform decoder is an optimization, not a dependency.
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

      ee.emit('data', new TextEncoder().encode('{"b":"€"}\n'));
      expect(events.map((e) => e.type)).toEqual(['data', 'data']);
      expect(events[1]).toEqual({ type: 'data', text: '{"b":"€"}' });

      // A chunk of a shape we cannot read at all is still terminal.
      ee.emit('data', 42);
      expect(events.map((e) => e.type)).toEqual(['data', 'data', 'error', 'close']);

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

  it('a socket whose destroy() synchronously re-emits error cannot recurse the teardown', async () => {
    // Injected sockets (the reason TcpSocketLike exists) may emit
    // 'error' synchronously from destroy(). The pre-open error handler
    // destroys the socket — without a settled guard that is handler →
    // destroy → 'error' → handler → … until the stack overflows. The
    // WS transport guards this exact class; TCP must too.
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const sock = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => sock,
      write: () => true,
      end: () => undefined,
      destroy: () => {
        ee.emit('error', new Error('destroy failed'));
      },
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => sock as unknown as TcpSocketLike,
    });
    const p = t.connect();
    ee.emit('error', new Error('boom'));
    await expect(p).rejects.toThrow();
  });

  it('emits an error and closes when a peer streams past the line-length cap', async () => {
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    let socketClosed = false;
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => {
        socketClosed = true;
        ee.emit('close', false);
      },
      destroy: () => {
        socketClosed = true;
        ee.emit('close', true);
      },
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      maxLineLength: 16,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));
    const p = t.connect();
    ee.emit('connect');
    await p;

    ee.emit('data', 'x'.repeat(64)); // newline-free flood past the cap
    await new Promise((r) => setTimeout(r, 10));

    const err = events.find((e) => e.type === 'error');
    expect(err).toBeDefined();
    expect(String((err as { cause: unknown }).cause)).toMatch(/line length limit/);
    // Published sockets get error THEN close — in-flight requests fail
    // over on the close instead of waiting out their timeouts.
    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    expect(socketClosed).toBe(true);
    await expect(t.send('x')).rejects.toBeInstanceOf(TransportError);
  });

  it('rejects connect() when the cap trips between the ready event and the continuation', async () => {
    // The ready event resolves the connect promise, but the continuation
    // that assigns `this.socket` runs a microtask later — a server that
    // floods immediately lands in that window. Reviving that socket would
    // report a live connection over a destroyed one.
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    let destroyed = false;
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => ee.emit('close', false),
      destroy: () => {
        destroyed = true;
      },
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      maxLineLength: 16,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    ee.emit('connect'); // resolves the connect promise
    ee.emit('data', 'x'.repeat(64)); // same tick, before the continuation

    await expect(p).rejects.toThrow(/line length limit exceeded/);
    expect(destroyed).toBe(true);
    await expect(t.send('x')).rejects.toBeInstanceOf(TransportError);
    // Nothing is emitted for a socket connect() never handed out — the
    // rejection IS the report, matching the native close listener's rule.
    // (A published connection still gets error+close so its in-flight
    // requests fail over instead of waiting out their timeouts.)
    expect(events).toEqual([]);
  });

  it('a stale socket close does not fire a terminal event for the current connection', async () => {
    const { EventEmitter } = await import('node:events');
    const sockets: Array<{ ee: InstanceType<typeof EventEmitter>; sock: TcpSocketLike }> = [];
    const makeSocket = (): TcpSocketLike => {
      const ee = new EventEmitter();
      const s = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        setEncoding: () => s,
        write: () => true,
        end: () => undefined,
        destroy: () => undefined,
      } as unknown as TcpSocketLike;
      sockets.push({ ee, sock: s });
      return s;
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => makeSocket(),
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    // Generation A connects; the consumer reconnects (generation B)
    // before A's delayed native close is delivered.
    const first = t.connect();
    sockets[0]!.ee.emit('connect');
    await first;
    const closeP = t.close();
    sockets[0]!.ee.emit('close', false); // A's own teardown
    await closeP;
    const second = t.connect();
    sockets[1]!.ee.emit('connect');
    await second;
    events.length = 0;

    sockets[0]!.ee.emit('close', false); // A's LATE close
    expect(events).toEqual([]); // must not describe B

    sockets[1]!.ee.emit('close', false); // B's own close still counts
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);
  });

  it('ignores every stale-socket event once a replacement connects', async () => {
    const { EventEmitter } = await import('node:events');
    const emitters: InstanceType<typeof EventEmitter>[] = [];
    const makeSocket = (): TcpSocketLike => {
      const ee = new EventEmitter();
      emitters.push(ee);
      const s = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        setEncoding: () => s,
        write: () => true,
        end: () => undefined,
        destroy: () => undefined,
      } as unknown as TcpSocketLike;
      return s;
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => makeSocket(),
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    // A connects and leaves a partial line in flight.
    const first = t.connect();
    emitters[0]!.emit('connect');
    await first;
    emitters[0]!.emit('data', '{"id":1,"resu');

    // A is closed (its socket object survives in this fake), then B
    // replaces it.
    const closeA = t.close();
    emitters[0]!.emit('close', false);
    await closeA;
    const second = t.connect();
    emitters[1]!.emit('connect');
    await second;
    events.length = 0;

    // Every kind of late event from A must be inert.
    emitters[0]!.emit('data', 'lt":"ok"}\n'); // would complete A's partial line
    emitters[0]!.emit('data', 'x'.repeat(200)); // would trip the cap
    emitters[0]!.emit('error', new Error('stale'));
    emitters[0]!.emit('close', false);
    expect(events).toEqual([]);

    // B's framing state is its own — A's bytes never entered it.
    emitters[1]!.emit('data', '{"id":2,"re');
    emitters[1]!.emit('data', 'sult":"b"}\n');
    expect(events).toEqual([{ type: 'data', text: '{"id":2,"result":"b"}' }]);
  });

  it('refuses a second connect while one is live, leaving the first usable', async () => {
    const { EventEmitter } = await import('node:events');
    const emitters: InstanceType<typeof EventEmitter>[] = [];
    const makeSocket = (): TcpSocketLike => {
      const ee = new EventEmitter();
      emitters.push(ee);
      const s = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        setEncoding: () => s,
        write: () => true,
        end: () => undefined,
        destroy: () => undefined,
      } as unknown as TcpSocketLike;
      return s;
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => makeSocket(),
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });
    const first = t.connect();
    emitters[0]!.emit('connect');
    await first;

    await expect(t.connect()).rejects.toThrow(/already connected/);
    expect(emitters).toHaveLength(1); // no second socket was created

    // The live connection is untouched — not write-only, still receiving.
    await t.send('{"id":1}');
    emitters[0]!.emit('data', '{"id":1,"result":"ok"}\n');
    expect(data).toEqual(['{"id":1,"result":"ok"}']);
  });

  it('emits nothing from a socket abandoned by close()', async () => {
    const { EventEmitter } = await import('node:events');
    const emitters: InstanceType<typeof EventEmitter>[] = [];
    const makeSocket = (): TcpSocketLike => {
      const ee = new EventEmitter();
      emitters.push(ee);
      const s = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        setEncoding: () => s,
        write: () => true,
        end: () => ee.emit('close', false),
        destroy: () => undefined,
      } as unknown as TcpSocketLike;
      return s;
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => makeSocket(),
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));
    const p = t.connect();
    emitters[0]!.emit('connect');
    await p;
    await t.close();
    events.length = 0;

    // A shim with queued events must not surface anything post-close.
    emitters[0]!.emit('data', '{"late":1}\n');
    emitters[0]!.emit('data', 'x'.repeat(200));
    emitters[0]!.emit('error', new Error('late'));
    emitters[0]!.emit('close', false);
    expect(events).toEqual([]);
  });

  it('a peer-initiated close frees the instance to connect again', async () => {
    // Reconnect must not require an explicit close(): the manager's
    // backoff loop calls connect() straight after a dropped link.
    const { EventEmitter } = await import('node:events');
    const emitters: InstanceType<typeof EventEmitter>[] = [];
    const makeSocket = (): TcpSocketLike => {
      const ee = new EventEmitter();
      emitters.push(ee);
      const s = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        setEncoding: () => s,
        write: () => true,
        end: () => undefined,
        destroy: () => undefined,
      } as unknown as TcpSocketLike;
      return s;
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => makeSocket(),
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const first = t.connect();
    emitters[0]!.emit('connect');
    await first;
    emitters[0]!.emit('close', true); // peer drops the link
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);

    const second = t.connect(); // must not throw "already connected"
    emitters[1]!.emit('connect');
    await second;
    await t.send('{"id":1}');
    expect(emitters).toHaveLength(2);
  });

  it('reconnect while a slow teardown is still running does not retire the replacement', async () => {
    const { EventEmitter } = await import('node:events');
    const emitters: InstanceType<typeof EventEmitter>[] = [];
    let releaseClose: (() => void) | undefined;
    const makeSocket = (): TcpSocketLike => {
      const ee = new EventEmitter();
      const index = emitters.length;
      emitters.push(ee);
      const s = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        setEncoding: () => s,
        write: () => true,
        // The first socket's teardown hangs until the test releases it.
        end: () => {
          if (index === 0) releaseClose = () => ee.emit('close', false);
          else ee.emit('close', false);
        },
        destroy: () => undefined,
      } as unknown as TcpSocketLike;
      return s;
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => makeSocket(),
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const first = t.connect();
    emitters[0]!.emit('connect');
    await first;

    const closeA = t.close(); // parks: A's end() never closes yet
    const connectB = t.connect(); // must wait for the close to finish
    await new Promise((r) => setTimeout(r, 10));
    expect(emitters).toHaveLength(1); // B has not even been created yet

    releaseClose!();
    await closeA;
    // B's connect is parked inside the lifecycle gate; how many
    // microtasks it needs to resume differs between engines (this raced
    // on Hermes in the on-device suite), so wait for its socket to exist.
    await waitFor(() => emitters.length === 2);
    emitters[1]!.emit('connect');
    await connectB;
    events.length = 0;

    // B is fully alive — A's teardown did not retire it.
    emitters[1]!.emit('data', '{"id":2}\n');
    expect(events).toEqual([{ type: 'data', text: '{"id":2}' }]);
  });

  it('emits nothing from a candidate whose connect failed', async () => {
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => undefined,
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    ee.emit('error', new Error('refused')); // pre-connect failure
    await expect(p).rejects.toBeInstanceOf(TransportError);
    events.length = 0;

    // Queued events from the dead candidate must not surface.
    ee.emit('data', '{"late":1}\n');
    ee.emit('data', 'x'.repeat(200));
    ee.emit('error', new Error('late'));
    ee.emit('close', false);
    expect(events).toEqual([]);
  });

  it('a peer close between ready and publication fails the connect', async () => {
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => undefined,
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    ee.emit('connect'); // resolves the inner promise
    ee.emit('close', false); // dies before the continuation publishes it

    await expect(p).rejects.toThrow(/closed during connect/);
    await expect(t.send('x')).rejects.toBeInstanceOf(TransportError);
    // No close event: connect() never exposed this socket to the caller.
    expect(events).toEqual([]);

    ee.emit('data', '{"late":1}\n');
    ee.emit('error', new Error('late'));
    expect(events).toEqual([]);
  });

  it('retires the generation on a published peer close', async () => {
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => undefined,
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    ee.emit('connect');
    await p;
    ee.emit('close', true); // peer drops a live connection
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);
    events.length = 0;

    ee.emit('data', '{"late":1}\n');
    ee.emit('error', new Error('late'));
    ee.emit('close', false);
    expect(events).toEqual([]);
  });

  it('nothing surfaces from a socket after a framing-limit teardown', async () => {
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => undefined,
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    ee.emit('connect');
    await p;

    ee.emit('data', 'x'.repeat(200)); // trips the cap
    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    events.length = 0;

    // Everything this socket queued behind the terminal pair is inert.
    ee.emit('data', '{"valid":1}\n');
    ee.emit('data', 'x'.repeat(200));
    ee.emit('error', new Error('late'));
    ee.emit('close', false);
    expect(events).toEqual([]);
  });

  it('a throwing listener cannot strand the socket during a framing teardown', async () => {
    const { EventEmitter } = await import('node:events');
    const emitters: InstanceType<typeof EventEmitter>[] = [];
    let destroyed = 0;
    const makeSocket = (): TcpSocketLike => {
      const ee = new EventEmitter();
      emitters.push(ee);
      const s = {
        on: ee.on.bind(ee),
        once: ee.once.bind(ee),
        setEncoding: () => s,
        write: () => true,
        end: () => undefined,
        destroy: () => {
          destroyed++;
        },
      } as unknown as TcpSocketLike;
      return s;
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => makeSocket(),
      maxLineLength: 64,
    });
    const seen: string[] = [];
    t.on((e) => {
      seen.push(e.type);
      if (e.type === 'error') throw new Error('consumer callback blew up');
    });

    const p = t.connect();
    emitters[0]!.emit('connect');
    await p;

    emitters[0]!.emit('data', 'x'.repeat(200)); // trips the cap

    expect(destroyed).toBe(1); // torn down despite the throw
    expect(seen).toEqual(['error', 'close']); // close still delivered
    // The stored socket was cleared, so a replacement connect is allowed.
    const second = t.connect();
    emitters[1]!.emit('connect');
    await second;
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

  it('a candidate that fails connect cannot emit data in the same turn', async () => {
    // An injected socket (the reason `TcpSocketLike` exists) may emit
    // synchronously. `reject` only schedules, so before the fix the
    // generation stayed current until the catch ran a microtask later —
    // and a `data` event fired in between surfaced as bytes from a
    // connection whose connect() had already failed.
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    let destroyed = false;
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
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

    const p = t.connect();
    queueMicrotask(() => {
      ee.emit('error', new Error('ECONNREFUSED'));
      // Same turn, after the failure: a compatible shim can do this.
      ee.emit('data', '{"id":1}\n');
    });
    await expect(p).rejects.toBeInstanceOf(TransportError);

    expect(events.filter((e) => e.type === 'data')).toEqual([]);
    expect(destroyed).toBe(true);
  });

  it('ends the connection on malformed UTF-8 instead of delivering replacement characters', async () => {
    // A non-fatal decoder turns an invalid byte into U+FFFD, which keeps
    // the JSON syntactically valid — the corruption then reaches the
    // application inside a txid or an address. Fail loudly instead.
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => undefined,
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

    // {"a":"<FF>"}\n — 0xff is never valid UTF-8.
    ee.emit('data', new Uint8Array([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d, 0x0a]));

    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    expect(String((events[0] as { cause: unknown }).cause)).toContain('undecodable data');
  });

  it('ends the connection on malformed UTF-8 arriving over a real socket', async () => {
    // The socket must stay in binary mode: `setEncoding('utf-8')` hands
    // over a string Node has already "repaired", so the malformed byte
    // reaches the caller as U+FFFD inside JSON that still parses and the
    // validator never sees what caused it.
    srv = await startTestTcpServer((sock) => {
      sock.write(Buffer.from([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d, 0x0a]));
    });
    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp' },
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));
    await t.connect();

    await waitFor(() => events.some((e) => e.type === 'close'));
    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    expect(events.filter((e) => e.type === 'data')).toEqual([]);
    // Close explicitly even though the transport tore itself down: this
    // suite also runs on-device, where react-native-tcp-socket's native
    // listener has crashed on a server closed under an open connection.
    await t.close();
  });

  it('preserves U+FEFF wherever the socket splits the stream', async () => {
    // TextDecoder strips a leading BOM on every decode call unless told
    // not to, and we decode chunk by chunk — so the same payload used to
    // survive or lose its U+FEFF depending on where the network split it.
    const line = '{"a":"\ufeffx"}';
    const payload = Buffer.from(`${line}\n`, 'utf-8');
    // Split immediately before the BOM's EF BB BF.
    const cut = payload.indexOf(0xef);
    expect(cut).toBeGreaterThan(0);
    srv = await startTestTcpServer((sock) => {
      sock.write(payload.subarray(0, cut));
      setTimeout(() => sock.write(payload.subarray(cut)), 10);
    });
    const t = new TcpTransport({
      endpoint: { host: '127.0.0.1', port: srv.port, protocol: 'tcp' },
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });
    await t.connect();

    await waitFor(() => data.length > 0);
    expect(data).toEqual([line]);
    await t.close();
  });

  it('a connect timeout cannot deliver data the socket flushes as it dies', async () => {
    // `destroy()` on an injected socket can emit buffered `data`
    // synchronously. Retiring only in the rejection's continuation left
    // that data looking like it came from a live connection.
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => {
        // A socket shim flushing what it had buffered on the way down.
        ee.emit('data', Buffer.from('{"id":1}\n'));
      },
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      connectTimeoutMs: 20,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    await expect(t.connect()).rejects.toThrow(/connect timeout/);
    expect(events.filter((e) => e.type === 'data')).toEqual([]);
  });

  it('a string chunk cannot jump ahead of a half-received character', async () => {
    // A shim that emits both shapes can interrupt a byte sequence with an
    // already-decoded string. Feeding that string straight to the framer
    // put it BEFORE the character whose bytes arrived first, producing
    // valid-looking but reordered JSON.
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => undefined,
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

    // '{"v":"' plus the first byte of '€'.
    ee.emit('data', new Uint8Array([0x7b, 0x22, 0x76, 0x22, 0x3a, 0x22, 0xe2]));
    ee.emit('data', 'A');
    ee.emit('data', new Uint8Array([0x82, 0xac, 0x22, 0x7d, 0x0a]));

    expect(events.filter((e) => e.type === 'data')).toEqual([]);
    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
  });
});

describe('TcpTransport — a pre-ready protocol failure settles connect()', () => {
  it('fails the attempt instead of leaving it to time out', async () => {
    // `failStream` mutes the native close, so an oversized line arriving
    // before the ready event left nothing to settle connect(): it waited
    // out the whole connect timeout on a socket already destroyed, and
    // the manager's reconnect then hit "connect already in progress".
    const { EventEmitter } = await import('node:events');
    const ee = new EventEmitter();
    const fakeSocket = {
      on: ee.on.bind(ee),
      once: ee.once.bind(ee),
      emit: ee.emit.bind(ee),
      setEncoding: () => fakeSocket,
      write: () => true,
      end: () => undefined,
      destroy: () => undefined,
    };
    const t = new TcpTransport({
      endpoint: { host: 'h', port: 1, protocol: 'tcp' },
      connect: () => fakeSocket as unknown as TcpSocketLike,
      maxLineLength: 16,
      connectTimeoutMs: 5_000,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    ee.emit('data', `${'x'.repeat(64)}\n`);

    const settled = await Promise.race([
      p.then(
        () => 'resolved',
        (e: Error) => e.message,
      ),
      new Promise((r) => setTimeout(() => r('still pending'), 200)),
    ]);

    expect(settled).toMatch(/line length limit exceeded/);
    expect(events).toEqual([]);
  });
});
