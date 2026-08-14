import { setTimeout as delay } from 'node:timers/promises';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { WebSocket as WsWebSocket } from 'ws';

import { TransportError } from '../../../src/errors/types.js';
import type { TransportEvent } from '../../../src/transport/types.js';
import { WsTransport } from '../../../src/transport/ws.js';
import { startTestWsServer, type TestWsServer } from '../../helpers/wsTestServer.js';

const WebSocketCtor = WsWebSocket as unknown as new (url: string) => WebSocket;
const HOST = '127.0.0.1';

type FakeListener = (ev: unknown) => void;

/**
 * Deterministic in-process WebSocket double: tests drive `open` /
 * `message` / `close` ordering explicitly, which is the only way to pin
 * the races around the connect continuation.
 */
class FakeWs {
  readonly listeners = new Map<string, FakeListener[]>();
  binaryType = '';
  readyState = 1;
  CLOSED = 3;
  closeCalls = 0;
  terminateCalls = 0;
  constructor(readonly url: string) {}
  addEventListener(type: string, cb: FakeListener): void {
    const arr = this.listeners.get(type) ?? [];
    arr.push(cb);
    this.listeners.set(type, arr);
  }
  removeEventListener(type: string, cb: FakeListener): void {
    const arr = this.listeners.get(type) ?? [];
    const i = arr.indexOf(cb);
    if (i >= 0) arr.splice(i, 1);
  }
  send(_data: string): void {}
  close(): void {
    this.closeCalls++;
  }
  terminate(): void {
    this.terminateCalls++;
  }
  fire(type: string, ev: Record<string, unknown> = {}): void {
    for (const cb of [...(this.listeners.get(type) ?? [])]) cb(ev);
  }
}

function fakeWsCtor(): { Ctor: new (url: string) => WebSocket; instances: FakeWs[] } {
  const instances: FakeWs[] = [];
  const Ctor = function (this: unknown, url: string) {
    const inst = new FakeWs(url);
    instances.push(inst);
    return inst;
  } as unknown as new (url: string) => WebSocket;
  return { Ctor, instances };
}

describe('WsTransport', () => {
  let srv: TestWsServer;

  beforeEach(async () => {
    srv = await startTestWsServer();
  });

  afterEach(async () => {
    await srv.close();
  });

  it('connects, sends newline-terminated, receives newline-split, closes', async () => {
    // Tunnel framing: the TCP server behind the bridge delimits with
    // newlines, so outbound payloads carry one and inbound splits on it.
    const received: string[] = [];
    srv.server.on('connection', (sock) => {
      sock.on('message', (data) => {
        received.push(data.toString('utf-8'));
        sock.send('{"id":1,"result":"ok"}\n');
      });
    });

    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws', wsFraming: 'newline' },
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
    // A self-initiated close emits no close event (same as TcpTransport):
    // the caller knows it closed, and the contract is that nothing from a
    // torn-down socket surfaces once close() resolves.
    expect(events.some((e) => e.type === 'close')).toBe(false);
  });

  it('emits one data event per newline-terminated message even when packed in one frame', async () => {
    // Tunnel framing: one message carrying several newline-delimited
    // payloads, split per line by the framer.
    srv.server.on('connection', (sock) => {
      sock.send('{"id":1,"result":1}\n{"id":2,"result":2}\n');
    });

    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws', wsFraming: 'newline' },
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
    // A byte tunnel relays the TCP stream with arbitrary message
    // boundaries — the very first message may end mid-line, so the
    // framing is declared, never inferred from traffic.
    srv.server.on('connection', (sock) => {
      sock.send('{"id":1,"resu');
      setTimeout(() => sock.send('lt":"ok"}\n'), 10);
    });

    const transport = new WsTransport({
      endpoint: { host: HOST, port: srv.port, protocol: 'ws', wsFraming: 'newline' },
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
      endpoint: { host: HOST, port: srv.port, protocol: 'ws', wsFraming: 'newline' },
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

  it('close() resolves within the backstop when the peer never completes the close handshake', async () => {
    // A dead / non-compliant peer may never emit 'close'; stop() and
    // suspend() await transport close, so it must self-bound (~500ms).
    type Listener = (ev: unknown) => void;
    let terminated = false;
    class NeverClosingWs {
      readonly listeners = new Map<string, Listener[]>();
      binaryType = '';
      readyState = 1;
      CLOSED = 3;
      constructor(_url: string) {}
      addEventListener(type: string, cb: Listener): void {
        const arr = this.listeners.get(type) ?? [];
        arr.push(cb);
        this.listeners.set(type, arr);
      }
      removeEventListener(type: string, cb: Listener): void {
        const arr = this.listeners.get(type) ?? [];
        const i = arr.indexOf(cb);
        if (i >= 0) arr.splice(i, 1);
      }
      send(_data: string): void {}
      close(): void {
        // Never emits 'close' — the handshake never completes.
      }
      terminate(): void {
        terminated = true;
      }
      fire(type: string): void {
        for (const cb of [...(this.listeners.get(type) ?? [])]) cb({});
      }
    }
    let instance!: NeverClosingWs;
    const Ctor = function (this: unknown, url: string) {
      instance = new NeverClosingWs(url);
      return instance;
    } as unknown as new (url: string) => WebSocket;

    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const connectP = transport.connect();
    instance.fire('open');
    await connectP;

    const started = Date.now();
    await transport.close(); // must not hang
    expect(Date.now() - started).toBeLessThan(2000);
    expect(terminated).toBe(true);
  });

  it('emits exactly one close event when the line cap trips on an established connection', async () => {
    // WsTransport is a public export: a consumer that does not detach on
    // the first terminal event must not see the overflow close twice.
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 16,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const connectP = transport.connect();
    instances[0]!.fire('open');
    await connectP; // connection is fully established first

    instances[0]!.fire('message', { data: 'x'.repeat(64) }); // flood past the cap
    instances[0]!.fire('close', { code: 1006 }); // native handshake completes

    expect(events.filter((e) => e.type === 'error')).toHaveLength(1);
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);
    expect(events.find((e) => e.type === 'close')).toMatchObject({
      reason: 'line length limit exceeded',
    });
  });

  it('rejects connect() when the cap trips between open and the connect continuation', async () => {
    // The open event resolves the connect promise, but the continuation
    // that assigns `this.ws` runs a microtask later — a server that
    // floods immediately lands in that window. The attempt must fail
    // rather than hand back a connection over an already-closed socket.
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 16,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const connectP = transport.connect();
    const ws = instances[0]!;
    ws.fire('open');
    ws.fire('message', { data: 'x'.repeat(64) }); // same tick as open

    await expect(connectP).rejects.toThrow(/line length limit exceeded/);
    expect(ws.closeCalls).toBeGreaterThanOrEqual(1);
    // No revived socket: send must report a dead transport.
    await expect(transport.send('x')).rejects.toBeInstanceOf(TransportError);
    // Nothing is emitted for a socket connect() never handed out — the
    // rejection IS the report, and the same rule the native close
    // listener follows. (A published connection does get error+close, so
    // its in-flight requests fail over instead of waiting out timeouts.)
    expect(events).toEqual([]);
  });

  it('a stale socket close does not fire a terminal event for the current connection', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    // Generation A connects, then the consumer reconnects (generation B)
    // before A's delayed native close is delivered.
    const first = transport.connect();
    instances[0]!.fire('open');
    await first;
    await transport.close(); // A torn down (emits its close)
    const second = transport.connect();
    instances[1]!.fire('open');
    await second;
    events.length = 0;

    instances[0]!.fire('close', { code: 1006 }); // A's late close
    expect(events).toEqual([]); // must not describe B

    instances[1]!.fire('close', { code: 1006 }); // B's own close still counts
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);
  });

  it('a synchronous close() throw still disarms the backstop', async () => {
    type Listener = (ev: unknown) => void;
    let terminated = false;
    class ThrowingCloseWs {
      readonly listeners = new Map<string, Listener[]>();
      binaryType = '';
      readyState = 1;
      CLOSED = 3;
      constructor(_url: string) {}
      addEventListener(type: string, cb: Listener): void {
        const arr = this.listeners.get(type) ?? [];
        arr.push(cb);
        this.listeners.set(type, arr);
      }
      removeEventListener(type: string, cb: Listener): void {
        const arr = this.listeners.get(type) ?? [];
        const i = arr.indexOf(cb);
        if (i >= 0) arr.splice(i, 1);
      }
      send(_data: string): void {}
      close(): void {
        throw new Error('close blew up');
      }
      terminate(): void {
        terminated = true;
      }
      fire(type: string): void {
        for (const cb of [...(this.listeners.get(type) ?? [])]) cb({});
      }
    }
    let instance!: ThrowingCloseWs;
    const Ctor = function (this: unknown, url: string) {
      instance = new ThrowingCloseWs(url);
      return instance;
    } as unknown as new (url: string) => WebSocket;

    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const connectP = transport.connect();
    instance.fire('open');
    await connectP;

    await transport.close(); // resolves despite the throw
    // The 500ms backstop must be disarmed — no late terminate().
    await delay(700);
    expect(terminated).toBe(false);
  });

  it('ignores every stale-socket event once a replacement connects', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws', wsFraming: 'newline' },
      WebSocket: Ctor,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    // A connects and leaves a partial line in flight.
    const first = transport.connect();
    const a = instances[0]!;
    a.fire('open');
    await first;
    a.fire('message', { data: '{"id":1,"resu' });

    // A is closed but never completes its handshake (backstop path), so
    // its object stays alive; B then replaces it.
    await transport.close();
    const second = transport.connect();
    const b = instances[1]!;
    b.fire('open');
    await second;
    events.length = 0;

    // Every kind of late event from A must be inert.
    a.fire('message', { data: 'lt":"ok"}\n' }); // would complete A's partial line
    a.fire('message', { data: 'x'.repeat(200) }); // would trip the cap
    a.fire('error', {});
    a.fire('close', { code: 1006 });
    expect(events).toEqual([]);

    // B's own framing state is untouched: its partial line completes on
    // its own bytes, not A's.
    b.fire('message', { data: '{"id":2,"re' });
    b.fire('message', { data: 'sult":"b"}\n' });
    expect(events).toEqual([{ type: 'data', text: '{"id":2,"result":"b"}' }]);
  });

  it('a late close after the close backstop does not terminate the replacement connection', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    // A connects, then close() times out on the backstop — `this.ws` is
    // null while the replacement connect is still in flight.
    const first = transport.connect();
    const a = instances[0]!;
    a.fire('open');
    await first;
    await transport.close(); // A never emits close → backstop path
    expect(a.terminateCalls).toBe(1);

    const second = transport.connect();
    const b = instances[1]!;
    events.length = 0;
    a.fire('close', { code: 1006 }); // A's late close, mid-connect for B
    expect(events).toEqual([]);

    b.fire('open');
    await second;
    b.fire('close', { code: 1000 }); // B's own close still counts
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);
  });

  it('refuses a second connect while one is live, leaving the first usable', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const data: string[] = [];
    transport.on((ev) => {
      if (ev.type === 'data') data.push(ev.text);
    });
    const first = transport.connect();
    instances[0]!.fire('open');
    await first;

    await expect(transport.connect()).rejects.toThrow(/already connected/);
    expect(instances).toHaveLength(1); // no second socket was even created

    // The live connection is untouched — not write-only, still receiving.
    await transport.send('{"id":1}');
    instances[0]!.fire('message', { data: '{"id":1,"result":"ok"}\n' });
    expect(data).toEqual(['{"id":1,"result":"ok"}']);
  });

  it('emits nothing from a socket abandoned by close(), even on the backstop path', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const connectP = transport.connect();
    const a = instances[0]!;
    a.fire('open');
    await connectP;
    // Peer never completes the handshake → close() returns via backstop
    // and the socket object stays alive in the caller's runtime.
    await transport.close();
    expect(a.terminateCalls).toBe(1);
    events.length = 0;

    // Nothing this object does may escape now that close() has resolved.
    a.fire('message', { data: '{"late":1}\n' });
    a.fire('message', { data: 'x'.repeat(200) });
    a.fire('error', {});
    a.fire('close', { code: 1006 });
    expect(events).toEqual([]);
  });

  it('a peer-initiated close frees the instance to connect again', async () => {
    // Reconnect must not require an explicit close(): the manager's
    // backoff loop calls connect() straight after a dropped link.
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const first = transport.connect();
    instances[0]!.fire('open');
    await first;
    instances[0]!.fire('close', { code: 1006 }); // peer drops the link
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);

    const second = transport.connect(); // must not throw "already connected"
    instances[1]!.fire('open');
    await second;
    await transport.send('{"id":1}');
    expect(instances).toHaveLength(2);
  });

  it('close() during the ready→publish window wins: connect rejects, nothing goes live', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const connectP = transport.connect();
    const ws = instances[0]!;
    ws.fire('open'); // resolves the inner promise; publication is a tick away
    const closeP = transport.close(); // lands in that window

    await expect(connectP).rejects.toThrow(/superseded|closed during connect/);
    await closeP;
    await expect(transport.send('x')).rejects.toBeInstanceOf(TransportError);
  });

  it('reconnect works immediately after an awaited close of a never-opening connect', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    // Never fires 'open': the attempt is still pending when close aborts it.
    const pending = transport.connect();
    const rejected = expect(pending).rejects.toThrow(/closed during connect/);
    await transport.close();
    await rejected;

    // No manual await of the aborted attempt — close() already settled it.
    const second = transport.connect();
    instances[1]!.fire('open');
    await second;
    await transport.send('{"id":1}');
  });

  it('a delayed second close does not retire the replacement connection', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const first = transport.connect();
    instances[0]!.fire('open');
    await first;
    // Two overlapping close callers must share one operation.
    const closeA = transport.close();
    const closeB = transport.close();
    await Promise.all([closeA, closeB]);

    const second = transport.connect();
    const b = instances[1]!;
    b.fire('open');
    await second;
    events.length = 0;

    // B must still be fully alive — not retired by the second close.
    b.fire('message', { data: '{"id":2}\n' });
    expect(events).toEqual([{ type: 'data', text: '{"id":2}' }]);
    b.fire('close', { code: 1006 });
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);
  });

  it('emits nothing from a candidate whose connect failed', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const p = transport.connect();
    const ws = instances[0]!;
    ws.fire('error', {}); // pre-open failure
    await expect(p).rejects.toBeInstanceOf(TransportError);
    events.length = 0;

    // Queued events from the dead candidate must not surface.
    ws.fire('message', { data: '{"late":1}\n' });
    ws.fire('message', { data: 'x'.repeat(200) });
    ws.fire('error', {});
    ws.fire('close', { code: 1006 });
    expect(events).toEqual([]);
  });

  it('a peer close between open and publication fails the connect', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const connectP = transport.connect();
    const ws = instances[0]!;
    ws.fire('open'); // resolves the inner promise
    ws.fire('close', { code: 1006 }); // dies before the continuation publishes it

    await expect(connectP).rejects.toThrow(/closed during connect/);
    await expect(transport.send('x')).rejects.toBeInstanceOf(TransportError);
    // No close event: connect() never exposed this socket to the caller.
    expect(events).toEqual([]);

    // Late traffic from the dead candidate stays inert.
    ws.fire('message', { data: '{"late":1}\n' });
    ws.fire('error', {});
    expect(events).toEqual([]);
  });

  it('retires the generation on a published peer close', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const connectP = transport.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await connectP;
    ws.fire('close', { code: 1006 }); // peer drops a live connection
    expect(events.filter((e) => e.type === 'close')).toHaveLength(1);
    events.length = 0;

    // Nothing more from that socket may surface.
    ws.fire('message', { data: '{"late":1}\n' });
    ws.fire('error', {});
    ws.fire('close', { code: 1006 });
    expect(events).toEqual([]);
  });

  it('close() aborts the live attempt even after a retired candidate fires late', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      connectTimeoutMs: 60_000, // close must NOT wait this out
    });
    // Candidate A fails, retiring itself.
    const aP = transport.connect();
    const a = instances[0]!;
    a.fire('error', {});
    await expect(aP).rejects.toBeInstanceOf(TransportError);

    // Candidate B is pending (never opens).
    const bP = transport.connect();
    const rejected = expect(bP).rejects.toThrow(/closed during connect/);
    // A's delayed events must not clear B's abort hook.
    a.fire('open');
    a.fire('error', {});

    const started = Date.now();
    await transport.close(); // aborts B rather than waiting for its timeout
    expect(Date.now() - started).toBeLessThan(2000);
    await rejected;
  });

  it('nothing surfaces from a socket after a framing-limit teardown', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 64,
    });
    const events: TransportEvent[] = [];
    transport.on((ev) => events.push(ev));

    const connectP = transport.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await connectP;

    ws.fire('message', { data: 'x'.repeat(200) }); // trips the cap
    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    events.length = 0;

    // Everything this socket queued behind the terminal pair is inert.
    ws.fire('message', { data: '{"valid":1}\n' });
    ws.fire('message', { data: 'x'.repeat(200) });
    ws.fire('error', {});
    ws.fire('close', { code: 1006 });
    expect(events).toEqual([]);
  });

  it('a throwing listener cannot strand the socket during a framing teardown', async () => {
    const { Ctor, instances } = fakeWsCtor();
    const transport = new WsTransport({
      endpoint: { host: 'h', port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 64,
    });
    const seen: string[] = [];
    transport.on((ev) => {
      seen.push(ev.type);
      if (ev.type === 'error') throw new Error('consumer callback blew up');
    });

    const connectP = transport.connect();
    const a = instances[0]!;
    a.fire('open');
    await connectP;

    a.fire('message', { data: 'x'.repeat(200) }); // trips the cap

    expect(a.closeCalls).toBe(1); // torn down despite the throw
    expect(seen).toEqual(['error', 'close']); // close still delivered
    // The stored socket was cleared, so a replacement connect is allowed.
    const second = transport.connect();
    instances[1]!.fire('open');
    await second;
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

  it('works without a global TextDecoder (Hermes): text and binary frames both flow', async () => {
    // Stock React Native Hermes has no TextDecoder. Constructing the
    // transport must not require one (the field initializer used to throw),
    // and binary frames must still decode — validation and assembly are
    // ours, so the platform decoder is an optimization, not a dependency.
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

      FakeWs.last.dispatch('message', { data: new TextEncoder().encode('{"b":"€"}\n').buffer });
      expect(events).toContainEqual({ type: 'data', text: '{"b":"€"}' });
      expect(events.some((e) => e.type === 'error')).toBe(false);

      // A frame of a shape we cannot read at all is still terminal.
      FakeWs.last.dispatch('message', { data: 42 });
      expect(
        events.some(
          (e) => e.type === 'error' && String(e.cause).includes('undecodable message data'),
        ),
      ).toBe(true);
      expect(events.some((e) => e.type === 'close')).toBe(true);

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

  it('decodes a multi-byte character split across two binary frames', async () => {
    // Binary frames are byte-sliced by whatever produced them; decoding
    // each frame independently turned a straddling code point into
    // replacement characters in both halves and corrupted the JSON line.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline' },
      WebSocket: Ctor,
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });
    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await p;

    // '€' is E2 82 AC — split after the first byte.
    ws.fire('message', { data: new Uint8Array([0x7b, 0x22, 0x62, 0x22, 0x3a, 0x22, 0xe2]) });
    ws.fire('message', { data: new Uint8Array([0x82, 0xac, 0x22, 0x7d, 0x0a]) });

    expect(data).toEqual(['{"b":"€"}']);
    await t.close();
  });

  it('treats an undecodable frame as terminal instead of leaving the socket open', async () => {
    // ElectrumClient deliberately ignores a standalone transport error
    // and waits for close, so emitting only `error` here left every
    // in-flight request to hang to its timeout with no reconnect
    // scheduled — on Hermes, where TextDecoder is absent, one binary
    // frame was enough.
    const g = globalThis as { TextDecoder?: unknown };
    const saved = g.TextDecoder;
    g.TextDecoder = undefined;
    try {
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: 42 });

      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      expect((events[0] as { cause: Error }).cause).toBeInstanceOf(TransportError);
      expect(ws.closeCalls).toBe(1);
      // The native close that follows must not surface a second time.
      ws.fire('close', { code: 1006 });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      // The instance is free to connect again.
      const again = t.connect();
      await delay(0);
      expect(instances).toHaveLength(2);
      instances[1]!.fire('open');
      await again;
      await t.close();
    } finally {
      g.TextDecoder = saved;
    }
  });

  it('does not let binary decoder state jump over an intervening text frame', async () => {
    // WebSocket permits mixing frame types. A binary frame that ends
    // mid-character used to leave those bytes in the streaming decoder,
    // so a later binary frame completed the character AFTER text that
    // arrived earlier on the wire — reordered content, and a newline in
    // the wrong place. A truncated byte stream is terminal instead.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));
    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await p;

    ws.fire('message', { data: new Uint8Array([0xe2]) }); // first byte of '€'
    ws.fire('message', { data: '{"id":1}\n' });

    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    expect(events.filter((e) => e.type === 'data')).toEqual([]);
  });

  it('keeps decoding binary frames after an intervening complete text frame', async () => {
    // The flush must only fail a stream that was actually truncated:
    // alternating whole text and whole binary frames is legal.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });
    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await p;

    ws.fire('message', { data: new TextEncoder().encode('{"a":1}\n') });
    ws.fire('message', { data: '{"b":2}\n' });
    ws.fire('message', { data: new TextEncoder().encode('{"c":"€"}\n') });

    expect(data).toEqual(['{"a":1}', '{"b":2}', '{"c":"€"}']);
    await t.close();
  });

  it('ends the connection on malformed UTF-8 in a binary frame', async () => {
    // A non-fatal decoder replaces the bad byte with U+FFFD and the JSON
    // still parses, so corrupted content would be delivered as data.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));
    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await p;

    // {"a":"<FF>"}\n — 0xff is never valid UTF-8.
    ws.fire('message', {
      data: new Uint8Array([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xff, 0x22, 0x7d, 0x0a]),
    });

    expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    expect(events.filter((e) => e.type === 'data')).toEqual([]);
  });

  it('lets an empty text frame pass between the halves of a split character', async () => {
    // An empty frame carries nothing, so it cannot reorder the byte
    // stream — ending a legitimately split sequence on a keepalive would
    // drop a working connection.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline' },
      WebSocket: Ctor,
    });
    const data: string[] = [];
    const events: TransportEvent[] = [];
    t.on((e) => {
      events.push(e);
      if (e.type === 'data') data.push(e.text);
    });
    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await p;

    ws.fire('message', { data: new Uint8Array([0x22, 0xe2]) }); // '"' + first byte of '€'
    ws.fire('message', { data: '' });
    ws.fire('message', { data: new Uint8Array([0x82, 0xac, 0x22, 0x0a]) });

    expect(data).toEqual(['"€"']);
    expect(events.some((e) => e.type === 'close')).toBe(false);
    await t.close();
  });

  it('stops delivering a chunk once a listener closes the transport', async () => {
    // Lines are emitted as the framer finds them, so a listener that
    // calls close() on the first line of a multi-line frame must not then
    // receive the rest: nothing from a retired socket may surface.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      // Tunnel framing — the multi-line frame this test dispatches
      // per-line is only legal there.
      endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline' },
      WebSocket: Ctor,
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type !== 'data') return;
      data.push(e.text);
      if (data.length === 1) void t.close();
    });
    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await p;

    ws.fire('message', { data: 'one\ntwo\n' });

    expect(data).toEqual(['one']);
  });

  it('a listener that resubscribes itself is not called again for the same event', async () => {
    // A Set iterator revisits an entry removed and re-added while it
    // runs, so unsubscribe-then-subscribe from inside a callback spun.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    let calls = 0;
    let unsubscribe: (() => void) | undefined;
    const listener = (e: TransportEvent): void => {
      if (e.type !== 'data') return;
      calls++;
      if (calls > 20) return; // guard so a regression fails instead of hanging
      unsubscribe?.();
      unsubscribe = t.on(listener);
    };
    unsubscribe = t.on(listener);
    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('open');
    await p;

    ws.fire('message', { data: '{"id":1}\n' });

    expect(calls).toBe(1);
    await t.close();
  });

  it('a candidate that fails before open cannot deliver data in the same turn', async () => {
    // `reject` only schedules; a socket that fires `error` and then a
    // buffered `message` in the same turn would otherwise still find its
    // generation current and emit data from a connection that never
    // opened — through ElectrumClient, a notification from a server the
    // caller was told it could not reach.
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws' },
      WebSocket: Ctor,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('error', {});
    ws.fire('message', { data: '{"id":1}\n' });

    // Captured BEFORE awaiting the rejection: the catch that follows a
    // failed attempt closes the socket too, so a check afterwards would
    // pass even if this branch stopped closing it.
    const closedBeforeReject = ws.closeCalls;
    await expect(p).rejects.toBeInstanceOf(TransportError);
    expect(events.filter((e) => e.type === 'data')).toEqual([]);
    expect(closedBeforeReject).toBeGreaterThan(0);
  });

  it('a protocol failure before open fails the attempt instead of hanging it', async () => {
    // `failStream` mutes the native close, so an unusable frame arriving
    // before the handshake completed left nothing to settle connect():
    // it sat out the whole connect timeout while the transport had
    // already declared the socket dead, and the manager's reconnect then
    // hit "connect already in progress".
    const { Ctor, instances } = fakeWsCtor();
    const t = new WsTransport({
      endpoint: { host: HOST, port: 1, protocol: 'ws' },
      WebSocket: Ctor,
      maxLineLength: 16,
      connectTimeoutMs: 5_000,
    });
    const events: TransportEvent[] = [];
    t.on((e) => events.push(e));

    const p = t.connect();
    const ws = instances[0]!;
    ws.fire('message', { data: `${'x'.repeat(64)}\n` });

    const settled = await Promise.race([
      p.then(
        () => 'resolved',
        (e: Error) => e.message,
      ),
      delay(200).then(() => 'still pending'),
    ]);

    expect(settled).toMatch(/line length limit exceeded/);
    // Nothing surfaced: listeners never saw this connection open.
    expect(events).toEqual([]);
  });

  describe('native message framing (Fulcrum-style WS servers)', () => {
    it('dispatches a complete unterminated message', async () => {
      // The Electrum protocol over native WebSocket sends one complete
      // JSON-RPC payload per message with NO trailing newline. Requiring
      // the delimiter held every response in the framer forever: each
      // request timed out and no compliant native server could ever
      // work. The message boundary IS the delimiter.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const data: string[] = [];
      t.on((e) => {
        if (e.type === 'data') data.push(e.text);
      });
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '{"id":1,"result":"ok"}' });
      expect(data).toEqual(['{"id":1,"result":"ok"}']);

      ws.fire('message', { data: '{"id":2,"result":"next"}' });
      expect(data).toEqual(['{"id":1,"result":"ok"}', '{"id":2,"result":"next"}']);
      await t.close();
    });

    it('keeps message framing after a terminated message (no sticky latch)', async () => {
      // A gateway may terminate some messages and not others. Message
      // framing must not be disabled by the first '\n' it sees — a
      // sticky latch here was tried and it silently concatenated every
      // later unterminated response with the next message.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const data: string[] = [];
      t.on((e) => {
        if (e.type === 'data') data.push(e.text);
      });
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '{"id":1}\n' }); // terminated
      ws.fire('message', { data: '{"id":2}' }); // bare — must still dispatch
      expect(data).toEqual(['{"id":1}', '{"id":2}']);
      await t.close();
    });

    it('tolerates a run of trailing terminators — padding, not payloads', async () => {
      // The old framer path dropped empty lines as keepalives; a peer
      // padding its response with '\n\n' (or CRLF pairs) sends one
      // payload plus padding, not an interior newline.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '{"id":1}\n\n' });
      ws.fire('message', { data: '\n{"id":2}\r\n\r\n' });
      expect(events).toEqual([
        { type: 'data', text: '{"id":1}' },
        { type: 'data', text: '{"id":2}' },
      ]);
      await t.close();
    });

    it('a terminator-only message is a keepalive', async () => {
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '\r\n\r\n' });
      expect(events).toEqual([]);
      await t.close();
    });

    it('pre-decode gate holds in message mode even with a trailing newline', async () => {
      // In message mode the whole (terminator-stripped) payload is one
      // line, so the oversize gate applies regardless of newline bytes —
      // a huge binary message must not buy a full decode allocation by
      // ending in 0x0a. Invalid UTF-8 payload: reaching the decoder
      // would fail as a decode error instead of the length error.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      const payload = new Uint8Array(192);
      payload.fill(0xff);
      payload[191] = 0x0a;
      ws.fire('message', { data: payload });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      const err = events[0] as { cause: unknown };
      expect(String(err.cause)).toMatch(/line length/);
    });

    it('a cap-length payload with a coalesced keepalive still delivers', async () => {
      // Allowance boundary, accept side: a maximum-size response whose
      // sloppy peer appends its own terminator plus a CRLF-CRLF
      // keepalive in the same message (5 terminator chars) must pass —
      // tightening the allowance below real coalesced-keepalive shapes
      // turns benign traffic into a spurious disconnect.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: 'x'.repeat(8) + '\n\r\n\r\n' });
      expect(events).toEqual([{ type: 'data', text: 'x'.repeat(8) }]);
      await t.close();
    });

    it('padding one char past the allowance fails', async () => {
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: 'x'.repeat(8) + '\n'.repeat(17) });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    });

    it('padding cannot exceed the cap — the raw message is bounded first', async () => {
      // Edge-terminator tolerance is charity for sloppy peers, not an
      // exemption from the bound: trimming an UNBOUNDED run before the
      // cap check let a hostile peer ship arbitrarily large messages
      // (and, for binary, buy the decode allocation) as long as the
      // core payload stayed small. The raw message is capped before any
      // trimming; a few terminator chars of padding fit, a flood fails.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '\n'.repeat(100) + '{}' });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      expect(events.filter((e) => e.type === 'data')).toEqual([]);
    });

    it('padding cannot buy the decode allocation for a binary message', async () => {
      // All-padding-plus-tail binary: stripping before the gate let it
      // through to the decoder. The pre-decode gate judges the raw
      // byte length (plus the padding allowance), so this fails on
      // length — invalid UTF-8 in the tail proves the decoder never ran.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      const payload = new Uint8Array(192);
      payload.fill(0x0a);
      payload[191] = 0xff;
      ws.fire('message', { data: payload });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      const err = events[0] as { cause: unknown };
      expect(String(err.cause)).toMatch(/line length/);
    });

    it('a pretty-printed JSON payload with interior newlines delivers whole', async () => {
      // RFC 8259 permits LF as insignificant whitespace between tokens;
      // a compliant server may pretty-print a response. The message is
      // the framing unit, so the bounded payload is emitted WHOLE — the
      // client's JSON decoder is the validator, and a message packing
      // several newline-separated roots fails there, loudly, as one
      // malformed value.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      const pretty = '{\n  "id": 1,\n  "result": "ok"\n}';
      ws.fire('message', { data: pretty });
      expect(events).toEqual([{ type: 'data', text: pretty }]);
      await t.close();
    });

    it('bounds the WHOLE message by the cap in message mode', async () => {
      // The message is the payload unit, so the cap applies to it as
      // one line — newline-joined fragments must not each pass a
      // per-line check while the total is unbounded.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: 'x'.repeat(9) });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    });

    it('honors the endpoint-declared line cap', async () => {
      // `ServerSpec.maxLineLength` reaches the transport as
      // `endpoint.maxLineLength`; without this fallback the manager has no
      // way to size the cap per server at all.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: 'way more than eight characters' });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    });

    it('a coalesced multi-line message within the message bound delivers', async () => {
      // A tunnel legitimately packs several newline-delimited responses
      // into one message (TCP chunking); lines are judged by the framer.
      // The MESSAGE itself is still bounded (cap + padding allowance) —
      // a real bridge relays kernel-sized chunks, far below any sane
      // cap, so the bound never touches legitimate tunnel traffic.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline', maxLineLength: 64 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      // 10 short lines in one message — aggregate above the per-line
      // cap is fine; lines are the framer's unit.
      const lines = Array.from({ length: 10 }, (_, i) => `"ln_${i}"`);
      const payload = new TextEncoder().encode(lines.join('\n') + '\n');
      ws.fire('message', { data: payload });
      expect(events.filter((e) => e.type === 'error')).toEqual([]);
      expect(events.filter((e) => e.type === 'data')).toHaveLength(10);
      await t.close();
    });

    it('a multibyte binary payload within the decoded bound delivers, same as text', async () => {
      // The gate's byte heuristic (ceil(N/3), the minimum decode) must
      // stay a MINIMUM: multibyte content carries up to 3 wire bytes
      // per decoded unit, and a payload whose decoded size fits the cap
      // is legitimate whichever frame type the peer chose. (The wire
      // bytes themselves are materialized by the platform before we run
      // — the pre-delivery bound is the WebSocket implementation's own
      // maxPayload, as documented.)
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 64 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      // 40 three-byte chars: 120 wire bytes, 40 decoded units — over
      // the cap in bytes, comfortably under it decoded.
      const payload = '\u20ac'.repeat(40);
      ws.fire('message', { data: new TextEncoder().encode(payload) });
      ws.fire('message', { data: payload });
      expect(events).toEqual([
        { type: 'data', text: payload },
        { type: 'data', text: payload },
      ]);
      await t.close();
    });

    it('an all-padding binary flood fails loudly, not silently', async () => {
      // ceil(N/3) is the MINIMUM decode; padding bytes decode 1:1, so a
      // binary flood of newline bytes up to 3x the bound passes the
      // pre-decode gate. The post-decode check must then fail it — the
      // old behavior swallowed it as a keepalive after scanning every
      // char.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: new Uint8Array(60).fill(0x0a) });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      await t.close();
    });

    it('two near-cap lines coalesced into one message deliver', async () => {
      // A bridge may legally coalesce complete responses, so a message
      // of several near-cap lines is valid — the aggregate default
      // scales as 4x the line cap (floored at 8 MiB) precisely so
      // legitimate coalescing never trips it.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: {
          host: HOST,
          port: 1,
          protocol: 'ws',
          wsFraming: 'newline',
          maxLineLength: 1_048_576,
        },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      const line = 'x'.repeat(1_000_000);
      ws.fire('message', { data: line + '\n' + line + '\n' });
      expect(events.filter((e) => e.type === 'error')).toEqual([]);
      expect(events.filter((e) => e.type === 'data')).toHaveLength(2);
      await t.close();
    });

    it('rejects a maxMessageLength below the line cap at construction', () => {
      // An aggregate bound under the per-line cap would tear the
      // connection down on every legal single-line response — a
      // misconfiguration, caught where it is written.
      expect(
        () =>
          new WsTransport({
            endpoint: {
              host: HOST,
              port: 1,
              protocol: 'ws',
              wsFraming: 'newline',
              maxLineLength: 1024,
              maxMessageLength: 512,
            },
            WebSocket: fakeWsCtor().Ctor,
          }),
      ).toThrow(/maxMessageLength/);
    });

    it('an aggregate rejection names maxMessageLength, not the line cap', async () => {
      // The two bounds are separate options; blaming the wrong one
      // sends an operator tuning a knob that will not help.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: {
          host: HOST,
          port: 1,
          protocol: 'ws',
          wsFraming: 'newline',
          maxLineLength: 8,
          maxMessageLength: 64,
        },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '{}\n'.repeat(30) });
      const err = events[0] as { cause: { cause?: unknown } };
      expect(String(err.cause.cause)).toMatch(/maxMessageLength \(64\)/);
      await t.close();
    });

    it('an invalid maxMessageLength names itself in the RangeError', () => {
      expect(
        () =>
          new WsTransport({
            endpoint: {
              host: HOST,
              port: 1,
              protocol: 'ws',
              wsFraming: 'newline',
              maxMessageLength: Number.NaN,
            },
            WebSocket: fakeWsCtor().Ctor,
          }),
      ).toThrow(/maxMessageLength must be a positive safe integer/);
    });

    it('maxMessageLength overrides the aggregate default per endpoint', async () => {
      // Operators with exotic bridges size the aggregate bound
      // themselves; the option is the escape hatch that keeps the
      // default from being an arbitrary constant somebody's traffic
      // eventually falsifies.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: {
          host: HOST,
          port: 1,
          protocol: 'ws',
          wsFraming: 'newline',
          maxLineLength: 8,
          maxMessageLength: 64,
        },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '{}\n'.repeat(30) });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      await t.close();
    });

    it('newline mode still bounds a message flood — browsers have no maxPayload', async () => {
      // Native browser WebSocket exposes no receive bound, so "let
      // maxPayload handle it" leaves browser wallets open: one enormous
      // message of tiny lines would decode whole and dispatch in one
      // synchronous storm. The aggregate default scales with the cap
      // (floored at 8 MiB) and fails the flood.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '{}\n'.repeat(3_000_000) });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      expect(events.filter((e) => e.type === 'data')).toEqual([]);
      await t.close();
    });

    it('a leading newline byte does not buy a hostile binary payload the decode', async () => {
      // The single-line pre-decode gate is skipped when 0x0a is
      // present; the aggregate pre-decode bound must still fire on raw
      // bytes. Invalid UTF-8 after the newline proves the decoder never
      // ran — reaching it would surface as a decode error.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      // Past 3x the 8 MiB default aggregate bound, so even the minimum
      // decode exceeds it.
      const payload = new Uint8Array(26_000_000);
      payload.fill(0xff);
      payload[0] = 0x0a;
      ws.fire('message', { data: payload });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      const err = events[0] as { cause: unknown };
      expect(String(err.cause)).toMatch(/line length/);
      expect(String(err.cause)).not.toMatch(/undecodable/);
    });

    it('a large aggregated newline-mode message of valid lines delivers', async () => {
      // RFC 6455 imposes no relationship between WebSocket messages and
      // the kernel reads a bridge relays: an aggregating bridge may
      // legally pack several near-cap responses into one multi-hundred-
      // KiB message. Lines are the framer's unit; the message is not.
      // (The receive bound for what the platform materializes is the
      // injected WebSocket implementation's own maxPayload.)
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: {
          host: HOST,
          port: 1,
          protocol: 'ws',
          wsFraming: 'newline',
          maxLineLength: 65536,
        },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      // Five ~60 KiB lines: ~300 KiB in one message, every line legal.
      const line = 'x'.repeat(60_000);
      ws.fire('message', { data: (line + '\n').repeat(5) });
      expect(events.filter((e) => e.type === 'error')).toEqual([]);
      expect(events.filter((e) => e.type === 'data')).toHaveLength(5);
      await t.close();
    });

    it('a coalesced burst larger than a small cap still delivers in newline mode', async () => {
      // A memory-constrained deployment tunes maxLineLength low, and a
      // notification burst coalesces into one multi-KB chunk of tiny
      // lines. The per-line cap governs lines; the aggregate DEFAULT
      // (8 MiB floor) never touches a burst like this — only an
      // explicit low maxMessageLength could.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: {
          host: HOST,
          port: 1,
          protocol: 'ws',
          wsFraming: 'newline',
          maxLineLength: 1024,
        },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      // ~60 KB of 200-char lines — a realistic kernel-sized chunk.
      const line = 'x'.repeat(199);
      ws.fire('message', { data: (line + '\n').repeat(300) });
      expect(events.filter((e) => e.type === 'error')).toEqual([]);
      expect(events.filter((e) => e.type === 'data')).toHaveLength(300);
      await t.close();
    });

    it('a CRLF-terminated line of exactly the cap length still delivers', async () => {
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: 'x'.repeat(8) + '\r\n' });
      expect(events).toEqual([{ type: 'data', text: 'x'.repeat(8) }]);
      await t.close();
    });

    it('newline mode rejects an oversized single-line binary before decoding it', async () => {
      // A payload with no 0x0a byte is one line by construction; if its
      // minimum decode already exceeds the per-line cap, the framer is
      // guaranteed to reject it — fail before the decode allocation.
      // Invalid UTF-8 proves the decoder never ran: reaching it would
      // surface as a decode error instead of the length error.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', wsFraming: 'newline', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: new Uint8Array(192).fill(0xff) });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      const err = events[0] as { cause: unknown };
      expect(String(err.cause)).toMatch(/line length/);
      expect(String(err.cause)).not.toMatch(/undecodable/);
    });

    it('rejects an oversized binary message before decoding it', async () => {
      // The message itself is already materialized by the platform, but
      // the UTF-8 decode would allocate a second full copy. A payload
      // whose minimum decoded size exceeds the cap must fail on length —
      // not reach the decoder at all (this payload is invalid UTF-8, so
      // reaching the decoder would surface as a decode error instead).
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: new Uint8Array(192).fill(0xff) });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      const err = events[0] as { cause: unknown };
      expect(String(err.cause)).toMatch(/line length/);
    });

    it('endpoint cap outranks the construction-opts cap', async () => {
      // opts.maxLineLength is a fleet-wide factory default; the endpoint
      // value sizes one specific server and must win, or the per-server
      // knob is a no-op for every custom-factory deployment.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws', maxLineLength: 8 },
        WebSocket: Ctor,
        maxLineLength: 1024,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: 'way more than eight characters' });
      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
    });

    it('sends one bare payload per message — no trailing newline', async () => {
      // A strict per-message parser may reject trailing bytes; the
      // outbound contract is the same one the inbound relies on.
      const { Ctor, instances } = fakeWsCtor();
      const sent: string[] = [];
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const p = t.connect();
      const ws = instances[0]!;
      ws.send = (data: string) => {
        sent.push(data);
      };
      ws.fire('open');
      await p;

      await t.send('{"id":1}');
      expect(sent).toEqual(['{"id":1}']);
      await t.close();
    });

    it('fails loudly when a binary message ends mid-character', async () => {
      // A message-framed peer never splits a character across messages:
      // pending decoder bytes at the boundary mean the payload is
      // truncated. Flushing would emit the broken prefix as data and
      // leak the tail into the next payload — corrupting two responses
      // silently instead of failing one connection loudly.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      // '{"a":"€' with the euro cut after its first byte.
      ws.fire('message', { data: new Uint8Array([0x7b, 0x22, 0x61, 0x22, 0x3a, 0x22, 0xe2]) });

      expect(events.map((e) => e.type)).toEqual(['error', 'close']);
      expect(events.filter((e) => e.type === 'data')).toEqual([]);
    });

    it('treats a lone CR message as a keepalive, not as data', async () => {
      // The synthetic boundary delimiter goes through the framer's own
      // CRLF rule, so a bare '\r' classifies exactly as '\r\n' would —
      // an empty keepalive line, dropped.
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const events: TransportEvent[] = [];
      t.on((e) => events.push(e));
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: '\r' });
      expect(events).toEqual([]);
      await t.close();
    });

    it('binary native messages dispatch at the message boundary too', async () => {
      const { Ctor, instances } = fakeWsCtor();
      const t = new WsTransport({
        endpoint: { host: HOST, port: 1, protocol: 'ws' },
        WebSocket: Ctor,
      });
      const data: string[] = [];
      t.on((e) => {
        if (e.type === 'data') data.push(e.text);
      });
      const p = t.connect();
      const ws = instances[0]!;
      ws.fire('open');
      await p;

      ws.fire('message', { data: new TextEncoder().encode('{"id":1,"result":"€"}') });
      expect(data).toEqual(['{"id":1,"result":"€"}']);
      await t.close();
    });
  });
});
