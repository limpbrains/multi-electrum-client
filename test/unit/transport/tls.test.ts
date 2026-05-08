// TLS transport unit tests.
//
// We don't spin up a real TLS server here — that needs a self-signed CA
// dance that's noisy. Instead we feed the transport a mock socket via the
// `connect` injection hook. The connect-and-emit-events pipeline shared
// with TcpTransport is already covered by tcp.test.ts (and integration
// tests will exercise real TLS on the Docker compose stack in M6+).

import { EventEmitter } from 'node:events';

import { describe, expect, it } from 'vitest';

import { TransportError } from '../../../src/errors/types.js';
import type { TcpSocketLike } from '../../../src/transport/tcp.js';
import { TlsTransport } from '../../../src/transport/tls.js';

class MockSocket extends EventEmitter implements TcpSocketLike {
  readonly writes: string[] = [];
  ended = false;
  destroyed = false;
  setEncoding(_encoding: BufferEncoding): this {
    return this;
  }
  write(chunk: string | Uint8Array): boolean {
    const s = typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf-8');
    this.writes.push(s);
    return true;
  }
  end(): void {
    this.ended = true;
    queueMicrotask(() => this.emit('close', false));
  }
  destroy(): void {
    this.destroyed = true;
    this.emit('close', true);
  }
}

describe('TlsTransport', () => {
  it('refuses non-tls protocol', () => {
    expect(
      () =>
        new TlsTransport({
          endpoint: { host: 'h', port: 1, protocol: 'tcp' },
        }),
    ).toThrow(TransportError);
    expect(
      () =>
        new TlsTransport({
          endpoint: { host: 'h', port: 1, protocol: 'wss' },
        }),
    ).toThrow(TransportError);
  });

  it('connects via injected socket factory and round-trips a message', async () => {
    const socket = new MockSocket();
    const t = new TlsTransport({
      endpoint: { host: 'h', port: 50002, protocol: 'tls' },
      connect: () => socket,
    });
    const data: string[] = [];
    t.on((e) => {
      if (e.type === 'data') data.push(e.text);
    });

    const connectPromise = t.connect();
    queueMicrotask(() => socket.emit('connect'));
    await connectPromise;

    await t.send('{"id":1}');
    expect(socket.writes).toEqual(['{"id":1}\n']);

    socket.emit('data', '{"id":1,"result":"ok"}\n');
    expect(data).toEqual(['{"id":1,"result":"ok"}']);

    await t.close();
    expect(socket.ended).toBe(true);
  });

  it('connect timeout rejects with TransportError', async () => {
    const socket = new MockSocket();
    const t = new TlsTransport({
      endpoint: { host: 'h', port: 50002, protocol: 'tls' },
      connect: () => socket,
      connectTimeoutMs: 20,
    });
    await expect(t.connect()).rejects.toBeInstanceOf(TransportError);
  });

  it('forwards send / close to the underlying socket', async () => {
    const socket = new MockSocket();
    const t = new TlsTransport({
      endpoint: { host: 'h', port: 50002, protocol: 'tls' },
      connect: () => socket,
    });
    const connectPromise = t.connect();
    queueMicrotask(() => socket.emit('connect'));
    await connectPromise;
    await t.send('hello');
    expect(socket.writes).toEqual(['hello\n']);
    await t.close();
  });
});
