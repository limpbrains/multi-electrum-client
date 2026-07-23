// Milestone-1 probe: exercises the whole vitest-on-device pipeline
// (Metro alias -> vitest-shim -> harness runtime) before pointing the
// harness at the real unit suite. Not part of the shipped test run.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { setTimeout as delay } from 'node:timers/promises';
import { EventEmitter } from 'node:events';
import net from 'node:net';
import { connect as tlsConnect } from 'node:tls';

describe('vitest shim pipeline', () => {
  it('basic sync assertion', () => {
    expect(1 + 1).toBe(2);
  });

  it('async rejects matcher', async () => {
    await expect(Promise.reject(new TypeError('boom'))).rejects.toBeInstanceOf(TypeError);
  });

  it('vi.fn + toHaveBeenCalledWith', () => {
    const f = vi.fn((x: number) => x * 2);
    expect(f(21)).toBe(42);
    expect(f).toHaveBeenCalledWith(21);
    expect(f).toHaveBeenCalledTimes(1);
  });

  it('vi.spyOn + restoreAllMocks', () => {
    const obj = { greet: () => 'hi' };
    const spy = vi.spyOn(obj, 'greet').mockReturnValue('mocked');
    expect(obj.greet()).toBe('mocked');
    vi.restoreAllMocks();
    expect(obj.greet()).toBe('hi');
    expect(spy).toHaveBeenCalledTimes(1);
  });

  it('Buffer global installed by setup', () => {
    expect(typeof Buffer).toBe('function');
    expect(Buffer.from('ab').toString('utf-8')).toBe('ab');
  });

  it('node:events alias works', () => {
    const ee = new EventEmitter();
    let got = '';
    ee.on('x', (v: string) => {
      got = v;
    });
    ee.emit('x', 'y');
    expect(got).toBe('y');
  });

  it('node:timers/promises delay works', async () => {
    const before = Date.now();
    await delay(20);
    expect(Date.now() - before).toBeGreaterThanOrEqual(15);
  });

  it('native tcp module is linked (loopback echo)', async () => {
    const received: string[] = [];
    const server = net.createServer((sock) => {
      sock.setEncoding('utf-8');
      sock.on('data', (chunk) => {
        received.push(typeof chunk === 'string' ? chunk : String(chunk));
        sock.write('pong\n');
      });
    });
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => resolve());
    });
    const addr = server.address();
    expect(addr).not.toBeNull();
    const port = (addr as { port: number }).port;
    expect(port).toBeGreaterThan(0);

    const replies: string[] = [];
    const client = net.connect({ host: '127.0.0.1', port });
    client.setEncoding('utf-8');
    await new Promise<void>((resolve, reject) => {
      client.on('error', reject);
      client.on('connect', () => resolve());
    });
    client.on('data', (chunk) => {
      replies.push(typeof chunk === 'string' ? chunk : String(chunk));
    });
    client.write('ping\n');
    for (let i = 0; i < 100 && replies.length === 0; i++) {
      await delay(10);
    }
    client.destroy();
    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
    expect(received.join('')).toContain('ping');
    expect(replies.join('')).toContain('pong');
  });

  it('node:tls alias importable', () => {
    expect(typeof tlsConnect).toBe('function');
  });
});

describe('fake timers', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it('advanceTimersByTime fires setTimeout callbacks', () => {
    const cb = vi.fn();
    setTimeout(cb, 1000);
    vi.advanceTimersByTime(999);
    expect(cb).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(cb).toHaveBeenCalledTimes(1);
  });

  it('advanceTimersByTimeAsync interleaves promises with timers', async () => {
    const order: string[] = [];
    const chained = new Promise<void>((resolve) => {
      setTimeout(() => {
        order.push('t1');
        resolve();
      }, 100);
    }).then(() => {
      order.push('p1');
      return new Promise<void>((resolve) => {
        setTimeout(() => {
          order.push('t2');
          resolve();
        }, 100);
      });
    });
    await vi.advanceTimersByTimeAsync(250);
    await chained;
    expect(order).toEqual(['t1', 'p1', 't2']);
  });
});

describe.each([
  { name: 'alpha', value: 1 },
  { name: 'beta', value: 2 },
])('describe.each case $name', ({ value }) => {
  it('receives the case object', () => {
    expect(value).toBeGreaterThan(0);
  });
});
