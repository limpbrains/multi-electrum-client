// Debug probe for the ws-shim: exercises the server half with a raw TCP
// client (no RN WebSocket involved), then the client half. Not part of the
// shipped run.
import { describe, expect, it } from 'vitest';
import net from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { WebSocketServer } from 'ws';
import { shimErrors } from '../harness/ws-shim';

const startServer = async (): Promise<{ srv: WebSocketServer; port: number }> => {
  const srv = new WebSocketServer({ port: 0 });
  await new Promise<void>((resolve, reject) => {
    srv.once('listening', () => resolve());
    srv.once('error', reject);
  });
  const addr = srv.address();
  if (!addr) throw new Error('no address');
  return { srv, port: addr.port };
};

describe('ws-shim probe', () => {
  it('server accepts a raw TCP handshake', async () => {
    const { srv, port } = await startServer();
    expect(port).toBeGreaterThan(0);

    const chunks: string[] = [];
    const sock = net.connect({ host: '127.0.0.1', port });
    await new Promise<void>((resolve, reject) => {
      sock.on('error', (e: Error) => reject(new Error('tcp connect failed: ' + e.message)));
      sock.on('connect', () => resolve());
    });
    sock.on('data', (chunk: Buffer | string) => {
      chunks.push(typeof chunk === 'string' ? chunk : chunk.toString('utf-8'));
    });
    sock.write(
      'GET /probe HTTP/1.1\r\n' +
        'Host: 127.0.0.1\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n' +
        'Sec-WebSocket-Version: 13\r\n\r\n',
    );
    for (let i = 0; i < 100 && chunks.length === 0; i++) await delay(20);
    const response = chunks.join('');
    expect(shimErrors).toEqual([]);
    expect(response).toContain('101 Switching Protocols');
    expect(response).toContain('s3pPLMBiTxaQ9kYGzzhZRbK+xOo=');
    sock.destroy();
    await new Promise<void>((r) => srv.close(() => r()));
  });

  it('RN WebSocket client completes handshake against the shim server', async () => {
    const { srv, port } = await startServer();
    let gotConnection = false;
    srv.on('connection', () => {
      gotConnection = true;
    });
    const ws = new WebSocket(`ws://127.0.0.1:${port}/`);
    const result = await new Promise<string>((resolve) => {
      const timer = setTimeout(() => resolve('timeout waiting for open'), 8000);
      ws.onopen = () => {
        clearTimeout(timer);
        resolve('open');
      };
      ws.onerror = (e: unknown) => {
        clearTimeout(timer);
        resolve('error: ' + String((e as { message?: string })?.message ?? e));
      };
      ws.onclose = (e: { code?: number; reason?: string }) => {
        resolve(`closed code=${e?.code} reason=${e?.reason}`);
      };
    });
    expect({ result, gotConnection }).toEqual({ result: 'open', gotConnection: true });
    ws.close();
    await new Promise<void>((r) => srv.close(() => r()));
  });
});
