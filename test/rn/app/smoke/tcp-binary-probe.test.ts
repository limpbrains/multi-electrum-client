// Debug probe: does react-native-tcp-socket's server socket deliver data
// without setEncoding (Buffer path), and does write(Buffer) work?
import { describe, expect, it } from 'vitest';
import net from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';

describe('tcp binary probe', () => {
  it('server receives Buffer chunks and can write Buffers back', async () => {
    const serverSeen: Array<{ type: string; len: number; text: string }> = [];
    const server = net.createServer((sock) => {
      sock.on('data', (chunk: Buffer | string) => {
        const isString = typeof chunk === 'string';
        const buf = isString ? Buffer.from(chunk, 'utf-8') : Buffer.from(chunk);
        serverSeen.push({ type: isString ? 'string' : 'buffer', len: buf.length, text: buf.toString('utf-8') });
        sock.write(Buffer.from([0x81, 0x02, 0x68, 0x69])); // binary write: "hi" ws-style frame
      });
    });
    await new Promise<void>((resolve, reject) => {
      server.once('error', reject);
      server.listen(0, '127.0.0.1', () => resolve());
    });
    const port = (server.address() as { port: number }).port;

    const got: number[][] = [];
    const client = net.connect({ host: '127.0.0.1', port });
    await new Promise<void>((resolve, reject) => {
      client.on('error', reject);
      client.on('connect', () => resolve());
    });
    client.on('data', (chunk: Buffer | string) => {
      const buf = typeof chunk === 'string' ? Buffer.from(chunk, 'utf-8') : Buffer.from(chunk);
      got.push(Array.from(buf));
    });
    client.write('hello');
    for (let i = 0; i < 100 && got.length === 0; i++) await delay(20);
    client.destroy();
    await new Promise<void>((r) => server.close(() => r()));

    expect(serverSeen).toEqual([{ type: 'buffer', len: 5, text: 'hello' }]);
    expect(got).toEqual([[0x81, 0x02, 0x68, 0x69]]);
  });
});
