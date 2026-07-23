// On-device stand-in for the 'ws' package, wired up via a Metro alias.
//
// The two ws-backed test files spin up a `WebSocketServer` inside the test
// body and drive it with per-test connection callbacks, so a host-side
// sidecar can't substitute — the server has to run in the RN runtime
// itself. This implements the minimal slice of RFC 6455 those tests (and
// RN's own WebSocket client) need, on top of react-native-tcp-socket:
//
//  - HTTP upgrade handshake (Sec-WebSocket-Accept via a local SHA-1)
//  - single-frame masked client->server text messages
//  - unmasked server->client text frames
//  - close frames, both directions
//
// No fragmentation, no ping/pong, no binary frames, no extensions — the
// tests exchange short newline-terminated JSON strings only.
//
// The client half re-exports the runtime's global WebSocket, which is what
// the tests hand to WsTransport as `WebSocketCtor`.
import TcpSocket from 'react-native-tcp-socket';
import { Buffer } from 'buffer';
import { EventEmitter } from 'events';

// ---------------------------------------------------------------------------
// SHA-1 (RFC 3174) — Hermes has no crypto.subtle; handshake needs exactly one
// digest of a short ASCII string, so a compact pure-JS implementation is fine.
const sha1 = (input: string): Buffer => {
  const data = Buffer.from(input, 'utf-8');
  const ml = data.length;
  const withPadding = Buffer.alloc(((ml + 8) >> 6 << 6) + 64);
  data.copy(withPadding);
  withPadding[ml] = 0x80;
  withPadding.writeUInt32BE(ml >>> 29, withPadding.length - 8);
  withPadding.writeUInt32BE((ml << 3) >>> 0, withPadding.length - 4);

  let h0 = 0x67452301, h1 = 0xefcdab89, h2 = 0x98badcfe, h3 = 0x10325476, h4 = 0xc3d2e1f0;
  const w = new Array<number>(80);
  const rol = (n: number, b: number): number => ((n << b) | (n >>> (32 - b))) >>> 0;

  for (let block = 0; block < withPadding.length; block += 64) {
    for (let i = 0; i < 16; i++) w[i] = withPadding.readUInt32BE(block + i * 4);
    for (let i = 16; i < 80; i++) w[i] = rol((w[i - 3]! ^ w[i - 8]! ^ w[i - 14]! ^ w[i - 16]!) >>> 0, 1);
    let a = h0, b = h1, c = h2, d = h3, e = h4;
    for (let i = 0; i < 80; i++) {
      let f: number, k: number;
      if (i < 20) { f = (b & c) | (~b & d); k = 0x5a827999; }
      else if (i < 40) { f = b ^ c ^ d; k = 0x6ed9eba1; }
      else if (i < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8f1bbcdc; }
      else { f = b ^ c ^ d; k = 0xca62c1d6; }
      const t = (rol(a, 5) + f + e + k + w[i]!) >>> 0;
      e = d; d = c; c = rol(b, 30); b = a; a = t;
    }
    h0 = (h0 + a) >>> 0; h1 = (h1 + b) >>> 0; h2 = (h2 + c) >>> 0; h3 = (h3 + d) >>> 0; h4 = (h4 + e) >>> 0;
  }
  const out = Buffer.alloc(20);
  out.writeUInt32BE(h0, 0); out.writeUInt32BE(h1, 4); out.writeUInt32BE(h2, 8);
  out.writeUInt32BE(h3, 12); out.writeUInt32BE(h4, 16);
  return out;
};

const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

// Debug aid: swallowed listener exceptions land here (asserted by the probe).
export const shimErrors: string[] = [];

// ---------------------------------------------------------------------------
// Server-side connection socket, shaped like the slice of `ws.WebSocket` the
// tests use: on('message', (Buffer) => ...), send(string), close().
class ServerSideSocket extends EventEmitter {
  private buffer = Buffer.alloc(0);
  private open = true;

  constructor(private readonly tcp: { write(b: Uint8Array | string): unknown; destroy(): void; on(ev: string, l: (...a: never[]) => void): unknown }) {
    super();
  }

  feed(chunk: Buffer): void {
    this.buffer = Buffer.concat([this.buffer, Buffer.from(chunk)]);
    for (;;) {
      const frame = this.readFrame();
      if (!frame) return;
      if (frame.opcode === 0x8) {
        this.sendClose();
        this.tcp.destroy();
        if (this.open) {
          this.open = false;
          this.emit('close');
        }
        return;
      }
      if (frame.opcode === 0x1 || frame.opcode === 0x2) {
        this.emit('message', frame.payload);
      }
      // Ignore ping/pong/continuation — not produced by the tests' traffic.
    }
  }

  private readFrame(): { opcode: number; payload: Buffer } | null {
    const buf = this.buffer;
    if (buf.length < 2) return null;
    const opcode = buf[0]! & 0x0f;
    const masked = (buf[1]! & 0x80) !== 0;
    let len = buf[1]! & 0x7f;
    let offset = 2;
    if (len === 126) {
      if (buf.length < 4) return null;
      len = buf.readUInt16BE(2);
      offset = 4;
    } else if (len === 127) {
      if (buf.length < 10) return null;
      len = Number(buf.readBigUInt64BE(2));
      offset = 10;
    }
    const maskLen = masked ? 4 : 0;
    if (buf.length < offset + maskLen + len) return null;
    const mask = masked ? buf.subarray(offset, offset + 4) : null;
    const payload = Buffer.from(buf.subarray(offset + maskLen, offset + maskLen + len));
    if (mask) {
      for (let i = 0; i < payload.length; i++) payload[i] = payload[i]! ^ mask[i % 4]!;
    }
    this.buffer = Buffer.from(buf.subarray(offset + maskLen + len));
    return { opcode, payload };
  }

  send(data: string | Uint8Array): void {
    if (!this.open) return;
    const payload = typeof data === 'string' ? Buffer.from(data, 'utf-8') : Buffer.from(data);
    let header: Buffer;
    if (payload.length < 126) {
      header = Buffer.from([0x81, payload.length]);
    } else if (payload.length < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x81;
      header[1] = 126;
      header.writeUInt16BE(payload.length, 2);
    } else {
      throw new Error('ws-shim: frame too large');
    }
    this.tcp.write(Buffer.concat([header, payload]));
  }

  private sendClose(): void {
    try {
      this.tcp.write(Buffer.from([0x88, 0x00]));
    } catch {
      // socket already gone
    }
  }

  close(): void {
    if (!this.open) return;
    this.open = false;
    this.sendClose();
    this.tcp.destroy();
    this.emit('close');
  }

  notifyTcpClosed(): void {
    if (!this.open) return;
    this.open = false;
    this.emit('close');
  }
}

// ---------------------------------------------------------------------------
// Minimal `ws.WebSocketServer`: constructor({ port }), 'listening' /
// 'connection' / 'error' events, address().port, close(cb).
export class WebSocketServer extends EventEmitter {
  private readonly server: ReturnType<typeof TcpSocket.createServer>;
  private readonly connections = new Set<ServerSideSocket>();

  constructor(options: { port: number }) {
    super();
    this.server = TcpSocket.createServer((tcp) => { this.handleTcpConnection(tcp); });
    this.server.on('error', (err: Error) => this.emit('error', err));
    this.server.listen({ port: options.port, host: '127.0.0.1' }, () => {
      this.emit('listening');
    });
  }

  private handleTcpConnection(tcp: {
    on(ev: string, l: (...a: never[]) => void): unknown;
    write(b: Uint8Array | string): unknown;
    destroy(): void;
  }): void {
    let handshakeBuf = Buffer.alloc(0);
    let ws: ServerSideSocket | null = null;

    (tcp as { on(ev: 'data', l: (chunk: Buffer | string) => void): unknown }).on('data', (chunk) => {
      try {
      const bytes = typeof chunk === 'string' ? Buffer.from(chunk, 'utf-8') : Buffer.from(chunk);
      if (ws) {
        ws.feed(bytes);
        return;
      }
      handshakeBuf = Buffer.concat([handshakeBuf, bytes]);
      const headerEnd = handshakeBuf.indexOf('\r\n\r\n');
      if (headerEnd === -1) return;

      // NOTE: on Hermes the buffer ponyfill's subarray() returns a plain
      // Uint8Array (species lookup falls back to Uint8Array), so toString
      // would render comma-separated decimals. Use ranged toString and wrap
      // every subarray result that needs Buffer methods in Buffer.from.
      const header = handshakeBuf.toString('utf-8', 0, headerEnd);
      const rest = Buffer.from(handshakeBuf.subarray(headerEnd + 4));
      const requestLine = header.split('\r\n')[0] ?? '';
      const url = requestLine.split(' ')[1] ?? '/';
      const keyMatch = header.match(/^sec-websocket-key:\s*(.+)$/im);
      if (!keyMatch) {
        tcp.destroy();
        return;
      }
      const accept = sha1(keyMatch[1]!.trim() + WS_GUID).toString('base64');
      tcp.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
          'Upgrade: websocket\r\n' +
          'Connection: Upgrade\r\n' +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      );

      ws = new ServerSideSocket(tcp);
      this.connections.add(ws);
      this.emit('connection', ws, { url });
      if (rest.length > 0) ws.feed(Buffer.from(rest));
      } catch (err) {
        shimErrors.push(String((err as Error)?.stack ?? err));
      }
    });
    (tcp as { on(ev: 'close', l: () => void): unknown }).on('close', () => {
      if (ws) {
        this.connections.delete(ws);
        ws.notifyTcpClosed();
      }
    });
    (tcp as { on(ev: 'error', l: (err: Error) => void): unknown }).on('error', () => {
      // surfaced via 'close'
    });
  }

  address(): { port: number } | null {
    return this.server.address();
  }

  close(callback?: () => void): void {
    for (const ws of this.connections) ws.close();
    this.connections.clear();
    this.server.close();
    // react-native-tcp-socket's Server only fires its close callback once
    // all connections report closed natively; the tests just await teardown,
    // so resolve on the next tick after destroying everything above.
    if (callback) setTimeout(callback, 0);
  }
}

// The tests import { WebSocket } from 'ws' purely as a client constructor to
// hand to WsTransport — RN's built-in WebSocket is exactly that.
export const WebSocket = globalThis.WebSocket;

export default { WebSocketServer, WebSocket };
