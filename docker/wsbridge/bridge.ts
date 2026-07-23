// ws ↔ tcp bridge for the browser demo (run with Bun — see the `demo`
// profile in ../compose.yml).
//
// Browsers can't open raw TCP sockets, and the stack's electrum servers
// are TCP-only. Each WebSocket client that connects here gets its own
// TCP connection to TARGET_HOST:TARGET_PORT; frames are relayed as raw
// bytes in both directions. Text and binary frames are both accepted
// from the browser (the library's WsTransport sends text); TCP data
// goes back as binary frames (WsTransport's toText decodes ArrayBuffer).
//
// The TCP connection is established BEFORE the WebSocket upgrade is
// accepted. If the upstream is down the handshake fails with 502, so
// the browser client's connect() rejects instead of flapping through a
// fake connected→disconnected cycle (which would also keep resetting
// the client's reconnect backoff).
//
// Deliberately minimal: no backpressure handling, no TLS — demo scale
// only. Off-the-shelf websockify images were rejected for being
// amd64-only (this repo's dev machines include arm64).

import net from 'node:net';

const PORT = Number(process.env['LISTEN_PORT'] ?? '8080');
const TARGET_HOST = process.env['TARGET_HOST'] ?? 'toxiproxy';
const TARGET_PORT = Number(process.env['TARGET_PORT'] ?? '52011');
const CONNECT_TIMEOUT_MS = 5000;

interface Ctx {
  sock: net.Socket | null;
}

function connectUpstream(): Promise<net.Socket | null> {
  return new Promise((resolve) => {
    const sock = net.connect({ host: TARGET_HOST, port: TARGET_PORT });
    const timer = setTimeout(() => {
      sock.destroy();
      resolve(null);
    }, CONNECT_TIMEOUT_MS);
    sock.once('connect', () => {
      clearTimeout(timer);
      resolve(sock);
    });
    sock.once('error', () => {
      clearTimeout(timer);
      resolve(null);
    });
  });
}

Bun.serve<Ctx, Record<string, never>>({
  port: PORT,
  async fetch(req, server) {
    const sock = await connectUpstream();
    if (!sock) return new Response('upstream unavailable', { status: 502 });
    // Buffer any upstream bytes until the ws is open.
    sock.pause();
    if (server.upgrade(req, { data: { sock } })) return undefined;
    sock.destroy();
    return new Response('ws-tcp bridge; connect via WebSocket', { status: 426 });
  },
  websocket: {
    open(ws) {
      const sock = ws.data.sock!;
      sock.on('data', (chunk: Buffer) => {
        ws.send(chunk);
      });
      sock.on('close', () => ws.close());
      sock.on('error', () => ws.close());
      sock.resume();
    },
    message(ws, message) {
      const data = typeof message === 'string' ? Buffer.from(message, 'utf-8') : message;
      ws.data.sock?.write(data);
    },
    close(ws) {
      ws.data.sock?.destroy();
      ws.data.sock = null;
    },
  },
});

console.log(`ws->tcp bridge listening on :${PORT} -> ${TARGET_HOST}:${TARGET_PORT}`);
