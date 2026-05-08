// Tiny helper to spin up a `ws` server on a random port for WsTransport tests.

import type { AddressInfo } from 'node:net';

import { WebSocketServer, type WebSocket } from 'ws';

export interface TestWsServer {
  port: number;
  server: WebSocketServer;
  /** Resolves with the first connected socket. */
  firstConnection(): Promise<WebSocket>;
  close(): Promise<void>;
}

export async function startTestWsServer(): Promise<TestWsServer> {
  const server = new WebSocketServer({ port: 0 });
  await new Promise<void>((resolve, reject) => {
    server.once('listening', () => resolve());
    server.once('error', reject);
  });
  const addr = server.address() as AddressInfo;
  const port = addr.port;

  let resolveFirst!: (sock: WebSocket) => void;
  const first = new Promise<WebSocket>((r) => {
    resolveFirst = r;
  });
  server.once('connection', (sock) => resolveFirst(sock));

  return {
    port,
    server,
    firstConnection: () => first,
    close() {
      return new Promise<void>((r) => server.close(() => r()));
    },
  };
}
