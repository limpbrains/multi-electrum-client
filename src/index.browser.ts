// Browser entry. Re-exports the shared surface (`src/index.core.ts`) —
// everything except `TcpTransport` / `TlsTransport` — and registers only
// `ws` / `wss` in the transport factory (via the core's WsTransport
// import). This keeps `node:net` / `node:tls` out of a browser bundler's
// resolution graph entirely. Browser users can still set `protocol: 'tcp'`
// / `'tls'` in `ServerSpec`, but the default factory will throw a clear
// `ProtocolError('no transport registered for protocol \'tcp\'')` rather
// than failing at module-resolve time.
//
// To use a custom transport in the browser (e.g. tunneling TCP via a
// WebSocket bridge) override `ManagerOptions.transportFactory`.

export * from './index.core.js';
