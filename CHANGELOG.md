# Changelog

All notable changes to `multi-electrum-client` are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Pre-release. The unit-test surface is complete, but the integration suite against the Docker compose stack (cross-impl parity, failover under toxiproxy, partial-batch retry, subscription catch-up, ban detection on strict configs) is not yet wired up. No version is cut until that lands.

### Added

- **Manager** (`ElectrumManager`) — single entry point for a pool of servers; transparent failover, partial-batch retry, microtask auto-batch coalescing, per-client telemetry.
- **Routing policies** — `roundRobin`, `failover`, `preferFastest`, `withSticky`. `RoutingPolicy` is a plain `(ctx) => ClientId | null` interface; custom policies are plain functions.
- **Typed method registry** — single source of truth for `manager.call(method, params)` overload resolution and the namespace API (`manager.scripthash.*`, `manager.transaction.*`, `manager.headers.*`, `manager.server.*`, `manager.estimateFee`).
- **Subscriptions** — handler-based `manager.scripthash.subscribe(hash, handler)` and `manager.headers.subscribe(handler)`; multi-handler dedup, disconnect-orphan / reconnect-replay with catch-up diff, fire-and-forget last-handler unsubscribe, `subscription-restored` event.
- **Cache** — caller-injected `CacheStore` with finality-gated writes for `blockchain.block.header` and `blockchain.transaction.get_merkle`. `MemoryCache` adapter ships with the package.
- **Peer discovery** — optional `discover: { enabled, onDiscover?, intervalMs? }` calls `server.peers.subscribe` on every connect; admitted ws/wss peers join the pool via `addServer`.
- **Error classifier** — pluggable; default heuristics for ElectrumX, Fulcrum, electrs ban / rate-limit strings + ECONNRESET / WS abnormal-closure mapping. `composeClassifier` for caller overrides.
- **Lifecycle** — `manager.suspend({ graceMs?, cancelInFlight? })` / `resume()`; FIFO transition chain handles 3+ overlapping calls correctly. Suspended calls queue and replay (or reject with `failOnSuspend: true`). `bindAppState(manager, AppState)` wires RN's AppState to lifecycle without importing react-native.
- **Transports** — `WsTransport` (universal: Node 22+, browser, RN, Bun), `TcpTransport` (`node:net`), `TlsTransport` (`node:tls`, awaits `secureConnect`). Shared `LineFramer` for all three. Transport registry lets the package ship a separate browser entry that registers only ws/wss.
- **Conditional package exports** — root `.` resolves to `index.browser.js` for browser bundlers (no `node:net` / `node:tls`), `index.js` for Node / RN / Bun.
- **Examples** — `examples/{node,bun,browser,rn}-basic.*`.

### Pending before 0.1.0

- Docker compose integration suite: cross-impl parity, failover under toxiproxy fault injection, partial-batch retry, subscription catch-up, ban detection on strict configs.
- RN parity tests via `react-native-harness`.
- CI workflows for `integration` and `rn`.

### Test stats (unit only)

233 unit tests covering policies, framing, registry, classifier, cache, discovery, lifecycle, transports.
