# Changelog

All notable changes to `multi-electrum-client` are documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Pre-release. The integration suite against the Docker compose stack now covers cross-impl parity, failover under toxiproxy, partial-batch retry, subscription catch-up, auto-reconnect, and ban detection on a strict-config Fulcrum. No version is cut until RN parity lands.

### Added

- **Manager** (`ElectrumManager`) — single entry point for a pool of servers; transparent failover, partial-batch retry, microtask auto-batch coalescing, per-client telemetry.
- **Routing policies** — `roundRobin`, `failover`, `preferFastest`, `withSticky`. `RoutingPolicy` is a plain `(ctx) => ClientId | null` interface; custom policies are plain functions.
- **Typed method registry** — single source of truth for `manager.call(method, params)` overload resolution and the namespace API (`manager.scripthash.*`, `manager.transaction.*`, `manager.headers.*`, `manager.server.*`, `manager.estimateFee`).
- **Subscriptions** — handler-based `manager.scripthash.subscribe(hash, handler)` and `manager.headers.subscribe(handler)`; multi-handler dedup, disconnect-orphan / reconnect-replay with catch-up diff, fire-and-forget last-handler unsubscribe, `subscription-restored` event.
- **Cache** — caller-injected `CacheStore` with finality-gated writes for `blockchain.block.header` and `blockchain.transaction.get_merkle`. `MemoryCache` adapter ships with the package.
- **Peer discovery** — optional `discover: { enabled, onDiscover?, intervalMs? }` calls `server.peers.subscribe` on every connect; admitted ws/wss peers join the pool via `addServer`.
- **Error classifier** — pluggable; default heuristics for ElectrumX (`"excessive resource usage"`, code -101), Fulcrum (`"Subscription limit reached"`, `RPC::Code_App_LimitExceeded`), and electrs ban / rate-limit strings, plus ECONNRESET / WS abnormal-closure mapping. `composeClassifier` for caller overrides.
- **Lifecycle** — `manager.suspend({ graceMs?, cancelInFlight? })` / `resume()`; FIFO transition chain handles 3+ overlapping calls correctly. Suspended calls queue and replay (or reject with `failOnSuspend: true`). `bindAppState(manager, AppState)` wires RN's AppState to lifecycle without importing react-native.
- **Auto-reconnect** — transport-level disconnects schedule an exponential-backoff reconnect (`reconnectBackoff: { minMs, maxMs, factor, jitter }`, default `500ms→30s`, ×2, ±20%). Successful connects reset the attempt counter; `removeServer` / `stop` / `suspend` cancel pending timers; suspend / resume keeps the lifecycle path in charge of reconnect.
- **Transports** — `WsTransport` (universal: Node 22+, browser, RN, Bun), `TcpTransport` (`node:net`), `TlsTransport` (`node:tls`, awaits `secureConnect`). Shared `LineFramer` for all three. Transport registry lets the package ship a separate browser entry that registers only ws/wss.
- **Conditional package exports** — root `.` resolves to `index.browser.js` for browser bundlers (no `node:net` / `node:tls`), `index.js` for Node / RN / Bun.
- **Examples** — `examples/{node,bun,browser,rn}-basic.*`.

### Fixed

- Classifier no longer claims `"excessive resource usage; bye!"` is a Fulcrum string — that wording is from aiorpcx (ElectrumX's RPC framework) and never appears in Fulcrum payloads. Fulcrum's actual rate-limit message (`"Subscription limit reached"` from `impl_generic_subscribe`) is now matched explicitly. The generic substring set picks up the same string when `serverSoftware` is unknown, so ban detection works without an explicit `server.version` handshake.

### Pending before 0.1.0

- RN parity tests. The original plan called for `react-native-harness`, but on inspection the package requires a full RN host app + simulator/emulator (it's designed for testing TurboModules in real native environments, not for running a JS-only library's test suite under Hermes/JSC). It's the wrong tool for our shape: a pure-JS library with no native modules. We'll evaluate alternatives (running the suite under Hermes directly via `react-native/jest-preset`'s standalone JS-engine path, or just shipping a thin smoke test inside an example app).
- CI workflow for `rn` once the strategy above is settled.

