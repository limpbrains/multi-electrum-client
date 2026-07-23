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
- **On-device React Native test runs** — the complete node unit suite (all 26 files, 244 tests) AND the docker-backed integration suite (7 files, 23 tests) run unmodified inside a real React Native runtime (Hermes) on iOS simulators and Android emulators via `react-native-harness`. A Metro alias maps `vitest` to a thin shim (`test/rn/app/harness/vitest-shim.ts`) — harness's `expect` is `@vitest/expect` and its mocks are `@vitest/spy`, so only fake timers (`@sinonjs/fake-timers`) and `describe.each` needed filling. `node:net` / `node:tls` resolve to `react-native-tcp-socket` through the exact alias the README documents for RN consumers, so the tcp/tls transport tests exercise the real native socket module on-device. The two `ws`-server-backed files run against `test/rn/app/harness/ws-shim.ts`, a minimal RFC 6455 WebSocket server implemented on react-native-tcp-socket — the ws server runs on the device itself. The integration suite reaches the host's docker stack from the device (iOS simulator via `127.0.0.1`, Android emulator via `10.0.2.2`); toxiproxy/bitcoind admin helpers are fetch-based and work as-is. Scripts: `test:rn`, `test:rn:setup`, `test:rn:ios`, `test:rn:android`, `test:rn:integration:ios`, `test:rn:integration:android`; CI jobs `rn-ios` (unit) / `rn-android` (unit + integration against the compose stack).

### Fixed

- A failed subscription rebind no longer strands the subscription until the next client state transition. Previously, if the wire `subscribe` issued by `restoreOrphans` failed (e.g. timed out on a freshly reconnected but not-yet-usable link), the record stayed orphaned silently — no retry, no event — until another disconnect/connect cycle. `rebindOnce` now retries with backoff (1s doubling, 10s cap) for as long as some client is connected. Found by running the integration suite on-device: the Android emulator's NAT reliably produces an established-but-dead first connection after a toxiproxy re-enable, which is exactly the shape of a mobile-network reconnect.
- Classifier no longer claims `"excessive resource usage; bye!"` is a Fulcrum string — that wording is from aiorpcx (ElectrumX's RPC framework) and never appears in Fulcrum payloads. Fulcrum's actual rate-limit message (`"Subscription limit reached"` from `impl_generic_subscribe`) is now matched explicitly. The generic substring set picks up the same string when `serverSoftware` is unknown, so ban detection works without an explicit `server.version` handshake.

### Pending before 0.1.0

- ~~RN parity tests~~ Landed via `react-native-harness` after all, reversing the earlier abandonment note. The objection stands factually — it does require a full RN host app (`test/rn/app/`) plus a simulator/emulator — but that cost buys the signal a standalone-Hermes run could not: the tcp/tls transports talking to a real native socket module (`react-native-tcp-socket`) under real Hermes timers and microtask scheduling, on both platforms.

