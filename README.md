# multi-electrum-client

Multi-server Electrum (Bitcoin) client for TypeScript with ban-aware routing,
partial-batch redirect, subscription restore, and lifecycle support for
Node, React Native, browser, and Bun.

> **Status:** pre-release. Unit + integration suites green (Docker
> Compose stack covers cross-impl parity, failover under toxiproxy fault
> injection, partial-batch retry, subscription catch-up, auto-reconnect,
> and ban detection). RN parity tests deferred — see [CHANGELOG](CHANGELOG.md).
> No `0.1.0` tag yet. Full design at
> [`docs/specs/2026-05-08-multi-electrum-client-design.md`](docs/specs/2026-05-08-multi-electrum-client-design.md).

## Why

Existing JS Electrum clients are single-server, weakly typed, and not friendly
to React Native. This library is a single, well-typed, multi-platform package
whose value proposition is **resilience**, not raw speed:

- One library instance manages multiple server connections; per-request routing.
- Ban / rate-limit detection per server software (ElectrumX, Fulcrum, electrs).
- Partial batch failures auto-redirect to another server, per item.
- Subscriptions replay + catch-up diff on reconnect — handlers don't miss events.
- Auto-reconnect on transport faults with exponential backoff + jitter.
- `suspend()` / `resume()` for React Native background lifecycle.

## Quick start

```ts
import { ElectrumManager, preferFastest, withSticky } from 'multi-electrum-client';

const manager = new ElectrumManager({
  network: 'mainnet',
  servers: [
    { id: 'a', host: 'electrum.example.org', port: 50004, protocol: 'wss' },
    { id: 'b', host: 'electrum2.example.org', port: 50004, protocol: 'wss' },
  ],
  policy: withSticky(preferFastest({ withinPct: 20 }), 'scripthash'),
});

await manager.start();

// Typed namespace API — params and result are inferred from the wire name.
const balance = await manager.scripthash.getBalance(scripthash);
//    ^? { confirmed: number; unconfirmed: number }

const txid = await manager.transaction.broadcast(rawTxHex);

// Auto-batch coalescing: same-microtask calls bound for the same server
// go out as a single JSON-RPC array; partial failures auto-retry on a
// different server.
const [bal, hist, tx] = await Promise.all([
  manager.scripthash.getBalance(h1),
  manager.scripthash.getHistory(h2),
  manager.transaction.get(txid),
]);

manager.on('client-banned', ({ clientId, reason }) => {
  // 'rate-limit' detected on `clientId`; manager routes around it.
});

await manager.stop();
```

## Examples

### Subscriptions with replay on reconnect

Handler-based subscriptions; the manager owns the wire `subscribe` and re-binds
across disconnects. If the subscription state drifts during the disconnect
window, the handler fires synthetically with the new status on reconnect — no
events missed.

```ts
const unsub = await manager.scripthash.subscribe(scripthash, (status) => {
  // Initial status fires synchronously; subsequent fires on every server push.
  console.log('status:', status);
});

manager.on('subscription-restored', ({ method, drift }) => {
  if (drift) console.log(`${method} drifted during reconnect; handler fired with new status`);
});

// Last-handler unsubscribe.
await unsub();
```

### Background lifecycle (mobile-friendly)

`suspend()` drains in-flight, closes sockets, and queues new calls. `resume()`
reconnects, replays subscriptions with catch-up, and drains the queue in order.
Pair with `bindAppState` on React Native to wire app foreground / background
events automatically.

```ts
import { AppState } from 'react-native';
import { bindAppState, ElectrumManager } from 'multi-electrum-client';

const manager = new ElectrumManager({ /* ... */ });
await manager.start();

const dispose = bindAppState(manager, AppState);
// app backgrounded → manager.suspend({ graceMs: 2000 })
// app foregrounded → manager.resume()

// On teardown:
dispose();
await manager.stop();
```

### Caching past finality

Caller-injected `CacheStore`; library writes only entries past `finalizedConfs`
(default 6) confirmations. Reads short-circuit the wire call; writes are
fire-and-forget. Built-in `MemoryCache` ships with the package; bring your own
adapter for AsyncStorage / IndexedDB / SQLite.

```ts
import { ElectrumManager, MemoryCache } from 'multi-electrum-client';

const manager = new ElectrumManager({
  network: 'mainnet',
  servers: [/* ... */],
  policy: preferFastest(),
  cache: new MemoryCache(),
  finalizedConfs: 6,
});

// First call hits the wire; second call (after the block has finalized)
// returns from cache without a round-trip.
const header1 = await manager.headers.getHeader(700_000);
const header2 = await manager.headers.getHeader(700_000); // cache hit
```

### Custom routing policy

`RoutingPolicy` is a plain `(ctx) => ClientId | null` interface. Compose your
own by wrapping a built-in:

```ts
import { failover, withSticky, type RoutingPolicy } from 'multi-electrum-client';

// Round-robin across two trusted servers, prefer the first whenever it's up.
const policy: RoutingPolicy = withSticky(failover(['primary', 'secondary']), 'scripthash');
```

`PickContext` carries `request`, `attempt`, `excluded`, `candidates` (with live
telemetry), `now`, and an optional `stickyKey`. Return `null` to bail with
`NoClientAvailableError`.

### Manager events

```ts
manager.on('client-state', ({ clientId, state }) => {
  // 'connecting' | 'connected' | 'disconnected' | 'banned'
});
manager.on('client-banned', ({ clientId, until, reason }) => { /* observability */ });
manager.on('subscription-restored', ({ method, params, drift }) => { /* observability */ });
manager.on('error', (err) => { /* recoverable transport / classifier failures */ });
```

The `error` event is observability-only — promises still reject. Use it for
metrics and debug logging, not control flow.

## Roadmap

| Milestone | Scope | Status |
|---|---|---|
| M0 | Skeleton: tsconfig, ESLint, Prettier, Vitest, tsup, GH Actions | ✅ |
| M1 | Single-client WebSocket transport + JSON-RPC framing + ElectrumClient | ✅ |
| M2 | ElectrumManager + RoutingPolicy built-ins + auto-batch coalescing + per-client telemetry + retry | ✅ |
| M3 | Typed method registry + namespace API (`manager.scripthash.*`, `manager.transaction.*`, …) + domain types | ✅ |
| M4 | Subscriptions registry (replay + catch-up diff) + per-server-software error classifier + cache + peer discovery | ✅ |
| M5 | Lifecycle (`suspend` / `resume`) + `bindAppState` helper | ✅ |
| M6 | TCP + TLS transports | ✅ |
| M7 | Polish + integration suite + property tests | ✅ |
| M8 | 0.1.0 release | in progress |

## Platform notes

- **Node** ≥ 20: works out of the box. Global `WebSocket` is stable in Node 22+; Node 20 needs `--experimental-websocket`, or pass `WebSocket` from the `ws` package via `WsTransport`'s `WebSocket` option. TCP / TLS use `node:net` / `node:tls`.
- **Bun**: works out of the box (`ws`, `tcp`, `tls`).
- **Browser**: only `ws` / `wss` are supported. The package's `browser` conditional export points to a separate entry that registers only the WebSocket transport — `node:net` / `node:tls` are never reached by your bundler's resolution graph. Constructing a server with `protocol: 'tcp'` / `'tls'` still throws `ProtocolError` clearly at runtime (rather than at bundle time).
- **React Native**: add a metro alias mapping `node:net` and `node:tls` to [`react-native-tcp-socket`](https://github.com/Rapsssito/react-native-tcp-socket). Its API is a 1:1 emulation of the Node modules; no platform branches inside the library. `WebSocket` is built into the RN runtime. This isn't theoretical: the library's own unit suite runs on-device in CI (iOS simulator + Android emulator, via [react-native-harness](https://github.com/callstackincubator/react-native-harness)) with exactly this alias — see `test/rn/app/metro.config.js`.

## More examples

Runnable snippets in [`examples/`](examples/):
- [`node-basic.ts`](examples/node-basic.ts) — Node 22+, mixed TLS / TCP transports.
- [`bun-basic.ts`](examples/bun-basic.ts) — Bun, top-level await, single-server failover.
- [`browser-basic.html`](examples/browser-basic.html) — `wss` only, `visibilitychange` → `suspend` / `resume`.
- [`rn-basic.tsx`](examples/rn-basic.tsx) — React Native, `bindAppState`, headers subscription.

### Interactive demo

A small web app ([`examples/demo`](examples/demo)) that connects to all
three local electrum servers (ElectrumX, Fulcrum, electrs) through
ws↔tcp bridges and visualizes live routing: packets flying per policy
decision, per-server telemetry, a rolling latency chart, policy
hot-swapping, lane cutting / latency injection via toxiproxy, and
suspend / resume.

```bash
docker compose -f docker/compose.yml --profile slim --profile demo up -d --wait
pnpm demo            # then open http://localhost:5173
```

## Development

```bash
pnpm install
pnpm test            # unit tests
pnpm typecheck
pnpm lint
pnpm build           # tsup -> dist/ (ESM + .d.ts)
```

Integration tests require Docker:

```bash
docker compose -f docker/compose.yml --profile slim up -d --wait
pnpm test:integration
```

### Running the unit suite on-device (React Native)

The same unit tests — all 26 files — run unmodified inside a real React
Native runtime (Hermes) through [react-native-harness](https://github.com/callstackincubator/react-native-harness),
hosted by the RN app in `test/rn/app/`. A Metro alias maps `vitest` onto the
harness runtime and `node:net` / `node:tls` onto `react-native-tcp-socket`,
so the tcp/tls transport tests talk to the real native socket module. The
`ws`-server-backed tests run against a minimal on-device WebSocket server
(`test/rn/app/harness/ws-shim.ts`) built on the same native socket module.

The docker-backed integration suite runs on-device too: the device reaches
the compose stack on the host (iOS simulator via `127.0.0.1`, Android
emulator via `10.0.2.2`).

Prerequisites: Xcode + an iOS simulator, and/or the Android SDK with an AVD.

```bash
pnpm test:rn:setup   # install app deps + pods (once)
# build & install the host app on your simulator/emulator (once per native change):
pnpm -C test/rn/app ios       # or: pnpm -C test/rn/app android
pnpm test:rn:ios     # run the unit suite on the iOS simulator
pnpm test:rn:android # run the unit suite on the Android emulator

# integration on-device (docker stack must be up, see above):
pnpm test:rn:integration:ios
pnpm test:rn:integration:android
```

Device pins live in `test/rn/app/rn-harness.config.mjs`; override locally
with `HARNESS_IOS_SIM` / `HARNESS_IOS_VERSION` / `HARNESS_ANDROID_AVD`.

## License

MIT — see `LICENSE`.
