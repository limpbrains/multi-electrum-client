# multi-electrum-client

Resilient multi-server Electrum (Bitcoin) client for TypeScript — ban-aware
routing, failover, and lifecycle support for Node, React Native, browser,
and Bun. Feature rundown in [Why](#why) below.

> **Status:** pre-release. Unit + integration suites green against the
> Docker Compose stack with toxiproxy fault injection — the full
> coverage list lives in the [CHANGELOG](CHANGELOG.md). Both suites also
> run on-device in a real React Native runtime (iOS simulator and
> Android emulator CI jobs). No `0.1.0` tag yet. Full design at
> [`docs/specs/2026-05-08-multi-electrum-client-design.md`](docs/specs/2026-05-08-multi-electrum-client-design.md).

## Why

Existing JS Electrum clients are single-server, weakly typed, and not friendly
to React Native. This library is a single, well-typed, multi-platform package
whose value proposition is **resilience**, not raw speed:

- One library instance manages multiple server connections; per-request routing.
- Ban / rate-limit detection per server software (ElectrumX, Fulcrum, electrs).
- Partial batch failures auto-redirect to another server, per item.
- Retryable batch failures re-route as one re-batched wire call per fallback
  server — a dead 300-item batch costs one extra round-trip, not 300 singles.
- Opt-in hedged requests (`hedging: { afterMs }`) — a server that accepts a
  request then hangs costs `afterMs` before the same call (or the whole
  coalesced wire batch) races on a second server; first answer wins. Only an
  allowlist of known-idempotent reads hedges by default (never `broadcast`);
  vendor methods opt in per call with `hedge: true`.
- Subscriptions replay + catch-up diff on reconnect — handlers don't miss events.
- Auto-reconnect on transport faults with exponential backoff + jitter.
- Aggregate `pool-state` events (`online` / `degraded` / `offline`) — one
  signal to drive an "offline" banner, including the ban-expiry recovery
  no per-client aggregation can see.
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

### Ensure connected (BlueWallet-style guard)

Before a call that must not fail silently (a broadcast, a fee bump),
wait until the pool is demonstrably live:

```ts
await manager.ensureConnected(); // resolves when a server is usable & answering
await manager.transaction.broadcast(rawTxHex);
```

The manager's reconnect loop does the actual recovery — `ensureConnected`
waits for it (default budget 30 s) and, per the probe policy, verifies
liveness with a wire `server.ping` through the normal routing pipeline.
That wire probe is what catches half-open sockets (established but
silently dead — the classic mobile-NAT shape after backgrounding) that
`pool-state` alone cannot see. `probe: 'auto'` (default) only pings when
no usable server has answered anything in the last 10 s, so it's free
while traffic is flowing.

Typed throws: `SuspendedError` (stopped / not started / suspended —
pass `resumeIfSuspended: true` to resume instead; also raised when a
`stop()`/`suspend()` lands mid-wait), `NoClientAvailableError` (budget
exhausted while offline or mid-resume/probe), or the last real ping
failure. The budget and `signal` cover the whole call — including the
`resumeIfSuspended` resume and the probe. Each concurrent caller keeps
its own budget and signal; only the liveness ping itself is shared.

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
telemetry), `now`, an optional `stickyKey`, and `probe` (`true` for hedge
picks — stateful policies should keep those side-effect-free: don't advance
cursors or move pins on a probe). Return `null` to bail with
`NoClientAvailableError`.

### Manager events

```ts
manager.on('client-state', ({ clientId, state }) => {
  // 'connecting' | 'connected' | 'disconnected' | 'banned'
});
manager.on('client-banned', ({ clientId, until, reason }) => { /* observability */ });
manager.on('subscription-restored', ({ method, params, drift }) => { /* observability */ });
manager.on('error', (err) => { /* recoverable transport / classifier failures */ });

// Aggregate connectivity — drive an "offline" banner from one event:
manager.on('pool-state', ({ status, usable, total }) => {
  // status: 'online' (all usable) | 'degraded' (some) | 'offline' (none)
  banner.hidden = status !== 'offline';
});
console.log(manager.poolState); // same snapshot on demand
```

The `error` event is observability-only — promises still reject. Use it for
metrics and debug logging, not control flow.

`pool-state` fires on aggregate *status* changes only (including a ban
expiring — the manager arms an internal timer, so offline→online lands
without any traffic). Exactly one baseline event fires after `await
start()` — even when every initial connect failed — and one after
`resume()`. While suspending / suspended the event is intentionally
silent: a deliberate pause is not an outage; read `manager.state` for
lifecycle.

## Security notes

- **Peer discovery is off by default and should stay curated.** With
  `discover: { enabled: true }` and no `onDiscover` callback, every peer a
  connected server advertises is admitted to the pool — a malicious server
  can route your wallet's queries to attacker-controlled infrastructure.
  In production always supply `onDiscover` with an explicit allowlist /
  validation, or keep discovery disabled.
- **Broadcast retries touch more than one server.** A transient failure
  while broadcasting retries the same raw transaction on another pooled
  server. Re-broadcasting is idempotent on-chain, but it discloses the
  transaction to an additional operator; pass `retry: 'none'` on the
  broadcast call if single-server semantics matter to you. (Broadcast is
  always hard-excluded from hedging.)
- **Hedged requests trade privacy for latency.** Hedging is opt-in; when
  enabled, an idempotent read (including scripthash queries) may be sent
  to a second server. Enable it only against servers you are equally
  willing to show your wallet's addresses to.
- **Never disable TLS verification in production.** `TlsTransport`'s
  `tlsOptions` accepts `rejectUnauthorized: false` for local test rigs
  only; against a real server that removes all transport security. Pin a
  CA via `tlsOptions.ca` instead.
- **Trust model.** Like every non-SPV Electrum client, this library
  trusts the serving peer's view of the chain: block headers, merkle
  proofs and the tip height used to gate finalized cache writes all
  come from whichever pooled server answered, and a malicious SERVING
  peer can misrepresent them. Multi-server pools, routing policies and
  the finality confirmation depth (`finalizedConfs`) reduce exposure;
  local header-chain validation (SPV) is out of scope. Subscription
  status payloads are change signals — resync on every callback rather
  than persisting the raw value (see the `subscribe` docs).
- **Line-length cap.** Inbound frames are bounded (32 MiB per line by
  default — sized so the protocol's largest valid responses, including
  verbose transactions, still fit) so a malicious or broken server
  cannot grow the framing buffer without limit; on overflow the
  transport emits one `error` plus one `close` event and disconnects.
  Size it per server with `ServerSpec.maxLineLength`, or pool-wide
  (including discovery-admitted peers) with the manager's
  `maxLineLength` option; `maxLineLength` on a transport's own options
  acts as a fleet default under any per-server value. On WebSocket the
  platform materializes each complete message before delivery, so the
  cap bounds what the framer retains and fails an oversized message
  loudly before decoding it — for a pre-delivery receive bound,
  configure the injected WebSocket implementation itself (e.g.
  `maxPayload` in the `ws` package).
  An invalid `maxLineLength` (NaN, Infinity, zero, negative, fractional)
  is rejected with `RangeError` rather than silently disabling the cap.

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

## WebSocket framing

Electrum-over-WebSocket has two incompatible wire shapes, and traffic
cannot tell them apart — a byte tunnel's fragment is byte-identical to a
native server's complete message — so the shape is **declared** per
server, never guessed:

- `wsFraming: 'message'` (**default**) — the native protocol: one
  complete JSON-RPC payload per WebSocket message, no trailing newline
  (Fulcrum's `ws=` mode, public WS gateways). The default is the
  protocol's own definition; this library has never been released with
  any other, so there is no deployed base the default could break.
- `wsFraming: 'newline'` — a ws↔tcp byte tunnel (websockify-style
  bridge) relaying a newline-delimited TCP stream with arbitrary
  message boundaries. If you built the bridge, declare it. In this mode
  one message may legally coalesce several complete lines; the
  aggregate message bound defaults to four full-cap lines (floored at
  8 MiB — ~128 MiB at the default line cap) and is tunable per server
  via `maxMessageLength` — lower it on memory-sensitive deployments,
  raise it for bridges that coalesce more than four near-cap responses.

## Platform notes

- **Node** ≥ 20: works out of the box. Global `WebSocket` is stable in Node 22+; Node 20 needs `--experimental-websocket`, or pass `WebSocket` from the `ws` package via `WsTransport`'s `WebSocket` option. TCP / TLS use `node:net` / `node:tls`.
- **Bun**: works out of the box (`ws`, `tcp`, `tls`).
- **Browser**: only `ws` / `wss` are supported. The package's `browser` conditional export points to a separate entry that registers only the WebSocket transport — `node:net` / `node:tls` are never reached by your bundler's resolution graph. Constructing a server with `protocol: 'tcp'` / `'tls'` still throws `ProtocolError` clearly at runtime (rather than at bundle time).
- **React Native**: add a metro alias mapping `node:net` and `node:tls` to [`react-native-tcp-socket`](https://github.com/Rapsssito/react-native-tcp-socket). Its API is a 1:1 emulation of the Node modules; no platform branches inside the library. `WebSocket` is built into the RN runtime. No polyfills are required: Hermes lacks `TextDecoder`/`Buffer` globals, but the library only reaches for them on a defensive path (a socket shim that ignores `setEncoding` and emits raw bytes) — normal operation decodes strings end-to-end. This isn't theoretical: the library's own unit suite runs on-device in CI (iOS simulator + Android emulator, via [react-native-harness](https://github.com/callstackincubator/react-native-harness)) with exactly this alias — see `test/rn/app/metro.config.js`. (The globals installed by `test/rn/app/harness/setup.ts` are test-harness plumbing for running the *node* suite verbatim, not something your app needs.)

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

The same unit tests — every file of the node suite — run unmodified inside a
real React Native runtime (Hermes) through [react-native-harness](https://github.com/callstackincubator/react-native-harness),
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
