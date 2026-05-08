# multi-electrum-client — design

## Context

A new TypeScript library for talking to Bitcoin Electrum servers, with the headline feature that one library instance manages multiple server connections and transparently routes each request to the best-available server. Existing JS Electrum clients are single-server, weakly typed, and not friendly to React Native; this project replaces them with a single, well-typed, multi-platform package whose value proposition is resilience: ban-aware routing, partial-batch redirect, subscription restore on reconnect, lifecycle-aware behavior for mobile.

Target environments: Node, React Native (callstackincubator/react-native-harness for tests; react-native-tcp-socket for TCP/TLS), Bun, browser (WebSocket only).

## Goals

- Build the low-level Electrum client from scratch (no wrapper around an existing JS lib).
- Multi-server orchestration: the caller writes one call site; the library picks the server.
- Strategy-based routing API; no rule DSL. Ship a small set of bullet-proof built-ins.
- Auto-retry of failed sub-requests in a batch on a different server (per-item granularity).
- Subscription replay + catch-up diff on reconnect (no missed events between handlers).
- Pluggable error classifier with sensible per-server-software defaults.
- Hand-written TypeScript domain types (friendly discriminated unions: `ConfirmedTx | UnconfirmedTx`, etc.). No runtime schema lib in the published bundle.
- Caller-injected cache via a 3-method `CacheStore` interface; library caches only data immutable past finality.
- RN-friendly lifecycle: `suspend()` / `resume()` and `bindAppState()` helper.
- Single npm package with conditional exports per platform.
- Strong test story: unit (Vitest), integration against a multi-server Docker compose stack with toxiproxy fault injection, RN parity via react-native-harness, type-level tests via `tsd`, snapshot tests for cross-impl response shapes.
- Optional peer discovery via `server.peers.subscribe`: when enabled, manager harvests peers from connected servers and grows its pool automatically; an `onDiscover` callback lets callers persist the peer list and veto entries.

## Non-Goals (MVP)

- Lightning, Liquid, or any non-Electrum protocols.
- Wallet logic (key derivation, signing, coin selection). Library is a transport-and-routing layer for read/broadcast operations.
- Cache eviction policy. That's the user's `CacheStore` problem.
- Hedging (parallel send to N clients, take first response) — post-MVP backlog.
- Reorg-driven cache invalidation — by construction we only cache past finality (`finalizedConfs` configurable, default 6).
- Deep observability tooling beyond an `error` event and per-client state events.

## MVP scope (vertical slice across platforms)

- Transport: WebSocket only.
- Methods: `server.version`, `server.ping`, `blockchain.scripthash.{get_balance, get_history, listunspent, subscribe, unsubscribe}`, `blockchain.transaction.{get, broadcast, get_merkle}`, `blockchain.headers.subscribe`, `blockchain.estimatefee`.
- Manager + 4 built-in policies: `roundRobin`, `failover`, `preferFastest`, `withSticky` wrapper.
- Microtask auto-batch coalescing, on by default.
- Auto-redirect partial-batch failures.
- Subscription replay + catch-up diff on reconnect.
- In-memory `MemoryCache` adapter; immutable-only caching.
- `suspend()` / `resume()` + RN `bindAppState` helper.
- Pluggable error classifier with built-in heuristics for ElectrumX, Fulcrum, electrs.
- Hand-written domain types; type-level tests; cross-impl snapshot tests.
- Docker compose harness: bitcoind regtest + ElectrumX × N configs + Fulcrum × N configs + electrs × N configs + toxiproxy.

Post-MVP: TCP + TLS transports (single file each, RN via metro alias to `react-native-tcp-socket`); Bun-specific transport optimizations; full Electrum method coverage; hedging; deeper observability.

## Architecture

### Layers

**Manager** (`src/manager.ts`). One instance per Bitcoin network (mainnet / testnet / regtest / signet). Owns the `Client` pool, the active `RoutingPolicy`, the injected `CacheStore`, the `ErrorClassifier`, the `SubscriptionRegistry`, and the `LifecycleController`. Public surface: `start`, `stop`, `call`, `batch`, namespace methods (`scripthash.*`, `transaction.*`, `headers.*`, `server.*`, `estimateFee`), `subscribe`/`unsubscribe`, `addServer`, `removeServer`, `suspend`, `resume`, `on`, `bindAppState`. All requests go through the manager.

**Client** (`src/client.ts`). Owns one connection to one server. Responsible for: opening / closing the socket via its `Transport`, JSON-RPC framing (newline-delimited for TCP/TLS, message-framed for WebSocket), request id allocation, in-flight request map, response routing back to caller promises, ping / keepalive, raw-error surfacing to the manager, per-client telemetry (latency EMA + p50 / p95, in-flight count, error counters). The client never decides routing.

**Transport** (`src/transport/{ws,tcp,tls}.ts`). Three single-file implementations behind one tiny interface (`connect / send(bytes) / on('data'|'close'|'error') / close`). `ws.ts` uses global `WebSocket` (works in Node 22+, browser, RN, Bun). `tcp.ts` imports `net`. `tls.ts` imports `tls`. RN users add a metro alias mapping `net` and `tls` to `react-native-tcp-socket` (its API is a 1:1 emulation of Node's modules) — documented in README, no platform branches inside the lib.

**RoutingPolicy** (`src/policy/{types,builtins}.ts`). Plain object with `pick(ctx)` and optional `onOutcome(o)`. Manager calls `pick` for every request and every retry, passing live `ClientView` snapshots and the `excluded` set of clients already tried for that logical request. Built-ins: `roundRobin()`, `failover(orderHint?)`, `preferFastest({ withinPct, tiebreak })`, `withSticky(inner, key)`. No `compose()` DSL — custom policies are plain functions.

**CacheStore** (`src/cache/{types,memory,keys}.ts`). Three async methods (`get / set / del`), caller-provided. `MemoryCache` is the only adapter shipped. Manager owns key namespacing (`et:<network>:v1:<bucket>:<id>`) and JSON serialization. Cache is consulted only for methods on a static allow-list (confirmed transactions ≥ `finalizedConfs`, finalized headers, finalized merkle proofs).

**ErrorClassifier** (`src/errors/{types,classifier}.ts`). Single `classify(err, ctx)` method. Default ships heuristics keyed on detected `serverSoftware`: ElectrumX `"excessive resource"` → `rate-limit`; Fulcrum `"excessive resource usage; bye!"` → `rate-limit`; ECONNRESET / ETIMEDOUT / WS 1006/1011 → `transport`; etc. Output drives manager's ban / retry / surface decisions and is forwarded to `RoutingPolicy.onOutcome`.

**SubscriptionRegistry** (`src/subscriptions/registry.ts`). Internal. Records `(method, params, handlers, clientId, lastKnownStatus, generation, stickyKey)` per active subscription. Multi-handler dedup at this layer (one wire subscription per `(method, params)`). On disconnect or rebind, the registry asks the policy for a new client per orphaned subscription, re-issues `subscribe`, compares the returned status to `lastKnownStatus`, fires synthetic notifications on drift.

**LifecycleController** (`src/lifecycle/{controller,rn-appstate}.ts`). Internal state machine: `created → running → suspending → suspended → resuming → running → stopped`. `suspend({ graceMs = 2000, cancelInFlight = false })` waits up to grace for in-flight to settle, then closes sockets; subscription registry preserved. `resume()` reconnects per current policy, replays subs with catch-up. Both methods awaitable and idempotent. `bindAppState(AppState, opts?)` is a small helper that lives in `rn-appstate.ts` and accepts `AppState` as a parameter (the lib does not import `react-native`).

### Data flow

- **Single call.** Caller invokes `manager.call(method, params, opts?)`. Manager checks the cache allow-list; on hit returns. Otherwise: `PickContext` → `policy.pick` → encode → client.send → await response. Success: cache if eligible, `policy.onOutcome`, return. Error: classifier → update client state (`bannedUntil` for `rate-limit`, etc.) → `policy.onOutcome` → if `opts.retry` permits and unexcluded clients exist, re-pick and retry.
- **Batch.** Manager splits into per-item logical requests, runs each through the per-call pipeline, aggregates in original order, returns one `Promise<Result[]>`. Where the policy picks the same client for many items, the client packs them into a real JSON-RPC array request to save round-trips; where it picks different clients, items go in parallel.
- **Subscribe.** Manager calls policy with `stickyKey` set (e.g., scripthash). Registry binds the subscription to the chosen client. Notifications arrive via the client's transport, the client emits to the manager, the manager looks up `(clientId, method, params)` and invokes the handlers.
- **Auto-batch coalescing.** Calls awaited within the same microtask are gathered, grouped by `policy.pick` result, and each group goes out as a single JSON-RPC batch. Per-call opt-out: `{ autoBatch: false }`. Per-manager opt-out via constructor option. Default window: one microtask (zero added latency).

### Key types

```ts
interface RoutingPolicy {
  pick(ctx: PickContext): ClientId | null;
  onOutcome?(o: Outcome): void;
}

interface PickContext {
  request: { method: string; params: unknown[] };
  attempt: number;
  excluded: ReadonlySet<ClientId>;
  candidates: readonly ClientView[];
  stickyKey?: string;
  now: number;
}

interface ClientView {
  id: ClientId;
  endpoint: { host: string; port: number; protocol: 'ws'|'tcp'|'tls' };
  state: 'connecting'|'connected'|'disconnected'|'banned';
  bannedUntil?: number;
  capabilities: { serverSoftware?: string; protocolVersion?: string };
  telemetry: {
    latency: { ema: number; p50: number; p95: number; samples: number };
    errors:  { rate: number; lastKind?: ErrorKind; lastAt?: number; consecutive: number };
    success: { count: number; lastAt?: number };
    inFlight: number;
    connectedSince?: number;
  };
}

type ErrorKind = 'rate-limit'|'timeout'|'transport'|'protocol'|'rpc-error'|'unknown';

interface CacheStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlMs?: number): Promise<void>;
  del(key: string): Promise<void>;
}

interface ErrorClassifier {
  classify(err: unknown, ctx: { serverSoftware?: string; method: string; durationMs: number }): ErrorKind;
}

interface CallOpts {
  signal?: AbortSignal;
  timeoutMs?: number;
  autoBatch?: boolean;
  retry?: 'auto' | 'none' | { maxAttempts: number };
  preferClient?: ClientId;
  bypassCache?: boolean;
  failOnSuspend?: boolean;
}

type Tx = ConfirmedTx | UnconfirmedTx;
interface ConfirmedTx   { status: 'confirmed';   txid: string; height: number; blockHash?: string; fee?: bigint; }
interface UnconfirmedTx { status: 'unconfirmed'; txid: string; height: 0 | -1; fee?: bigint; }
```

### Public method surface

The shape evolves across milestones:

**M3 (shipped) — one-shot wrappers.** Each namespace method is a thin `call`
wrapper. `subscribe` calls return the *initial* status / tip; M3 has no
handler routing, so server-pushed notifications are silently dropped.

```ts
manager.scripthash.getBalance(hash)              // -> blockchain.scripthash.get_balance
manager.scripthash.getHistory(hash)
manager.scripthash.listUnspent(hash)
manager.scripthash.subscribe(hash)                // returns Promise<ScripthashStatus>; M4 reshapes
manager.scripthash.unsubscribe(hash)
manager.transaction.get(txid)                     // raw hex
manager.transaction.getVerbose(txid)              // TxVerbose
manager.transaction.broadcast(rawHex)
manager.transaction.getMerkle(txid, height)
manager.headers.getTip()                          // blockchain.headers.subscribe; one-shot
manager.headers.getHeader(height)
manager.estimateFee(blocks)
manager.server.version(clientName, protocolVersion)
manager.server.ping()
manager.server.banner()

manager.call(method, params?, opts?)              // params optional for empty-tuple methods
manager.batch(requests, opts?)
```

**M4 (planned) — handler-based subscriptions.** `headers.getTip()` becomes a
shorthand; the handler-registering form lives at:

```ts
manager.headers.subscribe(handler)                // -> () => unsub
manager.scripthash.subscribe(hash, handler)       // -> () => unsub; status pushed via handler
```

### Constructor

```ts
new ElectrumManager({
  network: 'mainnet' | 'testnet' | 'regtest' | 'signet',
  servers: [{ id, host, port, protocol: 'ws'|'tcp'|'tls' }, ...],
  policy: RoutingPolicy,
  cache: CacheStore,                             // default: new MemoryCache()
  classifier?: ErrorClassifier,                  // default: defaultClassifier
  autoBatch?: boolean,                           // default true
  requestTimeoutMs?: number,                     // default 10_000
  reconnectBackoff?: { minMs, maxMs, factor, jitter },
  cooldownMs?: number,                           // default 60_000
  finalizedConfs?: number,                       // default 6
  discover?: {
    enabled: boolean,                            // default false
    onDiscover?: (peer: ServerSpec) => boolean | Promise<boolean>,
    intervalMs?: number,                         // re-poll interval, default 6h
  },
});
```

### Events

```ts
manager.on('client-state', ({ clientId, state }) => {});
manager.on('client-banned', ({ clientId, until, reason }) => {});
manager.on('subscription-restored', ({ params, drift }) => {});
manager.on('error', (err) => {});
```

### Errors

```ts
class SuspendedError extends Error {}
class TimeoutError   extends Error {}
class TransportError extends Error { cause: unknown; }
class RpcError       extends Error { code: number; data?: unknown; }
class ProtocolError  extends Error {}
```

### Subscription details

- Multiple callers subscribing to the same `(method, params)` share one wire subscription; manager fans out to all handlers.
- Last `unsub` triggers `blockchain.scripthash.unsubscribe` on servers that support it (ElectrumX ≥ 1.4.2, Fulcrum, electrs); on older servers manager stops dispatching but cannot tell server to stop pushing — documented.
- Idempotency contract: handlers may receive the same status more than once across rebinds; document that handlers should be idempotent. We do not deduplicate beyond "skip notification if new status equals stored last-known".

### Server discovery

Optional, off by default. When `discover.enabled === true` the manager calls `server.peers.subscribe` against each connected server (one-shot on connect, plus a periodic re-poll every `intervalMs`, default 6h). The response is parsed into a list of `ServerSpec` candidates.

For each candidate not already in the pool:

1. Manager calls `discover.onDiscover(peer)` if provided. The callback returns `boolean | Promise<boolean>`. `true` → admit; `false` → skip silently. A thrown / rejected callback also skips, but the manager emits the thrown value on the `error` event so a buggy `onDiscover` is observable rather than silently dropping every candidate. Callback can also persist the peer to the user's storage (e.g., write to AsyncStorage) before returning.
2. If admitted, manager `addServer(peer)`. The new client connects on the next event loop turn and joins the routing pool.

The default policy when `onDiscover` is omitted is "admit everything". Users that want a curated list pass an `onDiscover` that returns `false` for entries that don't match their allow-rules.

Notes:
- Only `ws` and `wss` peers are admitted by default; TCP/TLS peers are skipped until M6 wires those transports. The `onDiscover` callback can override.
- Manager never removes discovered servers automatically. Caller drives `removeServer(id)` based on telemetry / their own logic.
- `server.peers.subscribe` returns a tuple shape (`[host, ip, features[]]`); the manager converts it to a `ServerSpec`. Servers that don't support peer discovery (e.g., some `electrs` configs) emit a JSON-RPC error which the manager swallows silently — discovery is best-effort.

### Cache rules

| Bucket | Key | What's cached | Write rule |
|---|---|---|---|
| `tx`  | `et:<n>:v1:tx:<txid>`              | confirmed tx, raw + decoded | only if `height ≤ tipHeight − finalizedConfs` at write time |
| `hdr` | `et:<n>:v1:hdr:<heightHex>`        | block header                | only if finalized |
| `mrk` | `et:<n>:v1:mrk:<txid>:<heightHex>` | merkle proof                | only if finalized |

Tip is tracked via the headers subscription. No TTL on cached entries (immutable). No reorg-invalidation logic in MVP — by construction only finalized data is stored. Mempool, scripthash status/history/unspent, fee estimates, banner, version, ping, headers below finality, and transactions below finality are never cached.

### Lifecycle details

- `suspend({ graceMs = 2000, cancelInFlight = false })`: wait grace for in-flight, then `SuspendedError`-reject any leftover; close all sockets; preserve subscription registry.
- `resume()`: reopen clients per current policy; replay every subscription with catch-up; emit `subscription-restored` events.
- Calls during `suspended` queue (not reject), and their `requestTimeoutMs` is measured from `resume()` start. Per-call opt-out: `{ failOnSuspend: true }`.
- `bindAppState(AppState, { suspendOn=['background','inactive'], resumeOn=['active'] })` returns a disposer.

## File / module layout

```
src/
  index.ts
  manager.ts
  client.ts
  policy/{types,builtins}.ts
  transport/{types,ws,tcp,tls}.ts
  cache/{types,memory,keys}.ts
  errors/{types,classifier}.ts
  protocol/{methods,types,framing}.ts
  subscriptions/registry.ts
  lifecycle/{controller,rn-appstate}.ts
  util/{ema,deferred,microtask-batcher}.ts
test/
  helpers/{regtest,toxic,serverPool,wait,randomTx}.ts
  unit/policy/{preferFastest,withSticky}.test.ts
  unit/classifier/{electrumx,fulcrum,electrs}.test.ts
  unit/batch/splitter.test.ts
  unit/subscriptions/registry.test.ts
  unit/framing.test.ts
  integration/{failover,partial-batch-retry,subscription-catchup,ban-detection,cross-impl-parity}.test.ts
  rn/{appstate,tcp-shim,parity}.test.ts
  types/{methods,policy}.test-d.ts
docker/
  compose.yml
  electrumx/{Dockerfile,configs/}
  fulcrum/{Dockerfile,configs/}
  electrs/{Dockerfile,configs/}
  toxiproxy/config.json
.github/workflows/{unit,integration,rn,types,lint}.yml
```

## Test infrastructure

### Three layers, shared logic

1. **Unit (Node, Vitest).** Pure-logic: policies, classifier, batch splitter, subscription registry, framing, EMA, key namespacing. Transport, network, time mocked.
2. **Integration (Node, Vitest, against Docker compose).** Real protocol traffic against real servers. Cross-impl parity, failover under injected faults, partial-batch retry, subscription catch-up, ban detection.
3. **RN parity (react-native-harness).** Same Vitest suite for platform-agnostic logic also runs in an RN process. Plus RN-specific tests: `bindAppState`, `suspend/resume` under simulated background, `react-native-tcp-socket` as `net`-shim.

### Docker compose — config matrix per server type

```
services:
  bitcoind:                  # btcpayserver/bitcoin:26.0, regtest

  electrumx-default:         # stock limits
  electrumx-strict:          # low MAX_SEND, MAX_SUBS, MAX_SESSION_SUBS
  electrumx-slow:            # behind toxiproxy

  fulcrum-default:           # cculianu/fulcrum:<pinned>
  fulcrum-strict:            # tight workqueue, max_clients_per_ip=1
  fulcrum-slow:              # behind toxiproxy

  electrs-default:           # getumbrel/electrs:v0.10.2
  electrs-strict:            # tight rpc_buffer / mempool_limit
  electrs-slow:              # behind toxiproxy

  toxiproxy:                 # ghcr.io/shopify/toxiproxy:2.9.0
```

Each server reachable via two ports: direct (`:5xxxx`) and toxiproxy lane (`:6xxxx`). Test helper `Toxic` flips latency / drop / bandwidth at runtime via toxiproxy HTTP API.

CI gate runs a minimal cross-product (one of each); full matrix runs nightly or on demand. Compose layout is our own (well-structured, profiles for slim vs full); bitkit's compose used as reference for image picks only.

### Type-level tests

`tsd` / `expectTypeOf` next to the protocol module; run as a separate Vitest suite.

```ts
expectType<Promise<{ confirmed: bigint; unconfirmed: bigint }>>(
  m.scripthash.getBalance('aabbcc...'),
);
// @ts-expect-error -- height is required
m.transaction.getMerkle('txid');
```

### Runtime shape pinning — no zod

Vitest snapshot tests against real server responses, recorded once per server type via integration suite. If a server's response shape changes, the snapshot diff is loud. Type registry stays in sync. No runtime schema dependency in published bundle.

### Property tests

`fast-check` for: routing policy invariants, batch splitter, framing parser.

### CI

`.github/workflows/`: `unit.yml`, `integration.yml`, `rn.yml`, `types.yml`, `lint.yml`. PRs require unit + types + lint green; integration + rn run in parallel and required before merge.

## Milestones

- **M0 — Skeleton (~1 wk).** Repo, tsconfig (strict), ESLint, Prettier, Vitest, tsup, GH Actions skeleton, exports map for Node/RN/browser/Bun, public types stubbed.
- **M1 — Single-client WS happy path (~2 wks).** `WsTransport`, `ElectrumClient`, framing, `server.version` + `server.ping` against ElectrumX in compose. Unit on framing + client. Integration ping test.
- **M2 — Manager + routing + auto-batch (~2 wks).** `ElectrumManager`, `RoutingPolicy` + 4 built-ins, microtask coalescing, batch splitter with auto-redirect. Unit on policies + splitter. Integration on failover under toxiproxy.
- **M3 — Method coverage + types (~2 wks).** Full MVP method set. Typed method registry (single source of truth for `Manager.call(...)` overloads and the namespace API). Domain types. Cross-impl parity snapshot tests slip to M4.
- **M4 — Subscriptions + classifier + discovery + cache (~3 wks).** `SubscriptionRegistry` with restore + catch-up diff (handler-based `manager.headers.subscribe(handler)` and `manager.scripthash.subscribe(hash, handler)` reshape from M3 one-shots). Pluggable `ErrorClassifier`. Optional peer discovery (`server.peers.subscribe`) with `onDiscover` callback. `CacheStore` + `MemoryCache`, finality-gated writes (now possible because the headers subscription tracks tip). Integration: catch-up under disconnect, ban detection on strict configs.
- **M5 — Lifecycle + RN parity (~2 wks).** `suspend` / `resume`, queue semantics, `bindAppState`. `react-native-harness` in CI. Same subset green in Node and RN. RN-specific tests.
- **M6 — TCP + TLS transports (~1.5 wks).** `tcp.ts` + `tls.ts`. Confirm RN works via metro alias to `react-native-tcp-socket`. Integration matrix expands to all three transports.
- **M7 — Polish + 0.1 release (~1 wk).** README, examples (Node, RN, browser, Bun), API docs, npm + JSR publish, semver baseline.

Total ≈ 11.5 wks of work; realistic solo calendar 14–16 wks.

## Verification (end-to-end)

1. **Unit:** `pnpm test` — all green.
2. **Type:** `pnpm typecheck` (alias `pnpm test:types`) — no errors. `test/types/**` is type-only; assertions live in unused functions and are validated by `tsc`, not vitest.
3. **Integration boot:** `docker compose -f docker/compose.yml up -d --wait` then `pnpm test:integration`.
4. **RN parity:** `pnpm test:rn` (driven by `react-native-harness`).
5. **Manual smoke (per milestone demo):** failover, subscription, suspend/resume scripted scenarios.
6. **Build + publish dry-run:** `pnpm build && npm pack` — package contents match conditional exports; no test files, no `react-native` import shipped to non-RN consumers.

## Research pointers (read before coding)

- Electrum protocol: https://electrum-protocol.readthedocs.io/en/latest/protocol-methods.html ; changes: https://electrum-protocol.readthedocs.io/en/latest/protocol-changes.html (target 1.4 or 1.4.2, document choice).
- ElectrumX source: https://github.com/spesmilo/electrumx — `electrumx/server/session.py` for error strings, limit knobs.
- Fulcrum source: https://github.com/cculianu/Fulcrum — `src/Servers.cpp` for ban messages.
- electrs (romanz): https://github.com/romanz/electrs — `src/electrum.rs` for coverage delta.
- BlueWallet's Electrum module: https://github.com/BlueWallet/BlueWallet/blob/master/blue_modules/BlueElectrum.ts — production RN consumer, real failover patterns.
- `@keep-network/electrum-client-js`: existing ESM TS client, partial types — read for type modeling.
- `react-native-tcp-socket`: https://github.com/Rapsssito/react-native-tcp-socket — Node `net`/`tls` API parity, metro alias docs.
- `react-native-harness`: https://github.com/callstackincubator/react-native-harness — sharing Vitest config across Node and RN runs.
- BTCPay regtest: https://github.com/btcpayserver/btcpayserver-docker — bitcoind regtest patterns.
- Bitkit compose: https://github.com/synonymdev/bitkit-react-native/blob/master/docker/docker-compose.yml — image picks: `btcpayserver/bitcoin:26.0`, `getumbrel/electrs:v0.10.2`.
- toxiproxy: https://github.com/Shopify/toxiproxy — HTTP API, supported toxics.
- `tsd`: https://github.com/tsdjs/tsd — type-level assertions.
- `fast-check`: https://github.com/dubzzz/fast-check — property tests for policies.
