# multi-electrum-client

Multi-server Electrum (Bitcoin) client for TypeScript with ban-aware routing,
partial-batch redirect, subscription restore, and lifecycle support for
Node, React Native, browser, and Bun.

> **Status:** pre-release. All MVP features land at the unit level — WS / TCP /
> TLS transports, manager + routing + auto-batch + retry, typed method registry
> + namespace API, subscriptions with replay + catch-up diff, finality-gated
> cache, peer discovery, per-server-software error classifier, lifecycle
> (suspend / resume + bindAppState). The integration suite against the Docker
> compose stack (cross-impl parity, failover under toxiproxy, partial-batch
> retry, subscription catch-up, ban detection) is not yet wired up; no `0.1.0`
> tag until it is. See
> [`docs/specs/2026-05-08-multi-electrum-client-design.md`](docs/specs/2026-05-08-multi-electrum-client-design.md)
> for the full design.

## Why

Existing JS Electrum clients are single-server, weakly typed, and not friendly
to React Native. This library is a single, well-typed, multi-platform package
whose value proposition is **resilience**:

- One library instance manages multiple server connections; routing per request.
- Ban / rate-limit detection per server software (ElectrumX, Fulcrum, electrs).
- Partial batch failures auto-redirect to another server, per item.
- Subscriptions replay + catch-up diff on reconnect — handlers don't miss events.
- `suspend()` / `resume()` for React Native background lifecycle.

## What works today

```ts
import {
  ElectrumManager,
  preferFastest,
  withSticky,
} from 'multi-electrum-client';

const manager = new ElectrumManager({
  network: 'mainnet',
  servers: [
    { id: 'a', host: 'electrum.example.org', port: 50004, protocol: 'wss' },
    { id: 'b', host: 'electrum2.example.org', port: 50004, protocol: 'wss' },
  ],
  policy: withSticky(preferFastest({ withinPct: 20 }), 'scripthash'),
});

await manager.start();

// Typed method registry — params + result inferred from the wire name:
const balance = await manager.scripthash.getBalance(scripthash);
//    ^? { confirmed: number; unconfirmed: number }

const txid = await manager.transaction.broadcast(rawTxHex);

// Auto-batch coalescing: same-microtask calls bound for the same server
// go out as a single JSON-RPC array; partial failures retry on another
// server transparently.
const [a, b, c] = await Promise.all([
  manager.scripthash.getBalance(h1),
  manager.scripthash.getHistory(h2),
  manager.transaction.get(txid),
]);

manager.on('client-banned', ({ clientId, reason }) => {
  // 'rate-limit' detected on `clientId`; manager will route around it.
});
```

## Roadmap

| Milestone | Scope | Status |
|---|---|---|
| M0 | Skeleton: tsconfig, ESLint, Prettier, Vitest, tsup, GH Actions | ✅ |
| M1 | Single-client WebSocket transport + JSON-RPC framing + ElectrumClient | ✅ |
| M2 | ElectrumManager + RoutingPolicy built-ins + auto-batch coalescing + per-client telemetry + retry | ✅ |
| M3 | Typed method registry + namespace API (`manager.scripthash.*`, `manager.transaction.*`, …) + domain types | ✅ |
| M4 | Subscriptions registry (replay + catch-up diff) + per-server-software error classifier + cache + peer discovery (`server.peers.subscribe`) | ✅ |
| M5 | Lifecycle (`suspend` / `resume`) + `bindAppState` helper | ✅ |
| M6 | TCP + TLS transports | ✅ |
| M7 | Polish + integration suite + 0.1.0 release | in progress |

## Platform notes

- **Node** ≥ 20: works out of the box. Global `WebSocket` is stable in Node 22+; Node 20 needs the `--experimental-websocket` flag, or pass `WebSocket` from the `ws` package via `WsTransport`'s `WebSocket` option. TCP / TLS use `node:net` / `node:tls`.
- **Bun**: works out of the box (`ws`, `tcp`, `tls`).
- **Browser**: only `ws` / `wss` are supported. The package's `browser` conditional export points to a separate entry that registers only the WebSocket transport — `node:net` / `node:tls` are never reached by your bundler's resolution graph. Constructing a server with `protocol: 'tcp'` / `'tls'` still throws `ProtocolError` clearly at runtime (rather than at bundle time).
- **React Native**: add a metro alias mapping `node:net` and `node:tls` to [`react-native-tcp-socket`](https://github.com/Rapsssito/react-native-tcp-socket). Its API is a 1:1 emulation of the Node modules; no platform branches inside the library. `WebSocket` is built into the RN runtime. For app lifecycle integration, pair `manager.suspend()`/`resume()` with the `bindAppState` helper:
  ```ts
  import { AppState } from 'react-native';
  import { bindAppState } from 'multi-electrum-client';
  const dispose = bindAppState(manager, AppState);
  ```

## Examples

Runnable snippets in [`examples/`](examples/):
- [`node-basic.ts`](examples/node-basic.ts) — Node 22+, mixed TLS / TCP transports.
- [`bun-basic.ts`](examples/bun-basic.ts) — Bun, top-level await, single-server failover.
- [`browser-basic.html`](examples/browser-basic.html) — `wss` only, `visibilitychange` → `suspend` / `resume`.
- [`rn-basic.tsx`](examples/rn-basic.tsx) — React Native, `bindAppState`, headers subscription.

## Development

```bash
pnpm install
pnpm test            # unit tests
pnpm typecheck
pnpm lint
pnpm build           # tsup -> dist/ (ESM + .d.ts)
```

Integration tests (M4+) require Docker:

```bash
docker compose -f docker/compose.yml --profile slim up -d --wait
pnpm test:integration
```

## License

MIT — see `LICENSE`.
