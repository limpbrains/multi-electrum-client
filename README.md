# multi-electrum-client

Multi-server Electrum (Bitcoin) client for TypeScript with ban-aware routing,
partial-batch redirect, subscription restore, and lifecycle support for
Node, React Native, browser, and Bun.

> **Status:** pre-release. Manager + routing + typed method registry are in
> place (M0–M3); subscriptions, cache, peer discovery, lifecycle, and TCP/TLS
> still landing. See
> [`docs/specs/2026-05-08-multi-electrum-client-design.md`](docs/specs/2026-05-08-multi-electrum-client-design.md)
> for the full design and roadmap.

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
| M7 | Polish + 0.1 release | next |

## Platform notes

- **Node** ≥ 20: works out of the box. Global `WebSocket` is stable in Node 22+; Node 20 needs the `--experimental-websocket` flag, or pass `WebSocket` from the `ws` package via `WsTransport`'s `WebSocket` option. TCP / TLS use `node:net` / `node:tls`.
- **Bun**: works out of the box (`ws`, `tcp`, `tls`).
- **Browser**: only the `ws` / `wss` transport is supported. Don't construct servers with `protocol: 'tcp'` or `'tls'` — `node:net` / `node:tls` aren't available.
- **React Native**: add a metro alias mapping `node:net` and `node:tls` to [`react-native-tcp-socket`](https://github.com/Rapsssito/react-native-tcp-socket). Its API is a 1:1 emulation of the Node modules; no platform branches inside the library. `WebSocket` is built into the RN runtime. For app lifecycle integration, pair `manager.suspend()`/`resume()` with the `bindAppState` helper:
  ```ts
  import { AppState } from 'react-native';
  import { bindAppState } from 'multi-electrum-client';
  const dispose = bindAppState(manager, AppState);
  ```

## Development

```bash
pnpm install
pnpm test            # 104+ unit tests
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
