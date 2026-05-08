# multi-electrum-client

Multi-server Electrum (Bitcoin) client for TypeScript with ban-aware routing,
partial-batch redirect, subscription restore, and lifecycle support for
Node, React Native, browser, and Bun.

> **Status:** scaffold (M0). Not usable yet. See
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

## Roadmap

| Milestone | Scope |
|---|---|
| M0 | Skeleton (this commit) |
| M1 | Single-client WebSocket happy path |
| M2 | Manager + RoutingPolicy + auto-batch coalescing |
| M3 | Method coverage + domain types + cache |
| M4 | Subscriptions + pluggable error classifier |
| M5 | Lifecycle (`suspend`/`resume`) + RN parity tests |
| M6 | TCP + TLS transports |
| M7 | Polish + 0.1 release |

## Platform notes

- **Node** ≥ 20: works out of the box. Global `WebSocket` is stable in Node 22+; Node 20 needs the `--experimental-websocket` flag for the `ws` transport.
- **Bun**: works out of the box.
- **Browser**: only the `ws` transport is supported.
- **React Native**: add a metro alias mapping `net` and `tls` to
  [`react-native-tcp-socket`](https://github.com/Rapsssito/react-native-tcp-socket).
  No platform branches inside the library.

## Development

```bash
pnpm install
pnpm test            # unit tests
pnpm typecheck
pnpm lint
pnpm build           # tsup -> dist/
```

Integration tests (M3+) require Docker:

```bash
docker compose -f docker/compose.yml --profile slim up -d --wait
pnpm test:integration
```

## License

MIT — see `LICENSE`.
