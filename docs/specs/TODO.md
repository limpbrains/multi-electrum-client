# TODO — deferred work

Notes captured during development that are intentionally out of scope for the
current milestone but worth keeping visible. Add new items at the top with a
date.

## 2026-05-08

### UI-oriented event emitter (post-MVP)

Manager already exposes `client-state` / `client-banned` / `subscription-restored`
/ `error` events. Add a thinner UI-friendly stream for status widgets:

- ~~**`status`**~~ — ✅ shipped as the `pool-state` event +
  `manager.poolState` getter (`online` / `degraded` / `offline` with
  `{usable, connected, total}` counts, ban-expiry timer, start/resume
  baseline guarantees). See CHANGELOG.
- **`message`** — fires once per inbound wire message (request response or
  notification). No payload, just a heartbeat — UIs can bind it to a
  blink/pulse animation. Must NOT include the message body; payloads can be
  large and binding them to renders kills perf. Still open.

Use case: a "network status" widget that shows a green/yellow/red dot and
blinks on incoming traffic. Keep it cheap to subscribe to (constant work
per fire, no allocation).

### Extras module — bitcoinjs-aware helpers (post-MVP)

BlueWallet's `BlueElectrum.ts`
(https://raw.githubusercontent.com/BlueWallet/BlueWallet/refs/heads/master/blue_modules/BlueElectrum.ts)
ships address-level helpers the core lib intentionally avoids — anything that
needs bitcoinjs (address → scripthash conversion, witness/redeem script
handling, signing prep). Port the useful ones to a separate `extras` module:

- `getTransactionsFullByAddress(manager, bitcoinjs, address)`
- similar `*ByAddress` wrappers around scripthash-keyed methods

Constraint: do **not** add `bitcoinjs-lib` to the lib's deps. Each helper
takes `bitcoinjs` as an explicit argument so callers control the version /
build (browserify-aes vs noble, etc.). Pattern:

```ts
import { getTransactionsFullByAddress } from 'multi-electrum-client/extras';
import * as bitcoinjs from 'bitcoinjs-lib';

const txs = await getTransactionsFullByAddress(manager, bitcoinjs, addr);
```

Module location candidate: `src/extras/{addresses,index}.ts` with a separate
export entry in `package.json` so tree-shaking works for users who don't
need it.
