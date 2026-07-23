// Shared export surface for both entry points. `src/index.ts` (Node / RN /
// Bun) re-exports this plus the TCP/TLS transports; `src/index.browser.ts`
// re-exports this alone so `node:net` / `node:tls` never enter a browser
// bundler's resolution graph.
//
// Importing `WsTransport` below pulls in `./transport/ws.js`, whose module
// body self-registers `ws` / `wss` in the transport factory — both entries
// get WebSocket support without an explicit side-effect import.

export type {
  ClientId,
  ClientView,
  Endpoint,
  Protocol,
  Telemetry,
  ConnectionState,
} from './client.js';

export type { RoutingPolicy, PickContext, Outcome } from './policy/types.js';

export type { CacheStore } from './cache/types.js';
export { MemoryCache } from './cache/memory.js';
export type { DiscoverOptions, OnDiscoverCallback } from './discovery.js';
export { parsePeerEntry, parsePeerList } from './discovery.js';

export type { ErrorKind, ErrorClassifier, ClassifyContext } from './errors/types.js';

export type {
  Tx,
  ConfirmedTx,
  UnconfirmedTx,
  TxVerbose,
  TxVin,
  TxVout,
  Balance,
  HistoryEntry,
  Unspent,
  BlockHeader,
  MerkleProof,
  ScripthashStatus,
  ServerVersion,
  FeeEstimate,
  FeeHistogram,
  FeeHistogramEntry,
  Scripthash,
  TxId,
  RawTxHex,
  BlockHash,
  HeaderHex,
  Network,
  ServerSpec,
  CallOpts,
  BatchRequest,
  ManagerOptions,
  ReconnectBackoff,
} from './protocol/types.js';

// Method registry (M3): one-source-of-truth for typed `manager.call(...)`
// and the namespace API. Type-only — there is no runtime registry to
// dereference. `methodNames` is the runtime list (allow-listing in custom
// policies, metrics, discovery UIs).
export type {
  Methods,
  MethodName,
  MethodNames,
  MethodSpec,
  ParamsOf,
  ResultOf,
} from './protocol/methods.js';
export { methodNames } from './protocol/methods.js';

export {
  SuspendedError,
  TimeoutError,
  TransportError,
  RpcError,
  ProtocolError,
} from './errors/types.js';

// Single-client surface:
export { ElectrumClient, type ElectrumClientOpts, type BatchCallItem } from './client.js';
export { WsTransport, type WsTransportOpts } from './transport/ws.js';
export type { Transport, TransportEvent, TransportListener } from './transport/types.js';
export {
  registerTransport,
  defaultTransportFactory,
  registeredProtocols,
} from './transport/factory.js';

// Manager + RoutingPolicy built-ins (M2) + subscriptions (M4):
export { ElectrumManager, type ManagerEvents } from './manager.js';
export type { SubscriptionHandler, Unsubscribe } from './subscriptions/types.js';

// Lifecycle (M5):
export type { LifecycleState, SuspendOptions } from './lifecycle/types.js';
export {
  bindAppState,
  type AppStateLike,
  type AppStateStatus,
  type BindAppStateOptions,
} from './lifecycle/rn-appstate.js';
export {
  roundRobin,
  failover,
  preferFastest,
  withSticky,
  type PreferFastestOpts,
  type StickyKeyFn,
} from './policy/builtins.js';
export { defaultClassifier, composeClassifier } from './errors/classifier.js';
export { NoClientAvailableError } from './errors/types.js';
export type { Result } from './util/result.js';
export { ok, err } from './util/result.js';
