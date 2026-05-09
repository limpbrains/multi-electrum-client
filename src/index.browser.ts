// Browser entry. Re-exports everything except `TcpTransport` and
// `TlsTransport`, and registers only `ws` / `wss` in the transport factory.
// This keeps `node:net` / `node:tls` out of a browser bundler's resolution
// graph entirely. Browser users can still set `protocol: 'tcp'` / `'tls'`
// in `ServerSpec`, but the default factory will throw a clear
// `ProtocolError('no transport registered for protocol \'tcp\'')` rather
// than failing at module-resolve time.
//
// To use a custom transport in the browser (e.g. tunneling TCP via a
// WebSocket bridge) override `ManagerOptions.transportFactory`.

// Side-effect: registers ws + wss in the transport factory. Must come before
// the manager is constructed.
import './transport/ws.js';

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

// Single-client surface (no TcpTransport / TlsTransport — browser only).
export { ElectrumClient, type ElectrumClientOpts, type BatchCallItem } from './client.js';
export { WsTransport, type WsTransportOpts } from './transport/ws.js';
export type { Transport, TransportEvent, TransportListener } from './transport/types.js';
export {
  registerTransport,
  defaultTransportFactory,
  registeredProtocols,
} from './transport/factory.js';

export { ElectrumManager, type ManagerEvents } from './manager.js';
export type { SubscriptionHandler, Unsubscribe } from './subscriptions/types.js';

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
