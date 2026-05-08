// multi-electrum-client — public entry. M0 skeleton: types only, no runtime impl yet.
// Implementation lands in M1+.

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

// Manager + RoutingPolicy built-ins (M2) + subscriptions (M4):
export { ElectrumManager, type ManagerEvents } from './manager.js';
export type { SubscriptionHandler, Unsubscribe } from './subscriptions/types.js';
export {
  roundRobin,
  failover,
  preferFastest,
  withSticky,
  type PreferFastestOpts,
  type StickyKeyFn,
} from './policy/builtins.js';
export { defaultClassifier } from './errors/classifier.js';
export { NoClientAvailableError } from './errors/types.js';
export type { Result } from './util/result.js';
export { ok, err } from './util/result.js';
