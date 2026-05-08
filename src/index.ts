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
  Balance,
  HistoryEntry,
  Unspent,
  BlockHeader,
  MerkleProof,
  ScripthashStatus,
  Network,
  ServerSpec,
  CallOpts,
  BatchRequest,
  ManagerOptions,
} from './protocol/types.js';

export {
  SuspendedError,
  TimeoutError,
  TransportError,
  RpcError,
  ProtocolError,
} from './errors/types.js';

// Single-client surface (M1):
export { ElectrumClient, type ElectrumClientOpts } from './client.js';
export { WsTransport, type WsTransportOpts } from './transport/ws.js';
export type { Transport, TransportEvent, TransportListener } from './transport/types.js';

// Manager + namespace API land in M2.
// export { ElectrumManager } from './manager.js';
