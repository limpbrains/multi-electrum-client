// Domain types for the Electrum protocol surface.
// These are the public, friendly TypeScript types — discriminated unions, branded ids
// where useful, and shapes that map to BlueWallet / Sparrow conventions.
// Real method registry + per-method param/result types arrive in M3.

import type { CacheStore } from '../cache/types.js';
import type { ClientId, Protocol } from '../client.js';
import type { ErrorClassifier } from '../errors/types.js';
import type { RoutingPolicy } from '../policy/types.js';

export type Network = 'mainnet' | 'testnet' | 'regtest' | 'signet';

export interface ServerSpec {
  id: ClientId;
  host: string;
  port: number;
  protocol: Protocol;
  /** Optional URL path for WebSocket endpoints, e.g. `/ws`. */
  path?: string;
}

export type Tx = ConfirmedTx | UnconfirmedTx;

export interface ConfirmedTx {
  status: 'confirmed';
  txid: string;
  /** Block height; > 0. */
  height: number;
  blockHash?: string;
  /** Fee in satoshis. */
  fee?: bigint;
}

export interface UnconfirmedTx {
  status: 'unconfirmed';
  txid: string;
  /** Electrum convention: 0 = unconfirmed, -1 = parent unconfirmed. */
  height: 0 | -1;
  fee?: bigint;
}

export interface Balance {
  /** Satoshis confirmed. */
  confirmed: bigint;
  /** Satoshis in mempool (positive = incoming, negative = outgoing). */
  unconfirmed: bigint;
}

export interface HistoryEntry {
  txid: string;
  /** > 0 confirmed, 0 unconfirmed, -1 parent unconfirmed. */
  height: number;
  /** Fee in sats; only set for unconfirmed entries by some servers. */
  fee?: bigint;
}

export interface Unspent {
  txid: string;
  vout: number;
  value: bigint;
  height: number;
}

export interface BlockHeader {
  height: number;
  hex: string;
}

export interface MerkleProof {
  blockHeight: number;
  pos: number;
  merkle: readonly string[];
}

/**
 * Status hash returned by `blockchain.scripthash.subscribe`. `null` = empty history.
 */
export type ScripthashStatus = string | null;

export interface CallOpts {
  signal?: AbortSignal;
  timeoutMs?: number;
  autoBatch?: boolean;
  retry?: 'auto' | 'none' | { maxAttempts: number };
  preferClient?: ClientId;
  bypassCache?: boolean;
  failOnSuspend?: boolean;
}

export interface BatchRequest {
  method: string;
  params: readonly unknown[];
}

export interface ReconnectBackoff {
  minMs: number;
  maxMs: number;
  factor: number;
  jitter: number;
}

export interface ManagerOptions {
  network: Network;
  servers: readonly ServerSpec[];
  policy: RoutingPolicy;
  cache?: CacheStore;
  classifier?: ErrorClassifier;
  autoBatch?: boolean;
  requestTimeoutMs?: number;
  reconnectBackoff?: ReconnectBackoff;
  cooldownMs?: number;
  finalizedConfs?: number;
}
