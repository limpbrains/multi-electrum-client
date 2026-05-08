// Domain types for the Electrum protocol surface.
//
// Public, friendly TypeScript types — discriminated unions, named tuples,
// shapes that map to BlueWallet / Sparrow conventions.
//
// Numeric values are typed as `number` to match the JSON-RPC wire protocol
// (Electrum servers serialize sats as JSON numbers). Caller code that needs
// exact precision past 2^53 — e.g. summing huge wallets — should `BigInt(...)`
// at the boundary. A future runtime decoder pass (post-M4) may upgrade these
// to `bigint` once we have schema-driven coercion in place.

import type { CacheStore } from '../cache/types.js';
import type { ClientId, Protocol } from '../client.js';
import type { ErrorClassifier } from '../errors/types.js';
import type { RoutingPolicy } from '../policy/types.js';

// --- Branded primitive aliases ---------------------------------------------
//
// Plain string aliases for documentation. We don't ship runtime branding
// (would require validation at every boundary); these are purely a hint to
// callers about what each string represents.

export type Scripthash = string;
export type TxId = string;
export type RawTxHex = string;
export type BlockHash = string;
export type HeaderHex = string;

// --- Network ---------------------------------------------------------------

export type Network = 'mainnet' | 'testnet' | 'regtest' | 'signet';

export interface ServerSpec {
  id: ClientId;
  host: string;
  port: number;
  protocol: Protocol;
  /** Optional URL path for WebSocket endpoints, e.g. `/ws`. */
  path?: string;
}

// --- Transactions ----------------------------------------------------------

export type Tx = ConfirmedTx | UnconfirmedTx;

export interface ConfirmedTx {
  status: 'confirmed';
  txid: TxId;
  /** Block height; > 0. */
  height: number;
  blockHash?: BlockHash;
  /** Fee in satoshis. */
  fee?: number;
}

export interface UnconfirmedTx {
  status: 'unconfirmed';
  txid: TxId;
  /** Electrum convention: 0 = unconfirmed, -1 = parent unconfirmed. */
  height: 0 | -1;
  fee?: number;
}

/**
 * Verbose form of `blockchain.transaction.get` (when called with `verbose=true`).
 * The server forwards bitcoind's RPC-decoded transaction shape verbatim, which
 * is large and not stable across server software versions. We type the few
 * widely-used fields and leave the rest as a permissive index signature.
 */
export interface TxVerbose {
  txid: TxId;
  hash: string;
  hex: RawTxHex;
  size: number;
  vsize: number;
  weight: number;
  version: number;
  locktime: number;
  vin: readonly TxVin[];
  vout: readonly TxVout[];
  blockhash?: BlockHash;
  confirmations?: number;
  time?: number;
  blocktime?: number;
  [key: string]: unknown;
}

export interface TxVin {
  txid?: TxId;
  vout?: number;
  scriptSig?: { asm: string; hex: string };
  txinwitness?: readonly string[];
  sequence: number;
  coinbase?: string;
  [key: string]: unknown;
}

export interface TxVout {
  value: number;
  n: number;
  scriptPubKey: {
    asm: string;
    hex: string;
    type?: string;
    address?: string;
    addresses?: readonly string[];
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

// --- Scripthash queries ----------------------------------------------------

export interface Balance {
  /** Satoshis confirmed. */
  confirmed: number;
  /** Satoshis in mempool (positive = incoming, negative = outgoing). */
  unconfirmed: number;
}

export interface HistoryEntry {
  txid: TxId;
  /** > 0 confirmed, 0 unconfirmed, -1 parent unconfirmed. */
  height: number;
  /** Fee in sats; only set for unconfirmed entries by some servers. */
  fee?: number;
}

export interface Unspent {
  txid: TxId;
  /** Output index. */
  tx_pos: number;
  /** Satoshis. */
  value: number;
  /** Block height; 0 if unconfirmed. */
  height: number;
}

/**
 * Status hash returned by `blockchain.scripthash.subscribe`. `null` = empty
 * history. Notification handlers receive this same shape.
 */
export type ScripthashStatus = string | null;

// --- Block headers + merkle proofs -----------------------------------------

export interface BlockHeader {
  height: number;
  hex: HeaderHex;
}

export interface MerkleProof {
  block_height: number;
  pos: number;
  merkle: readonly string[];
}

// --- Server / fees ---------------------------------------------------------

/** `[serverSoftware, protocolVersion]`, e.g. `['ElectrumX 1.16.0', '1.4.2']`. */
export type ServerVersion = readonly [serverSoftware: string, protocolVersion: string];

/**
 * Fee estimate from `blockchain.estimatefee` — BTC per kvB. `-1` means the
 * server has no estimate for the requested confirmation target.
 */
export type FeeEstimate = number;

// --- Call-site options -----------------------------------------------------

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
