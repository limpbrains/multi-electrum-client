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
import type { DiscoverOptions } from '../discovery.js';
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
  /**
   * Payload framing for `ws`/`wss` peers. `'message'` (default) — the
   * native Electrum-over-WebSocket protocol, one complete JSON-RPC per
   * message. `'newline'` — a byte tunnel relaying a newline-delimited
   * TCP stream over frames with arbitrary boundaries. Declared, not
   * guessed; see `Endpoint.wsFraming`.
   */
  wsFraming?: 'message' | 'newline';
  /**
   * Per-server override of the transport's line/response cap — see
   * `Endpoint.maxLineLength`.
   */
  maxLineLength?: number;
  /**
   * Per-server aggregate WebSocket message bound for `'newline'`
   * framing — see `Endpoint.maxMessageLength`.
   */
  maxMessageLength?: number;
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
 * The server forwards bitcoind's RPC-decoded transaction shape verbatim.
 * Server-software versions occasionally add fields beyond what's typed here;
 * consumers who need them `as`-cast to a wider shape rather than reaching
 * through an index signature (which would weaken the named-field types to
 * `unknown`).
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
}

export interface TxVin {
  txid?: TxId;
  vout?: number;
  scriptSig?: { asm: string; hex: string };
  txinwitness?: readonly string[];
  sequence: number;
  coinbase?: string;
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
  };
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
  /**
   * Output index. Wire field is `tx_pos` (snake_case) — kept verbatim for now
   * because no runtime decoder ships in M3. The post-M4 decoder pass will
   * normalize wire names to a parallel camelCase domain type.
   */
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
  /**
   * Wire field is `block_height` (snake_case). Kept verbatim until the
   * post-M4 decoder pass remaps wire names to a domain type.
   */
  block_height: number;
  pos: number;
  merkle: readonly string[];
}

// --- Server / fees ---------------------------------------------------------

/** `[serverSoftware, protocolVersion]`, e.g. `['ElectrumX 1.16.0', '1.4.2']`. */
export type ServerVersion = readonly [serverSoftware: string, protocolVersion: string];

/**
 * Fee estimate from `blockchain.estimatefee` — BTC per kvB.
 *
 * **Sentinel:** the server returns `-1` (literally negative one) when it has
 * no estimate for the requested confirmation target — typically because it
 * just started, the mempool is empty, or the depth is past its cache. Callers
 * MUST treat any negative value as "no estimate available" and fall back to a
 * floor / cached value. We keep the wire-truthful `number` type rather than
 * normalizing to `number | null` so consumers see the raw signal; future
 * decoders may add a `getFeeEstimate(): number | null` helper if useful.
 */
export type FeeEstimate = number;

/**
 * Mempool fee histogram entry from `mempool.get_fee_histogram`. Wire shape
 * is `[fee, vsize]` where:
 *  - `fee` is the **rounded-up** fee rate in sat/vB,
 *  - `vsize` is the cumulative virtual size (bytes) of mempool transactions
 *    paying at least that fee rate.
 *
 * The server returns the array in **descending fee order**, so the entry
 * at index 0 covers the highest-paying mempool slice. Wallets reach
 * `nextBlock` fee by walking entries until cumulative `vsize` exceeds the
 * remaining block weight.
 *
 * Source: Electrum protocol 1.4+ — https://electrumx.readthedocs.io
 */
export type FeeHistogramEntry = readonly [feeSatVb: number, vsize: number];

/** Full histogram is just an array of entries. Empty mempool → `[]`. */
export type FeeHistogram = readonly FeeHistogramEntry[];

// --- Call-site options -----------------------------------------------------

export interface CallOpts {
  signal?: AbortSignal;
  timeoutMs?: number;
  autoBatch?: boolean;
  retry?: 'auto' | 'none' | { maxAttempts: number };
  /**
   * Per-call hedging override. `false` suppresses a manager-enabled hedge
   * for this call. By default only methods on the manager's explicit
   * allowlist of known-idempotent reads hedge; `hedge: true` is the
   * escape hatch that allows hedging a method OUTSIDE that allowlist
   * (e.g. a vendor-specific read) — by setting it the caller asserts the
   * method is idempotent. Requires `ManagerOptions.hedging` for the delay
   * value — with no manager-level config there is nothing to arm, so
   * `hedge: true` is a no-op. Methods with side effects
   * (`blockchain.transaction.broadcast`) and session-bound methods
   * (`server.version` — session negotiation, `*.subscribe` /
   * `*.unsubscribe`) NEVER hedge — `hedge: true` does not override that.
   */
  hedge?: boolean;
  /**
   * Hint to bypass `policy.pick` and route to this exact client. Honored by
   * the manager when the client is connected, non-banned, and not in the
   * `excluded` set; otherwise the call falls through to the normal pick.
   * Used internally by SubscriptionRegistry to send wire-level
   * unsubscribes to the same server we subscribed on.
   */
  preferClient?: ClientId;
  /**
   * Escalates `preferClient` from a hint to an ADDRESSED call: that
   * exact connection or nobody — no `policy.pick` fallback, because
   * the call targets protocol state living on one specific session (a
   * wire unsubscribe, a discovery probe of one peer) where any
   * fallback is a misroute. Ban-AWARE by default: strict addressing is
   * not permission to send new work to a peer sitting out a cooldown.
   * Requires `retry: 'none'` — an addressed call has exactly one
   * meaningful dispatch, and any retry policy would silently reduce to
   * nothing (the failed pin joins `excluded` and the strict branch
   * then no-picks); other combinations are rejected up front.
   */
  pinStrict?: boolean;
  /**
   * With `pinStrict`, also allow a BANNED (but connected) target. Only
   * for cleanup of state the session already holds — the registry's
   * wire unsubscribe must reach the connection that owns the
   * subscription even while that server cools down. New work never
   * sets this.
   */
  pinBanExempt?: boolean;
  /**
   * Forwarded into `PickContext.stickyKey` so user policies wrapped in
   * `withSticky(...)` can pin requests beyond the scripthash heuristic.
   * Default: undefined (the policy receives the field as undefined and
   * falls back to its own heuristics).
   */
  stickyKey?: string;
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
  /**
   * Pool-wide default for the transport line/response cap, applied to
   * every server the manager installs — including peers admitted by
   * discovery, which no per-`ServerSpec` value can reach. A spec's own
   * `maxLineLength` overrides it for that server. See
   * `ServerSpec.maxLineLength`.
   */
  maxLineLength?: number;
  reconnectBackoff?: ReconnectBackoff;
  cooldownMs?: number;
  finalizedConfs?: number;
  /**
   * Optional peer discovery. When `enabled`, the manager calls
   * `server.peers.subscribe` on every connect (and periodically afterward)
   * and admits ws/wss peers it doesn't already have. See `DiscoverOptions`.
   */
  discover?: DiscoverOptions;
  /**
   * Opt-in hedged requests. When a call (or a coalesced auto-batch group)
   * hasn't settled within `afterMs`, the manager fires the same request —
   * or the same wire batch — on a second eligible client (policy pick
   * with the first excluded) WITHOUT cancelling the first; the first real
   * answer settles the caller(s) and the loser's late reply is swallowed
   * (still recorded in telemetry). Bounds a hung-but-accepting server to
   * `afterMs` instead of the full `requestTimeoutMs`.
   *
   * Only methods on the built-in allowlist of known-idempotent reads
   * hedge by default (the typed registry minus broadcast / subscribe /
   * unsubscribe, plus `server.features`,
   * `blockchain.scripthash.get_mempool` and `blockchain.relayfee`);
   * unknown/vendor methods require an explicit per-call
   * `CallOpts.hedge: true`. Broadcast, `server.version` (session
   * negotiation) and `*.subscribe` / `*.unsubscribe` are hard-excluded
   * regardless. A coalesced batch
   * group hedges as a whole only when EVERY item in it is hedge-eligible
   * and has budget for a second dispatch; mixed groups are not hedged.
   * Tradeoff: an item the winning branch failed retryably waits for the
   * sibling dispatch (up to `requestTimeoutMs`) before retrying
   * elsewhere, where an unhedged call would retry immediately — the
   * price of never racing a duplicate onto a third server. `afterMs`
   * must be a positive number (the constructor throws otherwise).
   * Absent = off (no behavior change); per-call override via
   * `CallOpts.hedge`.
   */
  hedging?: { afterMs: number };
  /**
   * Issue `server.version(clientName, protocolVersion)` on every
   * connect to populate the client's `capabilities.serverSoftware`.
   * The classifier consults that to pick its per-software substring
   * table; without the handshake every classification falls through
   * to the generic table.
   *
   * **Eventual-consistency window**: the handshake fires
   * fire-and-forget AFTER the client transitions to `'connected'`,
   * so a request issued in the same microtask as the state event
   * (e.g. on a freshly added server during a burst) can race ahead
   * of the version response and be classified against the generic
   * table. The generic table is a deliberately narrower superset of
   * the vendor lists' "well-known" phrases — for the rate-limit
   * codepath specifically the practical difference is small (vendor
   * tables and the generic table both catch the canonical strings),
   * but if you need vendor-exact classification on the very first
   * call, await `server.version` yourself before issuing it.
   *
   * Default: `true`. Set `false` for tests / workflows that drive
   * `server.version` themselves — ElectrumX 1.16+ rejects a second
   * version call on the same session with `"server.version already
   * sent"`.
   */
  handshakeOnConnect?: boolean;
}
