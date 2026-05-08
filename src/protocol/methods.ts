// Type-safe registry of Electrum protocol methods.
//
// Single source of truth for both `Manager.call(method, params)` overload
// resolution and the namespace API (`manager.scripthash.getBalance(...)`).
// The registry is type-only: there is no runtime data behind `Methods`. We
// expose `methodNames` as a runtime array for callers who need to enumerate
// supported names (allow-listing a custom routing policy, building a
// metrics dashboard, etc.).
//
// Coverage: the MVP method set called out in the design spec. Server-specific
// or rarely-used methods are intentionally absent — callers reach those via
// the `as`-cast escape hatch on `manager.call('vendor.specific.x', [...])`.

import type {
  Balance,
  BlockHeader,
  FeeEstimate,
  HistoryEntry,
  MerkleProof,
  RawTxHex,
  Scripthash,
  ScripthashStatus,
  ServerVersion,
  TxId,
  Unspent,
} from './types.js';

export interface MethodSpec<P extends readonly unknown[] = readonly unknown[], R = unknown> {
  /** Phantom field. Type-level only; nothing reads it at runtime. */
  readonly params: P;
  /** Phantom field. Type-level only; nothing reads it at runtime. */
  readonly result: R;
}

/**
 * The MVP method registry. Each entry pairs the wire method name with phantom
 * `params` (positional tuple) and `result` types. To add a method: extend
 * this interface and append the name to `methodNames` below.
 */
export interface Methods {
  // server.*
  'server.ping': MethodSpec<readonly [], null>;
  'server.version': MethodSpec<
    readonly [clientName: string, protocolVersion: string],
    ServerVersion
  >;
  'server.banner': MethodSpec<readonly [], string>;

  // blockchain.scripthash.*
  'blockchain.scripthash.get_balance': MethodSpec<readonly [scripthash: Scripthash], Balance>;
  'blockchain.scripthash.get_history': MethodSpec<
    readonly [scripthash: Scripthash],
    readonly HistoryEntry[]
  >;
  'blockchain.scripthash.listunspent': MethodSpec<
    readonly [scripthash: Scripthash],
    readonly Unspent[]
  >;
  'blockchain.scripthash.subscribe': MethodSpec<
    readonly [scripthash: Scripthash],
    ScripthashStatus
  >;
  'blockchain.scripthash.unsubscribe': MethodSpec<readonly [scripthash: Scripthash], boolean>;

  // blockchain.transaction.*
  // Non-verbose form (returns raw hex). The verbose form (2nd param `true`,
  // returns TxVerbose) is exposed via the `manager.transaction.getVerbose`
  // namespace wrapper rather than a registry overload — a union return type
  // would force every happy-path caller to narrow.
  'blockchain.transaction.get': MethodSpec<readonly [txid: TxId], RawTxHex>;
  'blockchain.transaction.broadcast': MethodSpec<readonly [rawTx: RawTxHex], TxId>;
  'blockchain.transaction.get_merkle': MethodSpec<
    readonly [txid: TxId, height: number],
    MerkleProof
  >;

  // blockchain.headers.*
  'blockchain.headers.subscribe': MethodSpec<readonly [], BlockHeader>;
  'blockchain.block.header': MethodSpec<readonly [height: number], string>;

  // blockchain.estimatefee
  'blockchain.estimatefee': MethodSpec<readonly [confirmationTarget: number], FeeEstimate>;
}

export type MethodName = keyof Methods;
export type ParamsOf<M extends MethodName> = Methods[M]['params'];
export type ResultOf<M extends MethodName> = Methods[M]['result'];

/**
 * Runtime list of registered method names. Useful for metrics, allow-list
 * checks in custom policies, and discovery UIs. The `satisfies` clause
 * guarantees this list stays in sync with the `Methods` interface (TS will
 * error on any name mismatch).
 */
export const methodNames = [
  'server.ping',
  'server.version',
  'server.banner',
  'blockchain.scripthash.get_balance',
  'blockchain.scripthash.get_history',
  'blockchain.scripthash.listunspent',
  'blockchain.scripthash.subscribe',
  'blockchain.scripthash.unsubscribe',
  'blockchain.transaction.get',
  'blockchain.transaction.broadcast',
  'blockchain.transaction.get_merkle',
  'blockchain.headers.subscribe',
  'blockchain.block.header',
  'blockchain.estimatefee',
] as const satisfies readonly MethodName[];

export type MethodNames = typeof methodNames;

// Exhaustiveness probe: TS errors at compile time if a method is added to
// the `Methods` interface without a matching entry in `methodNames` (or vice
// versa). `satisfies` alone only catches typos, not omissions.
type _ExhaustivenessCheck =
  Exclude<MethodName, MethodNames[number]> extends never
    ? true
    : ['methodNames is missing entries for', Exclude<MethodName, MethodNames[number]>];
const _exhaustivenessCheck: _ExhaustivenessCheck = true;
void _exhaustivenessCheck;
