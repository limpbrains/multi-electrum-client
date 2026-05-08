// Type-safe registry of Electrum protocol methods.
//
// Single source of truth for both `Manager.call(method, params)` overload
// resolution and the namespace API (`manager.scripthash.getBalance(...)`).
// Each entry pairs the wire method name with phantom params/result types;
// `spec<P, R>()` is type-only and produces no runtime data.
//
// Coverage: the MVP method set called out in the design spec. Server-specific
// or rarely-used methods are intentionally absent — callers reach those via
// the low-level `manager.call('vendor.specific.method', [...])` escape hatch
// which falls through to the second overload (string method, unknown[] params,
// caller-asserted result).

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
  /** Phantom field. Never read at runtime; only carries the param tuple type. */
  readonly params: P;
  /** Phantom field. Never read at runtime; only carries the result type. */
  readonly result: R;
}

function spec<P extends readonly unknown[], R>(): MethodSpec<P, R> {
  // The cast is a compile-time lie. Nothing reads .params or .result at
  // runtime; the registry is only consulted by the type system.
  return undefined as unknown as MethodSpec<P, R>;
}

/**
 * The MVP method registry. Add an entry here and both `manager.call` and the
 * namespace API gain typing in lockstep.
 */
export const methods = {
  // server.*
  'server.ping': spec<readonly [], null>(),
  'server.version': spec<readonly [clientName: string, protocolVersion: string], ServerVersion>(),
  'server.banner': spec<readonly [], string>(),

  // blockchain.scripthash.*
  'blockchain.scripthash.get_balance': spec<readonly [scripthash: Scripthash], Balance>(),
  'blockchain.scripthash.get_history': spec<
    readonly [scripthash: Scripthash],
    readonly HistoryEntry[]
  >(),
  'blockchain.scripthash.listunspent': spec<
    readonly [scripthash: Scripthash],
    readonly Unspent[]
  >(),
  'blockchain.scripthash.subscribe': spec<readonly [scripthash: Scripthash], ScripthashStatus>(),
  'blockchain.scripthash.unsubscribe': spec<readonly [scripthash: Scripthash], boolean>(),

  // blockchain.transaction.*
  // The non-verbose form is the common one (returns raw hex). For verbose
  // decoded responses, callers go through the raw `manager.call(method,
  // [txid, true])` escape hatch — the union return type would force every
  // happy-path caller to narrow.
  'blockchain.transaction.get': spec<readonly [txid: TxId], RawTxHex>(),
  'blockchain.transaction.broadcast': spec<readonly [rawTx: RawTxHex], TxId>(),
  'blockchain.transaction.get_merkle': spec<readonly [txid: TxId, height: number], MerkleProof>(),

  // blockchain.headers.*
  'blockchain.headers.subscribe': spec<readonly [], BlockHeader>(),
  'blockchain.block.header': spec<readonly [height: number], string>(),

  // blockchain.estimatefee
  'blockchain.estimatefee': spec<readonly [confirmationTarget: number], FeeEstimate>(),
} as const;

export type Methods = typeof methods;
export type MethodName = keyof Methods;
export type ParamsOf<M extends MethodName> = Methods[M]['params'];
export type ResultOf<M extends MethodName> = Methods[M]['result'];
