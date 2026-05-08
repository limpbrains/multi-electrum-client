// Type-level tests for the M3 method registry and namespace API.
//
// This file is **not** executed by vitest — only type-checked by `tsc`. The
// `expectTypeOf(...)` calls live inside a function that is never invoked, so
// the chained matchers run at compile time (any type mismatch errors there)
// while the call expressions never fire at runtime. `vitest.config.ts` excludes
// `test/types/**` from its run; `pnpm typecheck` covers this file.
//
// Why not let vitest run it: each `expectTypeOf(m.call(...))` evaluates the
// `m.call(...)` argument at runtime to pass it to expectTypeOf. With
// `declare const m`, runtime would throw ReferenceError. Building a real
// manager instead would actually fire requests against MockTransport and leak
// unhandled promise rejections.

import { expectTypeOf } from 'vitest';

import type { ElectrumManager, TxVerbose } from '../../src/index.js';
import type {
  Balance,
  BlockHeader,
  FeeEstimate,
  HistoryEntry,
  MerkleProof,
  RawTxHex,
  ScripthashStatus,
  ServerVersion,
  TxId,
  Unspent,
} from '../../src/index.js';

declare const m: ElectrumManager;

// Wrapped in an unused function so vitest can ignore the file but tsc still
// validates every assertion (function bodies are type-checked even when never
// called).
//
// DO NOT add top-level statements that reference `m` outside this function.
// `declare const m` has no runtime value; any top-level `m.x` access would
// throw ReferenceError if vitest ever included this file by accident.
function _typeAssertions(): void {
  // --- manager.call typed overload ---

  expectTypeOf(m.call('blockchain.scripthash.get_balance', ['hash'])).toEqualTypeOf<
    Promise<Balance>
  >();
  expectTypeOf(m.call('blockchain.scripthash.get_history', ['hash'])).toEqualTypeOf<
    Promise<readonly HistoryEntry[]>
  >();
  expectTypeOf(m.call('blockchain.scripthash.listunspent', ['hash'])).toEqualTypeOf<
    Promise<readonly Unspent[]>
  >();
  expectTypeOf(m.call('blockchain.scripthash.subscribe', ['hash'])).toEqualTypeOf<
    Promise<ScripthashStatus>
  >();
  expectTypeOf(m.call('blockchain.transaction.get', ['txid'])).toEqualTypeOf<Promise<RawTxHex>>();
  expectTypeOf(m.call('blockchain.transaction.broadcast', ['hex'])).toEqualTypeOf<Promise<TxId>>();
  expectTypeOf(m.call('blockchain.transaction.get_merkle', ['txid', 100])).toEqualTypeOf<
    Promise<MerkleProof>
  >();
  expectTypeOf(m.call('blockchain.headers.subscribe', [])).toEqualTypeOf<Promise<BlockHeader>>();
  expectTypeOf(m.call('blockchain.estimatefee', [6])).toEqualTypeOf<Promise<FeeEstimate>>();
  expectTypeOf(m.call('server.version', ['client', '1.4'])).toEqualTypeOf<Promise<ServerVersion>>();
  expectTypeOf(m.call('server.ping', [])).toEqualTypeOf<Promise<null>>();

  // --- params optional for empty-tuple methods ---

  expectTypeOf(m.call('server.ping')).toEqualTypeOf<Promise<null>>();
  expectTypeOf(m.call('server.banner')).toEqualTypeOf<Promise<string>>();
  expectTypeOf(m.call('blockchain.headers.subscribe')).toEqualTypeOf<Promise<BlockHeader>>();

  // --- escape hatch (non-literal method) ---

  const dynamic: string = 'vendor.specific';
  expectTypeOf(m.call(dynamic, [1, 2, 3])).toEqualTypeOf<Promise<unknown>>();

  // --- @ts-expect-error: bad params for known methods ---

  // @ts-expect-error -- height is required as second arg
  m.call('blockchain.transaction.get_merkle', ['txid']);
  // @ts-expect-error -- scripthash must be a string
  m.call('blockchain.scripthash.get_balance', [42]);
  // @ts-expect-error -- params required for non-empty-tuple method
  m.call('blockchain.scripthash.get_balance');

  // --- namespace API mirrors call surface ---

  expectTypeOf(m.scripthash.getBalance('hash')).toEqualTypeOf<Promise<Balance>>();
  expectTypeOf(m.scripthash.getHistory('hash')).toEqualTypeOf<Promise<readonly HistoryEntry[]>>();
  expectTypeOf(m.scripthash.listUnspent('hash')).toEqualTypeOf<Promise<readonly Unspent[]>>();
  expectTypeOf(m.scripthash.subscribe('hash')).toEqualTypeOf<Promise<ScripthashStatus>>();
  expectTypeOf(m.scripthash.unsubscribe('hash')).toEqualTypeOf<Promise<boolean>>();

  expectTypeOf(m.transaction.get('txid')).toEqualTypeOf<Promise<RawTxHex>>();
  expectTypeOf(m.transaction.broadcast('hex')).toEqualTypeOf<Promise<TxId>>();
  expectTypeOf(m.transaction.getMerkle('txid', 100)).toEqualTypeOf<Promise<MerkleProof>>();
  expectTypeOf(m.transaction.getVerbose('txid')).toEqualTypeOf<Promise<TxVerbose>>();

  expectTypeOf(m.headers.getTip()).toEqualTypeOf<Promise<BlockHeader>>();
  expectTypeOf(m.headers.getHeader(100)).toEqualTypeOf<Promise<string>>();

  expectTypeOf(m.server.ping()).toEqualTypeOf<Promise<null>>();
  expectTypeOf(m.server.version('cli', '1.4')).toEqualTypeOf<Promise<ServerVersion>>();
  expectTypeOf(m.server.banner()).toEqualTypeOf<Promise<string>>();

  expectTypeOf(m.estimateFee(6)).toEqualTypeOf<Promise<FeeEstimate>>();

  // --- @ts-expect-error: bad namespace args ---

  // @ts-expect-error -- expects string scripthash
  m.scripthash.getBalance(42);
  // @ts-expect-error -- height required as 2nd arg
  m.transaction.getMerkle('txid');
}

void _typeAssertions;
