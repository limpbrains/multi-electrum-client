// Type-level tests for the M3 method registry and namespace API.
//
// These run as empty test cases at runtime (`expectTypeOf` is compile-time
// only) but the file is included in the regular Vitest suite so any TS
// regression here fails CI.

import { describe, expectTypeOf, it } from 'vitest';

import { ElectrumManager, failover, type ServerSpec } from '../../src/index.js';
import { MockTransport } from '../helpers/mockTransport.js';
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

const SERVERS: ServerSpec[] = [{ id: 'a', host: 'a', port: 1, protocol: 'ws' }];

function makeManager(): ElectrumManager {
  return new ElectrumManager({
    network: 'regtest',
    servers: SERVERS,
    policy: failover(),
    transportFactory: () => new MockTransport(),
  });
}

describe('manager.call typed overload', () => {
  it('infers result from registry for known methods', () => {
    const m = makeManager();
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
    expectTypeOf(m.call('blockchain.transaction.broadcast', ['hex'])).toEqualTypeOf<
      Promise<TxId>
    >();
    expectTypeOf(m.call('blockchain.transaction.get_merkle', ['txid', 100])).toEqualTypeOf<
      Promise<MerkleProof>
    >();
    expectTypeOf(m.call('blockchain.headers.subscribe', [])).toEqualTypeOf<Promise<BlockHeader>>();
    expectTypeOf(m.call('blockchain.estimatefee', [6])).toEqualTypeOf<Promise<FeeEstimate>>();
    expectTypeOf(m.call('server.version', ['client', '1.4'])).toEqualTypeOf<
      Promise<ServerVersion>
    >();
    expectTypeOf(m.call('server.ping', [])).toEqualTypeOf<Promise<null>>();
  });

  it('returns Promise<unknown> for non-literal method names (escape hatch)', () => {
    const m = makeManager();
    const dynamic: string = 'vendor.specific';
    expectTypeOf(m.call(dynamic, [1, 2, 3])).toEqualTypeOf<Promise<unknown>>();
    // Caller-asserted result via `as`-cast on the awaited value.
    expectTypeOf(m.call(dynamic, [1, 2, 3])).resolves.toBeUnknown();
  });

  it('rejects wrong param shapes for known methods', () => {
    const m = makeManager();
    // @ts-expect-error -- height is required as second arg
    m.call('blockchain.transaction.get_merkle', ['txid']);
    // @ts-expect-error -- scripthash must be a string
    m.call('blockchain.scripthash.get_balance', [42]);
    // @ts-expect-error -- empty params not accepted for required-arg method
    m.call('blockchain.scripthash.get_balance', []);
  });
});

describe('manager namespace API', () => {
  it('mirrors the call surface in camelCase', () => {
    const m = makeManager();
    expectTypeOf(m.scripthash.getBalance('hash')).toEqualTypeOf<Promise<Balance>>();
    expectTypeOf(m.scripthash.getHistory('hash')).toEqualTypeOf<Promise<readonly HistoryEntry[]>>();
    expectTypeOf(m.scripthash.listUnspent('hash')).toEqualTypeOf<Promise<readonly Unspent[]>>();
    expectTypeOf(m.scripthash.subscribe('hash')).toEqualTypeOf<Promise<ScripthashStatus>>();
    expectTypeOf(m.scripthash.unsubscribe('hash')).toEqualTypeOf<Promise<boolean>>();

    expectTypeOf(m.transaction.get('txid')).toEqualTypeOf<Promise<RawTxHex>>();
    expectTypeOf(m.transaction.broadcast('hex')).toEqualTypeOf<Promise<TxId>>();
    expectTypeOf(m.transaction.getMerkle('txid', 100)).toEqualTypeOf<Promise<MerkleProof>>();

    expectTypeOf(m.headers.subscribe()).toEqualTypeOf<Promise<BlockHeader>>();
    expectTypeOf(m.headers.getHeader(100)).toEqualTypeOf<Promise<string>>();

    expectTypeOf(m.server.ping()).toEqualTypeOf<Promise<null>>();
    expectTypeOf(m.server.version('cli', '1.4')).toEqualTypeOf<Promise<ServerVersion>>();
    expectTypeOf(m.server.banner()).toEqualTypeOf<Promise<string>>();

    expectTypeOf(m.estimateFee(6)).toEqualTypeOf<Promise<FeeEstimate>>();
  });

  it('rejects wrong arg types', () => {
    const m = makeManager();
    // @ts-expect-error -- expects string scripthash
    m.scripthash.getBalance(42);
    // @ts-expect-error -- height required as 2nd arg
    m.transaction.getMerkle('txid');
  });
});
