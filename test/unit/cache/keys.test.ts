import { describe, expect, it } from 'vitest';

import { buildKey, cacheSpec } from '../../../src/cache/keys.js';

describe('buildKey', () => {
  it('formats network + bucket + id with the et:v1 prefix', () => {
    expect(buildKey('mainnet', 'hdr', 'a')).toBe('et:mainnet:v1:hdr:a');
    expect(buildKey('regtest', 'mrk', 'tx:ff')).toBe('et:regtest:v1:mrk:tx:ff');
  });
});

describe('cacheSpec', () => {
  it('returns a hdr spec for blockchain.block.header', () => {
    expect(cacheSpec('blockchain.block.header', [42])).toEqual({
      bucket: 'hdr',
      id: '2a',
      finalityHeight: 42,
    });
  });

  it('rejects block.header with non-integer height', () => {
    expect(cacheSpec('blockchain.block.header', [-1])).toBeNull();
    expect(cacheSpec('blockchain.block.header', ['42'])).toBeNull();
    expect(cacheSpec('blockchain.block.header', [1.5])).toBeNull();
    expect(cacheSpec('blockchain.block.header', [])).toBeNull();
  });

  it('returns a mrk spec for blockchain.transaction.get_merkle', () => {
    expect(cacheSpec('blockchain.transaction.get_merkle', ['abcd', 100])).toEqual({
      bucket: 'mrk',
      id: 'abcd:64',
      finalityHeight: 100,
    });
  });

  it('rejects merkle with bad params', () => {
    expect(cacheSpec('blockchain.transaction.get_merkle', ['abcd'])).toBeNull();
    expect(cacheSpec('blockchain.transaction.get_merkle', [42, 100])).toBeNull();
    expect(cacheSpec('blockchain.transaction.get_merkle', ['', 100])).toBeNull();
  });

  it('returns null for non-cacheable methods', () => {
    expect(cacheSpec('blockchain.scripthash.get_balance', ['hash'])).toBeNull();
    expect(cacheSpec('blockchain.transaction.get', ['txid'])).toBeNull();
    expect(cacheSpec('server.ping', [])).toBeNull();
  });
});
