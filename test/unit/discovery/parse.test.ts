import { describe, expect, it } from 'vitest';

import { parsePeerEntry, parsePeerList } from '../../../src/discovery.js';

describe('parsePeerEntry', () => {
  it('extracts ws:port and wss:port features', () => {
    const out = parsePeerEntry([
      'electrum.example.com',
      '1.2.3.4',
      ['v1.4', 's50002', 't50001', 'ws:50003', 'wss:50004'],
    ]);
    expect(out).toEqual([
      {
        id: 'electrum.example.com:50003',
        host: 'electrum.example.com',
        port: 50003,
        protocol: 'ws',
      },
      {
        id: 'electrum.example.com:50004',
        host: 'electrum.example.com',
        port: 50004,
        protocol: 'wss',
      },
    ]);
  });

  it('skips entries without ws/wss features', () => {
    expect(parsePeerEntry(['x.example.com', '', ['v1.4', 's50002', 't50001']])).toEqual([]);
  });

  it('returns empty for malformed entries', () => {
    expect(parsePeerEntry(null)).toEqual([]);
    expect(parsePeerEntry({})).toEqual([]);
    expect(parsePeerEntry([])).toEqual([]);
    expect(parsePeerEntry([42])).toEqual([]);
    expect(parsePeerEntry(['host'])).toEqual([]);
    expect(parsePeerEntry(['host', '', null])).toEqual([]);
  });

  it('rejects malformed port numbers', () => {
    expect(parsePeerEntry(['x', '', ['ws:abc']])).toEqual([]);
    expect(parsePeerEntry(['x', '', ['ws:0']])).toEqual([]);
    expect(parsePeerEntry(['x', '', ['ws:99999']])).toEqual([]);
  });
});

describe('parsePeerList', () => {
  it('flattens entries from multiple peers', () => {
    const out = parsePeerList([
      ['a.example.com', '1.1.1.1', ['ws:1']],
      ['b.example.com', '2.2.2.2', ['wss:2']],
    ]);
    expect(out).toHaveLength(2);
    expect(out[0]?.host).toBe('a.example.com');
    expect(out[1]?.host).toBe('b.example.com');
  });

  it('returns empty array for non-array response', () => {
    expect(parsePeerList(null)).toEqual([]);
    expect(parsePeerList('something')).toEqual([]);
    expect(parsePeerList({})).toEqual([]);
  });
});
