import { describe, expect, it } from 'vitest';

import { PeerDiscoveryRunner, parsePeerEntry, parsePeerList } from '../../../src/discovery.js';

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
describe('PeerDiscoveryRunner — per-client state lifecycle', () => {
  it('a probe whose client left the pool mid-flight admits nothing', async () => {
    // removeServer → forget() deletes the generation entry, which
    // un-cancels a probe that captured generation 0 — the explicit
    // hasClient(self) check after the call is what stops a removed
    // server's peer list from still being admitted.
    let inPool = true;
    const added: string[] = [];
    let release!: (v: unknown) => void;
    const runner = new PeerDiscoveryRunner(
      { enabled: true },
      {
        call: () => new Promise((r) => (release = r)),
        hasClient: (id) => (id === 'src' ? inPool : false),
        addServer: (spec) => {
          added.push(spec.id);
        },
        isStopped: () => false,
        onError: () => undefined,
      },
    );
    const run = runner.runFor('src');
    // The server is removed while its probe is in flight.
    inPool = false;
    runner.forget('src');
    release([['peer.example.com', '1.1.1.1', ['v1.4', 'ws:50001']]]);
    await run;
    expect(added).toEqual([]);
  });

  it('forget() drops the tracked entry cancelFor() leaves behind', () => {
    const runner = new PeerDiscoveryRunner(
      { enabled: true },
      {
        call: () => Promise.resolve([]),
        hasClient: () => false,
        addServer: () => undefined,
        isStopped: () => false,
        onError: () => undefined,
      },
    );
    // cancelFor only bumps (an in-flight probe must observe the
    // mismatch) — under discovery-driven churn that left one entry per
    // ever-seen client id, forever.
    runner.cancelFor('peer-1');
    runner.cancelFor('peer-2');
    expect(runner.trackedClients()).toBe(2);
    runner.forget('peer-1');
    expect(runner.trackedClients()).toBe(1);
    runner.forget('peer-2');
    expect(runner.trackedClients()).toBe(0);
  });
});
