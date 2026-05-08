import { describe, expect, it } from 'vitest';

import { composeClassifier, defaultClassifier } from '../../../src/errors/classifier.js';
import {
  ProtocolError,
  RpcError,
  TimeoutError,
  TransportError,
  type ClassifyContext,
} from '../../../src/errors/types.js';

const baseCtx = (software?: string): ClassifyContext => ({
  method: 'blockchain.scripthash.get_balance',
  durationMs: 100,
  ...(software !== undefined ? { serverSoftware: software } : {}),
});

describe('defaultClassifier — typed errors', () => {
  it('classifies TimeoutError as timeout', () => {
    expect(defaultClassifier.classify(new TimeoutError('slow'), baseCtx())).toBe('timeout');
  });

  it('classifies TransportError as transport', () => {
    expect(defaultClassifier.classify(new TransportError('dead'), baseCtx())).toBe('transport');
  });

  it('classifies ProtocolError as protocol', () => {
    expect(defaultClassifier.classify(new ProtocolError('bad frame'), baseCtx())).toBe('protocol');
  });

  it('classifies generic RpcError as rpc-error', () => {
    expect(defaultClassifier.classify(new RpcError('bad txid', 1), baseCtx())).toBe('rpc-error');
  });
});

describe('defaultClassifier — ElectrumX rate-limit', () => {
  it('detects "excessive resource usage" on RpcError', () => {
    expect(
      defaultClassifier.classify(
        new RpcError('Excessive resource usage; banned', 1),
        baseCtx('ElectrumX 1.16.0'),
      ),
    ).toBe('rate-limit');
  });

  it('detects "too many requests"', () => {
    expect(
      defaultClassifier.classify(
        new RpcError('too many requests in window', 1),
        baseCtx('ElectrumX 1.16.0'),
      ),
    ).toBe('rate-limit');
  });

  it('does not false-positive on routine errors', () => {
    expect(
      defaultClassifier.classify(new RpcError('history too long', 1), baseCtx('ElectrumX 1.16.0')),
    ).toBe('rpc-error');
  });
});

describe('defaultClassifier — Fulcrum rate-limit', () => {
  it('detects "excessive resource usage; bye!"', () => {
    expect(
      defaultClassifier.classify(
        new RpcError('excessive resource usage; bye!', 1),
        baseCtx('Fulcrum 1.10.0'),
      ),
    ).toBe('rate-limit');
  });

  it('detects "too many concurrent"', () => {
    expect(
      defaultClassifier.classify(
        new RpcError('too many concurrent connections', 1),
        baseCtx('Fulcrum 1.10.0'),
      ),
    ).toBe('rate-limit');
  });

  it('detects literal "banned"', () => {
    expect(
      defaultClassifier.classify(
        new RpcError('client banned for 60s', 1),
        baseCtx('Fulcrum 1.10.0'),
      ),
    ).toBe('rate-limit');
  });
});

describe('defaultClassifier — electrs rate-limit', () => {
  it('detects "rate limit" message', () => {
    expect(
      defaultClassifier.classify(new RpcError('rate limit hit', 1), baseCtx('electrs 0.10.2')),
    ).toBe('rate-limit');
  });

  it('does not match "excessive" alone (electrs uses different vocabulary)', () => {
    expect(
      defaultClassifier.classify(new RpcError('excessive history', 1), baseCtx('electrs 0.10.2')),
    ).toBe('rpc-error');
  });
});

describe('defaultClassifier — unknown serverSoftware (generic table)', () => {
  it('matches the broad generic substrings when software is undefined', () => {
    expect(defaultClassifier.classify(new RpcError('You have been banned', 1), baseCtx())).toBe(
      'rate-limit',
    );
    expect(defaultClassifier.classify(new RpcError('excessive load', 1), baseCtx())).toBe(
      'rate-limit',
    );
  });
});

describe('defaultClassifier — untyped network errors', () => {
  it('classifies ECONNRESET as transport', () => {
    const e = Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' });
    expect(defaultClassifier.classify(e, baseCtx())).toBe('transport');
  });

  it('classifies "socket hang up" as transport', () => {
    expect(defaultClassifier.classify(new Error('socket hang up'), baseCtx())).toBe('transport');
  });

  it('classifies WS abnormal closure messages as transport', () => {
    expect(
      defaultClassifier.classify(
        new Error('WebSocket was closed: 1006 abnormal closure'),
        baseCtx(),
      ),
    ).toBe('transport');
  });

  it('classifies an arbitrary non-Error message as unknown', () => {
    expect(defaultClassifier.classify('something blew up', baseCtx())).toBe('unknown');
  });

  it('classifies a non-typed rate-limit message before transport', () => {
    // Some servers send a banner-style ban message right before slamming
    // the socket; we want rate-limit, not transport.
    expect(
      defaultClassifier.classify(
        new Error('excessive resource usage; bye!'),
        baseCtx('Fulcrum 1.10.0'),
      ),
    ).toBe('rate-limit');
  });
});

describe('composeClassifier', () => {
  it('lets overrides win when they return a kind', () => {
    const composed = composeClassifier([
      (err) => {
        if (err instanceof Error && err.message === 'custom-ban') return 'rate-limit';
        return undefined;
      },
    ]);
    expect(composed.classify(new Error('custom-ban'), baseCtx())).toBe('rate-limit');
  });

  it('falls through to default when overrides return undefined', () => {
    const composed = composeClassifier([() => undefined]);
    expect(composed.classify(new TimeoutError('slow'), baseCtx())).toBe('timeout');
  });

  it('honors override order — first match wins', () => {
    const composed = composeClassifier([
      () => 'rate-limit',
      () => 'transport', // never reached
    ]);
    expect(composed.classify(new Error('whatever'), baseCtx())).toBe('rate-limit');
  });
});
