import { describe, expect, it } from 'vitest';

import { TelemetryAccumulator } from '../../../src/util/telemetry.js';

describe('TelemetryAccumulator', () => {
  it('records success counts and timestamp', () => {
    const t = new TelemetryAccumulator();
    t.recordSuccess(50, 1000);
    t.recordSuccess(100, 1100);
    expect(t.successSnapshot()).toEqual({ count: 2, lastAt: 1100 });
  });

  it('seeds EMA on first sample then smooths', () => {
    const t = new TelemetryAccumulator();
    t.recordSuccess(100, 0);
    expect(t.latency().ema).toBe(100);
    t.recordSuccess(200, 1);
    // ema = 100*(1-0.2) + 200*0.2 = 120
    expect(t.latency().ema).toBeCloseTo(120, 5);
  });

  it('computes p50 / p95 from the bounded sample buffer', () => {
    const t = new TelemetryAccumulator();
    for (let i = 1; i <= 20; i++) t.recordSuccess(i * 10, i);
    const lat = t.latency();
    expect(lat.samples).toBe(20);
    expect(lat.p50).toBe(100); // index 9 of [10,20,...,200]
    expect(lat.p95).toBe(190); // index 18
  });

  it('resets consecutive errors on success', () => {
    const t = new TelemetryAccumulator();
    t.recordError('timeout', 50, 1);
    t.recordError('timeout', 50, 2);
    expect(t.errorsSnapshot().consecutive).toBe(2);
    t.recordSuccess(50, 3);
    expect(t.errorsSnapshot().consecutive).toBe(0);
  });

  it('reports error rate over total attempts', () => {
    const t = new TelemetryAccumulator();
    t.recordSuccess(50, 1);
    t.recordSuccess(50, 2);
    t.recordSuccess(50, 3);
    t.recordError('timeout', 50, 4);
    expect(t.errorsSnapshot().rate).toBeCloseTo(0.25, 5);
  });

  it('records lastKind and lastAt on error', () => {
    const t = new TelemetryAccumulator();
    t.recordError('rate-limit', 30, 9999);
    const snap = t.errorsSnapshot();
    expect(snap.lastKind).toBe('rate-limit');
    expect(snap.lastAt).toBe(9999);
  });

  it('caps the sample buffer at 32', () => {
    const t = new TelemetryAccumulator();
    for (let i = 0; i < 100; i++) t.recordSuccess(i, i);
    expect(t.latency().samples).toBe(32);
  });
});
