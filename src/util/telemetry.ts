// Per-client telemetry accumulator. Owned by Manager (one per client).
// Tracks: latency EMA + ring buffer for p50/p95, success/error counts, last-
// outcome timestamps, consecutive-errors. Cheap (no allocations on the hot
// path beyond the bounded samples array).

import type { ErrorKind } from '../errors/types.js';
import type { Telemetry } from '../client.js';

const MAX_SAMPLES = 32;
const ALPHA = 0.2;

export class TelemetryAccumulator {
  private readonly samples: number[] = [];
  private ema = 0;
  private successCount = 0;
  private errorCount = 0;
  private lastSuccessAt: number | undefined;
  private lastErrorKind: ErrorKind | undefined;
  private lastErrorAt: number | undefined;
  private consecutiveErrors = 0;

  recordSuccess(latencyMs: number, now: number): void {
    this.pushSample(latencyMs);
    this.ema = this.ema === 0 ? latencyMs : this.ema * (1 - ALPHA) + latencyMs * ALPHA;
    this.successCount++;
    this.lastSuccessAt = now;
    this.consecutiveErrors = 0;
  }

  recordError(kind: ErrorKind, latencyMs: number, now: number): void {
    this.pushSample(latencyMs);
    // Latency on errored requests still informs the EMA: a 10s timeout on
    // server X should make X look slow next time policy.pick runs.
    this.ema = this.ema === 0 ? latencyMs : this.ema * (1 - ALPHA) + latencyMs * ALPHA;
    this.errorCount++;
    this.lastErrorKind = kind;
    this.lastErrorAt = now;
    this.consecutiveErrors++;
  }

  /**
   * Record an error that has no associated request latency (e.g. a
   * malformed inbound frame). Updates counts / lastKind / consecutive but
   * leaves the latency EMA and percentile samples untouched — a zero
   * sample would drag the EMA down and make a broken server look fast.
   */
  recordErrorNoLatency(kind: ErrorKind, now: number): void {
    this.errorCount++;
    this.lastErrorKind = kind;
    this.lastErrorAt = now;
    this.consecutiveErrors++;
  }

  latency(): Telemetry['latency'] {
    if (this.samples.length === 0) {
      return { ema: 0, p50: 0, p95: 0, samples: 0 };
    }
    const sorted = [...this.samples].sort((a, b) => a - b);
    return {
      ema: this.ema,
      p50: quantile(sorted, 0.5),
      p95: quantile(sorted, 0.95),
      samples: sorted.length,
    };
  }

  successSnapshot(): Telemetry['success'] {
    return {
      count: this.successCount,
      ...(this.lastSuccessAt !== undefined ? { lastAt: this.lastSuccessAt } : {}),
    };
  }

  errorsSnapshot(): Telemetry['errors'] {
    const total = this.successCount + this.errorCount;
    const rate = total === 0 ? 0 : this.errorCount / total;
    return {
      rate,
      consecutive: this.consecutiveErrors,
      ...(this.lastErrorKind !== undefined ? { lastKind: this.lastErrorKind } : {}),
      ...(this.lastErrorAt !== undefined ? { lastAt: this.lastErrorAt } : {}),
    };
  }

  private pushSample(v: number): void {
    this.samples.push(v);
    if (this.samples.length > MAX_SAMPLES) this.samples.shift();
  }
}

function quantile(sortedAsc: readonly number[], q: number): number {
  if (sortedAsc.length === 0) return 0;
  const idx = Math.floor((sortedAsc.length - 1) * q);
  return sortedAsc[idx]!;
}
