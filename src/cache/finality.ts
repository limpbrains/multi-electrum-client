// Finality-gated cache: read-on-call, write-on-success, only for entries
// whose underlying data is past `finalizedConfs` confirmations from the
// current chain tip. All functions are pure given a `CacheStore`,
// `network`, and the current `tipHeight` — the manager owns the
// `tipHeight` mutable cell and passes it in.
//
// Why a module of standalone functions instead of a class: the manager
// already owns `tipHeight`, the `CacheStore`, the `network`, and the
// emit-error path. Wrapping these into a class would just re-thread the
// same dependencies through a constructor; the call sites stay equally
// noisy.
//
// Error policy: every read / write failure surfaces via the injected
// `onError` callback. The cache is caller-supplied (`CacheStore`), so
// neither failure must break the wire request — corrupt-entry deletes
// are best-effort, serialization failures skip the write, and adapter
// `set` rejections are caught here so the caller never has to layer a
// second `.catch` on top.

import type { CacheStore } from './types.js';
import { buildKey, cacheSpec, type CacheableSpec } from './keys.js';
import type { Network } from '../protocol/types.js';

/**
 * Returns the cache spec for `(method, params)` if the method is on the
 * cacheable allow-list, else `null`. Caller must already have decided
 * to consult the cache (i.e. checked `bypassCache` and that a store is
 * wired up); this is a thin wrapper that exists only so manager doesn't
 * import `cacheSpec` from `./keys.js` directly.
 */
export function findCacheSpec(method: string, params: readonly unknown[]): CacheableSpec | null {
  return cacheSpec(method, params);
}

/**
 * True iff `height` is at least `finalizedConfs` blocks behind the
 * current tip. Returns `false` while `tipHeight` is unknown — manager
 * treats that as "don't write yet" rather than caching potentially
 * non-final data.
 */
export function isFinalized(
  height: number,
  tipHeight: number | undefined,
  finalizedConfs: number,
): boolean {
  if (tipHeight === undefined) return false;
  return tipHeight - height >= finalizedConfs;
}

/**
 * Read a cached value or return the `MISS` sentinel. Adapter / parse
 * failures surface via `onError` and also resolve to `MISS` (the
 * request falls through to the wire). Corrupt entries are best-effort
 * deleted so a follow-up read doesn't hit the same bad row.
 *
 * Returns `unknown` on hit; the `MISS` sentinel is a unique symbol so
 * callers can disambiguate `undefined` (a legitimate cached value) from
 * "no entry / read failed" without sentinel collision risk.
 */
export const MISS = Symbol('cache-miss');

export async function readFromCache(
  cache: CacheStore,
  network: Network,
  spec: CacheableSpec,
  onError: (e: unknown) => void,
): Promise<unknown | typeof MISS> {
  const key = buildKey(network, spec.bucket, spec.id);
  let raw: string | null;
  try {
    raw = await cache.get(key);
  } catch (e) {
    onError(e);
    return MISS;
  }
  if (raw === null) return MISS;
  try {
    return JSON.parse(raw);
  } catch (e) {
    onError(e);
    void cache.del(key).catch(() => undefined);
    return MISS;
  }
}

/**
 * Write `value` under `spec`. Catches both serialization failures and
 * adapter `set` rejections internally; both routes call `onError` and
 * resolve. Caller does NOT need to layer a `.catch` on top of the
 * returned promise — every failure is already surfaced once via the
 * callback.
 */
export async function writeToCache(
  cache: CacheStore,
  network: Network,
  spec: CacheableSpec,
  value: unknown,
  onError: (e: unknown) => void,
): Promise<void> {
  const key = buildKey(network, spec.bucket, spec.id);
  let serialized: string;
  try {
    serialized = JSON.stringify(value);
  } catch (e) {
    onError(e);
    return;
  }
  try {
    await cache.set(key, serialized);
  } catch (e) {
    onError(e);
  }
}
