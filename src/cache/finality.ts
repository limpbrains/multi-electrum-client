// Finality-gated cache: read-on-call, write-on-success, only for entries
// whose underlying data is past `finalizedConfs` confirmations from the
// current chain tip. All four functions are pure given a `CacheStore`,
// `network`, and the current `tipHeight` — the manager owns the
// `tipHeight` mutable cell and passes it in.
//
// Why a module of standalone functions instead of a class: the manager
// already owns `tipHeight`, the `CacheStore`, the `network`, and the
// emit-error path. Wrapping these into a class would just re-thread the
// same dependencies through a constructor; the call sites stay equally
// noisy. Functions keep the cache layer obviously stateless and let the
// manager mutate `tipHeight` from the headers subscription without
// reaching through an extra abstraction.
//
// Errors here surface via the injected `onError` callback. The cache is
// caller-supplied (`CacheStore` interface) — any read / write failure
// must not break the request, so corrupt-entry deletes are best-effort
// and serialization failures skip the write.

import type { CacheStore } from './types.js';
import { buildKey, cacheSpec, type CacheableSpec } from './keys.js';
import type { Network } from '../protocol/types.js';

export type CacheSpec = CacheableSpec;

/**
 * Look up the cache spec for `(method, params)` if a cache is wired up
 * and the method is cacheable. Returns `null` for either "no cache" or
 * "method not on the allow-list" — both branches collapse into "skip
 * the cache".
 */
export function findCacheSpec(
  cache: CacheStore | undefined,
  method: string,
  params: readonly unknown[],
): CacheSpec | null {
  if (!cache) return null;
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
 * Read a cached value or return `undefined`. Adapter / parse failures
 * surface via `onError` and resolve to `undefined` (the request falls
 * through to the wire). Corrupt entries are best-effort deleted so a
 * follow-up read doesn't hit the same bad row.
 */
export async function readFromCache(
  cache: CacheStore,
  network: Network,
  spec: CacheSpec,
  onError: (e: unknown) => void,
): Promise<unknown | undefined> {
  const key = buildKey(network, spec.bucket, spec.id);
  let raw: string | null;
  try {
    raw = await cache.get(key);
  } catch (e) {
    onError(e);
    return undefined;
  }
  if (raw === null) return undefined;
  try {
    return JSON.parse(raw);
  } catch (e) {
    onError(e);
    void cache.del(key).catch(() => undefined);
    return undefined;
  }
}

/**
 * Write `value` under `spec`. Non-serializable values (circular refs,
 * `BigInt`) are skipped rather than thrown — a failed write means the
 * next call refetches, which is the same outcome as the cache being
 * empty. Adapter `set` errors propagate to the caller (the manager
 * already wraps the call site in `.catch(emit('error', ...))`).
 */
export async function writeToCache(
  cache: CacheStore,
  network: Network,
  spec: CacheSpec,
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
  await cache.set(key, serialized);
}
