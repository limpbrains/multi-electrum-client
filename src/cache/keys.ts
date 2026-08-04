// Cache key namespacing + cacheable-method registry.
//
// Cache is consulted only for methods on this static allow-list. Each entry
// names a bucket, extracts an id from the wire params, and reports the
// finalized-height the manager should gate the write on. The manager owns
// the actual JSON serialization and the cacheable-vs-not decision; this
// module is pure data.
//
// The bucket / key shape is `et:<network>:v1:<bucket>:<id>`. The `v1` lets a
// future cache-format change land without poisoning existing entries (bump
// to `v2`, old entries miss + die naturally on the user's eviction policy).

import type { Network } from '../protocol/types.js';

/** Top-level prefix for every key. Keep stable — bumping breaks user caches. */
const PREFIX = 'et';
const VERSION = 'v1';

/** Build a fully-qualified cache key from its three parts. */
export function buildKey(network: Network, bucket: string, id: string): string {
  return `${PREFIX}:${network}:${VERSION}:${bucket}:${id}`;
}

/**
 * Per-method extraction: given the wire params, return the bucket + id parts
 * plus the height that must be `<= tip - finalizedConfs` for the write to be
 * safe. Returns `null` when the params don't satisfy the cache contract for
 * that method (e.g. invalid types, missing height).
 *
 * MVP allow-list:
 *  - `blockchain.block.header` — bucket `hdr`, id is height in hex.
 *  - `blockchain.transaction.get_merkle` — bucket `mrk`, id is `txid:hexHeight`.
 *
 * Notably *not* cached: raw `blockchain.transaction.get`. The wire response
 * is just hex; it doesn't carry confirmation depth, so we can't gate the
 * write without a separate height side-channel. A future verbose-form pass
 * (post-M4) will likely cache `transaction.get` too, keyed off
 * `result.confirmations`.
 */
export interface CacheableSpec {
  bucket: string;
  id: string;
  /** Block height that must be finalized for the write to land. */
  finalityHeight: number;
}

type CacheExtractor = (params: readonly unknown[]) => CacheableSpec | null;

const heightToHex = (h: number): string => h.toString(16);

const isValidHeight = (h: unknown): h is number =>
  typeof h === 'number' && Number.isInteger(h) && h >= 0;

const CACHEABLE: Record<string, CacheExtractor> = {
  'blockchain.block.header': (params) => {
    const height = params[0];
    if (!isValidHeight(height)) return null;
    return { bucket: 'hdr', id: heightToHex(height), finalityHeight: height };
  },
  'blockchain.transaction.get_merkle': (params) => {
    const txid = params[0];
    const height = params[1];
    if (typeof txid !== 'string' || txid.length === 0) return null;
    if (!isValidHeight(height)) return null;
    return {
      bucket: 'mrk',
      id: `${txid}:${heightToHex(height)}`,
      finalityHeight: height,
    };
  },
};

/**
 * Resolve a method + params pair to a cache spec, or `null` if the call is
 * not cacheable. Manager calls this once per request before enqueuing.
 */
export function cacheSpec(method: string, params: readonly unknown[]): CacheableSpec | null {
  // `method` is caller-controlled: hasOwn guard keeps prototype keys
  // ('constructor', 'toString', …) from resolving to inherited functions.
  if (!Object.hasOwn(CACHEABLE, method)) return null;
  return CACHEABLE[method]!(params);
}
