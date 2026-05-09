// Default ErrorClassifier — per-server-software heuristics for the three
// Electrum implementations we ship support for: ElectrumX, Fulcrum, electrs.
//
// The classifier maps an arbitrary `unknown` thrown out of a wire call into a
// stable `ErrorKind`. The kind drives manager decisions: `rate-limit` cools
// the client down for `cooldownMs`; `transport` / `timeout` retry on a
// different client; `rpc-error` / `protocol` surface as caller-visible
// failures without retry; `unknown` is conservatively treated as transient.
//
// Server-software detection is best-effort: `ctx.serverSoftware` is populated
// from the `server.version` handshake when the client connects. The
// per-software helpers below also accept input where `serverSoftware` is
// unset (early-handshake errors) and fall back to substring heuristics that
// work across all three implementations.

import {
  ProtocolError,
  RpcError,
  TimeoutError,
  TransportError,
  type ClassifyContext,
  type ErrorClassifier,
  type ErrorKind,
} from './types.js';

// --- Per-software substring tables -----------------------------------------

/**
 * Substrings ElectrumX (via aiorpcx) uses when rejecting a client for
 * resource reasons. The canonical phrase is `"excessive resource usage"`
 * — emitted by aiorpcx's `ExcessiveSessionCostError` path with JSON-RPC
 * code -101. Source:
 *   - aiorpcx `aiorpcx/session.py` raises
 *     `RPCError(JSONRPC.EXCESSIVE_RESOURCE_USAGE, 'excessive resource usage')`.
 *   - https://github.com/spesmilo/electrumx — `electrumx/server/session.py`.
 *
 * Note: the literal `"; bye!"` suffix some older docs reference is a
 * client-side wallet rendering artifact, not part of the wire payload.
 */
const ELECTRUMX_RATE_LIMIT_SUBSTRINGS = [
  'excessive resource usage',
  'excessive request',
  'too many requests',
  'request limit',
  'session timed out',
];

/**
 * Fulcrum's rate-limit / ban payloads. The canonical message emitted by
 * `impl_generic_subscribe` when `max_subs_per_ip` is exceeded is
 * `"Subscription limit reached"` (JSON-RPC code
 * `RPC::Code_App_LimitExceeded`). Other paths (`max_clients_per_ip`,
 * `Excessive errors...disconnecting`) drop the socket without an RPC
 * payload — those surface as `transport`, not `rate-limit`.
 *
 * Source: https://github.com/cculianu/Fulcrum — `src/Servers.cpp`
 * (`impl_generic_subscribe` / `impl_generic_handle_subs_limitreached_exc`).
 */
const FULCRUM_RATE_LIMIT_SUBSTRINGS = ['subscription limit reached', 'banned'];

/**
 * electrs has the loosest rate-limit signaling — typically just closes the
 * socket. When it does emit messages they tend to be terse. Source:
 * https://github.com/romanz/electrs — `src/electrum.rs`.
 */
const ELECTRS_RATE_LIMIT_SUBSTRINGS = ['too many requests', 'rate limit', 'connection rejected'];

/**
 * Common across all three implementations (used when `serverSoftware`
 * is unknown — e.g. before / instead of an explicit `server.version`
 * handshake). The strings are specific enough that false-positives on
 * unrelated RPC errors are negligible.
 */
const GENERIC_RATE_LIMIT_SUBSTRINGS = [
  'rate limit',
  'rate-limit',
  'banned',
  'excessive',
  'too many',
  'subscription limit reached', // Fulcrum
];

// --- Helpers ---------------------------------------------------------------

function lowerMessage(error: unknown): string {
  if (error instanceof Error) return (error.message ?? '').toLowerCase();
  return String(error).toLowerCase();
}

function matchesAny(haystack: string, needles: readonly string[]): boolean {
  for (const n of needles) {
    if (haystack.includes(n)) return true;
  }
  return false;
}

/**
 * Pick the rate-limit substring set for the named software. Anything we
 * don't recognize falls back to the generic list (which is a strict superset
 * of the three vendor lists by design — we'd rather false-positive a ban
 * than miss one and let the client keep hammering a server that's already
 * cutting it off).
 */
function rateLimitSubstrings(software: string | undefined): readonly string[] {
  if (software === undefined) return GENERIC_RATE_LIMIT_SUBSTRINGS;
  const sw = software.toLowerCase();
  if (sw.includes('electrumx')) return ELECTRUMX_RATE_LIMIT_SUBSTRINGS;
  if (sw.includes('fulcrum')) return FULCRUM_RATE_LIMIT_SUBSTRINGS;
  if (sw.includes('electrs')) return ELECTRS_RATE_LIMIT_SUBSTRINGS;
  return GENERIC_RATE_LIMIT_SUBSTRINGS;
}

/**
 * Standard Node / browser network-error spellings that mean "the transport
 * died" — connection reset, timed out, refused, host unreachable, plus the
 * generic WebSocket abnormal-close codes. We don't enumerate WS close codes
 * exhaustively because TransportError already wraps them; the substring
 * check is for raw `Error`s that bypass our wrapping (e.g. an `Error` from
 * `globalThis.WebSocket` listeners).
 */
const TRANSPORT_SUBSTRINGS = [
  'econnreset',
  'econnrefused',
  'econnaborted',
  'etimedout',
  'ehostunreach',
  'enetunreach',
  'epipe',
  'socket hang up',
  'websocket is not open',
  'websocket was closed',
  'connection closed',
  'abnormal closure', // WS 1006
  'going away', // WS 1001
];

// --- Default classifier ----------------------------------------------------

export const defaultClassifier: ErrorClassifier = {
  classify(error, ctx: ClassifyContext): ErrorKind {
    // Typed errors first — these come from our own framing / transport / RPC
    // layers and are unambiguous.
    if (error instanceof TimeoutError) return 'timeout';
    if (error instanceof TransportError) return 'transport';
    if (error instanceof ProtocolError) return 'protocol';

    if (error instanceof RpcError) {
      const msg = lowerMessage(error);
      if (matchesAny(msg, rateLimitSubstrings(ctx.serverSoftware))) {
        return 'rate-limit';
      }
      return 'rpc-error';
    }

    // Untyped error: fall back to message inspection. Rate-limit checks come
    // before transport because some servers send a final RPC-shaped error
    // ("excessive resource usage", "Subscription limit reached") immediately
    // before slamming the socket closed; we want to ban-list them, not just
    // retry on transport.
    const msg = lowerMessage(error);
    if (matchesAny(msg, rateLimitSubstrings(ctx.serverSoftware))) {
      return 'rate-limit';
    }
    if (matchesAny(msg, TRANSPORT_SUBSTRINGS)) return 'transport';
    return 'unknown';
  },
};

// --- Combinator for caller customization -----------------------------------

/**
 * Compose user-supplied overrides with the default classifier. The first
 * override that returns a non-`undefined` `ErrorKind` wins; otherwise we fall
 * through to `defaultClassifier`. Lets callers add domain-specific patterns
 * (e.g. a private-server software banner) without rewriting the whole
 * classifier.
 */
export function composeClassifier(
  overrides: ReadonlyArray<(err: unknown, ctx: ClassifyContext) => ErrorKind | undefined>,
  base: ErrorClassifier = defaultClassifier,
): ErrorClassifier {
  return {
    classify(err, ctx) {
      for (const o of overrides) {
        const k = o(err, ctx);
        if (k !== undefined) return k;
      }
      return base.classify(err, ctx);
    },
  };
}
