// Default ErrorClassifier — minimal heuristics for M2.
// Real per-server-software detection (ElectrumX vs Fulcrum vs electrs) lands
// in M4 where we own the strict-config Docker stack to verify each pattern.

import {
  ProtocolError,
  RpcError,
  TimeoutError,
  TransportError,
  type ErrorClassifier,
  type ErrorKind,
} from './types.js';

export const defaultClassifier: ErrorClassifier = {
  classify(error): ErrorKind {
    if (error instanceof TimeoutError) return 'timeout';
    if (error instanceof TransportError) return 'transport';
    if (error instanceof ProtocolError) return 'protocol';
    if (error instanceof RpcError) {
      const msg = (error.message ?? '').toLowerCase();
      // TODO(M4): per-server-software classifiers. The 'banned' substring
      // match below will false-positive on legitimate RPC errors that
      // mention bans (e.g. address-banned errors from some servers); the
      // M4 strict-config Docker stack will let us swap this for tighter
      // per-server heuristics keyed on `ctx.serverSoftware`.
      if (
        msg.includes('excessive resource') ||
        msg.includes('rate limit') ||
        msg.includes('banned') ||
        msg.includes('too many')
      ) {
        return 'rate-limit';
      }
      return 'rpc-error';
    }
    return 'unknown';
  },
};
