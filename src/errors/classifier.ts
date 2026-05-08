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
