// Error types + ErrorClassifier interface. Default classifier ships in M4 (errors/classifier.ts).

export type ErrorKind =
  | 'rate-limit'
  | 'timeout'
  | 'transport'
  | 'protocol'
  | 'rpc-error'
  | 'unknown';

export interface ClassifyContext {
  serverSoftware?: string;
  method: string;
  durationMs: number;
}

export interface ErrorClassifier {
  classify(err: unknown, ctx: ClassifyContext): ErrorKind;
}

export class SuspendedError extends Error {
  override readonly name = 'SuspendedError';
}

export class TimeoutError extends Error {
  override readonly name = 'TimeoutError';
}

export class TransportError extends Error {
  override readonly name = 'TransportError';
  override readonly cause: unknown;
  constructor(message: string, cause?: unknown) {
    super(message);
    this.cause = cause;
  }
}

export class RpcError extends Error {
  override readonly name = 'RpcError';
  readonly code: number;
  readonly data?: unknown;
  constructor(message: string, code: number, data?: unknown) {
    super(message);
    this.code = code;
    if (data !== undefined) this.data = data;
  }
}

export class ProtocolError extends Error {
  override readonly name = 'ProtocolError';
}
