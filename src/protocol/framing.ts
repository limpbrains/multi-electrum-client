// JSON-RPC 2.0 framing for Electrum.
//
// We accept a slightly lax inbound shape (jsonrpc field optional, params optional on
// notifications) because some servers omit it, but always emit canonical 2.0 outbound.
// Each WebSocket message holds exactly one root: an object (single message) or an
// array (batch). TCP/TLS newline-framing arrives in M6 as a separate parser layer.

import { ProtocolError } from '../errors/types.js';

export type JsonRpcId = number | string | null;

export interface JsonRpcRequest {
  jsonrpc: '2.0';
  method: string;
  params: readonly unknown[];
  id: JsonRpcId;
}

export interface JsonRpcSuccessResponse {
  id: JsonRpcId;
  result: unknown;
}

export interface JsonRpcErrorResponse {
  id: JsonRpcId;
  error: { code: number; message: string; data?: unknown };
}

export type JsonRpcResponse = JsonRpcSuccessResponse | JsonRpcErrorResponse;

export interface JsonRpcNotification {
  method: string;
  params: readonly unknown[];
}

export type JsonRpcMessage = JsonRpcResponse | JsonRpcNotification;

export function encodeRequest(req: JsonRpcRequest): string {
  return JSON.stringify({
    jsonrpc: '2.0',
    method: req.method,
    params: req.params,
    id: req.id,
  });
}

export function encodeBatch(reqs: readonly JsonRpcRequest[]): string {
  if (reqs.length === 0) {
    throw new ProtocolError('cannot encode empty batch');
  }
  return JSON.stringify(
    reqs.map((req) => ({
      jsonrpc: '2.0',
      method: req.method,
      params: req.params,
      id: req.id,
    })),
  );
}

export function decodeMessage(text: string): JsonRpcMessage | JsonRpcMessage[] {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (err) {
    throw new ProtocolError(`invalid JSON: ${(err as Error).message}`);
  }
  if (Array.isArray(parsed)) {
    return parsed.map((m) => validate(m));
  }
  if (parsed === null || typeof parsed !== 'object') {
    throw new ProtocolError(`expected JSON object or array, got ${describe(parsed)}`);
  }
  return validate(parsed);
}

function validate(raw: unknown): JsonRpcMessage {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new ProtocolError('expected JSON-RPC message object');
  }
  const obj = raw as Record<string, unknown>;

  // Notification: has `method`, no `id` (server -> client push).
  if (typeof obj['method'] === 'string' && !('id' in obj)) {
    return {
      method: obj['method'],
      params: Array.isArray(obj['params']) ? (obj['params'] as readonly unknown[]) : [],
    };
  }

  // Response: has `id`, plus `result` xor `error`.
  if ('id' in obj) {
    if ('error' in obj) {
      const e = obj['error'];
      if (e === null || typeof e !== 'object') {
        throw new ProtocolError('error response: error is not an object');
      }
      const err = e as Record<string, unknown>;
      if (typeof err['code'] !== 'number' || typeof err['message'] !== 'string') {
        throw new ProtocolError('error response: malformed code or message');
      }
      const out: JsonRpcErrorResponse = {
        id: obj['id'] as JsonRpcId,
        error: { code: err['code'], message: err['message'] },
      };
      if (err['data'] !== undefined) out.error.data = err['data'];
      return out;
    }
    if ('result' in obj) {
      return {
        id: obj['id'] as JsonRpcId,
        result: obj['result'],
      };
    }
    throw new ProtocolError('response missing both result and error');
  }

  throw new ProtocolError('unrecognized JSON-RPC message shape');
}

function describe(v: unknown): string {
  if (v === null) return 'null';
  return typeof v;
}
