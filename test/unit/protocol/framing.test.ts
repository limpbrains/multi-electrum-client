import { describe, expect, it } from 'vitest';

import { ProtocolError } from '../../../src/errors/types.js';
import {
  decodeMessage,
  encodeBatch,
  encodeRequest,
  type JsonRpcErrorResponse,
  type JsonRpcMessage,
  type JsonRpcNotification,
  type JsonRpcRequest,
  type JsonRpcSuccessResponse,
} from '../../../src/protocol/framing.js';

describe('encodeRequest', () => {
  it('emits canonical JSON-RPC 2.0 request', () => {
    const req: JsonRpcRequest = { jsonrpc: '2.0', method: 'server.ping', params: [], id: 1 };
    expect(encodeRequest(req)).toBe('{"jsonrpc":"2.0","method":"server.ping","params":[],"id":1}');
  });

  it('preserves param order', () => {
    const req: JsonRpcRequest = {
      jsonrpc: '2.0',
      method: 'blockchain.transaction.get',
      params: ['abc', true],
      id: 2,
    };
    expect(encodeRequest(req)).toContain('"params":["abc",true]');
  });

  it('always emits jsonrpc:"2.0" even if caller omits it', () => {
    const text = encodeRequest({
      jsonrpc: '2.0',
      method: 'server.ping',
      params: [],
      id: 7,
    });
    expect(JSON.parse(text)).toEqual({
      jsonrpc: '2.0',
      method: 'server.ping',
      params: [],
      id: 7,
    });
  });
});

describe('encodeBatch', () => {
  it('emits a JSON array of requests', () => {
    const batch: JsonRpcRequest[] = [
      { jsonrpc: '2.0', method: 'server.ping', params: [], id: 1 },
      { jsonrpc: '2.0', method: 'server.version', params: ['1.0', '1.4'], id: 2 },
    ];
    const parsed = JSON.parse(encodeBatch(batch));
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed).toHaveLength(2);
    expect(parsed[0].id).toBe(1);
    expect(parsed[1].id).toBe(2);
    expect(parsed[1].method).toBe('server.version');
  });

  it('rejects empty batch', () => {
    expect(() => encodeBatch([])).toThrow(ProtocolError);
  });
});

describe('decodeMessage — single message', () => {
  it('parses a successful response', () => {
    const msg = decodeMessage('{"jsonrpc":"2.0","id":1,"result":null}') as JsonRpcSuccessResponse;
    expect(msg.id).toBe(1);
    expect(msg.result).toBeNull();
  });

  it('parses an error response', () => {
    const text = '{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"boom"}}';
    const msg = decodeMessage(text) as JsonRpcErrorResponse;
    expect(msg.id).toBe(1);
    expect(msg.error.code).toBe(-32603);
    expect(msg.error.message).toBe('boom');
  });

  it('parses an error response with data', () => {
    const text = '{"jsonrpc":"2.0","id":1,"error":{"code":2,"message":"x","data":{"detail":"y"}}}';
    const msg = decodeMessage(text) as JsonRpcErrorResponse;
    expect(msg.error.data).toEqual({ detail: 'y' });
  });

  it('parses a notification (no id)', () => {
    const text =
      '{"jsonrpc":"2.0","method":"blockchain.headers.subscribe","params":[{"height":1}]}';
    const msg = decodeMessage(text) as JsonRpcNotification;
    expect(msg.method).toBe('blockchain.headers.subscribe');
    expect(msg.params).toEqual([{ height: 1 }]);
  });

  it('accepts response without jsonrpc field (lax)', () => {
    const msg = decodeMessage('{"id":1,"result":42}') as JsonRpcSuccessResponse;
    expect(msg.result).toBe(42);
  });

  it('accepts notification with missing params (defaults to empty array)', () => {
    const msg = decodeMessage('{"method":"server.ping"}') as JsonRpcNotification;
    expect(msg.params).toEqual([]);
  });
});

describe('decodeMessage — batch', () => {
  it('parses a batch response', () => {
    const text = '[{"jsonrpc":"2.0","id":1,"result":1},{"jsonrpc":"2.0","id":2,"result":2}]';
    const msgs = decodeMessage(text) as JsonRpcMessage[];
    expect(Array.isArray(msgs)).toBe(true);
    expect(msgs).toHaveLength(2);
    const ids = msgs.map((m) => ('id' in m ? m.id : null));
    expect(ids).toEqual([1, 2]);
  });

  it('parses a batch with mixed success and error', () => {
    const text = '[{"id":1,"result":"ok"},{"id":2,"error":{"code":1,"message":"bad"}}]';
    const msgs = decodeMessage(text) as JsonRpcMessage[];
    expect((msgs[0] as JsonRpcSuccessResponse).result).toBe('ok');
    expect((msgs[1] as JsonRpcErrorResponse).error.code).toBe(1);
  });
});

describe('decodeMessage — protocol errors', () => {
  it('throws ProtocolError on invalid JSON', () => {
    expect(() => decodeMessage('{not-json')).toThrow(ProtocolError);
  });

  it('throws ProtocolError on non-object/non-array root', () => {
    expect(() => decodeMessage('"plain string"')).toThrow(ProtocolError);
    expect(() => decodeMessage('42')).toThrow(ProtocolError);
    expect(() => decodeMessage('null')).toThrow(ProtocolError);
  });

  it('throws ProtocolError on response missing both result and error', () => {
    expect(() => decodeMessage('{"jsonrpc":"2.0","id":1}')).toThrow(ProtocolError);
  });

  it('throws ProtocolError on malformed error object', () => {
    expect(() => decodeMessage('{"id":1,"error":{"code":"bad","message":"x"}}')).toThrow(
      ProtocolError,
    );
    expect(() => decodeMessage('{"id":1,"error":null}')).toThrow(ProtocolError);
  });
});
