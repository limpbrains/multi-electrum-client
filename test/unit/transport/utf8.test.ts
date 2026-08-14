import { describe, expect, it } from 'vitest';

import {
  asBytes,
  decodeValidated,
  scanUtf8,
  Utf8DecodeError,
  Utf8Stream,
} from '../../../src/transport/utf8.js';

const bytes = (...b: number[]): Uint8Array => new Uint8Array(b);

describe('Utf8Stream', () => {
  it('carries a character split across chunks without asking the decoder to stream', () => {
    // This is the property React Native's TextDecoder polyfill does NOT
    // provide: it accepts `{ stream: true }` and ignores it, so on-device
    // a split '€' decoded to three replacement characters. The tail is
    // held here instead, and the platform decoder only ever sees whole
    // characters.
    const s = new Utf8Stream();
    expect(s.decode(bytes(0x22, 0xe2))).toBe('"');
    expect(s.hasPending()).toBe(true);
    expect(s.decode(bytes(0x82, 0xac, 0x22))).toBe('€"');
    expect(s.hasPending()).toBe(false);
  });

  it('holds a character split one byte at a time', () => {
    const s = new Utf8Stream();
    // '𝄞' — F0 9D 84 9E, the longest sequence.
    expect(s.decode(bytes(0xf0))).toBe('');
    expect(s.decode(bytes(0x9d))).toBe('');
    expect(s.decode(bytes(0x84))).toBe('');
    expect(s.decode(bytes(0x9e))).toBe('𝄞');
    expect(s.hasPending()).toBe(false);
  });

  it('reports nothing pending when a chunk ends on a character boundary', () => {
    const s = new Utf8Stream();
    expect(s.decode(bytes(0x7b, 0x7d, 0x0a))).toBe('{}\n');
    expect(s.hasPending()).toBe(false);
  });

  it('rejects malformed bytes without relying on TextDecoder options', () => {
    // React Native's TextDecoder polyfill ignores both `stream` and
    // `fatal` (on-device CI proved each in turn), so validity is decided
    // by this module's own scanner and behaves identically everywhere.
    const cases: [string, Uint8Array][] = [
      ['bare continuation byte', bytes(0x80)],
      ['overlong two-byte form', bytes(0xc0, 0x80)],
      ['overlong three-byte form', bytes(0xe0, 0x80, 0x80)],
      ['UTF-16 surrogate', bytes(0xed, 0xa0, 0x80)],
      ['beyond U+10FFFF', bytes(0xf4, 0x90, 0x80, 0x80)],
      ['undefined lead byte', bytes(0xf5, 0x80, 0x80, 0x80)],
      ['truncated sequence followed by ASCII', bytes(0xe2, 0x82, 0x41)],
    ];
    for (const [name, input] of cases) {
      const s = new Utf8Stream();
      expect(() => s.decode(input), name).toThrow(Utf8DecodeError);
    }
  });

  it('accepts every well-formed sequence length', () => {
    const s = new Utf8Stream();
    expect(s.decode(new TextEncoder().encode('a£€𝄞'))).toBe('a£€𝄞');
  });

  it('rejects a bad continuation byte that arrives in the next chunk', () => {
    const s = new Utf8Stream();
    expect(s.decode(bytes(0xe2))).toBe('');
    expect(s.hasPending()).toBe(true);
    expect(() => s.decode(bytes(0x28, 0xa1))).toThrow(Utf8DecodeError);
    // The rejected tail must not linger for the chunk after it.
    expect(s.hasPending()).toBe(false);
  });

  it('throws on bytes that are not valid UTF-8 at all', () => {
    // Malformed input must not decode to U+FFFD: a replacement character
    // leaves the surrounding JSON valid, so the corruption would reach
    // the caller as data.
    const s = new Utf8Stream();
    expect(() => s.decode(bytes(0x22, 0xff, 0x22))).toThrow(Utf8DecodeError);
  });

  it('throws on a lead byte whose continuation bytes are wrong', () => {
    const s = new Utf8Stream();
    expect(() => s.decode(bytes(0xe2, 0x28, 0xa1))).toThrow(Utf8DecodeError);
  });

  it('reset drops a half-received character', () => {
    const s = new Utf8Stream();
    s.decode(bytes(0xe2));
    expect(s.hasPending()).toBe(true);
    s.reset();
    expect(s.hasPending()).toBe(false);
  });

  it('asBytes normalizes ArrayBuffer, views and rejects anything else', () => {
    const buf = new Uint8Array([1, 2, 3, 4]).buffer;
    expect(asBytes(buf)).toEqual(new Uint8Array([1, 2, 3, 4]));
    // A view must be honoured, offset and length included.
    expect(asBytes(new Uint8Array(buf, 1, 2))).toEqual(new Uint8Array([2, 3]));
    expect(asBytes('text')).toBeUndefined();
    expect(asBytes(undefined)).toBeUndefined();
  });

  it('scanUtf8 reports where an incomplete character starts', () => {
    expect(scanUtf8(bytes(0x61, 0x62))).toBe(2);
    expect(scanUtf8(bytes(0x61, 0xe2, 0x82))).toBe(1);
    expect(scanUtf8(new TextEncoder().encode('€'))).toBe(3);
  });
});

describe('Utf8Stream — platform-independent decoding', () => {
  it('preserves U+FEFF at a chunk boundary', () => {
    // TextDecoder strips a leading BOM on every call unless `ignoreBOM`
    // is honoured, and this class decodes chunk by chunk — so the same
    // payload used to keep or lose its U+FEFF depending on where the
    // network split it.
    const whole = new TextEncoder().encode('a﻿b');
    const s = new Utf8Stream();
    expect(s.decode(whole.subarray(0, 1))).toBe('a');
    expect(s.decode(whole.subarray(1))).toBe('﻿b');
  });

  it('decodes without any platform TextDecoder', () => {
    const g = globalThis as { TextDecoder?: unknown };
    const saved = g.TextDecoder;
    g.TextDecoder = undefined;
    try {
      const s = new Utf8Stream();
      expect(s.decode(new Uint8Array([0x61, 0xc2, 0xa3]))).toBe('a£');
      expect(s.decode(new Uint8Array([0xe2, 0x82, 0xac, 0xf0, 0x9d, 0x84, 0x9e]))).toBe('€𝄞');
      expect(() => s.decode(new Uint8Array([0xff]))).toThrow(Utf8DecodeError);
    } finally {
      g.TextDecoder = saved;
    }
  });

  it('decodeValidated matches the platform decoder over a large mixed payload', () => {
    // The manual path is only reached on runtimes whose decoder we reject,
    // so pin it against the reference implementation here.
    const text = 'a£€𝄞﻿'.repeat(5000);
    const encoded = new TextEncoder().encode(text);
    expect(decodeValidated(encoded)).toBe(text);
  });
});
