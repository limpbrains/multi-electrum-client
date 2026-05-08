import { describe, expect, it } from 'vitest';

import { LineFramer } from '../../../src/transport/lineFramer.js';

describe('LineFramer', () => {
  it('emits one entry per complete line, retains the trailing partial', () => {
    const f = new LineFramer();
    expect(f.push('{"a":1}\n{"b"')).toEqual(['{"a":1}']);
    expect(f.push(':2}\n')).toEqual(['{"b":2}']);
    expect(f.push('')).toEqual([]);
  });

  it('handles \\r\\n line endings', () => {
    const f = new LineFramer();
    expect(f.push('a\r\nb\r\n')).toEqual(['a', 'b']);
  });

  it('drops empty lines (server keepalives)', () => {
    const f = new LineFramer();
    expect(f.push('\n\n')).toEqual([]);
  });

  it('reset() drops the partial buffer', () => {
    const f = new LineFramer();
    f.push('partial-no-newline');
    f.reset();
    expect(f.push('after\n')).toEqual(['after']);
  });

  it('multiple complete lines in one chunk', () => {
    const f = new LineFramer();
    expect(f.push('a\nb\nc\n')).toEqual(['a', 'b', 'c']);
  });
});
