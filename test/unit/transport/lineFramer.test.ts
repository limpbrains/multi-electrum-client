import { describe, expect, it } from 'vitest';

import {
  DEFAULT_MAX_LINE_LENGTH,
  LineFramer,
  LineTooLongError,
} from '../../../src/transport/lineFramer.js';

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

describe('LineFramer — maxLineLength validation', () => {
  it.each([
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['zero', 0],
    ['negative', -1],
    ['fractional', 10.5],
  ])('rejects %s — an invalid cap must not silently disable the limit', (_label, value) => {
    expect(() => new LineFramer(value)).toThrow(RangeError);
  });

  it('accepts a valid custom limit', () => {
    const f = new LineFramer(32);
    expect(f.push('hi\n')).toEqual(['hi']);
    expect(() => f.push('x'.repeat(33))).toThrow(LineTooLongError);
  });
});

describe('LineFramer — line length cap', () => {
  it('throws on a single oversized newline-free chunk and resets', () => {
    const f = new LineFramer(10);
    expect(() => f.push('x'.repeat(11))).toThrow(LineTooLongError);
    // Buffer was reset — the framer stays usable.
    expect(f.push('ok\n')).toEqual(['ok']);
  });

  it('throws once accumulated newline-free chunks exceed the cap', () => {
    const f = new LineFramer(10);
    expect(f.push('xxxx')).toEqual([]);
    expect(f.push('xxxx')).toEqual([]);
    expect(() => f.push('xxxx')).toThrow(LineTooLongError);
  });

  it('throws on an oversized COMPLETE line even when followed by valid data', () => {
    const f = new LineFramer(10);
    expect(() => f.push('x'.repeat(11) + '\nok\n')).toThrow(LineTooLongError);
    // Post-overflow the buffer is clean; new data parses normally.
    expect(f.push('next\n')).toEqual(['next']);
  });

  it('throws when the retained trailing partial exceeds the cap', () => {
    const f = new LineFramer(10);
    expect(() => f.push('ok\n' + 'y'.repeat(11))).toThrow(LineTooLongError);
  });

  it('accepts lines exactly at the cap', () => {
    const f = new LineFramer(5);
    expect(f.push('abcde\n')).toEqual(['abcde']);
  });
});

describe('LineFramer — chunk-boundary and delimiter handling', () => {
  it('does not count a provisional trailing CR against the cap', () => {
    // Where the transport happens to split its reads must not decide
    // whether a line is legal: 'abcde\r\n' in one push is accepted (the
    // CR belongs to the delimiter), so the same bytes split across two
    // pushes must be too.
    const whole = new LineFramer(5);
    expect(whole.push('abcde\r\n')).toEqual(['abcde']);

    const split = new LineFramer(5);
    expect(split.push('abcde\r')).toEqual([]);
    expect(split.push('\n')).toEqual(['abcde']);
  });

  it('sealing blocks stays linear when fragments are tiny', () => {
    // Sealing by concatenation (`block += fragment`) copies the whole
    // block on every chunk, because strings are immutable: a 4 MiB line
    // delivered byte-by-byte cost 6.6s of blocked event loop, and the
    // peer chooses its own fragmentation. Fragments are joined once per
    // block instead.
    //
    // Asserted as a COMPLEXITY RATIO, not a wall-clock bound: an absolute
    // bound is a property of the engine, not the code — linear on Hermes
    // (~3.4s on an emulator) overlaps quadratic on Node (~6.5s), so no
    // single constant discriminates on every platform this suite runs
    // on. An 8x workload costs ~8x when linear and ~64x when quadratic;
    // the threshold sits between with a 3x margin each way.
    const cost = (bytes: number): number => {
      const f = new LineFramer();
      const t0 = Date.now();
      for (let i = 0; i < bytes; i++) f.pushEach('x', () => undefined);
      return Math.max(1, Date.now() - t0);
    };
    cost(256 * 1024); // JIT warm-up: the first run pays compilation
    const small = cost(512 * 1024);
    const large = cost(4 * 1024 * 1024 - 8);
    expect(large / small).toBeLessThan(24);

    // 4096 sealed blocks (4 MiB / 1024 fragments each) plus whatever is
    // still unsealed — bounded by the LINE, not by the 4 million chunks.
    const f = new LineFramer();
    for (let i = 0; i < 4 * 1024 * 1024 - 8; i++) f.pushEach('x', () => undefined);
    expect(f.pendingBlocks()).toBeLessThanOrEqual(4096 + 1024);
  });

  it('still rejects a real overlong line that ends in CR', () => {
    const f = new LineFramer(5);
    expect(() => f.push('abcdef\r')).toThrow(LineTooLongError);
  });

  it('handles a delimiter flood without materializing a segment per newline', () => {
    // `split('\n')` allocated one array entry per delimiter before any
    // length check, so a mostly-newline chunk allocated in proportion to
    // the delimiter count rather than to the data it carried.
    const f = new LineFramer(1024);
    // `process.memoryUsage` is Node-only; this suite also runs on-device
    // under Hermes, where the behavioural half of the assertion still
    // holds.
    const measure =
      typeof process !== 'undefined' && typeof process.memoryUsage === 'function'
        ? () => process.memoryUsage().heapUsed
        : null;
    const start = measure?.() ?? 0;
    expect(f.push('\n'.repeat(2_000_000))).toEqual([]);
    if (measure) {
      // The 2 MiB chunk itself is unavoidable; an array of 2M segments is
      // not (it cost ~16 MB+ before).
      expect(measure() - start).toBeLessThan(8 * 1024 * 1024);
    }
  });

  it('validates a complete line before the delimiters that follow it', () => {
    const f = new LineFramer(8);
    expect(() => f.push(`${'x'.repeat(64)}\n${'\n'.repeat(100_000)}`)).toThrow(LineTooLongError);
  });
});

describe('LineFramer — streaming output', () => {
  it('never holds more than one line while scanning a frame of short lines', () => {
    // Every line here is far below maxLineLength, so no cap trips — but
    // collecting them all held ~13x the frame's own size (measured), which
    // turns a large frame the peer is free to send into an out-of-memory
    // kill. Streaming bounds the extra memory to the current line.
    const f = new LineFramer();
    const chunk = 'x\n'.repeat(500_000);
    const measure =
      typeof process !== 'undefined' && typeof process.memoryUsage === 'function'
        ? () => process.memoryUsage().heapUsed
        : null;

    let count = 0;
    let peak = 0;
    const start = measure?.() ?? 0;
    f.pushEach(chunk, () => {
      count++;
      if (measure && count % 50_000 === 0) peak = Math.max(peak, measure() - start);
    });

    expect(count).toBe(500_000);
    if (measure) {
      // The 1 MB chunk itself is unavoidable; 500k retained strings are not.
      expect(peak).toBeLessThan(6 * 1024 * 1024);
    }
  });

  it('pushEach delivers the lines that precede an overflow, then throws', () => {
    const f = new LineFramer(8);
    const seen: string[] = [];
    expect(() => f.pushEach(`ok\n${'x'.repeat(64)}\n`, (l) => seen.push(l))).toThrow(
      LineTooLongError,
    );
    expect(seen).toEqual(['ok']);
  });
});

describe('LineFramer — cost of a newline-free drip', () => {
  it('stays linear when a line arrives in many small chunks', () => {
    // Appending to one growing string built a rope that every scan then
    // flattened, so the work was quadratic in the cap: 4 MiB delivered in
    // 256-byte pieces blocked the event loop for ~4.6s before the limit
    // could trip — a remote CPU stall reachable from a single response.
    const f = new LineFramer();
    const chunk = 'x'.repeat(256);
    const pushes = Math.floor(DEFAULT_MAX_LINE_LENGTH / 256) - 1;

    const t0 = Date.now();
    for (let i = 0; i < pushes; i++) f.pushEach(chunk, () => undefined);
    const elapsed = Date.now() - t0;

    // Measured at ~2ms after the fix; the bound is loose enough for a
    // slow device but nowhere near the seconds the old scan cost.
    expect(elapsed).toBeLessThan(1500);
    // And the cap still trips on the piece that crosses it.
    expect(() => f.pushEach('x'.repeat(4096), () => undefined)).toThrow(LineTooLongError);
  });
});

describe('LineFramer — retained pieces are bounded by length, not by chunk count', () => {
  it('a byte-by-byte line does not cost a string per byte', () => {
    // The peer chooses how it fragments the stream. Keeping one entry per
    // chunk turned a line well inside the cap into millions of tiny
    // strings — measured at ~34 MiB of heap for a 4 MiB line delivered
    // one byte at a time.
    const f = new LineFramer();
    for (let i = 0; i < 512 * 1024; i++) f.pushEach('x', () => undefined);

    // Retained pieces scale with the LINE, not with the 524,288 chunks it
    // arrived in: fragments seal into a block every 1024 of them (or
    // every 64 KiB, whichever comes first), so this is bounded by
    // length/1024 sealed blocks plus an unsealed run. Asserted on the
    // count rather than on heap growth, which is too GC-dependent.
    expect(f.pendingBlocks()).toBeLessThanOrEqual(512 + 1024);
    // And it still frames correctly once the line ends.
    const out: string[] = [];
    f.pushEach('!\n', (line) => out.push(line));
    expect(out).toHaveLength(1);
    expect(out[0]!.length).toBe(512 * 1024 + 1);
  });
});

describe('LineFramer — protocol envelope', () => {
  it('default cap admits the largest protocol-valid response', () => {
    // A consensus-valid 4M-weight-unit transaction serializes to ~4 MB
    // and comes back from blockchain.transaction.get as ~8 MB of hex;
    // ElectrumX ships MAX_SEND = 8,100,000 for exactly this reason
    // (10,000,000 on AuxPoW chains). A default below that envelope
    // makes the framer reject a valid response, and every retry
    // deterministically dies on the same data.
    const f = new LineFramer();
    // 8,100,000: ElectrumX MAX_SEND (raw hex of a consensus-max tx);
    // 10,000,000: the AuxPoW default; ~17M: a verbose
    // blockchain.transaction.get of a witness-heavy consensus-max tx,
    // whose JSON carries the serialized tx in `hex` (~8 MB) AND the
    // witness again in vin[].txinwitness (~8 MB); ~28M: a BASE-heavy
    // tx — up to ~1 MB of script rendered as `scriptPubKey.asm`, where
    // one opcode byte becomes a ~23-char mnemonic
    // ('OP_CHECKLOCKTIMEVERIFY '), ~23 MB of asm plus the hex fields.
    for (const envelope of [8_100_000, 10_000_000, 17_000_000, 28_000_000]) {
      const line = 'x'.repeat(envelope);
      expect(f.push(line + '\n')).toEqual([line]);
    }
  });
});
