// Global environment shims for running the node unit suite on-device.
// Imported first from vitest-shim.ts, so it runs before any test body.
import { Buffer } from 'buffer';
import { Event as ShimEvent, EventTarget as ShimEventTarget } from 'event-target-shim';

declare const global: typeof globalThis & {
  Buffer?: typeof Buffer;
  process?: { env: Record<string, string | undefined> };
};

// tls.test.ts calls Buffer.from unguarded, and src/transport/tcp.ts
// prefers Buffer decoding when chunks arrive as bytes.
global.Buffer ??= Buffer;

// RN provides a minimal `process`; make sure `process.env` reads don't throw.
global.process ??= { env: {} };
global.process.env ??= {};

// chai (pulled in by react-native-harness's @vitest/expect integration)
// subclasses the DOM Event/EventTarget globals at module-init time. The
// harness runtime installs the same shim, but only in the host-app bundle's
// module graph — the per-test-file bundle evaluates its own copy of chai
// before that, so install the globals here too (this module runs first).
const g = globalThis as typeof globalThis & {
  Event?: unknown;
  EventTarget?: unknown;
};
if (typeof g.Event !== 'function') g.Event = ShimEvent;
if (typeof g.EventTarget !== 'function') g.EventTarget = ShimEventTarget;

// RN's AbortController polyfill predates the `reason` argument: it always
// aborts with a plain 'aborted' error, so tests asserting on custom abort
// reasons fail. Replace it with a small spec-conformant shim when needed.
const a = globalThis as typeof globalThis & {
  AbortController?: new () => {
    signal: { reason?: unknown };
    abort: (reason?: unknown) => void;
  };
  AbortSignal?: unknown;
};
const abortReasonSupported = (() => {
  try {
    const c = new a.AbortController!();
    c.abort('probe');
    return c.signal.reason === 'probe';
  } catch {
    return false;
  }
})();
if (!abortReasonSupported) {
  class ShimAbortSignal extends ShimEventTarget {
    aborted = false;
    reason: unknown = undefined;
    onabort: ((ev: unknown) => void) | null = null;
    throwIfAborted(): void {
      if (this.aborted) throw this.reason;
    }
  }
  class ShimAbortController {
    readonly signal = new ShimAbortSignal();
    abort(reason?: unknown): void {
      const signal = this.signal;
      if (signal.aborted) return;
      signal.aborted = true;
      signal.reason =
        reason !== undefined
          ? reason
          : Object.assign(new Error('The operation was aborted.'), { name: 'AbortError' });
      const ev = new ShimEvent('abort');
      signal.onabort?.(ev);
      signal.dispatchEvent(ev);
    }
  }
  a.AbortController = ShimAbortController as unknown as typeof a.AbortController;
  a.AbortSignal = ShimAbortSignal;
}

// Hermes has no TextEncoder/TextDecoder; src/transport/tcp.ts uses
// TextDecoder for its defensive Uint8Array path and tcp.test.ts uses
// TextEncoder. UTF-8 only, backed by the buffer ponyfill.
const t = globalThis as typeof globalThis & {
  TextEncoder?: unknown;
  TextDecoder?: unknown;
};
if (typeof t.TextEncoder !== 'function') {
  t.TextEncoder = class TextEncoder {
    readonly encoding = 'utf-8';
    encode(input = ''): Uint8Array {
      const buf = Buffer.from(input, 'utf-8');
      return new Uint8Array(buf.buffer, buf.byteOffset, buf.byteLength);
    }
  };
}
if (typeof t.TextDecoder !== 'function') {
  t.TextDecoder = class TextDecoder {
    readonly encoding: string;
    constructor(label = 'utf-8') {
      this.encoding = label;
      if (!/^utf-?8$/i.test(label)) {
        throw new RangeError(`TextDecoder shim supports utf-8 only, got '${label}'`);
      }
    }
    decode(input?: ArrayBuffer | ArrayBufferView): string {
      if (input === undefined) return '';
      const view = ArrayBuffer.isView(input)
        ? Buffer.from(input.buffer, input.byteOffset, input.byteLength)
        : Buffer.from(input);
      return view.toString('utf-8');
    }
  };
}
