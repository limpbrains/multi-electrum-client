// Newline-delimited message framer.
//
// Electrum's TCP/TLS wire protocol delimits JSON-RPC messages with `\n`.
// WebSocket frames *usually* carry one message each, but middlebox proxies
// (and some servers) coalesce or split frames, so we use the same framing
// layer for all three transports. Buffering survives partial chunks: a
// chunk like `'{"id":1}\n{"id":2}\n{"i'` yields two complete messages and
// retains the trailing `'{"i'` for the next push.
//
// The retained buffer AND every complete line are bounded by
// `maxLineLength` — without a cap, a malicious or broken server could
// stream newline-free data forever and grow the buffer until the process
// (or browser tab / RN app) runs out of memory. On overflow `push` resets
// the buffer and throws `LineTooLongError`; the transports translate that
// into an `error` event and tear the connection down.

/**
 * Default cap on one logical line / the retained partial buffer, in
 * UTF-16 code units (≈ bytes for the ASCII JSON Electrum speaks).
 *
 * The floor is the protocol's own envelope, not typical traffic: a
 * consensus-valid 4M-weight-unit transaction serializes to ~4 MB and
 * comes back from `blockchain.transaction.get` as ~8 MB of hex —
 * ElectrumX ships MAX_SEND = 8,100,000 precisely so such a transaction
 * can be served (10,000,000 for AuxPoW chains). The VERBOSE form is
 * bigger still: the daemon's JSON carries the serialized transaction in
 * `hex` (~8 MB) and the witness a second time in `vin[].txinwitness`
 * (~8 MB for a witness-heavy transaction) — and for a BASE-heavy
 * transaction, `scriptPubKey.asm` renders each opcode byte as a
 * mnemonic up to ~23 chars ('OP_CHECKLOCKTIMEVERIFY '), so ~1 MB of
 * consensus-valid script becomes ~23 MB of asm, plus the hex fields:
 * a valid verbose response approaches ~28 MB. A default below the
 * envelope makes the framer reject a valid response, and because the
 * same data returns on every retry, the failure is deterministic and
 * permanent. 32 MiB covers the verbose worst case plus JSON overhead
 * while still bounding a hostile newline-free stream; per-server
 * tuning goes through `ServerSpec.maxLineLength` /
 * `Endpoint.maxLineLength`.
 */
export const DEFAULT_MAX_LINE_LENGTH = 32 * 1024 * 1024;

/** '\r' — compared by code unit so the scan does no slicing. */
const CR = 13;

export class LineTooLongError extends Error {
  override readonly name = 'LineTooLongError';
}

/**
 * Validate a caller-supplied cap. Exported so transports can reject a bad
 * value at construction time — they build one framer per connect attempt,
 * so without this an invalid option would only surface on the first
 * connect, long after the misconfiguration.
 */
export function assertMaxLineLength(maxLineLength: number, name = 'maxLineLength'): void {
  // NaN makes every `>` comparison false and Infinity is never exceeded —
  // either would silently remove the memory-exhaustion boundary. Zero and
  // negatives fail closed (they reject all data) but are still
  // misconfiguration, so reject them too rather than degrade.
  if (!Number.isSafeInteger(maxLineLength) || maxLineLength <= 0) {
    throw new RangeError(`${name} must be a positive safe integer, got ${String(maxLineLength)}`);
  }
}

export class LineFramer {
  /**
   * The in-progress line, kept as the chunks it arrived in rather than
   * one growing string. Appending to a string builds a rope that every
   * `indexOf` then flattens, so a peer dripping a newline-free response
   * in small pieces made the work quadratic in the cap: 4 MiB delivered
   * in 1 KiB chunks blocked the event loop for ~1.4s before the limit
   * could trip, and 256-byte chunks cost ~4.6s. Each chunk is now scanned
   * exactly once and the pieces are joined only to emit a finished line.
   */
  private parts: string[] = [];
  /**
   * Fragments received since the last block was sealed. They are joined
   * ONCE, when they are worth a block — never appended to a string one
   * at a time. Strings are immutable, so `block += fragment` copies the
   * whole block on every chunk: sealing by concatenation cost 6.6s of
   * blocked event loop for a 4 MiB line delivered byte-by-byte, which a
   * peer choosing its WebSocket fragmentation controls outright. Joining
   * once per block copies each byte once.
   */
  private fragments: string[] = [];
  private fragmentsLen = 0;
  /**
   * Bytes that seal a block. The peer chooses how it fragments the
   * stream, so one retained entry per chunk let a 4 MiB line delivered
   * byte-by-byte become 4 million strings (~34 MiB heap, ~70 MiB RSS)
   * even though the line itself was within the cap.
   *
   * Sealing bounds the retained entries to `line / BLOCK` sealed blocks
   * plus at most `MAX_FRAGMENTS` unsealed ones — and with tiny chunks the
   * fragment cap seals first, so the real bound is `line / MAX_FRAGMENTS`
   * blocks (4096 for a 4 MiB line of 1-byte chunks). Either way it
   * follows the LINE, never the chunk count.
   */
  private static readonly BLOCK = 64 * 1024;
  /**
   * Cap on unsealed fragments, so a flood of 1-byte chunks cannot hold
   * 65 536 tiny strings while waiting for a block's worth of bytes.
   */
  private static readonly MAX_FRAGMENTS = 1024;
  /** Length of the line so far (sealed blocks + fragments), delimiter excluded. */
  private pendingLen = 0;
  private readonly maxLineLength: number;

  constructor(maxLineLength: number = DEFAULT_MAX_LINE_LENGTH) {
    assertMaxLineLength(maxLineLength);
    this.maxLineLength = maxLineLength;
  }

  /**
   * Append `chunk` and return every complete line that has accumulated.
   * Empty lines are filtered (some servers emit a keepalive `\n` that has
   * no JSON to decode). The trailing partial line is retained until the
   * next push.
   *
   * Throws `LineTooLongError` (after resetting, so the framer stays
   * usable) when the retained partial line or any complete line exceeds
   * `maxLineLength`.
   */
  push(chunk: string): string[] {
    // Convenience wrapper: collects what `pushEach` streams. Transports
    // use `pushEach` so a frame full of short lines is never held twice.
    const out: string[] = [];
    this.pushEach(chunk, (line) => out.push(line));
    return out;
  }

  /**
   * Append `chunk` and hand every complete line to `onLine` as it is
   * found. Streaming rather than returning an array matters under a
   * hostile peer: one WebSocket frame of `"x\n"` repeated holds a string
   * per line at once, measured at ~13x the frame's own size, so a frame
   * the peer is free to make large turns into an out-of-memory kill even
   * though every individual line is far below `maxLineLength`. Emitting
   * as we scan bounds the extra memory to a single line.
   *
   * `onLine` runs during the scan; a line it receives has already been
   * validated, and a later overflow still throws (after the lines that
   * preceded it were delivered, which is what the transport would have
   * emitted anyway before tearing the connection down).
   */
  pushEach(chunk: string, onLine: (line: string) => void): void {
    let start = 0;
    for (let nl = chunk.indexOf('\n'); nl !== -1; nl = chunk.indexOf('\n', start)) {
      // Check before joining: an over-long line must not be materialized
      // just to be rejected. `- 1` leaves room for a trailing CR, which
      // belongs to the delimiter rather than the line.
      if (this.pendingLen + (nl - start) - 1 > this.maxLineLength) {
        // Capture BEFORE reset() zeroes pendingLen — the message is an
        // operator's only lead when tuning the cap from logs, and the
        // post-reset value reported just the final chunk's size.
        const lineLen = this.pendingLen + (nl - start);
        this.reset();
        throw new LineTooLongError(
          `line of ${lineLen} chars exceeds maxLineLength (${this.maxLineLength})`,
        );
      }
      const line = this.takeLine(chunk.slice(start, nl));
      // What /\r?\n/ would have consumed.
      const len =
        line.length > 0 && line.charCodeAt(line.length - 1) === CR ? line.length - 1 : line.length;
      if (len > this.maxLineLength) {
        this.reset();
        throw new LineTooLongError(
          `line of ${len} chars exceeds maxLineLength (${this.maxLineLength})`,
        );
      }
      // Empty lines are dropped (keepalive '\n').
      if (len > 0) onLine(len === line.length ? line : line.slice(0, len));
      start = nl + 1;
    }

    const tail = start === 0 ? chunk : chunk.slice(start);
    if (tail.length > 0) {
      this.fragments.push(tail);
      this.fragmentsLen += tail.length;
      this.pendingLen += tail.length;
      if (
        this.fragmentsLen >= LineFramer.BLOCK ||
        this.fragments.length >= LineFramer.MAX_FRAGMENTS
      ) {
        this.parts.push(this.fragments.join(''));
        this.fragments = [];
        this.fragmentsLen = 0;
      }
    }
    // A trailing '\r' may still turn out to be the first half of a CRLF
    // that the next chunk completes, so it does not count against the cap
    // yet: whether a line fits must not depend on where the transport
    // happened to split its reads.
    const retained = this.pendingLen - (this.endsWithCR() ? 1 : 0);
    if (retained > this.maxLineLength) {
      this.reset();
      throw new LineTooLongError(
        `line exceeds maxLineLength (${this.maxLineLength}) without a newline`,
      );
    }
  }

  /** Join the retained pieces with `last` and start a fresh line. */
  private takeLine(last: string): string {
    if (this.parts.length === 0 && this.fragments.length === 0) return last;
    const line = [...this.parts, ...this.fragments, last].join('');
    this.parts = [];
    this.fragments = [];
    this.fragmentsLen = 0;
    this.pendingLen = 0;
    return line;
  }

  private endsWithCR(): boolean {
    const last = this.fragments[this.fragments.length - 1] ?? this.parts[this.parts.length - 1];
    return last !== undefined && last.charCodeAt(last.length - 1) === CR;
  }

  /**
   * Number of pieces the in-progress line is held in. Diagnostic: the
   * bound it exposes (line length / BLOCK, never one per network chunk)
   * is the whole point of the blocking above, and heap measurements are
   * too GC-dependent to assert on.
   */
  pendingBlocks(): number {
    // Fragments count individually: collapsing an unsealed run to 1 would
    // hide exactly the regression this diagnostic exists to catch — a
    // change that stopped sealing would report the same number as one
    // that seals correctly.
    return this.parts.length + this.fragments.length;
  }

  /** Drop any pending partial line. Call on transport close / reset. */
  reset(): void {
    this.parts = [];
    this.fragments = [];
    this.fragmentsLen = 0;
    this.pendingLen = 0;
  }
}
