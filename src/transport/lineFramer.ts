// Newline-delimited message framer.
//
// Electrum's TCP/TLS wire protocol delimits JSON-RPC messages with `\n`.
// WebSocket frames *usually* carry one message each, but middlebox proxies
// (and some servers) coalesce or split frames, so we use the same framing
// layer for all three transports. Buffering survives partial chunks: a
// chunk like `'{"id":1}\n{"id":2}\n{"i'` yields two complete messages and
// retains the trailing `'{"i'` for the next push.

export class LineFramer {
  private buf = '';

  /**
   * Append `chunk` to the internal buffer and return every complete line
   * that has accumulated. Empty lines are filtered (some servers emit a
   * keepalive `\n` that has no JSON to decode). The trailing partial line
   * is retained until the next push.
   */
  push(chunk: string): string[] {
    this.buf += chunk;
    // Fast path: no complete line yet — `buf` never retains a '\n', so
    // this check is exact and skips all allocation for partial frames.
    if (!this.buf.includes('\n')) return [];
    const parts = this.buf.split('\n');
    this.buf = parts.pop() ?? '';
    const out: string[] = [];
    for (let p of parts) {
      if (p.endsWith('\r')) p = p.slice(0, -1); // what /\r?\n/ consumed
      if (p.length > 0) out.push(p);
    }
    return out;
  }

  /** Drop any pending partial line. Call on transport close / reset. */
  reset(): void {
    this.buf = '';
  }
}
