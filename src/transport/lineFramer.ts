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
    const parts = this.buf.split(/\r?\n/);
    this.buf = parts.pop() ?? '';
    return parts.filter((p) => p.length > 0);
  }

  /** Drop any pending partial line. Call on transport close / reset. */
  reset(): void {
    this.buf = '';
  }
}
