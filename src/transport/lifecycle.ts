// Connect / close serialization for a transport instance.
//
// Both transports own exactly one live socket and stamp every connect
// attempt with a generation so a superseded socket's handlers go inert
// (see `Transport` in ./types.ts for the contract). Doing that with a
// loose flag plus a counter left overlap windows: a close landing
// between the ready event and the publication of the socket, an
// immediate reconnect racing a not-yet-cleared "connecting" flag, a
// slow teardown retiring the connection that replaced it, and two
// concurrent closes each retiring a generation. Those are all the same
// bug — unserialized lifecycle — so the ordering lives here once,
// shared by both transports, instead of being re-derived per socket
// type.
//
// Contract enforced:
//  - one connect at a time, and none while a socket is open;
//  - `connect()` waits out a close that is already underway, so
//    `await close(); connect()` is always valid;
//  - `close()` is idempotent (concurrent callers share one operation),
//    retires the active generation BEFORE anything else so a connect
//    mid-publication cannot land behind it, aborts an in-flight connect
//    and awaits its settlement before tearing down;
//  - a failed attempt retires itself, so its handlers cannot emit after
//    `connect()` rejected.

import { TransportError } from '../errors/types.js';

export class TransportLifecycle {
  private generation = 0;
  private connectOp: Promise<void> | null = null;
  private closeOp: Promise<void> | null = null;
  private abort: (() => void) | null = null;
  /** Generation that owns `abort`; -1 when the slot is empty. */
  private abortGen = -1;

  /** True once `gen` is no longer the active generation. */
  isStale(gen: number): boolean {
    return gen !== this.generation;
  }

  /**
   * Register (or clear) the hook that aborts the in-flight connect.
   * `close()` invokes it so a pending attempt fails fast instead of
   * outliving the teardown.
   */
  setAbort(gen: number, fn: (() => void) | null): void {
    if (fn === null) {
      // Only the owner may clear the slot: a delayed event from a retired
      // candidate must not wipe the live candidate's hook, or close()
      // would have nothing to abort and would sit out the full connect
      // timeout instead of its 500ms teardown bound.
      if (this.abortGen === gen) {
        this.abort = null;
        this.abortGen = -1;
      }
      return;
    }
    if (this.isStale(gen)) return; // a retired candidate may not arm anything
    this.abort = fn;
    this.abortGen = gen;
  }

  /**
   * Retire the active generation: every handler captured by the current
   * attempt becomes stale. Callers use this when an attempt dies.
   */
  retire(): void {
    this.generation++;
  }

  /**
   * Run one connect attempt under the lifecycle rules. `isOpen` reports
   * whether a socket is already published; `run` receives the attempt's
   * generation stamp.
   */
  async connect(isOpen: () => boolean, run: (gen: number) => Promise<void>): Promise<void> {
    // A close in progress owns the transport — wait it out rather than
    // rejecting, so the documented reconnect sequence always works even
    // when the caller does not await the previous connect's rejection.
    while (this.closeOp) await this.closeOp.catch(() => undefined);
    if (this.connectOp) throw new TransportError('connect already in progress');
    if (isOpen()) throw new TransportError('already connected; close() first');
    const gen = ++this.generation;
    const op = run(gen).finally(() => {
      this.connectOp = null;
      this.setAbort(gen, null);
    });
    this.connectOp = op;
    return op;
  }

  /** Run teardown under the lifecycle rules; concurrent calls share it. */
  async close(teardown: () => Promise<void>): Promise<void> {
    if (this.closeOp) return this.closeOp;
    const op = (async () => {
      // Retire FIRST: an attempt sitting between its ready event and the
      // publication of its socket must find itself stale and discard the
      // socket instead of going live behind our back.
      this.retire();
      const abort = this.abort;
      this.abort = null;
      this.abortGen = -1;
      abort?.();
      // Let the aborted attempt settle before teardown so `connecting`
      // state is clear by the time this close resolves.
      if (this.connectOp) await this.connectOp.catch(() => undefined);
      await teardown();
    })().finally(() => {
      this.closeOp = null;
    });
    this.closeOp = op;
    return op;
  }
}
