// Shared listener fan-out for the transports.
//
// Both transports need the same three properties, and each had its own
// hand-written copy of them (the manager keeps a separate per-event-name
// implementation with the same semantics — see `ElectrumManager.emit`):
//
//  - REVISIT SAFETY: a Set iterator revisits an entry removed and
//    re-added while it runs, so a listener that unsubscribes and
//    resubscribes itself from inside its own callback was called again,
//    forever. Emission therefore iterates a snapshot.
//  - ISOLATION: a consumer callback must not break the emitter —
//    a throwing listener is swallowed so the remaining listeners (and
//    the socket's own teardown) keep running. Listener bugs surface
//    through the manager's `error` event, which wraps its own callbacks.
//  - CHEAP SNAPSHOTS: emit runs once per framed line on the hottest
//    path, so the snapshot is copy-on-write — invalidated when the
//    listener set changes, not rebuilt per event.

export class ListenerSet<T> {
  private readonly listeners = new Set<(value: T) => void>();
  private snapshot: readonly ((value: T) => void)[] | null = null;

  /** Register `listener`; returns its unsubscriber. */
  add(listener: (value: T) => void): () => void {
    this.listeners.add(listener);
    this.snapshot = null;
    return () => {
      this.listeners.delete(listener);
      this.snapshot = null;
    };
  }

  emit(value: T): void {
    const snap = (this.snapshot ??= [...this.listeners]);
    for (const l of snap) {
      try {
        l(value);
      } catch {
        // Swallowed by design — see ISOLATION above.
      }
    }
  }
}
