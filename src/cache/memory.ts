// MemoryCache — default in-memory CacheStore. Unbounded; users with bounded storage
// inject their own adapter implementing the CacheStore interface.

import type { CacheStore } from './types.js';

interface Entry {
  value: string;
  expiresAt?: number;
}

export class MemoryCache implements CacheStore {
  private readonly store = new Map<string, Entry>();

  async get(key: string): Promise<string | null> {
    const entry = this.store.get(key);
    if (!entry) return null;
    if (entry.expiresAt !== undefined && entry.expiresAt <= Date.now()) {
      this.store.delete(key);
      return null;
    }
    return entry.value;
  }

  async set(key: string, value: string, ttlMs?: number): Promise<void> {
    const entry: Entry = { value };
    if (ttlMs !== undefined) entry.expiresAt = Date.now() + ttlMs;
    this.store.set(key, entry);
  }

  async del(key: string): Promise<void> {
    this.store.delete(key);
  }
}
