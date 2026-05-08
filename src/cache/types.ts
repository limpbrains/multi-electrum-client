// CacheStore — caller-provided KV. Manager handles JSON serialization and key namespacing.

export interface CacheStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlMs?: number): Promise<void>;
  del(key: string): Promise<void>;
}
