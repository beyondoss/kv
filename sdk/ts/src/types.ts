export interface KvEntry {
  value: Uint8Array;
  ttl?: number;
  metadata?: unknown;
}

export interface KvSetOptions {
  ttl?: number;
  metadata?: unknown;
  nx?: boolean;
}

export interface KvListOptions {
  prefix?: string;
  cursor?: string;
  limit?: number;
}

export interface KvListResult {
  keys: KvListKey[];
  cursor?: string;
  complete: boolean;
}

export interface KvListKey {
  name: string;
}
