import { createKvClient, type KvClient } from "../client.js";

export function getHttpUrl(): string {
  const url = process.env["KV_TEST_HTTP_URL"];
  if (!url) throw new Error("KV_TEST_HTTP_URL not set — is globalSetup running?");
  return url;
}

export function getRespUrl(): string {
  const url = process.env["KV_TEST_RESP_URL"];
  if (!url) throw new Error("KV_TEST_RESP_URL not set — is globalSetup running?");
  return url;
}

/** HTTP client scoped to a randomly-named namespace to avoid cross-test pollution. */
export function httpClient(namespace?: string): KvClient {
  return createKvClient({ url: getHttpUrl(), namespace: namespace ?? uniqueNs() });
}

/** RESP client on the given db number (default 0). */
export function respClient(db = 0): KvClient {
  return createKvClient({ url: getRespUrl(), db });
}

/** Unique key with optional prefix — guarantees tests don't collide. */
export function uniqueKey(prefix = "k"): string {
  return `${prefix}:${crypto.randomUUID()}`;
}

/** Unique namespace name for HTTP isolation. */
export function uniqueNs(): string {
  return `ns-${crypto.randomUUID()}`;
}

export function enc(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

export function dec(b: Uint8Array): string {
  return new TextDecoder().decode(b);
}
