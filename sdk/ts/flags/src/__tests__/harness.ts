import { createKvClient, type KvClient } from "@beyond.dev/kv";

export function getHttpUrl(): string {
  const url = process.env["KV_TEST_HTTP_URL"];
  if (!url) {
    throw new Error("KV_TEST_HTTP_URL not set — is globalSetup running?");
  }
  return url;
}

export function uniqueNs(): string {
  return `flags-${crypto.randomUUID()}`;
}

export function kvClient(namespace?: string): KvClient {
  return createKvClient({
    url: getHttpUrl(),
    namespace: namespace ?? uniqueNs(),
  });
}

export function sleep(ms: number) {
  return new Promise<void>((r) => setTimeout(r, ms));
}

/** Write a flag definition directly to KV. */
export async function writeDef(
  kv: KvClient,
  name: string,
  def: unknown,
): Promise<void> {
  const { error } = await kv.set(`flags:def:${name}`, JSON.stringify(def));
  if (error) throw error;
}

/** Delete a flag definition. */
export async function deleteDef(kv: KvClient, name: string): Promise<void> {
  await kv.delete(`flags:def:${name}`);
}

/** Read a per-id pref bundle (raw JSON or null). */
export async function readPrefs(
  kv: KvClient,
  id: string,
): Promise<Record<string, unknown> | null> {
  const { data, error } = await kv.get(`flags:user:${id}`);
  if (error) throw error;
  if (!data) return null;
  return data.json<Record<string, unknown>>();
}
