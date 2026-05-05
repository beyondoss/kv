import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createKvClient } from "../client.js";
import { createServerKvClient } from "../next/index.js";
import { getHttpUrl, getRespUrl, uniqueKey, uniqueNs } from "./harness.js";

describe("createKvClient — backend routing", () => {
  it("selects the HTTP backend for http:// URLs", async () => {
    const kv = createKvClient({ url: getHttpUrl(), namespace: uniqueNs() });
    const key = uniqueKey();
    await kv.set(key, "http");
    const { data: entry } = await kv.get(key);
    expect(new TextDecoder().decode(entry!.value)).toBe("http");
  });

  it("selects the RESP backend for redis:// URLs", async () => {
    const kv = createKvClient({ url: getRespUrl(), db: 0 });
    const key = uniqueKey();
    await kv.set(key, "resp");
    const { data: entry } = await kv.get(key);
    expect(new TextDecoder().decode(entry!.value)).toBe("resp");
  });
});

describe("createServerKvClient — environment configuration", () => {
  const savedEnv: Record<string, string | undefined> = {};

  beforeEach(() => {
    for (const k of ["KV_URL", "KV_DB", "KV_NAMESPACE"]) {
      savedEnv[k] = process.env[k];
      delete process.env[k];
    }
  });

  afterEach(() => {
    for (const [k, v] of Object.entries(savedEnv)) {
      if (v === undefined) {
        delete process.env[k];
      } else {
        process.env[k] = v;
      }
    }
  });

  it("throws when KV_URL is not set", () => {
    expect(() => createServerKvClient()).toThrow(/KV_URL/);
  });

  it("creates an HTTP client from an http:// KV_URL", async () => {
    process.env["KV_URL"] = getHttpUrl();
    process.env["KV_NAMESPACE"] = uniqueNs();
    const kv = createServerKvClient();
    const key = uniqueKey();
    await kv.set(key, "server-kv");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
  });

  it("creates a RESP client from a redis:// KV_URL", async () => {
    process.env["KV_URL"] = getRespUrl();
    process.env["KV_DB"] = "0";
    const kv = createServerKvClient();
    const key = uniqueKey();
    await kv.set(key, "server-resp");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
  });

  it("passes KV_DB to the RESP backend", () => {
    process.env["KV_URL"] = getRespUrl();
    process.env["KV_DB"] = "2";
    // Just verify it constructs without error; db routing is tested in resp.test.ts
    expect(() => createServerKvClient()).not.toThrow();
  });

  it("passes KV_NAMESPACE to the HTTP backend", async () => {
    const ns = uniqueNs();
    process.env["KV_URL"] = getHttpUrl();
    process.env["KV_NAMESPACE"] = ns;
    const kv = createServerKvClient();
    const key = uniqueKey();
    await kv.set(key, "namespaced");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
  });
});
