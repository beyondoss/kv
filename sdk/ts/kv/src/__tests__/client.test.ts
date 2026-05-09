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
    for (const k of ["BEYOND_KV_URL", "BEYOND_KV_DB", "BEYOND_KV_NAMESPACE"]) {
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

  it("throws when BEYOND_KV_URL is not set", () => {
    expect(() => createServerKvClient()).toThrow(/BEYOND_KV_URL/);
  });

  it("creates an HTTP client from an http:// BEYOND_KV_URL", async () => {
    process.env["BEYOND_KV_URL"] = getHttpUrl();
    process.env["BEYOND_KV_NAMESPACE"] = uniqueNs();
    const kv = createServerKvClient();
    const key = uniqueKey();
    await kv.set(key, "server-kv");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
  });

  it("creates a RESP client from a redis:// BEYOND_KV_URL", async () => {
    process.env["BEYOND_KV_URL"] = getRespUrl();
    process.env["BEYOND_KV_DB"] = "0";
    const kv = createServerKvClient();
    const key = uniqueKey();
    await kv.set(key, "server-resp");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
  });

  it("passes BEYOND_KV_DB to the RESP backend", () => {
    process.env["BEYOND_KV_URL"] = getRespUrl();
    process.env["BEYOND_KV_DB"] = "2";
    // Just verify it constructs without error; db routing is tested in resp.test.ts
    expect(() => createServerKvClient()).not.toThrow();
  });

  it("passes BEYOND_KV_NAMESPACE to the HTTP backend", async () => {
    const ns = uniqueNs();
    process.env["BEYOND_KV_URL"] = getHttpUrl();
    process.env["BEYOND_KV_NAMESPACE"] = ns;
    const kv = createServerKvClient();
    const key = uniqueKey();
    await kv.set(key, "namespaced");
    const { data: entry } = await kv.get(key);
    expect(entry).not.toBeNull();
  });
});
