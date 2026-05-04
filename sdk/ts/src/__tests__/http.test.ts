import { describe, expect, it } from "vitest";
import { KvError, KvNotFoundError } from "../errors.js";
import { dec, enc, getHttpUrl, httpClient, uniqueKey, uniqueNs } from "./harness.js";

// Each test gets its own namespace via httpClient() so tests never share state.

describe("HTTP backend — get / set / delete", () => {
  it("get returns null for a missing key", async () => {
    const kv = httpClient();
    expect(await kv.get(uniqueKey())).toBeNull();
  });

  it("get returns the stored value", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "hello");
    const entry = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("hello");
  });

  it("get round-trips binary data", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const bytes = new Uint8Array([0, 1, 127, 128, 255]);
    await kv.set(key, bytes);
    const entry = await kv.get(key);
    expect(entry?.value).toEqual(bytes);
  });

  it("get returns ttl when set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v", { ttl: 60 });
    const entry = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });

  it("get returns undefined ttl when not set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    const entry = await kv.get(key);
    expect(entry?.ttl).toBeUndefined();
  });

  it("get returns metadata when set", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    const meta = { region: "us-east-1", version: 42 };
    await kv.set(key, "v", { metadata: meta });
    const entry = await kv.get(key);
    expect(entry?.metadata).toEqual(meta);
  });

  it("set overwrites an existing key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "first");
    await kv.set(key, "second");
    const entry = await kv.get(key);
    expect(dec(entry!.value)).toBe("second");
  });

  it("delete removes a key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.delete(key);
    expect(await kv.get(key)).toBeNull();
  });

  it("delete on a missing key does not throw", async () => {
    const kv = httpClient();
    await expect(kv.delete(uniqueKey())).resolves.toBeUndefined();
  });
});

describe("HTTP backend — getOrThrow", () => {
  it("throws KvNotFoundError for a missing key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await expect(kv.getOrThrow(key)).rejects.toSatisfy(
      (e) => e instanceof KvNotFoundError && e.key === key && e.status === 404,
    );
  });

  it("returns the entry for an existing key", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "found");
    const entry = await kv.getOrThrow(key);
    expect(dec(entry.value)).toBe("found");
  });
});

describe("HTTP backend — NX / XX", () => {
  it("nx succeeds on a missing key", async () => {
    const kv = httpClient();
    await expect(kv.set(uniqueKey(), "v", { nx: true })).resolves.toBeUndefined();
  });

  it("nx throws KvError(409) when the key already exists", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "original");
    await expect(kv.set(key, "new", { nx: true })).rejects.toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );
    expect(dec((await kv.get(key))!.value)).toBe("original");
  });

  it("xx succeeds when the key exists", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.set(key, "old");
    await expect(kv.set(key, "new", { xx: true })).resolves.toBeUndefined();
    expect(dec((await kv.get(key))!.value)).toBe("new");
  });

  it("xx throws KvError(409) when the key does not exist", async () => {
    const kv = httpClient();
    await expect(kv.set(uniqueKey(), "v", { xx: true })).rejects.toSatisfy(
      (e) => e instanceof KvError && e.status === 409,
    );
  });
});

describe("HTTP backend — list", () => {
  it("returns an empty result for an empty namespace", async () => {
    const kv = httpClient();
    const result = await kv.list();
    expect(result.keys).toHaveLength(0);
    expect(result.complete).toBe(true);
  });

  it("returns all keys inserted into the namespace", async () => {
    const kv = httpClient();
    const keys = [uniqueKey("a"), uniqueKey("b"), uniqueKey("c")];
    await kv.mset(keys.map((key) => ({ key, value: "v" })));

    const result = await kv.list();
    const names = result.keys.map((k) => k.name);
    for (const key of keys) {
      expect(names).toContain(key);
    }
    expect(result.complete).toBe(true);
  });

  it("filters by prefix", async () => {
    const kv = httpClient();
    const prefix = `pfx:${crypto.randomUUID()}`;
    const matching = [`${prefix}:a`, `${prefix}:b`];
    const other = [uniqueKey("other")];
    await kv.mset([...matching, ...other].map((key) => ({ key, value: "v" })));

    const result = await kv.list({ prefix });
    const names = result.keys.map((k) => k.name);
    expect(names.sort()).toEqual(matching.sort());
  });

  it("paginates correctly using cursor", async () => {
    const kv = httpClient();
    const prefix = `page:${crypto.randomUUID()}`;
    const total = 5;
    const allKeys = Array.from({ length: total }, (_, i) => `${prefix}:${i}`);
    await kv.mset(allKeys.map((key) => ({ key, value: "v" })));

    const seen: string[] = [];
    let cursor: string | undefined;
    let complete = false;

    while (!complete) {
      const page = await kv.list({ prefix, cursor, limit: 2 });
      seen.push(...page.keys.map((k) => k.name));
      complete = page.complete;
      cursor = page.cursor;
    }

    expect(seen.sort()).toEqual(allKeys.sort());
  });
});

describe("HTTP backend — mget / mset", () => {
  it("mget returns null for missing keys and values for present keys", async () => {
    const kv = httpClient();
    const existing = uniqueKey();
    const missing = uniqueKey();
    await kv.set(existing, "hi");
    const results = await kv.mget([existing, missing]);
    expect(results).toHaveLength(2);
    expect(dec(results[0]!.value)).toBe("hi");
    expect(results[1]).toBeNull();
  });

  it("mget with empty array returns empty array", async () => {
    const kv = httpClient();
    expect(await kv.mget([])).toEqual([]);
  });

  it("mset sets all entries atomically", async () => {
    const kv = httpClient();
    const entries = [
      { key: uniqueKey(), value: "one" },
      { key: uniqueKey(), value: "two" },
      { key: uniqueKey(), value: enc("three") },
    ];
    await kv.mset(entries);
    const results = await kv.mget(entries.map((e) => e.key));
    expect(dec(results[0]!.value)).toBe("one");
    expect(dec(results[1]!.value)).toBe("two");
    expect(dec(results[2]!.value)).toBe("three");
  });

  it("mset with empty array is a no-op", async () => {
    const kv = httpClient();
    await expect(kv.mset([])).resolves.toBeUndefined();
  });

  it("mset respects per-entry ttl", async () => {
    const kv = httpClient();
    const key = uniqueKey();
    await kv.mset([{ key, value: "v", opts: { ttl: 60 } }]);
    const entry = await kv.get(key);
    expect(entry?.ttl).toBeGreaterThan(0);
    expect(entry?.ttl).toBeLessThanOrEqual(60);
  });
});

describe("HTTP backend — namespace isolation", () => {
  it("keys in different namespaces do not overlap", async () => {
    const nsA = uniqueNs();
    const nsB = uniqueNs();
    const url = getHttpUrl();
    const { createKvClient } = await import("../client.js");
    const kvA = createKvClient({ url, namespace: nsA });
    const kvB = createKvClient({ url, namespace: nsB });

    const key = uniqueKey();
    await kvA.set(key, "in-a");

    expect(await kvB.get(key)).toBeNull();
    expect(dec((await kvA.get(key))!.value)).toBe("in-a");
  });
});

describe("HTTP backend — observability hooks", () => {
  it("fires onCommand and onResponse for each operation", async () => {
    const commands: string[] = [];
    const responses: string[] = [];
    const { createKvClient } = await import("../client.js");
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onCommand: (e) => commands.push(e.command),
      onResponse: (e) => responses.push(e.command),
    });

    const key = uniqueKey();
    await kv.set(key, "v");
    await kv.get(key);
    await kv.delete(key);

    expect(commands).toEqual(["SET", "GET", "DEL"]);
    expect(responses).toEqual(["SET", "GET", "DEL"]);
  });

  it("onResponse includes a non-negative durationMs", async () => {
    const durations: number[] = [];
    const { createKvClient } = await import("../client.js");
    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      onResponse: (e) => durations.push(e.durationMs),
    });

    await kv.set(uniqueKey(), "v");
    expect(durations[0]).toBeGreaterThanOrEqual(0);
  });

  it("onMetadataParseError is called when x-kv-metadata is invalid JSON", async () => {
    const errors: unknown[] = [];
    let capturedKey = "";
    const { createKvClient } = await import("../client.js");

    // Intercept fetch to inject a bad metadata header on GET responses.
    const realFetch = globalThis.fetch;
    const ns = uniqueNs();
    const key = uniqueKey();

    const mockFetch: typeof fetch = async (input, init) => {
      const res = await realFetch(input, init);
      if (res.status === 200) {
        const headers = new Headers(res.headers);
        headers.set("x-kv-metadata", "{bad json}");
        return new Response(res.body, { status: res.status, headers });
      }
      return res;
    };

    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: ns,
      fetch: mockFetch,
      onMetadataParseError: (k, _raw, err) => {
        capturedKey = k;
        errors.push(err);
      },
    });

    await kv.set(key, "v");
    await kv.get(key);

    expect(errors).toHaveLength(1);
    expect(capturedKey).toBe(key);
  });
});

describe("HTTP backend — retry on 5xx", () => {
  it("retries on 5xx and succeeds on subsequent attempt", async () => {
    let attempt = 0;
    const { createKvClient } = await import("../client.js");
    const ns = uniqueNs();
    const key = uniqueKey();

    // First call returns 503; subsequent calls delegate to the real server.
    const realFetch = globalThis.fetch;
    const mockFetch: typeof fetch = async (input, init) => {
      if (attempt++ === 0) {
        return new Response("Service Unavailable", { status: 503 });
      }
      return realFetch(input, init);
    };

    const kv = createKvClient({ url: getHttpUrl(), namespace: ns, fetch: mockFetch, retries: 2 });
    await kv.set(key, "v");
    expect(attempt).toBeGreaterThan(1);
  });
});

describe("HTTP backend — key encoding edge cases", () => {
  it.each([
    ["key with spaces", "key with spaces"],
    ["path/like/key", "path/like/key"],
    ["key?with=query", "key?with=query"],
    ["unicode 日本語", "unicode 日本語"],
    ["colon:key", "colon:key"],
  ])("round-trips key %s", async (_label, key) => {
    const kv = httpClient();
    await kv.set(key, "v");
    const entry = await kv.get(key);
    expect(entry).not.toBeNull();
    expect(dec(entry!.value)).toBe("v");
    expect(await kv.get(key + "-missing")).toBeNull();
  });
});

describe("HTTP backend — timeout", () => {
  it("aborts the request when the timeout elapses", async () => {
    const { createKvClient } = await import("../client.js");

    // Fetch that hangs forever until aborted.
    const hangingFetch: typeof fetch = (_input, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
      });

    const kv = createKvClient({
      url: getHttpUrl(),
      namespace: uniqueNs(),
      fetch: hangingFetch,
      timeout: 50,
      retries: 0,
    });

    await expect(kv.get(uniqueKey())).rejects.toThrow();
  });
});

describe("HTTP backend — close", () => {
  it("close() resolves without error", async () => {
    const kv = httpClient();
    await expect(kv.close()).resolves.toBeUndefined();
  });
});
