# @beyond.dev/kv

Read and write keys against Beyond KV from TypeScript. Auto-selects RESP or HTTP backend from the URL — same API either way.

## Install

```sh
npm install @beyond.dev/kv
```

## Quick start

```typescript
import { createKvClient } from "@beyond.dev/kv";

const kv = createKvClient({ url: "https://kv.example.beyond.dev" });

await kv.set("user:1", JSON.stringify({ name: "Alice" }));
const { data, error } = await kv.get("user:1");
if (data) console.log(data.json()); // { name: "Alice" }
```

Operations never throw. Errors surface in `result.error`.

## Schema-typed keys

Declare key patterns once, get typed reads and writes everywhere:

```typescript
import { z } from "zod";

const kv = createKvClient({
  url: "https://kv.example.beyond.dev",
  schema: {
    "user:*": z.object({ name: z.string(), email: z.string() }),
    "session:*": z.object({ token: z.string(), expiresAt: z.number() }),
  },
});

await kv.set("user:1", { name: "Alice", email: "alice@example.com" });
const { data: user } = await kv.get("user:1"); // { name: string; email: string } | null
```

Works with Zod, ArkType, or any library with a `.parse()` method.

## TTL

```typescript
// Expire in 60 seconds
await kv.set("session:abc", token, { ttlSecs: 60 });

// Adjust expiry without rewriting the value
await kv.expire("session:abc", { ttlSecs: 300 });
await kv.expire("session:abc", { persist: true }); // Remove TTL
```

## Atomic operations

```typescript
// Atomic increment / decrement
const { data: count } = await kv.incr("hits");
const { data: count } = await kv.decr("credits", 5);

// Swap and return the old value
const { data: prev } = await kv.getAndSet("lock", "owner-id");

// Compare-and-swap — succeeds only if revision matches
const { data: entry } = await kv.get("config");
const { data: rev } = await kv.cas("config", newValue, entry?.revision ?? 0);
```

## Batch

```typescript
const { data: results } = await kv.batch([
  { type: "get", key: "user:1" },
  { type: "set", key: "user:2", value: "..." },
  { type: "delete", key: "user:3" },
  { type: "incr", key: "counter" },
]);
```

## Watch

```typescript
for await (const event of kv.watch("user:1")) {
  if (event.type === "set") console.log("updated:", event.entry);
  if (event.type === "del") console.log("deleted");
}

// Watch all keys under a prefix
for await (const event of kv.watch("user:", { prefix: true })) {
  console.log(event.key, event.type);
}
```

## Next.js

```typescript
// app/api/route.ts
import { createServerKvClient } from "@beyond.dev/kv/next";

export async function GET() {
  const kv = createServerKvClient();
  const { data } = await kv.get("my-key");
  return Response.json({ value: data?.json() ?? null });
}
```

`createServerKvClient` reads `BEYOND_KV_URL` automatically. Works in Server Components and Route Handlers.

## Environment variables

| Variable              | Required | Description                                                 |
| --------------------- | -------- | ----------------------------------------------------------- |
| `BEYOND_KV_URL`       | Yes      | Server URL (`redis://`, `rediss://`, `http://`, `https://`) |
| `BEYOND_KV_DB`        | No       | Database index 0–15 (RESP backend only)                     |
| `BEYOND_KV_NAMESPACE` | No       | Namespace name (HTTP backend only)                          |

## Backends

| URL scheme              | Backend        |
| ----------------------- | -------------- |
| `redis://`, `rediss://` | RESP (ioredis) |
| `http://`, `https://`   | HTTP (fetch)   |

The `KvClient` interface is identical for both.
