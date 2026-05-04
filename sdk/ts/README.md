# @beyond.dev/kv

Store and retrieve binary data — from any JavaScript runtime, over RESP or HTTP.

## Quick Start

```sh
npm install @beyond.dev/kv
```

```ts
import { createKvClient } from "@beyond.dev/kv";

const kv = createKvClient({ url: "redis://localhost:6379" });

await kv.set("user:123", "Jane Smith");
const entry = await kv.get("user:123");
const value = new TextDecoder().decode(entry!.value);

await kv.close();
```

The URL scheme picks the backend: `redis://` uses RESP, `http://` uses the HTTP API. Same interface, either way.

## Install

```sh
npm install @beyond.dev/kv
# or
pnpm add @beyond.dev/kv
```

Requires Node.js 18+.

## API

### `createKvClient(options)`

```ts
import { createKvClient } from "@beyond.dev/kv";

const kv = createKvClient({
  url: "redis://localhost:6379", // or http://localhost:4869
  timeout: 5000, // per-command timeout in ms
  retries: 2, // retry attempts on failure
  onCommand: (e) => {}, // called before each command
  onResponse: (e) => {}, // called after each command
});
```

**RESP-only options:**

| Option | Type     | Default | Description                                |
| ------ | -------- | ------- | ------------------------------------------ |
| `db`   | `number` | `0`     | Database index (0–15), maps to a namespace |

**HTTP-only options:**

| Option      | Type           | Default            | Description                 |
| ----------- | -------------- | ------------------ | --------------------------- |
| `namespace` | `string`       | `"default"`        | Namespace to use            |
| `fetch`     | `typeof fetch` | `globalThis.fetch` | Custom fetch implementation |

---

### `kv.get(key)`

```ts
const entry = await kv.get("my-key");
// entry is KvEntry | null
```

### `kv.getOrThrow(key)`

```ts
const entry = await kv.getOrThrow("my-key");
// throws KvNotFoundError if missing
```

### `kv.set(key, value, opts?)`

```ts
await kv.set("my-key", "value");
await kv.set("my-key", new Uint8Array([1, 2, 3]));
await kv.set("my-key", "value", { ttl: 60 }); // expires in 60s
await kv.set("my-key", "value", { nx: true }); // only if missing
await kv.set("my-key", "value", { xx: true }); // only if present
await kv.set("my-key", "value", { metadata: { v: 1 } }); // HTTP backend only
```

Returns `void`. Throws `KvError` with status `409` if `nx` or `xx` conditions aren't met.

### `kv.delete(key)`

```ts
await kv.delete("my-key");
```

### `kv.mget(keys)`

```ts
const entries = await kv.mget(["key1", "key2", "key3"]);
// (KvEntry | null)[]
```

### `kv.mset(entries)`

```ts
await kv.mset([
  { key: "a", value: "val-a" },
  { key: "b", value: "val-b", opts: { ttl: 60 } },
]);
```

Only `ttl` is supported in batch set options.

### `kv.list(opts?)`

```ts
let cursor: string | undefined;

while (true) {
  const page = await kv.list({ prefix: "user:", cursor, limit: 100 });
  for (const { name } of page.keys) {
    console.log(name);
  }
  if (page.complete) break;
  cursor = page.cursor;
}
```

### `kv.close()`

```ts
await kv.close();
```

Closes the underlying connection. Call when done.

---

## Types

### `KvEntry`

```ts
interface KvEntry {
  value: Uint8Array; // always binary — decode with TextDecoder if needed
  ttl?: number; // remaining TTL in seconds
  metadata?: unknown; // arbitrary JSON — HTTP backend only
}
```

### `KvSetOptions`

```ts
interface KvSetOptions {
  ttl?: number;
  metadata?: unknown; // HTTP backend only
  nx?: boolean; // set only if key doesn't exist
  xx?: boolean; // set only if key exists
}
```

### `KvListOptions` / `KvListResult`

```ts
interface KvListOptions {
  prefix?: string;
  cursor?: string;
  limit?: number;
}

interface KvListResult {
  keys: { name: string }[];
  cursor?: string;
  complete: boolean;
}
```

---

## Errors

```ts
import { KvError, KvNotFoundError } from "@beyond.dev/kv";

try {
  await kv.getOrThrow("missing");
} catch (err) {
  if (err instanceof KvNotFoundError) {
    console.log(err.key); // the key that wasn't found
  } else if (err instanceof KvError) {
    console.log(err.code, err.status, err.message);
  }
}
```

`KvNotFoundError` extends `KvError` with `code: "not_found"` and `status: 404`.

---

## Observability

```ts
const kv = createKvClient({
  url: "...",
  onCommand: ({ command, keyCount }) => {
    metrics.increment(`kv.command.${command.toLowerCase()}`, keyCount);
  },
  onResponse: ({ command, keyCount, durationMs }) => {
    metrics.timing(`kv.latency.${command.toLowerCase()}`, durationMs);
  },
});
```

---

## Next.js

```ts
import { createServerKvClient } from "@beyond.dev/kv/next";

export async function GET() {
  const kv = createServerKvClient();
  const entry = await kv.get("config");
  return Response.json({
    value: entry ? new TextDecoder().decode(entry.value) : null,
  });
}
```

Set these environment variables:

| Variable       | Required | Description                               |
| -------------- | -------- | ----------------------------------------- |
| `KV_URL`       | yes      | `redis://...` or `http://...`             |
| `KV_DB`        | no       | Database index for RESP (default: `0`)    |
| `KV_NAMESPACE` | no       | Namespace for HTTP (default: `"default"`) |

Throws if `KV_URL` is not set.
