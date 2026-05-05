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
console.log(entry?.text()); // "Jane Smith"

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

If you know your backend ahead of time, use the typed factories directly — they only expose options that apply to that backend:

```ts
import { createHttpKvClient, createRespKvClient } from "@beyond.dev/kv";

const http = createHttpKvClient({
  url: "http://localhost:4869",
  namespace: "prod",
});
const resp = createRespKvClient({ url: "redis://localhost:6379", db: 2 });
```

**RESP-only options (`KvRespClientOptions`):**

| Option | Type     | Default | Description                                |
| ------ | -------- | ------- | ------------------------------------------ |
| `db`   | `number` | `0`     | Database index (0–15), maps to a namespace |

**HTTP-only options (`KvHttpClientOptions`):**

| Option      | Type           | Default            | Description                 |
| ----------- | -------------- | ------------------ | --------------------------- |
| `namespace` | `string`       | `"default"`        | Namespace to use            |
| `fetch`     | `typeof fetch` | `globalThis.fetch` | Custom fetch implementation |

---

### `kv.get(key)`

```ts
const entry = await kv.get("my-key");
// entry is KvEntry | null

if (entry) {
  console.log(entry.text()); // decode as UTF-8
  console.log(entry.json<MyType>()); // parse as JSON
}
```

### `kv.getOrThrow(key)`

```ts
const entry = await kv.getOrThrow("my-key");
// throws KvNotFoundError if missing
console.log(entry.text());
```

### `kv.set(key, value, opts?)`

```ts
await kv.set("my-key", "value");
await kv.set("my-key", new Uint8Array([1, 2, 3]));
await kv.set("my-key", "value", { ttl: 60 }); // expires in 60s
await kv.set("my-key", "value", { ifAbsent: true }); // only if missing
await kv.set("my-key", "value", { ifPresent: true }); // only if present
await kv.set("my-key", "value", { ifMatch: entry.revision }); // compare-and-swap
await kv.set("my-key", "value", { metadata: { v: 1 } }); // HTTP backend only
```

Returns `void`. Throws `KvError` with status `409` if `ifAbsent`, `ifPresent`, or `ifMatch` conditions aren't met.

### `kv.delete(key)`

```ts
await kv.delete("my-key");
await kv.delete("my-key", { ifMatch: entry.revision }); // compare-and-delete
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

do {
  const page = await kv.list({ prefix: "user:", cursor, limit: 100 });
  for (const { name } of page.keys) {
    console.log(name);
  }
  cursor = page.nextCursor;
} while (cursor !== undefined);
```

### `kv.watch(key, opts?)`

```ts
for await (const event of kv.watch("config", { signal: ac.signal })) {
  if (event.type === "ready") {
    console.log("connected, initial state delivered");
  } else if (event.type === "set") {
    console.log(`${event.key} = ${event.text()}`); // event.value is Uint8Array
  } else if (event.type === "del") {
    console.log(`${event.key} deleted`);
  }
}
```

Watch a prefix:

```ts
for await (const event of kv.watch("cfg:", { prefix: true })) { ... }
```

Resume after disconnect:

```ts
for await (const event of kv.watch("cfg", { since: lastRevision })) { ... }
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
  value: Uint8Array; // raw bytes
  text(): string; // decode as UTF-8
  json<T>(): T; // parse as JSON
  ttl?: number; // remaining TTL in seconds
  metadata?: unknown; // arbitrary JSON — HTTP backend only
  revision: number; // monotonically increasing write timestamp (ms)
}
```

### `KvSetOptions`

```ts
interface KvSetOptions {
  ttl?: number;
  metadata?: unknown; // HTTP backend only
  ifAbsent?: boolean; // set only if key doesn't exist
  ifPresent?: boolean; // set only if key exists
  ifMatch?: number; // compare-and-swap: set only if revision matches
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
  nextCursor?: string; // absent when scan is complete
}
```

### `KvWatchEvent`

```ts
type KvWatchEvent =
  | { type: "ready" }
  | {
    type: "set";
    key: string;
    value: Uint8Array;
    ttl?: number;
    metadata?: unknown;
    revision: number;
  }
  | { type: "del"; key: string; revision: number };
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
  return Response.json({ value: entry?.text() ?? null });
}
```

Set these environment variables:

| Variable       | Required | Description                               |
| -------------- | -------- | ----------------------------------------- |
| `KV_URL`       | yes      | `redis://...` or `http://...`             |
| `KV_DB`        | no       | Database index for RESP (default: `0`)    |
| `KV_NAMESPACE` | no       | Namespace for HTTP (default: `"default"`) |

Throws if `KV_URL` is not set.
