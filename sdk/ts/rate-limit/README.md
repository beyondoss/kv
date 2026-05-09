# @beyond.dev/rate-limit

Enforce per-key request limits backed by `@beyond.dev/kv` — one call, no tokens-per-second math, no Redis configuration required.

## Quick Start

```sh
npm install @beyond.dev/rate-limit
```

```ts
import { createRateLimiter, slidingWindow } from "@beyond.dev/rate-limit";

const limiter = createRateLimiter({
  url: "https://your-kv.beyond.dev",
  algorithm: slidingWindow({ limit: 100, window: 60_000 }),
});

const { data, error } = await limiter.limit("user:123");

if (error) {
  // KV unreachable — fail open or closed, your call
  throw error;
}

if (!data.allowed) {
  return new Response("Too Many Requests", {
    status: 429,
    headers: { "Retry-After": String(Math.ceil(data.retryAfter! / 1000)) },
  });
}
```

## Algorithms

### `slidingWindow({ limit, window })` — recommended

Weighted two-bucket approximation. No burst at window boundaries. O(1) KV state.

```ts
slidingWindow({ limit: 100, window: 60_000 }); // 100 req/min
```

### `fixedWindow({ limit, window })`

Simple time buckets. Allows up to 2× the limit at window edges.

```ts
fixedWindow({ limit: 1000, window: 3_600_000 }); // 1000 req/hr
```

### `tokenBucket({ capacity, refillRate })`

Sustains a steady rate while absorbing bursts up to `capacity`.

```ts
tokenBucket({ capacity: 50, refillRate: 10 }); // bursts to 50, sustains 10 req/s
```

## Blocking Until Allowed

```ts
// Wait up to 5 seconds for a slot. Throws RateLimitError if timeout elapses.
const info = await limiter.blockFor("user:123", 5_000);
```

## Framework Middleware

All middleware set `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`, and `Retry-After` headers. All default to keying on client IP.

### Hono

```ts
import { createRateLimiter } from "@beyond.dev/rate-limit";
import { rateLimitMiddleware } from "@beyond.dev/rate-limit/hono";

const limiter = createRateLimiter({ url: "https://your-kv.beyond.dev" });

app.use(
  rateLimitMiddleware(limiter, {
    key: (c) => c.req.header("x-api-key") ?? "anon",
    skip: (c) => c.req.path === "/health",
    onDenied: (c, info) =>
      c.json({ error: "rate_limited", retryAfter: info.retryAfter }, 429),
  }),
);
```

### Next.js

```ts
// middleware.ts
import { createRateLimiter } from "@beyond.dev/rate-limit";
import { withRateLimit } from "@beyond.dev/rate-limit/next";

const limiter = createRateLimiter({ url: "https://your-kv.beyond.dev" });

export default withRateLimit(limiter, {
  skip: (req) => req.nextUrl.pathname === "/api/health",
});

export const config = { matcher: ["/((?!_next|favicon.ico).*)"] };
```

> Use an `http://` backend URL in Next.js edge middleware — the RESP protocol is not available in the edge runtime.

### Fastify

```ts
import { rateLimitPlugin } from "@beyond.dev/rate-limit/fastify";

await app.register(rateLimitPlugin, {
  limiter,
  key: (req) => req.headers["x-user-id"] as string ?? req.ip,
  onDenied: (req, reply, info) =>
    reply.code(429).send({ code: "RATE_LIMITED" }),
});
```

### Express

```ts
import { rateLimitMiddleware } from "@beyond.dev/rate-limit/express";

app.use(
  rateLimitMiddleware(limiter, {
    key: (req) => req.user?.id ?? req.ip,
    skip: (req) => req.path === "/health",
  }),
);
```

## Environment-Based Configuration

Use the singleton `rateLimit` export when configuration lives in environment variables.

```ts
import { rateLimit } from "@beyond.dev/rate-limit";

// Reads from:
// BEYOND_KV_URL (required)
// BEYOND_RATE_LIMIT_ALGORITHM  — "sliding" | "fixed" | "token" (default: "sliding")
// BEYOND_RATE_LIMIT_LIMIT      — default 100
// BEYOND_RATE_LIMIT_WINDOW     — default 60000 (ms)
// BEYOND_RATE_LIMIT_CAPACITY   — token bucket only
// BEYOND_RATE_LIMIT_REFILL_RATE — token bucket only

const { data, error } = await rateLimit.limit("user:123");
```

## Observability

```ts
const limiter = createRateLimiter({
  url: "https://your-kv.beyond.dev",
  algorithm: slidingWindow({ limit: 100, window: 60_000 }),
  onRequest: ({ command }) =>
    metrics.increment("ratelimit.request", { command }),
  onResponse: ({ command, durationMs, allowed }) =>
    metrics.histogram("ratelimit.duration", durationMs, { command, allowed }),
});
```

## API

### `createRateLimiter(opts)`

| Option       | Type                                  | Default                | Description                               |
| ------------ | ------------------------------------- | ---------------------- | ----------------------------------------- |
| `url`        | `string`                              | `BEYOND_KV_URL`        | KV backend URL                            |
| `algorithm`  | `Algorithm`                           | `slidingWindow({...})` | Rate limiting strategy                    |
| `keyPrefix`  | `string`                              | `"rl"`                 | KV namespace prefix                       |
| `timeout`    | `number`                              | —                      | Per-operation KV timeout (ms)             |
| `retries`    | `number`                              | `2`                    | Max retries on transient failures         |
| `onRequest`  | `(e: RateLimitRequestEvent) => void`  | —                      | Fires before each `limit`/`blockFor` call |
| `onResponse` | `(e: RateLimitResponseEvent) => void` | —                      | Fires after each operation                |

### `RateLimiter`

```ts
interface RateLimiter {
  limit(
    key: string,
  ): Promise<
    { data: RateLimitInfo; error: undefined } | {
      data: undefined;
      error: RateLimitError;
    }
  >;
  blockFor(key: string, timeoutMs: number): Promise<RateLimitInfo>;
  close(): Promise<void>;
}
```

### `RateLimitInfo`

```ts
{
  allowed: boolean;
  remaining: number;
  limit: number;
  reset: number;       // absolute timestamp (ms) when the window resets
  retryAfter?: number; // ms to wait before retrying (present when denied)
}
```

### `RateLimitError`

```ts
class RateLimitError extends Error {
  code: "timeout" | "kv_error";
  key: string;
  retryAfter?: number;
}
```
