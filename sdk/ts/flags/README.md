# @beyond.dev/flags

Evaluate feature flags against your KV store — with targeting rules, percentage rollouts, and user opt-in/out — in a single round-trip per request.

## Install

```sh
npm install @beyond.dev/flags @beyond.dev/kv
```

## Quick Start

```ts
import { createFlags } from "@beyond.dev/flags";
import { createClient } from "@beyond.dev/kv";

const kv = createClient({ url: process.env.BEYOND_KV_URL });
export const flags = createFlags(kv, { watch: true });

// Define flags at module level
export const newCheckout = flags("new-checkout", false);
export const searchVariant = flags("search-variant", ["off", "v1", "v2"]);

// Evaluate with explicit context — waits for snapshot automatically
const value = await newCheckout({ id: userId });
```

Or use the lazy singleton (reads `BEYOND_KV_URL` automatically):

```ts
import { flags } from "@beyond.dev/flags";

export const newCheckout = flags("new-checkout", false);
```

## Framework Middleware

Middleware establishes a per-request scope so flags evaluate without passing context on every call.

**Hono**

```ts
import { createFlags } from "@beyond.dev/flags";
import { flags } from "@beyond.dev/flags/hono";

const client = createFlags(kv);
app.use(flags(client, { context: (c) => ({ id: c.get("userId") ?? "anon" }) }));

app.get("/checkout", async (c) => {
  const enabled = await newCheckout(); // zero-arg — context comes from scope
  return c.json({ enabled });
});
```

**Express**

```ts
import { flags } from "@beyond.dev/flags/express";

app.use(flags(client, { context: (req) => ({ id: req.user?.id ?? "anon" }) }));
```

**Fastify**

```ts
import flagsPlugin from "@beyond.dev/flags/fastify";

await app.register(flagsPlugin, {
  client,
  context: (req) => ({ id: req.user?.id ?? "anon" }),
});
```

Install the relevant peer dependency for your framework:

```sh
npm install hono          # Hono
npm install express       # Express
npm install fastify fastify-plugin  # Fastify
npm install next          # Next.js
```

**Next.js (RSC)**

```ts
import { withFlags } from "@beyond.dev/flags/next";

export default async function Page() {
  return withFlags({ id: userId }, async () => {
    const enabled = await newCheckout();
    return <CheckoutPage enabled={enabled} />;
  });
}
```

## Vercel Flags SDK Adapter

Already using the [Vercel Flags SDK](https://flags-sdk.dev) (`flags/next`)? Back its `flag()` declarations with Beyond KV via the adapter — you keep the SDK's `flag()`, toolbar overrides, precompute, and discovery endpoint, and Beyond KV supplies the values (same defs, rules, rollout, and user prefs as the native API).

```sh
npm install @beyond.dev/flags @beyond.dev/kv flags
```

```ts
import { flag } from "flags/next";
import { createKvClient } from "@beyond.dev/kv";
import { beyondAdapter } from "@beyond.dev/flags/adapter";

const kv = createKvClient({ url: process.env.BEYOND_KV_URL });
const adapter = beyondAdapter(kv);

export const newCheckout = flag<boolean>({
  key: "new-checkout",
  defaultValue: false,
  adapter,
  // identify supplies the entities passed to evaluation; `id` is the bucket key.
  identify: ({ headers }) => ({
    id: headers.get("x-user-id") ?? "anon",
    plan: "free",
  }),
});

const enabled = await newCheckout(); // resolved by Beyond KV through the host
```

**Read modes** — `beyondAdapter(kv, { mode })`:

- `"snapshot"` (default) keeps an in-memory snapshot synced via KV watch/poll — zero round-trips per evaluation. Best for long-lived Node servers.
- `"request"` fetches defs per request (cached by request headers). Best for short-lived edge/serverless functions that can't hold a persistent watch.

**Batching** — share one `adapter` instance and one `identify` reference across flags, and the host's `evaluate([...])` resolves them in a single `bulkDecide` call.

**Toolbar discovery** — wire `adapter.getProviderData()` into the SDK's `createFlagsDiscoveryEndpoint`, merging with the host's `getProviderData(flags)` via `mergeProviderData` for code-side metadata.

> Note: per-user writes (`.set()`/`.reset()`) and `.when()` overrides are native-only — the adapter still _reads_ user prefs, but the Vercel toolbar replaces code overrides. Use the native `createFlags` API (above) if you want those.

## Flag Definitions

Store flag definitions in KV under `flags:def:<name>`. Definitions control behavior without deploys.

```ts
// FlagDef shape
{
  on: true,              // kill switch — false disables regardless of rules
  rules: [
    { when: { plan: 'pro' }, value: true }  // first-match wins
  ],
  rollout: { percent: 20, value: true }     // deterministic by user id
}
```

Bucketing for rollouts is deterministic: the same `id` always produces the same result for a given flag name, and different flags produce uncorrelated cohorts.

## Evaluation Precedence

Each flag call resolves through this chain, stopping at the first match:

1. **Kill switch** — `on: false` returns the default immediately
2. **User preference** — explicit opt-in/out stored per user
3. **Code override** — `.when()` escape hatch
4. **Rules** — first matching rule wins
5. **Rollout** — percentage bucket
6. **Default** — code-declared value

## User Preferences

Let users opt in or out. Preferences are stored in KV and override all operational state (rules, rollout).

```ts
await newCheckout.set({ id: userId }, true); // opt in
await newCheckout.reset({ id: userId }); // revert to ops state
```

## Code Overrides

Override before rules and rollout — useful for internal users, tests, or emergency bypasses.

```ts
newCheckout.when(({ context }) =>
  context.email?.endsWith("@beyond.dev") ? true : undefined
);
```

Return `undefined` to fall through to the next step.

## Context Typing

Extend `FlagContext` to add app-specific attributes that rules can match against.

```ts
declare module "@beyond.dev/flags" {
  interface FlagContext {
    plan?: "free" | "pro" | "enterprise";
    country?: string;
  }
}

// Now usable in rules and overrides
newCheckout.when(({ context }) =>
  context.plan === "enterprise" ? true : undefined
);
```

## Observability

```ts
const flags = createFlags(kv, {
  onEvaluate: (event) => {
    // event.name, event.value, event.reason, event.durationMs
    metrics.record("flag.eval", event);
  },
  onError: (event) => {
    logger.error({ source: event.source, err: event.error });
  },
});
```

`event.reason` is one of: `"default"` `"off"` `"user-pref"` `"override"` `"rule"` `"rollout"` `"no-snapshot"` `"error"`

## Options

| Option       | Type                               | Default | Description                                                   |
| ------------ | ---------------------------------- | ------- | ------------------------------------------------------------- |
| `watch`      | `boolean`                          | `true`  | Stream flag definition updates via KV watch                   |
| `refresh`    | `number`                           | `30`    | Poll interval in seconds (fallback when watch is unavailable) |
| `onEvaluate` | `(event: FlagEvent) => void`       | —       | Called after each evaluation                                  |
| `onError`    | `(event: FlagsErrorEvent) => void` | —       | Called on KV or snapshot errors                               |
