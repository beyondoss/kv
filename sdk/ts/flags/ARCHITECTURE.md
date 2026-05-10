# `@beyond.dev/flags` Architecture

Takes a `FlagContext` (user/request attributes) and KV-stored flag state (`FlagDef`, `UserPrefs`) and produces a typed flag value with a deterministic reason — one KV round-trip per request regardless of how many flags are evaluated.

## Data Flow

### Flag Evaluation (happy path)

```
Request arrives
      │
      ▼
Middleware (hono/express/fastify/next)
  context(req) → FlagContext { id, ...attrs }
      │
      ├─ runWithScope(ctx)    ← wraps chain (Hono, Express, Next RSC)
      └─ enterScope(ctx)      ← one-way set (Fastify, Next edge)
      │
      ▼
Route handler: await flag()          (zero-arg)
  OR           await flag(ctx)       (explicit)
      │
      ├─ zero-arg: read context from AsyncLocalStorage scope
      │            fetch UserPrefs for scope.id (cached per-request)
      │
      └─ explicit: fetch UserPrefs for ctx.id (no cache)
      │
      ▼
evaluate(name, default, context, def, prefs, override)
  1. def absent          → "no-snapshot"  → default
  2. def.on === false    → "off"          → default
  3. name in prefs       → "user-pref"   → prefs[name]
  4. override(ctx) ≠ undefined → "override" → override(ctx)
  5. walk def.rules      → first match   → "rule" + ruleIndex
  6. bucket(id, name) < rollout.percent  → "rollout" → rollout.value
  7. fallthrough         → "default"     → default
      │
      ▼
emit FlagEvent { name, value, reason, durationMs, id, ruleIndex?, error? }
      │
      ▼
return typed value T
```

### Error paths

```
No ALS scope + zero-arg call  → throw FlagError("no_context")
ctx.id === ""                 → throw FlagError("missing_id")
eval throws                   → emit FlagEvent(reason: "error") + rethrow
UserPrefs fetch fails         → emit FlagsErrorEvent(source: "user-prefs"), treat as null
flag.set/reset fails (CAS)    → throw FlagError("kv_error") + emit FlagsErrorEvent
watch stream fails            → emit FlagsErrorEvent(source: "watch") + backoff + poll fallback
snapshot load fails           → emit FlagsErrorEvent(source: "snapshot")
```

### Snapshot sync

```
createFlags(kv, opts)
      │
      ▼
snapshot.start()
  ├─ loadAll(): list(flags:def:*) + batchGet() → in-memory Map
  └─ opts.watch !== false:
       kv.watch("flags:def:*") → stream deltas → apply to Map
       on error: exponential backoff (1s→60s) + poll fallback
     opts.watch === false (or fallback):
       setInterval(loadAll, refresh * 1000)   [timer unref'd]
      │
      ▼
flag eval: snapshot.get(name) → O(1) → FlagDef | undefined
```

### User pref mutation (CAS loop)

```
flag.set(ctx, value) / flag.reset(ctx)
      │
      ▼
for attempt 0..4:
  get(flags:user:{id}) → current entry + revision
  next = mutator(current)    (set: add key; reset: delete key)

  if next is empty:
    if entry exists → kv.delete(key)  [done]
    else            → no-op           [done]
  else if entry exists:
    kv.cas(key, next, revision)
      200 → done
      409 → retry
  else:
    kv.set(key, next, ifAbsent: true)
      200 → done
      409 (race) → retry

max retries exceeded → throw FlagError("kv_error")
```

## Concepts & Terminology

| Term          | What It Controls                                                              | NOT                                                 |
| ------------- | ----------------------------------------------------------------------------- | --------------------------------------------------- |
| `FlagContext` | Input to every eval; source of `id` for bucketing and pref lookup             | Not persisted; rebuilt per-request                  |
| `FlagDef`     | KV-stored kill switch, rules, rollout — the ops-managed half of a flag        | Not the type or default (those are in code)         |
| `UserPrefs`   | Per-`id` sparse map of opted-in flag values stored at `flags:user:{id}`       | Not a profile; only non-default flags appear        |
| `Scope`       | AsyncLocalStorage slot holding the current request's context + cached prefs   | Not a session; lives for one request only           |
| `Snapshot`    | In-memory `Map<name, FlagDef>` kept fresh via watch or polling                | Not the source of truth; KV is                      |
| `Rollout`     | Deterministic % of ids that get a value: `fnv1a32(id + name) % 100 < percent` | Not random; same id always buckets the same way     |
| `Rule`        | First-match-wins partial context match; every present key must match          | Not a fallback; runs before rollout                 |
| `Override`    | Code-level `flag.when(ctx => ...)` — highest precedence after user-prefs      | Not stored in KV; compiled into the flag definition |
| `EvalReason`  | Why a particular value was returned (e.g. "rule", "rollout", "off")           | Not a priority level; just provenance               |

## Core Mechanisms

### Evaluation precedence

The eval engine in `eval.ts` (called from `flag.ts`) applies checks in strict order — the first non-`undefined` result wins:

```
user-pref > override > rule > rollout > default
```

`on === false` short-circuits before all of the above and always returns the flag's `default`.

`def` absent (no snapshot yet) short-circuits before even `on`, returning `default` with reason `"no-snapshot"`.

### Deterministic bucketing (`hash.ts`)

```ts
bucket(id, flagName) = fnv1a32(`${id}:${flagName}`) % 100
```

- 32-bit FNV-1a: fast, stable across Node/Deno/Bun/workerd
- Combining `id + flagName` ensures different flags produce uncorrelated cohorts
- Same `id + flagName` → same bucket → stable across requests and deployments

### Per-request pref caching (`als.ts`)

`FlagsScope` holds `prefs` (resolved) and `prefsPromise` (in-flight). The first zero-arg `flag()` call within a scope fetches prefs and stores them. Every subsequent call in the same scope reuses them — one KV round-trip per request regardless of how many flags are evaluated. Explicit-context evals (`await flag(ctx)`) skip the scope cache and always fetch directly.

### Rule matching

A rule matches when every key in `rule.when` satisfies its constraint against `context`:

- `undefined` constraint → skip (wildcard)
- array constraint → `context[key]` must equal one element (any-of)
- scalar constraint → `context[key]` must strictly equal it

Rules are walked in declaration order; the first match wins. `ruleIndex` in `FlagEvent` reports which rule matched.

### Context augmentation

`Context` is an empty augmentable interface. Apps extend it via module declaration merging:

```ts
declare module "@beyond.dev/flags" {
  interface Context {
    plan: "free" | "pro" | "enterprise";
    country: string;
  }
}
```

This makes `ctx.plan` and `ctx.country` typed everywhere — in `flag.when()` overrides, rule `when` constraints, and middleware `context()` builders.

## ALS Scope Propagation by Framework

Different frameworks expose different hooks, requiring two propagation strategies:

| Framework    | Strategy     | Function                                | Scope lifetime                       |
| ------------ | ------------ | --------------------------------------- | ------------------------------------ |
| Hono         | Wrap chain   | `runWithScope(ctx, next)`               | duration of `next()`                 |
| Express      | Wrap chain   | `runWithScope(ctx, next)`               | duration of `next()`                 |
| Fastify      | One-way set  | `enterScope(ctx)`                       | entire request context               |
| Next.js RSC  | Wrap body    | `withFlags(ctx, body)` = `runWithScope` | duration of `body()`                 |
| Next.js edge | Wrap handler | `runWithScope(ctx, handler)`            | middleware only — not route handlers |

**Important for Next.js edge middleware**: the scope established in `middleware.ts` does **not** propagate into App Router route handlers — Next dispatches them in a separate async context. Route handlers should use explicit `await flag(ctx)` instead.

## KV Schema

| Key                | Value                     | Written by                    |
| ------------------ | ------------------------- | ----------------------------- |
| `flags:def:<name>` | `FlagDef` JSON            | CLI / ops tooling             |
| `flags:user:<id>`  | `UserPrefs` JSON (sparse) | `flag.set()` / `flag.reset()` |

`UserPrefs` is sparse: only flags where the user's value differs from the flag's `default` are stored. An empty `UserPrefs` object results in the key being deleted.

## File Map

| File                                | What It Does                                                                                                                  |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `src/types.ts`                      | All public types: `FlagContext`, `Rule`, `Rollout`, `FlagDef`, `UserPrefs`, `FlagEvent`, `FlagsErrorEvent`, `EvalReason`      |
| `src/errors.ts`                     | `FlagError` with machine-readable `code` field (`no_context`, `missing_id`, `kv_error`, `invalid_state`, `watch_unavailable`) |
| `src/als.ts`                        | `AsyncLocalStorage` wrapper: `FlagsScope`, `currentScope()`, `runWithScope()`, `enterScope()`                                 |
| `src/flag.ts`                       | `Flag<T>` public interface and `FlagRuntime` internal interface; `makeFlag()` factory                                         |
| `src/flags.ts`                      | `FlagsClient`, `createFlags()`, lazy `flags` singleton; `mutateUserPrefs()` CAS loop; `Runtime` impl                          |
| `src/eval.ts`                       | Pure synchronous `evaluate()` — the 7-step precedence chain                                                                   |
| `src/hash.ts`                       | `fnv1a32()` and `bucket(id, flagName)` for deterministic rollouts                                                             |
| `src/snapshot.ts`                   | `Snapshot` class: in-memory `Map<name, FlagDef>`, watch+polling sync, `fetchUserPrefs()`                                      |
| `src/middleware/hono.ts`            | Hono `MiddlewareHandler` — wraps chain with `runWithScope`                                                                    |
| `src/middleware/express.ts`         | Express `RequestHandler` — wraps chain with `runWithScope`, errors via `next(err)`                                            |
| `src/middleware/fastify.ts`         | Fastify plugin — `onRequest` hook uses `enterScope` (can't wrap chain)                                                        |
| `src/middleware/next.ts`            | `withFlags(ctx, body)` — RSC helper, thin `runWithScope` wrapper                                                              |
| `src/middleware/next-middleware.ts` | Next.js edge `NextMiddleware` factory — scope lives only in middleware, not route handlers                                    |

## Snapshot Lifecycle

```
UNSTARTED
    │ .start()
    ▼
LOADING
    │ loadAll() resolves
    ├─ ready promise resolves
    ▼
SYNCING
    ├─ watch mode: streaming kv.watch("flags:def:*") deltas
    │   on error → backoff + polling fallback
    └─ poll mode: setInterval(loadAll, refresh * 1000)
    │ .close()
    ▼
CLOSED
    ├─ watch stream aborted
    └─ polling timer cleared
```

## Configuration

| Option       | Default | Runtime Effect                                                                       |
| ------------ | ------- | ------------------------------------------------------------------------------------ |
| `refresh`    | `30`    | Polling interval in seconds when watch is disabled or as fallback                    |
| `watch`      | `true`  | When true, streams KV change events for sub-second propagation; false forces polling |
| `onEvaluate` | —       | Called after every `flag()` call with `FlagEvent`; must not throw                    |
| `onError`    | —       | Called for snapshot/watch/pref failures with `FlagsErrorEvent`; never blocks eval    |

**Environment variable**: `BEYOND_KV_URL` is read by the lazy `flags` singleton to construct the KV client. `createFlags(kv)` bypasses this for explicit client injection.

## Failure Modes

| Failure                                    | What Actually Happens                                                    | Recovery                                              |
| ------------------------------------------ | ------------------------------------------------------------------------ | ----------------------------------------------------- |
| Zero-arg flag call with no active scope    | Throws `FlagError("no_context")`                                         | Ensure middleware is registered before route handlers |
| `ctx.id === ""`                            | Throws `FlagError("missing_id")`                                         | Middleware must supply a non-empty id                 |
| Snapshot not yet loaded                    | Returns flag's `default` with reason `"no-snapshot"`                     | Await `client.ready()` at startup                     |
| Watch stream error                         | `onError` called; backoff + falls back to polling                        | Auto-recovers; no eval interruption                   |
| User prefs KV fetch fails                  | `onError` called; prefs treated as `null`                                | Evals continue, user-pref branch skipped              |
| `flag.set/reset` CAS conflict (≤4 retries) | Retries; emits `onError` + throws `FlagError("kv_error")` on max retries | Caller must handle                                    |
| `BEYOND_KV_URL` not set                    | Default `flags` singleton throws at first call                           | Set env var or use `createFlags(kv)`                  |
| KV entry has invalid JSON                  | `onError` called; flag treated as absent (`"no-snapshot"`)               | Fix via CLI                                           |

## Why It Behaves This Way

### Why user-prefs rank above code overrides

User prefs represent an explicit, per-user decision (opt-in/out). Overrides are code-level defaults for a group. Honoring the user's specific choice above a group default matches user expectations and prevents ops rollouts from silently undoing user preferences.

### Why `enterScope` instead of `runWithScope` for Fastify

Fastify's `onRequest` hook fires outside the route handler's call stack — there's no `next()` to wrap. `AsyncLocalStorage.enterWith()` propagates the store forward into all subsequent async work in the same context, which is exactly the request lifetime needed here. `runWithScope` requires a synchronous boundary to wrap.

### Why the Next.js edge scope doesn't reach route handlers

Next.js App Router runs edge middleware and route handlers in separate V8 contexts (different worker invocations). ALS state doesn't cross that boundary. Documenting this constraint prevents the silent bug of deploying middleware that sets up a scope that never reaches the code that needs it.

### Why bucketing uses `id + flagName` (not just `id`)

Using only `id` would mean a user in the 20% cohort for flag A would also be in the first 20% for every other flag — cohorts would be perfectly correlated. Combining `id + flagName` decorrelates cohorts across flags so rollouts are independent.

### Why prefs are sparse (only non-defaults stored)

Most users never opt in or out of any flag. Storing only deviations means the `flags:user:{id}` key doesn't exist for most ids, keeping KV storage proportional to actual customization rather than user count × flag count.
