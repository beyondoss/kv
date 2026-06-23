/**
 * Second e2e tranche: broader Vercel Flags SDK surface, all through the REAL
 * host (`flags@4.2.0`). Complements e2e-flags-sdk.test.ts (core decision path)
 * by covering the integration surfaces a "compatible" adapter must coexist with:
 *
 *   1. non-boolean values (string variants, JSON/object flags)
 *   2. precompute round-trip (evaluate → serialize → getPrecomputed / flag(code))
 *   3. Vercel Toolbar override cookie (must win over KV, must skip our decide)
 *   4. discovery endpoint + mergeProviderData (auth via FLAGS_SECRET access proof)
 *
 * As before we never call the adapter directly — the host does. Keys/ids are
 * per-test UUIDs because the test KV server shares one keyspace (see
 * http.ts `nsToIndex`).
 */
import { randomBytes } from "node:crypto";
import type { KvClient } from "@beyond.dev/kv";
import { createAccessProof, encryptOverrides, mergeProviderData } from "flags";
import {
  createFlagsDiscoveryEndpoint,
  evaluate,
  flag,
  getPrecomputed,
  getProviderData,
  serialize,
} from "flags/next";
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";
import { beyondAdapter } from "../adapter.js";
import type { FlagContext } from "../types.js";
import { kvClient, writeDef } from "./harness.js";
import "./test-context.js";

const uid = () => crypto.randomUUID();

// The host signs/encrypts with a base64url-encoded 256-bit key.
const SECRET = randomBytes(32).toString("base64url");

// biome-ignore lint/suspicious/noExplicitAny: minimal headless request stub
function request(headers: Record<string, string> = {}): any {
  return { headers };
}

describe("e2e surface: real flags/next host → beyond adapter → real KV", () => {
  let kv: KvClient;
  let prevSecret: string | undefined;

  beforeAll(() => {
    // The override-cookie and precompute paths read process.env.FLAGS_SECRET as
    // a default; set it for the duration of this file.
    prevSecret = process.env["FLAGS_SECRET"];
    process.env["FLAGS_SECRET"] = SECRET;
  });

  afterAll(() => {
    if (prevSecret === undefined) delete process.env["FLAGS_SECRET"];
    else process.env["FLAGS_SECRET"] = prevSecret;
  });

  beforeEach(() => {
    kv = kvClient();
  });

  // ── 1. Non-boolean values ────────────────────────────────────────────────

  it("resolves a string-variant flag end to end", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rules: [{ when: { plan: "pro" }, value: "v2" }],
      rollout: { percent: 100, value: "v1" },
    });
    const adapter = beyondAdapter<string>(kv, { mode: "request" });
    const variant = flag<string, FlagContext>({
      key,
      defaultValue: "off",
      options: ["off", "v1", "v2"],
      adapter,
      identify: ({ headers }) => ({
        id: headers.get("x-user-id") ?? "anon",
        plan: (headers.get("x-plan") as FlagContext["plan"]) ?? "free",
      }),
    });
    try {
      // pro → rule "v2"; free → rollout "v1".
      expect(await variant(request({ "x-user-id": "u", "x-plan": "pro" }))).toBe(
        "v2",
      );
      expect(
        await variant(request({ "x-user-id": "u", "x-plan": "free" })),
      ).toBe("v1");
    } finally {
      await adapter.close();
    }
  });

  it("resolves a JSON/object flag end to end", async () => {
    type Config = { theme: string; max: number };
    const key = uid();
    const def = { theme: "dark", max: 5 };
    await writeDef(kv, key, { on: true, rollout: { percent: 100, value: def } });
    const adapter = beyondAdapter<Config>(kv, { mode: "request" });
    const config = flag<Config>({
      key,
      defaultValue: { theme: "light", max: 1 },
      adapter,
      identify: () => ({ id: "u" }),
    });
    try {
      expect(await config(request())).toEqual({ theme: "dark", max: 5 });
      // Kill switch → declared default object.
      await writeDef(kv, key, { on: false });
      expect(await config(request())).toEqual({ theme: "light", max: 1 });
    } finally {
      await adapter.close();
    }
  });

  // ── 2. Precompute round-trip ─────────────────────────────────────────────

  it("precompute round-trip: evaluate → serialize → getPrecomputed / flag(code)", async () => {
    const key = uid();
    await writeDef(kv, key, {
      on: true,
      rollout: { percent: 100, value: "v2" },
    });
    const adapter = beyondAdapter<string>(kv, { mode: "request" });
    const variant = flag<string>({
      key,
      defaultValue: "off",
      options: ["off", "v1", "v2"],
      adapter,
      identify: () => ({ id: "u" }),
    });
    try {
      // Evaluate live (headless, with a request), then serialize to a signed code.
      const values = await evaluate([variant], request());
      expect(values).toEqual(["v2"]);
      const code = await serialize([variant], values, SECRET);
      expect(typeof code).toBe("string");

      // Read the value back from the code two ways — both are decode-only
      // (no KV, no request), proving the precompute contract holds.
      expect(await getPrecomputed(variant, [variant], code, SECRET)).toBe("v2");
      // The flag's precomputed call shape: flag(code, group, secret).
      // biome-ignore lint/suspicious/noExplicitAny: precomputed call shape
      expect(await (variant as any)(code, [variant], SECRET)).toBe("v2");
    } finally {
      await adapter.close();
    }
  });

  // ── 3. Toolbar override cookie ───────────────────────────────────────────

  it("override cookie wins over KV and skips our decide", async () => {
    const key = uid();
    // KV says true (100% rollout); the override will force false.
    await writeDef(kv, key, { on: true, rollout: { percent: 100 } });
    const adapter = beyondAdapter<boolean>(kv, { mode: "request" });
    const decideSpy = vi.spyOn(adapter, "decide");
    const f = flag<boolean>({
      key,
      defaultValue: false,
      adapter,
      identify: () => ({ id: "u" }),
    });
    try {
      // Sanity: without an override, KV drives it true.
      expect(await f(request())).toBe(true);
      expect(decideSpy).toHaveBeenCalledTimes(1);

      // With the toolbar override cookie, the host short-circuits to the
      // override value and never calls decide for that flag.
      decideSpy.mockClear();
      const cookie = await encryptOverrides({ [key]: false }, SECRET);
      expect(
        await f(request({ cookie: `vercel-flag-overrides=${cookie}` })),
      ).toBe(false);
      expect(decideSpy).not.toHaveBeenCalled();
    } finally {
      await adapter.close();
    }
  });

  // ── 4. Discovery endpoint + mergeProviderData ────────────────────────────

  it("discovery endpoint serves merged provider data and enforces auth", async () => {
    const kvKey = uid();
    const codeKey = uid();
    await writeDef(kv, kvKey, { on: true });
    const adapter = beyondAdapter<boolean>(kv, {
      mode: "request",
      origin: (k) => `https://flags.example/${k}`,
    });
    // A flag declared in code (description lives here, not in KV).
    const declared = flag<boolean>({
      key: codeKey,
      defaultValue: false,
      description: "Declared in code",
      adapter,
      identify: () => ({ id: "u" }),
    });

    const endpoint = createFlagsDiscoveryEndpoint(
      async () =>
        mergeProviderData([
          // biome-ignore lint/suspicious/noExplicitAny: host typing friction under exactOptionalPropertyTypes
          getProviderData({ [codeKey]: declared } as any),
          adapter.getProviderData(),
        ]),
      { secret: SECRET },
    );

    try {
      // Authorized request → 200 with merged definitions + sdk-version header.
      const proof = await createAccessProof(SECRET);
      const ok = await endpoint(
        new Request("https://test/.well-known/vercel/flags", {
          headers: { Authorization: `Bearer ${proof}` },
          // biome-ignore lint/suspicious/noExplicitAny: NextRequest stand-in; endpoint only reads headers
        }) as any,
      );
      expect(ok.status).toBe(200);
      expect(ok.headers.get("x-flags-sdk-version")).toBeTruthy();
      const body = (await ok.json()) as {
        definitions: Record<string, { description?: string; origin?: string }>;
      };
      // Code-declared metadata survives the merge…
      expect(body.definitions[codeKey]?.description).toBe("Declared in code");
      // …and the KV-sourced flag (with adapter origin) is present too.
      expect(body.definitions[kvKey]).toBeDefined();
      expect(body.definitions[kvKey]?.origin).toBe(
        `https://flags.example/${kvKey}`,
      );

      // Unauthorized request → 401.
      const denied = await endpoint(
        // biome-ignore lint/suspicious/noExplicitAny: NextRequest stand-in
        new Request("https://test/.well-known/vercel/flags") as any,
      );
      expect(denied.status).toBe(401);
    } finally {
      await adapter.close();
    }
  });
});
