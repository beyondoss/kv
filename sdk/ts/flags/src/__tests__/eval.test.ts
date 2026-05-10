import { describe, expect, it } from "vitest";
import { evaluate } from "../eval.js";
import { bucket, fnv1a32 } from "../hash.js";
import type { FlagContext, FlagDef } from "../types.js";
import "./test-context.js";

const ctx = (
  overrides: Partial<FlagContext> & { id: string },
): FlagContext => ({ ...overrides, id: overrides.id });

describe("evaluate — eval order", () => {
  it("returns default when no state in snapshot", () => {
    const r = evaluate("x", false, ctx({ id: "u_1" }), undefined, null);
    expect(r).toEqual({ value: false, reason: "no-snapshot" });
  });

  it("returns default when on === false (kill switch beats everything)", () => {
    const state: FlagDef<boolean> = {
      on: false,
      rules: [{ when: { id: "u_1" }, value: true }],
      rollout: { percent: 100 },
    };
    const r = evaluate("x", false, ctx({ id: "u_1" }), state, { x: true });
    expect(r).toEqual({ value: false, reason: "off" });
  });

  it("returns user pref over rules / rollout / override", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: {}, value: false }],
      rollout: { percent: 0 },
    };
    const r = evaluate(
      "x",
      false,
      ctx({ id: "u_1" }),
      state,
      { x: true },
      () => false,
    );
    expect(r).toEqual({ value: true, reason: "user-pref" });
  });

  it("user-pref check is sparse — absent field falls through", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: { id: "u_1" }, value: true }],
    };
    const r = evaluate("x", false, ctx({ id: "u_1" }), state, { other: false });
    expect(r.reason).toBe("rule");
    expect(r.value).toBe(true);
  });

  it("override beats rules and rollout when defined and returns a value", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: {}, value: false }],
      rollout: { percent: 0 },
    };
    const r = evaluate("x", false, ctx({ id: "u_1" }), state, null, () => true);
    expect(r).toEqual({ value: true, reason: "override" });
  });

  it("override returning undefined falls through to rules", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: { id: "u_1" }, value: true }],
    };
    const r = evaluate(
      "x",
      false,
      ctx({ id: "u_1" }),
      state,
      null,
      () => undefined,
    );
    expect(r.reason).toBe("rule");
    expect(r.value).toBe(true);
  });

  it("walks rules in order, first match wins", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [
        { when: { plan: "pro" }, value: true },
        { when: { plan: "free" }, value: false },
      ],
    };
    const r = evaluate("x", false, ctx({ id: "u", plan: "pro" }), state, null);
    expect(r).toEqual({ value: true, reason: "rule", ruleIndex: 0 });
  });

  it("any-of array constraint matches if context value is any of them", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: { country: ["US", "CA"] }, value: true }],
    };
    const r = evaluate(
      "x",
      false,
      ctx({ id: "u", country: "CA" }),
      state,
      null,
    );
    expect(r.reason).toBe("rule");
    expect(r.value).toBe(true);
  });

  it("rule with multiple `when` keys requires all to match", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: { plan: "pro", country: "US" }, value: true }],
    };
    const r1 = evaluate(
      "x",
      false,
      ctx({ id: "u", plan: "pro", country: "US" }),
      state,
      null,
    );
    const r2 = evaluate(
      "x",
      false,
      ctx({ id: "u", plan: "pro", country: "CA" }),
      state,
      null,
    );
    expect(r1.reason).toBe("rule");
    expect(r2.reason).toBe("default");
  });

  it("rollout falls through when rule matches first", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: { id: "u_match" }, value: false }],
      rollout: { percent: 100 },
    };
    const r = evaluate("x", true, ctx({ id: "u_match" }), state, null);
    expect(r.value).toBe(false);
    expect(r.reason).toBe("rule");
  });

  it("rollout returns rollout.value when set, else `true` for booleans", () => {
    const stateBool: FlagDef<boolean> = { on: true, rollout: { percent: 100 } };
    const stateVar: FlagDef<string> = {
      on: true,
      rollout: { percent: 100, value: "v2" },
    };
    expect(evaluate("a", false, ctx({ id: "u" }), stateBool, null).value).toBe(
      true,
    );
    expect(evaluate("a", "off", ctx({ id: "u" }), stateVar, null).value).toBe(
      "v2",
    );
  });

  it("empty when: {} matches any context (no constraints required)", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: {}, value: true }],
    };
    const r = evaluate(
      "x",
      false,
      ctx({ id: "u_any", plan: "free" }),
      state,
      null,
    );
    expect(r.reason).toBe("rule");
    expect(r.value).toBe(true);
  });

  it("falls through to default when nothing matches", () => {
    const state: FlagDef<boolean> = {
      on: true,
      rules: [{ when: { id: "other" }, value: true }],
      rollout: { percent: 0 },
    };
    const r = evaluate("x", false, ctx({ id: "u" }), state, null);
    expect(r).toEqual({ value: false, reason: "default" });
  });
});

describe("rollout — determinism", () => {
  it("returns the same answer for same id + same flag", () => {
    const state: FlagDef<boolean> = { on: true, rollout: { percent: 50 } };
    const a = evaluate("flag-a", false, ctx({ id: "u_1" }), state, null);
    const b = evaluate("flag-a", false, ctx({ id: "u_1" }), state, null);
    expect(a.value).toBe(b.value);
  });

  it("two flags with same percent give uncorrelated cohorts", () => {
    const state: FlagDef<boolean> = { on: true, rollout: { percent: 50 } };
    const ids = Array.from({ length: 5_000 }, (_, i) => `u_${i}`);
    let differs = 0;
    for (const id of ids) {
      const a = evaluate("flag-a", false, ctx({ id }), state, null).value;
      const b = evaluate("flag-b", false, ctx({ id }), state, null).value;
      if (a !== b) differs++;
    }
    // Two independent 50% rollouts → expected ~50% disagreement (2500/5000).
    // Wide tolerance because FNV-1a is decent but not perfect for short inputs.
    expect(differs).toBeGreaterThan(1_500);
    expect(differs).toBeLessThan(3_500);
  });

  it("0% never returns true; 100% always returns true", () => {
    const off: FlagDef<boolean> = { on: true, rollout: { percent: 0 } };
    const on: FlagDef<boolean> = { on: true, rollout: { percent: 100 } };
    for (let i = 0; i < 200; i++) {
      expect(
        evaluate("x", false, ctx({ id: `u_${i}` }), off, null).reason,
      ).toBe("default");
      expect(
        evaluate("x", false, ctx({ id: `u_${i}` }), on, null).reason,
      ).toBe("rollout");
    }
  });

  it("percent ~50 distributes within 5% over 10k ids", () => {
    const state: FlagDef<boolean> = { on: true, rollout: { percent: 50 } };
    let on = 0;
    const N = 10_000;
    for (let i = 0; i < N; i++) {
      if (evaluate("x", false, ctx({ id: `u_${i}` }), state, null).value) on++;
    }
    expect(on).toBeGreaterThan(N * 0.45);
    expect(on).toBeLessThan(N * 0.55);
  });

  it("clamps out-of-range percent", () => {
    const negative: FlagDef<boolean> = { on: true, rollout: { percent: -10 } };
    const high: FlagDef<boolean> = { on: true, rollout: { percent: 200 } };
    expect(
      evaluate("x", false, ctx({ id: "u" }), negative, null).reason,
    ).toBe("default");
    expect(
      evaluate("x", false, ctx({ id: "u" }), high, null).reason,
    ).toBe("rollout");
  });
});

describe("hash", () => {
  it("fnv1a32 is deterministic", () => {
    expect(fnv1a32("hello")).toBe(fnv1a32("hello"));
  });

  it("fnv1a32 differs for different inputs", () => {
    expect(fnv1a32("a")).not.toBe(fnv1a32("b"));
  });

  it("bucket combines id and flag name", () => {
    expect(bucket("u", "a")).not.toBe(bucket("u", "b"));
    expect(bucket("u", "a")).toBe(bucket("u", "a"));
  });

  it("bucket is in [0, 100)", () => {
    for (let i = 0; i < 500; i++) {
      const b = bucket(`u_${i}`, "x");
      expect(b).toBeGreaterThanOrEqual(0);
      expect(b).toBeLessThan(100);
    }
  });
});
