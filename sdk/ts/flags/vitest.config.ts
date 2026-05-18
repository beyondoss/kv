import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globalSetup: ["./src/__tests__/global-setup.ts"],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    include: ["src/__tests__/**/*.test.ts"],
    forceExit: true,
    // All test files share one beyond-kv via globalSetup. Parallel forks
    // would race on shared keys and inflate CAS contention to flake levels.
    fileParallelism: false,
  },
});
