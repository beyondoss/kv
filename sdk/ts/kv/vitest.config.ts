import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globalSetup: ["./src/__tests__/global-setup.ts"],
    testTimeout: 30_000,
    hookTimeout: 30_000,
    include: ["src/__tests__/**/*.test.ts"],
    // ioredis keeps the event loop alive after tests finish
    forceExit: true,
    // All test files share one beyond-kv instance via globalSetup. Running
    // files in parallel forks lets requests from different suites race on
    // RESP db 0 (CAS, TTL, INCR, LOCK suites all mutate shared state), so
    // pin everything to a single fork. Tests within a file still run in
    // declaration order.
    fileParallelism: false,
  },
});
