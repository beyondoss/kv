import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "middleware/hono": "src/middleware/hono.ts",
    "middleware/next": "src/middleware/next.ts",
    "middleware/fastify": "src/middleware/fastify.ts",
    "middleware/express": "src/middleware/express.ts",
  },
  format: "esm",
  dts: true,
  clean: true,
  treeshake: true,
  deps: {
    neverBundle: [
      "@beyond.dev/kv",
      "hono",
      "next",
      "fastify",
      "fastify-plugin",
      "express",
    ],
  },
});
