import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    adapter: "src/adapter.ts",
    "middleware/hono": "src/middleware/hono.ts",
    "middleware/express": "src/middleware/express.ts",
    "middleware/fastify": "src/middleware/fastify.ts",
    "middleware/next": "src/middleware/next.ts",
    "middleware/next-middleware": "src/middleware/next-middleware.ts",
  },
  format: "esm",
  dts: true,
  clean: true,
  treeshake: true,
  deps: {
    neverBundle: ["next", "express", "fastify", "fastify-plugin", "hono", "flags"],
  },
});
