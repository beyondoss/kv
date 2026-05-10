import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    "next/index": "src/next/index.ts",
    cache: "src/cache.ts",
  },
  format: "esm",
  dts: true,
  clean: true,
  treeshake: true,
  deps: { neverBundle: ["next"] },
});
