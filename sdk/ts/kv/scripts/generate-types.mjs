import { execSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const dir = dirname(fileURLToPath(import.meta.url));
const spec = resolve(dir, "../../../../openapi/v1.json");
const out = resolve(dir, "../src/types.ts");

execSync(`npx openapi-typescript ${spec} -o ${out}`, { stdio: "inherit" });
