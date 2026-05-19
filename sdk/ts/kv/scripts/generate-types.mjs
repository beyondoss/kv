import { execSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const dir = dirname(fileURLToPath(import.meta.url));
const root = resolve(dir, "../../../..");
const spec = resolve(root, "openapi/v1.json");
const out = resolve(dir, "../src/types.ts");

execSync(`npx openapi-typescript ${spec} -o ${out}`, { stdio: "inherit" });
execSync(`dprint fmt ${out}`, { stdio: "inherit", cwd: root });
