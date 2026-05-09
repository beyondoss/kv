import { type ChildProcess, spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

let serverProcess: ChildProcess | undefined;
let tempDataDir: string | undefined;

function findFreePort(): Promise<number> {
  return new Promise((res, rej) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address() as { port: number };
      srv.close((err) => (err ? rej(err) : res(port)));
    });
    srv.on("error", rej);
  });
}

async function waitForHealthy(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${url}/livez`);
      if (res.ok) return;
    } catch {
      // server not up yet
    }
    await new Promise<void>((r) => setTimeout(r, 150));
  }
  throw new Error(
    `beyond-kv did not become healthy at ${url} within ${timeoutMs}ms`,
  );
}

export async function setup(): Promise<void> {
  const [httpPort, respPort] = await Promise.all([
    findFreePort(),
    findFreePort(),
  ]);

  tempDataDir = mkdtempSync(join(tmpdir(), "beyond-kv-rl-test-"));

  const binaryPath = process.env["BEYOND_KV_BINARY"]
    ?? resolve(__dirname, "../../../../target/debug/beyond-kv");

  serverProcess = spawn(binaryPath, ["serve"], {
    env: {
      ...process.env,
      KV_DATA_DIR: tempDataDir,
      KV_ADDRESS: `127.0.0.1:${httpPort}`,
      KV_RESP_PORT: String(respPort),
      KV_MEMORY_BYTES: String(32 * 1024 * 1024),
      KV_THREADS: "1",
      RUST_LOG: "error",
    },
    stdio: ["pipe", "pipe", "inherit"],
  });

  serverProcess.on("error", (err) => {
    throw new Error(`Failed to start beyond-kv: ${err.message}`);
  });

  const httpBaseUrl = `http://127.0.0.1:${httpPort}`;
  await waitForHealthy(httpBaseUrl);

  process.env["RL_TEST_HTTP_URL"] = httpBaseUrl;
  process.env["RL_TEST_RESP_URL"] = `redis://127.0.0.1:${respPort}`;
}

export async function teardown(): Promise<void> {
  serverProcess?.kill("SIGTERM");
  serverProcess = undefined;
  if (tempDataDir) {
    rmSync(tempDataDir, { recursive: true, force: true });
    tempDataDir = undefined;
  }
}
