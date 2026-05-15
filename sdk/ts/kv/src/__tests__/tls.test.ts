/**
 * E2E mTLS test for both the HTTP and RESP backends.
 *
 * Spawns a `beyond-kv` process with TLS enabled, generates certs in-process
 * using @peculiar/x509, then exercises set/get over both transports.
 */
import "reflect-metadata";
import * as x509 from "@peculiar/x509";
import { type ChildProcess, spawn } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createHttpKvClient } from "../http.js";
import { createRespKvClient } from "../resp.js";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

// ── cert helpers ──────────────────────────────────────────────────────────────

interface TestCerts {
  caPem: string;
  serverCertPem: string;
  serverKeyPem: string;
  clientCertPem: string;
  clientKeyPem: string;
}

function toPem(label: string, der: ArrayBuffer): string {
  const b64 = Buffer.from(der).toString("base64");
  return `-----BEGIN ${label}-----\n${
    b64.match(/.{1,64}/g)!.join("\n")
  }\n-----END ${label}-----\n`;
}

async function generateTestCerts(): Promise<TestCerts> {
  const alg = { name: "ECDSA", namedCurve: "P-256" };
  const sigAlg: EcdsaParams = { name: "ECDSA", hash: "SHA-256" };

  const caKeys = await crypto.subtle.generateKey(alg, true, ["sign", "verify"]);
  const caCert = await x509.X509CertificateGenerator.createSelfSigned({
    keys: caKeys,
    name: "CN=Test CA",
    notBefore: new Date("2020-01-01"),
    notAfter: new Date("2099-12-31"),
    signingAlgorithm: sigAlg,
    extensions: [new x509.BasicConstraintsExtension(true, undefined, true)],
  });

  const serverKeys = await crypto.subtle.generateKey(alg, true, [
    "sign",
    "verify",
  ]);
  const serverCert = await x509.X509CertificateGenerator.create({
    subject: "CN=localhost",
    issuer: "CN=Test CA",
    publicKey: serverKeys.publicKey,
    signingKey: caKeys.privateKey,
    notBefore: new Date("2020-01-01"),
    notAfter: new Date("2099-12-31"),
    signingAlgorithm: sigAlg,
    extensions: [
      new x509.SubjectAlternativeNameExtension([
        { type: "dns", value: "localhost" },
        { type: "ip", value: "127.0.0.1" },
      ]),
      new x509.ExtendedKeyUsageExtension([
        "1.3.6.1.5.5.7.3.1",
        "1.3.6.1.5.5.7.3.2",
      ]),
    ],
  });

  const clientKeys = await crypto.subtle.generateKey(alg, true, [
    "sign",
    "verify",
  ]);
  const clientCert = await x509.X509CertificateGenerator.create({
    subject: "CN=client",
    issuer: "CN=Test CA",
    publicKey: clientKeys.publicKey,
    signingKey: caKeys.privateKey,
    notBefore: new Date("2020-01-01"),
    notAfter: new Date("2099-12-31"),
    signingAlgorithm: sigAlg,
    extensions: [
      new x509.ExtendedKeyUsageExtension(["1.3.6.1.5.5.7.3.2"]),
    ],
  });

  return {
    caPem: caCert.toString("pem"),
    serverCertPem: serverCert.toString("pem"),
    serverKeyPem: toPem(
      "PRIVATE KEY",
      await crypto.subtle.exportKey("pkcs8", serverKeys.privateKey),
    ),
    clientCertPem: clientCert.toString("pem"),
    clientKeyPem: toPem(
      "PRIVATE KEY",
      await crypto.subtle.exportKey("pkcs8", clientKeys.privateKey),
    ),
  };
}

// ── port helper ───────────────────────────────────────────────────────────────

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

async function waitForHealthy(
  url: string,
  certs: TestCerts,
  timeoutMs = 30_000,
): Promise<void> {
  const { default: https } = await import("node:https");
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    await new Promise<void>((r) => setTimeout(r, 150));
    try {
      await new Promise<void>((resolve, reject) => {
        const req = https.get(
          `${url}/livez`,
          {
            ca: certs.caPem,
            cert: certs.clientCertPem,
            key: certs.clientKeyPem,
            rejectUnauthorized: true,
          },
          (res) => {
            res.resume();
            if (res.statusCode === 200) resolve();
            else reject(new Error(`status ${res.statusCode}`));
          },
        );
        req.on("error", reject);
        req.end();
      });
      return;
    } catch {
      // server not up yet
    }
  }
  throw new Error(
    `beyond-kv TLS server did not become healthy at ${url} within ${timeoutMs}ms`,
  );
}

// ── test state ────────────────────────────────────────────────────────────────

let serverProcess: ChildProcess | undefined;
let tempDataDir: string | undefined;
let certDir: string | undefined;
let certs: TestCerts;
let tlsHttpUrl: string;
let tlsRespUrl: string;

beforeAll(async () => {
  certs = await generateTestCerts();

  // Write cert files to a temp dir for the server process.
  certDir = mkdtempSync(join(tmpdir(), "beyond-kv-tls-certs-"));
  const certFile = join(certDir, "server.crt");
  const keyFile = join(certDir, "server.key");
  const caFile = join(certDir, "ca.crt");
  writeFileSync(certFile, certs.serverCertPem);
  writeFileSync(keyFile, certs.serverKeyPem);
  writeFileSync(caFile, certs.caPem);

  const [httpPort, respPort] = await Promise.all([
    findFreePort(),
    findFreePort(),
  ]);

  tempDataDir = mkdtempSync(join(tmpdir(), "beyond-kv-tls-data-"));

  const binaryPath = process.env["BEYOND_KV_BINARY"]
    ?? resolve(__dirname, "../../../../../target/debug/beyond-kv");

  serverProcess = spawn(binaryPath, ["serve"], {
    env: {
      ...process.env,
      KV_DATA_DIR: tempDataDir,
      KV_ADDRESS: `127.0.0.1:${httpPort}`,
      KV_RESP_PORT: String(respPort),
      KV_MEMORY_BYTES: String(32 * 1024 * 1024),
      KV_THREADS: "1",
      RUST_LOG: "error",
      BEYOND_TLS_CERT: certFile,
      BEYOND_TLS_KEY: keyFile,
      BEYOND_TLS_CA: caFile,
    },
    stdio: ["pipe", "pipe", "inherit"],
  });

  serverProcess.on("error", (err) => {
    throw new Error(`Failed to start beyond-kv TLS server: ${err.message}`);
  });

  tlsHttpUrl = `https://127.0.0.1:${httpPort}`;
  tlsRespUrl = `rediss://127.0.0.1:${respPort}`;

  await waitForHealthy(tlsHttpUrl, certs);
}, 60_000);

afterAll(async () => {
  serverProcess?.kill("SIGTERM");
  serverProcess = undefined;
  if (tempDataDir) {
    rmSync(tempDataDir, { recursive: true, force: true });
    tempDataDir = undefined;
  }
  if (certDir) {
    rmSync(certDir, { recursive: true, force: true });
    certDir = undefined;
  }
});

// ── tests ─────────────────────────────────────────────────────────────────────

describe("HTTP backend with mTLS", () => {
  it("set and get roundtrip over mTLS", async () => {
    const kv = createHttpKvClient({
      url: tlsHttpUrl,
      tls: {
        ca: certs.caPem,
        cert: certs.clientCertPem,
        key: certs.clientKeyPem,
      },
    });

    try {
      const key = `tls-http:${crypto.randomUUID()}`;
      const { error: setErr } = await kv.set(key, "hello-tls");
      expect(setErr).toBeUndefined();

      const { data: entry, error: getErr } = await kv.get(key);
      expect(getErr).toBeUndefined();
      expect(entry?.text()).toBe("hello-tls");
    } finally {
      await kv.close();
    }
  });

  it("rejects a client with no TLS config", async () => {
    // Without TLS cert material the server requires mTLS — Node's built-in
    // fetch will fail the handshake (self-signed / unknown CA).
    const kv = createHttpKvClient({ url: tlsHttpUrl });
    try {
      const { error } = await kv.get("any-key");
      expect(error).toBeDefined();
    } finally {
      await kv.close();
    }
  });
});

describe("RESP backend with mTLS (rediss://)", () => {
  it("set and get roundtrip over mTLS", async () => {
    const kv = createRespKvClient({
      url: tlsRespUrl,
      tls: {
        ca: certs.caPem,
        cert: certs.clientCertPem,
        key: certs.clientKeyPem,
      },
    });

    try {
      const key = `tls-resp:${crypto.randomUUID()}`;
      const { error: setErr } = await kv.set(key, "hello-resp-tls");
      expect(setErr).toBeUndefined();

      const { data: entry, error: getErr } = await kv.get(key);
      expect(getErr).toBeUndefined();
      expect(entry?.text()).toBe("hello-resp-tls");
    } finally {
      await kv.close();
    }
  });
});
