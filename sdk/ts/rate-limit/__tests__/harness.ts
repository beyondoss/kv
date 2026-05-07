import {
  type Algorithm,
  createRateLimiter,
  type RateLimiter,
} from "../src/client.js";

export function getHttpUrl(): string {
  const url = process.env["RL_TEST_HTTP_URL"];
  if (!url) {
    throw new Error("RL_TEST_HTTP_URL not set — is globalSetup running?");
  }
  return url;
}

export function getRespUrl(): string {
  const url = process.env["RL_TEST_RESP_URL"];
  if (!url) {
    throw new Error("RL_TEST_RESP_URL not set — is globalSetup running?");
  }
  return url;
}

/** HTTP-backed rate limiter with a unique key prefix to avoid cross-test pollution. */
export function httpRateLimiter(algorithm: Algorithm): RateLimiter {
  return createRateLimiter({
    url: getHttpUrl(),
    algorithm,
    keyPrefix: uniquePrefix(),
  });
}

/** RESP-backed rate limiter with a unique key prefix to avoid cross-test pollution. */
export function respRateLimiter(algorithm: Algorithm): RateLimiter {
  return createRateLimiter({
    url: getRespUrl(),
    algorithm,
    keyPrefix: uniquePrefix(),
  });
}

/** Unique key to avoid collisions across tests. */
export function uniqueKey(prefix = "user"): string {
  return `${prefix}:${crypto.randomUUID()}`;
}

function uniquePrefix(): string {
  return `rl-test-${crypto.randomUUID()}`;
}

export function sleep(ms: number): Promise<void> {
  return new Promise<void>((r) => setTimeout(r, ms));
}
