/**
 * End-to-end tests for the RESP3 WATCH / PWATCH / UNWATCH wire protocol.
 *
 * These tests open raw TCP connections and speak RESP3 manually because
 * ioredis does not support receiving Push frames from custom server commands.
 * The globalSetup harness already starts a real beyond-kv binary, so we just
 * need the RESP port from KV_TEST_RESP_URL.
 */

import * as net from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createKvClient, type KvClient } from "../client.js";
import { getRespUrl, uniqueKey } from "./harness.js";

// ── minimal RESP3 codec ───────────────────────────────────────────────────────

type RespValue =
  | string
  | number
  | null
  | Uint8Array
  | RespValue[]
  | Map<RespValue, RespValue>
  | { push: RespValue[] };

function encodeResp(...args: string[]): Buffer {
  const parts: string[] = [`*${args.length}\r\n`];
  for (const a of args) parts.push(`$${Buffer.byteLength(a)}\r\n${a}\r\n`);
  return Buffer.from(parts.join(""));
}

class Resp3Reader {
  private buf = Buffer.alloc(0);
  private pos = 0;

  feed(chunk: Buffer): void {
    this.buf = Buffer.concat([this.buf.subarray(this.pos), chunk]);
    this.pos = 0;
  }

  /** Try to parse one complete RESP3 value. Returns undefined if more data needed. */
  tryRead(): RespValue | undefined {
    const saved = this.pos;
    try {
      return this.readValue();
    } catch (e) {
      if (e instanceof NeedMore) {
        this.pos = saved;
        return undefined;
      }
      throw e;
    }
  }

  private readLine(): string {
    const i = this.buf.indexOf("\r\n", this.pos);
    if (i === -1) throw new NeedMore();
    const line = this.buf.toString("utf8", this.pos, i);
    this.pos = i + 2;
    return line;
  }

  private readValue(): RespValue {
    if (this.pos >= this.buf.length) throw new NeedMore();
    const type = String.fromCharCode(this.buf[this.pos]!);
    this.pos++;
    switch (type) {
      case "+":
        return this.readLine();
      case "-":
        throw new Error(this.readLine());
      case ":":
        return parseInt(this.readLine(), 10);
      case "_": {
        this.readLine();
        return null;
      }
      case "$": {
        const len = parseInt(this.readLine(), 10);
        if (len === -1) return null;
        if (this.pos + len + 2 > this.buf.length) throw new NeedMore();
        const bytes = this.buf.subarray(this.pos, this.pos + len);
        this.pos += len + 2;
        return new Uint8Array(bytes);
      }
      case "*": {
        const count = parseInt(this.readLine(), 10);
        if (count === -1) return null;
        const arr: RespValue[] = [];
        for (let i = 0; i < count; i++) arr.push(this.readValue());
        return arr;
      }
      case "%": {
        const count = parseInt(this.readLine(), 10);
        const map = new Map<RespValue, RespValue>();
        for (let i = 0; i < count; i++) {
          const k = this.readValue();
          const v = this.readValue();
          map.set(k, v);
        }
        return map;
      }
      case ">": {
        const count = parseInt(this.readLine(), 10);
        const arr: RespValue[] = [];
        for (let i = 0; i < count; i++) arr.push(this.readValue());
        return { push: arr };
      }
      default:
        throw new Error(`unknown RESP3 type: ${type}`);
    }
  }
}

class NeedMore extends Error {}

function respStr(v: RespValue): string {
  if (v instanceof Uint8Array) return Buffer.from(v).toString("utf8");
  if (typeof v === "string") return v;
  throw new Error(`expected string, got ${typeof v}`);
}

// ── connection helper ─────────────────────────────────────────────────────────

function parseRespAddr(url: string): { host: string; port: number } {
  const u = new URL(url);
  return { host: u.hostname, port: parseInt(u.port, 10) };
}

type Waiter = { resolve: (v: RespValue) => void; reject: (e: unknown) => void };

class Resp3Conn {
  private sock: net.Socket;
  private reader = new Resp3Reader();
  private pending: Waiter[] = [];

  constructor(sock: net.Socket) {
    this.sock = sock;
    sock.on("data", (chunk: Buffer) => {
      this.reader.feed(chunk);
      this.drain();
    });
  }

  private drain(): void {
    while (this.pending.length > 0) {
      let val: RespValue | undefined;
      try {
        val = this.reader.tryRead();
      } catch (e) {
        // Route RESP error responses (-ERR ...) to the waiting recv() promise.
        const { reject } = this.pending.shift()!;
        reject(e);
        continue;
      }
      if (val === undefined) break;
      const { resolve } = this.pending.shift()!;
      resolve(val);
    }
  }

  send(buf: Buffer): void {
    this.sock.write(buf);
  }

  recv(): Promise<RespValue> {
    return new Promise((resolve, reject) => {
      let val: RespValue | undefined;
      try {
        val = this.reader.tryRead();
      } catch (e) {
        reject(e);
        return;
      }
      if (val !== undefined) {
        resolve(val);
      } else {
        this.pending.push({ resolve, reject });
      }
    });
  }

  async hello3(): Promise<void> {
    this.send(encodeResp("HELLO", "3"));
    await this.recv(); // RESP3 map reply
  }

  /** Skip frames until we see a ready push, returning all frames seen. */
  async waitReady(): Promise<{ push: RespValue[] }[]> {
    const seen: { push: RespValue[] }[] = [];
    while (true) {
      const frame = (await this.recv()) as { push: RespValue[] };
      seen.push(frame);
      if (respStr(frame.push[1]!) === "ready") return seen;
    }
  }

  close(): void {
    this.sock.destroy();
  }
}

function openConn(addr: { host: string; port: number }): Promise<Resp3Conn> {
  return new Promise((resolve, reject) => {
    const sock = net.createConnection(addr.port, addr.host, () => {
      resolve(new Resp3Conn(sock));
    });
    sock.on("error", reject);
  });
}

// ── test setup ────────────────────────────────────────────────────────────────

let addr: { host: string; port: number };
let kv: KvClient; // ioredis client for writes in watch tests

beforeAll(() => {
  addr = parseRespAddr(getRespUrl());
  kv = createKvClient({ url: getRespUrl(), db: 0 });
});

afterAll(() => kv.close());

// ── tests ─────────────────────────────────────────────────────────────────────

describe("RESP3 WATCH — exact key", () => {
  it("rejects WATCH on a RESP2 connection", async () => {
    const conn = await openConn(addr);
    // Do NOT send HELLO 3 — connection is RESP2 by default.
    conn.send(encodeResp("WATCH", uniqueKey()));
    // Server returns -WRONGTYPE error; our recv() rejects with that error.
    await expect(conn.recv()).rejects.toThrow(/RESP3/i);
    conn.close();
  });

  it("emits a ready push after HELLO 3 + WATCH", async () => {
    const conn = await openConn(addr);
    await conn.hello3();
    conn.send(encodeResp("WATCH", uniqueKey()));
    const push = (await conn.recv()) as { push: RespValue[] };
    expect(push.push).toBeDefined();
    expect(respStr(push.push[0]!)).toBe("watch");
    expect(respStr(push.push[1]!)).toBe("ready");
    conn.close();
  });

  it("delivers a set push when the key is written after subscribe", async () => {
    const key = uniqueKey("rw");
    const conn = await openConn(addr);
    await conn.hello3();
    conn.send(encodeResp("WATCH", key));
    // Wait for ready.
    const ready = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(ready.push[1]!)).toBe("ready");

    // Write from the ioredis client.
    await kv.set(key, "hello");

    const push = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(push.push[0]!)).toBe("watch");
    expect(respStr(push.push[1]!)).toBe("set");
    expect(respStr(push.push[2]!)).toBe(key);
    expect(respStr(push.push[3]!)).toBe("hello");
    const revision = parseInt(respStr(push.push[4]!), 10);
    expect(revision).toBeGreaterThan(0);

    conn.close();
  });

  it("delivers a del push when the key is deleted", async () => {
    const key = uniqueKey("rw");
    await kv.set(key, "v");

    const conn = await openConn(addr);
    await conn.hello3();
    conn.send(encodeResp("WATCH", key));
    // Key already exists — server sends initial set frame then ready.
    await conn.waitReady();

    await kv.delete(key);

    const push = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(push.push[1]!)).toBe("del");
    expect(respStr(push.push[2]!)).toBe(key);

    conn.close();
  });

  it("initial push contains current value when key already exists (since=0)", async () => {
    const key = uniqueKey("rw");
    await kv.set(key, "existing");

    const conn = await openConn(addr);
    await conn.hello3();
    conn.send(encodeResp("WATCH", key));

    // First frame is the current-value Set push.
    const initial = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(initial.push[1]!)).toBe("set");
    expect(respStr(initial.push[2]!)).toBe(key);
    expect(respStr(initial.push[3]!)).toBe("existing");

    // Second frame is ready.
    const ready = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(ready.push[1]!)).toBe("ready");

    conn.close();
  });

  it("UNWATCH stops the stream", async () => {
    const key = uniqueKey("rw");
    const conn = await openConn(addr);
    await conn.hello3();
    conn.send(encodeResp("WATCH", key));
    const ready = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(ready.push[1]!)).toBe("ready");

    conn.send(encodeResp("UNWATCH"));
    // After UNWATCH the connection closes on the server side — we should not
    // receive any more pushes. Confirm by timing out a short wait with no frame.
    const result = await Promise.race([
      conn.recv().then(() => "got-frame"),
      new Promise<string>((r) => setTimeout(() => r("timeout"), 300)),
    ]);
    expect(result).toBe("timeout");

    conn.close();
  });

  it("WATCH SINCE replays missed mutations on reconnect", async () => {
    const key = uniqueKey("rw");

    // Subscribe before writing so we capture the real revision from the live push.
    const conn1 = await openConn(addr);
    await conn1.hello3();
    conn1.send(encodeResp("WATCH", key));
    await conn1.waitReady(); // key doesn't exist yet — just ready

    await kv.set(key, "v1");
    const pushV1 = (await conn1.recv()) as { push: RespValue[] };
    const revAfterV1 = parseInt(respStr(pushV1.push[4]!), 10);
    expect(revAfterV1).toBeGreaterThan(0);
    conn1.close();

    // Write v2 and delete while "disconnected".
    await kv.set(key, "v2");
    await kv.delete(key);

    // Reconnect with SINCE = revAfterV1 — should replay set(v2) + del.
    const conn2 = await openConn(addr);
    await conn2.hello3();
    conn2.send(encodeResp("WATCH", key, "SINCE", String(revAfterV1)));

    const replay1 = (await conn2.recv()) as { push: RespValue[] };
    expect(respStr(replay1.push[1]!)).toBe("set");
    expect(respStr(replay1.push[3]!)).toBe("v2");

    const replay2 = (await conn2.recv()) as { push: RespValue[] };
    expect(respStr(replay2.push[1]!)).toBe("del");

    const ready = (await conn2.recv()) as { push: RespValue[] };
    expect(respStr(ready.push[1]!)).toBe("ready");

    conn2.close();
  });
});

describe("RESP3 PWATCH — prefix", () => {
  it("emits a ready push after PWATCH", async () => {
    const conn = await openConn(addr);
    await conn.hello3();
    conn.send(encodeResp("PWATCH", `pfx:${crypto.randomUUID()}:`));
    const push = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(push.push[1]!)).toBe("ready");
    conn.close();
  });

  it("delivers set events for keys matching the prefix only", async () => {
    const prefix = `pfx:${crypto.randomUUID()}:`;
    const match = `${prefix}a`;
    const noMatch = uniqueKey("other");

    const conn = await openConn(addr);
    await conn.hello3();
    conn.send(encodeResp("PWATCH", prefix));
    const ready = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(ready.push[1]!)).toBe("ready");

    await kv.set(noMatch, "x"); // should not arrive
    await kv.set(match, "y"); // should arrive

    const push = (await conn.recv()) as { push: RespValue[] };
    expect(respStr(push.push[1]!)).toBe("set");
    expect(respStr(push.push[2]!)).toBe(match);

    conn.close();
  });
});
