import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createProcessor } from "../src/index.js";
import { RedisStore, type RedisClient } from "../src/stores/redis.js";

// Skipped unless REDIS_URL is set (CI provides one; see .github/workflows).
//   REDIS_URL=redis://localhost:6379 npm test
const url = process.env.REDIS_URL;

describe.skipIf(!url)("RedisStore (integration)", () => {
  const prefix = `oncekit_test:${Date.now()}:`;
  let redis: { eval: RedisClient["eval"]; keys(p: string): Promise<string[]>; del(...k: string[]): Promise<number>; quit(): Promise<unknown> };
  let store: RedisStore;

  beforeAll(async () => {
    const IORedis = (await import("ioredis")).default;
    redis = new IORedis(url as string) as unknown as typeof redis;
    store = new RedisStore(redis as RedisClient, { prefix });
  });

  afterAll(async () => {
    const keys = await redis.keys(`${prefix}*`);
    if (keys.length) await redis.del(...keys);
    await redis.quit();
  });

  it("dedupes across separate processor instances (durable)", async () => {
    const a = createProcessor({ store });
    const b = createProcessor({ store });
    let calls = 0;

    const first = await a.run("k1", async () => {
      calls++;
      return { ok: true };
    });
    const second = await b.run("k1", async () => {
      calls++;
      return { ok: false };
    });

    expect(first.status).toBe("executed");
    expect(second.status).toBe("duplicate");
    expect(second.result).toEqual({ ok: true });
    expect(calls).toBe(1);
  });

  it("runs the effect once under a concurrent burst (atomic Lua claim)", async () => {
    const once = createProcessor({ store });
    let calls = 0;

    const results = await Promise.all(
      Array.from({ length: 30 }, () =>
        once.run("hot", async () => {
          calls++;
          await new Promise((r) => setTimeout(r, 15));
          return "v";
        }),
      ),
    );

    expect(calls).toBe(1);
    expect(results.filter((r) => r.status === "executed")).toHaveLength(1);
  });

  it("reclaims and completes a crashed (expired-lease) key", async () => {
    const leaseMs = 100;
    const claim = await store.claim("stuck", leaseMs, Date.now());
    expect(claim.outcome).toBe("claimed");

    const worker = createProcessor({ store, leaseMs });
    expect((await worker.run("stuck", async () => "late")).status).toBe("in_flight");

    await new Promise((r) => setTimeout(r, leaseMs + 50));

    const recovered = await worker.run("stuck", async () => "late");
    expect(recovered.status).toBe("recovered");
    expect(recovered.result).toBe("late");
  });

  it("lists dead letters and retries them", async () => {
    const once = createProcessor({
      store,
      retry: { maxAttempts: 2, baseMs: 1, maxMs: 2, factor: 2, jitter: false },
    });

    await once.run("dl", async () => {
      throw new Error("boom");
    });

    const dead = await once.deadLetters();
    expect(dead.some((d) => d.key === "dl")).toBe(true);

    const retried = await once.retryKey("dl", async () => "fixed");
    expect(retried.status).toBe("executed");
    expect((await once.deadLetters()).some((d) => d.key === "dl")).toBe(false);
  });
});
