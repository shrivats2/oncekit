import { describe, expect, it, vi } from "vitest";
import { createProcessor, MemoryStore } from "../src/index.js";

const fastRetry = { maxAttempts: 3, baseMs: 1, maxMs: 2, factor: 2, jitter: false };

describe("dead-lettering", () => {
  it("retries a failing effect then dead-letters it", async () => {
    const onDeadLetter = vi.fn();
    const p = createProcessor({
      store: new MemoryStore(),
      retry: fastRetry,
      onDeadLetter,
    });

    const effect = vi.fn(async () => {
      throw new Error("provider down");
    });

    const result = await p.run("evt", effect);

    expect(result.status).toBe("dead_letter");
    expect(result.error).toBe("provider down");
    expect(result.attempts).toBe(3);
    expect(effect).toHaveBeenCalledTimes(3);
    expect(onDeadLetter).toHaveBeenCalledTimes(1);
    expect(onDeadLetter).toHaveBeenCalledWith(
      { key: "evt", error: "provider down", attempts: 3 },
      expect.any(Error),
    );
  });

  it("succeeds without retrying when the effect passes first time", async () => {
    const p = createProcessor({ store: new MemoryStore(), retry: fastRetry });
    const effect = vi.fn(async () => "ok");
    const r = await p.run("evt", effect);
    expect(r.status).toBe("executed");
    expect(r.attempts).toBe(1);
    expect(effect).toHaveBeenCalledTimes(1);
  });

  it("recovers on a later attempt if the effect starts succeeding", async () => {
    const p = createProcessor({ store: new MemoryStore(), retry: fastRetry });
    let calls = 0;
    const effect = vi.fn(async () => {
      calls++;
      if (calls < 2) throw new Error("transient");
      return "recovered-value";
    });

    const r = await p.run("evt", effect);
    expect(r.status).toBe("executed");
    expect(r.result).toBe("recovered-value");
    expect(r.attempts).toBe(2);
  });

  it("does not re-run a dead-lettered key on subsequent calls", async () => {
    const p = createProcessor({ store: new MemoryStore(), retry: fastRetry });
    const effect = vi.fn(async () => {
      throw new Error("boom");
    });

    await p.run("evt", effect);
    effect.mockClear();

    const again = await p.run("evt", () => "should-not-run");
    expect(again.status).toBe("dead_letter");
    expect(effect).not.toHaveBeenCalled();
  });

  it("lists dead letters and can retry them successfully", async () => {
    const p = createProcessor({ store: new MemoryStore(), retry: fastRetry });

    await p.run("evt", async () => {
      throw new Error("boom");
    });

    const dead = await p.deadLetters();
    expect(dead).toHaveLength(1);
    expect(dead[0]?.key).toBe("evt");
    expect(dead[0]?.status).toBe("failed");

    const retried = await p.retryKey("evt", async () => "fixed");
    expect(retried.status).toBe("executed");
    expect(retried.result).toBe("fixed");

    expect(await p.deadLetters()).toHaveLength(0);
    const rec = await p.inspect("evt");
    expect(rec?.status).toBe("done");
  });
});
