import { describe, expect, it, vi } from "vitest";
import { createProcessor, MemoryStore } from "../src/index.js";

describe("concurrency", () => {
  it("runs the effect once under a burst of concurrent calls for one key", async () => {
    const p = createProcessor({ store: new MemoryStore() });
    let running = 0;
    let maxConcurrent = 0;
    const effect = vi.fn(async () => {
      running++;
      maxConcurrent = Math.max(maxConcurrent, running);
      await new Promise((r) => setTimeout(r, 20));
      running--;
      return "ok";
    });

    const results = await Promise.all(
      Array.from({ length: 25 }, () => p.run("hot_key", effect)),
    );

    // The effect never ran twice at the same time, and ran exactly once total.
    expect(maxConcurrent).toBe(1);
    expect(effect).toHaveBeenCalledTimes(1);

    const executed = results.filter((r) => r.status === "executed");
    const inFlight = results.filter((r) => r.status === "in_flight");
    expect(executed).toHaveLength(1);
    expect(inFlight).toHaveLength(24);
  });

  it("with waitForInFlight, losers wait and receive the cached result", async () => {
    const p = createProcessor({
      store: new MemoryStore(),
      waitForInFlight: true,
      pollMs: 5,
    });
    const effect = vi.fn(async () => {
      await new Promise((r) => setTimeout(r, 30));
      return "shared-result";
    });

    const results = await Promise.all(
      Array.from({ length: 10 }, () => p.run("k", effect)),
    );

    expect(effect).toHaveBeenCalledTimes(1);
    for (const r of results) {
      expect(r.result).toBe("shared-result");
      expect(["executed", "duplicate"]).toContain(r.status);
    }
    // Exactly one actually executed; the rest waited and read the cache.
    expect(results.filter((r) => r.status === "executed")).toHaveLength(1);
    expect(results.filter((r) => r.status === "duplicate")).toHaveLength(9);
  });
});
