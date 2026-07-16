import { describe, expect, it, vi } from "vitest";
import { createProcessor, MemoryStore } from "../src/index.js";

describe("crash recovery", () => {
  it("takes over and re-runs a key whose owner crashed mid-flight", async () => {
    const store = new MemoryStore();

    // A controllable clock so we can fast-forward past the lease.
    let clock = 1_000_000;
    const now = () => clock;

    const leaseMs = 10_000;

    // Worker A claims the key, then "crashes" before finalizing: we simulate
    // this by claiming directly through the store and never completing it.
    const claimA = await store.claim("job_1", leaseMs, now());
    expect(claimA.outcome).toBe("claimed");

    // Worker B tries while A still holds a live lease → must not run.
    const workerB = createProcessor({ store, leaseMs, now });
    const effect = vi.fn(async () => "done-by-B");

    const early = await workerB.run("job_1", effect);
    expect(early.status).toBe("in_flight");
    expect(effect).not.toHaveBeenCalled();

    // Time passes beyond A's lease — A is presumed dead.
    clock += leaseMs + 1;

    const recovered = await workerB.run("job_1", effect);
    expect(recovered.status).toBe("recovered");
    expect(recovered.result).toBe("done-by-B");
    expect(effect).toHaveBeenCalledTimes(1);

    const rec = await workerB.inspect("job_1");
    expect(rec?.status).toBe("done");
    // attempts: A's claim (1) + B's takeover (2).
    expect(rec?.attempts).toBe(2);
  });

  it("does not re-run a key that already completed, even after the lease window", async () => {
    const store = new MemoryStore();
    let clock = 0;
    const p = createProcessor({ store, leaseMs: 1_000, now: () => clock });
    const effect = vi.fn(async () => "v");

    await p.run("k", effect);
    clock += 10_000; // long past any lease
    const again = await p.run("k", effect);

    expect(again.status).toBe("duplicate");
    expect(effect).toHaveBeenCalledTimes(1);
  });
});
