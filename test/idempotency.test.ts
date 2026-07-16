import { describe, expect, it, vi } from "vitest";
import { createProcessor, MemoryStore } from "../src/index.js";

describe("idempotency", () => {
  it("runs the effect once and caches the result for duplicates", async () => {
    const p = createProcessor({ store: new MemoryStore() });
    const effect = vi.fn(async () => ({ charged: true, id: "ch_1" }));

    const first = await p.run("evt_1", effect);
    const second = await p.run("evt_1", effect);
    const third = await p.run("evt_1", effect);

    expect(first.status).toBe("executed");
    expect(first.result).toEqual({ charged: true, id: "ch_1" });

    expect(second.status).toBe("duplicate");
    expect(second.result).toEqual({ charged: true, id: "ch_1" });
    expect(third.status).toBe("duplicate");

    // The side effect ran exactly once, no matter how many times we asked.
    expect(effect).toHaveBeenCalledTimes(1);
  });

  it("treats distinct keys independently", async () => {
    const p = createProcessor({ store: new MemoryStore() });
    const effect = vi.fn(async (n: number) => n * 2);

    const a = await p.run("a", () => effect(1));
    const b = await p.run("b", () => effect(2));

    expect(a).toMatchObject({ status: "executed", result: 2 });
    expect(b).toMatchObject({ status: "executed", result: 4 });
    expect(effect).toHaveBeenCalledTimes(2);
  });

  it("caches falsy and undefined results too", async () => {
    const p = createProcessor({ store: new MemoryStore() });
    const effect = vi.fn(async () => 0);

    await p.run("zero", effect);
    const dup = await p.run("zero", effect);

    expect(dup.status).toBe("duplicate");
    expect(dup.result).toBe(0);
    expect(effect).toHaveBeenCalledTimes(1);
  });
});
