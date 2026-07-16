import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createProcessor } from "../src/index.js";
import { PostgresStore, type SqlClient } from "../src/stores/postgres.js";

/**
 * Runs the core guarantees against a real Postgres instance.
 * Skipped unless DATABASE_URL is set (CI provides one; see .github/workflows).
 *
 *   DATABASE_URL=postgres://localhost/oncekit npm test
 */
const url = process.env.DATABASE_URL;

describe.skipIf(!url)("PostgresStore (integration)", () => {
  const TABLE = "oncekit_test";
  let pool: SqlClient & { end(): Promise<void> };
  let store: PostgresStore;

  beforeAll(async () => {
    const pg = await import("pg");
    pool = new pg.Pool({ connectionString: url }) as unknown as SqlClient & {
      end(): Promise<void>;
    };
    store = new PostgresStore(pool, { table: TABLE });
    await pool.query(`DROP TABLE IF EXISTS ${TABLE}`);
    await store.migrate();
  });

  afterAll(async () => {
    await pool?.query(`DROP TABLE IF EXISTS ${TABLE}`);
    await pool?.end();
  });

  it("dedupes across separate processor instances (durable)", async () => {
    const a = createProcessor({ store });
    const b = createProcessor({ store }); // a "different process" sharing the DB
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

  it("runs the effect once under a concurrent burst (atomic claim)", async () => {
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
    // Simulate a crashed owner: claim directly and never finalize.
    const claim = await store.claim("stuck", leaseMs, Date.now());
    expect(claim.outcome).toBe("claimed");

    const worker = createProcessor({ store, leaseMs });
    const early = await worker.run("stuck", async () => "late");
    expect(early.status).toBe("in_flight");

    await new Promise((r) => setTimeout(r, leaseMs + 50));

    const recovered = await worker.run("stuck", async () => "late");
    expect(recovered.status).toBe("recovered");
    expect(recovered.result).toBe("late");
  });
});
