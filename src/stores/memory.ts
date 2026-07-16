import type { ClaimResult, Store, StoredRecord } from "../store.js";

// In-memory store. State is lost on restart and not shared across processes —
// use it for tests, single-process apps, and demos. `claim` is atomic here for
// free: it does no `await` before it mutates the map, and Node runs one turn of
// the event loop at a time.
export class MemoryStore implements Store {
  private readonly records = new Map<string, StoredRecord>();

  async claim(key: string, leaseMs: number, now: number): Promise<ClaimResult> {
    const existing = this.records.get(key);

    if (!existing) {
      const record: StoredRecord = {
        key,
        status: "pending",
        attempts: 1,
        leaseExpiresAt: now + leaseMs,
        createdAt: now,
        updatedAt: now,
      };
      this.records.set(key, record);
      return { outcome: "claimed", record: { ...record } };
    }

    if (existing.status === "done") return { outcome: "done", record: { ...existing } };
    if (existing.status === "failed") return { outcome: "failed", record: { ...existing } };

    const leaseLive = existing.leaseExpiresAt !== null && existing.leaseExpiresAt > now;
    if (leaseLive) return { outcome: "in_flight", record: { ...existing } };

    // Lease expired: the previous owner is presumed dead. Take it over.
    existing.attempts += 1;
    existing.leaseExpiresAt = now + leaseMs;
    existing.updatedAt = now;
    return { outcome: "reclaimed", record: { ...existing } };
  }

  async finalize(key: string, result: unknown, now: number): Promise<void> {
    const record = this.records.get(key);
    if (!record) return;
    record.status = "done";
    record.result = result;
    record.leaseExpiresAt = null;
    record.updatedAt = now;
  }

  async markFailed(key: string, error: string, attempts: number, now: number): Promise<void> {
    const record = this.records.get(key);
    if (!record) return;
    record.status = "failed";
    record.error = error;
    record.attempts = attempts;
    record.leaseExpiresAt = null;
    record.updatedAt = now;
  }

  async extendLease(key: string, leaseMs: number, now: number): Promise<void> {
    const record = this.records.get(key);
    if (!record || record.status !== "pending") return;
    record.leaseExpiresAt = now + leaseMs;
    record.updatedAt = now;
  }

  async get(key: string): Promise<StoredRecord | undefined> {
    const record = this.records.get(key);
    return record ? { ...record } : undefined;
  }

  async listDeadLetters(limit: number): Promise<StoredRecord[]> {
    return [...this.records.values()]
      .filter((r) => r.status === "failed")
      .sort((a, b) => a.updatedAt - b.updatedAt)
      .slice(0, limit)
      .map((r) => ({ ...r }));
  }

  async remove(key: string): Promise<void> {
    this.records.delete(key);
  }

  size(): number {
    return this.records.size;
  }
}
