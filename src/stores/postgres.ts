import type {
  ClaimOutcome,
  ClaimResult,
  RecordStatus,
  Store,
  StoredRecord,
} from "../store.js";

// A `pg.Pool` or `pg.Client` satisfies this. Kept minimal so oncekit doesn't
// take a hard dependency on `pg`.
export interface SqlClient {
  query(text: string, params?: unknown[]): Promise<{ rows: Array<Record<string, unknown>> }>;
}

export interface PostgresStoreOptions {
  table?: string;
}

interface DbRow {
  key: string;
  status: RecordStatus;
  result: unknown;
  error: string | null;
  attempts: string | number;
  lease_expires_at: string | number | null;
  created_at: string | number;
  updated_at: string | number;
}

// Durable, cross-process store. Call `migrate()` once before use. Atomicity of
// `claim` comes from Postgres, not from application-level locking.
export class PostgresStore implements Store {
  private readonly table: string;

  constructor(
    private readonly client: SqlClient,
    options: PostgresStoreOptions = {},
  ) {
    const table = options.table ?? "oncekit_records";
    // Table names can't be parameterized, so validate before interpolating.
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)) {
      throw new Error(`oncekit: invalid table name ${JSON.stringify(table)}`);
    }
    this.table = table;
  }

  async migrate(): Promise<void> {
    await this.client.query(`
      CREATE TABLE IF NOT EXISTS ${this.table} (
        key              text PRIMARY KEY,
        status           text NOT NULL,
        result           jsonb,
        error            text,
        attempts         integer NOT NULL DEFAULT 0,
        lease_expires_at bigint,
        created_at       bigint NOT NULL,
        updated_at       bigint NOT NULL
      );
    `);
    await this.client.query(
      `CREATE INDEX IF NOT EXISTS ${this.table}_status_idx ON ${this.table} (status, updated_at);`,
    );
  }

  async claim(key: string, leaseMs: number, now: number): Promise<ClaimResult> {
    const leaseUntil = now + leaseMs;

    // First writer wins the insert.
    const inserted = await this.client.query(
      `INSERT INTO ${this.table} (key, status, attempts, lease_expires_at, created_at, updated_at)
       VALUES ($1, 'pending', 1, $2, $3, $3)
       ON CONFLICT (key) DO NOTHING
       RETURNING *;`,
      [key, leaseUntil, now],
    );
    if (inserted.rows.length > 0) {
      return { outcome: "claimed", record: toRecord(inserted.rows[0] as unknown as DbRow) };
    }

    // Otherwise take over an abandoned (expired-lease) pending row.
    const reclaimed = await this.client.query(
      `UPDATE ${this.table}
         SET attempts = attempts + 1, lease_expires_at = $2, updated_at = $3
       WHERE key = $1 AND status = 'pending'
         AND (lease_expires_at IS NULL OR lease_expires_at <= $3)
       RETURNING *;`,
      [key, leaseUntil, now],
    );
    if (reclaimed.rows.length > 0) {
      return { outcome: "reclaimed", record: toRecord(reclaimed.rows[0] as unknown as DbRow) };
    }

    const current = await this.client.query(`SELECT * FROM ${this.table} WHERE key = $1;`, [key]);
    const row = current.rows[0] as unknown as DbRow | undefined;
    if (!row) return this.claim(key, leaseMs, now); // row deleted mid-claim; start over
    const outcome: ClaimOutcome =
      row.status === "done" ? "done" : row.status === "failed" ? "failed" : "in_flight";
    return { outcome, record: toRecord(row) };
  }

  async finalize(key: string, result: unknown, now: number): Promise<void> {
    await this.client.query(
      `UPDATE ${this.table} SET status = 'done', result = $2::jsonb, lease_expires_at = NULL, updated_at = $3 WHERE key = $1;`,
      [key, JSON.stringify(result ?? null), now],
    );
  }

  async markFailed(key: string, error: string, attempts: number, now: number): Promise<void> {
    await this.client.query(
      `UPDATE ${this.table} SET status = 'failed', error = $2, attempts = $3, lease_expires_at = NULL, updated_at = $4 WHERE key = $1;`,
      [key, error, attempts, now],
    );
  }

  async extendLease(key: string, leaseMs: number, now: number): Promise<void> {
    await this.client.query(
      `UPDATE ${this.table} SET lease_expires_at = $2, updated_at = $3 WHERE key = $1 AND status = 'pending';`,
      [key, now + leaseMs, now],
    );
  }

  async get(key: string): Promise<StoredRecord | undefined> {
    const res = await this.client.query(`SELECT * FROM ${this.table} WHERE key = $1;`, [key]);
    const row = res.rows[0] as unknown as DbRow | undefined;
    return row ? toRecord(row) : undefined;
  }

  async listDeadLetters(limit: number): Promise<StoredRecord[]> {
    const res = await this.client.query(
      `SELECT * FROM ${this.table} WHERE status = 'failed' ORDER BY updated_at ASC LIMIT $1;`,
      [limit],
    );
    return (res.rows as unknown as DbRow[]).map(toRecord);
  }

  async remove(key: string): Promise<void> {
    await this.client.query(`DELETE FROM ${this.table} WHERE key = $1;`, [key]);
  }
}

// bigint columns come back as strings from node-pg.
function toRecord(row: DbRow): StoredRecord {
  const record: StoredRecord = {
    key: row.key,
    status: row.status,
    attempts: Number(row.attempts),
    leaseExpiresAt: row.lease_expires_at === null ? null : Number(row.lease_expires_at),
    createdAt: Number(row.created_at),
    updatedAt: Number(row.updated_at),
  };
  if (row.result !== null && row.result !== undefined) record.result = row.result;
  if (row.error !== null && row.error !== undefined) record.error = row.error;
  return record;
}
