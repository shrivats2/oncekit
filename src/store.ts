export type RecordStatus = "pending" | "done" | "failed";

export interface StoredRecord {
  key: string;
  status: RecordStatus;
  result?: unknown;
  error?: string;
  attempts: number;
  leaseExpiresAt: number | null;
  createdAt: number;
  updatedAt: number;
}

export type ClaimOutcome =
  | "claimed"
  | "reclaimed"
  | "done"
  | "failed"
  | "in_flight";

export interface ClaimResult {
  outcome: ClaimOutcome;
  record: StoredRecord;
}

// The whole library's correctness rests on `claim` being atomic: two callers
// racing on the same key must never both get an executing outcome.
export interface Store {
  // no record            -> pending w/ fresh lease -> "claimed"
  // done / failed        -> that terminal state
  // pending, lease live   -> "in_flight"
  // pending, lease expired -> take it over, bump attempts -> "reclaimed"
  claim(key: string, leaseMs: number, now: number): Promise<ClaimResult>;
  finalize(key: string, result: unknown, now: number): Promise<void>;
  markFailed(key: string, error: string, attempts: number, now: number): Promise<void>;
  extendLease(key: string, leaseMs: number, now: number): Promise<void>;
  get(key: string): Promise<StoredRecord | undefined>;
  listDeadLetters(limit: number): Promise<StoredRecord[]>;
  remove(key: string): Promise<void>;
}
