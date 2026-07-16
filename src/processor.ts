import type { Store, StoredRecord } from "./store.js";
import { backoffDelay, resolveRetry, type RetryPolicy } from "./retry.js";

export interface DeadLetter {
  key: string;
  error: string;
  attempts: number;
}

export interface ProcessorOptions {
  store: Store;
  // How long a claim is held before another worker may take it over. Set it
  // above your slowest effect. Default 30s.
  leaseMs?: number;
  retry?: Partial<RetryPolicy>;
  onDeadLetter?: (info: DeadLetter, error: unknown) => void | Promise<void>;
  // If true, a duplicate for an in-flight key waits for the owner and returns
  // its result instead of returning "in_flight" immediately.
  waitForInFlight?: boolean;
  pollMs?: number;
  waitTimeoutMs?: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

export type RunStatus = "executed" | "recovered" | "duplicate" | "in_flight" | "dead_letter";

export interface RunResult<T> {
  status: RunStatus;
  result?: T;
  error?: string;
  attempts: number;
}

const defaultSleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export class Processor {
  private readonly store: Store;
  private readonly leaseMs: number;
  private readonly retry: RetryPolicy;
  private readonly onDeadLetter?: ProcessorOptions["onDeadLetter"];
  private readonly waitForInFlight: boolean;
  private readonly pollMs: number;
  private readonly waitTimeoutMs: number;
  private readonly now: () => number;
  private readonly sleep: (ms: number) => Promise<void>;

  constructor(options: ProcessorOptions) {
    this.store = options.store;
    this.leaseMs = options.leaseMs ?? 30_000;
    this.retry = resolveRetry(options.retry);
    this.onDeadLetter = options.onDeadLetter;
    this.waitForInFlight = options.waitForInFlight ?? false;
    this.pollMs = options.pollMs ?? 50;
    this.waitTimeoutMs = options.waitTimeoutMs ?? this.leaseMs;
    this.now = options.now ?? Date.now;
    this.sleep = options.sleep ?? defaultSleep;
  }

  // Run `effect` at most once for `key`. Duplicates return the cached result;
  // concurrent callers get "in_flight"; a key whose owner crashed is reclaimed
  // and re-run once its lease expires; an effect that keeps throwing is
  // dead-lettered after `retry.maxAttempts`.
  async run<T>(key: string, effect: () => T | Promise<T>): Promise<RunResult<T>> {
    const deadline = this.now() + this.waitTimeoutMs;

    for (;;) {
      const { outcome, record } = await this.store.claim(key, this.leaseMs, this.now());

      if (outcome === "done") {
        return { status: "duplicate", result: record.result as T, attempts: 0 };
      }
      if (outcome === "failed") {
        return { status: "dead_letter", error: record.error ?? "unknown error", attempts: record.attempts };
      }
      if (outcome === "in_flight") {
        if (this.waitForInFlight && this.now() < deadline) {
          await this.sleep(this.pollMs);
          continue;
        }
        return { status: "in_flight", attempts: 0 };
      }
      return this.execute(key, effect, outcome === "reclaimed");
    }
  }

  private async execute<T>(
    key: string,
    effect: () => T | Promise<T>,
    reclaimed: boolean,
  ): Promise<RunResult<T>> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= this.retry.maxAttempts; attempt++) {
      try {
        const result = await effect();
        await this.store.finalize(key, result, this.now());
        return { status: reclaimed ? "recovered" : "executed", result, attempts: attempt };
      } catch (err) {
        lastError = err;
        if (attempt < this.retry.maxAttempts) {
          await this.sleep(backoffDelay(attempt, this.retry));
        }
      }
    }

    const message = errorMessage(lastError);
    await this.store.markFailed(key, message, this.retry.maxAttempts, this.now());
    if (this.onDeadLetter) {
      await this.onDeadLetter({ key, error: message, attempts: this.retry.maxAttempts }, lastError);
    }
    return { status: "dead_letter", error: message, attempts: this.retry.maxAttempts };
  }

  inspect(key: string): Promise<StoredRecord | undefined> {
    return this.store.get(key);
  }

  deadLetters(limit = 100): Promise<StoredRecord[]> {
    return this.store.listDeadLetters(limit);
  }

  // Clears a key's state and runs it fresh. You pass the effect again because
  // oncekit stores keys and results, never your payloads.
  async retryKey<T>(key: string, effect: () => T | Promise<T>): Promise<RunResult<T>> {
    await this.store.remove(key);
    return this.run(key, effect);
  }

  forget(key: string): Promise<void> {
    return this.store.remove(key);
  }
}

export function createProcessor(options: ProcessorOptions): Processor {
  return new Processor(options);
}

function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  if (typeof err === "string") return err;
  try {
    return JSON.stringify(err);
  } catch {
    return String(err);
  }
}
