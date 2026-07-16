export interface RetryPolicy {
  maxAttempts: number;
  baseMs: number;
  maxMs: number;
  factor: number;
  jitter: boolean;
}

export const defaultRetry: RetryPolicy = {
  maxAttempts: 3,
  baseMs: 100,
  maxMs: 5_000,
  factor: 2,
  jitter: true,
};

export function backoffDelay(attempt: number, p: RetryPolicy): number {
  const raw = Math.min(p.maxMs, p.baseMs * Math.pow(p.factor, attempt - 1));
  return p.jitter ? Math.random() * raw : raw;
}

export function resolveRetry(partial?: Partial<RetryPolicy>): RetryPolicy {
  return { ...defaultRetry, ...partial };
}
