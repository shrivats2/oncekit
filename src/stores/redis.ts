import type {
  ClaimOutcome,
  ClaimResult,
  RecordStatus,
  Store,
  StoredRecord,
} from "../store.js";

// Any client whose `eval` matches ioredis's positional signature works —
// `ioredis` satisfies this directly. All operations go through Lua so the
// claim (and every mutation) is atomic without pipelining or WATCH loops.
export interface RedisClient {
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
}

export interface RedisStoreOptions {
  // Key namespace. Default "oncekit:".
  prefix?: string;
  // How long a completed (`done`) record is retained before Redis expires it.
  // Set this to your provider's redelivery window. Default 24h.
  resultTtlMs?: number;
}

const CLAIM = `
local k = KEYS[1]
local now = tonumber(ARGV[1])
local status = redis.call('HGET', k, 'status')
if not status then
  redis.call('HSET', k, 'key', ARGV[4], 'status', 'pending', 'attempts', 1,
    'lease', now + tonumber(ARGV[2]), 'created', now, 'updated', now)
  redis.call('PEXPIRE', k, tonumber(ARGV[3]))
  return {'claimed', redis.call('HGETALL', k)}
end
if status == 'done' then return {'done', redis.call('HGETALL', k)} end
if status == 'failed' then return {'failed', redis.call('HGETALL', k)} end
local lease = tonumber(redis.call('HGET', k, 'lease'))
if lease ~= nil and lease > now then
  return {'in_flight', redis.call('HGETALL', k)}
end
redis.call('HSET', k, 'attempts', tonumber(redis.call('HGET', k, 'attempts')) + 1,
  'lease', now + tonumber(ARGV[2]), 'updated', now)
return {'reclaimed', redis.call('HGETALL', k)}
`;

const FINALIZE = `
redis.call('HSET', KEYS[1], 'status', 'done', 'result', ARGV[2], 'updated', ARGV[1])
redis.call('HDEL', KEYS[1], 'lease')
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]))
return 1
`;

const MARK_FAILED = `
redis.call('HSET', KEYS[1], 'status', 'failed', 'error', ARGV[2],
  'attempts', tonumber(ARGV[3]), 'updated', ARGV[1])
redis.call('HDEL', KEYS[1], 'lease')
redis.call('PERSIST', KEYS[1])
redis.call('ZADD', KEYS[2], tonumber(ARGV[1]), ARGV[4])
return 1
`;

const EXTEND = `
if redis.call('HGET', KEYS[1], 'status') == 'pending' then
  redis.call('HSET', KEYS[1], 'lease', tonumber(ARGV[1]) + tonumber(ARGV[2]), 'updated', ARGV[1])
end
return 1
`;

const LIST_DEAD = `
local members = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[2]) - 1)
local out = {}
for i = 1, #members do
  out[i] = redis.call('HGETALL', ARGV[1] .. members[i])
end
return out
`;

const REMOVE = `
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
`;

// Fast, in-memory store. Note the tradeoff (see docs/design.md): Redis is
// memory-first, so run it as a durable data store — AOF on, no key eviction —
// not as a cache, or it can drop the very records that prevent double-runs.
export class RedisStore implements Store {
  private readonly prefix: string;
  private readonly resultTtlMs: number;
  private readonly deadKey: string;

  constructor(
    private readonly redis: RedisClient,
    options: RedisStoreOptions = {},
  ) {
    this.prefix = options.prefix ?? "oncekit:";
    this.resultTtlMs = options.resultTtlMs ?? 24 * 60 * 60 * 1000;
    this.deadKey = `${this.prefix}dead`;
  }

  private rk(key: string): string {
    return `${this.prefix}r:${key}`;
  }

  async claim(key: string, leaseMs: number, now: number): Promise<ClaimResult> {
    const res = (await this.redis.eval(
      CLAIM,
      1,
      this.rk(key),
      now,
      leaseMs,
      this.resultTtlMs,
      key,
    )) as [ClaimOutcome, string[]];
    return { outcome: res[0], record: parseHash(res[1]) as StoredRecord };
  }

  async finalize(key: string, result: unknown, now: number): Promise<void> {
    await this.redis.eval(
      FINALIZE,
      1,
      this.rk(key),
      now,
      JSON.stringify(result ?? null),
      this.resultTtlMs,
    );
  }

  async markFailed(key: string, error: string, attempts: number, now: number): Promise<void> {
    await this.redis.eval(MARK_FAILED, 2, this.rk(key), this.deadKey, now, error, attempts, key);
  }

  async extendLease(key: string, leaseMs: number, now: number): Promise<void> {
    await this.redis.eval(EXTEND, 1, this.rk(key), now, leaseMs);
  }

  async get(key: string): Promise<StoredRecord | undefined> {
    const flat = (await this.redis.eval(
      `return redis.call('HGETALL', KEYS[1])`,
      1,
      this.rk(key),
    )) as string[];
    return parseHash(flat);
  }

  async listDeadLetters(limit: number): Promise<StoredRecord[]> {
    const rows = (await this.redis.eval(
      LIST_DEAD,
      1,
      this.deadKey,
      `${this.prefix}r:`,
      limit,
    )) as string[][];
    return rows.map((r) => parseHash(r)).filter((r): r is StoredRecord => r !== undefined);
  }

  async remove(key: string): Promise<void> {
    await this.redis.eval(REMOVE, 2, this.rk(key), this.deadKey, key);
  }
}

// HGETALL comes back as a flat [field, value, field, value, ...] array.
function parseHash(flat: string[] | undefined): StoredRecord | undefined {
  if (!flat || flat.length === 0) return undefined;
  const h: Record<string, string> = {};
  for (let i = 0; i < flat.length; i += 2) h[flat[i] as string] = flat[i + 1] as string;

  const record: StoredRecord = {
    key: h.key as string,
    status: h.status as RecordStatus,
    attempts: Number(h.attempts),
    leaseExpiresAt: h.lease !== undefined ? Number(h.lease) : null,
    createdAt: Number(h.created),
    updatedAt: Number(h.updated),
  };
  if (h.result !== undefined) record.result = JSON.parse(h.result);
  if (h.error !== undefined) record.error = h.error;
  return record;
}
