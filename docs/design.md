# Design notes

This document explains what oncekit guarantees, how, and — just as importantly
— what it does not. If you only read one section, read
[The exactly-once problem](#the-exactly-once-problem).

## The model: reserve → finalize → reconcile

Every key flows through three phases.

1. **Reserve** — a `claim` atomically creates a `pending` record with a
   time-boxed *lease* and hands ownership to exactly one caller. Concurrent
   claimants see `in_flight`, or the terminal `done` / `failed` state.
2. **Finalize** — on success, the result is stored under the key and the
   record becomes `done`. Future claims short-circuit to the cached result.
3. **Reconcile** — if the owner never finalizes (it crashed, was OOM-killed,
   lost the network), the lease expires and the next claimant *reclaims* the
   key and re-runs the effect.

The entire correctness argument rests on one property: **`claim` is atomic.**
Two workers racing on the same key must never both receive an
executing outcome (`claimed` / `reclaimed`).

- `MemoryStore` gets this for free: Node runs one turn of the event loop at a
  time and `claim` performs no `await` before it mutates the map.
- `PostgresStore` gets it from the database: `INSERT ... ON CONFLICT DO
  NOTHING` lets exactly one row win the insert, and the takeover is a single
  conditional `UPDATE ... WHERE status = 'pending' AND lease_expires_at <= now`
  that only one transaction can satisfy.

## The exactly-once problem

**True exactly-once execution of an arbitrary side effect is impossible in a
system where processes can crash.** Consider the two operations that matter:

```
A: perform the effect      (charge the card)
B: record that it happened  (finalize the key)
```

There is always an instant *between* A and B. If the process dies there, then:

- If we assume A happened and skip it on retry → we risk **losing** effects
  that never actually completed.
- If we re-run A on retry → we risk **duplicating** effects that did complete.

No amount of engineering removes this instant. It can only be **moved** to a
place where a duplicate or a loss is harmless. oncekit gives you three ways to
do that, in increasing order of strength.

### Level 1 — effectively-once (the default)

oncekit removes every duplicate that does **not** involve a crash in the A→B
gap: redelivery, concurrent delivery, and benign retries all collapse to a
single execution. A crash strictly between A and B is the only window that can
still double-run, and it is bounded by your lease and retry settings.

For many effects this is enough, because the effect is naturally idempotent
(upserting a row by primary key, setting a flag, writing to an object store at
a deterministic path).

### Level 2 — transactional finalize (true once, for DB effects)

If your effect is a database write, put the write and the finalize in the
**same transaction**. Now A and B commit atomically — there is no gap:

```ts
await db.transaction(async (tx) => {
  await tx.insert(orders).values(order);   // A
  await tx.execute(finalizeKeyStatement);  // B — same commit as A
});
```

Either both happen or neither does. A crash before commit leaves a `pending`
key with an expired lease, which is safely reclaimed and retried; a crash after
commit leaves a `done` key, which dedupes. This is genuine exactly-once for the
database. (A first-class transactional store helper is on the roadmap; today you
wire it with your own client.)

### Level 3 — provider idempotency keys (for non-transactional effects)

You cannot put "charge a card" in your database transaction. For effects that
live in someone else's system, align **their** idempotency key with your
oncekit key:

```ts
await once.run(event.id, () =>
  stripe.charges.create(
    { amount, currency, customer },
    { idempotencyKey: event.id },   // same key oncekit dedupes on
  ),
);
```

Now a reclaim-and-retry after a crash is safe: the provider recognizes the key
and returns the original charge instead of making a second one. You have pushed
the dedupe responsibility to the only system that can actually enforce it — the
one performing the effect.

## Failure windows, walked through

| When the crash happens | State left behind | What the next caller does |
| --- | --- | --- |
| Before `claim` commits | nothing | claims fresh, runs the effect |
| After `claim`, before the effect | `pending`, lease ticking | waits (`in_flight`) until the lease expires, then reclaims |
| Mid-effect | `pending`, lease ticking | reclaims after lease expiry and re-runs (Level 1: possible double effect; Levels 2–3: safe) |
| After effect, before `finalize` | `pending` | reclaims and re-runs — **this is the irreducible gap**; close it with Level 2 or 3 |
| After `finalize` | `done` + cached result | dedupes, returns the cached result |
| After `markFailed` | `failed` | returns `dead_letter`; you `retryKey` when ready |

## Leases and timeouts

The lease is the knob that trades *recovery speed* against *double-execution
risk under Level 1*:

- **Too short** and a slow-but-alive worker gets its key stolen, causing an
  unnecessary re-run.
- **Too long** and genuine crashes take that long to recover.

Set `leaseMs` comfortably above your p99 effect duration. For long effects, call
`extendLease` as a heartbeat, or raise the lease for that key class. There is no
universally correct value — it is a property of your workload, so oncekit makes
it explicit rather than guessing.

## Retries and dead-letters

Within a single claim, a throwing effect is retried up to `retry.maxAttempts`
with exponential backoff and optional jitter. When attempts are exhausted the
key becomes `failed` and `onDeadLetter` fires once. Dead-lettered keys are
**not** retried automatically on later calls — they return `dead_letter`
immediately — so a poison message can't burn your workers in a hot loop. You
decide when to `retryKey`, after a fix or a human looks at it.

## Choosing a store, and why not just Redis

The store is pluggable because there is no single right answer — there is a
speed↔durability curve, and different workloads sit at different points.

First, calibrate: **the store is rarely the bottleneck.** A claim plus a finalize
is two single-key indexed operations, ~1–4ms on Postgres with a round trip. The
effect you wrap is usually 10–500ms. Optimizing the store before the store is
your limit is optimizing the wrong thing.

**Postgres is the right default.** You likely already run it, it survives the
crash it's meant to recover from, and it's the only option that reaches genuine
exactly-once (Level 2 above: finalize in the same transaction as a DB effect).

**Redis is faster, and a clean fit for the claim primitive** — `SET NX PX` (or,
here, a Lua script) is reserve-with-lease in one atomic command, and the key TTL
*is* the lease. Reach for it at high throughput (tens of thousands of dedup
ops/sec) with idempotent or non-transactional effects. But two caveats decide
whether it's correct:

1. **It must not behave like a cache.** A cache evicts under memory pressure by
   design. If it evicts your `done` record, the next redelivery re-runs the
   effect — a double charge, not a cache miss. Run Redis with no key eviction on
   these keys; a pure cache (memcached) is the wrong tool outright.
2. **Memory-first means weaker durability.** Default persistence can lose the
   last seconds of writes on a crash — again, lost dedup state means a re-run.
   `appendfsync always` closes the window but spends most of the speed you came
   for. And Redis can't do a transactional finalize with a SQL effect, so you're
   at effectively-once, not exactly-once.

`RedisStore` mitigates growth by giving `done` records a retention TTL (the
provider's redelivery window), which doubles as automatic cleanup — one thing
Postgres still needs a sweeper for. That's the trade in miniature: Redis buys
speed and self-expiry; Postgres buys durability and transactional correctness.

## Why oncekit stores keys, not payloads

oncekit persists keys, statuses, results, and errors — never your original
message payloads. Payloads can be large, sensitive, and already live in your
queue or webhook log. Keeping them out means the store stays small and free of
PII, and it makes the trust boundary obvious: to replay a dead letter you supply
the effect again (you still have the payload), and oncekit supplies the
at-most-once guarantee around it.

## Non-goals

- **Not a queue.** oncekit does not deliver or schedule messages; it makes your
  processing of them correct. Put it *inside* your consumer.
- **Not a distributed lock manager.** The lease is a soft ownership hint scoped
  to a key, not a general mutual-exclusion primitive.
- **Not a workflow engine.** No multi-step orchestration, no sagas. One key,
  one effect, done well.
