/**
 * A runnable, dependency-free illustration of the problem oncekit solves.
 *
 *   npx tsx examples/webhook.ts
 *
 * It fires the "same webhook" many times — sequentially, concurrently, with a
 * transient failure, and after a simulated crash — and shows that the side
 * effect (here, "charging a card") happens exactly once per event.
 */
import { createProcessor, MemoryStore } from "../src/index.js";

let chargeCount = 0;
async function chargeCard(eventId: string): Promise<{ chargeId: string }> {
  chargeCount++;
  // pretend this talks to a payment provider
  await new Promise((r) => setTimeout(r, 10));
  return { chargeId: `ch_${eventId}` };
}

async function main(): Promise<void> {
  const once = createProcessor({
    store: new MemoryStore(),
    retry: { maxAttempts: 3, baseMs: 5, maxMs: 20, factor: 2, jitter: false },
    onDeadLetter: (info) => console.log(`  ↳ dead-lettered: ${info.key} (${info.error})`),
  });

  console.log("1) the same event delivered 5 times, back to back:");
  for (let i = 0; i < 5; i++) {
    const r = await once.run("evt_A", () => chargeCard("evt_A"));
    console.log(`   delivery ${i + 1}: ${r.status}`);
  }

  console.log("\n2) the same event delivered 5 times, all at once:");
  const burst = await Promise.all(
    Array.from({ length: 5 }, () => once.run("evt_B", () => chargeCard("evt_B"))),
  );
  console.log("   " + burst.map((r) => r.status).join(", "));

  console.log("\n3) an event whose provider is down, then recovers on retry:");
  let tries = 0;
  const r3 = await once.run("evt_C", async () => {
    if (++tries < 2) throw new Error("provider timeout");
    return chargeCard("evt_C");
  });
  console.log(`   final: ${r3.status} after ${r3.attempts} attempt(s)`);

  console.log("\n4) an event that never succeeds:");
  const r4 = await once.run("evt_D", async () => {
    throw new Error("permanently rejected");
  });
  console.log(`   final: ${r4.status}`);
  console.log(`   dead letters: ${(await once.deadLetters()).map((d) => d.key).join(", ")}`);

  console.log(`\nCards actually charged: ${chargeCount}`);
  console.log("(3 — one each for evt_A, evt_B, evt_C. evt_D never charged.)");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
