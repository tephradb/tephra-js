// A minimal append then read round-trip. Run it against a local server with:
//
//   npx tsx examples/roundtrip.ts
//
// In your own project, import from the package instead: import { Client, Event } from "@tephradb/client".

import {
  AppendCondition,
  Client,
  ErrorCode,
  Event,
  Query,
  QueryItem,
  ServerError,
  ZERO,
} from "../src/index.js";

async function main(): Promise<void> {
  const client = await Client.connect("127.0.0.1:9000");
  try {
    // Append an event: a type, tags, and an opaque payload.
    const payload = new TextEncoder().encode(JSON.stringify({ course: "c1", seats: 30 }));
    const event = Event.create("CourseOpened", ["course:c1"], payload);
    const result = await client.append([event]);
    console.log(`recorded positions ${result.first} to ${result.last}`);

    // An idempotency guard: `existsOnly` reports a duplicate as a distinct AlreadyExists error
    // (not a boundary Conflict), so a retry can be treated as "already applied".
    const dedupe = AppendCondition.existsOnly(Query.items(QueryItem.withTags("cmd:order-42")));
    await client.append([Event.create("OrderPlaced", ["cmd:order-42"])], dedupe);
    try {
      await client.append([Event.create("OrderPlaced", ["cmd:order-42"])], dedupe);
      console.log("duplicate order unexpectedly succeeded");
    } catch (err) {
      if (err instanceof ServerError && err.code === ErrorCode.AlreadyExists) {
        console.log(`duplicate order already applied at ${err.conflictPosition}`);
      } else {
        throw err;
      }
    }

    // Read every event from the beginning.
    const { events, watermark } = await client.readAll(Query.all(), ZERO);
    for (const seq of events) {
      console.log(`${seq.position} ${seq.event.type}`);
    }
    console.log(`watermark ${watermark}`);
  } finally {
    await client.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
