// A minimal append then read round-trip. Run it against a local server with:
//
//   npx tsx examples/roundtrip.ts
//
// In your own project, import from the package instead: import { Client, Event } from "@tephradb/client".

import { Client, Event, Query, ZERO } from "../src/index.js";

async function main(): Promise<void> {
  const client = await Client.connect("127.0.0.1:9000");
  try {
    // Append an event: a type, tags, and an opaque payload.
    const payload = new TextEncoder().encode(JSON.stringify({ course: "c1", seats: 30 }));
    const event = Event.create("CourseOpened", ["course:c1"], payload);
    const result = await client.append([event]);
    console.log(`recorded positions ${result.first} to ${result.last}`);

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
