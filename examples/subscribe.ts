// A live subscription: catch up on existing events, then tail new ones. Run it with:
//
//   npx tsx examples/subscribe.ts
//
// In your own project, import from the package instead: import { Client } from "@tephradb/client".

import { Client, Query, ZERO, isCaughtUp } from "../src/index.js";

async function main(): Promise<void> {
  const client = await Client.connect("127.0.0.1:9000");
  const subscription = client.subscribe(Query.all(), ZERO);

  process.on("SIGINT", () => {
    void subscription.close().then(() => client.close());
  });

  for await (const item of subscription) {
    if (isCaughtUp(item)) {
      console.log(`caught up at ${item.watermark}`);
      continue;
    }
    console.log(`${item.event.position} ${item.event.event.type}`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
