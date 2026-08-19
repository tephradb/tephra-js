# @tephradb/client

A TypeScript client for a [tephra](https://github.com/tephradb/tephra) event store, speaking its
length-prefixed protobuf-over-TCP protocol. It is wire-compatible with `tephra-server` and mirrors
the design of the reference Rust `tephra-client` and the Go client: a single, concurrent-safe
`Client` that multiplexes many requests over a control socket plus a pool of bulk read sockets.

```sh
npm install @tephradb/client
```

Requires Node.js 18 or newer, and tephra 0.4 or newer (which introduced the mandatory `Hello`
handshake this client speaks). It has zero runtime dependencies (the protobuf codec is hand written).

## Quick start

```ts
import { Client, Event, Query, ZERO } from "@tephradb/client";

const client = await Client.connect("127.0.0.1:9000");
try {
  const event = Event.create("Enrolled", ["course:c1", "student:s1"], new TextEncoder().encode("{}"));
  await client.append([event]);

  const { events, watermark } = await client.readAll(Query.all(), ZERO);
  for (const seq of events) {
    console.log(`${seq.position} ${seq.event.type}`);
  }
} finally {
  await client.close();
}
```

## Concepts

- **Event**: a type, a set of tags, and an opaque payload (a `Uint8Array`). Build one with
  `Event.create`, which validates the type and tags (non-empty, at most 65535 bytes each, no
  duplicate tags) exactly as the server does, and stores tags sorted so identical sets encode
  identically.
- **Position**: a dense, 1-based global order, held as a `bigint`. `ZERO` is before everything (the
  start cursor for a forward read); `MAX` is the "from the tip" cursor for a backward read.
- **Query**: `Query.all()` matches everything; `Query.items(...)` OR's items, where each item AND's
  its tags and OR's its types (an empty item set matches nothing, distinct from the catch-all).
  Build items with `QueryItem.ofTypes`, `QueryItem.withTags`, or `QueryItem.of`.
- **AppendCondition**: a dynamic consistency boundary. Reject the append if any event after `after`
  matches the query. `after` defaults to `ZERO`, which considers the whole log (the uniqueness-guard
  pattern). Build one with `AppendCondition.create(query, after?)`.

## Reads and pagination

`read` returns a `ReadStream`, an async iterable; drive it with `for await`, then read its
`watermark` once it ends. `readAll` drains one into an array. `readBack` and `readAllBack` are the
newest-first duals.

`after` (exclusive) and `limit` compose into a stateless pagination cursor: read a page, then read
again with `after` set to the last position returned.

```ts
let cursor = ZERO;
for (;;) {
  const page = await client.readAll(query, cursor, 100);
  for (const seq of page.events) {
    handle(seq);
  }
  if (page.events.length === 0) {
    break;
  }
  cursor = page.events[page.events.length - 1].position; // next page starts here, no gap or duplicate
}
```

A streaming read is an async iterable, so you can also consume it incrementally:

```ts
for await (const seq of client.read(Query.all(), ZERO)) {
  console.log(`${seq.position} ${seq.event.type}`);
}
```

## Subscriptions

`subscribe` catches up on matching events, then tails new ones live, delivering a caught-up marker
each time it reaches the live edge:

```ts
import { isCaughtUp } from "@tephradb/client";

const subscription = client.subscribe(Query.all(), ZERO);
for await (const item of subscription) {
  if (isCaughtUp(item)) {
    continue;
  }
  handle(item.event);
}
```

Cancel a stream by calling `close`, or by passing an `AbortSignal` and aborting it. Either sends a
best-effort cancel to the server so it stops producing frames. Breaking out of a `for await` loop
closes the stream too.

## Errors

The client throws typed errors, all extending `TephraError`. It performs no automatic retries or
reconnection: on a durable failure it surfaces the error and leaves policy to you.

- `ServerError`: the server returned an error. `code` is an `ErrorCode`; `retryable` marks an
  advisory same-batch append conflict (safe to retry); `conflictPosition` is set for a durable
  append conflict.
- `ProtocolError`: the peer sent something outside the protocol.
- `ConnError`: the connection failed with requests in flight; every in-flight request is failed with
  it (never left hanging). The underlying cause is available on `cause`.
- `FrameTooLargeError`: a frame exceeded the configured maximum (`length` and `max` report the sizes).
- `ValidationError`: an event type or tag failed validation before it reached the wire.
- `ClosedError`: the client was closed.

```ts
import { ErrorCode, ServerError } from "@tephradb/client";

try {
  await client.append([event], guard);
} catch (err) {
  if (err instanceof ServerError && err.code === ErrorCode.Conflict) {
    // handle the append conflict
  } else {
    throw err;
  }
}
```

## Configuration and design

`Client.connect` takes an options object; the defaults mirror the reference Rust and Go clients:

| Option | Default | Meaning |
| --- | --- | --- |
| `bulkConnections` | 4 | Dedicated bulk sockets for reads and subscriptions. `0` folds reads onto the control socket. |
| `maxInflightRequests` | 1024 | Outstanding requests per socket before backpressure. |
| `requestQueueDepth` | 256 | Outbound queue depth per socket. |
| `maxFrameLen` | 16 MiB | Largest frame accepted or produced. |
| `connectTimeout` | none | Bounds the dial, in milliseconds. |
| `tls` | off | `true` for the system roots, or an object for a private CA, mutual TLS, or a custom `minVersion`. |
| `authToken` | none | A bearer token presented in each socket's opening handshake (see [Authentication](#authentication)). |
| `signal` | none | An `AbortSignal` that aborts the connect. |

A `Client` is safe to use concurrently. Internally each socket runs a reader loop (which
demultiplexes responses by request id) and a writer loop (which coalesces queued frames into one
flush per burst). Appends and stats ride the **control** socket; reads and subscriptions round-robin
across the **bulk** pool. Splitting the lanes keeps a large read response from delaying a small
append (head-of-line blocking), and each stream buffers its frames so a slow consumer never stalls
the shared socket; backpressure comes instead from the per-socket in-flight budget.

Every operation also accepts an `AbortSignal` for cancellation and deadlines:

```ts
const controller = new AbortController();
const timer = setTimeout(() => controller.abort(), 5000);
try {
  await client.append([event], null, { signal: controller.signal });
} finally {
  clearTimeout(timer);
}
```

## TLS

The tephra server can serve implicit TLS (TLS 1.3, server-authenticated). Enable it on the client
with the `tls` option, which passes through to Node's `tls.connect`:

```ts
import { readFileSync } from "node:fs";

// Verify against the system roots (a public CA):
const client = await Client.connect("tephra.example.com:9000", { tls: true });

// Or trust a private CA, and present a client certificate for mutual TLS:
const client = await Client.connect("tephra.internal:9000", {
  tls: {
    ca: readFileSync("ca.pem"),
    cert: readFileSync("client.pem"),
    key: readFileSync("client-key.pem"),
  },
});
```

`servername` defaults to the host in the dial address, so verifying a hostname certificate needs no
extra configuration. The TLS session is established before the first frame; the wire protocol is
unchanged, so everything else behaves identically to a plaintext connection.

## Authentication

Every connection opens with a mandatory `Hello`/`HelloAck` handshake that negotiates the protocol
version, a single compatibility gate: a client and server must be on matching protocol versions. The
client runs it on each socket (control and bulk) before any request rides it; you never see the
handshake, but a version mismatch fails the connect with a `ProtocolError`.

When the server requires authentication, pass a bearer token with `authToken`. It is carried in each
socket's `Hello`:

```ts
const client = await Client.connect("tephra.example.com:9000", {
  tls: true,
  authToken: process.env.TEPHRA_TOKEN,
});
```

The server gates tokens behind TLS, so pair `authToken` with `tls` (a plaintext token is only
accepted by a server explicitly configured to allow it, e.g. behind a TLS-terminating proxy). A
missing or rejected token fails the connect with a `ServerError` whose `code` is
`ErrorCode.Unauthenticated`, up front rather than on the first request:

```ts
import { ErrorCode, ServerError } from "@tephradb/client";

try {
  await Client.connect("tephra.example.com:9000", { tls: true, authToken: "wrong" });
} catch (err) {
  if (err instanceof ServerError && err.code === ErrorCode.Unauthenticated) {
    // bad or missing token
  }
}
```

Leaving `authToken` unset connects unauthenticated, which a server with no tokens configured accepts.

## Development

The wire format is implemented by hand in `src/proto`, so consumers need no protobuf toolchain. The
schema it mirrors is committed at `proto/tephra/v1/tephra.proto` for reference.

```sh
npm run build          # dual ESM + CJS bundle with type declarations (tsup)
npm run typecheck      # tsc --noEmit
npm run lint           # biome
npm test               # unit tests (no server needed)
npm run test:integration   # integration tests against a real tephra-server
```

The integration tests build `tephra-server` from a sibling `../tephra` checkout (override with
`TEPHRA_REPO`, or point `TEPHRA_SERVER_BIN` at a prebuilt binary). They skip themselves when neither
is available.

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
