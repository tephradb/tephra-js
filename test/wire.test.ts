import { describe, expect, test } from "vitest";
import {
  PROTOCOL_VERSION,
  type WireRequest,
  type WireResponse,
  decodeRequest,
  decodeResponse,
  encodeRequest,
  encodeResponse,
} from "../src/proto/messages.js";
import { Writer } from "../src/proto/wire.js";

const U64_MAX = 0xffff_ffff_ffff_ffffn;

function roundtripRequest(request: WireRequest): WireRequest {
  return decodeRequest(encodeRequest(request));
}

function roundtripResponse(response: WireResponse): WireResponse {
  return decodeResponse(encodeResponse(response));
}

describe("request roundtrip", () => {
  test("append with events and a condition", () => {
    const request: WireRequest = {
      requestId: 1n,
      kind: {
        kind: "append",
        append: {
          events: [
            {
              type: "CourseOpened",
              tags: ["course:c1", "seats:30"],
              payload: new Uint8Array([0, 1, 2, 255, 0]),
            },
          ],
          condition: {
            failIfEventsMatch: {
              all: false,
              items: [{ types: ["UsernameReserved"], tags: ["username:alice"] }],
            },
            after: 42n,
          },
        },
      },
    };
    expect(roundtripRequest(request)).toEqual(request);
  });

  test("append with no condition leaves it absent", () => {
    const request: WireRequest = {
      requestId: 2n,
      kind: {
        kind: "append",
        append: { events: [{ type: "X", tags: [], payload: new Uint8Array(0) }] },
      },
    };
    const decoded = roundtripRequest(request);
    expect(decoded.kind).toEqual({
      kind: "append",
      append: { events: [{ type: "X", tags: [], payload: new Uint8Array(0) }] },
    });
    if (decoded.kind.kind === "append") {
      expect(decoded.kind.append.condition).toBeUndefined();
    }
  });

  test("read distinguishes an absent limit from an explicit zero", () => {
    const unlimited: WireRequest = {
      requestId: 3n,
      kind: { kind: "read", read: { query: { all: true, items: [] }, after: 0n, reverse: false } },
    };
    const decodedUnlimited = roundtripRequest(unlimited);
    if (decodedUnlimited.kind.kind === "read") {
      expect(decodedUnlimited.kind.read.limit).toBeUndefined();
    }

    const capped: WireRequest = {
      requestId: 4n,
      kind: {
        kind: "read",
        read: { query: { all: true, items: [] }, after: 0n, limit: 0n, reverse: true },
      },
    };
    const decodedCapped = roundtripRequest(capped);
    if (decodedCapped.kind.kind === "read") {
      expect(decodedCapped.kind.read.limit).toBe(0n);
      expect(decodedCapped.kind.read.reverse).toBe(true);
    }
  });

  test("backward read from the tip carries u64::MAX", () => {
    const request: WireRequest = {
      requestId: 5n,
      kind: {
        kind: "read",
        read: { query: { all: true, items: [] }, after: U64_MAX, reverse: true },
      },
    };
    const decoded = roundtripRequest(request);
    if (decoded.kind.kind === "read") {
      expect(decoded.kind.read.after).toBe(U64_MAX);
    }
  });

  test("subscribe, cancel, and stats", () => {
    const subscribe: WireRequest = {
      requestId: 6n,
      kind: { kind: "subscribe", subscribe: { query: { all: true, items: [] }, after: 7n } },
    };
    expect(roundtripRequest(subscribe)).toEqual(subscribe);

    const cancel: WireRequest = { requestId: 8n, kind: { kind: "cancel", cancel: { target: 6n } } };
    expect(roundtripRequest(cancel)).toEqual(cancel);

    const stats: WireRequest = { requestId: 9n, kind: { kind: "stats" } };
    expect(roundtripRequest(stats)).toEqual(stats);
  });

  test("hello with a bearer token", () => {
    const request: WireRequest = {
      requestId: 1n,
      kind: { kind: "hello", hello: { protocolVersion: PROTOCOL_VERSION, authToken: "s3cret" } },
    };
    expect(roundtripRequest(request)).toEqual(request);
  });

  test("hello without a token leaves it absent", () => {
    const request: WireRequest = {
      requestId: 1n,
      kind: { kind: "hello", hello: { protocolVersion: PROTOCOL_VERSION } },
    };
    const decoded = roundtripRequest(request);
    if (decoded.kind.kind === "hello") {
      expect(decoded.kind.hello.protocolVersion).toBe(PROTOCOL_VERSION);
      expect(decoded.kind.hello.authToken).toBeUndefined();
    }
  });
});

describe("response roundtrip", () => {
  test("append response with large positions", () => {
    const response: WireResponse = {
      requestId: 10n,
      kind: { kind: "append", append: { first: U64_MAX - 1n, last: U64_MAX } },
    };
    expect(roundtripResponse(response)).toEqual(response);
  });

  test("read events then read end", () => {
    const events: WireResponse = {
      requestId: 11n,
      kind: {
        kind: "readEvents",
        readEvents: {
          events: [
            { position: 1n, event: { type: "A", tags: ["t:1"], payload: new Uint8Array([9]) } },
            { position: U64_MAX, event: { type: "B", tags: [], payload: new Uint8Array(0) } },
          ],
        },
      },
    };
    expect(roundtripResponse(events)).toEqual(events);

    const end: WireResponse = {
      requestId: 11n,
      kind: { kind: "readEnd", readEnd: { watermark: 12n } },
    };
    expect(roundtripResponse(end)).toEqual(end);
  });

  test("caught-up marker", () => {
    const response: WireResponse = {
      requestId: 12n,
      kind: { kind: "caughtUp", caughtUp: { watermark: 99n } },
    };
    expect(roundtripResponse(response)).toEqual(response);
  });

  test("error response with a conflict position", () => {
    const response: WireResponse = {
      requestId: 13n,
      kind: {
        kind: "error",
        error: { code: 1, message: "condition failed", conflictPosition: 5n, retryable: false },
      },
    };
    expect(roundtripResponse(response)).toEqual(response);
  });

  test("error response without a conflict position leaves it absent", () => {
    const response: WireResponse = {
      requestId: 14n,
      kind: { kind: "error", error: { code: 5, message: "bad request", retryable: true } },
    };
    const decoded = roundtripResponse(response);
    if (decoded.kind.kind === "error") {
      expect(decoded.kind.error.conflictPosition).toBeUndefined();
      expect(decoded.kind.error.retryable).toBe(true);
    }
  });

  test("hello ack carries the protocol and server versions", () => {
    const response: WireResponse = {
      requestId: 1n,
      kind: {
        kind: "helloAck",
        helloAck: { protocolVersion: PROTOCOL_VERSION, serverVersion: "0.1.0" },
      },
    };
    expect(roundtripResponse(response)).toEqual(response);
  });

  test("stats response carries every field", () => {
    const response: WireResponse = {
      requestId: 15n,
      kind: {
        kind: "stats",
        stats: {
          eventCount: 1000n,
          segmentCount: 3n,
          diskBytes: 4096n,
          uptimeSeconds: 60n,
          activeConnections: 5n,
          activeSubscriptions: 2n,
          version: "0.1.0",
          connectionsRefused: 0n,
          maxConnections: 1024n,
          connectionsReaped: 1n,
        },
      },
    };
    expect(roundtripResponse(response)).toEqual(response);
  });
});

describe("forward compatibility", () => {
  test("an unknown field is skipped", () => {
    // Hand-build a Response with an unknown varint field and an unknown length-delimited field
    // interleaved with the known ones, then confirm decoding ignores them.
    const w = new Writer();
    w.uint64Present(1, 21n); // request_id
    w.uint64Present(13, 123n); // unknown varint field
    w.message(14, new Uint8Array([1, 2, 3])); // unknown length-delimited field
    w.message(4, encodeReadEnd(77n)); // known: read_end (field 4)
    const decoded = decodeResponse(w.finish());
    expect(decoded.requestId).toBe(21n);
    expect(decoded.kind).toEqual({ kind: "readEnd", readEnd: { watermark: 77n } });
  });
});

// A tiny local helper for the forward-compatibility test.
function encodeReadEnd(watermark: bigint): Uint8Array {
  const w = new Writer();
  w.uint64(1, watermark);
  return w.finish();
}
