import { type Server, type Socket, createServer } from "node:net";
import { afterEach, describe, expect, test } from "vitest";
import { DEFAULT_MAX_FRAME_LEN, FrameDecoder, writeFrame } from "../src/framing.js";
import {
  AppendCondition,
  Client,
  ConnError,
  ErrorCode,
  Event,
  PROTOCOL_VERSION,
  ProtocolError,
  Query,
  QueryItem,
  ServerError,
  isCaughtUp,
} from "../src/index.js";
import {
  type WireRequest,
  type WireResponse,
  decodeRequest,
  encodeResponse,
} from "../src/proto/messages.js";
import { Deferred } from "../src/util.js";

interface ServerConn {
  socket: Socket;
  send(response: WireResponse): void;
}

type Handler = (request: WireRequest, conn: ServerConn) => void;

/** Answers the mandatory opening Hello. Defaults to acknowledging with the negotiated version. */
type HelloHandler = (request: WireRequest, conn: ServerConn) => void;

const defaultHello: HelloHandler = (request, conn) => {
  conn.send({
    requestId: request.requestId,
    kind: {
      kind: "helloAck",
      helloAck: { protocolVersion: PROTOCOL_VERSION, serverVersion: "test" },
    },
  });
};

/** A minimal in-memory server speaking the tephra wire protocol, for driving the client in tests. */
class MockServer {
  private readonly sockets: Socket[] = [];

  private constructor(private readonly server: Server) {}

  static start(handler: Handler, onHello: HelloHandler = defaultHello): Promise<MockServer> {
    const server = createServer();
    const mock = new MockServer(server);
    server.on("connection", (socket) => {
      mock.sockets.push(socket);
      socket.setNoDelay(true);
      socket.on("error", () => {});
      const decoder = new FrameDecoder(DEFAULT_MAX_FRAME_LEN);
      const conn: ServerConn = {
        socket,
        send: (response) => {
          if (!socket.destroyed) {
            socket.write(writeFrame(encodeResponse(response), DEFAULT_MAX_FRAME_LEN));
          }
        },
      };
      socket.on("data", (chunk) => {
        let bodies: Uint8Array[];
        try {
          bodies = decoder.push(chunk);
        } catch {
          socket.destroy();
          return;
        }
        for (const body of bodies) {
          const request = decodeRequest(body);
          // The first frame on every socket is a Hello; answer it out of band so per-test handlers
          // only see real requests (matching the mandatory handshake the client now performs).
          if (request.kind.kind === "hello") {
            onHello(request, conn);
          } else {
            handler(request, conn);
          }
        }
      });
    });
    return new Promise((resolve) => {
      server.listen(0, "127.0.0.1", () => resolve(mock));
    });
  }

  get port(): number {
    const address = this.server.address();
    if (address === null || typeof address === "string") {
      throw new Error("server is not listening on a TCP port");
    }
    return address.port;
  }

  async close(): Promise<void> {
    for (const socket of this.sockets) {
      socket.destroy();
    }
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }
}

const servers: MockServer[] = [];
const clients: Client[] = [];

/** Starts a mock server registered for teardown, without connecting a client. */
async function startServer(handler: Handler, onHello?: HelloHandler): Promise<MockServer> {
  const server = await MockServer.start(handler, onHello);
  servers.push(server);
  return server;
}

async function connect(
  handler: Handler,
  options?: Parameters<typeof Client.connect>[1],
  onHello?: HelloHandler,
) {
  const server = await startServer(handler, onHello);
  const client = await Client.connect(`127.0.0.1:${server.port}`, options);
  clients.push(client);
  return { server, client };
}

afterEach(async () => {
  await Promise.all(clients.splice(0).map((client) => client.close()));
  await Promise.all(servers.splice(0).map((server) => server.close()));
});

describe("Client", () => {
  test("append returns the assigned position range", async () => {
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "append") {
        conn.send({
          requestId: request.requestId,
          kind: { kind: "append", append: { first: 1n, last: 1n } },
        });
      }
    });
    const event = Event.create("Enrolled", ["course:c1"], new Uint8Array([123]));
    const result = await client.append([event]);
    expect(result).toEqual({ first: 1n, last: 1n });
  });

  test("stats maps every field", async () => {
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "stats") {
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "stats",
            stats: {
              eventCount: 5n,
              segmentCount: 1n,
              diskBytes: 2048n,
              uptimeSeconds: 30n,
              activeConnections: 5n,
              activeSubscriptions: 0n,
              version: "0.1.0",
              connectionsRefused: 0n,
              maxConnections: 0n,
              connectionsReaped: 0n,
            },
          },
        });
      }
    });
    const stats = await client.stats();
    expect(stats.eventCount).toBe(5n);
    expect(stats.version).toBe("0.1.0");
    expect(stats.diskBytes).toBe(2048n);
  });

  test("read streams events then a watermark", async () => {
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "read") {
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "readEvents",
            readEvents: {
              events: [
                { position: 1n, event: { type: "A", tags: ["t:1"], payload: new Uint8Array([1]) } },
                { position: 2n, event: { type: "B", tags: [], payload: new Uint8Array() } },
              ],
            },
          },
        });
        conn.send({
          requestId: request.requestId,
          kind: { kind: "readEnd", readEnd: { watermark: 2n } },
        });
      }
    });
    const { events, watermark } = await client.readAll(Query.all());
    expect(events.map((event) => event.event.type)).toEqual(["A", "B"]);
    expect(events.map((event) => event.position)).toEqual([1n, 2n]);
    expect(watermark).toBe(2n);
  });

  test("multiplexes concurrent reads, demuxing by request id", async () => {
    // Each read echoes its own cursor back as the watermark, so a crossed wire would be caught.
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "read") {
        conn.send({
          requestId: request.requestId,
          kind: { kind: "readEnd", readEnd: { watermark: request.kind.read.after } },
        });
      }
    });
    const cursors = Array.from({ length: 32 }, (_, i) => BigInt(i + 1));
    const watermarks = await Promise.all(
      cursors.map(async (cursor) => (await client.readAll(Query.all(), cursor)).watermark),
    );
    expect(watermarks).toEqual(cursors);
  });

  test("surfaces a server error as a ServerError", async () => {
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "append") {
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "error",
            error: { code: 1, message: "condition failed", conflictPosition: 5n, retryable: false },
          },
        });
      }
    });
    const event = Event.create("UsernameReserved", ["username:alice"]);
    await expect(client.append([event])).rejects.toBeInstanceOf(ServerError);
    try {
      await client.append([event]);
    } catch (err) {
      const serverError = err as ServerError;
      expect(serverError.code).toBe(ErrorCode.Conflict);
      expect(serverError.retryable).toBe(false);
      expect(serverError.conflictPosition).toBe(5n);
    }
  });

  test("sends an existence clause and reports AlreadyExists distinctly", async () => {
    let sawExistence = false;
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "append") {
        // The condition carries the existence clause but no boundary items.
        const condition = request.kind.append.condition;
        sawExistence =
          condition?.failIfExists?.items.some((item) => item.tags.includes("cmd:abc")) === true &&
          condition.failIfEventsMatch.all === false &&
          condition.failIfEventsMatch.items.length === 0;
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "error",
            error: {
              code: 9,
              message: "already exists",
              conflictPosition: 1n,
              retryable: false,
            },
          },
        });
      }
    });
    const condition = AppendCondition.existsOnly(Query.items(QueryItem.withTags("cmd:abc")));
    const event = Event.create("OrderPlaced", ["cmd:abc"]);
    try {
      await client.append([event], condition);
      throw new Error("expected an AlreadyExists conflict");
    } catch (err) {
      const serverError = err as ServerError;
      expect(serverError).toBeInstanceOf(ServerError);
      expect(serverError.code).toBe(ErrorCode.AlreadyExists);
      expect(serverError.retryable).toBe(false);
      expect(serverError.conflictPosition).toBe(1n);
    }
    expect(sawExistence).toBe(true);
  });

  test("fails an in-flight request when the connection drops", async () => {
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "append") {
        conn.socket.destroy();
      }
    });
    const event = Event.create("Enrolled", ["course:c1"]);
    await expect(client.append([event])).rejects.toBeInstanceOf(ConnError);
  });

  test("subscribe yields events then a caught-up marker, and closing cancels server-side", async () => {
    let subscriptionId: bigint | undefined;
    const cancelled = new Deferred<bigint>();
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "subscribe") {
        subscriptionId = request.requestId;
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "readEvents",
            readEvents: {
              events: [{ position: 1n, event: { type: "A", tags: [], payload: new Uint8Array() } }],
            },
          },
        });
        conn.send({
          requestId: request.requestId,
          kind: { kind: "caughtUp", caughtUp: { watermark: 1n } },
        });
      } else if (request.kind.kind === "cancel") {
        cancelled.resolve(request.kind.cancel.target);
      }
    });

    const seen: string[] = [];
    for await (const item of client.subscribe(Query.all())) {
      if (isCaughtUp(item)) {
        seen.push("caughtUp");
        break;
      }
      seen.push(item.event.event.type);
    }
    expect(seen).toEqual(["A", "caughtUp"]);
    expect(await cancelled.promise).toBe(subscriptionId);
  });

  test("a large streaming read does not block a concurrent append", async () => {
    // The append rides the control socket while the read streams on a bulk socket. The server holds
    // the read open (never sends ReadEnd), yet the append must still complete.
    const { client } = await connect((request, conn) => {
      if (request.kind.kind === "read") {
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "readEvents",
            readEvents: {
              events: Array.from({ length: 100 }, (_, i) => ({
                position: BigInt(i + 1),
                event: { type: "Big", tags: [], payload: new Uint8Array(1024) },
              })),
            },
          },
        });
        // Deliberately no ReadEnd: the read stays open.
      } else if (request.kind.kind === "append") {
        conn.send({
          requestId: request.requestId,
          kind: { kind: "append", append: { first: 7n, last: 7n } },
        });
      }
    });

    const stream = client.read(Query.all());
    const iterator = stream[Symbol.asyncIterator]();
    // Pull one event so the read is genuinely active.
    await iterator.next();
    const result = await client.append([Event.create("Enrolled", ["course:c1"])]);
    expect(result).toEqual({ first: 7n, last: 7n });
    await stream.close();
  });
});

describe("authentication", () => {
  test("opens with a Hello carrying the protocol version and no token by default", async () => {
    const hello = new Deferred<WireRequest>();
    await connect(
      (_request, _conn) => {},
      { bulkConnections: 0 },
      (request, conn) => {
        hello.resolve(request);
        defaultHello(request, conn);
      },
    );
    const request = await hello.promise;
    expect(request.kind.kind).toBe("hello");
    if (request.kind.kind === "hello") {
      expect(request.kind.hello.protocolVersion).toBe(PROTOCOL_VERSION);
      expect(request.kind.hello.authToken).toBeUndefined();
    }
  });

  test("presents the configured bearer token in the Hello", async () => {
    const hello = new Deferred<WireRequest>();
    await connect(
      (_request, _conn) => {},
      { authToken: "s3cret", bulkConnections: 0 },
      (request, conn) => {
        hello.resolve(request);
        defaultHello(request, conn);
      },
    );
    const request = await hello.promise;
    if (request.kind.kind === "hello") {
      expect(request.kind.hello.authToken).toBe("s3cret");
    }
  });

  test("authenticates every socket in the pool", async () => {
    let hellos = 0;
    await connect(
      (_request, _conn) => {},
      { bulkConnections: 3 },
      (request, conn) => {
        hellos += 1;
        defaultHello(request, conn);
      },
    );
    // Client.connect resolves only once every socket has been acknowledged: 1 control + 3 bulk.
    expect(hellos).toBe(4);
  });

  test("a rejected token fails the connect with an Unauthenticated ServerError", async () => {
    const server = await startServer(
      (_request, _conn) => {},
      (request, conn) => {
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "error",
            error: { code: 8, message: "invalid or missing auth token", retryable: false },
          },
        });
      },
    );
    let caught: unknown;
    try {
      await Client.connect(`127.0.0.1:${server.port}`, { authToken: "wrong", bulkConnections: 0 });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ServerError);
    expect((caught as ServerError).code).toBe(ErrorCode.Unauthenticated);
  });

  test("a HelloAck with a mismatched protocol version fails with a ProtocolError", async () => {
    const server = await startServer(
      (_request, _conn) => {},
      (request, conn) => {
        conn.send({
          requestId: request.requestId,
          kind: {
            kind: "helloAck",
            helloAck: { protocolVersion: PROTOCOL_VERSION + 1, serverVersion: "test" },
          },
        });
      },
    );
    let caught: unknown;
    try {
      await Client.connect(`127.0.0.1:${server.port}`, { bulkConnections: 0 });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(ProtocolError);
  });
});
