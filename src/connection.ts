// One multiplexed connection over a single socket. A reader loop demultiplexes response frames by
// request id, and a writer loop coalesces queued frames into one flush per burst. Many requests can
// be in flight at once and complete out of order; the per-socket in-flight budget provides
// backpressure. This mirrors the Go client's conn (tephra-go/client.go) and the Rust AsyncClient.

import { sequencedFromWire, serverErrorFromWire } from "./convert.js";
import { ConnError, ProtocolError } from "./errors.js";
import { FrameDecoder, writeFrame } from "./framing.js";
import type { ResolvedConfig } from "./options.js";
import {
  PROTOCOL_VERSION,
  type RequestKind,
  type WireAppendCondition,
  type WireAppendResponse,
  type WireEvent,
  type WireQuery,
  type WireStatsResponse,
  decodeResponse,
  encodeRequest,
} from "./proto/messages.js";
import { ReadStream, SubscribeStream } from "./streams.js";
import { dialSocket } from "./transport.js";
import type { SequencedEvent, SubEvent } from "./types.js";
import { Deferred, Gate, Semaphore, asError } from "./util.js";

import type { Duplex } from "node:stream";

type Pending =
  | {
      type: "unary";
      expect: "append" | "stats";
      deferred: Deferred<WireAppendResponse | WireStatsResponse>;
      permit: boolean;
    }
  | { type: "hello"; deferred: Deferred<void>; permit: boolean }
  | { type: "read"; stream: ReadStream; permit: boolean }
  | { type: "subscribe"; stream: SubscribeStream; permit: boolean };

/** A single multiplexed connection. Created by `Conn.connect`, owned by a `Client`. */
export class Conn {
  private readonly pending = new Map<bigint, Pending>();
  private readonly inflight: Semaphore;
  private readonly queueSlots: Semaphore;
  private readonly decoder: FrameDecoder;
  private readonly writerGate = new Gate();
  private readonly outbound: Uint8Array[] = [];
  private readonly priority: Uint8Array[] = [];
  private idCounter = 0n;
  private dead = false;
  private deathCause: Error | null = null;

  private constructor(
    private readonly socket: Duplex,
    private readonly config: ResolvedConfig,
  ) {
    this.inflight = new Semaphore(config.maxInflightRequests);
    this.queueSlots = new Semaphore(config.requestQueueDepth);
    this.decoder = new FrameDecoder(config.maxFrameLen);

    socket.on("data", (chunk: Uint8Array) => this.onData(chunk));
    socket.on("error", (err: Error) => this.failConnection(new ConnError("connection error", err)));
    socket.on("close", () => this.failConnection(new ConnError("connection closed")));

    void this.runWriter();
  }

  static async connect(
    host: string,
    port: number,
    config: ResolvedConfig,
    signal?: AbortSignal,
  ): Promise<Conn> {
    const socket = await dialSocket(host, port, config, signal);
    const conn = new Conn(socket, config);
    // The mandatory opening Hello: negotiate the protocol version and authenticate before the
    // connection carries any request. A version mismatch or a rejected token fails the connect.
    try {
      await conn.hello(config.authToken);
    } catch (err) {
      await conn.close();
      throw err;
    }
    return conn;
  }

  // -------------------------------------------------------------------------
  // Operations
  // -------------------------------------------------------------------------

  append(
    events: WireEvent[],
    condition: WireAppendCondition | undefined,
    signal?: AbortSignal,
  ): Promise<WireAppendResponse> {
    return this.unary(
      { kind: "append", append: { events, condition } },
      "append",
      signal,
    ) as Promise<WireAppendResponse>;
  }

  stats(signal?: AbortSignal): Promise<WireStatsResponse> {
    return this.unary({ kind: "stats" }, "stats", signal) as Promise<WireStatsResponse>;
  }

  read(
    query: WireQuery,
    after: bigint,
    limit: bigint | undefined,
    reverse: boolean,
    signal?: AbortSignal,
  ): ReadStream {
    const id = this.nextId();
    const stream = new ReadStream(() => this.cancelStream(id), signal);
    this.pending.set(id, { type: "read", stream, permit: false });
    void this.sendStreamRequest(id, { kind: "read", read: { query, after, limit, reverse } });
    return stream;
  }

  subscribe(query: WireQuery, after: bigint, signal?: AbortSignal): SubscribeStream {
    const id = this.nextId();
    const stream = new SubscribeStream(() => this.cancelStream(id), signal);
    this.pending.set(id, { type: "subscribe", stream, permit: false });
    void this.sendStreamRequest(id, { kind: "subscribe", subscribe: { query, after } });
    return stream;
  }

  async close(): Promise<void> {
    this.failConnection(new ConnError("connection closed"));
    this.socket.destroy();
  }

  // -------------------------------------------------------------------------
  // Sending
  // -------------------------------------------------------------------------

  private unary(
    kind: RequestKind,
    expect: "append" | "stats",
    signal?: AbortSignal,
  ): Promise<WireAppendResponse | WireStatsResponse> {
    if (this.dead) {
      return Promise.reject(this.deadError());
    }
    const id = this.nextId();
    const deferred = new Deferred<WireAppendResponse | WireStatsResponse>();
    this.pending.set(id, { type: "unary", expect, deferred, permit: false });

    const onAbort = signal
      ? () => this.cancelUnary(id, asError(signal.reason, "the operation was aborted"))
      : undefined;
    if (signal && onAbort) {
      if (signal.aborted) {
        onAbort();
      } else {
        signal.addEventListener("abort", onAbort, { once: true });
      }
    }

    void this.sendUnaryRequest(id, kind);

    if (signal && onAbort) {
      void deferred.promise
        .finally(() => signal.removeEventListener("abort", onAbort))
        .catch(() => {});
    }
    return deferred.promise;
  }

  private async sendUnaryRequest(id: bigint, kind: RequestKind): Promise<void> {
    let heldPermit = false;
    try {
      await this.inflight.acquire();
      heldPermit = true;
      const entry = this.pending.get(id);
      if (!entry) {
        this.inflight.release();
        return;
      }
      if (this.dead) {
        throw this.deadError();
      }
      entry.permit = true;
      heldPermit = false;
      await this.enqueue(
        writeFrame(encodeRequest({ requestId: id, kind }), this.config.maxFrameLen),
      );
    } catch (err) {
      if (heldPermit) {
        this.inflight.release();
      }
      const entry = this.finalize(id);
      if (entry?.type === "unary") {
        entry.deferred.reject(asError(err, "failed to send request"));
      }
    }
  }

  private async sendStreamRequest(id: bigint, kind: RequestKind): Promise<void> {
    let heldPermit = false;
    try {
      await this.inflight.acquire();
      heldPermit = true;
      const entry = this.pending.get(id);
      if (!entry) {
        // Cancelled before the request went out.
        this.inflight.release();
        return;
      }
      if (this.dead) {
        throw this.deadError();
      }
      entry.permit = true;
      heldPermit = false;
      await this.enqueue(
        writeFrame(encodeRequest({ requestId: id, kind }), this.config.maxFrameLen),
      );
    } catch (err) {
      if (heldPermit) {
        this.inflight.release();
      }
      const entry = this.finalize(id);
      if (entry && (entry.type === "read" || entry.type === "subscribe")) {
        entry.stream.failWith(asError(err, "failed to send request"));
      }
    }
  }

  /**
   * Sends the mandatory opening Hello and awaits the server's HelloAck, so a version mismatch or a
   * rejected token fails the connect before any real request. Runs once, at connect, before the
   * connection is handed to the Client, so the Hello is the first frame on the wire.
   */
  private hello(token: string | undefined): Promise<void> {
    const id = this.nextId();
    const deferred = new Deferred<void>();
    this.pending.set(id, { type: "hello", deferred, permit: false });
    void this.sendHelloRequest(id, token);
    return deferred.promise;
  }

  private async sendHelloRequest(id: bigint, token: string | undefined): Promise<void> {
    let heldPermit = false;
    try {
      await this.inflight.acquire();
      heldPermit = true;
      const entry = this.pending.get(id);
      if (!entry) {
        this.inflight.release();
        return;
      }
      if (this.dead) {
        throw this.deadError();
      }
      entry.permit = true;
      heldPermit = false;
      const hello = { protocolVersion: PROTOCOL_VERSION, authToken: token };
      await this.enqueue(
        writeFrame(
          encodeRequest({ requestId: id, kind: { kind: "hello", hello } }),
          this.config.maxFrameLen,
        ),
      );
    } catch (err) {
      if (heldPermit) {
        this.inflight.release();
      }
      const entry = this.finalize(id);
      if (entry?.type === "hello") {
        entry.deferred.reject(asError(err, "failed to send hello"));
      }
    }
  }

  /** Queues a request frame, waiting for outbound room (the queue-depth backpressure path). */
  private async enqueue(frame: Uint8Array): Promise<void> {
    await this.queueSlots.acquire();
    if (this.dead) {
      this.queueSlots.release();
      throw this.deadError();
    }
    this.outbound.push(frame);
    this.writerGate.signal();
  }

  /** Sends a best-effort cancel for a request id. It jumps the queue and consumes no slot. */
  private sendCancel(target: bigint): void {
    if (this.dead) {
      return;
    }
    const frame = writeFrame(
      encodeRequest({ requestId: this.nextId(), kind: { kind: "cancel", cancel: { target } } }),
      this.config.maxFrameLen,
    );
    this.priority.push(frame);
    this.writerGate.signal();
  }

  // -------------------------------------------------------------------------
  // Writer loop
  // -------------------------------------------------------------------------

  private async runWriter(): Promise<void> {
    try {
      while (!this.dead) {
        if (this.priority.length === 0 && this.outbound.length === 0) {
          await this.writerGate.wait();
          continue;
        }
        const batch: Uint8Array[] = [];
        let released = 0;
        while (this.priority.length > 0) {
          batch.push(this.priority.shift() as Uint8Array);
        }
        while (this.outbound.length > 0) {
          batch.push(this.outbound.shift() as Uint8Array);
          released += 1;
        }
        const ok = this.socket.write(concatFrames(batch));
        for (let i = 0; i < released; i++) {
          this.queueSlots.release();
        }
        if (!ok) {
          await this.waitDrain();
        }
      }
    } catch (err) {
      this.failConnection(new ConnError("write failed", asError(err, "write failed")));
    }
  }

  private waitDrain(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const onDrain = (): void => {
        cleanup();
        resolve();
      };
      const onError = (): void => {
        cleanup();
        reject(this.deadError());
      };
      const cleanup = (): void => {
        this.socket.removeListener("drain", onDrain);
        this.socket.removeListener("close", onError);
        this.socket.removeListener("error", onError);
      };
      this.socket.once("drain", onDrain);
      this.socket.once("close", onError);
      this.socket.once("error", onError);
    });
  }

  // -------------------------------------------------------------------------
  // Reader loop
  // -------------------------------------------------------------------------

  private onData(chunk: Uint8Array): void {
    let frames: Uint8Array[];
    try {
      frames = this.decoder.push(chunk);
    } catch (err) {
      this.failConnection(new ConnError("framing error", asError(err, "framing error")));
      return;
    }
    for (const body of frames) {
      try {
        this.route(body);
      } catch (err) {
        this.failConnection(new ConnError("protocol error", asError(err, "protocol error")));
        return;
      }
    }
  }

  private route(body: Uint8Array): void {
    const response = decodeResponse(body);
    const id = response.requestId;
    const kind = response.kind;

    if (id === 0n) {
      // A frame the server could not attribute to a request (a decode or framing error on its side).
      if (kind.kind === "error") {
        this.failConnection(
          new ConnError("unattributed server error", serverErrorFromWire(kind.error)),
        );
      }
      return;
    }

    const entry = this.pending.get(id);
    if (!entry) {
      // A late frame for a request that was already cancelled or completed. Ignore it.
      return;
    }

    if (entry.type === "hello") {
      this.finalize(id);
      if (kind.kind === "helloAck") {
        const server = kind.helloAck.protocolVersion;
        if (server !== PROTOCOL_VERSION) {
          // The server only sends a HelloAck on its success path, where the version matches, so this
          // is defensive against a non-conforming server rather than real negotiation.
          entry.deferred.reject(
            new ProtocolError(
              `server protocol version ${server} does not match client ${PROTOCOL_VERSION}`,
            ),
          );
        } else {
          entry.deferred.resolve();
        }
      } else if (kind.kind === "error") {
        entry.deferred.reject(serverErrorFromWire(kind.error));
      } else {
        entry.deferred.reject(new ProtocolError(`unexpected ${kind.kind} response to hello`));
      }
      return;
    }

    if (entry.type === "unary") {
      this.finalize(id);
      if (kind.kind === "error") {
        entry.deferred.reject(serverErrorFromWire(kind.error));
      } else if (entry.expect === "append" && kind.kind === "append") {
        entry.deferred.resolve(kind.append);
      } else if (entry.expect === "stats" && kind.kind === "stats") {
        entry.deferred.resolve(kind.stats);
      } else {
        entry.deferred.reject(
          new ProtocolError(`unexpected ${kind.kind} response for a ${entry.expect} request`),
        );
      }
      return;
    }

    if (entry.type === "read") {
      this.routeRead(id, entry.stream, kind);
      return;
    }

    this.routeSubscribe(id, entry.stream, kind);
  }

  private routeRead(id: bigint, stream: ReadStream, kind: ResponseKindOf): void {
    switch (kind.kind) {
      case "readEvents": {
        let mapped: SequencedEvent[];
        try {
          mapped = kind.readEvents.events.map(sequencedFromWire);
        } catch (err) {
          this.abortStream(id, () => stream.failWith(asError(err, "protocol error")));
          return;
        }
        for (const event of mapped) {
          stream.deliverItem(event);
        }
        return;
      }
      case "readEnd":
        this.finalize(id);
        stream.endWith(kind.readEnd.watermark);
        return;
      case "error":
        this.finalize(id);
        stream.failWith(serverErrorFromWire(kind.error));
        return;
      default:
        this.abortStream(id, () =>
          stream.failWith(new ProtocolError(`unexpected ${kind.kind} response for a read`)),
        );
    }
  }

  private routeSubscribe(id: bigint, stream: SubscribeStream, kind: ResponseKindOf): void {
    switch (kind.kind) {
      case "readEvents": {
        let mapped: SubEvent[];
        try {
          mapped = kind.readEvents.events.map((wireEvent) => ({
            kind: "event" as const,
            event: sequencedFromWire(wireEvent),
          }));
        } catch (err) {
          this.abortStream(id, () => stream.failWith(asError(err, "protocol error")));
          return;
        }
        for (const item of mapped) {
          stream.deliverItem(item);
        }
        return;
      }
      case "caughtUp":
        stream.deliverItem({ kind: "caughtUp", watermark: kind.caughtUp.watermark });
        return;
      case "error":
        this.finalize(id);
        stream.failWith(serverErrorFromWire(kind.error));
        return;
      default:
        this.abortStream(id, () =>
          stream.failWith(new ProtocolError(`unexpected ${kind.kind} response for a subscription`)),
        );
    }
  }

  /** Cancels a still-open request server-side then fails its stream locally (a protocol violation). */
  private abortStream(id: bigint, failStream: () => void): void {
    if (this.pending.has(id)) {
      this.sendCancel(id);
      this.finalize(id);
    }
    failStream();
  }

  // -------------------------------------------------------------------------
  // Bookkeeping
  // -------------------------------------------------------------------------

  private nextId(): bigint {
    this.idCounter += 1n;
    return this.idCounter;
  }

  /** Removes a request and releases its in-flight permit if it held one. */
  private finalize(id: bigint): Pending | undefined {
    const entry = this.pending.get(id);
    if (!entry) {
      return undefined;
    }
    this.pending.delete(id);
    if (entry.permit) {
      this.inflight.release();
    }
    return entry;
  }

  private cancelStream(id: bigint): void {
    if (!this.pending.has(id)) {
      return;
    }
    this.sendCancel(id);
    this.finalize(id);
  }

  private cancelUnary(id: bigint, err: Error): void {
    const entry = this.pending.get(id);
    if (!entry) {
      return;
    }
    this.sendCancel(id);
    this.finalize(id);
    if (entry.type === "unary") {
      entry.deferred.reject(err);
    }
  }

  private deadError(): Error {
    return this.deathCause ?? new ConnError("connection closed");
  }

  private failConnection(err: Error): void {
    if (this.dead) {
      return;
    }
    this.dead = true;
    this.deathCause = err;

    const pending = [...this.pending.values()];
    this.pending.clear();
    for (const entry of pending) {
      if (entry.type === "unary" || entry.type === "hello") {
        entry.deferred.reject(err);
      } else {
        entry.stream.failWith(err);
      }
    }

    // Wake the writer and anything waiting for outbound room so they observe the death.
    this.writerGate.signal();
    this.queueSlots.drainWaiters();
  }
}

// The response oneof, re-exported locally for the routing switches.
type ResponseKindOf = ReturnType<typeof decodeResponse>["kind"];

function concatFrames(frames: Uint8Array[]): Uint8Array {
  if (frames.length === 1) {
    return frames[0] as Uint8Array;
  }
  let total = 0;
  for (const frame of frames) {
    total += frame.length;
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const frame of frames) {
    out.set(frame, offset);
    offset += frame.length;
  }
  return out;
}
