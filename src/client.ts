// The public client. One Client multiplexes many requests over a control socket (appends, stats)
// plus a pool of bulk sockets (reads, subscriptions), correlating each response to its request.
// Splitting the lanes keeps a large read response from delaying a small append, and the bulk pool
// keeps concurrent reads from serializing on one connection. This mirrors the Go Client and the
// reference Rust AsyncClient. The client performs no automatic retries or reconnection: on a
// durable failure it surfaces the error, leaving policy to the caller.

import { Conn } from "./connection.js";
import {
  appendResultFromWire,
  conditionToWire,
  eventToWire,
  queryToWire,
  statsFromWire,
} from "./convert.js";
import { ClosedError } from "./errors.js";
import { type CallOptions, type ClientOptions, resolveConfig } from "./options.js";
import type { ReadStream, SubscribeStream } from "./streams.js";
import {
  type AppendCondition,
  type AppendResult,
  type Event,
  MAX,
  type Position,
  type Query,
  type SequencedEvent,
  type Stats,
  ZERO,
} from "./types.js";

const DEFAULT_PORT = 9000;

/** Parses a "host:port" address, defaulting the port to 9000. IPv6 hosts may be bracketed. */
function parseAddress(addr: string): { host: string; port: number } {
  const trimmed = addr.trim();
  if (trimmed.startsWith("[")) {
    const end = trimmed.indexOf("]");
    if (end === -1) {
      throw new TypeError(`invalid address ${JSON.stringify(addr)}`);
    }
    const host = trimmed.slice(1, end);
    const rest = trimmed.slice(end + 1);
    const port = rest.startsWith(":") ? Number(rest.slice(1)) : DEFAULT_PORT;
    return { host, port };
  }
  const colon = trimmed.lastIndexOf(":");
  if (colon === -1) {
    return { host: trimmed, port: DEFAULT_PORT };
  }
  const host = trimmed.slice(0, colon);
  const port = Number(trimmed.slice(colon + 1));
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new TypeError(`invalid port in address ${JSON.stringify(addr)}`);
  }
  return { host: host || "127.0.0.1", port };
}

function toBigIntLimit(limit: number | bigint | undefined): bigint | undefined {
  if (limit === undefined) {
    return undefined;
  }
  return typeof limit === "bigint" ? limit : BigInt(limit);
}

/**
 * A connected tephra client, safe to use concurrently. Obtain one with `Client.connect` and release
 * it with `close`.
 */
export class Client {
  private nextBulk = 0;
  private closed = false;

  private constructor(
    private readonly control: Conn,
    private readonly bulk: Conn[],
  ) {}

  /**
   * Connects to a tephra server at `addr` ("host:port", port defaulting to 9000). Opens a control
   * socket plus `bulkConnections` bulk sockets concurrently; if any fails, the rest are closed and
   * the connect rejects.
   */
  static async connect(addr: string, options: ClientOptions = {}): Promise<Client> {
    const { host, port } = parseAddress(addr);
    const config = resolveConfig(options);

    const dials: Promise<Conn>[] = [Conn.connect(host, port, config, options.signal)];
    for (let i = 0; i < config.bulkConnections; i++) {
      dials.push(Conn.connect(host, port, config, options.signal));
    }

    const settled = await Promise.allSettled(dials);
    const conns: Conn[] = [];
    let failure: unknown;
    for (const result of settled) {
      if (result.status === "fulfilled") {
        conns.push(result.value);
      } else if (failure === undefined) {
        failure = result.reason;
      }
    }
    if (failure !== undefined) {
      await Promise.all(conns.map((conn) => conn.close()));
      throw failure;
    }

    const [control, ...bulk] = conns as [Conn, ...Conn[]];
    return new Client(control, bulk);
  }

  /** Round-robins over the bulk pool, falling back to the control socket when the pool is empty. */
  private pickBulk(): Conn {
    if (this.bulk.length === 0) {
      return this.control;
    }
    const conn = this.bulk[this.nextBulk % this.bulk.length] as Conn;
    this.nextBulk = (this.nextBulk + 1) % this.bulk.length;
    return conn;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new ClosedError();
    }
  }

  /**
   * Appends events as one atomic batch, optionally guarded by `condition`, and resolves with the
   * position range the batch was assigned. Many appends may run concurrently.
   */
  async append(
    events: readonly Event[],
    condition?: AppendCondition | null,
    opts: CallOptions = {},
  ): Promise<AppendResult> {
    this.ensureOpen();
    const wire = await this.control.append(
      events.map(eventToWire),
      condition ? conditionToWire(condition) : undefined,
      opts.signal,
    );
    return appendResultFromWire(wire);
  }

  /** Fetches a snapshot of the server's operational state. */
  async stats(opts: CallOptions = {}): Promise<Stats> {
    this.ensureOpen();
    return statsFromWire(await this.control.stats(opts.signal));
  }

  /**
   * Starts a forward read (ascending position order). `after` is an exclusive lower bound (`ZERO`
   * reads from the start); `limit` caps the events returned (omitted means unlimited). The returned
   * stream is an async iterable.
   */
  read(
    query: Query,
    after: Position = ZERO,
    limit?: number | bigint,
    opts: CallOptions = {},
  ): ReadStream {
    this.ensureOpen();
    return this.pickBulk().read(
      queryToWire(query),
      after,
      toBigIntLimit(limit),
      false,
      opts.signal,
    );
  }

  /**
   * The newest-first dual of `read` (descending position order). `before` is an exclusive upper
   * bound; `readBack(query, MAX, limit)` streams from the current durable tip.
   */
  readBack(
    query: Query,
    before: Position = MAX,
    limit?: number | bigint,
    opts: CallOptions = {},
  ): ReadStream {
    this.ensureOpen();
    return this.pickBulk().read(
      queryToWire(query),
      before,
      toBigIntLimit(limit),
      true,
      opts.signal,
    );
  }

  /** Drains a forward read fully, returning the events and the watermark the read was pinned to. */
  async readAll(
    query: Query,
    after: Position = ZERO,
    limit?: number | bigint,
    opts: CallOptions = {},
  ): Promise<{ events: SequencedEvent[]; watermark: Position }> {
    return this.read(query, after, limit, opts).collect();
  }

  /** The newest-first dual of `readAll`. */
  async readAllBack(
    query: Query,
    before: Position = MAX,
    limit?: number | bigint,
    opts: CallOptions = {},
  ): Promise<{ events: SequencedEvent[]; watermark: Position }> {
    return this.readBack(query, before, limit, opts).collect();
  }

  /**
   * Opens a live subscription: catch up on matching events after `after`, then tail new ones as they
   * are committed, delivering a caught-up marker each time the stream reaches the live edge. The
   * returned stream runs until it is closed, its signal aborts, it errors, or the connection closes.
   */
  subscribe(query: Query, after: Position = ZERO, opts: CallOptions = {}): SubscribeStream {
    this.ensureOpen();
    return this.pickBulk().subscribe(queryToWire(query), after, opts.signal);
  }

  /** Closes every connection, failing any in-flight request. Idempotent. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await Promise.all([this.control.close(), ...this.bulk.map((conn) => conn.close())]);
  }
}
