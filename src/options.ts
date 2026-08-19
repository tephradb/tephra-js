// Connection options and their defaults. The defaults mirror the reference Rust and Go clients:
// 16 MiB frames, 1024 in-flight requests per socket, a 256-deep outbound queue, and a pool of 4
// bulk sockets that carry reads and subscriptions separately from the control socket.

import type { ConnectionOptions } from "node:tls";

/** The largest single frame accepted or produced. Must match or exceed the server's limit. */
export const DEFAULT_MAX_FRAME_LEN = 16 * 1024 * 1024;

/** Dedicated bulk sockets for streaming reads and subscriptions, separate from the control socket. */
export const DEFAULT_BULK_CONNECTIONS = 4;

/** Outstanding requests per socket before an operation waits for a free slot (backpressure). */
export const DEFAULT_MAX_INFLIGHT_REQUESTS = 1024;

/** Outbound queue depth per socket; once full, an operation waits for room to send. */
export const DEFAULT_REQUEST_QUEUE_DEPTH = 256;

/**
 * TLS settings passed through to Node's `tls.connect`. Anything the standard connection options
 * allow works here (`ca` for a private CA, `cert`/`key` for mutual TLS, `servername`, `minVersion`,
 * `rejectUnauthorized`), except the fields the client controls itself (host, port, socket).
 */
export type TlsOptions = Omit<ConnectionOptions, "host" | "port" | "path" | "socket" | "timeout">;

/** Options for `Client.connect`. */
export interface ClientOptions {
  /**
   * Dedicated bulk sockets for reads and subscriptions (default 4). Set to 0 to fold reads onto the
   * control socket (which reintroduces head-of-line blocking behind a large read).
   */
  bulkConnections?: number;
  /** Outstanding requests per socket before backpressure (default 1024). */
  maxInflightRequests?: number;
  /** Outbound queue depth per socket (default 256). */
  requestQueueDepth?: number;
  /** Largest frame accepted or produced (default 16 MiB). */
  maxFrameLen?: number;
  /**
   * Enable TLS. `true` uses the system roots with `servername` defaulting to the dial host; an
   * object passes through to `tls.connect` for a private CA, mutual TLS, or a custom `minVersion`.
   */
  tls?: boolean | TlsOptions;
  /** Bounds the dial (in milliseconds). Applies to the connect only, not later operations. */
  connectTimeout?: number;
  /** Aborts the connect. */
  signal?: AbortSignal;
}

/** Options accepted by an individual operation. */
export interface CallOptions {
  /** Aborts the operation. For a stream, aborting cancels it server-side and ends iteration. */
  signal?: AbortSignal;
}

/** The resolved configuration, with every default applied. */
export interface ResolvedConfig {
  bulkConnections: number;
  maxInflightRequests: number;
  requestQueueDepth: number;
  maxFrameLen: number;
  tls?: TlsOptions;
  connectTimeout?: number;
}

function positiveOr(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value) || value < 1) {
    return fallback;
  }
  return Math.floor(value);
}

/** Applies defaults and clamps invalid values, mirroring the Go client's `normalize`. */
export function resolveConfig(options: ClientOptions = {}): ResolvedConfig {
  const bulk = options.bulkConnections;
  return {
    bulkConnections:
      bulk === undefined || !Number.isFinite(bulk) || bulk < 0
        ? DEFAULT_BULK_CONNECTIONS
        : Math.floor(bulk),
    maxInflightRequests: positiveOr(options.maxInflightRequests, DEFAULT_MAX_INFLIGHT_REQUESTS),
    requestQueueDepth: positiveOr(options.requestQueueDepth, DEFAULT_REQUEST_QUEUE_DEPTH),
    maxFrameLen: positiveOr(options.maxFrameLen, DEFAULT_MAX_FRAME_LEN),
    tls:
      options.tls === undefined || options.tls === false
        ? undefined
        : options.tls === true
          ? {}
          : options.tls,
    connectTimeout: options.connectTimeout,
  };
}
