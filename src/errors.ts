// The error hierarchy the client surfaces. The client performs no automatic retries or
// reconnection: on a durable failure it throws, leaving policy to the caller. A ServerError carries
// the server's code and a Retryable hint; the rest signal transport, protocol, or lifecycle faults.

import type { Position } from "./types.js";

/**
 * A protocol error code from the server, decoupled from the wire enum so callers do not depend on
 * the generated numbering. An unrecognized or unspecified wire code maps to Unknown.
 */
export const ErrorCode = {
  /** The unspecified code, or any code this build does not recognize. */
  Unknown: "unknown",
  /** An append condition failed. See ServerError.conflictPosition. */
  Conflict: "conflict",
  /** A read cursor was past the server's durable tip. */
  AfterBeyondTip: "after_beyond_tip",
  /** An append carried zero events. */
  Empty: "empty",
  /** A frame or payload exceeded the server's limit. */
  TooLarge: "too_large",
  /** The request was malformed (for example an invalid name). */
  BadRequest: "bad_request",
  /** The server hit an internal failure. */
  Internal: "internal",
  /** The server is shutting down. */
  Shutdown: "shutdown",
  /** The connection failed authentication: a missing or invalid token, or a non-Hello first frame. */
  Unauthenticated: "unauthenticated",
} as const;

export type ErrorCode = (typeof ErrorCode)[keyof typeof ErrorCode];

// Wire ErrorCode enum values (proto/tephra/v1/tephra.proto).
const WIRE_ERROR_CODES: Record<number, ErrorCode> = {
  0: ErrorCode.Unknown,
  1: ErrorCode.Conflict,
  2: ErrorCode.AfterBeyondTip,
  3: ErrorCode.Empty,
  4: ErrorCode.TooLarge,
  5: ErrorCode.BadRequest,
  6: ErrorCode.Internal,
  7: ErrorCode.Shutdown,
  8: ErrorCode.Unauthenticated,
};

/** Maps a wire error code to the public ErrorCode. */
export function errorCodeFromWire(code: number): ErrorCode {
  return WIRE_ERROR_CODES[code] ?? ErrorCode.Unknown;
}

/** The base class for every error this client throws, so callers can catch the whole family. */
export class TephraError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** An input failed validation before it ever reached the wire (an invalid event type or tag). */
export class ValidationError extends TephraError {}

/**
 * The server returned an error response. `retryable` marks an advisory same-batch append conflict
 * (safe to retry), distinct from a durable one (terminal). `conflictPosition` is set only for a
 * durable append conflict.
 */
export class ServerError extends TephraError {
  readonly code: ErrorCode;
  readonly retryable: boolean;
  readonly conflictPosition?: Position;

  constructor(args: {
    code: ErrorCode;
    message: string;
    retryable: boolean;
    conflictPosition?: Position;
  }) {
    super(`server error (${args.code}, retryable=${args.retryable}): ${args.message}`);
    this.code = args.code;
    this.retryable = args.retryable;
    this.conflictPosition = args.conflictPosition;
  }
}

/**
 * The peer sent something that does not fit the protocol: the wrong frame kind for a request, a
 * response for an unexpected request id, or an event that fails to decode.
 */
export class ProtocolError extends TephraError {}

/**
 * A frame's length exceeded the configured maximum. On read it is reported before the body is
 * allocated; on write, before any byte reaches the wire.
 */
export class FrameTooLargeError extends TephraError {
  readonly length: number;
  readonly max: number;

  constructor(length: number, max: number) {
    super(`frame length ${length} exceeds the maximum of ${max}`);
    this.length = length;
    this.max = max;
  }
}

/**
 * A connection failed with requests in flight. Every request outstanding on the socket is failed
 * with this error so no caller hangs. The underlying cause (an I/O error, or an unattributed server
 * error captured from the wire) is available on `cause`.
 */
export class ConnError extends TephraError {
  override readonly cause?: Error;

  constructor(reason: string, cause?: Error) {
    super(cause ? `${reason}: ${cause.message}` : reason);
    this.cause = cause;
  }
}

/** An operation was attempted on a client (or one of its streams) that has already been closed. */
export class ClosedError extends TephraError {
  constructor() {
    super("client is closed");
  }
}
