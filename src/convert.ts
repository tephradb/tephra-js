// Conversions between the friendly public types and the wire structs. Keeping these in one place
// lets the connection layer speak wire structs while callers only ever see the public types.

import { ProtocolError, ServerError, errorCodeFromWire } from "./errors.js";
import type {
  WireAppendCondition,
  WireAppendResponse,
  WireErrorResponse,
  WireEvent,
  WireQuery,
  WireSequencedEvent,
  WireStatsResponse,
} from "./proto/messages.js";
import {
  type AppendCondition,
  type AppendResult,
  Event,
  type Query,
  type SequencedEvent,
  type Stats,
} from "./types.js";

export function eventToWire(event: Event): WireEvent {
  return { type: event.type, tags: event.tags.slice(), payload: event.payload };
}

export function queryToWire(query: Query): WireQuery {
  if (query.matchAll) {
    return { all: true, items: [] };
  }
  return {
    all: false,
    items: query.items.map((item) => ({ types: item.types.slice(), tags: item.tags.slice() })),
  };
}

export function conditionToWire(condition: AppendCondition): WireAppendCondition {
  return {
    failIfEventsMatch: queryToWire(condition.failIfEventsMatch),
    after: condition.after,
    failIfExists:
      condition.failIfExists !== undefined ? queryToWire(condition.failIfExists) : undefined,
  };
}

export function appendResultFromWire(res: WireAppendResponse): AppendResult {
  return { first: res.first, last: res.last };
}

/**
 * Builds a SequencedEvent from a wire message. The server is the source of truth for a stored
 * event (it validated the event on append and stores tags in canonical sorted order), so the fields
 * are taken verbatim rather than re-validated. A frame carrying no event is the one thing the server
 * never sends, so it is treated as a protocol error.
 */
export function sequencedFromWire(seq: WireSequencedEvent): SequencedEvent {
  if (seq.event === undefined) {
    throw new ProtocolError("server sent a sequenced event with no event");
  }
  return {
    position: seq.position,
    event: Event.fromServer(seq.event.type, seq.event.tags, seq.event.payload),
  };
}

export function statsFromWire(res: WireStatsResponse): Stats {
  return {
    eventCount: res.eventCount,
    segmentCount: res.segmentCount,
    diskBytes: res.diskBytes,
    uptimeSeconds: res.uptimeSeconds,
    activeConnections: res.activeConnections,
    activeSubscriptions: res.activeSubscriptions,
    connectionsRefused: res.connectionsRefused,
    connectionsReaped: res.connectionsReaped,
    maxConnections: res.maxConnections,
    version: res.version,
  };
}

export function serverErrorFromWire(res: WireErrorResponse): ServerError {
  return new ServerError({
    code: errorCodeFromWire(res.code),
    message: res.message,
    retryable: res.retryable,
    conflictPosition: res.conflictPosition,
  });
}
