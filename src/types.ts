// The friendly public value types. The wire protobuf types are an implementation detail hidden
// behind these (see src/proto). An Event validates its type and tags the same way the server does,
// so a bad name is caught locally rather than round-tripping to a server error.

import { ValidationError } from "./errors.js";

/**
 * A 1-based global position in the log, held as a bigint so it spans the whole u64 range. Position
 * `ZERO` is "before everything".
 */
export type Position = bigint;

/**
 * `ZERO` is the position before the first event: the start cursor for a forward read, and the
 * "consider the whole log" bound for an append condition.
 */
export const ZERO: Position = 0n;

/**
 * `MAX` is the largest representable position, the "from the tip" cursor for a backward read:
 * `readBack(query, MAX, limit)` streams newest-first from the current durable tip.
 */
export const MAX: Position = 0xffff_ffff_ffff_ffffn;

/**
 * The maximum length, in bytes, of an event type or tag. The engine stores each with a fixed-width
 * uint16 length, so the field capacity (u16::MAX) is the limit.
 */
export const MAX_NAME_LEN = 65535;

const EMPTY_PAYLOAD = new Uint8Array(0);

/** Counts the UTF-8 byte length of a string without allocating, for the name-length check. */
function utf8ByteLength(value: string): number {
  let bytes = 0;
  for (let i = 0; i < value.length; i++) {
    const code = value.charCodeAt(i);
    if (code < 0x80) {
      bytes += 1;
    } else if (code < 0x800) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      bytes += 4;
      i++;
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

function validateName(value: string, what: string): void {
  if (value.length === 0) {
    throw new ValidationError(`${what} must not be empty`);
  }
  const len = utf8ByteLength(value);
  if (len > MAX_NAME_LEN) {
    throw new ValidationError(
      `${what} is ${len} bytes, exceeding the ${MAX_NAME_LEN}-byte maximum`,
    );
  }
}

/**
 * Validates each tag, then returns a sorted, duplicate-free copy. A duplicate is an error rather
 * than being silently deduped, so an event never round-trips to something the caller did not submit.
 */
function validatedTagSet(tags: readonly string[]): string[] {
  const out = tags.slice();
  for (const tag of out) {
    validateName(tag, "tag");
  }
  out.sort();
  for (let i = 1; i < out.length; i++) {
    if (out[i] === out[i - 1]) {
      throw new ValidationError(`duplicate tag ${JSON.stringify(out[i])}`);
    }
  }
  return out;
}

/**
 * Validates each type name, preserving order (types are OR'd and low cardinality, so they are
 * neither sorted nor deduped).
 */
function validatedTypes(types: readonly string[]): string[] {
  const out = types.slice();
  for (const type of out) {
    validateName(type, "event type");
  }
  return out;
}

/**
 * An event to append: a non-empty type, a set of tags, and an opaque payload. Build one with
 * `Event.create`, which validates the type and tags exactly as the server does. Tags are stored
 * sorted so identical sets encode identically.
 */
export class Event {
  readonly type: string;
  readonly tags: readonly string[];
  readonly payload: Uint8Array;

  private constructor(type: string, tags: readonly string[], payload: Uint8Array) {
    this.type = type;
    this.tags = tags;
    this.payload = payload;
  }

  /**
   * Builds an event, validating the type and tags (each non-empty and at most `MAX_NAME_LEN` bytes)
   * and rejecting a duplicate tag. Throws a ValidationError on bad input.
   */
  static create(
    type: string,
    tags: readonly string[] = [],
    payload: Uint8Array = EMPTY_PAYLOAD,
  ): Event {
    validateName(type, "event type");
    return new Event(type, validatedTagSet(tags), payload);
  }

  /** Builds an Event from server-supplied fields, trusting the server's own validation. */
  static fromServer(type: string, tags: readonly string[], payload: Uint8Array): Event {
    return new Event(type, tags, payload);
  }
}

/** An event together with the position it was assigned in the global order. */
export interface SequencedEvent {
  position: Position;
  event: Event;
}

/** The outcome of a successful append: the position range the batch was assigned. */
export interface AppendResult {
  first: Position;
  last: Position;
}

/** A snapshot of a server's operational state, returned by `Client.stats`. */
export interface Stats {
  /** Total durable events, which (positions being dense and 1-based) is also the tip position. */
  eventCount: bigint;
  /** On-disk log segments in the data directory. */
  segmentCount: bigint;
  /** Total bytes on disk in the data directory (log segments plus index sidecars). */
  diskBytes: bigint;
  /** Seconds since the server began accepting connections. */
  uptimeSeconds: bigint;
  /** Connections currently being served, including this one. */
  activeConnections: bigint;
  /** Live subscriptions across all connections. */
  activeSubscriptions: bigint;
  /** Connections refused at the connection cap. Monotonic. */
  connectionsRefused: bigint;
  /** Connections reaped for a handshake, idle, or incomplete-frame timeout. Monotonic. */
  connectionsReaped: bigint;
  /** The server's configured maximum concurrent connections, or 0 when unlimited. */
  maxConnections: bigint;
  /** The server's crate version. */
  version: string;
}

/**
 * One item from a SubscribeStream: either a matching event, or a live-edge marker reporting that
 * the subscription drained everything up to `watermark` and is now tailing live (re-armed after
 * each subsequent catch-up burst).
 */
export type SubEvent =
  | { readonly kind: "event"; readonly event: SequencedEvent }
  | { readonly kind: "caughtUp"; readonly watermark: Position };

/** Reports whether a SubEvent is a live-edge marker rather than an event. */
export function isCaughtUp(item: SubEvent): item is Extract<SubEvent, { kind: "caughtUp" }> {
  return item.kind === "caughtUp";
}

/**
 * One alternative in a Query: a type constraint AND a tag constraint. An event matches when its
 * type is one of `types` (an empty type list matches any type) and its tags contain all of `tags`.
 * Build one with `of`, `ofTypes`, or `withTags`.
 */
export class QueryItem {
  readonly types: readonly string[];
  readonly tags: readonly string[];

  private constructor(types: readonly string[], tags: readonly string[]) {
    this.types = types;
    this.tags = tags;
  }

  /** An item constraining on both types (OR'd; empty means any type) and tags (AND'd). */
  static of(types: readonly string[], tags: readonly string[]): QueryItem {
    return new QueryItem(validatedTypes(types), validatedTagSet(tags));
  }

  /** An item constraining only on type (matching any tags). */
  static ofTypes(...types: string[]): QueryItem {
    return QueryItem.of(types, []);
  }

  /** An item constraining only on tags (matching any type). */
  static withTags(...tags: string[]): QueryItem {
    return QueryItem.of([], tags);
  }
}

/**
 * Selects which events a read, subscription, or append condition covers. It is either the catch-all
 * (`Query.all`) or a set of items OR'd together (`Query.items`). An empty item set matches nothing,
 * which is deliberately distinct from the catch-all.
 */
export class Query {
  readonly matchAll: boolean;
  readonly items: readonly QueryItem[];

  private constructor(matchAll: boolean, items: readonly QueryItem[]) {
    this.matchAll = matchAll;
    this.items = items;
  }

  /** The catch-all query, matching every event. */
  static all(): Query {
    return new Query(true, []);
  }

  /** A query over a set of items OR'd together. With no items it matches nothing. */
  static items(...items: QueryItem[]): Query {
    return new Query(false, items);
  }
}

/**
 * Guards an append: the append is rejected if any event after `after` matches `failIfEventsMatch`.
 * `after` is an exclusive lower bound; `ZERO` (the default) considers the whole log, i.e. fail if
 * any event matches. Build one with `AppendCondition.create`, optionally passing an `after` bound.
 */
export class AppendCondition {
  readonly failIfEventsMatch: Query;
  readonly after: Position;

  private constructor(failIfEventsMatch: Query, after: Position) {
    this.failIfEventsMatch = failIfEventsMatch;
    this.after = after;
  }

  /** A condition over the given query, checking events strictly after `after` (default `ZERO`). */
  static create(failIfEventsMatch: Query, after: Position = ZERO): AppendCondition {
    return new AppendCondition(failIfEventsMatch, after);
  }
}
