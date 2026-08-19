// Encoders and decoders for the tephra.v1 protobuf messages, built on the minimal wire codec.
//
// These operate on plain "wire" structs (bigint for every uint64, Uint8Array for bytes) that the
// client layer maps to and from the friendly public types. Both directions are provided: the client
// encodes a Request and decodes a Response, and the mirrored functions let a test server do the
// reverse. Field numbers and the oneof layout follow proto/tephra/v1/tephra.proto exactly.

import { Reader, Writer } from "./wire.js";

// ---------------------------------------------------------------------------
// Wire structs
// ---------------------------------------------------------------------------

export interface WireEvent {
  type: string;
  tags: string[];
  payload: Uint8Array;
}

export interface WireQueryItem {
  types: string[];
  tags: string[];
}

export interface WireQuery {
  all: boolean;
  items: WireQueryItem[];
}

export interface WireAppendCondition {
  failIfEventsMatch: WireQuery;
  after: bigint;
}

export interface WireAppendRequest {
  events: WireEvent[];
  condition?: WireAppendCondition;
}

export interface WireReadRequest {
  query: WireQuery;
  after: bigint;
  limit?: bigint;
  reverse: boolean;
}

export interface WireSubscribeRequest {
  query: WireQuery;
  after: bigint;
}

export interface WireCancelRequest {
  target: bigint;
}

export type RequestKind =
  | { kind: "append"; append: WireAppendRequest }
  | { kind: "read"; read: WireReadRequest }
  | { kind: "subscribe"; subscribe: WireSubscribeRequest }
  | { kind: "cancel"; cancel: WireCancelRequest }
  | { kind: "stats" }
  | { kind: "none" };

export interface WireRequest {
  requestId: bigint;
  kind: RequestKind;
}

export interface WireAppendResponse {
  first: bigint;
  last: bigint;
}

export interface WireSequencedEvent {
  position: bigint;
  event?: WireEvent;
}

export interface WireReadEvents {
  events: WireSequencedEvent[];
}

export interface WireReadEnd {
  watermark: bigint;
}

export interface WireSubscribeCaughtUp {
  watermark: bigint;
}

export interface WireStatsResponse {
  eventCount: bigint;
  segmentCount: bigint;
  diskBytes: bigint;
  uptimeSeconds: bigint;
  activeConnections: bigint;
  activeSubscriptions: bigint;
  version: string;
  connectionsRefused: bigint;
  maxConnections: bigint;
  connectionsReaped: bigint;
}

export interface WireErrorResponse {
  code: number;
  message: string;
  conflictPosition?: bigint;
  retryable: boolean;
}

export type ResponseKind =
  | { kind: "append"; append: WireAppendResponse }
  | { kind: "readEvents"; readEvents: WireReadEvents }
  | { kind: "readEnd"; readEnd: WireReadEnd }
  | { kind: "error"; error: WireErrorResponse }
  | { kind: "caughtUp"; caughtUp: WireSubscribeCaughtUp }
  | { kind: "stats"; stats: WireStatsResponse }
  | { kind: "none" };

export interface WireResponse {
  requestId: bigint;
  kind: ResponseKind;
}

// ---------------------------------------------------------------------------
// Event, Query, condition
// ---------------------------------------------------------------------------

function encodeEvent(event: WireEvent): Uint8Array {
  const w = new Writer();
  w.string(1, event.type);
  for (const tag of event.tags) {
    w.repeatedString(2, tag);
  }
  w.bytes(3, event.payload);
  return w.finish();
}

function decodeEvent(r: Reader): WireEvent {
  const event: WireEvent = { type: "", tags: [], payload: new Uint8Array(0) };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        event.type = r.string();
        break;
      case 2:
        event.tags.push(r.string());
        break;
      case 3:
        event.payload = r.bytes().slice();
        break;
      default:
        r.skip(wireType);
    }
  }
  return event;
}

function encodeQueryItem(item: WireQueryItem): Uint8Array {
  const w = new Writer();
  for (const type of item.types) {
    w.repeatedString(1, type);
  }
  for (const tag of item.tags) {
    w.repeatedString(2, tag);
  }
  return w.finish();
}

function decodeQueryItem(r: Reader): WireQueryItem {
  const item: WireQueryItem = { types: [], tags: [] };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        item.types.push(r.string());
        break;
      case 2:
        item.tags.push(r.string());
        break;
      default:
        r.skip(wireType);
    }
  }
  return item;
}

function encodeQuery(query: WireQuery): Uint8Array {
  const w = new Writer();
  w.bool(1, query.all);
  for (const item of query.items) {
    w.message(2, encodeQueryItem(item));
  }
  return w.finish();
}

function decodeQuery(r: Reader): WireQuery {
  const query: WireQuery = { all: false, items: [] };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        query.all = r.bool();
        break;
      case 2:
        query.items.push(decodeQueryItem(r.message()));
        break;
      default:
        r.skip(wireType);
    }
  }
  return query;
}

function encodeCondition(condition: WireAppendCondition): Uint8Array {
  const w = new Writer();
  w.message(1, encodeQuery(condition.failIfEventsMatch));
  w.uint64(2, condition.after);
  return w.finish();
}

function decodeCondition(r: Reader): WireAppendCondition {
  const condition: WireAppendCondition = {
    failIfEventsMatch: { all: false, items: [] },
    after: 0n,
  };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        condition.failIfEventsMatch = decodeQuery(r.message());
        break;
      case 2:
        condition.after = r.varintBig();
        break;
      default:
        r.skip(wireType);
    }
  }
  return condition;
}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

function encodeAppendRequest(req: WireAppendRequest): Uint8Array {
  const w = new Writer();
  for (const event of req.events) {
    w.message(1, encodeEvent(event));
  }
  if (req.condition !== undefined) {
    w.message(2, encodeCondition(req.condition));
  }
  return w.finish();
}

function decodeAppendRequest(r: Reader): WireAppendRequest {
  const req: WireAppendRequest = { events: [] };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        req.events.push(decodeEvent(r.message()));
        break;
      case 2:
        req.condition = decodeCondition(r.message());
        break;
      default:
        r.skip(wireType);
    }
  }
  return req;
}

function encodeReadRequest(req: WireReadRequest): Uint8Array {
  const w = new Writer();
  w.message(1, encodeQuery(req.query));
  w.uint64(2, req.after);
  if (req.limit !== undefined) {
    // `limit` is proto `optional`: a present 0 means "return nothing", distinct from unlimited.
    w.uint64Present(3, req.limit);
  }
  w.bool(4, req.reverse);
  return w.finish();
}

function decodeReadRequest(r: Reader): WireReadRequest {
  const req: WireReadRequest = { query: { all: false, items: [] }, after: 0n, reverse: false };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        req.query = decodeQuery(r.message());
        break;
      case 2:
        req.after = r.varintBig();
        break;
      case 3:
        req.limit = r.varintBig();
        break;
      case 4:
        req.reverse = r.bool();
        break;
      default:
        r.skip(wireType);
    }
  }
  return req;
}

function encodeSubscribeRequest(req: WireSubscribeRequest): Uint8Array {
  const w = new Writer();
  w.message(1, encodeQuery(req.query));
  w.uint64(2, req.after);
  return w.finish();
}

function decodeSubscribeRequest(r: Reader): WireSubscribeRequest {
  const req: WireSubscribeRequest = { query: { all: false, items: [] }, after: 0n };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        req.query = decodeQuery(r.message());
        break;
      case 2:
        req.after = r.varintBig();
        break;
      default:
        r.skip(wireType);
    }
  }
  return req;
}

function encodeCancelRequest(req: WireCancelRequest): Uint8Array {
  const w = new Writer();
  w.uint64(1, req.target);
  return w.finish();
}

function decodeCancelRequest(r: Reader): WireCancelRequest {
  const req: WireCancelRequest = { target: 0n };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        req.target = r.varintBig();
        break;
      default:
        r.skip(wireType);
    }
  }
  return req;
}

export function encodeRequest(request: WireRequest): Uint8Array {
  const w = new Writer();
  w.uint64(1, request.requestId);
  switch (request.kind.kind) {
    case "append":
      w.message(2, encodeAppendRequest(request.kind.append));
      break;
    case "read":
      w.message(3, encodeReadRequest(request.kind.read));
      break;
    case "subscribe":
      w.message(4, encodeSubscribeRequest(request.kind.subscribe));
      break;
    case "cancel":
      w.message(5, encodeCancelRequest(request.kind.cancel));
      break;
    case "stats":
      // StatsRequest is empty; the tag alone sets the oneof.
      w.message(6, new Uint8Array(0));
      break;
    case "none":
      break;
  }
  return w.finish();
}

export function decodeRequest(bytes: Uint8Array): WireRequest {
  const r = new Reader(bytes);
  const request: WireRequest = { requestId: 0n, kind: { kind: "none" } };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        request.requestId = r.varintBig();
        break;
      case 2:
        request.kind = { kind: "append", append: decodeAppendRequest(r.message()) };
        break;
      case 3:
        request.kind = { kind: "read", read: decodeReadRequest(r.message()) };
        break;
      case 4:
        request.kind = { kind: "subscribe", subscribe: decodeSubscribeRequest(r.message()) };
        break;
      case 5:
        request.kind = { kind: "cancel", cancel: decodeCancelRequest(r.message()) };
        break;
      case 6:
        r.message();
        request.kind = { kind: "stats" };
        break;
      default:
        r.skip(wireType);
    }
  }
  return request;
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

function encodeSequencedEvent(event: WireSequencedEvent): Uint8Array {
  const w = new Writer();
  w.uint64(1, event.position);
  if (event.event !== undefined) {
    w.message(2, encodeEvent(event.event));
  }
  return w.finish();
}

function decodeSequencedEvent(r: Reader): WireSequencedEvent {
  const event: WireSequencedEvent = { position: 0n };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        event.position = r.varintBig();
        break;
      case 2:
        event.event = decodeEvent(r.message());
        break;
      default:
        r.skip(wireType);
    }
  }
  return event;
}

function encodeAppendResponse(res: WireAppendResponse): Uint8Array {
  const w = new Writer();
  w.uint64(1, res.first);
  w.uint64(2, res.last);
  return w.finish();
}

function decodeAppendResponse(r: Reader): WireAppendResponse {
  const res: WireAppendResponse = { first: 0n, last: 0n };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        res.first = r.varintBig();
        break;
      case 2:
        res.last = r.varintBig();
        break;
      default:
        r.skip(wireType);
    }
  }
  return res;
}

function encodeReadEvents(res: WireReadEvents): Uint8Array {
  const w = new Writer();
  for (const event of res.events) {
    w.message(1, encodeSequencedEvent(event));
  }
  return w.finish();
}

function decodeReadEvents(r: Reader): WireReadEvents {
  const res: WireReadEvents = { events: [] };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        res.events.push(decodeSequencedEvent(r.message()));
        break;
      default:
        r.skip(wireType);
    }
  }
  return res;
}

function encodeWatermark(watermark: bigint): Uint8Array {
  const w = new Writer();
  w.uint64(1, watermark);
  return w.finish();
}

function decodeWatermark(r: Reader): bigint {
  let watermark = 0n;
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    if (fieldNo === 1) {
      watermark = r.varintBig();
    } else {
      r.skip(wireType);
    }
  }
  return watermark;
}

function encodeStatsResponse(res: WireStatsResponse): Uint8Array {
  const w = new Writer();
  w.uint64(1, res.eventCount);
  w.uint64(2, res.segmentCount);
  w.uint64(3, res.diskBytes);
  w.uint64(4, res.uptimeSeconds);
  w.uint64(5, res.activeConnections);
  w.uint64(6, res.activeSubscriptions);
  w.string(7, res.version);
  w.uint64(8, res.connectionsRefused);
  w.uint64(9, res.maxConnections);
  w.uint64(10, res.connectionsReaped);
  return w.finish();
}

function decodeStatsResponse(r: Reader): WireStatsResponse {
  const res: WireStatsResponse = {
    eventCount: 0n,
    segmentCount: 0n,
    diskBytes: 0n,
    uptimeSeconds: 0n,
    activeConnections: 0n,
    activeSubscriptions: 0n,
    version: "",
    connectionsRefused: 0n,
    maxConnections: 0n,
    connectionsReaped: 0n,
  };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        res.eventCount = r.varintBig();
        break;
      case 2:
        res.segmentCount = r.varintBig();
        break;
      case 3:
        res.diskBytes = r.varintBig();
        break;
      case 4:
        res.uptimeSeconds = r.varintBig();
        break;
      case 5:
        res.activeConnections = r.varintBig();
        break;
      case 6:
        res.activeSubscriptions = r.varintBig();
        break;
      case 7:
        res.version = r.string();
        break;
      case 8:
        res.connectionsRefused = r.varintBig();
        break;
      case 9:
        res.maxConnections = r.varintBig();
        break;
      case 10:
        res.connectionsReaped = r.varintBig();
        break;
      default:
        r.skip(wireType);
    }
  }
  return res;
}

function encodeErrorResponse(res: WireErrorResponse): Uint8Array {
  const w = new Writer();
  w.enum(1, res.code);
  w.string(2, res.message);
  if (res.conflictPosition !== undefined) {
    w.uint64Present(3, res.conflictPosition);
  }
  w.bool(4, res.retryable);
  return w.finish();
}

function decodeErrorResponse(r: Reader): WireErrorResponse {
  const res: WireErrorResponse = { code: 0, message: "", retryable: false };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        res.code = r.varint();
        break;
      case 2:
        res.message = r.string();
        break;
      case 3:
        res.conflictPosition = r.varintBig();
        break;
      case 4:
        res.retryable = r.bool();
        break;
      default:
        r.skip(wireType);
    }
  }
  return res;
}

export function encodeResponse(response: WireResponse): Uint8Array {
  const w = new Writer();
  w.uint64(1, response.requestId);
  switch (response.kind.kind) {
    case "append":
      w.message(2, encodeAppendResponse(response.kind.append));
      break;
    case "readEvents":
      w.message(3, encodeReadEvents(response.kind.readEvents));
      break;
    case "readEnd":
      w.message(4, encodeWatermark(response.kind.readEnd.watermark));
      break;
    case "error":
      w.message(5, encodeErrorResponse(response.kind.error));
      break;
    case "caughtUp":
      w.message(6, encodeWatermark(response.kind.caughtUp.watermark));
      break;
    case "stats":
      w.message(7, encodeStatsResponse(response.kind.stats));
      break;
    case "none":
      break;
  }
  return w.finish();
}

export function decodeResponse(bytes: Uint8Array): WireResponse {
  const r = new Reader(bytes);
  const response: WireResponse = { requestId: 0n, kind: { kind: "none" } };
  while (!r.done) {
    const { fieldNo, wireType } = r.tag();
    switch (fieldNo) {
      case 1:
        response.requestId = r.varintBig();
        break;
      case 2:
        response.kind = { kind: "append", append: decodeAppendResponse(r.message()) };
        break;
      case 3:
        response.kind = { kind: "readEvents", readEvents: decodeReadEvents(r.message()) };
        break;
      case 4:
        response.kind = { kind: "readEnd", readEnd: { watermark: decodeWatermark(r.message()) } };
        break;
      case 5:
        response.kind = { kind: "error", error: decodeErrorResponse(r.message()) };
        break;
      case 6:
        response.kind = { kind: "caughtUp", caughtUp: { watermark: decodeWatermark(r.message()) } };
        break;
      case 7:
        response.kind = { kind: "stats", stats: decodeStatsResponse(r.message()) };
        break;
      default:
        r.skip(wireType);
    }
  }
  return response;
}
