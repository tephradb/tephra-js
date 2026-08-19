// @tephradb/client: a TypeScript client for a tephra event store, speaking its length-prefixed
// protobuf-over-TCP protocol. See the README for a guided tour; the Client class is the entry point.

export { Client } from "./client.js";

export { PROTOCOL_VERSION } from "./proto/messages.js";

export {
  AppendCondition,
  type AppendResult,
  Event,
  isCaughtUp,
  MAX,
  MAX_NAME_LEN,
  type Position,
  Query,
  QueryItem,
  type SequencedEvent,
  type Stats,
  type SubEvent,
  ZERO,
} from "./types.js";

export type { ReadStream, SubscribeStream } from "./streams.js";

export {
  ClosedError,
  ConnError,
  ErrorCode,
  FrameTooLargeError,
  ProtocolError,
  ServerError,
  TephraError,
  ValidationError,
} from "./errors.js";

export {
  type CallOptions,
  type ClientOptions,
  DEFAULT_BULK_CONNECTIONS,
  DEFAULT_MAX_FRAME_LEN,
  DEFAULT_MAX_INFLIGHT_REQUESTS,
  DEFAULT_REQUEST_QUEUE_DEPTH,
  type TlsOptions,
} from "./options.js";
