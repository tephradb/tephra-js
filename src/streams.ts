// The streaming results of a read or a subscription, exposed as async iterables so a caller can
// drive them with `for await`. Each is backed by an unbounded internal buffer fed by the connection
// reader, so a slow consumer never stalls the shared socket; backpressure comes instead from the
// per-socket in-flight budget. This mirrors the Go client's streamBase (tephra-go/stream.go).

import { ProtocolError } from "./errors.js";
import type { Position, SequencedEvent, SubEvent } from "./types.js";
import { asError } from "./util.js";

/**
 * A single-consumer push queue with a terminal state. The connection pushes items and then either
 * finishes cleanly (a read's ReadEnd) or fails; the consumer awaits items in order and, once the
 * queue is drained, observes the terminal outcome.
 */
class Sink<T> {
  private readonly queue: T[] = [];
  private terminalError: Error | null = null;
  private done = false;
  private waiting: {
    resolve: (result: IteratorResult<T>) => void;
    reject: (reason: Error) => void;
  } | null = null;

  push(item: T): void {
    if (this.done) {
      return;
    }
    if (this.waiting) {
      const waiter = this.waiting;
      this.waiting = null;
      waiter.resolve({ value: item, done: false });
    } else {
      this.queue.push(item);
    }
  }

  finish(): void {
    if (this.done) {
      return;
    }
    this.done = true;
    if (this.waiting) {
      const waiter = this.waiting;
      this.waiting = null;
      waiter.resolve({ value: undefined, done: true });
    }
  }

  fail(err: Error): void {
    if (this.done) {
      return;
    }
    this.terminalError = err;
    this.done = true;
    if (this.waiting) {
      const waiter = this.waiting;
      this.waiting = null;
      waiter.reject(err);
    }
  }

  next(): Promise<IteratorResult<T>> {
    const item = this.queue.shift();
    if (item !== undefined) {
      return Promise.resolve({ value: item, done: false });
    }
    if (this.done) {
      return this.terminalError
        ? Promise.reject(this.terminalError)
        : Promise.resolve({ value: undefined, done: true });
    }
    if (this.waiting) {
      return Promise.reject(new ProtocolError("stream is already being iterated"));
    }
    return new Promise<IteratorResult<T>>((resolve, reject) => {
      this.waiting = { resolve, reject };
    });
  }
}

/** Shared machinery for both stream kinds: buffering, cancellation, and abort-signal wiring. */
abstract class BaseStream<T> implements AsyncIterable<T> {
  protected readonly sink = new Sink<T>();
  private closed = false;
  private readonly signal?: AbortSignal;
  private abortListener?: () => void;

  constructor(
    private readonly onCancel: () => void,
    signal?: AbortSignal,
  ) {
    this.signal = signal;
    if (signal) {
      if (signal.aborted) {
        queueMicrotask(() => this.onAbort());
      } else {
        this.abortListener = () => this.onAbort();
        signal.addEventListener("abort", this.abortListener, { once: true });
      }
    }
  }

  /** @internal Delivered by the connection reader. */
  deliverItem(item: T): void {
    this.sink.push(item);
  }

  /** @internal A terminal error from the connection (protocol, server, or connection failure). */
  failWith(err: Error): void {
    this.markClosed();
    this.sink.fail(err);
  }

  protected finishClean(): void {
    this.markClosed();
    this.sink.finish();
  }

  private markClosed(): void {
    this.closed = true;
    this.detachAbort();
  }

  private detachAbort(): void {
    if (this.abortListener && this.signal) {
      this.signal.removeEventListener("abort", this.abortListener);
      this.abortListener = undefined;
    }
  }

  private onAbort(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.detachAbort();
    this.onCancel();
    this.sink.fail(asError(this.signal?.reason, "the operation was aborted"));
  }

  /** Stops the stream, cancelling it server-side (best effort), and ends iteration cleanly. */
  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.detachAbort();
    this.onCancel();
    this.sink.finish();
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: () => this.sink.next(),
      return: async (): Promise<IteratorResult<T>> => {
        await this.close();
        return { value: undefined, done: true };
      },
    };
  }
}

/**
 * A forward or backward read, yielding events in position order until a terminating watermark.
 * After iteration ends cleanly, `watermark` is the position the read was pinned to.
 */
export class ReadStream extends BaseStream<SequencedEvent> {
  private watermarkValue: Position | undefined;

  /** @internal The read's terminating ReadEnd frame. */
  endWith(watermark: Position): void {
    this.watermarkValue = watermark;
    this.finishClean();
  }

  /** The position the read was pinned to, available once the stream has ended cleanly. */
  get watermark(): Position | undefined {
    return this.watermarkValue;
  }

  /** Drains the whole stream into an array, returning the events and the read's watermark. */
  async collect(): Promise<{ events: SequencedEvent[]; watermark: Position }> {
    const events: SequencedEvent[] = [];
    for await (const event of this) {
      events.push(event);
    }
    return { events, watermark: this.watermarkValue ?? 0n };
  }
}

/**
 * A live subscription, yielding events and re-armed caught-up markers indefinitely. It ends only on
 * `close`, an aborted signal, an error, or the connection closing (it never finishes on its own).
 */
export class SubscribeStream extends BaseStream<SubEvent> {}
