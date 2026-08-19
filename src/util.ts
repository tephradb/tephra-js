// Small async primitives shared by the connection layer: a resolvable promise, a counting semaphore
// for the in-flight budget, and an async gate the writer loop waits on. Nothing here is specific to
// tephra; it is the plumbing that turns callback-driven sockets into awaitable operations.

/** A promise together with its resolve and reject, for handing a result across async boundaries. */
export class Deferred<T> {
  readonly promise: Promise<T>;
  resolve!: (value: T) => void;
  reject!: (reason: Error) => void;

  constructor() {
    this.promise = new Promise<T>((resolve, reject) => {
      this.resolve = resolve;
      this.reject = reject;
    });
  }
}

/**
 * A counting semaphore with FIFO waiters. The connection holds one sized to the in-flight budget:
 * a permit is acquired before a request goes on the wire and released when it completes, so awaiting
 * a permit is the backpressure path when the budget is exhausted.
 */
export class Semaphore {
  private permits: number;
  private readonly waiters: Array<() => void> = [];

  constructor(permits: number) {
    this.permits = permits;
  }

  acquire(): Promise<void> {
    if (this.permits > 0) {
      this.permits -= 1;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  release(): void {
    const next = this.waiters.shift();
    if (next) {
      next();
    } else {
      this.permits += 1;
    }
  }

  /** Wakes every waiter, used when the connection dies so blocked producers observe the death. */
  drainWaiters(): void {
    while (this.waiters.length > 0) {
      const next = this.waiters.shift();
      next?.();
    }
  }
}

/**
 * A one-shot-per-wait gate: consumers `wait()` for the next `signal()`. The writer loop uses it to
 * sleep until a frame is queued, rather than spinning.
 */
export class Gate {
  private waiter: (() => void) | null = null;
  private signalled = false;

  signal(): void {
    if (this.waiter) {
      const resume = this.waiter;
      this.waiter = null;
      resume();
    } else {
      this.signalled = true;
    }
  }

  wait(): Promise<void> {
    if (this.signalled) {
      this.signalled = false;
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.waiter = resolve;
    });
  }
}

/** Coerces an AbortSignal reason (or anything thrown) into an Error, preserving an existing one. */
export function asError(reason: unknown, fallback: string): Error {
  if (reason instanceof Error) {
    return reason;
  }
  const err = new Error(reason === undefined ? fallback : String(reason));
  if (reason === undefined) {
    err.name = "AbortError";
  }
  return err;
}
