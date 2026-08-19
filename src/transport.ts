// Establishes the TCP (or TLS) transport for one connection. A tephra connection is a plain byte
// stream: for TLS the session is negotiated before the first frame, and the wire protocol is
// unchanged afterward. `servername` defaults to the dial host so verifying a hostname certificate
// needs no extra configuration.

import { connect as netConnect } from "node:net";
import type { Duplex } from "node:stream";
import { connect as tlsConnect } from "node:tls";
import type { ResolvedConfig } from "./options.js";
import { asError } from "./util.js";

/** Opens a socket to host:port, negotiating TLS when configured, and sets TCP_NODELAY. */
export function dialSocket(
  host: string,
  port: number,
  config: ResolvedConfig,
  signal?: AbortSignal,
): Promise<Duplex> {
  return new Promise<Duplex>((resolve, reject) => {
    if (signal?.aborted) {
      reject(asError(signal.reason, "connect aborted"));
      return;
    }

    let settled = false;
    const socket = config.tls
      ? tlsConnect({ ...config.tls, host, port, servername: config.tls.servername ?? host })
      : netConnect({ host, port });

    const cleanup = (): void => {
      socket.setTimeout(0);
      socket.removeListener("error", onError);
      socket.removeListener(readyEvent, onReady);
      if (signal) {
        signal.removeEventListener("abort", onAbort);
      }
    };

    const fail = (err: Error): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      socket.destroy();
      reject(err);
    };

    const onReady = (): void => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve(socket);
    };

    const onError = (err: Error): void => fail(err);
    const onAbort = (): void => fail(asError(signal?.reason, "connect aborted"));

    const readyEvent = config.tls ? "secureConnect" : "connect";
    socket.setNoDelay(true);
    socket.once(readyEvent, onReady);
    socket.once("error", onError);
    if (config.connectTimeout !== undefined) {
      socket.setTimeout(config.connectTimeout, () => fail(new Error("connect timed out")));
    }
    if (signal) {
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
}
