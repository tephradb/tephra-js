// Length-prefixed framing: every message on the wire is a 4-byte big-endian uint32 length followed
// by that many bytes of protobuf body. There is no magic, version, or handshake. This matches the
// tephra server and the Go and Rust clients exactly (see tephra-go/framing.go).

import { FrameTooLargeError } from "./errors.js";

export { DEFAULT_MAX_FRAME_LEN } from "./options.js";

/**
 * Prepends the 4-byte big-endian length prefix to an already-encoded body. Rejects an oversized
 * frame before any byte reaches the wire, so the stream stays frame-aligned.
 */
export function writeFrame(body: Uint8Array, maxFrameLen: number): Uint8Array {
  if (body.length > maxFrameLen) {
    throw new FrameTooLargeError(body.length, maxFrameLen);
  }
  const out = new Uint8Array(4 + body.length);
  const len = body.length;
  out[0] = (len >>> 24) & 0xff;
  out[1] = (len >>> 16) & 0xff;
  out[2] = (len >>> 8) & 0xff;
  out[3] = len & 0xff;
  out.set(body, 4);
  return out;
}

function readU32BE(buf: Uint8Array, offset: number): number {
  return (
    (buf[offset] as number) * 0x100_0000 +
    (((buf[offset + 1] as number) << 16) |
      ((buf[offset + 2] as number) << 8) |
      (buf[offset + 3] as number))
  );
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) {
    return b;
  }
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0);
  out.set(b, a.length);
  return out;
}

/**
 * A stateful decoder that buffers incoming socket chunks and yields complete frame bodies. It
 * checks a frame's length against the maximum before waiting for its body, so a hostile prefix can
 * never make it allocate an unbounded buffer.
 */
export class FrameDecoder {
  private buffer: Uint8Array = new Uint8Array(0);

  constructor(private readonly maxFrameLen: number) {}

  /** Feeds a chunk and returns every complete frame body it now yields (possibly none). */
  push(chunk: Uint8Array): Uint8Array[] {
    this.buffer = concat(this.buffer, chunk);
    const frames: Uint8Array[] = [];
    let offset = 0;
    while (this.buffer.length - offset >= 4) {
      const len = readU32BE(this.buffer, offset);
      if (len > this.maxFrameLen) {
        throw new FrameTooLargeError(len, this.maxFrameLen);
      }
      if (this.buffer.length - offset - 4 < len) {
        break;
      }
      frames.push(this.buffer.subarray(offset + 4, offset + 4 + len));
      offset += 4 + len;
    }
    // Keep only the unparsed remainder, copied out so we do not retain the consumed bytes.
    this.buffer = offset === 0 ? this.buffer : this.buffer.slice(offset);
    return frames;
  }

  /** Reports whether a partial frame is buffered (a torn frame if the connection ends here). */
  get hasPartial(): boolean {
    return this.buffer.length > 0;
  }
}
