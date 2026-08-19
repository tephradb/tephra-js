// A minimal protobuf3 wire codec, hand written so the client carries no runtime dependencies.
//
// It covers exactly the surface the tephra protocol uses: varints (as bigint for 64-bit fields so a
// Position can span the whole u64 range), length-delimited fields (strings, bytes, embedded
// messages), and the ability to skip an unknown field so a newer server never breaks an older
// client. See proto/tephra/v1/tephra.proto for the schema these encoders and decoders mirror.

// Protobuf wire types.
export const WIRE_VARINT = 0;
export const WIRE_FIXED64 = 1;
export const WIRE_LEN = 2;
export const WIRE_FIXED32 = 5;

const U64_MASK = (1n << 64n) - 1n;

/** A growable buffer that appends protobuf fields in schema order. */
export class Writer {
  private buf: Uint8Array;
  private len = 0;

  constructor(capacity = 64) {
    this.buf = new Uint8Array(capacity);
  }

  private reserve(extra: number): void {
    const needed = this.len + extra;
    if (needed <= this.buf.length) {
      return;
    }
    let cap = this.buf.length * 2;
    while (cap < needed) {
      cap *= 2;
    }
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }

  private pushByte(byte: number): void {
    this.reserve(1);
    this.buf[this.len++] = byte;
  }

  /** Appends a base-128 varint from a non-negative number (used for tags and 32-bit fields). */
  varint(value: number): void {
    // Values wider than 32 bits (only request ids and positions in practice) go through bigint.
    if (value > 0xffff_ffff || value < 0) {
      this.varintBig(BigInt(value));
      return;
    }
    let v = value >>> 0;
    while (v >= 0x80) {
      this.pushByte((v & 0x7f) | 0x80);
      v >>>= 7;
    }
    this.pushByte(v);
  }

  /** Appends a base-128 varint from a bigint, wrapping into the unsigned 64-bit range. */
  varintBig(value: bigint): void {
    let v = value & U64_MASK;
    while (v >= 0x80n) {
      this.pushByte(Number((v & 0x7fn) | 0x80n));
      v >>= 7n;
    }
    this.pushByte(Number(v));
  }

  private tag(fieldNo: number, wireType: number): void {
    this.varint((fieldNo << 3) | wireType);
  }

  /** Writes a uint64 field from a bigint, omitting it when zero (proto3 default). */
  uint64(fieldNo: number, value: bigint): void {
    if (value === 0n) {
      return;
    }
    this.tag(fieldNo, WIRE_VARINT);
    this.varintBig(value);
  }

  /** Writes a uint64 field unconditionally, even when zero (for a present `optional` field). */
  uint64Present(fieldNo: number, value: bigint): void {
    this.tag(fieldNo, WIRE_VARINT);
    this.varintBig(value);
  }

  /** Writes a bool field, omitting it when false (proto3 default). */
  bool(fieldNo: number, value: boolean): void {
    if (!value) {
      return;
    }
    this.tag(fieldNo, WIRE_VARINT);
    this.pushByte(1);
  }

  /** Writes an enum field from its integer value, omitting it when zero (proto3 default). */
  enum(fieldNo: number, value: number): void {
    if (value === 0) {
      return;
    }
    this.tag(fieldNo, WIRE_VARINT);
    this.varint(value);
  }

  /** Writes a string field, omitting it when empty (proto3 default). */
  string(fieldNo: number, value: string): void {
    if (value.length === 0) {
      return;
    }
    this.bytes(fieldNo, encodeUtf8(value));
  }

  /** Writes a bytes field, omitting it when empty (proto3 default). */
  bytes(fieldNo: number, value: Uint8Array): void {
    if (value.length === 0) {
      return;
    }
    this.tag(fieldNo, WIRE_LEN);
    this.varint(value.length);
    this.reserve(value.length);
    this.buf.set(value, this.len);
    this.len += value.length;
  }

  /** Writes an embedded message field from its already-encoded bytes. */
  message(fieldNo: number, value: Uint8Array): void {
    this.tag(fieldNo, WIRE_LEN);
    this.varint(value.length);
    this.reserve(value.length);
    this.buf.set(value, this.len);
    this.len += value.length;
  }

  /** Writes a repeated string element (each carries its own tag, so empties are kept). */
  repeatedString(fieldNo: number, value: string): void {
    this.tag(fieldNo, WIRE_LEN);
    const encoded = encodeUtf8(value);
    this.varint(encoded.length);
    this.reserve(encoded.length);
    this.buf.set(encoded, this.len);
    this.len += encoded.length;
  }

  finish(): Uint8Array {
    return this.buf.subarray(0, this.len);
  }
}

/** A cursor over a protobuf message body, reading fields until the buffer is exhausted. */
export class Reader {
  private pos = 0;

  constructor(private readonly buf: Uint8Array) {}

  get done(): boolean {
    return this.pos >= this.buf.length;
  }

  /** Reads a field tag, returning the field number and wire type. */
  tag(): { fieldNo: number; wireType: number } {
    const key = Number(this.varintBig());
    return { fieldNo: key >>> 3, wireType: key & 0x7 };
  }

  /** Reads a base-128 varint as a bigint (the widest case). */
  varintBig(): bigint {
    let result = 0n;
    let shift = 0n;
    for (;;) {
      if (this.pos >= this.buf.length) {
        throw new RangeError("varint truncated");
      }
      const byte = this.buf[this.pos++] as number;
      result |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) {
        return result & U64_MASK;
      }
      shift += 7n;
    }
  }

  /** Reads a varint that is known to fit in a 32-bit range (enums, tags). */
  varint(): number {
    return Number(this.varintBig());
  }

  bool(): boolean {
    return this.varintBig() !== 0n;
  }

  /** Reads a length-delimited field as a subarray view into the underlying buffer. */
  bytes(): Uint8Array {
    const len = Number(this.varintBig());
    const end = this.pos + len;
    if (end > this.buf.length) {
      throw new RangeError("length-delimited field truncated");
    }
    const view = this.buf.subarray(this.pos, end);
    this.pos = end;
    return view;
  }

  string(): string {
    return decodeUtf8(this.bytes());
  }

  /** Reads a length-delimited field and returns a Reader scoped to it (an embedded message). */
  message(): Reader {
    return new Reader(this.bytes());
  }

  /** Skips a field of the given wire type, so unknown fields do not break decoding. */
  skip(wireType: number): void {
    switch (wireType) {
      case WIRE_VARINT:
        this.varintBig();
        return;
      case WIRE_FIXED64:
        this.pos += 8;
        return;
      case WIRE_LEN: {
        // Read the length first: varintBig advances pos, so folding it into `+=` would capture the
        // pre-advance pos and drop the length bytes.
        const len = Number(this.varintBig());
        this.pos += len;
        return;
      }
      case WIRE_FIXED32:
        this.pos += 4;
        return;
      default:
        throw new RangeError(`unsupported wire type ${wireType}`);
    }
  }
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: false });

export function encodeUtf8(value: string): Uint8Array {
  return textEncoder.encode(value);
}

export function decodeUtf8(bytes: Uint8Array): string {
  return textDecoder.decode(bytes);
}
