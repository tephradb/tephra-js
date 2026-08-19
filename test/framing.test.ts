import { describe, expect, test } from "vitest";
import { FrameTooLargeError } from "../src/errors.js";
import { DEFAULT_MAX_FRAME_LEN, FrameDecoder, writeFrame } from "../src/framing.js";

describe("writeFrame", () => {
  test("prepends a 4-byte big-endian length", () => {
    const body = new Uint8Array(300).fill(7);
    const frame = writeFrame(body, DEFAULT_MAX_FRAME_LEN);
    expect(frame.length).toBe(304);
    // 300 == 0x0000_012C, big-endian.
    expect(Array.from(frame.subarray(0, 4))).toEqual([0x00, 0x00, 0x01, 0x2c]);
    expect(frame[4]).toBe(7);
  });

  test("rejects an oversized body before any byte reaches the wire", () => {
    const body = new Uint8Array(10);
    expect(() => writeFrame(body, 4)).toThrow(FrameTooLargeError);
  });
});

describe("FrameDecoder", () => {
  test("yields a complete frame", () => {
    const decoder = new FrameDecoder(DEFAULT_MAX_FRAME_LEN);
    const frame = writeFrame(new Uint8Array([1, 2, 3]), DEFAULT_MAX_FRAME_LEN);
    const bodies = decoder.push(frame);
    expect(bodies).toHaveLength(1);
    expect(Array.from(bodies[0] as Uint8Array)).toEqual([1, 2, 3]);
    expect(decoder.hasPartial).toBe(false);
  });

  test("reassembles a frame split across chunks", () => {
    const decoder = new FrameDecoder(DEFAULT_MAX_FRAME_LEN);
    const frame = writeFrame(new Uint8Array([10, 20, 30, 40]), DEFAULT_MAX_FRAME_LEN);
    expect(decoder.push(frame.subarray(0, 2))).toHaveLength(0);
    expect(decoder.hasPartial).toBe(true);
    expect(decoder.push(frame.subarray(2, 5))).toHaveLength(0);
    const bodies = decoder.push(frame.subarray(5));
    expect(bodies).toHaveLength(1);
    expect(Array.from(bodies[0] as Uint8Array)).toEqual([10, 20, 30, 40]);
  });

  test("yields multiple frames from one chunk", () => {
    const decoder = new FrameDecoder(DEFAULT_MAX_FRAME_LEN);
    const a = writeFrame(new Uint8Array([1]), DEFAULT_MAX_FRAME_LEN);
    const b = writeFrame(new Uint8Array([2, 2]), DEFAULT_MAX_FRAME_LEN);
    const combined = new Uint8Array(a.length + b.length);
    combined.set(a, 0);
    combined.set(b, a.length);
    const bodies = decoder.push(combined);
    expect(bodies).toHaveLength(2);
    expect(Array.from(bodies[0] as Uint8Array)).toEqual([1]);
    expect(Array.from(bodies[1] as Uint8Array)).toEqual([2, 2]);
  });

  test("rejects a frame whose prefix exceeds the maximum", () => {
    const decoder = new FrameDecoder(4);
    // A length prefix of 100 with maxFrameLen 4.
    const prefix = new Uint8Array([0x00, 0x00, 0x00, 0x64]);
    expect(() => decoder.push(prefix)).toThrow(FrameTooLargeError);
  });
});
