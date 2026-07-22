// Frame codec (ADR-0001 D1): binary MessagePack on the wire by default, with a
// JSON codec for debugging / human-readable transports.
//
// Two codecs, identical fidelity:
//   - binary: @msgpack/msgpack. Natively preserves the wire-relevant value
//     types — number incl. NaN/±Infinity, Uint8Array (bin), Date (timestamp
//     extension), and bigint (useBigInt64). Compact and fast.
//   - json:   the M1 tagged value codec (./codec.ts), so JSON debugging keeps
//     full fidelity for Date/bigint/etc. that bare JSON would lose.
//
// NOTE (deferred micro-opt): ADR-0001 D1 mentions an explicit 1-byte type tag.
// We use MessagePack's short string `t` discriminator instead; a numeric tag
// saves ~9 bytes/frame, dwarfed by row payloads, so it is deferred to M9 and
// measured rather than assumed. Flagged here rather than silently dropped.

import { decode as mpDecode, encode as mpEncode, ExtensionCodec } from "@msgpack/msgpack"
import { decode as valueDecode, encode as valueEncode } from "./codec.ts"
import type { ClientFrame, ServerFrame } from "./frames.ts"

export type Frame = ClientFrame | ServerFrame

/** What a transport may hand `decode` (workerd delivers binary as ArrayBuffer). */
export type WireIn = ArrayBuffer | Uint8Array | string
/** What `encode` emits — bytes for binary, string for JSON. */
export type WireOut = Uint8Array | string

export interface FrameCodec {
  readonly binary: boolean
  encode(frame: Frame): WireOut
  decode(data: WireIn): Frame
}

function toBytes(d: WireIn): Uint8Array {
  if (d instanceof Uint8Array) return d
  if (d instanceof ArrayBuffer) return new Uint8Array(d)
  return new TextEncoder().encode(d)
}

// workerd's SqlStorage returns BLOB columns as bare ArrayBuffer. @msgpack/msgpack
// only special-cases ArrayBuffer.isView, so a bare ArrayBuffer would fall through
// to encodeMap and arrive as {} — silently (issue #27, ADR-0017). The extension
// codec is consulted before that fallthrough: encode the raw bytes, decode to
// Uint8Array so BLOBs land on the client exactly like a Uint8Array column value.
const ARRAY_BUFFER_EXT_TYPE = 0
const extensionCodec = new ExtensionCodec()
extensionCodec.register({
  type: ARRAY_BUFFER_EXT_TYPE,
  encode: (v) => (v instanceof ArrayBuffer ? new Uint8Array(v) : null),
  // Copy out of the decode buffer: `bytes` is a subarray VIEW of the whole
  // incoming frame — returning it as-is would alias the wire buffer (mutable
  // by the caller) and keep the full frame alive behind a small BLOB.
  decode: (bytes) => bytes.slice(), // Uint8Array — normalized, never back to ArrayBuffer
})

const MSGPACK_OPTS = { useBigInt64: true, extensionCodec } as const

const binaryCodec: FrameCodec = {
  binary: true,
  encode: (frame) => mpEncode(frame, MSGPACK_OPTS),
  decode: (data) => mpDecode(toBytes(data), MSGPACK_OPTS) as Frame,
}

const jsonCodec: FrameCodec = {
  binary: false,
  encode: (frame) => valueEncode(frame),
  decode: (data) =>
    valueDecode(typeof data === "string" ? data : new TextDecoder().decode(toBytes(data))) as Frame,
}

export interface FrameCodecOptions {
  /** Binary MessagePack (default) vs JSON (human-readable, for debugging). */
  binary?: boolean
}

export function createFrameCodec(opts: FrameCodecOptions = {}): FrameCodec {
  return opts.binary === false ? jsonCodec : binaryCodec
}
