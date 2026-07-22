import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: the codec is the contract between server and client. A frame that
// doesn't round-trip is a desync. Both codecs (binary default, JSON debug) must
// be interchangeable — identical fidelity — so switching transports can never
// change behaviour. Value-type fidelity matters because row payloads carry
// Date/bigint/bytes that bare JSON silently corrupts (ADR-0001 D1/D17).

const clientFrames: Array<ClientFrame> = [
  { t: "sub", subId: "s1", collection: "messages", since: "42" },
  { t: "sub", subId: "s2", collection: "messages", limit: 50, offset: 0 },
  { t: "unsub", subId: "s1" },
  { t: "mut", txId: "tx1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "hi" } }] },
  { t: "mut", txId: "tx2", collection: "messages", ops: [{ type: "delete", key: "a" }] },
  { t: "call", txId: "tx3", name: "messages:cancel", args: { reason: "x" } },
]

const serverFrames: Array<ServerFrame> = [
  { t: "snap", sub: "s1", key: "a", row: { id: "a", body: "hi", n: 1 }, seq: "1" },
  { t: "snap-end", sub: "s1", seq: "1" },
  { t: "d", sub: "s1", key: "a", op: "update", cols: { body: "yo" }, seq: "2" },
  { t: "d", sub: "s1", key: "a", op: "delete", seq: "3" },
  { t: "uptodate", seq: "3" },
  { t: "committed", txId: "tx1", seq: "4", result: { ok: true } },
  { t: "rejected", txId: "tx2", error: { code: "FORBIDDEN", message: "nope" } },
  { t: "reset", sub: "s1" },
  { t: "reset" },
]

for (const binary of [true, false]) {
  const name = binary ? "binary" : "json"
  const codec = createFrameCodec({ binary })

  describe(`frame codec (${name})`, () => {
    it("round-trips every client frame", () => {
      for (const f of clientFrames) expect(codec.decode(codec.encode(f))).toEqual(f)
    })

    it("round-trips every server frame", () => {
      for (const f of serverFrames) expect(codec.decode(codec.encode(f))).toEqual(f)
    })

    it("preserves wire-relevant value types inside a row payload", () => {
      const f: ServerFrame = {
        t: "snap",
        sub: "s",
        key: "k",
        row: {
          when: new Date(1_700_000_000_000),
          bytes: new Uint8Array([1, 2, 3]),
          big: 42n,
          bad: NaN,
          nested: { a: [1, 2, 3], b: "z" },
        },
        seq: "9",
      }
      const r = codec.decode(codec.encode(f)) as Extract<ServerFrame, { t: "snap" }>
      const row = r.row as Record<string, unknown>
      expect(row.when).toBeInstanceOf(Date)
      expect((row.when as Date).getTime()).toBe(1_700_000_000_000)
      expect(Array.from(row.bytes as Uint8Array)).toEqual([1, 2, 3])
      expect(row.big).toBe(42n)
      expect(row.bad).toBeNaN()
      expect(row.nested).toEqual({ a: [1, 2, 3], b: "z" })
    })

    it("normalizes a bare ArrayBuffer row value (workerd BLOB) to Uint8Array", () => {
      // workerd's SqlStorage returns BLOB columns as bare ArrayBuffer. msgpack
      // only special-cases ArrayBuffer.isView, so without normalization a bare
      // ArrayBuffer fell through to encodeMap and arrived as {} — silently
      // (issue #27). Both codecs must deliver the bytes as a Uint8Array.
      const f: ServerFrame = {
        t: "snap",
        sub: "s",
        key: "k",
        row: { payload: new Uint8Array([1, 2, 254]).buffer },
        seq: "1",
      }
      const encoded = codec.encode(f)
      const r = codec.decode(encoded) as Extract<ServerFrame, { t: "snap" }>
      const payload = (r.row as Record<string, unknown>).payload
      expect(payload).toBeInstanceOf(Uint8Array)
      expect(Array.from(payload as Uint8Array)).toEqual([1, 2, 254])
      // The decoded bytes must be a COPY, not a view aliasing the wire buffer:
      // an aliased view is mutable through the transport's buffer and pins the
      // whole frame allocation behind a small BLOB.
      if (encoded instanceof Uint8Array) encoded.fill(0)
      expect(Array.from(payload as Uint8Array)).toEqual([1, 2, 254])
    })

    it(`encodes as ${binary ? "Uint8Array" : "string"}`, () => {
      const out = codec.encode({ t: "uptodate", seq: "1" })
      if (binary) expect(out).toBeInstanceOf(Uint8Array)
      else expect(typeof out).toBe("string")
    })
  })
}

describe("frame codec (binary) transport quirks", () => {
  it("decodes an ArrayBuffer, as workerd delivers binary messages", () => {
    const codec = createFrameCodec({ binary: true })
    const u8 = codec.encode({ t: "uptodate", seq: "7" }) as Uint8Array
    const ab = new ArrayBuffer(u8.byteLength)
    new Uint8Array(ab).set(u8)
    expect(codec.decode(ab)).toEqual({ t: "uptodate", seq: "7" })
  })
})
