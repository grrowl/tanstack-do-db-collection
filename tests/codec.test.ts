import { describe, expect, it } from "vitest"
import { decode, encode } from "../src/wire/codec.ts"

// WHY: every special value here is one that naive JSON.stringify silently
// corrupts (drops, coerces, or loses type). The codec exists so wire deltas and
// at-rest `value TEXT` survive round-trips losslessly (ADR-0001 D17). A test
// that only checked strings/numbers could not fail when fidelity regresses.

const roundtrip = (v: unknown): unknown => decode(encode(v))

describe("tagged value codec", () => {
  it("round-trips JSON-safe primitives", () => {
    for (const v of ["", "hi", 0, 1, -1, 1.5, true, false, null]) {
      expect(roundtrip(v)).toEqual(v)
    }
  })

  it("round-trips nested objects and arrays", () => {
    const v = { a: 1, b: [1, "two", { c: true }], d: { e: null } }
    expect(roundtrip(v)).toEqual(v)
  })

  it("preserves bigint beyond Number.MAX_SAFE_INTEGER", () => {
    expect(roundtrip(123n)).toBe(123n)
    expect(roundtrip({ x: 9007199254740993n })).toEqual({ x: 9007199254740993n })
  })

  it("preserves Date as a Date (not an ISO string)", () => {
    const d = new Date("2026-05-29T12:00:00.000Z")
    const r = roundtrip(d)
    expect(r).toBeInstanceOf(Date)
    expect((r as Date).getTime()).toBe(d.getTime())
  })

  it("preserves NaN and +/-Infinity (JSON turns these into null)", () => {
    expect(roundtrip(NaN)).toBeNaN()
    expect(roundtrip(Infinity)).toBe(Infinity)
    expect(roundtrip(-Infinity)).toBe(-Infinity)
  })

  it("preserves negative zero", () => {
    expect(Object.is(roundtrip(-0), -0)).toBe(true)
    expect(Object.is(roundtrip(0), 0)).toBe(true)
  })

  it("preserves undefined, including as a retained object property", () => {
    expect(roundtrip(undefined)).toBeUndefined()
    const r = roundtrip({ a: undefined, b: 1 }) as Record<string, unknown>
    expect(r).toEqual({ a: undefined, b: 1 })
    expect("a" in r).toBe(true) // key retained, not dropped like JSON would
  })

  it("preserves Uint8Array bytes and type", () => {
    const u = new Uint8Array([0, 1, 2, 254, 255])
    const r = roundtrip(u)
    expect(r).toBeInstanceOf(Uint8Array)
    expect(Array.from(r as Uint8Array)).toEqual([0, 1, 2, 254, 255])
  })

  it("normalizes a bare ArrayBuffer (workerd BLOB) to Uint8Array", () => {
    // workerd's SqlStorage returns BLOB columns as bare ArrayBuffer, which
    // bare JSON stringifies to {} — silent corruption (issue #27). Normalized
    // at emission: the decoder yields a Uint8Array with the exact bytes.
    const r = roundtrip({ payload: new Uint8Array([1, 2, 255]).buffer }) as {
      payload: unknown
    }
    expect(r.payload).toBeInstanceOf(Uint8Array)
    expect(Array.from(r.payload as Uint8Array)).toEqual([1, 2, 255])
  })

  it("does not misinterpret user data shaped like an internal tag", () => {
    // Collision-proofing: this object must survive as plain data.
    const v = { $type: "bigint", value: "not-a-bigint", m: [[[], "date"]] }
    expect(roundtrip(v)).toEqual(v)
  })

  it("handles special values nested deep in mixed structures", () => {
    const v = {
      id: "x",
      counts: [1n, 2n, NaN],
      when: new Date(0),
      meta: { missing: undefined, blob: new Uint8Array([7, 8]) },
    }
    const r = roundtrip(v) as typeof v
    expect(r.counts[0]).toBe(1n)
    expect(r.counts[2]).toBeNaN()
    expect(r.when).toBeInstanceOf(Date)
    expect(r.meta.missing).toBeUndefined()
    expect(Array.from(r.meta.blob)).toEqual([7, 8])
  })

  it("throws loudly on unsupported types", () => {
    expect(() => encode(() => 1)).toThrow(/cannot encode/)
    expect(() => encode(Symbol("x"))).toThrow(/cannot encode/)
  })
})
