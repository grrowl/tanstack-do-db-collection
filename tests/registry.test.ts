import { describe, expect, it } from "vitest"
import { Registry } from "../src/server/registry.ts"

// WHY: `{table, pk}` are interpolated raw into trigger DDL and SELECTs, so
// defineCollection rejects unsafe identifiers, the reserved `_sync_` prefix, and
// duplicate collections at registration. The structural D9 rule (the pk is a sole
// TEXT client-supplied key) is now enforced against the REAL table by
// registerSync -> assertSyncCompatible (see assert-sync-compatible.test.ts),
// since the author owns the schema (ADR-0007).

const define = (def: { table: string; pk: string }) => () => new Registry().defineCollection(def)

describe("Registry.defineCollection — identifier + registration guards", () => {
  it("accepts a valid { table, pk }", () => {
    expect(define({ table: "messages", pk: "id" })).not.toThrow()
  })

  it("rejects the reserved _sync_ table prefix", () => {
    expect(define({ table: "_sync_x", pk: "id" })).toThrow(/reserved/)
  })

  it("rejects an invalid table identifier (no SQL injection into trigger DDL)", () => {
    expect(define({ table: "bad name", pk: "id" })).toThrow(/invalid table/)
  })

  it("rejects an invalid pk identifier", () => {
    expect(define({ table: "t", pk: "1bad" })).toThrow(/invalid pk/)
  })

  it("rejects defining the same collection twice", () => {
    const r = new Registry().defineCollection({ table: "m", pk: "id" })
    expect(() => r.defineCollection({ table: "m", pk: "id" })).toThrow(/already defined/)
  })
})
