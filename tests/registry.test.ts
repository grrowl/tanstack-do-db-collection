import { describe, expect, it } from "vitest"
import { compileSchema, type SyncSchema } from "../src/server/registry.ts"

// WHY: `{table, pk}` are interpolated raw into trigger DDL and SELECTs, so the
// registration path (registerSync -> compileSchema) rejects unsafe identifiers
// and the reserved `_sync_` prefix. The structural D9 rule (the pk is a sole
// TEXT client-supplied key) is now enforced against the REAL table by
// registerSync -> assertSyncCompatible (see assert-sync-compatible.test.ts),
// since the author owns the schema (ADR-0007).
//
// The object-schema API (ADR-0014) keys a collection by its table name, so the
// table IS the object key: `compileSchema` is the guard site, and a one-entry
// schema value is the unit under test. (The old imperative "define the same
// collection twice" case is structurally unrepresentable now — duplicate object
// keys collapse — so that runtime guard can no longer be reached or asserted.)

const compile = (table: string, pk: string) => () =>
  compileSchema({ collections: { [table]: { pk } }, commands: {} } as SyncSchema)

describe("compileSchema — identifier + registration guards", () => {
  it("accepts a valid { table, pk }", () => {
    expect(compile("messages", "id")).not.toThrow()
  })

  it("rejects the reserved _sync_ table prefix", () => {
    expect(compile("_sync_x", "id")).toThrow(/reserved/)
  })

  it("rejects an invalid table identifier (no SQL injection into trigger DDL)", () => {
    expect(compile("bad name", "id")).toThrow(/invalid table/)
  })

  it("rejects an invalid pk identifier", () => {
    expect(compile("t", "1bad")).toThrow(/invalid pk/)
  })
})
