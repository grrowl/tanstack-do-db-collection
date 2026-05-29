import { describe, expect, it } from "vitest"
import { Registry } from "../src/server/registry.ts"

// WHY: optimistic sync requires the optimistic row id to equal the confirmed
// row id, so keys must be client-supplied and stable — never server-assigned
// (AUTOINCREMENT / INTEGER rowid alias). These tests pin that rule at
// registration so a schema that would break optimism can never reach runtime
// (ADR-0001 D9). Each case asserts a *reason* the schema is rejected, not just
// that some error is thrown.

const define = (def: { table: string; pk: string; ddl: string }) =>
  () => new Registry().defineCollection(def)

describe("Registry.defineCollection - client-key enforcement (D9)", () => {
  it("accepts a column-level TEXT PRIMARY KEY", () => {
    expect(
      define({ table: "messages", pk: "id", ddl: "CREATE TABLE messages (id TEXT PRIMARY KEY, body TEXT)" }),
    ).not.toThrow()
  })

  it("accepts a table-level PRIMARY KEY on a TEXT column", () => {
    expect(
      define({ table: "t", pk: "id", ddl: "CREATE TABLE t (id TEXT, body TEXT, PRIMARY KEY (id))" }),
    ).not.toThrow()
  })

  it("rejects AUTOINCREMENT (server-assigned key breaks id parity)", () => {
    expect(
      define({ table: "t", pk: "id", ddl: "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)" }),
    ).toThrow(/AUTOINCREMENT/i)
  })

  it("rejects an INTEGER rowid-alias primary key", () => {
    expect(
      define({ table: "t", pk: "id", ddl: "CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)" }),
    ).toThrow(/must be TEXT/)
  })

  it("rejects a non-TEXT pk type", () => {
    expect(
      define({ table: "t", pk: "id", ddl: "CREATE TABLE t (id BLOB PRIMARY KEY)" }),
    ).toThrow(/must be TEXT/)
  })

  it("rejects when the pk column is absent from the DDL", () => {
    expect(
      define({ table: "t", pk: "id", ddl: "CREATE TABLE t (other TEXT PRIMARY KEY)" }),
    ).toThrow(/not found/)
  })

  it("rejects a TEXT column that is not declared PRIMARY KEY", () => {
    expect(
      define({ table: "t", pk: "id", ddl: "CREATE TABLE t (id TEXT, body TEXT)" }),
    ).toThrow(/PRIMARY KEY/)
  })

  it("rejects a composite primary key", () => {
    expect(
      define({ table: "t", pk: "id", ddl: "CREATE TABLE t (id TEXT, k TEXT, PRIMARY KEY (id, k))" }),
    ).toThrow(/PRIMARY KEY/)
  })

  it("rejects the reserved _sync_ table prefix", () => {
    expect(
      define({ table: "_sync_x", pk: "id", ddl: "CREATE TABLE _sync_x (id TEXT PRIMARY KEY)" }),
    ).toThrow(/reserved/)
  })

  it("rejects invalid identifiers (no SQL injection into trigger DDL)", () => {
    expect(
      define({ table: "bad name", pk: "id", ddl: "CREATE TABLE x (id TEXT PRIMARY KEY)" }),
    ).toThrow(/invalid table/)
  })

  it("rejects defining the same collection twice", () => {
    const r = new Registry().defineCollection({
      table: "m",
      pk: "id",
      ddl: "CREATE TABLE m (id TEXT PRIMARY KEY)",
    })
    expect(() => r.defineCollection({ table: "m", pk: "id", ddl: "CREATE TABLE m (id TEXT PRIMARY KEY)" })).toThrow(
      /already defined/,
    )
  })
})
