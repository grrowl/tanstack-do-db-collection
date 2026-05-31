import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { assertSyncCompatible, hydrateRows, initSchema, installTriggers, readChangesSince } from "../src/server/changes.ts"

// WHY: ADR-0007 makes the author own schema + migrations and only validates the
// real table at registerSync. Two promises ride on that and are easy to assert
// hollowly, so pin them against actual SQLite (in workerd — no Wrangler needed,
// since these drive a real DO's storage directly):
//   1. A migrate -> register -> migrate-again -> register-again cycle works, and
//      a column ADDED later flows to clients with NO trigger change — because the
//      CDC triggers capture only (key, op) and rows are hydrated fresh at drain.
//   2. The pk check is TEXT *affinity* (TEXT/VARCHAR/CHAR/…), not the literal
//      string "TEXT" — but INTEGER keys (rowid alias, breaks id parity) are out.

// Run a closure against a fresh DO's raw SqlStorage.
function inDO<T>(name: string, fn: (sql: SqlStorage) => T): Promise<T> {
  const stub = env.TEST_DO.get(env.TEST_DO.idFromName(name))
  return runInDurableObject(stub, (_i, s) => fn(s.storage.sql))
}

describe("author-owned schema evolution (ADR-0007)", () => {
  it("survives migrate -> register -> migrate -> register; a new column flows without touching triggers", async () => {
    await inDO("evolve", (sql) => {
      initSchema(sql)

      // --- deploy v1: author migrates, then registers sync ---
      sql.exec(`CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT)`)
      assertSyncCompatible(sql, "items", "id")
      installTriggers(sql, "items", "id")
      sql.exec(`INSERT INTO items(id, name) VALUES ('a', 'one')`)

      expect(readChangesSince(sql, 0).map((c) => [c.key, c.op])).toEqual([["a", "insert"]])

      // --- deploy v2: author runs a migration adding a column, re-registers ---
      // (simulates the next wake/deploy: registerSync re-runs every construction)
      sql.exec(`ALTER TABLE items ADD COLUMN status TEXT`)
      assertSyncCompatible(sql, "items", "id") // re-validate: still compatible
      installTriggers(sql, "items", "id") // idempotent (CREATE TRIGGER IF NOT EXISTS)

      // A write touching the NEW column is captured by the UNCHANGED trigger,
      // which only ever referenced NEW.id — proof the capture is column-agnostic.
      sql.exec(`UPDATE items SET status = 'done' WHERE id = 'a'`)
      expect(readChangesSince(sql, 0).map((c) => c.op)).toEqual(["insert", "update"])

      // And late hydration reads the live row, so the new column is present in
      // what gets broadcast — schema evolution flows end-to-end, no trigger churn.
      expect(hydrateRows(sql, "items", "id", ["a"]).get("a")).toMatchObject({
        id: "a",
        name: "one",
        status: "done",
      })
    })
  })

  it("triggers installed once stay singular across repeated registration (idempotent)", async () => {
    await inDO("idempotent", (sql) => {
      initSchema(sql)
      sql.exec(`CREATE TABLE t (id TEXT PRIMARY KEY, v TEXT)`)
      for (let i = 0; i < 3; i++) installTriggers(sql, "t", "id") // re-register on every wake

      const triggers = Array.from(
        sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='t'"),
      )
      expect(triggers.length).toBe(3) // one each for insert/update/delete — not 9
      // And a single write still yields exactly one change row (no duplicate capture).
      sql.exec(`INSERT INTO t(id, v) VALUES ('x', '1')`)
      expect(readChangesSince(sql, 0).length).toBe(1)
    })
  })

  it("accepts any TEXT-affinity pk (VARCHAR/CHAR) and rejects INTEGER keys", async () => {
    await inDO("affinity", (sql) => {
      sql.exec(`CREATE TABLE v (id VARCHAR PRIMARY KEY)`) // TEXT affinity
      expect(() => assertSyncCompatible(sql, "v", "id")).not.toThrow()

      sql.exec(`CREATE TABLE c (id CHARACTER PRIMARY KEY)`) // TEXT affinity
      expect(() => assertSyncCompatible(sql, "c", "id")).not.toThrow()

      sql.exec(`CREATE TABLE i (id INTEGER PRIMARY KEY)`) // INTEGER affinity — forbidden
      expect(() => assertSyncCompatible(sql, "i", "id")).toThrow(/TEXT affinity/)

      sql.exec(`CREATE TABLE n (id REAL PRIMARY KEY)`) // numeric — forbidden
      expect(() => assertSyncCompatible(sql, "n", "id")).toThrow(/TEXT affinity/)
    })
  })
})
