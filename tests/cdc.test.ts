import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import {
  currentSeq,
  getDrainCursor,
  initSchema,
  installTriggers,
  readChangesSince,
  setDrainCursor,
} from "../src/server/changes.ts"

// WHY: `_sync_changes` is the one ordered stream the whole design rests on. If
// a write fails to log, clients miss the change; if a log row survives a
// rolled-back write, clients see a phantom. These tests pin both directions -
// capture and atomicity - not merely "a row appeared".

const freshStub = () => env.TEST_DO.get(env.TEST_DO.idFromName(crypto.randomUUID()))

/** Install framework schema + a user `items` table with capture triggers. */
function setup(sql: SqlStorage): void {
  initSchema(sql)
  sql.exec(`CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, name TEXT, n INTEGER)`)
  installTriggers(sql, "items", "id")
}

describe("CDC: AFTER triggers -> _sync_changes (D12)", () => {
  it("logs an insert with the row key and advances the cursor", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      setup(sql)
      expect(currentSeq(sql)).toBe(0)
      sql.exec("INSERT INTO items(id,name,n) VALUES('a','alice',1)")
      expect(
        Array.from(sql.exec<{ tbl: string; key: string; op: string }>("SELECT tbl,key,op FROM _sync_changes")),
      ).toEqual([{ tbl: "items", key: "a", op: "insert" }])
      expect(currentSeq(sql)).toBe(1)
    })
  })

  it("logs update and delete with the correct op and key", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      setup(sql)
      sql.exec("INSERT INTO items(id,name,n) VALUES('a','alice',1)")
      sql.exec("UPDATE items SET n=2 WHERE id='a'")
      sql.exec("DELETE FROM items WHERE id='a'")
      expect(
        Array.from(sql.exec<{ op: string; key: string }>("SELECT op,key FROM _sync_changes ORDER BY seq")),
      ).toEqual([
        { op: "insert", key: "a" },
        { op: "update", key: "a" },
        { op: "delete", key: "a" },
      ])
    })
  })

  it("readChangesSince returns only rows after the cursor, in seq order", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      setup(sql)
      sql.exec("INSERT INTO items(id,name,n) VALUES('a','a',1)")
      sql.exec("INSERT INTO items(id,name,n) VALUES('b','b',1)")
      expect(readChangesSince(sql, 0).map((r) => r.key)).toEqual(["a", "b"])
      expect(readChangesSince(sql, 1).map((r) => r.key)).toEqual(["b"])
      expect(readChangesSince(sql, 2)).toEqual([])
    })
  })

  it("drain cursor persists and round-trips", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      setup(sql)
      expect(getDrainCursor(sql)).toBe(0)
      setDrainCursor(sql, 5)
      expect(getDrainCursor(sql)).toBe(5)
      setDrainCursor(sql, 9)
      expect(getDrainCursor(sql)).toBe(9)
    })
  })

  it("rolls back the change-log row atomically with a failed write", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      setup(sql)
      sql.exec("INSERT INTO items(id,name,n) VALUES('a','a',1)")
      expect(currentSeq(sql)).toBe(1)

      // A transaction that writes then throws must leave NO trace - neither the
      // user row nor the trigger's _sync_changes row. The trigger INSERT shares
      // the transactionSync SAVEPOINT, so both roll back together (ADR-0001 D12).
      expect(() =>
        state.storage.transactionSync(() => {
          sql.exec("INSERT INTO items(id,name,n) VALUES('b','b',1)")
          throw new Error("boom")
        }),
      ).toThrow("boom")

      expect(Array.from(sql.exec<{ id: string }>("SELECT id FROM items")).map((r) => r.id)).toEqual(["a"])
      expect(currentSeq(sql)).toBe(1) // no orphaned change-log row
    })
  })

  it("seq is strictly monotonic and unique across many writes", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      setup(sql)
      for (let i = 0; i < 10; i++) {
        sql.exec("INSERT INTO items(id,name,n) VALUES(?,?,?)", `k${i}`, "x", i)
      }
      const seqs = readChangesSince(sql, 0).map((r) => r.seq)
      expect(seqs).toEqual([...seqs].sort((a, b) => a - b))
      expect(new Set(seqs).size).toBe(10)
    })
  })
})
