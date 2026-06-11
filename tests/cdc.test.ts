import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import {
  currentSeq,
  getDrainCursor,
  hydrateRows,
  initSchema,
  installTriggers,
  readChangesSince,
  readChangesSinceFor,
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

  // WHY: chunking must not drop or duplicate keys — the batched IN query must
  // return exactly the rows that exist, regardless of how many chunks are needed
  // (ADR-0007 D9: TEXT pk, String(row[pk]) keying must match the changelog key).
  it("hydrateRows returns exactly the existing keys across multiple chunks (no drops, no dups)", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      setup(sql)
      // Seed 150 rows — spans 3 chunks of 64 (64+64+22).
      const ids: string[] = []
      for (let i = 0; i < 150; i++) {
        const id = `row-${String(i).padStart(3, "0")}`
        ids.push(id)
        sql.exec("INSERT INTO items(id,name,n) VALUES(?,?,?)", id, `name-${i}`, i)
      }
      // Request all existing keys plus a handful of nonexistent ones.
      const nonexistent = ["missing-1", "missing-2", "missing-3"]
      const result = hydrateRows(sql, "items", "id", [...ids, ...nonexistent])
      // Exactly the 150 seeded rows — nonexistent keys absent, no dups.
      expect(result.size).toBe(150)
      for (const id of ids) {
        const row = result.get(id)
        expect(row).toBeDefined()
        expect(row!["id"]).toBe(id)
      }
      for (const id of nonexistent) {
        expect(result.has(id)).toBe(false)
      }
    })
  })

  // WHY: readChangesSinceFor must use the (tbl,seq) index path and return only
  // the requested table's rows — interleaved writes to other tables must not
  // appear. A mid-stream cursor must window correctly, matching the per-table
  // contract that emitCatchUp relies on for exactly-once catch-up delivery.
  it("readChangesSinceFor isolates by table and windows by cursor", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      initSchema(sql)
      // Two tables with interleaved writes.
      sql.exec(`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`)
      sql.exec(`CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, name TEXT)`)
      installTriggers(sql, "messages", "id")
      installTriggers(sql, "files", "id")

      sql.exec("INSERT INTO messages(id,body) VALUES('m1','hello')")
      sql.exec("INSERT INTO files(id,name) VALUES('f1','doc.pdf')")
      sql.exec("INSERT INTO messages(id,body) VALUES('m2','world')")
      sql.exec("INSERT INTO files(id,name) VALUES('f2','img.png')")
      sql.exec("INSERT INTO messages(id,body) VALUES('m3','!')")

      // readChangesSinceFor("messages", 0) returns only messages rows, ascending.
      const msgRows = readChangesSinceFor(sql, "messages", 0)
      expect(msgRows.map((r) => r.key)).toEqual(["m1", "m2", "m3"])
      expect(msgRows.every((r) => r.tbl === "messages")).toBe(true)
      // seq values are ascending.
      const seqs = msgRows.map((r) => r.seq)
      expect(seqs).toEqual([...seqs].sort((a, b) => a - b))

      // readChangesSinceFor("files", 0) returns only files rows.
      const fileRows = readChangesSinceFor(sql, "files", 0)
      expect(fileRows.map((r) => r.key)).toEqual(["f1", "f2"])
      expect(fileRows.every((r) => r.tbl === "files")).toBe(true)

      // Mid-stream cursor: only rows after the seq of m2 (the 3rd change overall).
      // seq of m2 is msgRows[1].seq; we want only m3 after that.
      const afterM2 = readChangesSinceFor(sql, "messages", msgRows[1]!.seq)
      expect(afterM2.map((r) => r.key)).toEqual(["m3"])
    })
  })
})
