import { env, runInDurableObject } from "cloudflare:test"
import type { SqlStorage } from "@cloudflare/workers-types"
import { describe, expect, it } from "vitest"
import { compactChanges, currentSeq, initSchema, installTriggers, readChangesSince } from "../src/server/changes.ts"
import { sweepDedup } from "../src/server/dedup.ts"

// WHY: compaction bounds the change log without breaking sync. It must collapse
// to the latest op per key (so storage is ~distinct keys, not total writes),
// preserve each key's LATEST including delete tombstones (so reconnect catch-up
// stays correct for any cursor), and keep the cursor monotonic. Dedup GC must
// be time-based and independent of the changelog (ADR-0002 C5).

const fresh = () => env.TEST_DO.get(env.TEST_DO.idFromName(crypto.randomUUID()))

function setup(sql: SqlStorage): void {
  initSchema(sql)
  sql.exec(`CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, n INTEGER)`)
  installTriggers(sql, "items", "id")
}

const summary = (sql: SqlStorage) => readChangesSince(sql, 0).map((r) => `${r.key}@${r.seq}:${r.op}`).sort()

describe("compaction + dedup GC (M7)", () => {
  it("collapses superseded changes to the latest op per key, keeping the cursor", async () => {
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      setup(sql)
      sql.exec("INSERT INTO items(id,n) VALUES('a',1)") // seq1
      sql.exec("UPDATE items SET n=2 WHERE id='a'") // seq2
      sql.exec("UPDATE items SET n=3 WHERE id='a'") // seq3
      expect(readChangesSince(sql, 0).length).toBe(3)

      compactChanges(sql)
      expect(summary(sql)).toEqual(["a@3:update"])
      expect(currentSeq(sql)).toBe(3) // MAX preserved -> cursor stays monotonic
    })
  })

  it("keeps the latest row per distinct key", async () => {
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      setup(sql)
      sql.exec("INSERT INTO items(id,n) VALUES('a',1)") // 1
      sql.exec("INSERT INTO items(id,n) VALUES('b',1)") // 2
      sql.exec("UPDATE items SET n=2 WHERE id='a'") // 3
      compactChanges(sql)
      expect(summary(sql)).toEqual(["a@3:update", "b@2:insert"])
    })
  })

  it("retains a delete tombstone as the latest op", async () => {
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      setup(sql)
      sql.exec("INSERT INTO items(id,n) VALUES('a',1)") // 1
      sql.exec("DELETE FROM items WHERE id='a'") // 2
      compactChanges(sql)
      expect(summary(sql)).toEqual(["a@2:delete"])
    })
  })

  it("keeps reconnect catch-up correct after compaction", async () => {
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      setup(sql)
      sql.exec("INSERT INTO items(id,n) VALUES('a',1)") // 1
      sql.exec("UPDATE items SET n=2 WHERE id='a'") // 2
      compactChanges(sql) // a collapsed to seq2
      // A client caught up to seq1 reconnects: changes>1 still yields a's latest.
      expect(readChangesSince(sql, 1).map((r) => `${r.key}@${r.seq}`)).toEqual(["a@2"])
    })
  })

  it("sweepDedup removes entries older than the retention window, keeps recent", async () => {
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      setup(sql)
      const now = 1_000_000_000
      const ins = (tx: string, ts: number) =>
        sql.exec("INSERT INTO _sync_seen_tx(tx_id,ok,cursor,error,result,ts) VALUES(?,1,'1',null,null,?)", tx, ts)
      ins("old", now - 7_200_000) // 2h old
      ins("recent", now - 60_000) // 1m old
      sweepDedup(sql, 3_600_000, now) // 1h retention
      expect(Array.from(sql.exec<{ tx_id: string }>("SELECT tx_id FROM _sync_seen_tx")).map((r) => r.tx_id)).toEqual([
        "recent",
      ])
    })
  })
})
