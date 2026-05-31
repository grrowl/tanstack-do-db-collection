import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { currentSeq, ensureTriggers, initSchema, installTriggers, readChangesSince } from "../src/server/changes.ts"

// WHY: registerSync reconciles the trigger set to the registered collections
// (ADR-0008). It must DROP triggers for a collection the author removed, or an
// orphaned table keeps firing capture triggers into `_sync_changes` forever -
// log churn for a stream no one consumes, and a "still synced?" correctness
// smell. These pin that the reap is real (orphan triggers gone, orphan writes
// captured nothing) AND surgical (a kept collection is untouched).

const freshStub = () => env.TEST_DO.get(env.TEST_DO.idFromName(crypto.randomUUID()))

/** Names of OUR triggers currently installed, sorted. */
function syncTriggers(sql: SqlStorage): Array<string> {
  return Array.from(
    sql.exec<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='trigger' AND name GLOB '_sync_changes_*' ORDER BY name",
    ),
  ).map((r) => r.name)
}

describe("ensureTriggers: reconcile capture triggers to the registry (ADR-0008)", () => {
  it("drops triggers for a removed collection; the kept one is untouched", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      initSchema(sql)
      sql.exec(`CREATE TABLE a (id TEXT PRIMARY KEY, v TEXT)`)
      sql.exec(`CREATE TABLE b (id TEXT PRIMARY KEY, v TEXT)`)

      // Register A + B, then write both: both capture.
      ensureTriggers(sql, [{ table: "a", pk: "id" }, { table: "b", pk: "id" }])
      expect(syncTriggers(sql)).toEqual([
        "_sync_changes_a_ad", "_sync_changes_a_ai", "_sync_changes_a_au",
        "_sync_changes_b_ad", "_sync_changes_b_ai", "_sync_changes_b_au",
      ])
      sql.exec("INSERT INTO a(id,v) VALUES('a1','x')")
      sql.exec("INSERT INTO b(id,v) VALUES('b1','x')")
      const seqAfterBoth = currentSeq(sql)
      expect(seqAfterBoth).toBe(2)

      // Re-register A only: B's triggers must be gone, A's intact.
      ensureTriggers(sql, [{ table: "a", pk: "id" }])
      expect(syncTriggers(sql)).toEqual([
        "_sync_changes_a_ad", "_sync_changes_a_ai", "_sync_changes_a_au",
      ])

      // A write to the orphaned table captures NOTHING; a write to A still does.
      sql.exec("INSERT INTO b(id,v) VALUES('b2','x')")
      expect(currentSeq(sql)).toBe(seqAfterBoth) // b2 logged nothing
      sql.exec("INSERT INTO a(id,v) VALUES('a2','x')")
      expect(readChangesSince(sql, seqAfterBoth).map((r) => `${r.tbl}:${r.key}`)).toEqual(["a:a2"])
    })
  })

  it("is idempotent: re-running with the same registry reaps nothing", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      initSchema(sql)
      sql.exec(`CREATE TABLE a (id TEXT PRIMARY KEY, v TEXT)`)
      ensureTriggers(sql, [{ table: "a", pk: "id" }])
      const before = syncTriggers(sql)
      ensureTriggers(sql, [{ table: "a", pk: "id" }])
      ensureTriggers(sql, [{ table: "a", pk: "id" }])
      expect(syncTriggers(sql)).toEqual(before)
    })
  })

  // WHY (rename): `ALTER TABLE … RENAME` is the second way orphan triggers
  // arise, and a nastier one than removal — it's a *correctness* bug, not just
  // bloat. SQLite keeps a table's triggers attached across a rename (same
  // trigger NAME, now firing on the new table), but our trigger body hardcodes
  // the original table name as a string literal in the change row. So after a
  // rename + re-register, an add-only registerSync would leave the old-named
  // trigger firing alongside the freshly-installed one: every write to the
  // renamed table logs TWICE, one row mislabelled with the old table name. The
  // reconcile reaps the stale trigger by name. The first test pins the SQLite
  // behaviour we depend on; the second proves the reconcile fixes the dup.

  it("PROBE: a renamed table keeps its old-named trigger, firing + mislabelling (the hazard)", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      initSchema(sql)
      sql.exec(`CREATE TABLE b (id TEXT PRIMARY KEY, v TEXT)`)
      ensureTriggers(sql, [{ table: "b", pk: "id" }])

      sql.exec(`ALTER TABLE b RENAME TO b2`)
      // The trigger moved with the table: its NAME is still `_sync_changes_b_*`,
      // but it now fires on b2. (Pins the SQLite semantics the reap relies on.)
      expect(syncTriggers(sql)).toEqual([
        "_sync_changes_b_ad", "_sync_changes_b_ai", "_sync_changes_b_au",
      ])

      // Simulate the OLD add-only registerSync: install b2's triggers without
      // reaping. Now b2 carries both trigger sets.
      installTriggers(sql, "b2", "id")
      const seq0 = currentSeq(sql)
      sql.exec("INSERT INTO b2(id,v) VALUES('x','1')")

      // The bug made visible: ONE insert produces TWO change rows — the stale
      // `_sync_changes_b_*` logs the row mislabelled as table 'b', the new one
      // as 'b2'.
      const rows = readChangesSince(sql, seq0)
      expect(rows.map((r) => `${r.tbl}:${r.key}`).sort()).toEqual(["b2:x", "b:x"])
    })
  })

  it("reconcile after a rename reaps the old-named trigger: the renamed table captures exactly once", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      initSchema(sql)
      sql.exec(`CREATE TABLE b (id TEXT PRIMARY KEY, v TEXT)`)
      ensureTriggers(sql, [{ table: "b", pk: "id" }])

      // The real deploy: rename the table, re-register under the new name. One
      // ensureTriggers call installs b2's triggers AND reaps the stale b ones.
      sql.exec(`ALTER TABLE b RENAME TO b2`)
      ensureTriggers(sql, [{ table: "b2", pk: "id" }])
      expect(syncTriggers(sql)).toEqual([
        "_sync_changes_b2_ad", "_sync_changes_b2_ai", "_sync_changes_b2_au",
      ])

      const seq0 = currentSeq(sql)
      sql.exec("INSERT INTO b2(id,v) VALUES('x','1')")
      expect(readChangesSince(sql, seq0).map((r) => `${r.tbl}:${r.key}`)).toEqual(["b2:x"])
    })
  })

  it("only ever touches OUR namespace, never an author's trigger", async () => {
    await runInDurableObject(freshStub(), (_i, state) => {
      const sql = state.storage.sql
      initSchema(sql)
      sql.exec(`CREATE TABLE a (id TEXT PRIMARY KEY, v TEXT, n INTEGER)`)
      // An author trigger that does NOT match our `_sync_changes_*` namespace.
      sql.exec(`CREATE TRIGGER author_guard AFTER INSERT ON a BEGIN
                  UPDATE a SET n = 1 WHERE id = NEW.id;
                END`)
      ensureTriggers(sql, [{ table: "a", pk: "id" }])
      // Reaping with an empty registry drops our triggers but spares the author's.
      ensureTriggers(sql, [])
      expect(syncTriggers(sql)).toEqual([])
      expect(
        Array.from(
          sql.exec<{ name: string }>(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name='author_guard'",
          ),
        ),
      ).toHaveLength(1)
    })
  })
})
