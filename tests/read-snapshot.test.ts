import { env, runInDurableObject } from "cloudflare:test"
import { describe, expect, it } from "vitest"

// WHY (ADR-0011 D1): SSR needs a snapshot + resume cursor out of the DO
// WITHOUT a WebSocket. The cursor must be a DURABLE high-water mark — not
// MAX(_sync_changes.seq), which retention can prune to 0 while the table still
// has rows. A bogus cursor 0 against live rows means a delete landing between
// render and hydration strands a stale row forever (the client can't resume,
// and a fresh snapshot doesn't carry tombstones). These pin: rows+cursor read
// at one position, predicate pushdown, fail-loud on unknown collections, and
// the high-water surviving a pruned-empty changelog.

type SnapshotReq = { collection: string; where?: unknown; orderBy?: unknown; limit?: number }
type SnapshotRes = { rows: Array<Record<string, unknown>>; cursor: string }

function stubFor(room: string): DurableObjectStub {
  return env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
}

/** Call over the binding like an SSR worker would (real RPC, not instance poking). */
async function readSnapshot(room: string, req: SnapshotReq): Promise<SnapshotRes> {
  const stub = stubFor(room) as unknown as { readSnapshot: (r: SnapshotReq) => Promise<SnapshotRes> }
  return stub.readSnapshot(req)
}

describe("readSnapshot RPC (SSR read path, ADR-0011 D1)", () => {
  it("returns current rows and a cursor that resumes past them", async () => {
    const room = `snap-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hi'),('b','yo')")
    })

    const { rows, cursor } = await readSnapshot(room, { collection: "messages" })
    expect(rows.map((r) => r.id).sort()).toEqual(["a", "b"])
    // The cursor covers the snapshot: every change that produced these rows is
    // at or below it, so a client resuming from it re-receives nothing.
    expect(BigInt(cursor)).toBeGreaterThanOrEqual(2n)

    // A later write is ABOVE the cursor — exactly what catch-up will deliver.
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('c','new')")
    })
    const after = await readSnapshot(room, { collection: "messages" })
    expect(BigInt(after.cursor)).toBeGreaterThan(BigInt(cursor))
  })

  it("pushes the where predicate into the read", async () => {
    const room = `snap-where-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','keep'),('b','drop')")
    })
    // The serialized @tanstack/db IR shape a collection's `where` carries.
    const where = { type: "func", name: "gt", args: [{ type: "ref", path: ["id"] }, { type: "val", value: "a" }] }
    const { rows } = await readSnapshot(room, { collection: "messages", where })
    expect(rows.map((r) => r.id)).toEqual(["b"])
  })

  it("throws on an unknown collection (fail loud, not empty-success)", async () => {
    const room = `snap-unknown-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), () => {}) // materialize schema
    await expect(readSnapshot(room, { collection: "nope" })).rejects.toThrow(/unknown collection/)
  })

  it("keeps a durable high-water cursor when retention has pruned the changelog empty", async () => {
    const room = `snap-prune-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hi')")
    })
    const before = await readSnapshot(room, { collection: "messages" })
    expect(BigInt(before.cursor)).toBeGreaterThan(0n)

    // Simulate retention pruning the whole log away (time passing). The drain
    // cursor in _sync_meta is the durable survivor the high-water must use.
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec(
        "INSERT INTO _sync_meta(k,v) VALUES('drain_cursor', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
        String(before.cursor),
      )
      s.storage.sql.exec("DELETE FROM _sync_changes")
    })

    const after = await readSnapshot(room, { collection: "messages" })
    expect(after.rows).toHaveLength(1) // table rows are untouched by retention
    expect(BigInt(after.cursor)).toBeGreaterThanOrEqual(BigInt(before.cursor)) // never regresses to 0
  })
})
