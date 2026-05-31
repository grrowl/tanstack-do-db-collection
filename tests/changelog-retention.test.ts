import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { initSchema, pruneChanges, readChangesSince } from "../src/server/changes.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: `_sync_changes` is bounded only by key-cardinality (compaction), so it
// grows unbounded as keys accumulate. ADR-0009 adds age-based retention: prune
// rows older than `changelogRetentionMs`, and on reconnect, RESET a client whose
// cursor predates the surviving floor so it does a full re-download instead of a
// silent (and now-incorrect) "you're up to date". These pin the prune mechanism,
// its `null` disable, and the reconnect gate — including the edge case C5 (ADR-
// 0002) warned about: a stale reconnect must never be told it's current when the
// changes it missed have been pruned.

// ─── Part A: pruneChanges (direct, like compactChanges) ──────────────────────

const fresh = () => env.TEST_DO.get(env.TEST_DO.idFromName(crypto.randomUUID()))

/** Seed a change row with an explicit ts (bypasses triggers for deterministic
 *  ages — the prune cares only about ts, not how the row got there). */
function seedChange(sql: SqlStorage, key: string, ts: number): void {
  sql.exec("INSERT INTO _sync_changes(tbl,key,op,ts) VALUES('items',?,'insert',?)", key, ts)
}

const keysIn = (sql: SqlStorage) => readChangesSince(sql, 0).map((r) => r.key).sort()

describe("pruneChanges: age-based changelog retention (ADR-0009)", () => {
  const NOW = 1_000_000_000_000
  const WINDOW = 2 * 86_400_000 // 2 days
  const CUTOFF = NOW - WINDOW

  it("deletes rows older than the window and keeps the rest (strict < boundary)", async () => {
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      initSchema(sql)
      seedChange(sql, "old", CUTOFF - 1) // older than cutoff -> pruned
      seedChange(sql, "edge", CUTOFF) //    exactly at cutoff -> kept (ts < cutoff is strict)
      seedChange(sql, "recent", CUTOFF + 1) // within window -> kept
      seedChange(sql, "now", NOW) //          fresh -> kept

      pruneChanges(sql, WINDOW, NOW)

      expect(keysIn(sql)).toEqual(["edge", "now", "recent"])
    })
  })

  it("with retention null, prunes nothing (disable switch)", async () => {
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      initSchema(sql)
      seedChange(sql, "ancient", CUTOFF - 1_000_000)
      seedChange(sql, "now", NOW)

      pruneChanges(sql, null, NOW)

      expect(keysIn(sql)).toEqual(["ancient", "now"]) // both survive
    })
  })

  it("at retention 0, drops everything strictly older than now but keeps a now-stamped row", async () => {
    // Pins the 'can't empty the just-drained rows' property: even the most
    // aggressive retention spares rows stamped at `now` (ts < now is strict).
    await runInDurableObject(fresh(), (_i, s) => {
      const sql = s.storage.sql
      initSchema(sql)
      seedChange(sql, "old", NOW - 1)
      seedChange(sql, "now", NOW)

      pruneChanges(sql, 0, NOW)

      expect(keysIn(sql)).toEqual(["now"])
    })
  })
})

// ─── Part B: the reconnect gate (WS-level, like catchup.test) ────────────────

const codec = createFrameCodec()

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 2000): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))
const committedSeq = (frames: Array<ServerFrame>, txId: string): string =>
  (frames.find((f) => f.t === "committed" && f.txId === txId) as Extract<ServerFrame, { t: "committed" }>).seq
const doFor = (room: string) => env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
const settled = (f: ServerFrame): boolean => f.t === "snap-end" || f.t === "uptodate"
const hasReset = (frames: Array<ServerFrame>, sub: string) => frames.some((f) => f.t === "reset" && f.sub === sub)
const snapKeys = (frames: Array<ServerFrame>, sub: string) =>
  frames.filter((f): f is Extract<ServerFrame, { t: "snap" }> => f.t === "snap" && f.sub === sub).map((f) => f.key)

describe("reconnect gate after retention prune (ADR-0009)", () => {
  it("EMPTY changelog + stale since -> reset + full snapshot, never a silent up-to-date", async () => {
    // The C5 edge: the log was pruned to empty, but the client carries a real
    // cursor. It MUST be reset (and re-snapshotted), not told it's current —
    // otherwise it keeps rows whose deletes were pruned away.
    const room = "ret-empty"
    const ws = await openWs(room)
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(ws, (f) => f.t === "snap-end")

    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "1" } }] })
    const c1 = await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t1")
    const cursor = committedSeq(c1, "t1")

    // Simulate a retention prune that emptied the log (the row's age exceeded the
    // window). The `messages` table still holds 'a' — only the changelog is gone.
    await runInDurableObject(doFor(room), (_i, s) => {
      s.storage.sql.exec("DELETE FROM _sync_changes")
    })

    send(ws, { t: "sub", subId: "s2", collection: "messages", since: cursor })
    const frames = await collectUntil(ws, settled)
    expect(hasReset(frames, "s2")).toBe(true) // reset, not catch-up
    expect(snapKeys(frames, "s2")).toContain("a") // full re-download follows
    ws.close()
  })

  it("since below the surviving floor -> reset + full snapshot", async () => {
    const room = "ret-below-floor"
    const ws = await openWs(room)
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(ws, (f) => f.t === "snap-end")

    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "1" } }] })
    const ca = await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t1")
    const early = committedSeq(ca, "t1")
    send(ws, { t: "mut", txId: "t2", collection: "messages", ops: [{ type: "insert", key: "b", cols: { id: "b", body: "2" } }] })
    await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t2")
    send(ws, { t: "mut", txId: "t3", collection: "messages", ops: [{ type: "insert", key: "c", cols: { id: "c", body: "3" } }] })
    await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t3")

    // Prune everything up to and including 'b': the floor now sits above `early`.
    await runInDurableObject(doFor(room), (_i, s) => {
      s.storage.sql.exec("DELETE FROM _sync_changes WHERE seq <= ?", Number(early) + 1)
    })

    send(ws, { t: "sub", subId: "s2", collection: "messages", since: early })
    const frames = await collectUntil(ws, settled)
    expect(hasReset(frames, "s2")).toBe(true)
    expect(snapKeys(frames, "s2").sort()).toEqual(["a", "b", "c"]) // full snapshot from the live table
    ws.close()
  })

  it("since at the floor-1 boundary (in-window) -> catch-up, no reset", async () => {
    const room = "ret-in-window"
    const ws = await openWs(room)
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(ws, (f) => f.t === "snap-end")

    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "1" } }] })
    const ca = await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t1")
    const sa = committedSeq(ca, "t1")
    send(ws, { t: "mut", txId: "t2", collection: "messages", ops: [{ type: "insert", key: "b", cols: { id: "b", body: "2" } }] })
    await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t2")

    // Prune only the oldest row ('a' @ sa): floor becomes sa+1, so a client at
    // `since = sa` sits exactly at floor-1 and can still be served incrementally.
    await runInDurableObject(doFor(room), (_i, s) => {
      s.storage.sql.exec("DELETE FROM _sync_changes WHERE seq <= ?", Number(sa))
    })

    send(ws, { t: "sub", subId: "s2", collection: "messages", since: sa })
    const frames = await collectUntil(ws, (f) => f.t === "uptodate")
    expect(hasReset(frames, "s2")).toBe(false) // catch-up, not reset
    expect(frames.some((f) => f.t === "snap" && f.sub === "s2")).toBe(false) // no re-snapshot
    const deltas = frames.filter((f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && f.sub === "s2")
    expect(deltas.map((d) => d.key)).toEqual(["b"]) // only the missed change
    ws.close()
  })
})
