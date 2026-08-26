import { createCollection } from "@tanstack/db"
import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions, type WebSocketLike, WebSocketTransport } from "../src/client/index.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: server-originated writes (an agent inserting a row, a webhook, a cron
// job, a bulk seed) live outside the client mutation flow — no txId, no receipt.
// `runSyncedWrite` is the sanctioned primitive: apply a raw write in a
// transaction, then broadcast the resulting CDC to connected clients (ADR-0006).
// A raw `sql.exec` without it fires the triggers but never broadcasts until some
// later mutation drains the backlog.

// runSyncedWrite is protected (subclass-facing); reach it in the test via the
// in-DO instance. registerSync already ran in the DO constructor (ADR-0007).
// `drainAndBroadcast` is the documented manual-drain trigger (ADR-0006), re-aliased
// protected on SyncDurableObject — reached the same way to prove the dark half.
type ServerApi = {
  runSyncedWrite: <T>(fn: (sql: SqlStorage) => T) => T
  drainAndBroadcast: () => void
}
const api = (i: unknown): ServerApi => i as unknown as ServerApi

const codec = createFrameCodec()

function realTransport(room: string): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

// A raw codec-level socket, for the wire-framing assertion (SW-D) that the
// collection abstraction hides: the batch's delta/`uptodate` boundary and seq.
async function openRawWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

/** Record every frame arriving on `ws` from now on. */
function recordFrames(ws: WebSocket): Array<ServerFrame> {
  const out: Array<ServerFrame> = []
  ws.addEventListener("message", (e: MessageEvent) => out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame))
  return out
}

/** Subscribe and await `snap-end`, so a later `recordFrames` sees only the
 *  frames the write under test produces (the empty snapshot is drained first).
 *  The listener is attached BEFORE the `sub` is sent, so a fast `snap-end` can't
 *  slip through in the gap. */
async function subscribeRaw(ws: WebSocket, subId: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("no snap-end")), 2000)
    const onMsg = (e: MessageEvent): void => {
      if ((codec.decode(e.data as ArrayBuffer) as ServerFrame).t === "snap-end") {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve()
      }
    }
    ws.addEventListener("message", onMsg)
    ws.send(codec.encode({ t: "sub", subId, collection: "messages" } satisfies ClientFrame))
  })
}

describe("runSyncedWrite (ADR-0006) — server-originated writes", () => {
  it("broadcasts a server-originated insert to a connected client", async () => {
    const room = "rsw-live"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const t = realTransport(room)
    await t.connect() // constructing the DO already ran registerSync (ADR-0007)
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    await messages.preload()
    expect(messages.size).toBe(0)

    // An agent (server-side) inserts a message.
    await runInDurableObject(stub, (instance) => {
      api(instance).runSyncedWrite((sql) => sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", "agent1", "hi"))
    })

    await waitFor(() => messages.get("agent1") !== undefined)
    expect(messages.get("agent1")).toMatchObject({ id: "agent1", body: "hi" })
    t.close()
  })

  it("a Drizzle-style direct ctx.storage.sql.exec inside runSyncedWrite still broadcasts (and the handle IS ctx.storage.sql)", async () => {
    // A Drizzle durable-object driver writes through ctx.storage.sql directly and
    // ignores the handle runSyncedWrite passes. The write must still fire the CDC
    // triggers and broadcast, because runSyncedWrite's handle IS ctx.storage.sql —
    // the same connection, no wrapper (the fold of the retired handle-identity pin).
    const room = "rsw-direct"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const t = realTransport(room)
    await t.connect()
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    await messages.preload()

    await runInDurableObject(stub, (instance, state) => {
      api(instance).runSyncedWrite((sql) => {
        expect(sql).toBe(state.storage.sql) // folded: same handle, so a direct exec is captured too
        state.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", "drz1", "via-ctx-storage")
      })
    })

    await waitFor(() => messages.get("drz1") !== undefined)
    expect(messages.get("drz1")).toMatchObject({ id: "drz1", body: "via-ctx-storage" })
    t.close()
  })

  it("a write OUTSIDE runSyncedWrite is trigger-captured but NOT broadcast until the next drain", async () => {
    // ADR-0006's dark half: a raw sql.exec fires the CDC triggers (the change
    // lands in _sync_changes) but is invisible to clients until some later drain
    // flushes the backlog. The suite states this invariant; this asserts it —
    // capture happens, broadcast does not, until drainAndBroadcast is called.
    const room = "rsw-outside"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const t = realTransport(room)
    await t.connect()
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    await messages.preload()

    const captured = await runInDurableObject(stub, (_instance, state) => {
      state.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", "raw1", "unsynced")
      return Array.from(state.storage.sql.exec("SELECT key, op FROM _sync_changes WHERE key='raw1'")) as Array<{
        key: string
        op: string
      }>
    })
    expect(captured).toEqual([{ key: "raw1", op: "insert" }]) // trigger fired: captured…

    // …but nothing broadcasts it: the client stays empty across several coalescer
    // ticks (tickMs=50) — no drain path runs, so it is never enqueued.
    await new Promise((r) => setTimeout(r, 300))
    expect(messages.get("raw1")).toBeUndefined()

    // A manual drainAndBroadcast (as any later mutation would) delivers the backlog.
    await runInDurableObject(stub, (instance) => api(instance).drainAndBroadcast())
    await waitFor(() => messages.get("raw1") !== undefined)
    expect(messages.get("raw1")).toMatchObject({ id: "raw1", body: "unsynced" })
    t.close()
  })

  it("a multi-statement runSyncedWrite reaches the client as one batch: all deltas, then a single uptodate at one seq", async () => {
    // The whole write commits and broadcasts atomically: every delta precedes a
    // single `uptodate` boundary, and all share one seq (one stream position). A
    // key touched twice in the batch (m2: insert then update) coalesces to one
    // delta carrying its latest value. Asserted at the wire, which the collection hides.
    const room = "rsw-batch"
    const ws = await openRawWs(room)
    await subscribeRaw(ws, "s1")
    const frames = recordFrames(ws)

    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, (instance) => {
      api(instance).runSyncedWrite((sql) => {
        sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", "m1", "one")
        sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", "m2", "two")
        sql.exec("UPDATE messages SET body=? WHERE id=?", "two!", "m2")
        sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", "m3", "three")
      })
    })

    await waitFor(() => frames.some((f) => f.t === "uptodate"))
    await new Promise((r) => setTimeout(r, 100)) // settle: catch any (wrong) trailing frame
    const kinds = frames.map((f) => f.t)
    // Exactly one uptodate, and it is the LAST frame — every delta precedes it.
    expect(kinds.filter((k) => k === "uptodate")).toHaveLength(1)
    expect(kinds[kinds.length - 1]).toBe("uptodate")

    const deltas = frames.filter((f) => f.t === "d") as Array<Extract<ServerFrame, { t: "d" }>>
    // Exactly one delta per key — m2's insert+update collapsed to one (not two).
    expect(deltas).toHaveLength(3)
    expect(new Set(deltas.map((d) => d.key))).toEqual(new Set(["m1", "m2", "m3"]))
    const m2 = deltas.find((d) => d.key === "m2")!
    expect((m2.cols as { body: string }).body).toBe("two!")
    // All deltas carry the single batch seq == the uptodate seq (one position).
    const up = frames.find((f) => f.t === "uptodate") as Extract<ServerFrame, { t: "uptodate" }>
    for (const d of deltas) expect(d.seq).toBe(up.seq)
    ws.close()
  })

  it("a write to an idle DO (no subscribers) reaches a later client via snapshot", async () => {
    const room = "rsw-idle"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    // No client connected. The DO registered at construction (ADR-0007); the agent writes.
    await runInDurableObject(stub, (instance) => {
      api(instance).runSyncedWrite((sql) => sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", "agent2", "queued"))
    })

    const t = realTransport(room)
    await t.connect()
    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (m) => m.id }))
    await messages.preload()
    await waitFor(() => messages.get("agent2") !== undefined)
    expect(messages.get("agent2")).toMatchObject({ id: "agent2", body: "queued" })
    t.close()
  })

  it("returns the closure's value", async () => {
    const room = "rsw-ret"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    const n = await runInDurableObject(stub, (instance) => {
      return api(instance).runSyncedWrite((sql) => {
        sql.exec("INSERT INTO messages(id,body) VALUES('r1','a'),('r2','b')")
        return Array.from(sql.exec("SELECT count(*) AS c FROM messages"))[0]!.c as number
      })
    })
    expect(n).toBe(2)
  })

  it("rejects an async (thenable-returning) closure and rolls back", async () => {
    const room = "rsw-async"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await expect(
      runInDurableObject(stub, (instance) => {
        api(instance).runSyncedWrite((sql) => {
          sql.exec("INSERT INTO messages(id,body) VALUES('x','x')")
          return Promise.resolve() // illegal — must be synchronous
        })
      }),
    ).rejects.toThrow(/synchronous/)
    // The insert rolled back with the transaction.
    const count = await runInDurableObject(stub, (instance, s) => {
      return Array.from(s.storage.sql.exec("SELECT count(*) AS c FROM messages"))[0]!.c as number
    })
    expect(count).toBe(0)
  })
})
