import { and, createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { ClientFrame } from "../src/wire/frames.ts"

// WHY: on-demand loads ONLY the subsets live queries request, instead of
// syncing the whole collection. These pin: distinct `where`s become distinct
// refcounted server subs (shared + released correctly), a live query loads only
// its subset (other rows stay absent), and a write outside every loaded subset
// is confirmed without stranding an optimistic phantom.

interface Msg {
  id: string
  body: string
}

const whereFunc = (name: string, field: string, value: unknown): unknown => ({
  type: "func",
  name,
  args: [
    { type: "ref", path: [field] },
    { type: "val", value },
  ],
})
const whereEq = (field: string, value: unknown): unknown => whereFunc("eq", field, value)

const spyControls = () => {
  const calls: Array<[string, ...Array<unknown>]> = []
  return {
    calls,
    controls: {
      collection: {},
      begin: () => calls.push(["begin"]),
      write: (m: unknown) => calls.push(["write", m]),
      commit: () => calls.push(["commit"]),
      markReady: () => calls.push(["markReady"]),
      truncate: () => calls.push(["truncate"]),
    },
  }
}

type FetchFrame = Extract<ClientFrame, { t: "fetch" }>

// A fake transport that records sub/unsub and immediately completes each
// subscription's (empty) snapshot, for deterministic refcount tests. `fetch`
// records the issued frame and returns canned rows keyed by the fetchId suffix
// (`-ties` / `-next`) so cursor load-more's double request can be asserted.
function fakeTransport(rows: { ties?: Array<unknown>; next?: Array<unknown> } = {}) {
  const subs: Array<{ subId: string; where: unknown }> = []
  const unsubs: Array<string> = []
  const fetches: Array<FetchFrame> = []
  const transport = {
    connect: async () => {},
    subscribe: async (subId: string, _table: string, handler: SubHandler, where?: unknown) => {
      subs.push({ subId, where })
      handler.onSnapEnd() // empty snapshot -> resolves loadSubset
    },
    unsubscribe: (subId: string) => unsubs.push(subId),
    fetch: async (frame: FetchFrame) => {
      fetches.push(frame)
      if (frame.fetchId.endsWith("-ties")) return rows.ties ?? []
      if (frame.fetchId.endsWith("-next")) return rows.next ?? []
      return []
    },
    sendMut: async () => ({}),
    close: () => {},
  } as unknown as WebSocketTransport
  return { subs, unsubs, fetches, transport }
}

interface LoadOpts {
  where?: unknown
  orderBy?: unknown
  limit?: number
  cursor?: { whereFrom: unknown; whereCurrent: unknown; lastKey?: unknown }
}
type OnDemand = {
  loadSubset: (o: LoadOpts) => true | Promise<void>
  unloadSubset: (o: LoadOpts) => void
}
const startOnDemand = (transport: WebSocketTransport) => {
  const { calls, controls } = spyControls()
  const adapter = doCollectionOptions<Msg>({ transport, table: "messages", getKey: (r) => r.id, syncMode: "on-demand" })
  const res = (adapter as unknown as { sync: { sync: (p: unknown) => OnDemand } }).sync.sync(controls)
  return { calls, res }
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("on-demand loadSubset (M11) — refcounting", () => {
  it("shares one server sub per distinct where and releases it on the last unload", async () => {
    const { subs, unsubs, transport } = fakeTransport()
    const { res } = startOnDemand(transport)
    const w = { where: whereEq("body", "x") }

    await res.loadSubset(w)
    await res.loadSubset(w) // same where -> no new sub
    expect(subs.length).toBe(1)

    res.unloadSubset(w) // refs 2 -> 1
    expect(unsubs.length).toBe(0)
    res.unloadSubset(w) // refs 1 -> 0 -> unsubscribe
    expect(unsubs).toEqual([subs[0]!.subId])
  })

  it("creates distinct subs for distinct wheres", async () => {
    const { subs, transport } = fakeTransport()
    const { res } = startOnDemand(transport)
    await res.loadSubset({ where: whereEq("body", "x") })
    await res.loadSubset({ where: whereEq("body", "y") })
    expect(subs.map((s) => s.where)).toEqual([whereEq("body", "x"), whereEq("body", "y")])
    expect(subs.length).toBe(2)
  })
})

describe("on-demand loadSubset (M12) — cursor load-more (scroll-back)", () => {
  const cursorOrderBy = [{ expression: { type: "ref", path: ["body"] }, compareOptions: { direction: "desc" } }]

  it("issues the double request (ties no-limit + next with-limit) and merges both pages", async () => {
    const tiesRows = [{ id: "t1", body: "16" }]
    const nextRows = [
      { id: "n1", body: "15" },
      { id: "n2", body: "14" },
    ]
    const { fetches, subs, transport } = fakeTransport({ ties: tiesRows, next: nextRows })
    const { calls, res } = startOnDemand(transport)

    const base = whereEq("room", "r1")
    const cursor = { whereFrom: whereFunc("lt", "body", "16"), whereCurrent: whereEq("body", "16"), lastKey: "t1" }
    await res.loadSubset({ where: base, orderBy: cursorOrderBy, limit: 5, cursor })

    // A cursor load is a one-shot fetch — it registers no live subscription.
    expect(subs.length).toBe(0)
    // Two requests: ties (boundary equals, NO limit) and next page (WITH limit).
    expect(fetches.length).toBe(2)
    const ties = fetches.find((f) => f.fetchId.endsWith("-ties"))!
    const next = fetches.find((f) => f.fetchId.endsWith("-next"))!
    expect(ties.limit).toBeUndefined()
    expect(next.limit).toBe(5)
    // Each cursor expression is combined with the base `where` (which the cursor
    // expressions deliberately exclude) via and().
    expect(ties.where).toEqual(and(base as never, cursor.whereCurrent as never))
    expect(next.where).toEqual(and(base as never, cursor.whereFrom as never))
    // Both pages land in the collection as inserts: ties first, then next page.
    const writes = calls.filter((c) => c[0] === "write").map((c) => (c[1] as { value: unknown }).value)
    expect(writes).toEqual([...tiesRows, ...nextRows])
  })

  it("takes no live refcount, so a sibling query's sub survives its unload", async () => {
    const { subs, unsubs, transport } = fakeTransport()
    const { res } = startOnDemand(transport)
    const base = whereEq("room", "r1")

    await res.loadSubset({ where: base }) // initial live sub, refs = 1
    expect(subs.length).toBe(1)

    const cursor = { whereFrom: whereFunc("lt", "body", "16"), whereCurrent: whereEq("body", "16") }
    await res.loadSubset({ where: base, orderBy: cursorOrderBy, limit: 5, cursor }) // one-shot
    expect(subs.length).toBe(1) // no new sub

    // The framework unloads EVERY loadedSubset, cursor loads included. A cursor
    // unload must be a no-op or it would under-count the still-live base sub.
    res.unloadSubset({ where: base, orderBy: cursorOrderBy, limit: 5, cursor })
    expect(unsubs.length).toBe(0)
    res.unloadSubset({ where: base }) // releases the live sub
    expect(unsubs).toEqual([subs[0]!.subId])
  })
})

describe("on-demand loadSubset (M11) — against the DO", () => {
  function realTransport(room: string): WebSocketTransport {
    return new WebSocketTransport({
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

  it("a live query loads only its subset; unqueried rows stay absent", async () => {
    const room = "od-subset"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','keep'),('b','drop')")
    })

    const messages = createCollection(
      doCollectionOptions<Msg>({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload() // ready, but empty — nothing synced eagerly
    expect(messages.size).toBe(0)

    // A live query's where is pushed to loadSubset -> only 'keep' rows load.
    const kept = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    await kept.preload()

    await waitFor(() => kept.get("a") !== undefined)
    expect(kept.get("a")).toMatchObject({ id: "a", body: "keep" })
    expect(messages.get("a")).toBeDefined()
    expect(messages.get("b")).toBeUndefined() // 'drop' subset never requested
    t.close()
  })

  it("loads only the bounded window (orderBy + limit), not the whole subset", async () => {
    const room = "od-window"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      for (let i = 1; i <= 20; i++) {
        s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${i}`, String(i).padStart(2, "0"))
      }
    })

    const messages = createCollection(
      doCollectionOptions<Msg>({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload()

    // Top-5 by body desc — the live query's limit must bound the load.
    const top5 = createLiveQueryCollection((q) =>
      q.from({ m: messages }).orderBy(({ m }) => m.body, "desc").limit(5),
    )
    await top5.preload()
    await waitFor(() => top5.size === 5)

    expect(top5.toArray.map((m) => m.body)).toEqual(["20", "19", "18", "17", "16"])
    // The collection loaded ONLY the bounded window, not all 20.
    expect(messages.size).toBe(5)
    t.close()
  })

  it("fetch returns a bounded ordered page (scroll-back round-trip)", async () => {
    const room = "od-fetch"
    const t = realTransport(room)
    await t.connect()
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      for (let i = 1; i <= 20; i++) {
        s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", `m${i}`, String(i).padStart(2, "0"))
      }
    })

    // Window already showed 20..16; scrolling back fetches the next page
    // strictly below "16", newest-first, bounded by the limit.
    const orderBy = [{ expression: { type: "ref", path: ["body"] }, compareOptions: { direction: "desc" } }]
    const rows = (await t.fetch({
      t: "fetch",
      fetchId: "f1",
      collection: "messages",
      where: whereFunc("lt", "body", "16"),
      orderBy,
      limit: 5,
    })) as Array<Msg>
    expect(rows.map((r) => r.body)).toEqual(["15", "14", "13", "12", "11"])
    t.close()
  })

  it("a write outside every loaded subset is confirmed without stranding a phantom", async () => {
    const room = "od-outside"
    const t = realTransport(room)
    await t.connect()
    const messages = createCollection(
      doCollectionOptions<Msg>({ transport: t, table: "messages", getKey: (m) => m.id, syncMode: "on-demand" }),
    )
    await messages.preload()
    const kept = createLiveQueryCollection((q) => q.from({ m: messages }).where(({ m }) => eq(m.body, "keep")))
    await kept.preload()

    // Insert a row OUTSIDE the loaded subset (body !== "keep").
    await messages.insert({ id: "z", body: "other" }).isPersisted.promise

    // It must not linger in the collection: the out-of-subset row is retired
    // (synthetic delete to the loaded sub; empty-commit backstop).
    await waitFor(() => messages.get("z") === undefined)
    expect(messages.get("z")).toBeUndefined()
    expect(kept.get("z")).toBeUndefined()
    t.close()
  })
})
