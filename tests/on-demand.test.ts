import { createCollection, createLiveQueryCollection, eq } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"

// WHY: on-demand loads ONLY the subsets live queries request, instead of
// syncing the whole collection. These pin: distinct `where`s become distinct
// refcounted server subs (shared + released correctly), a live query loads only
// its subset (other rows stay absent), and a write outside every loaded subset
// is confirmed without stranding an optimistic phantom.

interface Msg {
  id: string
  body: string
}

const whereEq = (field: string, value: unknown): unknown => ({
  type: "func",
  name: "eq",
  args: [
    { type: "ref", path: [field] },
    { type: "val", value },
  ],
})

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

// A fake transport that records sub/unsub and immediately completes each
// subscription's (empty) snapshot, for deterministic refcount tests.
function fakeTransport() {
  const subs: Array<{ subId: string; where: unknown }> = []
  const unsubs: Array<string> = []
  const transport = {
    connect: async () => {},
    subscribe: async (subId: string, _table: string, handler: SubHandler, where?: unknown) => {
      subs.push({ subId, where })
      handler.onSnapEnd() // empty snapshot -> resolves loadSubset
    },
    unsubscribe: (subId: string) => unsubs.push(subId),
    sendMut: async () => ({}),
    close: () => {},
  } as unknown as WebSocketTransport
  return { subs, unsubs, transport }
}

type OnDemand = {
  loadSubset: (o: { where?: unknown }) => true | Promise<void>
  unloadSubset: (o: { where?: unknown }) => void
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
