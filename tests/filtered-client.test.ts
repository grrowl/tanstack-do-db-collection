import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions, WriteOutsideSubError } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: a filtered collection must (a) sync only the matching subset from the
// server, and (b) reject a write whose row would fall outside the filter before
// any network I/O — preventing an optimistic phantom that the server would
// never confirm. The predicate is compiled once from the same IR sent to the
// server, so client preflight and server filter agree.

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

function connect(room: string): Promise<WebSocketTransport<TestApi>> {
  const t = new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
  return t.connect().then(() => t)
}

async function waitFor(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

type Call = [string, unknown?]

function startFiltered(transport: WebSocketTransport<TestApi>, where: unknown): { calls: Array<Call>; adapter: ReturnType<typeof doCollectionOptions<TestApi, "messages">> } {
  const calls: Array<Call> = []
  const adapter = doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id, where })
  ;(adapter as unknown as { sync: { sync: (p: unknown) => void } }).sync.sync({
    collection: { get: () => undefined }, // adapter consults held keys (held-insert upsert)
    begin: () => calls.push(["begin"]),
    write: (m: unknown) => calls.push(["write", m]),
    commit: () => calls.push(["commit"]),
    markReady: () => calls.push(["markReady"]),
    truncate: () => calls.push(["truncate"]),
  })
  return { calls, adapter }
}

describe("doCollectionOptions filtered (M5 client)", () => {
  it("syncs only the matching subset from the server", async () => {
    const room = "fc-snap"
    const t = await connect(room)
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','x'),('b','y')")
    })
    const { calls } = startFiltered(t, whereEq("body", "x"))
    await waitFor(() => calls.some((c) => c[0] === "markReady"))
    const writes = calls.filter((c) => c[0] === "write")
    expect(writes).toEqual([["write", { type: "insert", value: { id: "a", body: "x" } }]])
    t.close()
  })

  it("rejects a write outside the filter before any I/O (WriteOutsideSubError)", async () => {
    const t = await connect("fc-preflight")
    const { adapter } = startFiltered(t, whereEq("body", "x"))
    const onInsert = (adapter as unknown as {
      onInsert: (p: { transaction: { id: string; mutations: Array<{ type: string; key: string; modified: Msg; changes: Msg }> } }) => Promise<void>
    }).onInsert

    await expect(
      onInsert({ transaction: { id: "tx1", mutations: [{ type: "insert", key: "b", modified: { id: "b", body: "y" }, changes: { id: "b", body: "y" } }] } }),
    ).rejects.toBeInstanceOf(WriteOutsideSubError)
    t.close()
  })

  it("accepts and confirms a write that matches the filter", async () => {
    const room = "fc-ok"
    const t = await connect(room)
    const { calls, adapter } = startFiltered(t, whereEq("body", "x"))
    await waitFor(() => calls.some((c) => c[0] === "markReady"))
    const onInsert = (adapter as unknown as {
      onInsert: (p: { transaction: { id: string; mutations: Array<{ type: string; key: string; modified: Msg; changes: Msg }> } }) => Promise<void>
    }).onInsert

    await onInsert({ transaction: { id: "tx1", mutations: [{ type: "insert", key: "a", modified: { id: "a", body: "x" }, changes: { id: "a", body: "x" } }] } })
    // Confirmed in-view: the row landed as a synced write before resolving.
    expect(calls.some((c) => c[0] === "write" && (c[1] as { value?: Msg }).value?.id === "a")).toBe(true)
    t.close()
  })
})
