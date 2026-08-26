import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: the adapter is the seam between the transport and TanStack DB's sync
// API. createCollection just consumes the config it returns, so the behaviour
// that matters is the frame -> begin/write/commit/markReady mapping and the
// mutation -> `mut` frame. We drive that mapping against the REAL DO using spy
// sync controls (no @tanstack/db runtime needed — the module is type-only),
// pinning: a snapshot becomes begin/write(insert)/commit/markReady, and a
// mutation both confirms AND lands as a synced write (delta applied before the
// mutationFn resolves — the C1 ordering, observed at the adapter boundary).

interface Msg {
  id: string
  body: string
}

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

/** Spy sync controls + the bound sync function from a fresh adapter. */
function startSync(transport: WebSocketTransport<TestApi>): { calls: Array<Call> } {
  const calls: Array<Call> = []
  const opts = doCollectionOptions({ transport, table: "messages", getKey: (r) => r.id })
  // sync lives on opts.sync.sync; invoke with spy controls (cast: type-only dep).
  const syncConfig = (opts as unknown as { sync: { sync: (p: unknown) => void } }).sync
  syncConfig.sync({
    collection: { get: () => undefined, _state: { syncedData: new Map() } }, // adapter consults synced rows
    begin: () => calls.push(["begin"]),
    write: (m: unknown) => calls.push(["write", m]),
    commit: () => calls.push(["commit"]),
    markReady: () => calls.push(["markReady"]),
    truncate: () => calls.push(["truncate"]),
  })
  return { calls }
}

interface PendingInsert {
  type: "insert"
  key: string
  modified: Msg
  changes: Msg
}

describe("doCollectionOptions (M3 adapter)", () => {
  it("maps a snapshot into begin / write(insert) / commit / markReady", async () => {
    const room = "dc-snap"
    const t = await connect(room)
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hi')")
    })
    const { calls } = startSync(t)
    await waitFor(() => calls.some((c) => c[0] === "markReady"))
    expect(calls.map((c) => c[0])).toEqual(["begin", "write", "commit", "markReady"])
    expect(calls[1]).toEqual(["write", { type: "insert", value: { id: "a", body: "hi" } }])
    t.close()
  })

  it("readies an empty collection without any write", async () => {
    const t = await connect("dc-empty")
    const { calls } = startSync(t)
    await waitFor(() => calls.some((c) => c[0] === "markReady"))
    expect(calls.map((c) => c[0])).toEqual(["markReady"])
    t.close()
  })

  it("sends a mut and lands the confirming delta as a synced write before resolving", async () => {
    const room = "dc-mut"
    const t = await connect(room)
    const adapter = doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id })
    const calls: Array<Call> = []
    ;(adapter as unknown as { sync: { sync: (p: unknown) => void } }).sync.sync({
      collection: { get: () => undefined, _state: { syncedData: new Map() } },
      begin: () => calls.push(["begin"]),
      write: (m: unknown) => calls.push(["write", m]),
      commit: () => calls.push(["commit"]),
      markReady: () => calls.push(["markReady"]),
      truncate: () => calls.push(["truncate"]),
    })
    await waitFor(() => calls.some((c) => c[0] === "markReady"))
    calls.length = 0

    const onInsertFn = (adapter as unknown as {
      onInsert: (p: { transaction: { id: string; mutations: Array<PendingInsert> } }) => Promise<void>
    }).onInsert
    await onInsertFn({
      transaction: { id: "tx1", mutations: [{ type: "insert", key: "a", modified: { id: "a", body: "hi" }, changes: { id: "a", body: "hi" } }] },
    })

    // onInsert resolved on `committed`; by C1 the delta is already applied:
    expect(calls).toContainEqual(["write", { type: "insert", value: { id: "a", body: "hi" } }])
    expect(calls.some((c) => c[0] === "commit")).toBe(true)
    t.close()
  })
})
