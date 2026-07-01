import { createCollection } from "@tanstack/db"
import type { SqlStorage } from "@cloudflare/workers-types"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions, type WebSocketLike, WebSocketTransport } from "../src/client/index.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: server-originated writes (an agent inserting a row, a webhook, a cron
// job, a bulk seed) live outside the client mutation flow — no txId, no receipt.
// `runSyncedWrite` is the sanctioned primitive: apply a raw write in a
// transaction, then broadcast the resulting CDC to connected clients (ADR-0006).
// A raw `sql.exec` without it fires the triggers but never broadcasts until some
// later mutation drains the backlog.

// runSyncedWrite is protected (subclass-facing); reach it in the test via the
// in-DO instance. registerSync already ran in the DO constructor (ADR-0007).
type ServerApi = {
  runSyncedWrite: <T>(fn: (sql: SqlStorage) => T) => T
}
const api = (i: unknown): ServerApi => i as unknown as ServerApi

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
