import { createCollection } from "@tanstack/db"
import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { doCollectionOptions } from "../src/client/do-collection.ts"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { TestApi } from "./test-worker.ts"

// WHY: a catch-up (reconnect or SSR hydration) emits the LATEST CDC op per
// changed key. A key that was deleted-and-reinserted while the client was away
// therefore arrives as op="insert" — for a key the client still HOLDS (its
// delete was compacted away with the rest of the window). TanStack's sync
// write throws DuplicateKeySyncError on insert-over-existing unless values
// deep-equal, so without normalization the reinsert aborts the whole catch-up
// transaction and the client wedges on stale state. The adapter must apply a
// held-key "insert" as the upsert it semantically is (ADR-0011 D4) — the same
// update-upsert contract move-in already relies on (ADR-0002 C4).

function makeTransport(room: string): WebSocketTransport<TestApi> {
  return new WebSocketTransport<TestApi>({
    url: `https://example.com/sync/${room}`,
    reconnectDelayMs: 20,
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

describe("catch-up reinsert lands as an upsert, not a DuplicateKeySyncError", () => {
  it("converges a key deleted-and-reinserted while the client was away", async () => {
    const room = `reinsert-${crypto.randomUUID()}`
    const t = makeTransport(room)
    await t.connect()
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))

    // Seed before subscribing so the snapshot carries the row and a cursor > 0.
    await runInDurableObject(stub, (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('k','v1')")
    })

    const messages = createCollection(doCollectionOptions({ transport: t, table: "messages", getKey: (r) => r.id }))
    await messages.preload()
    expect(messages.get("k")).toMatchObject({ id: "k", body: "v1" })

    // Drop the socket; while away, 'k' is deleted then reinserted with a new
    // value — the catch-up's latest-op-per-key for 'k' is op="insert".
    await runInDurableObject(stub, (_i, state) => {
      for (const sock of state.getWebSockets()) sock.close(1000, "drop")
    })
    await runInDurableObject(stub, (_i, s) => {
      s.storage.sql.exec("DELETE FROM messages WHERE id='k'")
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('k','v2')")
    })

    // After auto-reconnect + catch-up, the held key must converge to v2 —
    // not wedge on a DuplicateKeySyncError-aborted transaction.
    await waitFor(() => messages.get("k")?.body === "v2")
    expect(messages.get("k")).toMatchObject({ id: "k", body: "v2" })
    t.close()
  })
})
