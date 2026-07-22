import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"

// WHY: when the socket drops, the client must transparently reconnect and
// re-establish its subscriptions FROM its single applied cursor, so it receives
// exactly the changes it missed while away (a catch-up delta), not a full
// re-snapshot and not a gap. This drives a real drop by closing the underlying
// socket and asserts the missed write arrives after reconnect.

interface Recorder {
  events: Array<[string, ...Array<unknown>]>
  handler: SubHandler
}
function recorder(): Recorder {
  const events: Array<[string, ...Array<unknown>]> = []
  return {
    events,
    handler: {
      onSnap: (k, r) => events.push(["snap", k, r]),
      onSnapEnd: () => events.push(["snap-end"]),
      onDelta: (op, k, c) => events.push(["d", op, k, c]),
      onUptodate: () => events.push(["uptodate"]),
      onReset: () => events.push(["reset"]),
    },
  }
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("transport auto-reconnect + resubscribe (M7)", () => {
  it("reconnects after a drop and catches up the missed write", async () => {
    const room = "rc-drop"
    const sockets: Array<WebSocketLike & { close: () => void }> = []
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 20,
      open: async () => {
        const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
        const ws = res.webSocket
        if (!ws) throw new Error("no webSocket")
        ws.accept()
        const like = ws as unknown as WebSocketLike & { close: () => void }
        sockets.push(like)
        return like
      },
    })
    await t.connect()

    // Seed a row first so the initial snapshot carries a cursor > 0; otherwise
    // resub(since=0) is (correctly) a fresh snapshot, not a catch-up.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('seed','present')")
    })

    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler)
    await waitFor(() => events.some((e) => e[0] === "snap-end")) // snapshot incl. seed; appliedSeq>0
    expect(events.some((e) => e[0] === "snap" && e[1] === "seed")).toBe(true)
    const before = events.length

    // A write lands while we're about to lose the socket.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('missed','while-away')")
    })

    // Force a realistic drop: close the server-side socket (network/hibernation
    // close), which fires the client's `close` event -> auto-reconnect.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, state) => {
      for (const sock of state.getWebSockets()) sock.close(1000, "drop")
    })

    // After auto-reconnect + resub(since=appliedSeq), the missed insert arrives
    // as a catch-up delta — without a fresh snapshot.
    await waitFor(() => events.slice(before).some((e) => e[0] === "d" && e[2] === "missed"))
    expect(sockets.length).toBeGreaterThanOrEqual(2) // a new socket was opened
    expect(events.slice(before).some((e) => e[0] === "snap")).toBe(false) // catch-up, not re-snapshot
    t.close()
  })
})
