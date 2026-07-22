import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"

// WHY: a transport must survive a reconnect attempt that fails to open — a
// transient outage must never permanently wedge the client. If open() rejects,
// the cached connectPromise stays a rejected promise forever: every subsequent
// connect() returns the same stale rejection, no close event ever fires (the
// socket never opened), and no further retry is ever scheduled. One outage
// longer than the reconnect delay permanently kills all subscriptions until reload.

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

async function waitFor(pred: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("transport reconnect recovery — failed open must not wedge the client", () => {
  it("Test A: recovers when open() fails once, then succeeds", async () => {
    const room = "rc-recover"
    let failNext = 0
    const sockets: Array<WebSocketLike & { close: () => void }> = []

    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 20,
      open: async () => {
        if (failNext > 0) {
          failNext--
          throw new Error("server unreachable")
        }
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

    // Seed a row so initial snapshot carries a cursor > 0; otherwise
    // resub(since=0) is (correctly) a fresh snapshot, not a catch-up.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('seed','present')")
    })

    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler)
    await waitFor(() => events.some((e) => e[0] === "snap-end")) // snapshot incl. seed; appliedSeq>0
    expect(events.some((e) => e[0] === "snap" && e[1] === "seed")).toBe(true)
    const before = events.length

    // Insert a row that will be missed during the outage.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('missed','while-away')")
    })

    // Make the NEXT open() attempt fail (simulates the server being unreachable
    // during the first reconnect window).
    failNext = 1

    // Drop the server-side sockets -> client close event -> auto-reconnect.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, state) => {
      for (const sock of state.getWebSockets()) sock.close(1000, "drop")
    })

    // The first auto-reconnect attempt fails (open() throws). Without the fix
    // the transport is permanently wedged here and this waitFor times out.
    // With the fix a second retry fires and the missed row arrives as a
    // catch-up delta — NOT a re-snapshot.
    await waitFor(
      () => events.slice(before).some((e) => e[0] === "d" && e[2] === "missed"),
      5000,
    )
    expect(sockets.length).toBeGreaterThanOrEqual(2) // at least two sockets opened
    expect(events.slice(before).some((e) => e[0] === "snap")).toBe(false) // catch-up, not re-snapshot

    t.close()
  })

  it("Test B: a caller-initiated connect() failure still rejects, and a later connect() retries fresh", async () => {
    let allowOpen = false

    const t = new WebSocketTransport({
      url: "wss://fake-recovery",
      reconnectDelay: 20,
      open: async () => {
        if (!allowOpen) throw new Error("server unreachable")
        // Return a fake WebSocketLike that does nothing — we just need the
        // connect() to succeed, not to exchange frames.
        const listeners = new Map<string, Array<(ev: { data?: unknown }) => void>>()
        const ws: WebSocketLike = {
          send: () => {},
          close: () => {},
          addEventListener: (type, l) => {
            const arr = listeners.get(type) ?? []
            arr.push(l)
            listeners.set(type, arr)
          },
          removeEventListener: () => {},
        }
        return ws
      },
    })

    // Fail-loud: the demand-path connect() must reject when open() throws.
    await expect(t.connect()).rejects.toThrow(/unreachable/)

    // Without the fix, connectPromise stays as the cached rejection and a
    // second connect() returns that same promise — still rejected.
    allowOpen = true
    await t.connect() // must succeed now

    t.close()
  })
})
