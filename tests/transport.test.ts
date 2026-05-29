import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { MutationRejectedError, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import type { ClientFrame } from "../src/wire/frames.ts"

// WHY: the transport is the client half of the inversion. These drive the real
// transport against the real DO (the workerd accepted socket is injected as the
// opener, since workerd has no `new WebSocket(url)`). They pin: snapshot frames
// reach the handler, a mutation is confirmed AND its delta applied before the
// confirmation resolves, the single cursor advances on commit boundaries, a
// rejected write surfaces as an error, and a command returns its result.

function connect(room: string): Promise<WebSocketTransport> {
  const t = new WebSocketTransport({
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

function recorder(): { events: Array<Array<unknown>>; handler: import("../src/client/transport.ts").SubHandler } {
  const events: Array<Array<unknown>> = []
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

async function waitFor(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

const mut = (txId: string, key: string, body: string): Extract<ClientFrame, { t: "mut" }> => ({
  t: "mut",
  txId,
  collection: "messages",
  ops: [{ type: "insert", key, cols: { id: key, body } }],
})

describe("WebSocketTransport (M3 client)", () => {
  it("routes a snapshot to the sub handler", async () => {
    const room = "tr-snap"
    const t = await connect(room)
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hi')")
    })
    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler)
    await waitFor(() => events.some((e) => e[0] === "snap-end"))
    expect(events).toContainEqual(["snap", "a", { id: "a", body: "hi" }])
    t.close()
  })

  it("confirms a mutation only after its delta is applied, advancing the cursor", async () => {
    const t = await connect("tr-mut")
    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler)
    await waitFor(() => events.some((e) => e[0] === "snap-end"))

    expect(t.appliedCursor).toBe("0")
    const res = await t.sendMut(mut("x1", "a", "hi"))
    expect(res).toBeDefined()
    // sendMut resolved on `committed`; the delta must already be applied (C1).
    expect(events.some((e) => e[0] === "d" && e[1] === "insert")).toBe(true)
    expect(BigInt(t.appliedCursor)).toBeGreaterThan(0n)
    t.close()
  })

  it("awaitSeq resolves once the applied cursor reaches the target", async () => {
    const t = await connect("tr-await")
    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler)
    await waitFor(() => events.some((e) => e[0] === "snap-end"))
    await t.sendMut(mut("x1", "a", "hi"))
    await expect(t.awaitSeq(t.appliedCursor)).resolves.toBeUndefined()
    t.close()
  })

  it("surfaces a rejected mutation as MutationRejectedError", async () => {
    const t = await connect("tr-reject")
    await expect(t.sendMut(mut("x1", "a", "FORBIDDEN"))).rejects.toBeInstanceOf(MutationRejectedError)
    t.close()
  })

  it("returns a command result via sendCall", async () => {
    const t = await connect("tr-call")
    const res = await t.sendCall({ t: "call", txId: "c1", name: "echo", args: { n: 1 } })
    expect(res.result).toEqual({ echoed: { n: 1 } })
    t.close()
  })

  // Regression (browser-only): browsers default WebSocket.binaryType to "blob",
  // which the codec can't decode — every server frame would be dropped. The
  // transport must force "arraybuffer". (workerd delivers ArrayBuffer already,
  // so only a direct check catches this.)
  it("forces binaryType=arraybuffer on the socket", async () => {
    const fake = {
      binaryType: "blob",
      send: () => {},
      close: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
    }
    const t = new WebSocketTransport({ url: "ws://x", open: async () => fake as unknown as WebSocketLike })
    await t.connect()
    expect(fake.binaryType).toBe("arraybuffer")
    t.close()
  })
})
