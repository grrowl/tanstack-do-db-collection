import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"

// WHY: the lifecycle is the load-bearing transport. These drive a real
// WebSocket through workerd end to end. They pin: the upgrade contract, the
// no-wake ping/pong heartbeat, and that identity is bound to the socket via
// serializeAttachment — the mechanism that lets the DO hibernate and wake
// without losing who is who (ADR-0001 D13). Frame round-trips over the wire
// are covered by the read-path tests (sync-read.test.ts).

async function openWs(path: string, headers: Record<string, string> = {}): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, {
    headers: { Upgrade: "websocket", ...headers },
  })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

function nextMessage(ws: WebSocket): Promise<string | ArrayBuffer> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("ws message timeout")), 2000)
    ws.addEventListener(
      "message",
      (e: MessageEvent) => {
        clearTimeout(timer)
        resolve(e.data as string | ArrayBuffer)
      },
      { once: true },
    )
  })
}

describe("SyncDurableObject WebSocket lifecycle (M2)", () => {
  it("rejects a non-websocket request with 426", async () => {
    const res = await SELF.fetch("https://example.com/sync/room1")
    expect(res.status).toBe(426)
  })

  it("answers the ping heartbeat with pong (runtime auto-response)", async () => {
    const ws = await openWs("/sync/room-ping")
    ws.send("ping")
    expect(await nextMessage(ws)).toBe("pong")
    ws.close()
  })

  it("binds the parsed attachment to the socket (identity survives hibernation)", async () => {
    const ws = await openWs("/sync/room-id", { "x-user": "alice" })
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName("room-id"))
    await runInDurableObject(stub, (_i, state) => {
      const sockets = state.getWebSockets()
      expect(sockets.length).toBe(1)
      expect(sockets[0]!.deserializeAttachment()).toEqual({ userId: "alice" })
    })
    ws.close()
  })

  it("ignores an undecodable frame without closing the socket", async () => {
    const ws = await openWs("/sync/room-junk")
    ws.send(new Uint8Array([0xff, 0xff, 0xff])) // not a valid frame
    // The socket stays usable: a following ping still pongs.
    ws.send("ping")
    expect(await nextMessage(ws)).toBe("pong")
    ws.close()
  })
})
