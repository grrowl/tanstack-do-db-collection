import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: reconnect catch-up is the spike's previously-dead snap-fallback path made
// live. A `sub` carrying a `since > 0` cursor must return only the changes after
// that cursor (a windowed delta), not a full snapshot — so a reconnecting client
// receives exactly what it missed. (since == 0 means "I have nothing" -> a fresh
// snapshot, covered elsewhere.)

const codec = createFrameCodec()

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

function collectUntil(ws: WebSocket, done: (f: ServerFrame) => boolean, timeoutMs = 2000): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))
const committedSeq = (frames: Array<ServerFrame>, txId: string): string =>
  (frames.find((f) => f.t === "committed" && f.txId === txId) as Extract<ServerFrame, { t: "committed" }>).seq

describe("reconnect catch-up (M7)", () => {
  it("a sub with `since` returns only changes after it, as a windowed delta", async () => {
    const ws = await openWs("cu-window")
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(ws, (f) => f.t === "snap-end")

    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "1" } }] })
    const c1 = await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t1")
    const afterA = committedSeq(c1, "t1")

    send(ws, { t: "mut", txId: "t2", collection: "messages", ops: [{ type: "insert", key: "b", cols: { id: "b", body: "2" } }] })
    await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t2")

    // Catch up from just after 'a': should deliver only 'b', never a snapshot.
    send(ws, { t: "sub", subId: "s2", collection: "messages", since: afterA })
    const cu = await collectUntil(ws, (f) => f.t === "uptodate")
    const s2Deltas = cu.filter((f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && f.sub === "s2")
    expect(s2Deltas.map((d) => d.key)).toEqual(["b"])
    expect(cu.some((f) => f.t === "snap" || f.t === "snap-end")).toBe(false)
    ws.close()
  })

  it("catching up at the current cursor yields a boundary and no deltas", async () => {
    const ws = await openWs("cu-empty")
    send(ws, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(ws, (f) => f.t === "snap-end")
    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "1" } }] })
    const c1 = await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t1")
    const cur = committedSeq(c1, "t1")

    send(ws, { t: "sub", subId: "s2", collection: "messages", since: cur })
    const cu = await collectUntil(ws, (f) => f.t === "uptodate")
    expect(cu.filter((f) => f.t === "d" && f.sub === "s2").length).toBe(0)
    ws.close()
  })
})
