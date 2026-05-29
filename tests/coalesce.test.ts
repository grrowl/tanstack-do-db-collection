import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: end-to-end proof that the coalescer collapses a burst for a
// non-originating subscriber. Two clients on the same DO: A writes the same row
// repeatedly in quick succession; B (a separate subscriber) must receive far
// fewer deltas than A's write count, converging on the final value. This is the
// throughput win the coalescer exists for.

const codec = createFrameCodec()

async function openWs(room: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws
}

function onFrames(ws: WebSocket, sink: (f: ServerFrame) => void): void {
  ws.addEventListener("message", (e: MessageEvent) => sink(codec.decode(e.data as ArrayBuffer) as ServerFrame))
}

function send(ws: WebSocket, frame: ClientFrame): void {
  ws.send(codec.encode(frame))
}

async function waitFor(pred: () => boolean, timeoutMs = 2000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("egress coalescing across clients (M4)", () => {
  it("a burst of same-key writes reaches a subscriber as far fewer deltas", async () => {
    const room = "coalesce"
    const a = await openWs(room)
    const b = await openWs(room)

    const bDeltas: Array<Extract<ServerFrame, { t: "d" }>> = []
    onFrames(b, (f) => {
      if (f.t === "d") bDeltas.push(f)
    })
    send(b, { t: "sub", subId: "sb", collection: "messages" })

    // A confirms via committed; seed an insert and let B receive it.
    const aFrames: Array<ServerFrame> = []
    onFrames(a, (f) => aFrames.push(f))
    send(a, { t: "sub", subId: "sa", collection: "messages" })
    await waitFor(() => aFrames.some((f) => f.t === "snap-end"))
    send(a, { t: "mut", txId: "ins", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "v0" } }] })
    await waitFor(() => aFrames.some((f) => f.t === "committed" && f.txId === "ins"))

    // Fire N rapid updates to the SAME key so they land within one tick window.
    const N = 5
    for (let i = 1; i <= N; i++) {
      send(a, { t: "mut", txId: `u${i}`, collection: "messages", ops: [{ type: "update", key: "a", cols: { body: `v${i}` } }] })
    }
    await waitFor(() => aFrames.filter((f) => f.t === "committed" && f.txId.startsWith("u")).length === N)

    // B converges on the final value, having received far fewer than N+1 deltas.
    await waitFor(() => bDeltas.some((d) => (d.cols as { body?: string } | undefined)?.body === `v${N}`))
    expect(bDeltas.length).toBeLessThan(N + 1)
    const last = bDeltas[bDeltas.length - 1]!
    expect((last.cols as { body?: string }).body).toBe(`v${N}`)

    a.close()
    b.close()
  })
})
