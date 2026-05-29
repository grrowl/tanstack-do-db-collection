import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: the write path is the inversion's core. These pin the ADR-0002
// invariants directly on the wire:
//   - C1 ordering: a write whose row is in view delivers the delta BEFORE the
//     committed receipt, so the client never advances its cursor past
//     undelivered data and never flickers the optimistic overlay.
//   - the no-subscription-match path: a write the client isn't watching is
//     still confirmed via committed alone (no delta).
//   - exactly-once: a retried txId replays the receipt, not the side effect.
//   - authorize denies before any write; the row is never created.

const codec = createFrameCodec()

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
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

function send(ws: WebSocket, frame: ClientFrame): void {
  ws.send(codec.encode(frame))
}

/** Subscribe and wait for the initial snap-end so later frames are post-sub. */
async function subscribe(ws: WebSocket, subId: string): Promise<void> {
  send(ws, { t: "sub", subId, collection: "messages" })
  await collectUntil(ws, (f) => f.t === "snap-end")
}

describe("write path: single-stream confirmation (M3)", () => {
  it("delivers the delta BEFORE the committed receipt for an in-view write (C1)", async () => {
    const ws = await openWs("/sync/w-order")
    await subscribe(ws, "s1")

    send(ws, { t: "mut", txId: "tx1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "hi" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "committed")

    const dIdx = frames.findIndex((f) => f.t === "d")
    const cIdx = frames.findIndex((f) => f.t === "committed")
    expect(dIdx).toBeGreaterThanOrEqual(0)
    expect(dIdx).toBeLessThan(cIdx) // delta strictly before receipt
    const delta = frames[dIdx] as Extract<ServerFrame, { t: "d" }>
    expect(delta.op).toBe("insert")
    expect(delta.cols).toEqual({ id: "a", body: "hi" })
    const committed = frames[cIdx] as Extract<ServerFrame, { t: "committed" }>
    expect(committed.txId).toBe("tx1")
    expect(BigInt(committed.seq)).toBeGreaterThan(0n)
    ws.close()
  })

  it("confirms a write matching no subscription with committed alone (no delta)", async () => {
    const ws = await openWs("/sync/w-nosub") // never subscribes
    send(ws, { t: "mut", txId: "tx1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "x" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "committed")
    expect(frames.map((f) => f.t)).toEqual(["committed"]) // no `d`, no `uptodate`
    ws.close()
  })

  it("replays the receipt on a retried txId without re-applying (exactly-once)", async () => {
    const ws = await openWs("/sync/w-dedup")
    await subscribe(ws, "s1")
    const mut: ClientFrame = { t: "mut", txId: "dup", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "one" } }] }

    send(ws, mut)
    const first = await collectUntil(ws, (f) => f.t === "committed")
    const firstSeq = (first.find((f) => f.t === "committed") as Extract<ServerFrame, { t: "committed" }>).seq

    send(ws, mut) // identical retry
    const second = await collectUntil(ws, (f) => f.t === "committed")
    // Same receipt seq, and no second insert delta (the retry didn't re-run).
    expect((second.find((f) => f.t === "committed") as Extract<ServerFrame, { t: "committed" }>).seq).toBe(firstSeq)
    expect(second.some((f) => f.t === "d")).toBe(false)
    ws.close()
  })

  it("rejects a denied write before applying it", async () => {
    const ws = await openWs("/sync/w-authz")
    await subscribe(ws, "s1")
    send(ws, { t: "mut", txId: "tx1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "FORBIDDEN" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const last = frames[frames.length - 1]!
    expect(last.t).toBe("rejected")
    if (last.t === "rejected") expect(last.error.message).toMatch(/forbidden/)
    expect(frames.some((f) => f.t === "d")).toBe(false) // nothing applied
    ws.close()
  })

  it("runs a command and returns its result on committed", async () => {
    const ws = await openWs("/sync/w-cmd")
    send(ws, { t: "call", txId: "c1", name: "echo", args: { n: 7 } })
    const frames = await collectUntil(ws, (f) => f.t === "committed")
    const committed = frames.find((f) => f.t === "committed") as Extract<ServerFrame, { t: "committed" }>
    expect(committed.result).toEqual({ echoed: { n: 7 } })
    ws.close()
  })

  it("fans an update and a delete to a subscriber as deltas", async () => {
    const ws = await openWs("/sync/w-upd")
    await subscribe(ws, "s1")
    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "v1" } }] })
    await collectUntil(ws, (f) => f.t === "committed")

    send(ws, { t: "mut", txId: "t2", collection: "messages", ops: [{ type: "update", key: "a", cols: { body: "v2" } }] })
    const upd = await collectUntil(ws, (f) => f.t === "committed")
    const ud = upd.find((f) => f.t === "d") as Extract<ServerFrame, { t: "d" }>
    expect(ud.op).toBe("update")
    expect((ud.cols as { body: string }).body).toBe("v2")

    send(ws, { t: "mut", txId: "t3", collection: "messages", ops: [{ type: "delete", key: "a" }] })
    const del = await collectUntil(ws, (f) => f.t === "committed")
    const dd = del.find((f) => f.t === "d") as Extract<ServerFrame, { t: "d" }>
    expect(dd.op).toBe("delete")
    expect(dd.key).toBe("a")
    ws.close()
  })
})
