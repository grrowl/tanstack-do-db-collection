import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (ADR-0011 C1′, generalizing ADR-0002 C1): the client's single cursor may
// only ever advance over a CONTIGUOUS applied prefix. Any server emission that
// advances the cursor (`snap-end`, catch-up `uptodate`, `committed`) must
// therefore be preceded by a flush of that socket's pending coalesced deltas —
// otherwise the cursor claims a seq whose delta is still sitting in the
// broadcaster buffer, and a drop before the tick loses that write forever
// (reconnect resumes from the claimed seq and skips it).
//
// This drives the failing interleaving found in adversarial review: two
// collections multiplexed on one socket; a write to `files` is enqueued but
// unflushed (the SlowTickDO never ticks); the same socket then opens a
// `messages` catch-up sub. The catch-up's `uptodate` carries the current seq —
// which includes the files write — so the files delta MUST be on the wire
// before it.

const codec = createFrameCodec()

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

/** Persistent recorder — attached once so no frame between steps is missed. */
function record(ws: WebSocket): Array<ServerFrame> {
  const out: Array<ServerFrame> = []
  ws.addEventListener("message", (e) => {
    out.push(codec.decode((e as MessageEvent).data as ArrayBuffer) as ServerFrame)
  })
  return out
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

const send = (ws: WebSocket, f: ClientFrame): void => ws.send(codec.encode(f))

describe("cursor barrier: pending deltas flush before any cursor-advancing frame (C1′)", () => {
  it("a catch-up sub on one collection cannot advance the cursor past another collection's buffered delta", async () => {
    const room = `barrier-${crypto.randomUUID()}`
    const ws1 = await openWs(`/slow/${room}`)
    const ws2 = await openWs(`/slow/${room}`)
    const f1 = record(ws1)
    const f2 = record(ws2)

    // ws1 watches BOTH collections on one socket.
    send(ws1, { t: "sub", subId: "m1", collection: "messages" })
    send(ws1, { t: "sub", subId: "f1", collection: "files" })
    await waitFor(() => f1.filter((f) => f.t === "snap-end").length === 2)

    // A confirmed write gives ws1 a real cursor S1 > 0 (catch-up needs one).
    send(ws1, { t: "mut", txId: "tx-m", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "hi" } }] })
    await waitFor(() => f1.some((f) => f.t === "committed"))
    const s1 = (f1.find((f) => f.t === "committed") as Extract<ServerFrame, { t: "committed" }>).seq

    // ws2 writes to `files`: ws1's delta for it is enqueued but NOT flushed
    // (SlowTickDO's coalescer never ticks inside this test).
    send(ws2, { t: "mut", txId: "tx-f", collection: "files", ops: [{ type: "insert", key: "x", cols: { id: "x", name: "doc" } }] })
    await waitFor(() => f2.some((f) => f.t === "committed"))
    const s2 = (f2.find((f) => f.t === "committed") as Extract<ServerFrame, { t: "committed" }>).seq
    expect(BigInt(s2)).toBeGreaterThan(BigInt(s1))

    // Same socket now opens a messages catch-up sub from S1. Its `uptodate`
    // carries the current seq (>= S2) and the client advances on it — so the
    // buffered files delta must arrive FIRST.
    const before = f1.length
    send(ws1, { t: "sub", subId: "m2", collection: "messages", since: s1 })
    await waitFor(() => f1.slice(before).some((f) => f.t === "uptodate" && BigInt(f.seq) >= BigInt(s2)))

    const since = f1.slice(before)
    const deltaIdx = since.findIndex((f) => f.t === "d" && f.sub === "f1" && f.key === "x")
    const boundaryIdx = since.findIndex((f) => f.t === "uptodate" && BigInt(f.seq) >= BigInt(s2))
    expect(deltaIdx).toBeGreaterThanOrEqual(0) // the buffered delta was flushed at all
    expect(deltaIdx).toBeLessThan(boundaryIdx) // ...and BEFORE the cursor boundary
    ws1.close()
    ws2.close()
  })

  it("a fresh snapshot on one collection cannot advance the cursor past another collection's buffered delta", async () => {
    const room = `barrier-snap-${crypto.randomUUID()}`
    const ws1 = await openWs(`/slow/${room}`)
    const ws2 = await openWs(`/slow/${room}`)
    const f1 = record(ws1)
    const f2 = record(ws2)

    send(ws1, { t: "sub", subId: "f1", collection: "files" })
    await waitFor(() => f1.some((f) => f.t === "snap-end"))

    // Buffered, unflushed files delta on ws1 (originating socket is ws2).
    send(ws2, { t: "mut", txId: "tx-f2", collection: "files", ops: [{ type: "insert", key: "y", cols: { id: "y", name: "img" } }] })
    await waitFor(() => f2.some((f) => f.t === "committed"))
    const s2 = (f2.find((f) => f.t === "committed") as Extract<ServerFrame, { t: "committed" }>).seq

    // A FRESH sub (no since) on messages snapshots at the current seq: its
    // snap-end advances the cursor to >= S2, so the files delta must precede it.
    const before = f1.length
    send(ws1, { t: "sub", subId: "m1", collection: "messages" })
    await waitFor(() => f1.slice(before).some((f) => f.t === "snap-end" && f.sub === "m1"))

    const since = f1.slice(before)
    const deltaIdx = since.findIndex((f) => f.t === "d" && f.sub === "f1" && f.key === "y")
    const endIdx = since.findIndex((f) => f.t === "snap-end" && f.sub === "m1")
    expect(deltaIdx).toBeGreaterThanOrEqual(0)
    expect(deltaIdx).toBeLessThan(endIdx)
    ws1.close()
    ws2.close()
  })
})
