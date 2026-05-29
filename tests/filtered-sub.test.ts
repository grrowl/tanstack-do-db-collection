import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: a filtered subscription must show only matching rows AND track
// membership transitions — a row edited OUT of the predicate must be removed
// from the client (synthetic delete), and one edited INTO it must appear. The
// server computes this with no before-image (ADR-0002 C4): match -> current
// row with the actual op; non-match -> idempotent delete. Predicate eval reuses
// @tanstack/db's compiler so operators match the client exactly.

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

/** where: <field> = <value> (plain-JSON IR, as it rides the wire). */
const whereEq = (field: string, value: unknown): unknown => ({
  type: "func",
  name: "eq",
  args: [
    { type: "ref", path: [field] },
    { type: "val", value },
  ],
})

const deltaFor = (frames: Array<ServerFrame>, key: string) =>
  frames.find((f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && f.key === key)

describe("filtered subscriptions (M5)", () => {
  it("snapshot includes only rows matching the where predicate", async () => {
    const room = "f-snap"
    const ws = await openWs(room)
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','x'),('b','y')")
    })
    send(ws, { t: "sub", subId: "s1", collection: "messages", where: whereEq("body", "x") })
    const frames = await collectUntil(ws, (f) => f.t === "snap-end")
    const snaps = frames.filter((f): f is Extract<ServerFrame, { t: "snap" }> => f.t === "snap")
    expect(snaps.map((s) => s.key)).toEqual(["a"]) // only the matching row
    ws.close()
  })

  it("emits move-out as a delete and move-in as a row delta", async () => {
    const ws = await openWs("f-move")
    send(ws, { t: "sub", subId: "s1", collection: "messages", where: whereEq("body", "x") })
    await collectUntil(ws, (f) => f.t === "snap-end")

    // In-view insert -> delta with the row.
    send(ws, { t: "mut", txId: "t1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "x" } }] })
    let frames = await collectUntil(ws, (f) => f.t === "committed")
    expect(deltaFor(frames, "a")).toMatchObject({ op: "insert", cols: { body: "x" } })

    // Edit OUT of the predicate -> synthetic delete (move-out).
    send(ws, { t: "mut", txId: "t2", collection: "messages", ops: [{ type: "update", key: "a", cols: { body: "y" } }] })
    frames = await collectUntil(ws, (f) => f.t === "committed")
    expect(deltaFor(frames, "a")).toMatchObject({ op: "delete" })

    // Edit back INTO the predicate -> row reappears (move-in via update upsert).
    send(ws, { t: "mut", txId: "t3", collection: "messages", ops: [{ type: "update", key: "a", cols: { body: "x" } }] })
    frames = await collectUntil(ws, (f) => f.t === "committed")
    expect(deltaFor(frames, "a")).toMatchObject({ op: "update", cols: { body: "x" } })
    ws.close()
  })
})
