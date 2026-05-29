import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: subscribe is the read half of the inversion — a client must receive the
// authoritative current state as a snapshot, terminated by an explicit boundary
// it can commit on. These pin: a full-collection snapshot reflects current
// rows, an empty collection still terminates, and unknown collections fail
// safe. The snapshot also exercises a real binary frame round-trip through the
// DO (client `sub` -> server `snap`/`snap-end`).

const codec = createFrameCodec()

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

/** Collect server frames until `done` returns true (or time out). */
function collectUntil(
  ws: WebSocket,
  done: (f: ServerFrame) => boolean,
  timeoutMs = 2000,
): Promise<Array<ServerFrame>> {
  return new Promise((resolve, reject) => {
    const out: Array<ServerFrame> = []
    const timer = setTimeout(() => reject(new Error(`timeout; got [${out.map((f) => f.t).join(",")}]`)), timeoutMs)
    const onMsg = (e: MessageEvent): void => {
      const f = codec.decode(e.data as ArrayBuffer) as ServerFrame
      out.push(f)
      if (done(f)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    }
    ws.addEventListener("message", onMsg)
  })
}

const sub = (subId: string, collection: string): ClientFrame => ({ t: "sub", subId, collection })

describe("read path: subscribe -> snapshot (M3)", () => {
  it("returns every current row as a snap, then snap-end", async () => {
    const room = "read-seed"
    const ws = await openWs(`/sync/${room}`) // upgrade creates the schema
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
    await runInDurableObject(stub, (_i, state) => {
      state.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hi'),('b','yo')")
    })

    ws.send(codec.encode(sub("s1", "messages")))
    const frames = await collectUntil(ws, (f) => f.t === "snap-end")

    const snaps = frames.filter((f): f is Extract<ServerFrame, { t: "snap" }> => f.t === "snap")
    expect(snaps.map((s) => s.key).sort()).toEqual(["a", "b"])
    expect(snaps.find((s) => s.key === "a")?.row).toEqual({ id: "a", body: "hi" })
    const end = frames.at(-1)
    expect(end?.t).toBe("snap-end")
    if (end?.t === "snap-end") expect(end.sub).toBe("s1")
    ws.close()
  })

  it("subscribing to an empty collection emits only snap-end", async () => {
    const ws = await openWs("/sync/read-empty")
    ws.send(codec.encode(sub("s1", "messages")))
    const frames = await collectUntil(ws, (f) => f.t === "snap-end")
    expect(frames.map((f) => f.t)).toEqual(["snap-end"])
    ws.close()
  })

  it("subscribing to an unknown collection fails safe with reset", async () => {
    const ws = await openWs("/sync/read-unknown")
    ws.send(codec.encode(sub("s1", "nonexistent")))
    const frames = await collectUntil(ws, (f) => f.t === "reset")
    expect(frames).toEqual([{ t: "reset", sub: "s1" }])
    ws.close()
  })
})
