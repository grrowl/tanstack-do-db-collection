import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: subset shaping must run in SQLite end-to-end — a subscription's
// orderBy/limit/offset shape the snapshot at the source, and an un-lowerable
// predicate is rejected (reset), never silently full-scanned.

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

async function seed(room: string, values: Array<[string, string]>): Promise<void> {
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
    for (const [id, body] of values) s.storage.sql.exec("INSERT INTO messages(id,body) VALUES(?,?)", id, body)
  })
}

const snapKeys = (frames: Array<ServerFrame>): Array<unknown> =>
  frames.filter((f): f is Extract<ServerFrame, { t: "snap" }> => f.t === "snap").map((s) => s.key)

describe("subset shaping pushed into SQLite (M6)", () => {
  it("orders the snapshot by a column", async () => {
    const room = "ss-order"
    const ws = await openWs(room)
    await seed(room, [["a", "1"], ["b", "3"], ["c", "2"]])
    send(ws, { t: "sub", subId: "s1", collection: "messages", orderBy: [{ col: "body", dir: "desc" }] as never })
    const frames = await collectUntil(ws, (f) => f.t === "snap-end")
    expect(snapKeys(frames)).toEqual(["b", "c", "a"]) // body 3,2,1
    ws.close()
  })

  it("applies limit and offset over an ordered snapshot", async () => {
    const room = "ss-page"
    const ws = await openWs(room)
    await seed(room, [["a", "1"], ["b", "2"], ["c", "3"], ["d", "4"], ["e", "5"]])
    send(ws, { t: "sub", subId: "s1", collection: "messages", orderBy: [{ col: "body", dir: "asc" }] as never, limit: 2, offset: 1 })
    const frames = await collectUntil(ws, (f) => f.t === "snap-end")
    expect(snapKeys(frames)).toEqual(["b", "c"]) // skip a, take b,c
    ws.close()
  })

  it("cold snapshot preserves insertion order when the client sends no orderBy (field-verified regression)", async () => {
    const room = "ss-cold-order"
    const ws = await openWs(room)
    // Insertion order deliberately NOT pk-lexicographic: a query plan that uses
    // the pk's autoindex (SQLite may pick it once a WHERE clause touches `id`)
    // would return sorted-by-id order ("a","m","z") instead of insertion order
    // — exactly the divergence the field-verified bug exposed. All three
    // inserts land in one synchronous block (same millisecond), so a `ts`-based
    // tiebreak could never disambiguate them even if one were used.
    await seed(room, [
      ["z", "1"],
      ["a", "2"],
      ["m", "3"],
    ])
    const where = { type: "func", name: "gt", args: [{ type: "ref", path: ["id"] }, { type: "val", value: "" }] }
    send(ws, { t: "sub", subId: "s1", collection: "messages", where } as never)
    const frames = await collectUntil(ws, (f) => f.t === "snap-end")
    expect(snapKeys(frames)).toEqual(["z", "a", "m"])
    ws.close()
  })

  it("rejects a subscription whose predicate cannot be lowered (reset)", async () => {
    const ws = await openWs("ss-reject")
    // ilike is outside the supported floor.
    const where = { type: "func", name: "ilike", args: [{ type: "ref", path: ["body"] }, { type: "val", value: "x" }] }
    send(ws, { t: "sub", subId: "s1", collection: "messages", where })
    const frames = await collectUntil(ws, (f) => f.t === "reset")
    expect(frames).toEqual([{ t: "reset", sub: "s1" }])
    ws.close()
  })
})
