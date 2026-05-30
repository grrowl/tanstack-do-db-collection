import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: afterCommit is the sanctioned home for the external side effects a
// synchronous mutation `execute` can't do. These pin the library's contract:
//   - it runs AFTER the committed receipt (off the client's critical path),
//   - it receives a working `sql` AND the DO's `env` (so it can reach bindings),
//   - a throwing hook is isolated — the mutation stays committed and its durable
//     write persists; only the hook's own work is dropped.
// (The test DO's `files:delete` afterCommit writes a marker into a plain side
//  table `_afterlog`, tagged "has-env" when env.SYNC_DO is present; key "boom"
//  makes the hook throw.)

const codec = createFrameCodec()

async function openWs(path: string): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket" } })
  expect(res.status).toBe(101)
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket on 101 response")
  ws.accept()
  return ws
}

function send(ws: WebSocket, frame: ClientFrame): void {
  ws.send(codec.encode(frame))
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

/** Read the afterCommit marker for `key`, or undefined (table may not exist
 *  until the first hook runs). */
function readMarker(room: string, key: string): Promise<string | undefined> {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
  return runInDurableObject(stub, (_i, s) => {
    try {
      const rows = [...s.storage.sql.exec("SELECT tag FROM _afterlog WHERE key = ?", key)] as Array<{ tag: string }>
      return rows.length ? rows[0]!.tag : undefined
    } catch {
      return undefined // _afterlog not created yet (no hook has run)
    }
  })
}

function fileExists(room: string, key: string): Promise<boolean> {
  const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
  return runInDurableObject(stub, (_i, s) => [...s.storage.sql.exec("SELECT id FROM files WHERE id = ?", key)].length > 0)
}

async function waitFor(pred: () => Promise<boolean>, timeoutMs = 2000): Promise<void> {
  const start = Date.now()
  while (!(await pred())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("afterCommit post-commit hook", () => {
  it("runs after the receipt with a working sql + env", async () => {
    const room = "ac-ok"
    const ws = await openWs(`/sync/${room}`)
    send(ws, { t: "mut", txId: "i1", collection: "files", ops: [{ type: "insert", key: "f1", cols: { id: "f1", name: "a" } }] })
    await collectUntil(ws, (f) => f.t === "committed")

    // The delete's receipt must NOT wait on the hook — when committed arrives the
    // marker may not be written yet (fire-and-forget). So we confirm, then poll.
    send(ws, { t: "mut", txId: "d1", collection: "files", ops: [{ type: "delete", key: "f1" }] })
    await collectUntil(ws, (f) => f.t === "committed")

    await waitFor(async () => (await readMarker(room, "f1")) !== undefined)
    expect(await readMarker(room, "f1")).toBe("has-env") // proves env reached the hook
    ws.close()
  })

  it("isolates a throwing hook: the mutation stays committed and its write persists", async () => {
    const room = "ac-boom"
    const ws = await openWs(`/sync/${room}`)
    send(ws, { t: "mut", txId: "i1", collection: "files", ops: [{ type: "insert", key: "boom", cols: { id: "boom", name: "b" } }] })
    await collectUntil(ws, (f) => f.t === "committed")
    expect(await fileExists(room, "boom")).toBe(true)

    // The delete's afterCommit throws (key === "boom"). The receipt must still be
    // `committed`, and the durable delete (execute) must have applied.
    send(ws, { t: "mut", txId: "d1", collection: "files", ops: [{ type: "delete", key: "boom" }] })
    const frames = await collectUntil(ws, (f) => f.t === "committed" || f.t === "rejected")
    expect(frames[frames.length - 1]!.t).toBe("committed")
    expect(await fileExists(room, "boom")).toBe(false) // execute applied despite the hook throwing

    // Give the (failing) hook a beat; it must not have written a marker.
    await new Promise((r) => setTimeout(r, 60))
    expect(await readMarker(room, "boom")).toBeUndefined()
    ws.close()
  })
})
