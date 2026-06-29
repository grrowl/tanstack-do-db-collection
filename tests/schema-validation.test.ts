import { SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: a per-op Standard Schema is a validation GATE (ADR-0014). A write whose
// `cols`/`args` fail the schema must be REJECTED before anything is applied, and
// a write that passes must commit unchanged. These pin that gate on the wire:
//   - insert validates the full row; a bad row is rejected with the validation
//     message (a mutation's authorize error is user-facing) and writes nothing.
//   - update validates the PARTIAL patch (the reason updates need their own
//     schema — a full-row schema would reject every valid partial).
//   - a command validates its args the same way, and its rejection carries the
//     validation reason and a VALIDATION code too (ADR-0014 unifies command and
//     mutation surfacing: authorize and validation errors surface, execute errors
//     stay sanitized).
// The `validated` collection + `requireBody` command in test-worker carry a
// hand-rolled schema that rejects an empty `body`.

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
    ws.addEventListener("message", function onMsg(e: MessageEvent) {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
      if (done(out[out.length - 1]!)) {
        clearTimeout(timer)
        ws.removeEventListener("message", onMsg)
        resolve(out)
      }
    })
  })
}

const last = (frames: Array<ServerFrame>): ServerFrame => frames[frames.length - 1]!

describe("per-op Standard Schema validation gate (ADR-0014)", () => {
  it("rejects an insert whose row fails the schema, applying nothing", async () => {
    const ws = await openWs("/sync/v-ins-bad")
    send(ws, { t: "mut", txId: "t1", collection: "validated", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const l = last(frames)
    expect(l.t).toBe("rejected")
    if (l.t === "rejected") {
      expect(l.error.message).toMatch(/validation failed/)
      expect(l.error.code).toBe("VALIDATION")
    }
    expect(frames.some((f) => f.t === "d")).toBe(false) // nothing applied
    ws.close()
  })

  it("commits an insert whose row passes the schema", async () => {
    const ws = await openWs("/sync/v-ins-ok")
    send(ws, { t: "mut", txId: "t1", collection: "validated", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "hi" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    expect(last(frames).t).toBe("committed")
    ws.close()
  })

  it("rejects an update whose PARTIAL patch fails the schema (the partial-schema reason)", async () => {
    const ws = await openWs("/sync/v-upd-bad")
    send(ws, { t: "mut", txId: "t1", collection: "validated", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "ok" } }] })
    await collectUntil(ws, (f) => f.t === "committed")
    // A patch carrying only { body: "" } — a full-row schema would wrongly reject
    // any partial; the update schema validates exactly the patch.
    send(ws, { t: "mut", txId: "t2", collection: "validated", ops: [{ type: "update", key: "a", cols: { body: "" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || (f.t === "committed" && f.txId === "t2"))
    const l = last(frames)
    expect(l.t).toBe("rejected")
    if (l.t === "rejected") {
      expect(l.error.message).toMatch(/validation failed/)
      expect(l.error.code).toBe("VALIDATION")
    }
    ws.close()
  })

  it("commits an update whose partial patch passes the schema", async () => {
    const ws = await openWs("/sync/v-upd-ok")
    send(ws, { t: "mut", txId: "t1", collection: "validated", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "ok" } }] })
    await collectUntil(ws, (f) => f.t === "committed")
    send(ws, { t: "mut", txId: "t2", collection: "validated", ops: [{ type: "update", key: "a", cols: { body: "edited" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || (f.t === "committed" && f.txId === "t2"))
    expect(last(frames).t).toBe("committed")
    ws.close()
  })

  it("rejects a command whose args fail the schema, surfacing the reason (ADR-0014)", async () => {
    const ws = await openWs("/sync/v-cmd-bad")
    send(ws, { t: "call", txId: "c1", name: "requireBody", args: { body: "" } })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const l = last(frames)
    expect(l.t).toBe("rejected")
    // A command's validation rejection now carries the reason + code, like a
    // mutation's — not the old generic "command failed".
    if (l.t === "rejected") {
      expect(l.error.message).toMatch(/validation failed/)
      expect(l.error.code).toBe("VALIDATION")
    }
    ws.close()
  })

  it("runs a command whose args pass the schema and returns its result", async () => {
    const ws = await openWs("/sync/v-cmd-ok")
    send(ws, { t: "call", txId: "c1", name: "requireBody", args: { body: "hello" } })
    const frames = await collectUntil(ws, (f) => f.t === "committed" || f.t === "rejected")
    const l = last(frames)
    expect(l.t).toBe("committed")
    if (l.t === "committed") expect(l.result).toEqual({ echoed: "hello" })
    ws.close()
  })

  // WHY: the gate validates but MUST NOT parse — the handler receives the original
  // wire value, never the schema's transformed output. If the transform leaked
  // through, the stored/broadcast row would diverge from the row the client already
  // applied optimistically, and a pk rewrite would break optimistic-id ==
  // confirmed-id (ADR-0001 D9). `transformed`'s schema uppercases `body`.
  it("gate-not-parser: a transforming schema's output is discarded; the raw wire value is stored and synced", async () => {
    const ws = await openWs("/sync/v-transform")
    // Subscribe first, so the committed insert broadcasts back as a delta carrying
    // the STORED cols (what the handler actually wrote).
    send(ws, { t: "sub", subId: "s1", collection: "transformed" })
    await collectUntil(ws, (f) => f.t === "snap-end")
    send(ws, { t: "mut", txId: "t1", collection: "transformed", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "hi" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "committed" && f.txId === "t1")
    expect(last(frames).t).toBe("committed") // the transform doesn't reject; it commits
    // The row that synced back carries the RAW "hi", not the schema's "HI" — proving
    // the handler got the wire value, not the parsed output.
    const delta = frames.find((f): f is Extract<ServerFrame, { t: "d" }> => f.t === "d" && f.sub === "s1")
    expect(delta).toBeDefined()
    expect((delta!.cols as { body: string }).body).toBe("hi")
    ws.close()
  })

  // WHY: uniform surfacing (ADR-0014 revises ADR-0012 D3) — a command's authorize
  // "throw to deny" reaches the client verbatim, exactly like a mutation's; only an
  // execute error stays sanitized (that path is covered by `boom`).
  it("a command's authorize throw surfaces its reason, not a generic rejection", async () => {
    const ws = await openWs("/sync/v-cmd-authz")
    send(ws, { t: "call", txId: "c1", name: "denyCall", args: undefined })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const l = last(frames)
    expect(l.t).toBe("rejected")
    if (l.t === "rejected") expect(l.error.message).toMatch(/call denied by authorize/)
    ws.close()
  })
})
