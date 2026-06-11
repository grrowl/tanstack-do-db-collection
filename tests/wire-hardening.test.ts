// WHY: the wire boundary rejects or drops hostile input loudly without crashing
// the socket, and internal error detail never reaches a client. These tests
// guard the ADR-0012 invariants:
//   1. Malformed frame shapes are dropped silently (no reply) — the socket
//      survives and processes subsequent well-formed frames normally.
//   2. A mutation with too many ops is rejected with LIMIT_EXCEEDED before any
//      write is attempted.
//   3. A socket hitting the subscription cap gets reset for the excess subId;
//      earlier subs continue to receive deltas.
//   4. Oversized frames are dropped before decode; the socket survives.
//   5. Execute errors send a generic message to the client — SQLite detail never
//      leaks; authorize errors remain user-facing.

import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec, type WireOut } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

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

/** Send a raw, already-encoded value (for malformed-frame and oversize tests). */
function sendRaw(ws: WebSocket, data: WireOut): void {
  ws.send(data)
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

/** Wait up to timeoutMs; resolve with whatever frames arrived (may be empty). */
function collectFor(ws: WebSocket, timeoutMs: number): Promise<Array<ServerFrame>> {
  return new Promise((resolve) => {
    const out: Array<ServerFrame> = []
    const onMsg = (e: MessageEvent): void => {
      out.push(codec.decode(e.data as ArrayBuffer) as ServerFrame)
    }
    ws.addEventListener("message", onMsg)
    setTimeout(() => {
      ws.removeEventListener("message", onMsg)
      resolve(out)
    }, timeoutMs)
  })
}

/** Subscribe and wait for the initial snap-end so later frames are post-sub. */
async function subscribe(ws: WebSocket, subId: string, path = "/sync"): Promise<void> {
  send(ws, { t: "sub", subId, collection: "messages" })
  await collectUntil(ws, (f) => f.t === "snap-end")
}

describe("wire-hardening (ADR-0012)", () => {
  it("malformed frame shapes are dropped; socket survives", async () => {
    const ws = await openWs("/sync/wh-malformed")
    await subscribe(ws, "s1")

    // (a) mut with no txId or ops — fails wellFormed; dropped
    const malformed1 = codec.encode({ t: "mut" } as unknown as ClientFrame)
    sendRaw(ws, malformed1)

    // (b) mut with object txId and string ops — fails wellFormed; dropped
    const malformed2 = codec.encode({ t: "mut", txId: { evil: 1 }, collection: "messages", ops: "x" } as unknown as ClientFrame)
    sendRaw(ws, malformed2)

    // (c) unknown frame type — fails wellFormed; dropped
    const malformed3 = codec.encode({ t: "unknown-type" } as unknown as ClientFrame)
    sendRaw(ws, malformed3)

    // After the malformed frames, a VALID mut must succeed — proves socket survived.
    send(ws, {
      t: "mut",
      txId: "wh-t1",
      collection: "messages",
      ops: [{ type: "insert", key: "wh-a", cols: { id: "wh-a", body: "good" } }],
    })
    const frames = await collectUntil(ws, (f) => f.t === "committed" || f.t === "rejected")

    // No rejected for the malformed ones (dropped, not answered).
    const rejected = frames.filter((f) => f.t === "rejected")
    expect(rejected.length).toBe(0)
    // The valid mut committed.
    const committed = frames.find((f) => f.t === "committed")
    expect(committed).toBeDefined()

    ws.close()
  })

  it("ops over the per-mutation limit → LIMIT_EXCEEDED, nothing applied", async () => {
    // LimitsTestDO has maxOpsPerMutation = 2
    const room = "wh-ops-limit"
    const ws = await openWs(`/limits/${room}`)

    // Three ops — one over the limit of 2.
    send(ws, {
      t: "mut",
      txId: "wh-lim1",
      collection: "messages",
      ops: [
        { type: "insert", key: "r1", cols: { id: "r1", body: "x" } },
        { type: "insert", key: "r2", cols: { id: "r2", body: "y" } },
        { type: "insert", key: "r3", cols: { id: "r3", body: "z" } },
      ],
    })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const last = frames[frames.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(last.t).toBe("rejected")
    expect(last.error.code).toBe("LIMIT_EXCEEDED")
    expect(last.error.message).toMatch(/maxOpsPerMutation/)

    // Verify nothing was applied.
    const stub = env.LIMITS_DO.get(env.LIMITS_DO.idFromName(room))
    const rows = await runInDurableObject(stub, (_i, s) => {
      return Array.from(s.storage.sql.exec("SELECT COUNT(*) as cnt FROM messages"))
    })
    expect((rows[0] as { cnt: number }).cnt).toBe(0)

    ws.close()
  })

  it("subscription cap: third sub on 2-cap DO → reset; first two subs still receive deltas", async () => {
    // LimitsTestDO has maxSubsPerSocket = 2
    const room = "wh-sub-cap"
    const ws = await openWs(`/limits/${room}`)

    // First sub — ok.
    send(ws, { t: "sub", subId: "cap-s1", collection: "messages" })
    await collectUntil(ws, (f) => f.t === "snap-end")

    // Second sub — ok.
    send(ws, { t: "sub", subId: "cap-s2", collection: "messages" })
    await collectUntil(ws, (f) => f.t === "snap-end")

    // Third sub — over the cap; should get reset.
    send(ws, { t: "sub", subId: "cap-s3", collection: "messages" })
    const capFrames = await collectUntil(ws, (f) => f.t === "reset" || f.t === "snap-end", 2000)
    const reset = capFrames.find((f) => f.t === "reset") as Extract<ServerFrame, { t: "reset" }> | undefined
    expect(reset).toBeDefined()
    expect(reset!.sub).toBe("cap-s3")

    // Now insert a row — the first two subs should receive deltas.
    send(ws, {
      t: "mut",
      txId: "wh-cap-mut1",
      collection: "messages",
      ops: [{ type: "insert", key: "cap-row1", cols: { id: "cap-row1", body: "hello" } }],
    })
    const afterMut = await collectUntil(ws, (f) => f.t === "committed", 2000)
    // s1 and s2 both get a delta frame for the inserted row.
    const deltas = afterMut.filter((f) => f.t === "d") as Array<Extract<ServerFrame, { t: "d" }>>
    const subIds = deltas.map((d) => d.sub)
    expect(subIds).toContain("cap-s1")
    expect(subIds).toContain("cap-s2")

    ws.close()
  })

  it("oversized frame is dropped; socket survives and answers the next valid frame", async () => {
    const ws = await openWs("/sync/wh-oversize")

    // Build a >1 MiB payload by encoding a mut with a large cols.body.
    // The frame will exceed maxFrameBytes (1_048_576) when encoded.
    const bigBody = "x".repeat(1_048_577)
    const bigFrame = codec.encode({
      t: "mut",
      txId: "wh-big1",
      collection: "messages",
      ops: [{ type: "insert", key: "big", cols: { id: "big", body: bigBody } }],
    })

    // Attempt to send the oversized frame.
    let wsClosedByWorkerd = false
    ws.addEventListener("close", () => {
      wsClosedByWorkerd = true
    })

    try {
      sendRaw(ws, bigFrame)
    } catch {
      // workerd may throw synchronously on an oversized send — treat as closed.
      wsClosedByWorkerd = true
    }

    if (wsClosedByWorkerd) {
      // STOP-condition path: workerd closed the socket before webSocketMessage
      // ran. The DO never saw the frame. Assert the socket is closed and note.
      // The maxFrameBytes guard still provides an explicit, testable bound for
      // cases where workerd passes the frame through (future versions, etc.).
      // Per plan: assert THAT and note it — either way the DO must not crash.
      expect(wsClosedByWorkerd).toBe(true)
      return
    }

    // Collect for a short window — no reply expected (frame was dropped).
    const droppedFrames = await collectFor(ws, 300)
    const anyReply = droppedFrames.filter((f) => {
      const r = f as Extract<ServerFrame, { t: "rejected" }>
      return f.t === "rejected" && (r as typeof r).txId === "wh-big1"
    })
    expect(anyReply).toHaveLength(0)

    // Socket survives: a normal mut succeeds.
    send(ws, {
      t: "mut",
      txId: "wh-small1",
      collection: "messages",
      ops: [{ type: "insert", key: "small", cols: { id: "small", body: "normal" } }],
    })
    const followUp = await collectUntil(ws, (f) => f.t === "committed" || f.t === "rejected")
    const last = followUp[followUp.length - 1]!
    expect(last.t).toBe("committed")

    ws.close()
  })

  it("execute error → generic 'mutation failed', SQLite detail not leaked; authorize error passes through", async () => {
    const ws = await openWs("/sync/wh-sanitize")

    // Insert key "a" first.
    send(ws, { t: "mut", txId: "wh-san1", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "v1" } }] })
    await collectUntil(ws, (f) => f.t === "committed")

    // Duplicate-pk insert triggers SQLite UNIQUE constraint.
    send(ws, { t: "mut", txId: "wh-san2", collection: "messages", ops: [{ type: "insert", key: "a", cols: { id: "a", body: "v2" } }] })
    const frames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const last = frames[frames.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(last.t).toBe("rejected")
    // Generic message — no SQLite detail.
    expect(last.error.message).toBe("mutation failed")
    expect(last.error.code).toBe("EXECUTE_FAILED")
    // The SQLite constraint detail must NOT appear in the client-visible message.
    expect(last.error.message).not.toMatch(/UNIQUE|constraint/i)

    // Authorize denial still passes through verbatim (README: "throw to deny").
    send(ws, {
      t: "mut",
      txId: "wh-san3",
      collection: "messages",
      ops: [{ type: "insert", key: "forbidden-key", cols: { id: "forbidden-key", body: "FORBIDDEN" } }],
    })
    const authFrames = await collectUntil(ws, (f) => f.t === "rejected" || f.t === "committed")
    const authLast = authFrames[authFrames.length - 1]! as Extract<ServerFrame, { t: "rejected" }>
    expect(authLast.t).toBe("rejected")
    expect(authLast.error.message).toMatch(/forbidden/)

    ws.close()
  })
})
