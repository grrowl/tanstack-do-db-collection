import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import {
  ConnectionLostError,
  MutationRejectedError,
  type SubHandler,
  WebSocketTransport,
  type WebSocketLike,
} from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY (issue #39 / ADR-0021): an in-flight mut/call whose socket drops
// UNEXPECTEDLY must not be abandoned to its generic 5s confirmation timeout —
// server-side, recordTx + broadcast run BEFORE the `committed` send, so a drop
// in that window means the write is already durably committed while the client's
// optimistic overlay times out and rolls back a write that SUCCEEDED.
//
// The contract these pin:
//   - Hold-and-replay (primary): on an unexpected drop with subscriptions active,
//     the transport reconnects and REPLAYS each in-flight txId; the server's dedup
//     table answers with the TRUE recorded outcome (committed/rejected). A write
//     the server committed resolves `committed`, never a timeout rejection.
//   - Typed fallback: when no reconnect can resolve it (terminal 4xxx close, or no
//     active subscriptions), the in-flight mut/call settles PROMPTLY with a typed,
//     distinguishable ConnectionLostError — never the generic timeout.
//   - Timeout semantics are UNCHANGED for a socket that stays open.

const codec = createFrameCodec()

function noopHandler(): SubHandler {
  return { onSnap: () => {}, onSnapEnd: () => {}, onDelta: () => {}, onUptodate: () => {}, onReset: () => {} }
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

interface Fake {
  ws: WebSocketLike
  sent: Array<ClientFrame>
  emit: (type: string, ev: { data?: unknown; code?: number; reason?: string }) => void
}

function makeFake(): Fake {
  const listeners = new Map<string, Array<(ev: { data?: unknown }) => void>>()
  const fake: Fake = {
    sent: [],
    emit: (type, ev) => {
      for (const l of listeners.get(type) ?? []) l(ev)
    },
    ws: {
      send: (data) => fake.sent.push(codec.decode(data as ArrayBuffer | string) as ClientFrame),
      close: () => {},
      addEventListener: (type, l) => {
        const arr = listeners.get(type) ?? []
        arr.push(l)
        listeners.set(type, arr)
      },
      removeEventListener: () => {},
    },
  }
  return fake
}

const mut = (txId: string, key: string, body: string): Extract<ClientFrame, { t: "mut" }> => ({
  t: "mut",
  txId,
  collection: "messages",
  ops: [{ type: "insert", key, cols: { id: key, body } }],
})

// --- Transport-level contract (fake sockets, exact timing control) ----------

describe("in-flight mut settlement across an unexpected close (#39) — transport contract", () => {
  it("drop-before-server-receives with NO subscriptions settles PROMPTLY with a typed ConnectionLostError", async () => {
    const fake = makeFake()
    const t = new WebSocketTransport({ url: "wss://x", open: () => fake.ws, timeoutMs: 60_000 })

    const p = t.sendMut(mut("T1", "a", "hi")) // no subscriptions registered
    const outcome = p.then(() => "resolved", (e) => e)
    await waitFor(() => fake.sent.some((f) => f.t === "mut")) // the frame left

    fake.emit("close", { code: 1006 }) // unexpected drop, no subs → no reconnect
    const r = await outcome
    // Settled by the drop, NOT the 60s timeout — and typed, not a generic Error.
    expect(r).toBeInstanceOf(ConnectionLostError)
    expect(r).not.toBeInstanceOf(MutationRejectedError)
    t.close()
  })

  it("holds an in-flight mut across a transient drop and REPLAYS it on reconnect (the resent frame lands on the new socket)", async () => {
    const fakes: Array<Fake> = []
    const t = new WebSocketTransport({
      url: "wss://x",
      reconnectDelay: () => 5,
      timeoutMs: 60_000,
      open: () => {
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    await waitFor(() => fakes.length === 1)

    const p = t.sendMut(mut("T2", "a", "hi"))
    let settled = false
    const outcome = p.then((v) => ((settled = true), v), (e) => ((settled = true), e))
    await waitFor(() => fakes[0]!.sent.some((f) => f.t === "mut" && (f as { txId?: string }).txId === "T2"))

    // Unexpected drop: the mut is parked, NOT rejected — the app keeps its overlay.
    fakes[0]!.emit("close", { code: 1006 })
    // Reconnect opens a fresh socket…
    await waitFor(() => fakes.length === 2)
    // …which is resubscribed AND replayed the parked mut (retry-through-dedup).
    await waitFor(() => fakes[1]!.sent.some((f) => f.t === "mut" && (f as { txId?: string }).txId === "T2"))
    expect(fakes[1]!.sent.some((f) => f.t === "sub" && (f as { subId?: string }).subId === "s1")).toBe(true)
    expect(settled).toBe(false) // still pending — held for the dedup answer

    // The server's dedup replay answers committed → the promise resolves (no rollback).
    fakes[1]!.emit("message", { data: codec.encode({ t: "committed", txId: "T2", seq: "1" } as ServerFrame) })
    await expect(outcome).resolves.toEqual({ result: undefined })
    t.close()
  })

  it("a TERMINAL 4xxx close settles the in-flight mut with ConnectionLostError (and fires onClosed)", async () => {
    const fakes: Array<Fake> = []
    const closed: Array<[number | undefined, string | undefined]> = []
    const t = new WebSocketTransport({
      url: "wss://x",
      reconnectDelay: 5,
      timeoutMs: 60_000,
      onClosed: (code, reason) => closed.push([code, reason]),
      open: () => {
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    await waitFor(() => fakes.length === 1)

    const p = t.sendMut(mut("T3", "a", "hi"))
    const outcome = p.then(() => "resolved", (e) => e)
    await waitFor(() => fakes[0]!.sent.some((f) => f.t === "mut" && (f as { txId?: string }).txId === "T3"))

    fakes[0]!.emit("close", { code: 4403, reason: "removed from this workspace" }) // terminal
    const r = await outcome
    expect(r).toBeInstanceOf(ConnectionLostError) // no reconnect will ever replay it
    await waitFor(() => closed.length === 1)
    expect(closed[0]).toEqual([4403, "removed from this workspace"])
    t.close()
  })

  it("unsubscribing the LAST sub while a reconnect is pending settles the parked mut typed and cancels the reconnect", async () => {
    const fakes: Array<Fake> = []
    let opens = 0
    const t = new WebSocketTransport({
      url: "wss://x",
      reconnectDelay: () => 30, // long enough to unsubscribe before it fires
      timeoutMs: 60_000,
      open: () => {
        opens++
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    await waitFor(() => opens === 1)

    const p = t.sendMut(mut("T5", "a", "hi"))
    const outcome = p.then(() => "resolved", (e) => e)
    await waitFor(() => fakes[0]!.sent.some((f) => f.t === "mut" && (f as { txId?: string }).txId === "T5"))

    fakes[0]!.emit("close", { code: 1006 }) // parked, reconnect scheduled (30ms)
    t.unsubscribe("s1") // last sub gone BEFORE the timer fires

    // The parked mut settles typed now — no replay against a subless reconnect.
    expect(await outcome).toBeInstanceOf(ConnectionLostError)
    // And the reconnect was cancelled: no second socket ever opens.
    await new Promise((r) => setTimeout(r, 60))
    expect(opens).toBe(1)
    t.close()
  })

  it("unsubscribing the last sub while the reconnect HANDSHAKE is already in flight still settles typed and never replays", async () => {
    const fakes: Array<Fake> = []
    let releaseSecond: (() => void) | null = null
    const t = new WebSocketTransport({
      url: "wss://x",
      reconnectDelay: () => 5, // let the timer fire, so open() #2 is in flight
      timeoutMs: 60_000,
      open: () => {
        if (fakes.length === 0) {
          const f = makeFake()
          fakes.push(f)
          return f.ws
        }
        // Second dial (the reconnect): park it on open() so we can unsubscribe
        // AFTER the timer fired but BEFORE the socket installs.
        return new Promise<WebSocketLike>((res) => {
          const f = makeFake()
          fakes.push(f)
          releaseSecond = () => res(f.ws)
        })
      },
    })
    await t.subscribe("s1", "messages", noopHandler())
    await waitFor(() => fakes.length === 1)

    const p = t.sendMut(mut("T6", "a", "hi"))
    const outcome = p.then(() => "resolved", (e) => e)
    await waitFor(() => fakes[0]!.sent.some((f) => f.t === "mut" && (f as { txId?: string }).txId === "T6"))

    fakes[0]!.emit("close", { code: 1006 }) // park + schedule reconnect
    await waitFor(() => releaseSecond !== null) // the reconnect handshake is now in flight

    t.unsubscribe("s1") // last sub gone WHILE open() #2 is parked
    // The parked mut settles typed immediately — it does not wait on the handshake.
    expect(await outcome).toBeInstanceOf(ConnectionLostError)

    releaseSecond!() // the handshake resolves; the socket may install…
    await new Promise((r) => setTimeout(r, 20))
    // …but it MUST NOT resubscribe or replay: no sub frame, no mut frame on it.
    expect(fakes[1]!.sent.some((f) => f.t === "mut")).toBe(false)
    expect(fakes[1]!.sent.some((f) => f.t === "sub")).toBe(false)
    t.close()
  })

  it("rejects a CONCURRENT duplicate in-flight txId loud (never corrupts the first waiter)", async () => {
    const fake = makeFake()
    const t = new WebSocketTransport({ url: "wss://x", open: () => fake.ws, timeoutMs: 60_000 })
    await t.subscribe("s1", "messages", noopHandler())

    const first = t.sendMut(mut("DUP", "a", "one"))
    await waitFor(() => fake.sent.some((f) => f.t === "mut"))
    // Second call reuses the in-flight txId → rejected loud, first left intact.
    const second = await t.sendMut(mut("DUP", "b", "two")).then(() => "resolved", (e) => e)
    expect(second).toBeInstanceOf(MutationRejectedError)
    expect((second as MutationRejectedError).code).toBe("DUPLICATE_TXID")

    // The FIRST waiter still settles from its receipt — not evicted by the second.
    fake.emit("message", { data: codec.encode({ t: "committed", txId: "DUP", seq: "1" } as ServerFrame) })
    await expect(first).resolves.toEqual({ result: undefined })
    t.close()
  })

  it("timeout semantics are UNCHANGED for a socket that stays open (generic confirmation timeout, not ConnectionLostError)", async () => {
    const fake = makeFake()
    const t = new WebSocketTransport({ url: "wss://x", open: () => fake.ws, timeoutMs: 30 })
    await t.subscribe("s1", "messages", noopHandler())

    const r = await t.sendMut(mut("T4", "a", "hi")).then(() => "resolved", (e) => e)
    expect(r).toBeInstanceOf(Error)
    expect(r).not.toBeInstanceOf(ConnectionLostError) // the socket never dropped
    expect((r as Error).message).toMatch(/confirmation timeout/)
    t.close()
  })
})

// --- Full-stack reconciliation (real DO, real dedup table) ------------------
//
// The sharp edge, end-to-end: a wrapper socket SWALLOWS the server's receipt for
// the in-flight txId, then the server-side socket is dropped — reproducing
// "committed server-side, socket died before `committed` arrived". On reconnect
// the transport replays the txId and the REAL dedup table answers with the true
// recorded outcome.

interface Wrapped extends WebSocketLike {
  swallowedReceipt: boolean
}

/** Wrap a real accepted client socket so that the FIRST `committed`/`rejected`
 *  for `txId` is dropped before it reaches the transport — the receipt is lost in
 *  flight while the server has already recorded the outcome. */
function dropReceiptFor(real: WebSocketLike, txId: string): Wrapped {
  const wrapped: Wrapped = {
    swallowedReceipt: false,
    send: (d) => real.send(d),
    close: (code, reason) => real.close(code, reason),
    addEventListener: (type, l) => {
      if (type !== "message") return real.addEventListener(type, l)
      real.addEventListener("message", (ev) => {
        if (!wrapped.swallowedReceipt) {
          try {
            const f = codec.decode(ev.data as ArrayBuffer | string) as ServerFrame
            if ((f.t === "committed" || f.t === "rejected") && f.txId === txId) {
              wrapped.swallowedReceipt = true
              return // swallow: the client never sees this receipt
            }
          } catch {
            /* not a frame we care about; fall through */
          }
        }
        l(ev)
      })
    },
    removeEventListener: (type, l) => real.removeEventListener(type, l),
  }
  return wrapped
}

async function openReal(room: string): Promise<WebSocketLike> {
  const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
  const ws = res.webSocket
  if (!ws) throw new Error("no webSocket")
  ws.accept()
  return ws as unknown as WebSocketLike
}

async function dropServerSockets(room: string): Promise<void> {
  await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, state) => {
    for (const sock of state.getWebSockets()) sock.close(1000, "drop")
  })
}

describe("in-flight mut settlement across an unexpected close (#39) — full-stack via dedup replay", () => {
  it("drop-after-commit-before-receipt: the mut RESOLVES committed after reconnect — never a timeout rollback", async () => {
    const room = "p39-commit"
    const wrappers: Array<Wrapped> = []
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 20,
      timeoutMs: 60_000, // large: prove the settlement is the reconnect+replay, not a timeout
      open: async () => {
        const real = await openReal(room)
        // Only the FIRST socket drops the receipt; the reconnect socket is clean.
        if (wrappers.length === 0) {
          const w = dropReceiptFor(real, "TC")
          wrappers.push(w)
          return w
        }
        return real
      },
    })
    await t.connect()
    await t.subscribe("s1", "messages", noopHandler())

    // Fire the mutation. The server commits + records it, broadcasts the delta,
    // then sends `committed` — which the wrapper swallows.
    const p = t.sendMut(mut("TC", "row", "landed"))
    let settled = false
    const outcome = p.then((v) => ((settled = true), v), (e) => ((settled = true), e))
    await waitFor(() => wrappers[0]!.swallowedReceipt) // receipt lost in flight

    expect(settled).toBe(false) // not rejected on the drop — held for the true answer
    await dropServerSockets(room) // socket dies → parked → reconnect → replay

    // The dedup table answers the replayed txId with the recorded `committed`.
    await expect(outcome).resolves.toBeDefined()
    // Sanity: the row really is committed server-side.
    await runInDurableObject(env.SYNC_DO.get(env.SYNC_DO.idFromName(room)), (_i, s) => {
      const rows = Array.from(s.storage.sql.exec("SELECT body FROM messages WHERE id = 'row'"))
      expect(rows[0]).toEqual({ body: "landed" })
    })
    t.close()
  })

  it("drop-after-reject-before-receipt: the mut settles with the TRUE MutationRejectedError after reconnect", async () => {
    const room = "p39-reject"
    const wrappers: Array<Wrapped> = []
    const t = new WebSocketTransport({
      url: `https://example.com/sync/${room}`,
      reconnectDelay: 20,
      timeoutMs: 60_000,
      open: async () => {
        const real = await openReal(room)
        if (wrappers.length === 0) {
          const w = dropReceiptFor(real, "TR")
          wrappers.push(w)
          return w
        }
        return real
      },
    })
    await t.connect()
    await t.subscribe("s1", "messages", noopHandler())

    // FORBIDDEN body → the server records a rejection; the wrapper swallows it.
    const p = t.sendMut(mut("TR", "row", "FORBIDDEN"))
    const outcome = p.then(() => "resolved", (e) => e)
    await waitFor(() => wrappers[0]!.swallowedReceipt)

    await dropServerSockets(room)

    // Replay resolves the "unknown" into the true recorded rejection — not a
    // ConnectionLostError, and not a spurious commit.
    const r = await outcome
    expect(r).toBeInstanceOf(MutationRejectedError)
    t.close()
  })
})
