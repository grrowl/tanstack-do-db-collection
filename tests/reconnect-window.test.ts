import { describe, expect, it } from "vitest"
import { WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: PRE-EXISTING bug found while grilling ADR-0011's forced-reconnect
// design (the bug itself is in the plain reconnect path, present on this
// branch; the forced-reconnect machinery is not). The `reconnecting` flag was
// set inside the reconnect TIMER, so a connect() triggered on demand — a
// mutation fired within the reconnect delay of a drop — established the fresh
// socket with the flag still false: NO resubscribeAll, every subscription
// silently dead (the server has no subs for the new socket), and the late
// timer's connect() early-returned, wedging the flag. The flag must be set
// when the reconnect is SCHEDULED, so whichever connect() establishes —
// timer-driven or demand-driven — runs the resubscribe path.

const codec = createFrameCodec()

interface Fake {
  ws: WebSocketLike
  sent: Array<ClientFrame>
  emit: (type: string, ev: { data?: unknown }) => void
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

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe("reconnect window (drop → demand-driven connect before the timer)", () => {
  it("a mutation-driven connect still resubscribes from the cursor", async () => {
    const fakes: Array<Fake> = []
    const t = new WebSocketTransport({
      url: "wss://fake",
      reconnectDelay: () => 60_000, // FIXED policy: the timer must NOT be what saves us (jitter could fire early)
      open: () => {
        const f = makeFake()
        fakes.push(f)
        return f.ws
      },
    })
    await t.subscribe(
      "s1",
      "messages",
      { onSnap: () => {}, onSnapEnd: () => {}, onDelta: () => {}, onUptodate: () => {}, onReset: () => {} },
    )
    fakes[0]!.emit("message", { data: codec.encode({ t: "snap-end", sub: "s1", seq: "5" } satisfies ServerFrame) })
    expect(t.appliedCursor).toBe("5")

    // Unexpected drop...
    fakes[0]!.emit("close", {})
    // ...and a mutation fires immediately, long before the reconnect timer.
    const mut = t.sendMut({ t: "mut", txId: "tx1", collection: "messages", ops: [] })
    // Wait for the mut FRAME (not just the socket): the send lands a microtask
    // after open(), and the committed reply below needs its waiter registered.
    await waitFor(() => fakes.length === 2 && fakes[1]!.sent.some((f) => f.t === "mut"))

    // The demand-driven socket must carry the resubscription, from the cursor.
    const resub = fakes[1]!.sent.find((f) => f.t === "sub") as Extract<ClientFrame, { t: "sub" }> | undefined
    expect(resub).toBeDefined()
    expect(resub!.since).toBe("5")

    // Settle the in-flight mutation cleanly, then shut down.
    fakes[1]!.emit("message", { data: codec.encode({ t: "committed", txId: "tx1", seq: "6" } satisfies ServerFrame) })
    await mut
    t.close()
  })
})
