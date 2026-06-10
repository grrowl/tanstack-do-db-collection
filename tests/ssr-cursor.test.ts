import { env, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { type SubHandler, WebSocketTransport, type WebSocketLike } from "../src/client/transport.ts"

// WHY (ADR-0011 D3): SSR hydration hands a client rows it did not stream — so
// the FIRST sub must be able to resume from the dehydrated cursor (server
// catch-up, not a redundant snapshot), and the transport must be able to claim
// that position before/around live traffic:
//   - seedCursor before any advance: a drop in the bootstrap window otherwise
//     resubscribes from 0 → fresh snapshot over hydrated rows → a row deleted
//     server-side meanwhile is never removed (snapshots carry no tombstones).
//   - seedCursor AFTER live advance (a late streamed SSR chunk): upstream has
//     already applied the chunk's possibly-stale rows — we cannot veto. The
//     transport claims the SHORTER prefix (always safe) and resubscribes, so
//     the catch-up replay re-freshens exactly the clobbered window.

interface Rec {
  events: Array<[string, ...Array<unknown>]>
  handler: SubHandler
}
function recorder(): Rec {
  const events: Array<[string, ...Array<unknown>]> = []
  return {
    events,
    handler: {
      onSnap: (k, r) => events.push(["snap", k, r]),
      onSnapEnd: () => events.push(["snap-end"]),
      onDelta: (op, k, c) => events.push(["d", op, k, c]),
      onUptodate: () => events.push(["uptodate"]),
      onReset: () => events.push(["reset"]),
    },
  }
}

function makeTransport(room: string): WebSocketTransport {
  return new WebSocketTransport({
    url: `https://example.com/sync/${room}`,
    reconnectDelayMs: 20,
    open: async () => {
      const res = await SELF.fetch(`https://example.com/sync/${room}`, { headers: { Upgrade: "websocket" } })
      const ws = res.webSocket
      if (!ws) throw new Error("no webSocket")
      ws.accept()
      return ws as unknown as WebSocketLike
    },
  })
}

async function waitFor(pred: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now()
  while (!pred()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor timeout")
    await new Promise((r) => setTimeout(r, 5))
  }
}

function stubFor(room: string): DurableObjectStub {
  return env.SYNC_DO.get(env.SYNC_DO.idFromName(room))
}

async function snapshotCursor(room: string): Promise<string> {
  const stub = stubFor(room) as unknown as {
    readSnapshot: (r: { collection: string }) => Promise<{ rows: Array<unknown>; cursor: string }>
  }
  return (await stub.readSnapshot({ collection: "messages" })).cursor
}

describe("transport cursor bootstrap (SSR hydration, ADR-0011 D3)", () => {
  it("a FIRST sub carrying `since` gets a catch-up, not a snapshot", async () => {
    const room = `ssr-since-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hydrated')")
    })
    const cursor = await snapshotCursor(room) // what dehydration exported
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('b','missed')")
    })

    const t = makeTransport(room)
    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler, undefined, undefined, undefined, cursor)
    await waitFor(() => events.some((e) => e[0] === "uptodate"))

    // The hydrated row is NOT re-streamed; only the post-cursor change is.
    expect(events.some((e) => e[0] === "snap")).toBe(false)
    expect(events.some((e) => e[0] === "snap-end")).toBe(false)
    expect(events.some((e) => e[0] === "d" && e[2] === "b")).toBe(true)
    expect(events.some((e) => e[0] === "d" && e[2] === "a")).toBe(false)
    t.close()
  })

  it("seedCursor claims the dehydrated position before any advance", async () => {
    const room = `ssr-seed-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','hydrated')")
    })
    const cursor = await snapshotCursor(room)

    const t = makeTransport(room)
    expect(t.appliedCursor).toBe("0")
    t.seedCursor(cursor)
    expect(t.appliedCursor).toBe(cursor)
    // A seed can never grow the claim without data.
    t.seedCursor(String(BigInt(cursor) + 100n))
    expect(t.appliedCursor).toBe(cursor)
    t.close()
  })

  it("a late seed (streamed chunk after live advance) regresses the claim and replays the window", async () => {
    const room = `ssr-late-${crypto.randomUUID()}`
    await runInDurableObject(stubFor(room), (_i, s) => {
      s.storage.sql.exec("INSERT INTO messages(id,body) VALUES('a','v1')")
    })
    const chunkCursor = await snapshotCursor(room) // a chunk dehydrated NOW...

    const t = makeTransport(room)
    const { events, handler } = recorder()
    await t.subscribe("s1", "messages", handler, undefined, undefined, undefined, chunkCursor)
    await waitFor(() => events.some((e) => e[0] === "uptodate"))

    // ...but it arrives LATE: live sync has moved on past another write
    // (driven through a real mut — raw SQL never broadcasts, ADR-0006).
    const t2 = makeTransport(room)
    await t2.sendMut({
      t: "mut",
      txId: `tx-${crypto.randomUUID()}`,
      collection: "messages",
      ops: [{ type: "update", key: "a", cols: { body: "v2" } }],
    })
    await waitFor(() => events.some((e) => e[0] === "d" && e[2] === "a"))
    t2.close()
    const advanced = t.appliedCursor
    expect(BigInt(advanced)).toBeGreaterThan(BigInt(chunkCursor))
    const before = events.length

    // Upstream already applied the chunk's stale rows; the transport claims
    // the shorter prefix and resubscribes — the replayed catch-up delivers the
    // post-chunk window again (idempotent) and re-freshens clobbered rows.
    t.seedCursor(chunkCursor)
    expect(t.appliedCursor).toBe(chunkCursor)
    await waitFor(() => events.slice(before).some((e) => e[0] === "d" && e[2] === "a"))
    await waitFor(() => BigInt(t.appliedCursor) >= BigInt(advanced))
    expect(events.slice(before).some((e) => e[0] === "snap")).toBe(false) // replay, not re-snapshot
    t.close()
  })
})
