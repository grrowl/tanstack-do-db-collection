import { env, evictDurableObject, runInDurableObject, SELF } from "cloudflare:test"
import { describe, expect, it } from "vitest"
import { createFrameCodec } from "../src/wire/frame-codec.ts"
import { encode as encodeValue } from "../src/wire/codec.ts"
import type { ClientFrame, ServerFrame } from "../src/wire/frames.ts"

// WHY: hibernatable WebSockets SURVIVE a Durable Object eviction — that is the
// entire point of the hibernation API (ADR-0001 D13). The client therefore
// never sees a `close`, never reconnects, and never re-sends its `sub` frames
// (the transport's only re-subscribe trigger is the close path). If the
// server-side subscription registry lives in instance memory only, a wake
// restores the socket set but not the subscriptions: the client's own
// mutations still confirm (`committed` flows fine) while its live queries go
// silently dead on a still-open socket. Reported against 0.5.1 downstream;
// this pins the contract that a subscription on a surviving socket keeps
// receiving deltas across an eviction-and-wake cycle.
//
// `evictDurableObject` (vitest-pool-workers >= 0.16.20; here since the vitest 4
// migration, PR #34) defaults to `webSockets: "hibernate"` — production
// semantics: instance memory torn down, durable storage and hibernatable
// sockets preserved. This is the real-eviction test issue #29 asked for.

const codec = createFrameCodec()

async function openWs(path: string, headers: Record<string, string> = {}): Promise<WebSocket> {
  const res = await SELF.fetch(`https://example.com${path}`, { headers: { Upgrade: "websocket", ...headers } })
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

describe("subscriptions survive hibernation wake", () => {
  it("keeps delivering deltas to a pre-eviction subscriber on its surviving socket", async () => {
    const room = "hib-sub-survives"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))

    // Subscribe on socket A and prove live fan-out works pre-eviction: A's own
    // mutation must come back as a delta before its receipt (ADR-0002 C1).
    const wsA = await openWs(`/sync/${room}`)
    let aClosed = false
    wsA.addEventListener("close", () => {
      aClosed = true
    })
    send(wsA, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(wsA, (f) => f.t === "snap-end")
    send(wsA, {
      t: "mut",
      txId: "pre1",
      collection: "messages",
      ops: [{ type: "insert", key: "pre", cols: { id: "pre", body: "before eviction" } }],
    })
    const preFrames = await collectUntil(wsA, (f) => f.t === "committed")
    expect(preFrames.some((f) => f.t === "d" && f.key === "pre")).toBe(true)

    // Real eviction: instance memory (incl. any in-memory subscription
    // registry) is gone; durable storage and the hibernatable socket survive.
    await evictDurableObject(stub)

    // Premise guard: the harness must NOT have closed A. If this fails, the
    // eviction semantics changed and the pin below would be red for the wrong
    // reason — a closed socket would make the client transport reconnect and
    // re-subscribe, masking the registry loss entirely.
    expect(aClosed).toBe(false)

    // Wake the DO from a DIFFERENT socket (the woken instance must serve B
    // before A has sent anything — A is idle, as in the field report). Start
    // collecting on A BEFORE the write so the delta cannot slip past.
    const wsB = await openWs(`/sync/${room}`)
    const deltaToA = collectUntil(wsA, (f) => f.t === "d" && f.key === "after-wake", 3000)
    send(wsB, {
      t: "mut",
      txId: "post1",
      collection: "messages",
      ops: [{ type: "insert", key: "after-wake", cols: { id: "after-wake", body: "written after wake" } }],
    })

    // B's path is intact either way — the write commits durably.
    await collectUntil(wsB, (f) => f.t === "committed")

    // THE PIN: A's subscription must have survived the wake. Without
    // persistence this times out — the woken registry is empty, so the drain
    // fans out to nobody while A's socket sits open and silent.
    const frames = await deltaToA
    const d = frames.find((f) => f.t === "d" && f.key === "after-wake")
    expect(d).toBeDefined()
    expect(d!.t === "d" && d!.sub).toBe("s1")
    expect(aClosed).toBe(false)

    wsA.close(1000, "done")
    wsB.close(1000, "done")
  })

  // Downstream field shape (their bump-verification case): a FRESH space that
  // hibernates before its first-ever broadcast. Same root cause, but this
  // variant also crosses the never-drained state (`_sync_meta` has no drain
  // cursor yet) with the wake restore — the general test above cannot catch a
  // regression specific to that interaction.
  it("survives eviction that happens before the first-ever broadcast", async () => {
    const room = "hib-first-broadcast"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))

    // Subscribe on a fresh DO: empty snapshot, and deliberately NO mutation —
    // nothing has ever been drained or broadcast when the eviction lands.
    const wsA = await openWs(`/sync/${room}`)
    let aClosed = false
    wsA.addEventListener("close", () => {
      aClosed = true
    })
    send(wsA, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(wsA, (f) => f.t === "snap-end")

    await evictDurableObject(stub)
    expect(aClosed).toBe(false) // premise guard, as above

    const wsB = await openWs(`/sync/${room}`)
    const deltaToA = collectUntil(wsA, (f) => f.t === "d" && f.key === "first", 3000)
    send(wsB, {
      t: "mut",
      txId: "first1",
      collection: "messages",
      ops: [{ type: "insert", key: "first", cols: { id: "first", body: "first ever write" } }],
    })
    await collectUntil(wsB, (f) => f.t === "committed")

    const frames = await deltaToA
    expect(frames.some((f) => f.t === "d" && f.key === "first" && f.sub === "s1")).toBe(true)
    expect(aClosed).toBe(false)

    wsA.close(1000, "done")
    wsB.close(1000, "done")
  })

  // Cohosted base (ADR-0015): the wake restore takes the tagged branch
  // (`getWebSockets(SYNC_TAG)`), not the bare-DO catch-all — the branch the
  // old host-matrix GAP note could only verify by code-reading. A host socket
  // shares the DO across the same eviction; the restore must re-attach the
  // sync socket's subscription and must NOT touch the host's socket.
  it("restores subscriptions over a cohosted base without touching host sockets", async () => {
    const room = "hib-cohosted"
    const stub = env.HOST_DO.get(env.HOST_DO.idFromName(room))

    const hostWs = await openWs(`/host/${room}/_host`)
    let hostGot = 0
    hostWs.addEventListener("message", () => {
      hostGot += 1
    })
    const wsA = await openWs(`/host/${room}/_sync`, { "x-user": "alice" })
    let aClosed = false
    wsA.addEventListener("close", () => {
      aClosed = true
    })
    send(wsA, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(wsA, (f) => f.t === "snap-end")

    await evictDurableObject(stub)
    expect(aClosed).toBe(false) // premise guard, as above

    const wsB = await openWs(`/host/${room}/_sync`, { "x-user": "bob" })
    const deltaToA = collectUntil(wsA, (f) => f.t === "d" && f.key === "co", 3000)
    send(wsB, {
      t: "mut",
      txId: "co1",
      collection: "messages",
      ops: [{ type: "insert", key: "co", cols: { id: "co", body: "cohosted wake" } }],
    })
    await collectUntil(wsB, (f) => f.t === "committed")

    const frames = await deltaToA
    expect(frames.some((f) => f.t === "d" && f.key === "co" && f.sub === "s1")).toBe(true)
    // The sync delta reached A, so the broadcast cycle is complete — and the
    // host's socket saw none of it (it survived the same eviction untouched).
    // Settle one delivery window first: a frame wrongly fanned out to the host
    // could still be in flight when A's delta resolves (codex review).
    await new Promise((r) => setTimeout(r, 100))
    expect(hostGot).toBe(0)

    hostWs.close(1000, "done")
    wsA.close(1000, "done")
    wsB.close(1000, "done")
  })

  // Reconcile-on-restore (ADR-0019 D3, codex review): a durable row whose
  // collection is not in the compiled schema (an older build persisted it,
  // then the schema dropped the collection) must NOT restore into a zombie
  // sub that no drain will ever service. The client gets a `reset` (truncate
  // is the correct terminal state — the collection is gone), the row is
  // deleted, and healthy subs on the same socket are untouched.
  it("resets a restored sub whose collection left the schema; healthy subs unaffected", async () => {
    const room = "hib-absent-collection"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))

    const wsA = await openWs(`/sync/${room}`)
    send(wsA, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(wsA, (f) => f.t === "snap-end")

    // Seed a row for THIS socket's durable id, naming a collection the schema
    // does not register — as if persisted by an older build whose schema had it.
    await runInDurableObject(stub, (_i, s) => {
      const tags = s.getWebSockets().flatMap((w) => s.getTags(w))
      const sid = tags.find((t) => t.startsWith("_tddc.sid:"))!.slice("_tddc.sid:".length)
      s.storage.sql.exec(
        "INSERT INTO _sync_subs(socket_id, sub_id, collection, where_ir) VALUES(?, 's-ghost', 'ghosts', NULL)",
        sid,
      )
    })

    await evictDurableObject(stub)

    // Wake the DO; restore + reconcile run in the constructor. Expect the
    // ghost sub's reset on the surviving socket…
    const resetP = collectUntil(wsA, (f) => f.t === "reset" && f.sub === "s-ghost", 3000)
    const wsB = await openWs(`/sync/${room}`)
    send(wsB, {
      t: "mut",
      txId: "g1",
      collection: "messages",
      ops: [{ type: "insert", key: "g", cols: { id: "g", body: "after ghost" } }],
    })
    await resetP
    // …while the healthy sub still delivers, and the ghost row is gone.
    await collectUntil(wsA, (f) => f.t === "d" && f.key === "g", 3000)
    const ghostRows = await runInDurableObject(stub, (_i, s) =>
      Array.from(s.storage.sql.exec("SELECT sub_id FROM _sync_subs WHERE sub_id = 's-ghost'")),
    )
    expect(ghostRows.length).toBe(0)

    wsA.close(1000, "done")
    wsB.close(1000, "done")
  })

  // Uncompilable restored predicate (ADR-0019 D3, codex review): `reset` would
  // strand the query — the client's onReset truncates but never re-subscribes,
  // while its cursor keeps advancing. The server must instead CLOSE the socket
  // (non-terminal code): reconnect machinery re-subscribes everything
  // recoverable and routes the bad predicate through the normal, app-visible
  // sub rejection. Pinned with the ADR-0013 `ne` operator, which the evaluator
  // floor rejects.
  it("closes the socket when a restored predicate no longer compiles", async () => {
    const room = "hib-bad-predicate"
    const stub = env.SYNC_DO.get(env.SYNC_DO.idFromName(room))

    const wsA = await openWs(`/sync/${room}`)
    const closed = new Promise<{ code: number }>((resolve) => {
      wsA.addEventListener("close", (e) => resolve({ code: (e as CloseEvent).code }))
    })
    send(wsA, { t: "sub", subId: "s1", collection: "messages" })
    await collectUntil(wsA, (f) => f.t === "snap-end")

    // Corrupt this socket's persisted predicate into IR outside the evaluator
    // floor (`ne` — only `not(eq(...))` is supported, ADR-0013).
    const badWhere = encodeValue({
      type: "func",
      name: "ne",
      args: [
        { type: "ref", path: ["body"] },
        { type: "val", value: "x" },
      ],
    })
    await runInDurableObject(stub, (_i, s) => {
      s.storage.sql.exec("UPDATE _sync_subs SET where_ir = ? WHERE sub_id = 's1'", badWhere)
    })

    await evictDurableObject(stub)

    // Any wake attempts the restore; the corrupt row must close A cleanly.
    const wsB = await openWs(`/sync/${room}`)
    const { code } = await closed
    expect(code).toBe(1011) // non-terminal: a real client reconnects + resubscribes
    const rows = await runInDurableObject(stub, (_i, s) =>
      Array.from(s.storage.sql.exec("SELECT sub_id FROM _sync_subs")),
    )
    expect(rows.length).toBe(0) // the retry must re-derive from the client

    wsB.close(1000, "done")
  })
})
