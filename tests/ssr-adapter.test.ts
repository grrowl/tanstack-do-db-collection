import { describe, expect, it } from "vitest"
import { doCollectionOptions, type DoSyncMeta } from "../src/client/do-collection.ts"
import type { Transport } from "../src/client/transport.ts"

// WHY (ADR-0011 D3, adapter-level ordering): on-demand readiness must GATE on
// the transient hydration catch-up sub being SENT — loadSubset subs only fire
// after ready, so on the single ordered socket the catch-up's truncate/deltas
// always precede subset snapshots. A markReady racing ahead (the bug: its
// connect().then() was registered first) lets a subset snapshot land at a seq
// the catch-up then stomps over — or, below the floor, lets the catch-up's
// truncate WIPE an already-loaded subset. Also pins the syncMeta hook
// contract: export shape, import validation, where-fingerprint downgrade,
// min-merge.

interface Msg {
  id: string
  body: string
}

/** Structural schema Api the branded transport carries, so `doCollectionOptions`
 *  infers Row = Msg for `table: "messages"` (matches `RowOf`/`CollectionName`). */
type Api = { collections: { messages: { __row?: Msg } } }

type Hooked = {
  sync: {
    sync: (p: unknown) => unknown
    exportSyncMeta: () => DoSyncMeta
    importSyncMeta: (m: unknown) => void
    mergeSyncMeta: (a: unknown, b: unknown) => DoSyncMeta
  }
}

function spyTransport(calls: Array<string>): Transport<Api> {
  return {
    connect: async () => {
      calls.push("connect")
    },
    subscribe: async (subId, _collection, _handler, _where, _orderBy, _limit, since) => {
      calls.push(`sub:${subId}:since=${since ?? "none"}`)
    },
    unsubscribe: (subId: string) => {
      calls.push(`unsub:${subId}`)
    },
    sendMut: () => Promise.reject(new Error("unused")),
    fetch: () => Promise.reject(new Error("unused")),
    close: () => {},
    appliedCursor: "7",
    seedCursor: () => {
      calls.push("seed")
    },
  }
}

const controls = {
  collection: { get: () => undefined },
  begin: () => {},
  write: () => {},
  commit: () => {},
  truncate: () => {},
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0))

describe("hydrated on-demand start ordering", () => {
  it("ready waits for the catch-up sub to be SENT; the catch-up precedes any subset sub", async () => {
    const calls: Array<string> = []
    const opts = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    opts.sync.importSyncMeta({ v: 1, cursor: "5" })
    opts.sync.sync({ ...controls, markReady: () => calls.push("ready") })
    await flush()

    const catchup = calls.findIndex((c) => c.startsWith("sub:messages#hydrate#") && c.endsWith("since=5"))
    const ready = calls.indexOf("ready")
    expect(catchup).toBeGreaterThanOrEqual(0)
    expect(ready).toBeGreaterThan(catchup)
  })

  it("without hydration there is no catch-up sub and ready follows connect", async () => {
    const calls: Array<string> = []
    const opts = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    opts.sync.sync({ ...controls, markReady: () => calls.push("ready") })
    await flush()
    expect(calls.filter((c) => c.startsWith("sub:"))).toEqual([])
    expect(calls).toContain("ready")
  })

  it("no resume point ('0'): hydrated rows are truncated, not left to go stale", async () => {
    const calls: Array<string> = []
    const truncated: Array<string> = []
    const opts = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      syncMode: "on-demand",
    }) as unknown as Hooked
    opts.sync.importSyncMeta({ v: 1, cursor: "0" })
    opts.sync.sync({
      ...controls,
      truncate: () => truncated.push("truncate"),
      markReady: () => calls.push("ready"),
    })
    await flush()
    expect(truncated).toEqual(["truncate"])
    expect(calls.filter((c) => c.startsWith("sub:"))).toEqual([]) // no unfiltered full snapshot
    expect(calls).toContain("ready")
  })
})

describe("syncMeta hooks", () => {
  const eq = (field: string, value: unknown): unknown => ({
    type: "func",
    name: "eq",
    args: [
      { type: "ref", path: [field] },
      { type: "val", value },
    ],
  })

  function makeOpts(where?: unknown): Hooked {
    return doCollectionOptions({
      transport: spyTransport([]),
      table: "messages",
      getKey: (r) => r.id,
      where,
    }) as unknown as Hooked
  }

  it("export round-trips through import; the eager where is fingerprinted", () => {
    const a = makeOpts(eq("body", "keep"))
    const meta = a.sync.exportSyncMeta()
    expect(meta).toMatchObject({ v: 1, cursor: "7" })
    expect(typeof meta.where).toBe("string")
    a.sync.importSyncMeta(meta) // same fingerprint: accepted (no throw)
  })

  it("a DIFFERENT where downgrades the cursor to the snapshot-reconcile path", async () => {
    const calls: Array<string> = []
    const renderSide = makeOpts(eq("body", "keep"))
    const meta = renderSide.sync.exportSyncMeta()

    const clientSide = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
      where: eq("body", "other"),
    }) as unknown as Hooked
    clientSide.sync.importSyncMeta(meta)
    expect(calls).not.toContain("seed") // an unsound cursor is never claimed
    clientSide.sync.sync({ ...controls, markReady: () => {} })
    await flush()
    // The eager sub must NOT resume from the foreign cursor.
    expect(calls.some((c) => c.startsWith("sub:") && c.endsWith("since=none"))).toBe(true)
  })

  it("rejects meta it does not understand — never resumes from garbage", () => {
    const o = makeOpts()
    expect(() => o.sync.importSyncMeta({ v: 2, cursor: "5" })).toThrow(/unrecognized sync meta/)
    expect(() => o.sync.importSyncMeta({ v: 1, cursor: "not-a-seq" })).toThrow()
    expect(() => o.sync.importSyncMeta(null)).toThrow(/unrecognized sync meta/)
  })

  it("unrecognized meta fails loud BUT safe: the rows already landed, so sync still reconciles", async () => {
    // Upstream applies the chunk's rows BEFORE importSyncMeta — a throw can't
    // veto them. If the throw also skipped our bookkeeping, sync would start
    // down the non-hydrated path and a server-deleted hydrated row would be
    // stale forever. The throw must leave the safe state behind: no resume
    // point ("0") → snapshot + reconcile.
    const calls: Array<string> = []
    const o = doCollectionOptions({
      transport: spyTransport(calls),
      table: "messages",
      getKey: (r) => r.id,
    }) as unknown as Hooked
    expect(() => o.sync.importSyncMeta({ v: 99, cursor: "5" })).toThrow(/unrecognized sync meta/)
    expect(calls).not.toContain("seed") // a cursor we can't read is never claimed
    o.sync.sync({ ...controls, markReady: () => {} })
    await flush()
    // Snapshot path (no since) — where the always-armed eager reconcile lives.
    expect(calls.some((c) => c.startsWith("sub:") && c.endsWith("since=none"))).toBe(true)
  })

  it("merge takes the EARLIER cursor — replay is idempotent, skipping is not", () => {
    const o = makeOpts()
    const merged = o.sync.mergeSyncMeta({ v: 1, cursor: "90" }, { v: 1, cursor: "100" })
    expect(merged.cursor).toBe("90")
    expect(o.sync.mergeSyncMeta({ v: 1, cursor: "100" }, { v: 1, cursor: "90" }).cursor).toBe("90")
  })
})
