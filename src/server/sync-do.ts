// SyncDurableObject — hibernating-WebSocket base class (ADR-0001 D13).
//
// Provides the lifecycle every sync-enabled DO shares:
//   - WebSocket upgrade with a subclass-typed attachment (parseAttachment),
//     bound via serializeAttachment so identity survives hibernation.
//   - ctx.acceptWebSocket (NOT addEventListener) for hibernation support.
//   - "ping"/"pong" auto-response registered once in the constructor — does not
//     wake or bill the DO.
//   - inbound frame decode (binary/JSON) dispatched to an onFrame hook.
//   - lazy schema + trigger init from the collection registry.
//
// Frame handling (sub/mut/call -> snap/d/committed/...) arrives in M3; this
// milestone establishes the lifecycle and the wire decode/encode path.

import { DurableObject } from "cloudflare:workers"
import type { SqlStorage, SqlStorageValue } from "@cloudflare/workers-types"
import { createFrameCodec, type FrameCodec } from "../wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../wire/frames.ts"
import {
  compactChanges,
  currentSeq,
  getDrainCursor,
  hydrateRows,
  initSchema,
  installTriggers,
  minChangeSeq,
  readChangesSince,
  setDrainCursor,
} from "./changes.ts"
import { Broadcaster } from "./broadcast.ts"
import { decodeResult, encodeResult, lookupTx, recordTx, type SeenTx, sweepDedup } from "./dedup.ts"
import type { Registry } from "./registry.ts"
import { compileSubsetQuery, UnsupportedPredicateError } from "./sql-compiler.ts"
import { SubscriptionRegistry } from "./subscriptions.ts"

export abstract class SyncDurableObject<Env = unknown, TUser = unknown> extends DurableObject<Env> {
  /** Subclasses declare their collections, mutations, and commands. */
  protected abstract registry: Registry<TUser>

  /** Wire codec. Binary MessagePack by default; override for a JSON transport. */
  protected readonly codec: FrameCodec = createFrameCodec()

  protected readonly subs = new SubscriptionRegistry()
  /** Egress coalescer tick (ms) — the single user-perceived-latency knob. */
  protected readonly tickMs: number = 50
  /** Compact the change log every this-many drained mutations (not on a timer —
   *  an alarm would wake idle DOs; this rides recent work). */
  protected readonly compactionEvery: number = 200
  /** Dedup retention window (ms), independent of changelog retention (C5). */
  protected readonly dedupRetentionMs: number = 3_600_000
  private writesSinceCompaction = 0
  protected readonly broadcaster: Broadcaster
  private readonly liveWs = new Set<WebSocket>()
  private schemaReady = false

  constructor(ctx: ConstructorParameters<typeof DurableObject>[0], env: Env) {
    super(ctx, env)
    // Auto-pong via the runtime: survives hibernation, no per-message billing.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"))
    // Restore the live-socket set after a hibernation wake.
    for (const ws of this.ctx.getWebSockets()) this.liveWs.add(ws)
    this.broadcaster = new Broadcaster((ws, frame) => this.send(ws, frame), this.tickMs)
    this.broadcaster.start(() => this.liveWs)
  }

  protected get sql(): SqlStorage {
    return this.ctx.storage.sql
  }

  /** Lazily create framework schema + per-collection table/triggers. Idempotent. */
  protected initRegistry(): void {
    if (this.schemaReady) return
    initSchema(this.sql)
    for (const c of this.registry.collections.values()) {
      this.sql.exec(c.ddl)
      installTriggers(this.sql, c.table, c.pk)
    }
    this.schemaReady = true
  }

  /**
   * Validate the upgrade and produce the attachment bound to the WebSocket
   * (available as `userFor(ws)` in handlers). Override to read a Worker-forged
   * claims header and/or reject by throwing a `Response`. Default: no identity.
   */
  protected parseAttachment(_req: Request): TUser | Promise<TUser> {
    return undefined as TUser
  }

  override async fetch(req: Request): Promise<Response> {
    if (req.headers.get("Upgrade") !== "websocket") {
      return new Response("expected websocket upgrade", { status: 426 })
    }

    let attachment: TUser
    try {
      attachment = await this.parseAttachment(req)
    } catch (e) {
      if (e instanceof Response) return e
      return new Response("unauthorized", { status: 401 })
    }

    this.initRegistry()

    const pair = new WebSocketPair()
    const client = pair[0]
    const server = pair[1]
    server.serializeAttachment(attachment)
    this.ctx.acceptWebSocket(server)
    this.liveWs.add(server)

    return new Response(null, { status: 101, webSocket: client })
  }

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    // "ping"/"pong" are handled by the auto-response and never arrive here.
    let frame: ClientFrame
    try {
      frame = this.codec.decode(message) as ClientFrame
    } catch {
      return // ignore undecodable frames
    }
    await this.dispatch(ws, frame)
  }

  override webSocketClose(ws: WebSocket): void {
    this.subs.removeAll(ws)
    this.liveWs.delete(ws)
  }

  override webSocketError(ws: WebSocket): void {
    this.subs.removeAll(ws)
    this.liveWs.delete(ws)
  }

  private async dispatch(ws: WebSocket, frame: ClientFrame): Promise<void> {
    switch (frame.t) {
      case "sub":
        return this.handleSub(ws, frame)
      case "unsub":
        this.subs.remove(ws, frame.subId)
        return
      case "mut":
        return this.handleMut(ws, frame)
      case "call":
        return this.handleCall(ws, frame)
      case "fetch":
        return this.handleFetch(ws, frame)
    }
  }

  /** One-shot paginated page fetch — a subset snapshot, NO live registration.
   *  Used by the client for cursor load-more; the window's live deltas already
   *  flow via the `sub` on the query's `where`.
   *
   *  The cursor double-read (`ties` boundary-equals, unbounded; `where`
   *  next-page, bounded by `limit`) runs as TWO SELECTs in ONE handler turn:
   *  synchronous SQLite, no `await` between them, so both observe the same
   *  database at one `seq`. The page therefore slots into the delta stream at a
   *  single position — a concurrent mutation is either reflected in it or
   *  arrives as a delta AFTER it, never split across the two reads (ADR-0003). */
  private handleFetch(ws: WebSocket, frame: Extract<ClientFrame, { t: "fetch" }>): void {
    const coll = this.registry.collections.get(frame.collection)
    if (!coll) {
      this.send(ws, { t: "page", fetchId: frame.fetchId, rows: [], seq: "0" })
      return
    }
    const seq = String(currentSeq(this.sql))
    try {
      const rows: Array<unknown> = []
      // Ties first (unbounded boundary set), then the bounded next page.
      if (frame.ties != null) {
        const tq = compileSubsetQuery(frame.collection, { where: frame.ties, orderBy: frame.orderBy })
        rows.push(...Array.from(this.sql.exec(tq.sql, ...tq.params)))
      }
      const nq = compileSubsetQuery(frame.collection, {
        where: frame.where,
        orderBy: frame.orderBy,
        limit: frame.limit,
      })
      rows.push(...Array.from(this.sql.exec(nq.sql, ...nq.params)))
      this.send(ws, { t: "page", fetchId: frame.fetchId, rows, seq })
    } catch (e) {
      if (e instanceof UnsupportedPredicateError) {
        console.error(`fetch '${frame.fetchId}' on '${frame.collection}' rejected: ${e.message}`)
        this.send(ws, { t: "page", fetchId: frame.fetchId, rows: [], seq })
        return
      }
      throw e
    }
  }

  /**
   * Apply a mutation atomically and confirm on the single ordered stream.
   *
   * Order is the load-bearing invariant (ADR-0002 C1): this connection's
   * matched deltas are flushed BEFORE its `committed` frame, so the client's
   * single cursor only ever advances over a contiguous applied prefix and the
   * optimistic overlay is never dropped before the authoritative row lands.
   * With no egress coalescer yet (M4), `drainAndBroadcast` sends deltas
   * synchronously here; M4 must preserve this by flushing the originating
   * socket before `committed`.
   */
  private async handleMut(ws: WebSocket, f: Extract<ClientFrame, { t: "mut" }>): Promise<void> {
    const seen = lookupTx(this.sql, f.txId)
    if (seen) return this.replayReceipt(ws, f.txId, seen)

    const user = this.userFor(ws)

    // Authorize every op BEFORE the transaction (may be async).
    try {
      for (const op of f.ops) {
        const def = this.registry.mutations.get(`${f.collection}:${op.type}`)
        if (!def) throw new Error(`no mutation handler for '${f.collection}:${op.type}'`)
        if (def.authorize) await def.authorize({ user, op, sql: this.sql })
      }
    } catch (e) {
      return this.rejectTx(ws, f.txId, errorMessage(e))
    }

    // Apply all ops in one synchronous transaction (atomic with the trigger
    // rows). A handler that returns a Promise is a programming error.
    let commitSeq: string
    try {
      this.ctx.storage.transactionSync(() => {
        for (const op of f.ops) {
          const def = this.registry.mutations.get(`${f.collection}:${op.type}`)!
          const result = def.execute({ user, op, sql: this.sql }) as unknown
          if (result !== undefined && typeof (result as PromiseLike<unknown>).then === "function") {
            throw new Error(`mutation '${f.collection}:${op.type}' execute must be synchronous`)
          }
        }
      })
      commitSeq = String(currentSeq(this.sql))
    } catch (e) {
      return this.rejectTx(ws, f.txId, errorMessage(e))
    }

    recordTx(this.sql, f.txId, true, commitSeq, null, null)
    // Enqueue deltas for all subscribers, then flush THIS socket before its
    // receipt (C1) so its deltas land first. Other subscribers flush on the
    // coalescer tick.
    this.drainAndBroadcast()
    this.broadcaster.flushOne(ws)
    this.send(ws, { t: "committed", txId: f.txId, seq: commitSeq })
  }

  /** Run a named command (outside any transaction) and confirm with its result. */
  private async handleCall(ws: WebSocket, f: Extract<ClientFrame, { t: "call" }>): Promise<void> {
    const seen = lookupTx(this.sql, f.txId)
    if (seen) return this.replayReceipt(ws, f.txId, seen)

    const def = this.registry.commands.get(f.name)
    if (!def) return this.rejectTx(ws, f.txId, `unknown command '${f.name}'`, "UNKNOWN_COMMAND")

    const user = this.userFor(ws)
    let result: unknown
    try {
      if (def.authorize) await def.authorize({ user, args: f.args, sql: this.sql })
      result = await def.execute({ user, args: f.args, sql: this.sql })
    } catch (e) {
      return this.rejectTx(ws, f.txId, errorMessage(e))
    }

    // Serialize the result for dedup replay BEFORE recording success. A
    // non-serializable result can't be replayed, so record an error rather
    // than risk re-running the command's side effects on retry.
    let stored: string | null
    try {
      stored = encodeResult(result)
    } catch (e) {
      return this.rejectTx(ws, f.txId, `non-serializable command result: ${errorMessage(e)}`, "NON_SERIALIZABLE")
    }

    const commitSeq = String(currentSeq(this.sql))
    recordTx(this.sql, f.txId, true, commitSeq, null, stored)
    this.drainAndBroadcast()
    this.broadcaster.flushOne(ws)
    this.send(ws, { t: "committed", txId: f.txId, seq: commitSeq, result })
  }

  private rejectTx(ws: WebSocket, txId: string, message: string, code?: string): void {
    recordTx(this.sql, txId, false, null, message, null)
    this.send(ws, { t: "rejected", txId, error: code ? { code, message } : { message } })
  }

  private replayReceipt(ws: WebSocket, txId: string, seen: SeenTx): void {
    if (seen.ok) {
      this.send(ws, { t: "committed", txId, seq: seen.cursor ?? "0", result: decodeResult(seen.result) })
    } else {
      this.send(ws, { t: "rejected", txId, error: { message: seen.error ?? "unknown" } })
    }
  }

  /**
   * Drain `_sync_changes` from the last broadcast watermark, fan out one `d`
   * per changed key to each subscriber of the affected collection, then a
   * single `uptodate` boundary per touched socket. Multiple changes to a key
   * within the drain collapse to the latest op.
   */
  protected drainAndBroadcast(): void {
    const sql = this.sql
    const last = getDrainCursor(sql)
    const changes = readChangesSince(sql, last)
    if (changes.length === 0) return
    const cursor = String(changes[changes.length - 1]!.seq)

    const byTable = new Map<string, Array<(typeof changes)[number]>>()
    for (const c of changes) {
      let arr = byTable.get(c.tbl)
      if (!arr) {
        arr = []
        byTable.set(c.tbl, arr)
      }
      arr.push(c)
    }

    for (const [tbl, tableChanges] of byTable) {
      const coll = this.registry.collections.get(tbl)
      if (!coll) continue
      const latest = new Map<string, (typeof tableChanges)[number]>()
      for (const c of tableChanges) latest.set(c.key, c)
      const liveKeys = [...latest.values()].filter((c) => c.op !== "delete").map((c) => c.key)
      const hydrated = hydrateRows(sql, tbl, coll.pk, liveKeys)

      // Enqueue into the coalescer; it flushes one `d` per surviving key plus a
      // single `uptodate` boundary per socket (on the tick, or via flushOne).
      for (const { ws, sub } of this.subs.forCollection(tbl)) {
        for (const [key, change] of latest) {
          const row = hydrated.get(key)
          // Always-emit rule (no before-image, ADR-0002 C4): a key that is
          // deleted, gone, or no longer matches this sub's predicate -> a
          // synthetic delete (idempotent; move-out). A matching live row ->
          // its current state with the actual op (move-in via update upserts
          // on the client — verified). Predicate is always-true when unfiltered.
          if (change.op === "delete" || !row || !sub.predicate(row)) {
            this.broadcaster.enqueue(ws, { subId: sub.subId, key, op: "delete" }, cursor)
          } else {
            // Full row as the partial patch; column-level diffs arrive later.
            this.broadcaster.enqueue(ws, { subId: sub.subId, key, op: change.op, cols: row }, cursor)
          }
        }
      }
    }

    setDrainCursor(sql, changes[changes.length - 1]!.seq)
    this.maybeCompact()
  }

  /**
   * Opportunistic GC: every `compactionEvery` drained mutations, collapse the
   * change log to latest-op-per-key and sweep expired dedup entries. Deferred
   * via `ctx.waitUntil` so it rides just after a burst of work — it never
   * blocks a mutation's response, and (unlike an alarm) never wakes an idle DO.
   * `waitUntil` keeps the DO alive until it completes.
   */
  private maybeCompact(): void {
    if (++this.writesSinceCompaction < this.compactionEvery) return
    this.writesSinceCompaction = 0
    this.ctx.waitUntil(
      (async (): Promise<void> => {
        compactChanges(this.sql)
        sweepDedup(this.sql, this.dedupRetentionMs, Date.now())
      })(),
    )
  }

  /** Full-collection subscribe: emit every current row as a snapshot, then a
   *  boundary. Predicate/subset shaping arrives in M5/M6. */
  private handleSub(ws: WebSocket, frame: Extract<ClientFrame, { t: "sub" }>): void {
    const coll = this.registry.collections.get(frame.collection)
    if (!coll) {
      // Unknown collection: drop the subscriber's view. Richer sub-error
      // signalling is deferred; for now reset is the honest minimum.
      this.send(ws, { t: "reset", sub: frame.subId })
      return
    }
    // Lower where/orderBy/limit/offset into SQLite. An un-lowerable predicate
    // (outside the supported floor) is rejected, not silently full-scanned.
    let query: { sql: string; params: Array<unknown> }
    try {
      query = compileSubsetQuery(frame.collection, {
        where: frame.where,
        orderBy: frame.orderBy,
        limit: frame.limit,
        offset: frame.offset,
      })
    } catch (e) {
      if (e instanceof UnsupportedPredicateError) {
        console.error(`sub '${frame.subId}' on '${frame.collection}' rejected: ${e.message}`)
        this.send(ws, { t: "reset", sub: frame.subId })
        return
      }
      throw e
    }

    const sub = this.subs.add(ws, frame.subId, frame.collection, frame.where)
    const seq = String(currentSeq(this.sql))

    // Reconnect catch-up: a `since` cursor asks for changes after that point
    // rather than a fresh snapshot. Serve a windowed delta while the change log
    // still reaches back that far; otherwise fall back to reset + snapshot
    // (the retention floor; exercised once compaction prunes — M7 next).
    const since = frame.since != null ? Number(frame.since) : 0
    if (since > 0) {
      const floor = minChangeSeq(this.sql)
      if (floor === 0 || since >= floor - 1) {
        this.emitCatchUp(ws, sub, coll, since, seq)
        return
      }
      this.send(ws, { t: "reset", sub: frame.subId })
    }

    const rows = Array.from(this.sql.exec(query.sql, ...query.params)) as Array<Record<string, SqlStorageValue>>
    for (const row of rows) {
      this.send(ws, { t: "snap", sub: frame.subId, key: row[coll.pk], row, seq })
    }
    this.send(ws, { t: "snap-end", sub: frame.subId, seq })
  }

  /** Windowed catch-up: the latest op per changed key since `since`, resolved
   *  through the sub's predicate (move-in/out), then an `uptodate` boundary. */
  private emitCatchUp(
    ws: WebSocket,
    sub: { subId: string; predicate: (row: Record<string, unknown>) => boolean },
    coll: { table: string; pk: string },
    since: number,
    seq: string,
  ): void {
    const changes = readChangesSince(this.sql, since).filter((c) => c.tbl === coll.table)
    const latest = new Map<string, (typeof changes)[number]>()
    for (const c of changes) latest.set(c.key, c)
    const liveKeys = [...latest.values()].filter((c) => c.op !== "delete").map((c) => c.key)
    const hydrated = hydrateRows(this.sql, coll.table, coll.pk, liveKeys)
    for (const [key, change] of latest) {
      const row = hydrated.get(key)
      if (change.op === "delete" || !row || !sub.predicate(row)) {
        this.send(ws, { t: "d", sub: sub.subId, key, op: "delete", seq })
      } else {
        this.send(ws, { t: "d", sub: sub.subId, key, op: change.op, cols: row, seq })
      }
    }
    this.send(ws, { t: "uptodate", seq })
  }

  /** Encode and send a server frame on one socket. */
  protected send(ws: WebSocket, frame: ServerFrame): void {
    ws.send(this.codec.encode(frame))
  }

  /** The attachment bound at upgrade, surviving hibernation. */
  protected userFor(ws: WebSocket): TUser {
    return ws.deserializeAttachment() as TUser
  }

  /** Live sockets — used by the broadcaster (M4) for fan-out. */
  protected getLiveWs(): Iterable<WebSocket> {
    return this.liveWs
  }
}

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}
