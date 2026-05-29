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
import type { SqlStorage } from "@cloudflare/workers-types"
import { createFrameCodec, type FrameCodec } from "../wire/frame-codec.ts"
import type { ClientFrame, ServerFrame } from "../wire/frames.ts"
import {
  currentSeq,
  getDrainCursor,
  hydrateRows,
  initSchema,
  installTriggers,
  readChangesSince,
  setDrainCursor,
  snapshotAll,
} from "./changes.ts"
import { decodeResult, encodeResult, lookupTx, recordTx, type SeenTx } from "./dedup.ts"
import type { Registry } from "./registry.ts"
import { SubscriptionRegistry } from "./subscriptions.ts"

export abstract class SyncDurableObject<Env = unknown, TUser = unknown> extends DurableObject<Env> {
  /** Subclasses declare their collections, mutations, and commands. */
  protected abstract registry: Registry<TUser>

  /** Wire codec. Binary MessagePack by default; override for a JSON transport. */
  protected readonly codec: FrameCodec = createFrameCodec()

  protected readonly subs = new SubscriptionRegistry()
  private readonly liveWs = new Set<WebSocket>()
  private schemaReady = false

  constructor(ctx: ConstructorParameters<typeof DurableObject>[0], env: Env) {
    super(ctx, env)
    // Auto-pong via the runtime: survives hibernation, no per-message billing.
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"))
    // Restore the live-socket set after a hibernation wake.
    for (const ws of this.ctx.getWebSockets()) this.liveWs.add(ws)
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
    // Deltas first (to this socket and every other subscriber), then receipt.
    this.drainAndBroadcast()
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

    const touched = new Set<WebSocket>()
    for (const [tbl, tableChanges] of byTable) {
      const coll = this.registry.collections.get(tbl)
      if (!coll) continue
      const latest = new Map<string, (typeof tableChanges)[number]>()
      for (const c of tableChanges) latest.set(c.key, c)
      const liveKeys = [...latest.values()].filter((c) => c.op !== "delete").map((c) => c.key)
      const hydrated = hydrateRows(sql, tbl, coll.pk, liveKeys)

      for (const { ws, sub } of this.subs.forCollection(tbl)) {
        for (const [key, change] of latest) {
          const row = hydrated.get(key)
          if (change.op === "delete" || !row) {
            this.send(ws, { t: "d", sub: sub.subId, key, op: "delete", seq: cursor })
          } else {
            // Full row as the partial patch; column-level diffs arrive later.
            this.send(ws, { t: "d", sub: sub.subId, key, op: change.op, cols: row, seq: cursor })
          }
        }
        touched.add(ws)
      }
    }

    for (const ws of touched) this.send(ws, { t: "uptodate", seq: cursor })
    setDrainCursor(sql, changes[changes.length - 1]!.seq)
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
    this.subs.add(ws, frame.subId, frame.collection)
    const seq = String(currentSeq(this.sql))
    for (const row of snapshotAll(this.sql, frame.collection)) {
      this.send(ws, { t: "snap", sub: frame.subId, key: row[coll.pk], row, seq })
    }
    this.send(ws, { t: "snap-end", sub: frame.subId, seq })
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
