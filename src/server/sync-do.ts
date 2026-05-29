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
import { currentSeq, initSchema, installTriggers, snapshotAll } from "./changes.ts"
import type { Registry } from "./registry.ts"
import { SubscriptionRegistry } from "./subscriptions.ts"

export abstract class SyncDurableObject<Env = unknown, TUser = unknown> extends DurableObject<Env> {
  /** Subclasses declare their collections (and, from M3, mutations/commands). */
  protected abstract registry: Registry

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
      // mut/call land in the next M3 increment (apply + single-stream
      // confirmation). Reject explicitly so a client gets a clear signal
      // rather than a silent hang.
      case "mut":
        this.send(ws, {
          t: "rejected",
          txId: frame.txId,
          error: { code: "UNIMPLEMENTED", message: "mutations land in the next increment" },
        })
        return
      case "call":
        this.send(ws, {
          t: "rejected",
          txId: frame.txId,
          error: { code: "UNIMPLEMENTED", message: "commands land in the next increment" },
        })
        return
    }
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
