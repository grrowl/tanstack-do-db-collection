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
import { initSchema, installTriggers } from "./changes.ts"
import type { Registry } from "./registry.ts"

export abstract class SyncDurableObject<Env = unknown, TUser = unknown> extends DurableObject<Env> {
  /** Subclasses declare their collections (and, from M3, mutations/commands). */
  protected abstract registry: Registry

  /** Wire codec. Binary MessagePack by default; override for a JSON transport. */
  protected readonly codec: FrameCodec = createFrameCodec()

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
    await this.onFrame(ws, frame)
  }

  override webSocketClose(ws: WebSocket): void {
    this.liveWs.delete(ws)
  }

  override webSocketError(ws: WebSocket): void {
    this.liveWs.delete(ws)
  }

  /**
   * Handle a decoded client frame. Default is a no-op; M3 implements the
   * sub/mut/call dispatch in the framework. Subclasses generally do not
   * override this directly once M3 lands.
   */
  protected onFrame(_ws: WebSocket, _frame: ClientFrame): void | Promise<void> {}

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
