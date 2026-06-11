// WebSocketTransport — browser client, one per Durable Object.
//
// The single-cursor half of the inversion (ADR-0002 C1). It tracks exactly one
// position, `appliedSeq`, advanced only at commit boundaries (snap-end /
// uptodate / committed) — never on individual snap/d frames — so the cursor is
// always a contiguous applied prefix. There is no second ("acked") cursor.
//
// Confirmation rides the same stream: `sendMut`/`sendCall` resolve when the
// server's `committed` for that txId arrives. Because the server flushes a
// mutation's matched deltas before its `committed` (server-side C1 ordering),
// by the time `committed` is processed the deltas are already applied and
// `appliedSeq >= commitSeq` holds.
//
// The socket opener is injectable: the browser default is `new WebSocket(url)`;
// other runtimes (and tests) provide an already-connected socket.

import { createFrameCodec, type FrameCodec } from "../wire/frame-codec.ts"
import type { ClientFrame, RowOp, ServerFrame } from "../wire/frames.ts"

/** Minimal structural socket — satisfied by both browser WebSocket and a
 *  workerd accepted client socket, avoiding the DOM-vs-workers type clash. */
export interface WebSocketLike {
  send(data: string | ArrayBuffer | ArrayBufferView): void
  close(code?: number, reason?: string): void
  addEventListener(type: string, listener: (ev: { data?: unknown }) => void): void
  removeEventListener(type: string, listener: (ev: { data?: unknown }) => void): void
}

/** A collection adapter's view of one subscription's inbound frames. */
export interface SubHandler {
  onSnap(key: unknown, row: unknown): void
  onSnapEnd(): void
  onDelta(op: RowOp, key: unknown, cols: Record<string, unknown> | undefined): void
  onUptodate(): void
  onReset(): void
}

export class MutationRejectedError extends Error {
  constructor(
    message: string,
    readonly code?: string,
  ) {
    super(message)
    this.name = "MutationRejectedError"
  }
}

export interface TransportOptions {
  url: string
  /** Returns a CONNECTED socket. Default opens `new WebSocket(url)` and resolves
   *  on its `open` event. Tests/other runtimes inject a ready socket. */
  open?: () => WebSocketLike | Promise<WebSocketLike>
  codec?: FrameCodec
  /** Confirmation/await timeout in ms. */
  timeoutMs?: number
  /** Delay before an auto-reconnect attempt after an unexpected drop (ms).
   *  Fixed for now; production should layer exponential backoff + jitter. */
  reconnectDelayMs?: number
}

interface SeqWaiter {
  target: bigint
  resolve: () => void
  reject: (e: Error) => void
  timer: ReturnType<typeof setTimeout>
}

interface TxWaiter {
  resolve: (r: { result?: unknown }) => void
  reject: (e: Error) => void
  timer: ReturnType<typeof setTimeout>
}

export class WebSocketTransport {
  private ws: WebSocketLike | null = null
  private connectPromise: Promise<void> | null = null
  private readonly codec: FrameCodec
  private readonly timeoutMs: number
  private readonly open: () => WebSocketLike | Promise<WebSocketLike>

  private readonly handlers = new Map<
    string,
    { handler: SubHandler; collection: string; where?: unknown; orderBy?: unknown; limit?: number }
  >()
  private appliedSeq = 0n
  private readonly seqWaiters: Array<SeqWaiter> = []
  private readonly pendingTx = new Map<string, TxWaiter>()
  private readonly pendingFetches = new Map<
    string,
    { resolve: (rows: Array<unknown>) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }
  >()
  /** Suppresses auto-reconnect after an intentional close(). */
  private intentionallyClosed = false
  /** True while reconnecting, so connect() resubscribes on success. */
  private reconnecting = false
  private readonly reconnectDelayMs: number

  constructor(opts: TransportOptions) {
    this.codec = opts.codec ?? createFrameCodec()
    this.timeoutMs = opts.timeoutMs ?? 5000
    this.reconnectDelayMs = opts.reconnectDelayMs ?? 250
    this.open =
      opts.open ??
      (() =>
        new Promise<WebSocketLike>((resolve, reject) => {
          const ws = new (globalThis as unknown as { WebSocket: new (u: string) => WebSocketLike }).WebSocket(opts.url)
          ws.addEventListener("open", () => resolve(ws))
          ws.addEventListener("error", () => reject(new Error("websocket error")))
        }))
  }

  /** Highest committed position the client has applied (stringified bigint). */
  get appliedCursor(): string {
    return String(this.appliedSeq)
  }

  async connect(): Promise<void> {
    if (this.ws) return
    if (this.connectPromise) return this.connectPromise
    this.connectPromise = (async () => {
      const ws = await this.open()
      // Browsers default WebSocket.binaryType to "blob"; force "arraybuffer" so
      // binary frames arrive as ArrayBuffer (workerd already does). Without this
      // the codec can't decode and every server frame is silently dropped.
      try {
        ;(ws as { binaryType?: string }).binaryType = "arraybuffer"
      } catch {
        /* some socket impls don't expose binaryType; codec handles AB/Uint8Array */
      }
      ws.addEventListener("message", (ev) => this.onMessage(ev.data))
      ws.addEventListener("close", () => {
        this.ws = null
        this.connectPromise = null
        // Auto-reconnect on an unexpected drop while subscriptions are active.
        if (!this.intentionallyClosed && this.handlers.size > 0) {
          // The flag is set at SCHEDULING time, not in the timer: a demand-
          // driven connect() (a mutation inside the reconnect window) may
          // establish the fresh socket first, and it must run the resubscribe
          // path too — or every subscription is silently dead on the new
          // socket and the late timer wedges the flag (pre-existing bug, found
          // in the ADR-0011 grill).
          this.reconnecting = true
          setTimeout(() => {
            void this.connect().catch(() => {
              /* next attempt retries on the following close */
            })
          }, this.reconnectDelayMs)
        }
      })
      this.ws = ws
      // On a reconnect, re-establish every subscription from our single applied
      // cursor so the server serves a windowed catch-up rather than a snapshot.
      if (this.reconnecting) {
        this.reconnecting = false
        this.resubscribeAll()
      }
    })()
    // A socket that never OPENED fires no close event, so the close-handler
    // recovery path (above) can't run. Clear the cached rejection so the next
    // connect() starts fresh, and re-arm the timer while subscriptions are
    // live — otherwise one unreachable attempt wedges the transport forever.
    this.connectPromise.catch(() => {
      this.connectPromise = null
      if (!this.intentionallyClosed && this.handlers.size > 0) {
        this.reconnecting = true
        setTimeout(() => {
          void this.connect().catch(() => {
            /* next attempt retries on the following close */
          })
        }, this.reconnectDelayMs)
      }
    })
    return this.connectPromise
  }

  /** Re-send a `sub` for every registered subscription, carrying `since`. */
  private resubscribeAll(): void {
    const since = this.appliedCursor
    for (const [subId, entry] of this.handlers) {
      this.sendFrame({
        t: "sub",
        subId,
        collection: entry.collection,
        where: entry.where,
        orderBy: entry.orderBy,
        limit: entry.limit,
        since,
      })
    }
  }

  close(): void {
    this.intentionallyClosed = true
    for (const w of this.seqWaiters.splice(0)) {
      clearTimeout(w.timer)
      w.reject(new Error("transport closed"))
    }
    for (const [, w] of this.pendingTx) {
      clearTimeout(w.timer)
      w.reject(new Error("transport closed"))
    }
    this.pendingTx.clear()
    for (const [, w] of this.pendingFetches) {
      clearTimeout(w.timer)
      w.reject(new Error("transport closed"))
    }
    this.pendingFetches.clear()
    try {
      this.ws?.close()
    } catch {
      /* ignore */
    }
    this.ws = null
    this.connectPromise = null
  }

  async subscribe(
    subId: string,
    collection: string,
    handler: SubHandler,
    where?: unknown,
    orderBy?: unknown,
    limit?: number,
  ): Promise<void> {
    this.handlers.set(subId, { handler, collection, where, orderBy, limit })
    await this.connect()
    this.sendFrame({ t: "sub", subId, collection, where, orderBy, limit })
  }

  unsubscribe(subId: string): void {
    this.handlers.delete(subId)
    if (this.ws) this.sendFrame({ t: "unsub", subId })
  }

  sendMut(frame: Extract<ClientFrame, { t: "mut" }>): Promise<{ result?: unknown }> {
    return this.sendAwaitingReceipt(frame, frame.txId)
  }

  sendCall(frame: Extract<ClientFrame, { t: "call" }>): Promise<{ result?: unknown }> {
    return this.sendAwaitingReceipt(frame, frame.txId)
  }

  /** One-shot page fetch; resolves with the page's rows. No live subscription. */
  async fetch(frame: Extract<ClientFrame, { t: "fetch" }>): Promise<Array<unknown>> {
    await this.connect()
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingFetches.delete(frame.fetchId)
        reject(new Error(`fetch timeout: ${frame.fetchId}`))
      }, this.timeoutMs)
      this.pendingFetches.set(frame.fetchId, { resolve, reject, timer })
      this.sendFrame(frame)
    })
  }

  /** Resolves once `appliedSeq >= target`. */
  awaitSeq(target: string): Promise<void> {
    const t = BigInt(target)
    if (this.appliedSeq >= t) return Promise.resolve()
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        const i = this.seqWaiters.findIndex((w) => w.resolve === resolve)
        if (i >= 0) this.seqWaiters.splice(i, 1)
        reject(new Error(`awaitSeq timeout: target=${target}`))
      }, this.timeoutMs)
      this.seqWaiters.push({ target: t, resolve, reject, timer })
    })
  }

  private async sendAwaitingReceipt(frame: ClientFrame, txId: string): Promise<{ result?: unknown }> {
    await this.connect()
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingTx.delete(txId)
        reject(new Error(`confirmation timeout: txId=${txId}`))
      }, this.timeoutMs)
      this.pendingTx.set(txId, { resolve, reject, timer })
      this.sendFrame(frame)
    })
  }

  private sendFrame(frame: ClientFrame): void {
    if (!this.ws) throw new Error("transport not connected")
    this.ws.send(this.codec.encode(frame))
  }

  private onMessage(data: unknown): void {
    let frame: ServerFrame
    try {
      frame = this.codec.decode(data as ArrayBuffer | string) as ServerFrame
    } catch {
      return
    }
    switch (frame.t) {
      case "snap":
        this.handlers.get(frame.sub)?.handler.onSnap(frame.key, frame.row)
        return
      case "snap-end":
        this.handlers.get(frame.sub)?.handler.onSnapEnd()
        this.advance(frame.seq)
        return
      case "d":
        this.handlers.get(frame.sub)?.handler.onDelta(frame.op, frame.key, frame.cols)
        return
      case "uptodate":
        for (const { handler } of this.handlers.values()) handler.onUptodate()
        this.advance(frame.seq)
        return
      case "committed": {
        const w = this.pendingTx.get(frame.txId)
        if (w) {
          clearTimeout(w.timer)
          this.pendingTx.delete(frame.txId)
          w.resolve({ result: frame.result })
        }
        this.advance(frame.seq)
        return
      }
      case "rejected": {
        const w = this.pendingTx.get(frame.txId)
        if (w) {
          clearTimeout(w.timer)
          this.pendingTx.delete(frame.txId)
          w.reject(new MutationRejectedError(frame.error.message, frame.error.code))
        }
        return
      }
      case "page": {
        const w = this.pendingFetches.get(frame.fetchId)
        if (w) {
          clearTimeout(w.timer)
          this.pendingFetches.delete(frame.fetchId)
          w.resolve(frame.rows)
        }
        return
      }
      case "reset":
        if (frame.sub) this.handlers.get(frame.sub)?.handler.onReset()
        else for (const { handler } of this.handlers.values()) handler.onReset()
        return
    }
  }

  private advance(seq: string): void {
    const s = BigInt(seq)
    if (s > this.appliedSeq) this.appliedSeq = s
    if (this.seqWaiters.length === 0) return
    const remaining: Array<SeqWaiter> = []
    for (const w of this.seqWaiters) {
      if (this.appliedSeq >= w.target) {
        clearTimeout(w.timer)
        w.resolve()
      } else {
        remaining.push(w)
      }
    }
    this.seqWaiters.length = 0
    this.seqWaiters.push(...remaining)
  }
}
