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

/** Reconnect delay policy (ADR-0016). Called once per reconnect attempt with
 *  the 1-based attempt number (reset to 1 after each successful open) and,
 *  when the drop came from a socket close, that close's code/reason (a failed
 *  `open()` has no close frame, so both are undefined). Return the delay in ms
 *  before the next attempt, or `null` to stop reconnecting (terminal — the
 *  transport surfaces it via `onClosed`). */
export type ReconnectDelayFn = (attempt: number, closeCode?: number, closeReason?: string) => number | null

/** The default reconnect policy: capped exponential backoff with full jitter —
 *  a uniform delay in [0, min(cap, base·2^(attempt−1))], cap 30 s (never below
 *  the base) — and application close codes (4000-4999) are terminal: the
 *  server closed deliberately (e.g. an accept-then-close 4403 auth rejection),
 *  so retrying cannot succeed. */
export function defaultReconnectDelay(baseMs: number, capMs = 30_000): ReconnectDelayFn {
  const cap = Math.max(capMs, baseMs)
  return (attempt, closeCode) => {
    if (closeCode !== undefined && closeCode >= 4000 && closeCode <= 4999) return null
    return Math.random() * Math.min(cap, baseMs * 2 ** (attempt - 1))
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
  /** Base delay (ms) for the DEFAULT reconnect policy's backoff — the attempt-1
   *  jitter ceiling. Ignored when `reconnectDelay` is provided. */
  reconnectDelayMs?: number
  /** Reconnect delay policy; replaces the default (`defaultReconnectDelay`).
   *  Return `null` to stop reconnecting. */
  reconnectDelay?: ReconnectDelayFn
  /** Called when an unexpected drop is TERMINAL — the policy returned `null`
   *  (default: any 4000-4999 application close, e.g. an auth rejection) — so
   *  the app can tell "re-auth needed" from a transient blip the transport is
   *  still retrying. Never called for an intentional `close()`. */
  onClosed?: (code: number | undefined, reason: string | undefined) => void
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

// --- Typed-command projection over the schema Api (`typeof schema`) ---------
// Structural-only: the client imports the Api *as a type*, so these recover the
// command map and each command's Args/Result from the phantom carriers the
// server's `CommandEntry` attaches — no server import, nothing at runtime.
type CommandsOf<Api> = Api extends { commands: infer C } ? C : Record<never, never>
/** Callable command names. `""` is excluded: `wellFormed` drops a call frame
 *  with an empty name, so a `""` command would hang on the wire; making it
 *  uncallable here turns that footgun into harmless dead code at no runtime cost. */
type CommandName<Api> = Exclude<keyof CommandsOf<Api> & string, "">
type ArgsOf<Entry> = Entry extends { __args?: infer A } ? A : never
type ResultOf<Entry> = Entry extends { __result?: infer R } ? R : never
/** The `transport.call.*` proxy: one method per command, args + result typed.
 *  `""` is remapped away for the same reason as `CommandName`. */
type CallProxy<Api> = {
  [K in keyof CommandsOf<Api> as K extends "" ? never : K]: (
    args: ArgsOf<CommandsOf<Api>[K]>,
  ) => Promise<ResultOf<CommandsOf<Api>[K]>>
}

export class WebSocketTransport<Api = unknown> {
  /** Phantom brand tying the transport to its schema `Api`, so a transport for
   *  one DO is not assignable where another DO's schema is expected. `declare`
   *  keeps it type-only — no runtime field. */
  declare readonly __api?: Api
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
  private readonly reconnectDelay: ReconnectDelayFn
  private readonly onClosed?: (code: number | undefined, reason: string | undefined) => void
  /** Consecutive reconnect attempts since the last successful open; the
   *  1-based value passed to the policy. */
  private reconnectAttempt = 0
  /** The one pending reconnect timer, so a successful open, a terminal stop,
   *  or close() can cancel it — a stale timer from an earlier transient drop
   *  must never fire a further attempt after any of those. */
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null

  constructor(opts: TransportOptions) {
    this.codec = opts.codec ?? createFrameCodec()
    this.timeoutMs = opts.timeoutMs ?? 5000
    this.reconnectDelay = opts.reconnectDelay ?? defaultReconnectDelay(opts.reconnectDelayMs ?? 250)
    this.onClosed = opts.onClosed
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
      ws.addEventListener("close", (ev) => {
        // Only the CURRENT socket's close may detach/reconnect. A stale
        // socket's late close (delivered after close()+connect() installed a
        // fresh socket) must not null the live connection (codex review).
        if (this.ws !== ws) return
        this.ws = null
        this.connectPromise = null
        // Auto-reconnect on an unexpected drop while subscriptions are active.
        if (!this.intentionallyClosed && this.handlers.size > 0) {
          const { code, reason } = ev as { code?: number; reason?: string }
          this.scheduleReconnect(code, reason)
        }
      })
      this.ws = ws
      // A successful open resets the backoff: a later blip starts from the
      // policy's first-attempt delay again, not the accumulated one. It also
      // supersedes any pending timer (a demand-driven connect beat it).
      this.reconnectAttempt = 0
      this.clearReconnectTimer()
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
        // A socket that never opened has no close frame — the policy sees an
        // undefined code (backoff, under the default).
        this.scheduleReconnect()
      }
    })
    return this.connectPromise
  }

  /** Consult the policy and either arm the next reconnect attempt or stop. */
  private scheduleReconnect(closeCode?: number, closeReason?: string): void {
    // The flag is set at SCHEDULING time, not in the timer: a demand-driven
    // connect() (a mutation inside the reconnect window) may establish the
    // fresh socket first, and it must run the resubscribe path too — or every
    // subscription is silently dead on the new socket and the late timer
    // wedges the flag (pre-existing bug, found in the ADR-0011 grill). It is
    // also set on a TERMINAL close, so an app-driven connect() after e.g.
    // re-auth still resubscribes from the cursor.
    this.reconnecting = true
    // At most one pending attempt: a newer drop supersedes an older timer.
    this.clearReconnectTimer()
    const delay = this.reconnectDelay(++this.reconnectAttempt, closeCode, closeReason)
    if (delay === null) {
      // Terminal (default: application close 4000-4999, e.g. an
      // accept-then-close auth rejection): retrying cannot help — surface the
      // close to the app instead of looping against the DO.
      this.onClosed?.(closeCode, closeReason)
      return
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      void this.connect().catch(() => {
        /* next attempt retries via the connect-failure path above */
      })
    }, delay)
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
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
    this.clearReconnectTimer()
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

  /**
   * Invoke a server command by name. Builds the `call` frame internally and
   * generates the txId (`crypto.randomUUID()`) — the app no longer supplies one.
   * Name + args are checked and the result is inferred against the schema `Api`.
   * Resolves with the command's result when its `committed` receipt arrives.
   */
  async sendCall<K extends CommandName<Api>>(
    name: K,
    args: ArgsOf<CommandsOf<Api>[K]>,
  ): Promise<ResultOf<CommandsOf<Api>[K]>> {
    const txId = crypto.randomUUID()
    const { result } = await this.sendAwaitingReceipt({ t: "call", txId, name, args }, txId)
    return result as ResultOf<CommandsOf<Api>[K]>
  }

  /** Sugar over `sendCall`: `transport.call.clearRoom({ … })`. One method per
   *  command on the schema `Api`; a Proxy forwarding to `sendCall`. */
  readonly call: CallProxy<Api> = new Proxy(
    {},
    {
      get:
        (_t, name: string) =>
        (args: unknown): Promise<unknown> =>
          this.sendCall(name as CommandName<Api>, args as ArgsOf<CommandsOf<Api>[CommandName<Api>]>),
    },
  ) as CallProxy<Api>

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
