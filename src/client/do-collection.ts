// doCollectionOptions — a TanStack DB collection-options creator backed by a
// Durable Object over a WebSocketTransport.
//
// Maps the transport's frame callbacks onto TanStack DB's sync API
// (begin/write/commit/markReady/truncate) and wraps mutations as `mut` frames
// confirmed on the single ordered stream.
//
// Two sync modes:
//   - 'eager' (default): subscribe to the whole collection (optionally filtered
//     by a static `where`) up front. A `where` also preflights writes.
//   - 'on-demand': sync nothing up front; the collection calls loadSubset(where)
//     as live queries mount, and unloadSubset when they unmount. Each distinct
//     `where` is one refcounted server subscription; ordering/limit are applied
//     client-side by IVM over the loaded rows. Writes that land outside every
//     loaded subset are confirmed and their optimistic overlay retired by a
//     post-mutation empty sync commit (ADR-0002 C2, verified).

import { compileSingleRowExpression, toBooleanPredicate, type CollectionConfig } from "@tanstack/db"
import type { MutOp, RowOp } from "../wire/frames.ts"
import { type SubHandler, WebSocketTransport } from "./transport.ts"

let subSeq = 0

export class WriteOutsideSubError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WriteOutsideSubError"
  }
}

// --- Row inference from a schema Api (`typeof schema`) ----------------------
// Mirrors the transport's command projection: structural-only, recovering a
// collection's Row from the phantom `__row` the server's `CollectionEntry`
// carries. The client needs NO runtime schema value — just the Api type.
type CollectionsOf<Api> = Api extends { collections: infer C } ? C : never
/** Table names declared on the schema Api. */
export type CollectionName<Api> = keyof CollectionsOf<Api> & string
/** The Row type of collection `K` on the schema Api. */
export type RowOf<Api, K extends PropertyKey> = K extends keyof CollectionsOf<Api>
  ? CollectionsOf<Api>[K] extends { __row?: infer R }
    ? R
    : never
  : never

export interface DoCollectionOptions<T extends object> {
  /** One transport per DO; shared by all collections on that DO. */
  transport: WebSocketTransport<any>
  /** Collection (table) name on the DO. */
  table: string
  /** Stable client-supplied key extractor (must match the server pk). */
  getKey: (row: T) => string
  /** Collection id; defaults to the table name. */
  id?: string
  /**
   * 'eager' (default) syncs the whole collection (optionally filtered by
   * `where`). 'on-demand' syncs only the subsets that live queries request.
   */
  syncMode?: "eager" | "on-demand"
  /** Eager-mode server-side filter + write preflight (a @tanstack/db IR). */
  where?: unknown
}

interface PendingMutationLike {
  type: RowOp
  key: string
  modified: unknown
  changes: unknown
}

interface SyncParams {
  collection: { get: (key: string) => unknown }
  begin: (options?: { immediate?: boolean }) => void
  write: (message: { type: RowOp; value?: unknown; key?: string }) => void
  commit: () => void
  markReady: () => void
  truncate: () => void
}

/** Subset of @tanstack/db's LoadSubsetOptions we consume. */
interface LoadSubsetOptions {
  where?: unknown
  orderBy?: unknown
  limit?: number
  offset?: number
  cursor?: { whereFrom: unknown; whereCurrent: unknown; lastKey?: unknown }
}

function compilePredicate(where: unknown): (row: Record<string, unknown>) => boolean {
  if (where === undefined || where === null) return () => true
  const evaluate = compileSingleRowExpression(where as never) as (
    row: Record<string, unknown>,
  ) => boolean | null
  return (row) => toBooleanPredicate(evaluate(row))
}

/** Api-typed options: Row is inferred from the schema `Api` + `table`, so the
 *  client needs no runtime schema value. `getKey` and the row type follow. */
export interface DoApiCollectionOptions<Api, K extends CollectionName<Api>> {
  /** One transport per DO, parameterized by the same schema `Api`. */
  transport: WebSocketTransport<Api>
  /** Collection (table) name on the DO — a key of the schema's collections. */
  table: K
  /** Stable client-supplied key extractor (must match the server pk). */
  getKey: (row: RowOf<Api, K>) => string
  /** Collection id; defaults to the table name. */
  id?: string
  syncMode?: "eager" | "on-demand"
  where?: unknown
}

// Api-driven: `doCollectionOptions<Api, "messages">({ transport, table, getKey })`
// — Row inferred from the schema. Listed first so a zero-type-arg call infers
// Api from the transport rather than collapsing Row to the explicit-T overload.
export function doCollectionOptions<Api, K extends CollectionName<Api>>(
  opts: DoApiCollectionOptions<Api, K>,
): CollectionConfig<RowOf<Api, K> & object, string>
// Explicit-Row: `doCollectionOptions<Message>({ transport, table, getKey })`.
export function doCollectionOptions<T extends object>(opts: DoCollectionOptions<T>): CollectionConfig<T, string>
export function doCollectionOptions(opts: DoCollectionOptions<any>): CollectionConfig<any, string> {
  const { transport, table, getKey, where } = opts
  const syncMode = opts.syncMode ?? "eager"
  const eagerSubId = `${table}#${++subSeq}`
  const matches = compilePredicate(where)

  // Set by sync(); used by mutationFn to retire no-subset-match optimistic rows.
  let emptyCommit: (() => void) | null = null

  const sync = (params: SyncParams): SyncConfigResult => {
    const { collection, begin, write, commit, markReady, truncate } = params
    let open = false
    const ensureBegin = (): void => {
      if (!open) {
        begin()
        open = true
      }
    }
    const flush = (): void => {
      if (open) {
        commit()
        open = false
      }
    }
    emptyCommit = (): void => {
      flush()
      begin()
      commit() // a standalone empty boundary; runs the direct-upsert clear path
    }

    const makeHandler = (onReady: () => void): SubHandler => ({
      onSnap: (_key, row) => {
        ensureBegin()
        write({ type: "insert", value: row })
      },
      onSnapEnd: () => {
        flush()
        onReady()
      },
      onDelta: (op, key, cols) => {
        ensureBegin()
        if (op === "delete") write({ type: "delete", key: key as string })
        // A catch-up emits the LATEST op per changed key, so a key deleted-and-
        // reinserted while we were away arrives as "insert" for a key we still
        // HOLD — TanStack's sync write throws DuplicateKeySyncError on that
        // unless values deep-equal. Apply a held-key insert as the upsert it
        // semantically is (update upserts; the move-in contract, ADR-0002 C4).
        else if (op === "insert" && collection.get(key as string) !== undefined) write({ type: "update", value: cols })
        else write({ type: op, value: cols })
      },
      onUptodate: () => flush(),
      onReset: () => {
        flush()
        begin()
        truncate()
        commit()
        // A reset is also the only terminal signal for a REJECTED sub (the
        // server sends `reset` with no `snap-end` for an unsupported predicate
        // or unknown collection). Mark ready here too, or this subset's load
        // promise — and the live query's preload() — would hang forever. For a
        // compaction/rotation reset (a valid sub that re-snapshots) this is an
        // idempotent no-op: onSnapEnd's onReady() has already fired.
        onReady()
      },
    })

    if (syncMode === "on-demand") {
      // Ready as soon as connected; data arrives per loadSubset.
      void transport.connect().then(() => markReady())
      // Distinct `where` -> one refcounted server subscription.
      const loaded = new Map<string, { subId: string; refs: number; ready: Promise<void> }>()
      const keyOf = (o: LoadSubsetOptions): string => JSON.stringify(o.where ?? null)

      // Cursor load-more (scroll-back). The live sub on `where` already streams
      // deltas for the whole subset, so this is a one-shot fetch of the older
      // rows the window now needs — NOT a new live registration.
      //
      // The fetch frame is a serialized `LoadSubsetOptions` (ADR-0005): we forward TanStack's
      // own `where` and `cursor` (whereFrom/whereCurrent) verbatim. The server
      // composes `base AND whereCurrent` (ties, unbounded) and `base AND whereFrom`
      // (next page, bounded by `limit`). It's ONE frame, so the server reads both
      // halves at one seq (atomic) and the client applies the whole page in one
      // macrotask — the write lands before any later delta. That ordering is what
      // prevents a concurrent delete from being undone by a stale tie (ADR-0003).
      //
      // Rows are written insert-if-ABSENT: a boundary tie already in the window
      // must not be re-inserted (a differing value would throw
      // DuplicateKeySyncError and abort the transaction), and a key the live sub
      // already holds keeps its fresher value. The live `where` sub stays the
      // source of truth for anything currently in the collection.
      const loadMore = async (o: LoadSubsetOptions): Promise<void> => {
        const { whereFrom, whereCurrent } = o.cursor!
        const page = await transport.fetch({
          t: "fetch",
          fetchId: `${table}#fetch#${++subSeq}`,
          collection: table,
          where: o.where,
          cursor: { whereFrom, whereCurrent },
          orderBy: o.orderBy,
          limit: o.limit,
        })
        ensureBegin()
        for (const r of page) {
          if (collection.get(getKey(r)) === undefined) write({ type: "insert", value: r })
        }
        flush()
      }

      const loadSubset = (o: LoadSubsetOptions): true | Promise<void> => {
        if (o.cursor) return loadMore(o)
        const key = keyOf(o)
        const existing = loaded.get(key)
        if (existing) {
          existing.refs++
          return existing.ready
        }
        let resolve!: () => void
        const ready = new Promise<void>((r) => {
          resolve = r
        })
        const subId = `${table}#${key}`
        loaded.set(key, { subId, refs: 1, ready })
        // Forward orderBy/limit so the INITIAL snapshot is the bounded window
        // (recent N), not the whole where-subset. The live sub's predicate is
        // still `where`, so entering rows (e.g. new messages) are delivered.
        void transport.subscribe(subId, table, makeHandler(resolve), o.where, o.orderBy, o.limit)
        return ready
      }

      const unloadSubset = (o: LoadSubsetOptions): void => {
        // Symmetric with loadSubset: a cursor load was a one-shot fetch that
        // never took a refcount, so its unload must not release one either —
        // otherwise a second live query on the same `where` is under-counted
        // and its still-live sub is torn down early.
        if (o.cursor) return
        const key = keyOf(o)
        const entry = loaded.get(key)
        if (entry && --entry.refs <= 0) {
          transport.unsubscribe(entry.subId)
          loaded.delete(key)
        }
      }

      return { loadSubset, unloadSubset, cleanup: () => transport.close() }
    }

    // eager
    void transport.subscribe(eagerSubId, table, makeHandler(markReady), where)
    return () => transport.unsubscribe(eagerSubId)
  }

  const mutationFn = async (params: {
    transaction: { id: string; mutations: ReadonlyArray<PendingMutationLike> }
  }): Promise<void> => {
    const ops: Array<MutOp> = params.transaction.mutations.map((m) => {
      // Eager filtered preflight: a write outside the static `where` would never
      // be confirmed by a delta — reject before any I/O.
      if (where != null && m.type !== "delete" && !matches(m.modified as Record<string, unknown>)) {
        throw new WriteOutsideSubError(
          `write to '${table}' (key '${m.key}') falls outside the collection's where filter`,
        )
      }
      return {
        type: m.type,
        key: m.key,
        cols:
          m.type === "delete"
            ? undefined
            : m.type === "insert"
              ? (m.modified as Record<string, unknown>)
              : (m.changes as Record<string, unknown>),
      }
    })
    await transport.sendMut({ t: "mut", txId: params.transaction.id, collection: table, ops })

    // On-demand: a confirmed write may land outside every loaded subset, so no
    // delta clears its direct optimistic upsert. A post-mutation empty sync
    // commit (after the tx completes) retires it; for an in-view write it is a
    // no-op (the synced row keeps it). See ADR-0002 C2.
    if (syncMode === "on-demand" && emptyCommit) {
      const run = emptyCommit
      setTimeout(() => run(), 0)
    }
  }

  return {
    id: opts.id ?? table,
    getKey,
    syncMode,
    sync: { sync, rowUpdateMode: "partial" },
    onInsert: mutationFn,
    onUpdate: mutationFn,
    onDelete: mutationFn,
  } as unknown as CollectionConfig<any, string>
}

/** What our sync() returns: a cleanup fn (eager) or the on-demand handlers. */
type SyncConfigResult =
  | (() => void)
  | {
      loadSubset: (o: LoadSubsetOptions) => true | Promise<void>
      unloadSubset: (o: LoadSubsetOptions) => void
      cleanup: () => void
    }
