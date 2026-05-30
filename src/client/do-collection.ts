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

export interface DoCollectionOptions<T extends object> {
  /** One transport per DO; shared by all collections on that DO. */
  transport: WebSocketTransport
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
  begin: (options?: { immediate?: boolean }) => void
  write: (message: { type: RowOp; value?: unknown; key?: string }) => void
  commit: () => void
  markReady: () => void
  truncate: () => void
}

/** Subset of @tanstack/db's LoadSubsetOptions we consume (where-based v1). */
interface LoadSubsetOptions {
  where?: unknown
  orderBy?: unknown
  limit?: number
  offset?: number
}

function compilePredicate(where: unknown): (row: Record<string, unknown>) => boolean {
  if (where === undefined || where === null) return () => true
  const evaluate = compileSingleRowExpression(where as never) as (
    row: Record<string, unknown>,
  ) => boolean | null
  return (row) => toBooleanPredicate(evaluate(row))
}

export function doCollectionOptions<T extends object>(
  opts: DoCollectionOptions<T>,
): CollectionConfig<T, string> {
  const { transport, table, getKey, where } = opts
  const syncMode = opts.syncMode ?? "eager"
  const eagerSubId = `${table}#${++subSeq}`
  const matches = compilePredicate(where)

  // Set by sync(); used by mutationFn to retire no-subset-match optimistic rows.
  let emptyCommit: (() => void) | null = null

  const sync = (params: SyncParams): SyncConfigResult => {
    const { begin, write, commit, markReady, truncate } = params
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
        else write({ type: op, value: cols })
      },
      onUptodate: () => flush(),
      onReset: () => {
        flush()
        begin()
        truncate()
        commit()
      },
    })

    if (syncMode === "on-demand") {
      // Ready as soon as connected; data arrives per loadSubset.
      void transport.connect().then(() => markReady())
      // Distinct `where` -> one refcounted server subscription.
      const loaded = new Map<string, { subId: string; refs: number; ready: Promise<void> }>()
      const keyOf = (o: LoadSubsetOptions): string => JSON.stringify(o.where ?? null)

      const loadSubset = (o: LoadSubsetOptions): true | Promise<void> => {
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
  } as unknown as CollectionConfig<T, string>
}

/** What our sync() returns: a cleanup fn (eager) or the on-demand handlers. */
type SyncConfigResult =
  | (() => void)
  | {
      loadSubset: (o: LoadSubsetOptions) => true | Promise<void>
      unloadSubset: (o: LoadSubsetOptions) => void
      cleanup: () => void
    }
