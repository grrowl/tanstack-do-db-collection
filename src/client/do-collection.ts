// doCollectionOptions — a TanStack DB collection-options creator backed by a
// Durable Object over a WebSocketTransport.
//
// Maps the transport's frame callbacks onto TanStack DB's sync API
// (begin/write/commit/markReady/truncate) and wraps mutations as `mut` frames
// confirmed on the single ordered stream (sendMut resolves on `committed`,
// which — given server-side C1 ordering — implies the confirming delta is
// already applied).
//
// Optional `where` (M5): a @tanstack/db BasicExpression IR. It is sent on the
// `sub` frame so the server ships only matching rows, and compiled locally to
// preflight writes — an insert/update whose resulting row wouldn't match the
// filter throws WriteOutsideSubError synchronously (preventing an out-of-filter
// optimistic phantom). Move-out of matching rows is handled by the server's
// synthetic delete.

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
  /** Optional server-side filter (a @tanstack/db BasicExpression IR). Set once
   *  at construction; also preflights writes. */
  where?: unknown
}

/** Minimal shape we read from a TanStack DB PendingMutation. */
interface PendingMutationLike {
  type: RowOp
  key: string
  modified: unknown
  changes: unknown
}

/** Minimal shape of the sync params we depend on (subset of SyncConfig.sync). */
interface SyncParams {
  begin: (options?: { immediate?: boolean }) => void
  write: (message: { type: RowOp; value?: unknown; key?: string }) => void
  commit: () => void
  markReady: () => void
  truncate: () => void
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
  const subId = `${table}#${++subSeq}`
  const matches = compilePredicate(where)

  const sync = (params: SyncParams): (() => void) => {
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

    const handler: SubHandler = {
      onSnap: (_key, row) => {
        ensureBegin()
        write({ type: "insert", value: row })
      },
      onSnapEnd: () => {
        flush()
        markReady()
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
    }

    void transport.subscribe(subId, table, handler, where)
    return () => transport.unsubscribe(subId)
  }

  const mutationFn = async (params: {
    transaction: { id: string; mutations: ReadonlyArray<PendingMutationLike> }
  }): Promise<void> => {
    const ops: Array<MutOp> = params.transaction.mutations.map((m) => {
      // Preflight: a write whose resulting row falls outside this collection's
      // filter would never be confirmed by a delta — reject before any I/O.
      if (m.type !== "delete" && !matches(m.modified as Record<string, unknown>)) {
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
  }

  return {
    id: opts.id ?? table,
    getKey,
    sync: { sync, rowUpdateMode: "partial" },
    onInsert: mutationFn,
    onUpdate: mutationFn,
    onDelete: mutationFn,
  } as unknown as CollectionConfig<T, string>
}
