// doCollectionOptions — a TanStack DB collection-options creator backed by a
// Durable Object over a WebSocketTransport.
//
// Maps the transport's frame callbacks onto TanStack DB's sync API
// (begin/write/commit/markReady/truncate) and wraps mutations as `mut` frames
// confirmed on the single ordered stream (sendMut resolves on `committed`,
// which — given server-side C1 ordering — implies the confirming delta is
// already applied).
//
// `@tanstack/db` is a TYPE-only import here: this module returns a plain config
// object and has no @tanstack/db runtime dependency. The consumer wires it into
// `createCollection`.

import type { CollectionConfig } from "@tanstack/db"
import type { MutOp, RowOp } from "../wire/frames.ts"
import { type SubHandler, WebSocketTransport } from "./transport.ts"

let subSeq = 0

export interface DoCollectionOptions<T extends object> {
  /** One transport per DO; shared by all collections on that DO. */
  transport: WebSocketTransport
  /** Collection (table) name on the DO. */
  table: string
  /** Stable client-supplied key extractor (must match the server pk). */
  getKey: (row: T) => string
  /** Collection id; defaults to the table name. */
  id?: string
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

export function doCollectionOptions<T extends object>(
  opts: DoCollectionOptions<T>,
): CollectionConfig<T, string> {
  const { transport, table, getKey } = opts
  const subId = `${table}#${++subSeq}`

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
      // Snapshot rows are full rows -> insert; the boundary commits + readies.
      onSnap: (_key, row) => {
        ensureBegin()
        write({ type: "insert", value: row })
      },
      onSnapEnd: () => {
        flush()
        markReady()
      },
      // Live deltas: delete by key; insert/update carry the (full, in M3) row.
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

    void transport.subscribe(subId, table, handler)
    return () => transport.unsubscribe(subId)
  }

  const mutationFn = async (params: {
    transaction: { id: string; mutations: ReadonlyArray<PendingMutationLike> }
  }): Promise<void> => {
    const ops: Array<MutOp> = params.transaction.mutations.map((m) => ({
      type: m.type,
      key: m.key,
      // insert: full new row; update: field-level diff (D19); delete: no cols.
      cols:
        m.type === "delete"
          ? undefined
          : m.type === "insert"
            ? (m.modified as Record<string, unknown>)
            : (m.changes as Record<string, unknown>),
    }))
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
