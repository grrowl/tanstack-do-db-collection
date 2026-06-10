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
import { encode as codecEncode } from "../wire/codec.ts"
import type { MutOp, RowOp } from "../wire/frames.ts"
import type { SubHandler, Transport } from "./transport.ts"

let subSeq = 0

export class WriteOutsideSubError extends Error {
  constructor(message: string) {
    super(message)
    this.name = "WriteOutsideSubError"
  }
}

export interface DoCollectionOptions<T extends object> {
  /** One transport per DO; shared by all collections on that DO. In the
   *  browser a WebSocketTransport; during SSR an SsrSnapshotTransport —
   *  created PER REQUEST (ADR-0011 D2). */
  transport: Transport
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

/** The opaque payload that rides TanStack's dehydrated state (ADR-0011 D3).
 *  Shape is ours; `v` gates forward evolution loudly. `where` fingerprints the
 *  eager filter the rows were dehydrated under: a cursor is only a sound
 *  resume point FOR THAT FILTER (catch-up emits changed keys only — an
 *  unchanged out-of-filter hydrated row would never be reconciled away). */
export interface DoSyncMeta {
  v: 1
  cursor: string
  where?: string
}

function parseSyncMeta(meta: unknown): DoSyncMeta {
  const m = meta as Partial<DoSyncMeta> | null
  if (m == null || m.v !== 1 || typeof m.cursor !== "string" || (m.where !== undefined && typeof m.where !== "string")) {
    throw new Error(`unrecognized sync meta (expected {v:1, cursor, where?}): ${JSON.stringify(meta)}`)
  }
  BigInt(m.cursor) // malformed cursor throws here — fail loud, never resume from garbage
  return { v: 1, cursor: m.cursor, ...(m.where === undefined ? {} : { where: m.where }) }
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

export function doCollectionOptions<T extends object>(
  opts: DoCollectionOptions<T>,
): CollectionConfig<T, string> {
  const { transport, table, getKey, where } = opts
  const syncMode = opts.syncMode ?? "eager"
  const eagerSubId = `${table}#${++subSeq}`
  const matches = compilePredicate(where)

  // Set by sync(); used by mutationFn to retire no-subset-match optimistic rows.
  let emptyCommit: (() => void) | null = null

  // SSR hydration's resume point (ADR-0011 D3). Set by importSyncMeta — which
  // upstream calls AFTER applying the dehydrated rows as synced upserts, and
  // possibly BEFORE sync() ever runs (lazy collections). Consumed exactly once
  // at sync start and cleared in cleanup: after a collection GC the rows are
  // wiped, so a retained cursor would resume over an empty store and silently
  // lose everything below it.
  let hydratedCursor: string | null = null

  const sync = (params: SyncParams): SyncConfigResult => {
    const { collection, begin, write, commit, markReady, truncate } = params
    const consumeHydratedCursor = (): string | null => {
      const hc = hydratedCursor
      hydratedCursor = null
      return hc
    }
    // Presence in SYNCED data — the combined view (collection.get) includes
    // optimistic overlays, which sync writes must never be steered by: a key
    // under an optimistic delete still exists synced (insert would throw), and
    // an optimistic-only insert does not (update would not upsert the synced
    // store the hydration correction targets). `_state.syncedData` is the same
    // seam upstream's DbClient hydration itself writes through.
    const syncedData = (): Map<string, unknown> | null =>
      (collection as { _state?: { syncedData?: Map<string, unknown> } })._state?.syncedData ?? null
    const syncedHas = (key: string): boolean => {
      const sd = syncedData()
      return sd ? sd.has(key) : collection.get(key) !== undefined
    }
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

    const makeHandler = (onReady: () => void, opts?: { reconcileSnapshots?: boolean }): SubHandler => {
      // `reconcileSnapshots` is armed only for a hydrated collection (ADR-0011
      // D4): a fresh snapshot can then arrive OVER held synced rows (cursor
      // "0" = no resume point), and a snapshot is authoritative SET semantics
      // — held keys absent from it were deleted server-side, and snapshots
      // carry no tombstones. Track the snapshot's keys and delete the rest at
      // the boundary; no truncate, so the first paint never flashes empty.
      // Initialized EAGERLY when armed: an EMPTY snapshot (zero snap frames,
      // the server wiped the table) must still reconcile everything away at
      // snap-end — a lazily-created set would silently skip it.
      let snapKeys: Set<string> | null = opts?.reconcileSnapshots ? new Set() : null
      return {
        onSnap: (_key, row) => {
          ensureBegin()
          const key = getKey(row as T)
          snapKeys?.add(key)
          // A held key's snapshot row is an upsert: hydrated rows may have
          // changed since dehydration, and a differing insert would throw
          // DuplicateKeySyncError. With the C1′ barrier a snapshot row is
          // never staler than the held synced row, so the snapshot wins.
          write(syncedHas(key) ? { type: "update", value: row } : { type: "insert", value: row })
        },
        onSnapEnd: () => {
          if (snapKeys) {
            const sd = syncedData()
            if (!sd) throw new Error("hydration reconcile requires collection._state.syncedData (incompatible @tanstack/db)")
            ensureBegin()
            for (const key of sd.keys()) {
              if (!snapKeys.has(key)) write({ type: "delete", key })
            }
            snapKeys = null // one boundary settles the hydrated state; disarm
          }
          flush()
          onReady()
        },
        onDelta: (op, key, cols) => {
          ensureBegin()
          if (op === "delete") write({ type: "delete", key: key as string })
          // A catch-up emits the LATEST op per changed key, so a key deleted-
          // and-reinserted while we were away arrives as "insert" for a key we
          // still HOLD — TanStack's sync write throws DuplicateKeySyncError on
          // that unless values deep-equal. Apply a held-key insert as the
          // upsert it semantically is (update upserts; move-in, ADR-0002 C4).
          else if (op === "insert" && syncedHas(key as string)) write({ type: "update", value: cols })
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
      }
    }

    if (syncMode === "on-demand") {
      // Hydration catch-up (ADR-0011 D3): the dehydrated rows are the union of
      // whatever subsets the server render loaded — per-subset resume is
      // unsound (a subset the render didn't cover has no since to resume
      // from, and overlapping predicates leave stale-delete holes). ONE
      // transient unfiltered sub from the dehydrated cursor covers every
      // changed key (always-emit ⇒ synthetic deletes included) in the
      // render→hydrate window, then unsubscribes at ITS terminal — never at a
      // broadcast boundary, which can precede its own frames. Semantic cost
      // (documented): rows outside any loaded subset that changed in the
      // window land in the collection.
      //
      // With NO resume point ("0"), or when the server resets the catch-up
      // (below the retention floor), the hydrated rows are honestly
      // UNRESUMABLE: truncate. In on-demand a full snapshot would strand
      // never-subscribed whole-table rows as permanently-stale state — worse
      // than a one-roundtrip refetch of the live subsets. The reset path
      // unsubscribes IMMEDIATELY so the server's trailing unfiltered
      // resnapshot is dropped on the floor (no handler), and the subset subs
      // repopulate right after.
      //
      // markReady gates on the catch-up sub FRAME being sent (not completed):
      // loadSubset subs only fire after ready, so on the single ordered
      // socket the catch-up's truncate/deltas always precede subset
      // snapshots. Ready never waits for data — stale-while-revalidate.
      const hc = consumeHydratedCursor()
      let readyGate: Promise<void>
      if (hc !== null && hc !== "0") {
        const catchupId = `${table}#hydrate#${++subSeq}`
        const done = (): void => transport.unsubscribe(catchupId)
        readyGate = transport.subscribe(
          catchupId,
          table,
          {
            onSnap: () => {}, // catch-ups never snapshot; reset's resnapshot is dropped (unsubbed)
            onSnapEnd: () => {},
            onDelta: makeHandler(() => {}).onDelta,
            onUptodate: (ownTerminal) => {
              flush()
              if (ownTerminal) done()
            },
            onReset: () => {
              flush()
              begin()
              truncate()
              commit()
              done() // before the trailing resnapshot frames arrive
            },
          },
          undefined,
          undefined,
          undefined,
          hc,
        )
      } else if (hc === "0") {
        // No resume point: drop the hydrated rows at sync start, honestly.
        readyGate = transport.connect().then(() => {
          begin()
          truncate()
          commit()
        })
      } else {
        readyGate = transport.connect()
      }
      void readyGate.then(() => markReady())

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
          if (collection.get(getKey(r as T)) === undefined) write({ type: "insert", value: r })
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

      return {
        loadSubset,
        unloadSubset,
        cleanup: () => {
          hydratedCursor = null // GC wiped the rows; a retained cursor would lie
          transport.close()
        },
      }
    }

    // eager
    {
      const hc = consumeHydratedCursor()
      if (hc !== null) {
        // Hydrated (ADR-0011 D3): the rows were applied upstream as synced
        // upserts before we ran. Resume from the dehydrated cursor (server
        // catch-up; below the floor an honest reset + resnapshot) — or, with
        // no resume point ("0"), take a fresh snapshot and RECONCILE it (D4).
        // Ready NOW: stale-while-revalidate is the explicit SSR contract —
        // first paint renders the hydrated rows, the boundary converges them.
        void transport.subscribe(
          eagerSubId,
          table,
          makeHandler(markReady, { reconcileSnapshots: true }),
          where,
          undefined,
          undefined,
          hc === "0" ? undefined : hc,
        )
        markReady()
      } else {
        void transport.subscribe(eagerSubId, table, makeHandler(markReady), where)
      }
    }
    return () => {
      hydratedCursor = null // GC wiped the rows; a retained cursor would lie
      transport.unsubscribe(eagerSubId)
    }
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

  // SSR syncMeta hooks (ADR-0011 D3) — called by TanStack's DbClient
  // dehydrate/hydrate (draft PR #1564); inert on older @tanstack/db versions.
  // The eager `where` fingerprint is the codec envelope — stable for the same
  // constructor code; a cross-deploy false mismatch merely downgrades to the
  // (always-sound) snapshot-reconcile path.
  const whereFingerprint = where == null ? undefined : codecEncode(where)
  const exportSyncMeta = (): DoSyncMeta => ({
    v: 1,
    cursor: transport.appliedCursor,
    ...(whereFingerprint === undefined ? {} : { where: whereFingerprint }),
  })
  const importSyncMeta = (meta: unknown): void => {
    // Upstream applies the dehydrated rows BEFORE this runs — there is no
    // veto. Validation failure throws out of hydrate(): fail loud, never
    // resume from a cursor we don't understand.
    const m = parseSyncMeta(meta)
    if (m.where === whereFingerprint) {
      hydratedCursor = m.cursor
      transport.seedCursor(m.cursor)
    } else {
      // The rows were dehydrated under a DIFFERENT eager filter: the cursor
      // is not a sound resume point for ours (see DoSyncMeta). "0" routes the
      // sync start to snapshot + reconcile; the transport cursor stays
      // unseeded so a bootstrap-window reconnect resnapshots too.
      hydratedCursor = "0"
    }
  }
  const mergeSyncMeta = (current: unknown, incoming: unknown): DoSyncMeta => {
    const a = parseSyncMeta(current)
    const b = parseSyncMeta(incoming)
    // MIN is self-healing: a late chunk's rows were already applied over
    // newer state (no veto); resuming from the EARLIER position replays the
    // window idempotently and re-freshens whatever the chunk clobbered.
    return BigInt(a.cursor) <= BigInt(b.cursor) ? a : b
  }

  return {
    id: opts.id ?? table,
    getKey,
    syncMode,
    sync: { sync, rowUpdateMode: "partial", exportSyncMeta, importSyncMeta, mergeSyncMeta },
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
