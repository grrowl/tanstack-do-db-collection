// In-memory subscription registry, keyed by WebSocket. Subscriptions live in
// memory only — they are lost on hibernation and re-established by the client's
// `resub` on reconnect (M7).
//
// A subscription carries an optional `where` predicate IR (M5). It is compiled
// with @tanstack/db's own evaluator so server-side filtering matches the
// client's operator semantics exactly (no second predicate implementation).

import { compileSingleRowExpression, toBooleanPredicate } from "@tanstack/db"
import { UnsupportedPredicateError } from "./sql-compiler.ts"

export interface Sub {
  subId: string
  collection: string
  /** Predicate IR as it arrived on the wire (BasicExpression), if filtered. */
  where?: unknown
  /** Compiled row test; always-true for an unfiltered subscription. */
  predicate: (row: Record<string, unknown>) => boolean
}

function compilePredicate(where: unknown): (row: Record<string, unknown>) => boolean {
  if (where === undefined || where === null) return () => true
  let evaluate: (row: Record<string, unknown>) => boolean | null
  try {
    evaluate = compileSingleRowExpression(where as never) as (row: Record<string, unknown>) => boolean | null
  } catch (e) {
    // @tanstack/db's evaluator rejects any operator outside its registry (e.g. a
    // hand-built `ne`, which it does not know — only `not(eq(...))`). The SQL
    // snapshot floor (sql-compiler.ts) and this JS delta floor MUST be the same
    // set, or a sub's membership depends on which path decided it. Re-throw as
    // UnsupportedPredicateError so handleSub rejects the sub with a `reset`
    // (fail loud) instead of letting this escape uncaught and hang the client. (ADR-0013)
    throw new UnsupportedPredicateError(
      `predicate not compilable by @tanstack/db evaluator: ${e instanceof Error ? e.message : String(e)}`,
    )
  }
  // toBooleanPredicate collapses SQL 3-valued null to false, matching SQL.
  return (row) => toBooleanPredicate(evaluate(row))
}

export class SubscriptionRegistry {
  private readonly subsByWs = new WeakMap<WebSocket, Map<string, Sub>>()
  // Reverse index for delta fan-out by collection (used from M3's write path).
  private readonly wsByCollection = new Map<string, Set<WebSocket>>()

  add(ws: WebSocket, subId: string, collection: string, where?: unknown): Sub {
    const sub: Sub = { subId, collection, where, predicate: compilePredicate(where) }
    let m = this.subsByWs.get(ws)
    if (!m) {
      m = new Map()
      this.subsByWs.set(ws, m)
    }
    m.set(subId, sub)
    let set = this.wsByCollection.get(collection)
    if (!set) {
      set = new Set()
      this.wsByCollection.set(collection, set)
    }
    set.add(ws)
    return sub
  }

  remove(ws: WebSocket, subId: string): void {
    const m = this.subsByWs.get(ws)
    const sub = m?.get(subId)
    if (!m || !sub) return
    m.delete(subId)
    let stillHas = false
    for (const s of m.values()) {
      if (s.collection === sub.collection) {
        stillHas = true
        break
      }
    }
    if (!stillHas) this.wsByCollection.get(sub.collection)?.delete(ws)
  }

  removeAll(ws: WebSocket): void {
    if (!this.subsByWs.has(ws)) return
    for (const set of this.wsByCollection.values()) set.delete(ws)
    this.subsByWs.delete(ws)
  }

  forWs(ws: WebSocket): Array<Sub> {
    return Array.from(this.subsByWs.get(ws)?.values() ?? [])
  }

  /** Number of active subscriptions on a single socket — for per-socket cap enforcement. */
  countFor(ws: WebSocket): number {
    return this.subsByWs.get(ws)?.size ?? 0
  }

  /** All (ws, sub) pairs subscribed to a collection — for delta fan-out. */
  forCollection(collection: string): Array<{ ws: WebSocket; sub: Sub }> {
    const out: Array<{ ws: WebSocket; sub: Sub }> = []
    const wss = this.wsByCollection.get(collection)
    if (!wss) return out
    for (const ws of wss) {
      const m = this.subsByWs.get(ws)
      if (!m) continue
      for (const sub of m.values()) {
        if (sub.collection === collection) out.push({ ws, sub })
      }
    }
    return out
  }
}
