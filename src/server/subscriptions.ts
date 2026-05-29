// In-memory subscription registry, keyed by WebSocket. Subscriptions live in
// memory only — they are lost on hibernation and re-established by the client's
// `resub` on reconnect (M7). For M3 a subscription is just (subId, collection);
// predicates/shaping arrive in M5/M6.

export interface Sub {
  subId: string
  collection: string
}

export class SubscriptionRegistry {
  private readonly subsByWs = new WeakMap<WebSocket, Map<string, Sub>>()
  // Reverse index for delta fan-out by collection (used from M3's write path).
  private readonly wsByCollection = new Map<string, Set<WebSocket>>()

  add(ws: WebSocket, subId: string, collection: string): Sub {
    const sub: Sub = { subId, collection }
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
