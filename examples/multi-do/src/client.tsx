// Multi-DO example — browser client.
//
// TWO Durable Objects ⇒ TWO transports. The client opens one WebSocket per DO
// (RoomDO at /rooms/:room/sync, InboxDO at /inbox/:user/sync) — there is no
// single muxed connection; each transport is the ordered stream for exactly one
// DO. Each Api is imported as a TYPE ONLY: nothing server-side is bundled (the
// `import type` is elided by esbuild), but Row/Args/Result are recovered
// structurally to type `transport.call.*` and the collections.

import { createCollection } from "@tanstack/db"
import { useLiveQuery } from "@tanstack/react-db"
import { createContext, useContext, useState, type ReactNode } from "react"
import { createRoot } from "react-dom/client"
import { ulid } from "ulid"
import { doCollectionOptions, WebSocketTransport } from "../../../src/client/index.ts"
import type { InboxApi } from "./inbox-schema.ts"
import type { RoomApi } from "./room-schema.ts"

// A throwaway identity persisted per browser, passed to both DOs as ?user=.
const user =
  localStorage.getItem("multi-do-user") ??
  (() => {
    const u = `user-${Math.random().toString(36).slice(2, 6)}`
    localStorage.setItem("multi-do-user", u)
    return u
  })()

const wsProto = location.protocol === "https:" ? "wss:" : "ws:"
const q = `user=${encodeURIComponent(user)}`

// One transport PER DO. Each is parameterized by that DO's Api, so its typed
// `call` namespace only exposes that DO's commands.
const roomTransport = new WebSocketTransport<RoomApi>({
  url: `${wsProto}//${location.host}/rooms/lobby/sync?${q}`,
})
const inboxTransport = new WebSocketTransport<InboxApi>({
  url: `${wsProto}//${location.host}/inbox/${encodeURIComponent(user)}/sync?${q}`,
})

// One collection per (DO, table). Row is inferred from the Api + table — no
// runtime schema value crosses the wire.
const messages = createCollection(
  doCollectionOptions<RoomApi, "messages">({ transport: roomTransport, table: "messages", getKey: (m) => m.id }),
)
const notifications = createCollection(
  doCollectionOptions<InboxApi, "notifications">({
    transport: inboxTransport,
    table: "notifications",
    getKey: (n) => n.id,
  }),
)

// --- SyncProvider: transports keyed BY DO so command namespaces never collide.
// `useSync("rooms").call.*` and `useSync("inbox").call.*` are two disjoint,
// independently-typed namespaces — even if both DOs named a command the same.
interface SyncMap {
  rooms: WebSocketTransport<RoomApi>
  inbox: WebSocketTransport<InboxApi>
}

const SyncContext = createContext<SyncMap | null>(null)

function SyncProvider({ value, children }: { value: SyncMap; children: ReactNode }): JSX.Element {
  return <SyncContext.Provider value={value}>{children}</SyncContext.Provider>
}

function useSync<K extends keyof SyncMap>(key: K): SyncMap[K] {
  const map = useContext(SyncContext)
  if (!map) throw new Error("useSync must be used inside a <SyncProvider>")
  return map[key]
}

function App(): JSX.Element {
  const rooms = useSync("rooms")
  const inbox = useSync("inbox")

  const { data: msgs } = useLiveQuery((qb) => qb.from({ m: messages }).orderBy(({ m }) => m.created_at, "asc"))
  const { data: notes } = useLiveQuery((qb) => qb.from({ n: notifications }).orderBy(({ n }) => n.created_at, "asc"))

  const [text, setText] = useState("")

  // The DO never joins/aggregates (ADR-0001) — a cross-DO view is assembled
  // HERE, client-side, by merging two live queries (one per DO) into a single
  // timeline. Both inputs stay live, so the merged feed updates on either DO.
  const feed: Array<{ id: string; at: number; from: "rooms" | "inbox"; label: string }> = [
    ...msgs.map((m) => ({ id: m.id, at: m.created_at, from: "rooms" as const, label: `${m.author}: ${m.content}` })),
    ...notes.map((n) => ({
      id: n.id,
      at: n.created_at,
      from: "inbox" as const,
      label: `[${n.read ? "read" : "unread"}] ${n.kind} — ${n.body}`,
    })),
  ].sort((a, b) => a.at - b.at)

  const post = (): void => {
    const content = text.trim()
    if (!content) return
    // Optimistic typed insert on the RoomDO collection.
    messages.insert({ id: ulid(), author: user, content, created_at: Date.now() })
    setText("")
  }

  // Drop a notification into MY inbox DO (optimistic insert; read = 0).
  const notify = (): void => {
    notifications.insert({
      id: ulid(),
      user,
      kind: "ping",
      body: `hello at ${new Date().toLocaleTimeString()}`,
      read: 0,
      created_at: Date.now(),
    })
  }

  // Commands — one per DO, reached through that DO's transport. clearRoom takes
  // no args (zero-arg call); both return a count on `committed`.
  const clearRoom = async (): Promise<void> => {
    const { deleted } = await rooms.call.clearRoom()
    console.log(`cleared ${deleted} message(s)`)
  }
  const markAllRead = async (): Promise<void> => {
    const { marked } = await inbox.call.markAllRead()
    console.log(`marked ${marked} notification(s) read`)
  }

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", maxWidth: 720, margin: "2rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 4 }}>tanstack-do-db · multi-DO</h2>
      <p style={{ color: "#666", marginTop: 0 }}>
        you are <b>{user}</b> · room <b>lobby</b> (RoomDO) + your inbox (InboxDO), two transports, one merged feed
      </p>

      <div style={{ display: "flex", gap: 8, marginBottom: 8 }}>
        <button type="button" onClick={() => void clearRoom()} disabled={msgs.length === 0} style={btn}>
          rooms.clearRoom()
        </button>
        <button
          type="button"
          onClick={() => void markAllRead()}
          disabled={notes.every((n) => n.read)}
          style={btn}
        >
          inbox.markAllRead()
        </button>
        <button type="button" onClick={notify} style={btn}>
          notify me
        </button>
      </div>

      <div
        style={{
          border: "1px solid #ddd",
          borderRadius: 8,
          padding: 12,
          height: 360,
          overflowY: "auto",
          background: "#fafafa",
        }}
      >
        {feed.length === 0 ? (
          <p style={{ color: "#999" }}>Empty — post a message or hit “notify me”.</p>
        ) : (
          feed.map((e) => (
            <div key={`${e.from}-${e.id}`} style={{ padding: "2px 0" }}>
              <b style={{ color: e.from === "rooms" ? "#357" : "#a60" }}>{e.from}</b> · {e.label}
            </div>
          ))
        )}
      </div>

      <form
        onSubmit={(e) => {
          e.preventDefault()
          post()
        }}
        style={{ display: "flex", gap: 8, marginTop: 8 }}
      >
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="message the room…"
          style={{ flex: 1, padding: 8, borderRadius: 6, border: "1px solid #ccc" }}
        />
        <button type="submit" style={{ padding: "8px 16px", borderRadius: 6 }}>
          post
        </button>
      </form>
    </div>
  )
}

const btn = { padding: "4px 10px", borderRadius: 6, border: "1px solid #ccc", background: "#fff" } as const

createRoot(document.getElementById("root")!).render(
  <SyncProvider value={{ rooms: roomTransport, inbox: inboxTransport }}>
    <App />
  </SyncProvider>,
)
