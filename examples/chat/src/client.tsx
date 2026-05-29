// Chat example — browser client. React + useLiveQuery over a DO-backed
// collection. Imports the library from source (../../../src/client).

import { createCollection } from "@tanstack/db"
import { useLiveQuery } from "@tanstack/react-db"
import { useState } from "react"
import { createRoot } from "react-dom/client"
import { ulid } from "ulid"
import { doCollectionOptions, WebSocketTransport } from "../../../src/client/index.ts"

interface Message {
  id: string
  author: string
  content: string
  created_at: number
}

// A throwaway identity persisted per browser, passed to the DO as ?user=.
const user =
  localStorage.getItem("chat-user") ??
  (() => {
    const u = `user-${Math.random().toString(36).slice(2, 6)}`
    localStorage.setItem("chat-user", u)
    return u
  })()

const wsProto = location.protocol === "https:" ? "wss:" : "ws:"
const transport = new WebSocketTransport({
  url: `${wsProto}//${location.host}/sync?room=lobby&user=${encodeURIComponent(user)}`,
})

// One transport per DO, shared by every collection on it.
const messages = createCollection(
  doCollectionOptions<Message>({ transport, table: "messages", getKey: (m) => m.id }),
)

function App(): JSX.Element {
  const { data } = useLiveQuery((q) => q.from({ m: messages }).orderBy(({ m }) => m.created_at, "asc"))
  const [text, setText] = useState("")

  const send = (): void => {
    const content = text.trim()
    if (!content) return
    // Optimistic: appears instantly, confirmed on the single stream.
    messages.insert({ id: ulid(), author: user, content, created_at: Date.now() })
    setText("")
  }

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 4 }}>tanstack-do-db chat</h2>
      <p style={{ color: "#666", marginTop: 0 }}>
        you are <b>{user}</b> · open a second tab to watch live sync
      </p>
      <div
        style={{
          border: "1px solid #ddd",
          borderRadius: 8,
          padding: 12,
          height: 320,
          overflowY: "auto",
          background: "#fafafa",
        }}
      >
        {data.length === 0 ? (
          <p style={{ color: "#999" }}>No messages yet — say hi.</p>
        ) : (
          data.map((m) => (
            <div key={m.id} style={{ padding: "2px 0" }}>
              <b style={{ color: m.author === user ? "#0a7" : "#357" }}>{m.author}</b>: {m.content}
            </div>
          ))
        )}
      </div>
      <form
        onSubmit={(e) => {
          e.preventDefault()
          send()
        }}
        style={{ display: "flex", gap: 8, marginTop: 8 }}
      >
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="message…"
          style={{ flex: 1, padding: 8, borderRadius: 6, border: "1px solid #ccc" }}
        />
        <button type="submit" style={{ padding: "8px 16px", borderRadius: 6 }}>
          send
        </button>
      </form>
    </div>
  )
}

createRoot(document.getElementById("root")!).render(<App />)
