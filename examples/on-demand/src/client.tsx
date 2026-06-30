// On-demand example — browser client. Each category is a panel; mounting it
// runs loadSubset(where category=X), unmounting runs unloadSubset. Only opened
// categories are ever loaded into the collection.

import { createCollection, eq } from "@tanstack/db"
import { useLiveQuery } from "@tanstack/react-db"
import { useState } from "react"
import { createRoot } from "react-dom/client"
import { ulid } from "ulid"
import { doCollectionOptions, WebSocketTransport } from "../../../src/client/index.ts"
import type { ItemsApi } from "./worker.ts"

const CATEGORIES = ["A", "B", "C"]
const room = new URLSearchParams(location.search).get("room") ?? "demo"
const wsProto = location.protocol === "https:" ? "wss:" : "ws:"
const transport = new WebSocketTransport<ItemsApi>({
  url: `${wsProto}//${location.host}/sync?room=${encodeURIComponent(room)}`,
})

const items = createCollection(
  doCollectionOptions({ transport, table: "items", getKey: (i) => i.id, syncMode: "on-demand" }),
)

// Mounting requests this category's subset; unmounting releases it.
function CategoryPanel({ category }: { category: string }): JSX.Element {
  const { data } = useLiveQuery((q) =>
    q
      .from({ i: items })
      .where(({ i }) => eq(i.category, category))
      .orderBy(({ i }) => i.created_at, "asc"),
  )
  return (
    <ul data-testid={`panel-${category}`} style={{ border: "1px solid #ddd", borderRadius: 8, padding: 12, minHeight: 80 }}>
      {data.length === 0 ? (
        <li style={{ color: "#999", listStyle: "none" }}>(nothing loaded for {category})</li>
      ) : (
        data.map((i) => (
          <li key={i.id} data-id={i.id}>
            {i.text}
          </li>
        ))
      )}
    </ul>
  )
}

function App(): JSX.Element {
  const [open, setOpen] = useState<string>("A")
  const [addCat, setAddCat] = useState<string>("A")
  const [text, setText] = useState("")

  const add = (): void => {
    const t = text.trim()
    if (!t) return
    items.insert({ id: ulid(), category: addCat, text: t, created_at: Date.now() })
    setText("")
  }

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 4 }}>tanstack-do-db — on-demand subsets</h2>
      <p style={{ color: "#666", marginTop: 0 }}>
        Each category loads only when opened. Items in unopened categories aren't synced.
        Currently loaded rows: <b data-testid="size">{items.size}</b>
      </p>
      <div style={{ display: "flex", gap: 8 }}>
        {CATEGORIES.map((c) => (
          <button
            key={c}
            data-testid={`open-${c}`}
            onClick={() => setOpen(c)}
            style={{ padding: "6px 14px", borderRadius: 6, fontWeight: open === c ? 700 : 400 }}
          >
            {c}
          </button>
        ))}
      </div>
      <h3>Category {open}</h3>
      {/* key forces remount on switch -> unloadSubset(old) + loadSubset(new) */}
      <CategoryPanel key={open} category={open} />
      <form onSubmit={(e) => { e.preventDefault(); add() }} style={{ display: "flex", gap: 8, marginTop: 12 }}>
        <select data-testid="add-cat" value={addCat} onChange={(e) => setAddCat(e.target.value)}>
          {CATEGORIES.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>
        <input
          data-testid="add-text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="item text…"
          style={{ flex: 1, padding: 8, borderRadius: 6, border: "1px solid #ccc" }}
        />
        <button type="submit" data-testid="add-btn" style={{ padding: "8px 16px", borderRadius: 6 }}>
          add
        </button>
      </form>
    </div>
  )
}

createRoot(document.getElementById("root")!).render(<App />)
