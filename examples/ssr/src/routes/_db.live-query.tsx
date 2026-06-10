// useLiveQuery over the hydrated collection: data is present from the first
// (server) render, `isReady` flips to live once the socket catches up.
// Stale-while-revalidate, never a flash of empty.

import { useLiveQuery } from "@tanstack/react-db"
import { createFileRoute } from "@tanstack/react-router"
import * as React from "react"
import { useTodos } from "../lib/todos-context.ts"
import type { Todo, TodosCollection } from "../lib/todos.ts"

export const Route = createFileRoute("/_db/live-query")({
  component: LiveQueryPage,
})

function LiveQueryPage() {
  return <Todos todos={useTodos()} />
}

function Todos({ todos }: { todos: TodosCollection }) {
  const [hydrated, setHydrated] = React.useState(false)
  const [text, setText] = React.useState("")
  const { data, isReady } = useLiveQuery((q) =>
    q.from({ t: todos as never }).orderBy(({ t }: { t: Todo }) => t.id, "asc"),
  ) as unknown as { data: Array<Todo>; isReady: boolean }

  React.useEffect(() => setHydrated(true), [])

  const add = () => {
    const t = text.trim()
    if (!t) return
    // Optimistic: appears instantly, confirmed on the single stream.
    todos.insert({ id: crypto.randomUUID(), text: t, done: 0 })
    setText("")
  }

  return (
    <main style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 4 }}>useLiveQuery todos</h2>
      <p style={{ color: "#666", marginTop: 0 }}>
        <span data-testid="hydration-state">{hydrated ? "hydrated" : "ssr"}</span>
        {" · "}
        <span data-testid="ready-state">{isReady ? "live" : "catching up"}</span>
        {" · "}
        rows: <b data-testid="ssr-row-count">{data.length}</b>
      </p>
      <ul data-testid="ssr-todo-list" style={{ listStyle: "none", padding: 0 }}>
        {data.map((t) => (
          <li key={t.id} data-testid={`ssr-todo-${t.id}`} style={{ padding: "4px 0" }}>
            <label style={{ textDecoration: t.done ? "line-through" : "none" }}>
              <input
                type="checkbox"
                checked={!!t.done}
                onChange={() => todos.update(t.id, (d) => void (d.done = d.done ? 0 : 1))}
              />{" "}
              {t.text}
            </label>
          </li>
        ))}
      </ul>
      <form
        onSubmit={(e) => {
          e.preventDefault()
          add()
        }}
        style={{ display: "flex", gap: 8 }}
      >
        <input
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="new todo…"
          style={{ flex: 1, padding: 8, borderRadius: 6, border: "1px solid #ccc" }}
        />
        <button type="submit" style={{ padding: "8px 16px", borderRadius: 6 }}>
          add
        </button>
      </form>
    </main>
  )
}
