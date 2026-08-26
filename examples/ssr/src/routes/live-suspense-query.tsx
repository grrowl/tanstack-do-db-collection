// The Suspense-streaming SSR path: NO loader preload. The query is discovered
// mid-render by useLiveSuspenseQuery; on the server the component suspends
// while the snapshot transport reads the DO, and routerWithDbClient streams
// the pending query result into the document (the browser shows the fallback
// from the streamed shell, then the streamed rows, then the live WebSocket
// result once sync converges). Toggling the filter changes the structured
// query IR — a new query identity, a new derived collection — which re-suspends
// in the browser until it computes.

import { eq } from "@tanstack/db"
import { useDbClient, useLiveSuspenseQuery } from "@tanstack/react-db"
import { createFileRoute } from "@tanstack/react-router"
import * as React from "react"
import { todosCollection } from "../lib/todos.ts"

export const Route = createFileRoute("/live-suspense-query")({
  component: SuspensePage,
})

function SuspensePage() {
  const [hydrated, setHydrated] = React.useState(false)
  const [openOnly, setOpenOnly] = React.useState(false)

  React.useEffect(() => setHydrated(true), [])

  return (
    <main style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 4 }}>useLiveSuspenseQuery todos</h2>
      <p style={{ color: "#666", marginTop: 0 }}>
        <span data-testid="hydration-state">{hydrated ? "hydrated" : "ssr"}</span>
      </p>
      <button
        type="button"
        data-testid="filter-toggle"
        onClick={() => setOpenOnly((v) => !v)}
        style={{ padding: "6px 12px", borderRadius: 6, marginBottom: 8 }}
      >
        {openOnly ? "show all" : "show only open"}
      </button>
      <React.Suspense fallback={<p data-testid="suspense-fallback">loading todos…</p>}>
        <TodoRows openOnly={openOnly} />
      </React.Suspense>
    </main>
  )
}

function TodoRows({ openOnly }: { openOnly: boolean }) {
  const todos = useDbClient().collection(todosCollection)
  // Config-object form: the derived query identity is the structured IR, so
  // flipping `openOnly` swaps in a new live query rather than reusing rows.
  const { data } = useLiveSuspenseQuery({
    query: (q) => {
      const base = q.from({ t: todosCollection })
      const scoped = openOnly ? base.where(({ t }) => eq(t.done, 0)) : base
      return scoped.orderBy(({ t }) => t.id, "asc")
    },
  })

  return (
    <>
      <p style={{ color: "#666" }}>
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
    </>
  )
}
