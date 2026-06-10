// useLiveSuspenseQuery over the hydrated collection. The point on display:
// hydration makes the source collection `ready` synchronously (the rows came
// with the document), so the FIRST paint never suspends — on the server or in
// the browser — and the raw HTML contains rows, not the fallback. Changing the
// query's identity (the where clause below) creates a new derived collection,
// which DOES suspend until it loads; the fallback counter makes that visible
// and testable.

import { eq } from "@tanstack/db"
import { useLiveSuspenseQuery } from "@tanstack/react-db"
import { createFileRoute } from "@tanstack/react-router"
import * as React from "react"
import { useTodos } from "../lib/todos-context.ts"
import type { Todo, TodosCollection } from "../lib/todos.ts"

export const Route = createFileRoute("/_db/live-suspense-query")({
  component: SuspensePage,
})

function SuspensePage() {
  const todos = useTodos()
  const [hydrated, setHydrated] = React.useState(false)
  const [openOnly, setOpenOnly] = React.useState(false)
  // Incremented by the fallback's mount effect: stays 0 if first paint never
  // suspends (the claim under test), goes up when an identity change does.
  const [fallbackCount, setFallbackCount] = React.useState(0)

  React.useEffect(() => setHydrated(true), [])

  return (
    <main style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "2rem auto", padding: "0 1rem" }}>
      <h2 style={{ marginBottom: 4 }}>useLiveSuspenseQuery todos</h2>
      <p style={{ color: "#666", marginTop: 0 }}>
        <span data-testid="hydration-state">{hydrated ? "hydrated" : "ssr"}</span>
        {" · "}
        fallbacks shown: <b data-testid="fallback-count">{fallbackCount}</b>
      </p>
      <button
        type="button"
        data-testid="filter-toggle"
        onClick={() => setOpenOnly((v) => !v)}
        style={{ padding: "6px 12px", borderRadius: 6, marginBottom: 8 }}
      >
        {openOnly ? "show all" : "show only open"}
      </button>
      <React.Suspense fallback={<Fallback onShown={() => setFallbackCount((c) => c + 1)} />}>
        <TodoRows todos={todos} openOnly={openOnly} />
      </React.Suspense>
    </main>
  )
}

function Fallback({ onShown }: { onShown: () => void }) {
  // Effects never run during SSR, so a server-rendered fallback would still be
  // visible in the raw HTML — the curl check covers that side.
  React.useEffect(() => onShown(), [onShown])
  return <p data-testid="suspense-fallback">loading todos…</p>
}

function TodoRows({ todos, openOnly }: { todos: TodosCollection; openOnly: boolean }) {
  // Config-object form: the derived query identity includes the structured
  // where clause, so flipping `openOnly` re-suspends (new collection) rather
  // than silently reusing the old rows.
  const { data } = useLiveSuspenseQuery({
    query: (q) => {
      const base = q.from({ t: todos as never })
      const scoped = openOnly ? base.where(({ t }: { t: Todo }) => eq(t.done as never, 0)) : base
      return scoped.orderBy(({ t }: { t: Todo }) => t.id, "asc")
    },
  }) as unknown as { data: Array<Todo> }

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
