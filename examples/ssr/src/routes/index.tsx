// The SSR round trip (ADR-0011): a server function reads ONE snapshot from the
// DO and dehydrates it into the route payload; the browser hydrates that state
// into a fresh DbClient, paints immediately, then converges live over the
// WebSocket (catch-up from the dehydrated cursor — updates, tombstones, all of
// it). Stale-while-revalidate, never a flash of empty.

import { DbClient } from "@tanstack/db"
import { DbProvider, useLiveQuery } from "@tanstack/react-db"
import { createFileRoute } from "@tanstack/react-router"
import { createServerFn } from "@tanstack/react-start"
import { env } from "cloudflare:workers"
import * as React from "react"
import { SsrSnapshotTransport, WebSocketTransport } from "../../../../src/client/index.ts"
import type { SnapshotRead } from "../../../../src/client/index.ts"
import { inertSsrTransport, todosOptions } from "../lib/todos.ts"
import type { Todo, TodosCollection } from "../lib/todos.ts"
import type { Env } from "../todos-do.ts"

// Server-only by construction (createServerFn): the browser never re-runs the
// DO read — it gets the dehydrated payload. One transport + one DbClient PER
// REQUEST; module scope would leak cursor state across requests (ADR-0011 D2).
const getDbState = createServerFn().handler(async () => {
  const ns = (env as unknown as Env).TODOS_DO
  const stub = ns.get(ns.idFromName("main")) as unknown as { readSnapshot: SnapshotRead }
  const transport = new SsrSnapshotTransport({ read: (req) => stub.readSnapshot(req) })
  const db = new DbClient()
  const todos = db.collection(todosOptions(transport)) as unknown as { preload: () => Promise<void> }
  await todos.preload()
  return db.dehydrate()
})

export const Route = createFileRoute("/")({
  loader: async () => ({ dbState: await getDbState() }),
  component: TodosPage,
})

function TodosPage() {
  const { dbState } = Route.useLoaderData()
  // One DbClient per browser tab, hydrated once from the loader payload. The
  // transport seam (ADR-0011 D2): the worker's render pass sits still on the
  // hydrated rows; the browser opens the real socket and converges.
  const [{ db, todos }] = React.useState(() => {
    const db = new DbClient()
    db.hydrate(dbState as never)
    const transport = import.meta.env.SSR
      ? inertSsrTransport()
      : new WebSocketTransport({
          url: `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/sync/main`,
        })
    const todos = db.collection(todosOptions(transport)) as unknown as TodosCollection
    return { db, todos }
  })

  return (
    <DbProvider client={db}>
      <Todos todos={todos} />
    </DbProvider>
  )
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
      <h2 style={{ marginBottom: 4 }}>tanstack-do-db SSR todos</h2>
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
