// The SSR round trip (ADR-0011), lifted to a pathless layout so both showcase
// pages share it: a server function reads ONE snapshot from the DO and
// dehydrates it into the layout's loader payload; the browser hydrates that
// state into a fresh DbClient, paints immediately, then converges live over
// the WebSocket. One loader + one DbClient (one socket) per tab — per-page
// DbClients would open a fresh WebSocket on every client-side navigation
// between the pages, and the old one is never closed.

import { DbClient } from "@tanstack/db"
import { DbProvider } from "@tanstack/react-db"
import { createFileRoute, Link, Outlet } from "@tanstack/react-router"
import { createServerFn } from "@tanstack/react-start"
import { getRequest } from "@tanstack/react-start/server"
import { env } from "cloudflare:workers"
import * as React from "react"
import { SsrSnapshotTransport, WebSocketTransport } from "../../../../src/client/index.ts"
import type { SnapshotRead } from "../../../../src/client/index.ts"
import { inertSsrTransport, todosOptions } from "../lib/todos.ts"
import type { TodosCollection } from "../lib/todos.ts"
import { TodosContext } from "../lib/todos-context.ts"
import type { Env, TodosApi } from "../todos-do.ts"

// What actually rides the wire: plain JSON. Upstream's DehydratedDbState
// types `value`/`syncMeta` as `unknown`/`Record<string, unknown>` (adapter-
// opaque by design), which Start's serializable validation can't see through
// — assert the boundary with the concrete shape this adapter produces
// (SQLite rows + {v, cursor[, where]}).
type SerializableDbState = {
  collections: Array<{
    collectionId: string
    rows: Array<{ key: string; value: Record<string, string | number | null> }>
    syncMeta?: { v: 1; cursor: string; where?: string }
  }>
}

// Server-only by construction (createServerFn): the browser never re-runs the
// DO read — it gets the dehydrated payload. One transport + one DbClient PER
// REQUEST; module scope would leak cursor state across requests (ADR-0011 D2).
const getDbState = createServerFn().handler(async () => {
  const ns = (env as unknown as Env).TODOS_DO
  const stub = ns.get(ns.idFromName("main")) as unknown as {
    readSyncSnapshot: (r: Parameters<SnapshotRead>[0], request: Request) => ReturnType<SnapshotRead>
  }
  // The DO runs the incoming request through parseAttachment — the SAME auth
  // gate the WS upgrade gets. This app has no auth, but the shape means an
  // app that does can't bypass its own check via the read path.
  const request = getRequest()
  const transport = new SsrSnapshotTransport<TodosApi>({ read: (req) => stub.readSyncSnapshot(req, request) })
  const db = new DbClient()
  const todos = db.collection(todosOptions(transport)) as unknown as { preload: () => Promise<void> }
  await todos.preload()
  return db.dehydrate() as SerializableDbState
})

export const Route = createFileRoute("/_db")({
  // Runs once per document request (whichever child is hit) and once per
  // client-side entry into the subtree — the children never re-fetch it.
  loader: async () => ({ dbState: await getDbState() }),
  component: DbLayout,
})

function DbLayout() {
  const { dbState } = Route.useLoaderData()
  // One DbClient per browser tab, hydrated once from the loader payload. The
  // transport seam (ADR-0011 D2): the worker's render pass sits still on the
  // hydrated rows; the browser opens the real socket and converges.
  const [{ db, todos }] = React.useState(() => {
    const db = new DbClient()
    db.hydrate(dbState as never)
    const transport = import.meta.env.SSR
      ? inertSsrTransport()
      : new WebSocketTransport<TodosApi>({
          url: `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/sync/main`,
        })
    const todos = db.collection(todosOptions(transport)) as unknown as TodosCollection
    return { db, todos }
  })

  return (
    <DbProvider client={db}>
      <TodosContext.Provider value={todos}>
        <nav style={{ fontFamily: "system-ui, sans-serif", maxWidth: 640, margin: "1rem auto 0", padding: "0 1rem", display: "flex", gap: 12 }}>
          <Link to="/">home</Link>
          <Link to="/live-query">useLiveQuery</Link>
          <Link to="/live-suspense-query">useLiveSuspenseQuery</Link>
        </nav>
        <Outlet />
      </TodosContext.Provider>
    </DbProvider>
  )
}
