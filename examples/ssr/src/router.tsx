// The SSR seam (ADR-0011 D2), wired the released way: one DbClient per server
// request / per browser tab, handed to the router via context and
// `routerWithDbClient` — which wraps the app in DbProvider, dehydrates the
// server client into the router payload (rows + our resume cursor as opaque
// syncMeta), hydrates the browser client from it, and streams late Suspense
// query results. The only app-specific part is the transport dependency: a
// snapshot read against the DO on the server, a WebSocket in the browser.

import { DbClient } from "@tanstack/react-db"
import { createRouter } from "@tanstack/react-router"
import { routerWithDbClient } from "@tanstack/react-router-with-db"
import { createIsomorphicFn } from "@tanstack/react-start"
import { SsrSnapshotTransport, WebSocketTransport } from "tanstack-durable-object-sync/client"
import type { SnapshotRead, Transport } from "tanstack-durable-object-sync/client"
import { TODOS_TRANSPORT } from "./lib/todos.ts"
import { routeTree } from "./routeTree.gen"
import type { Env, TodosApi } from "./todos-do.ts"

export type RouterContext = {
  dbClient: DbClient
}

const createTransport = createIsomorphicFn()
  .server(
    (): (() => Transport<TodosApi>) =>
      () =>
        // No WebSocket from the render path: each subscribe is ONE snapshot
        // read over DO RPC. The worker-only modules are imported lazily so
        // they never reach the browser bundle.
        new SsrSnapshotTransport<TodosApi>({
          read: async (req) => {
            const [{ env }, { getRequest }] = await Promise.all([
              import("cloudflare:workers"),
              import("@tanstack/react-start/server"),
            ])
            const ns = (env as unknown as Env).TODOS_DO
            const stub = ns.get(ns.idFromName("main")) as unknown as {
              readSyncSnapshot: (r: Parameters<SnapshotRead>[0], request: Request) => ReturnType<SnapshotRead>
            }
            // The DO runs the incoming request through parseAttachment — the
            // SAME auth gate the WS upgrade gets. This app has no auth, but the
            // shape means an app that does can't bypass its own check via SSR.
            return stub.readSyncSnapshot(req, getRequest())
          },
        }),
  )
  .client(
    (): (() => Transport<TodosApi>) =>
      () =>
        new WebSocketTransport<TodosApi>({
          url: `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/sync/main`,
        }),
  )

export function getRouter() {
  // Per request on the server, once per tab in the browser. The collection
  // factory (lib/todos.ts) pulls the matching transport out of this bag.
  const dbClient = new DbClient({ [TODOS_TRANSPORT]: createTransport() })
  const router = createRouter({
    routeTree,
    context: { dbClient },
    scrollRestoration: true,
  })
  return routerWithDbClient(router, dbClient)
}
