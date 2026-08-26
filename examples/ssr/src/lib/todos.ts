// One collection descriptor, two transports (ADR-0011 D2): the server render
// reads a snapshot from the DO, the browser goes live over WebSocket. The
// descriptor is shared — `collectionOptions(id, factory)` gives every DbClient
// a fresh adapter config, and the factory pulls the environment's transport
// out of the client's dependency bag (injected in router.tsx). The id matches
// the table name ("todos") on every side — that match is what routes the
// dehydrated rows back into this collection on hydrate.

import { collectionOptions } from "@tanstack/db"
import { doCollectionOptions } from "tanstack-durable-object-sync/client"
import type { Transport } from "tanstack-durable-object-sync/client"
import type { TodosApi } from "../todos-do.ts"

export interface Todo {
  id: string
  text: string
  /** SQLite INTEGER 0/1 — kept raw so optimistic and confirmed rows are identical. */
  done: number
}

/** DbClient dependency key: `() => Transport<TodosApi>` — snapshot transport on
 *  the server, WebSocket transport in the browser (see router.tsx). */
export const TODOS_TRANSPORT = "todosTransport"

export const todosCollection = collectionOptions("todos", (client) => {
  const createTransport = client.requireDependency<() => Transport<TodosApi>>(TODOS_TRANSPORT)
  return doCollectionOptions<TodosApi, "todos">({
    transport: createTransport(),
    table: "todos",
    getKey: (t) => t.id,
  })
})
