// One collection shape, three transports (ADR-0011 D2): the loader reads a
// snapshot from the DO, the browser goes live over WebSocket, and the worker's
// React render pass sits still on rows hydrate() already applied. The id
// defaults to the table name ("todos") on every side — that match is what lets
// hydrate() route the dehydrated rows into this collection.

import { collectionOptions } from "@tanstack/db"
import { doCollectionOptions, SsrSnapshotTransport } from "../../../../src/client/index.ts"
import type { Transport } from "../../../../src/client/index.ts"

export interface Todo {
  id: string
  text: string
  /** SQLite INTEGER 0/1 — kept raw so optimistic and confirmed rows are identical. */
  done: number
}

/** The branded options DbClient wants, around our adapter. One per DbClient;
 *  the `as never` casts bridge the vendored draft-PR types (see tests/ssr-*). */
export function todosOptions(transport: Transport) {
  return collectionOptions(
    doCollectionOptions<Todo>({ transport, table: "todos", getKey: (t) => t.id }) as never,
  ) as never
}

/** What the component's collection can do; `db.collection` on the draft-PR
 *  build is untyped, so the caller casts to this. */
export interface TodosCollection {
  insert: (t: Todo) => unknown
  update: (key: string, fn: (draft: Todo) => void) => unknown
}

/** The worker's render pass needs no data source — hydrate() applied the
 *  loader's rows before the first paint, and convergence is the browser's job.
 *  A never-resolving read keeps the snapshot transport inert (no second DO
 *  read, mutations still fail loud) for the lifetime of the request. */
export function inertSsrTransport(): Transport {
  return new SsrSnapshotTransport({ read: () => new Promise(() => {}) })
}
