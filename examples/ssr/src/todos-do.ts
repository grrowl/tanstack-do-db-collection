// SSR example — the sync DO. One `todos` collection plus the three row
// mutations the browser client sends, authored with the object-schema API
// (defineSync; ADR-0014). Depends on the local package (`file:../..`), so run
// `npm run build` at the repo root before installing here.

import { defineSync, SyncDurableObject } from "tanstack-durable-object-sync"
import type { Todo } from "./lib/todos.ts"

export interface Env {
  TODOS_DO: DurableObjectNamespace
}

const UPDATABLE = new Set(["text", "done"])

const sync = defineSync<unknown, Env>()

const todosSchema = sync.schema({
  collections: {
    todos: sync.collection<Todo>({
      pk: "id",
      mutations: {
        insert: {
          execute: ({ op, sql }) => {
            sql.exec("INSERT INTO todos(id, text, done) VALUES (?, ?, ?)", op.cols.id, op.cols.text, op.cols.done)
          },
        },
        update: {
          // A toggle/edit sends a getChanges() diff; build the SET from the
          // present keys, allowing only the updatable columns.
          execute: ({ op, sql }) => {
            const cols = op.cols as Record<string, unknown>
            const keys = Object.keys(cols).filter((k) => UPDATABLE.has(k))
            if (keys.length === 0) return
            const set = keys.map((k) => `"${k}" = ?`).join(", ")
            sql.exec(`UPDATE todos SET ${set} WHERE id = ?`, ...keys.map((k) => cols[k]), op.key)
          },
        },
        delete: {
          execute: ({ op, sql }) => {
            sql.exec("DELETE FROM todos WHERE id = ?", op.key)
          },
        },
      },
    }),
  },
})

/** The schema's type — the client brands its transports with this so
 *  `doCollectionOptions` infers the `todos` row type (no runtime schema value). */
export type TodosApi = typeof todosSchema

export class TodosDO extends SyncDurableObject<Env> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      // You own your schema (ADR-0007); the framework wires sync after.
      this.sql.exec(`CREATE TABLE IF NOT EXISTS todos (
        id   TEXT PRIMARY KEY,
        text TEXT NOT NULL,
        done INTEGER NOT NULL DEFAULT 0
      )`)
      this.registerSync(todosSchema)
      // Seed AFTER registerSync so the rows flow through CDC and the first
      // render gets a real (nonzero) resume cursor. Direct SQL is fine here:
      // boot precedes any socket, so there is nothing to broadcast (ADR-0006).
      this.sql.exec(`INSERT OR IGNORE INTO todos(id, text, done) VALUES
        ('seed-1', 'Server-render this list', 1),
        ('seed-2', 'Hydrate without a flash of empty', 0),
        ('seed-3', 'Converge live over WebSocket', 0)`)
    })
  }
}
