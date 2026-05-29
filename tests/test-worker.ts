// Test worker entry — declares the DO classes bound by vitest.config's
// miniflare.durableObjects, and routes WebSocket upgrades to the sync DO.

import { DurableObject } from "cloudflare:workers"
import { Registry } from "../src/server/registry.ts"
import { SyncDurableObject } from "../src/server/sync-do.ts"

/** Bare DO for the M1 CDC tests; they drive storage via runInDurableObject. */
export class TestDO extends DurableObject {}

interface Claims {
  userId: string
}

/** Sync DO exercised by the WS lifecycle (M2) and read-path (M3) tests. The
 *  framework owns sub/mut/call dispatch; the subclass only declares its
 *  collections (mutations/commands arrive with the write-path increment). */
interface MsgRow {
  id: string
  body: string
}

export class SyncTestDO extends SyncDurableObject<unknown, Claims> {
  protected registry = new Registry<Claims>()
    .defineCollection({
      table: "messages",
      pk: "id",
      ddl: `CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`,
    })
    .defineMutation({
      collection: "messages",
      type: "insert",
      authorize: ({ op }) => {
        if ((op.cols as unknown as MsgRow).body === "FORBIDDEN") throw new Error("forbidden body")
      },
      execute: ({ op, sql }) => {
        const c = op.cols as unknown as MsgRow
        sql.exec("INSERT INTO messages(id, body) VALUES (?, ?)", c.id, c.body)
      },
    })
    .defineMutation({
      collection: "messages",
      type: "update",
      execute: ({ op, sql }) => {
        const c = op.cols as unknown as { body: string }
        sql.exec("UPDATE messages SET body = ? WHERE id = ?", c.body, op.key as string)
      },
    })
    .defineMutation({
      collection: "messages",
      type: "delete",
      execute: ({ op, sql }) => {
        sql.exec("DELETE FROM messages WHERE id = ?", op.key as string)
      },
    })
    .defineCommand({
      name: "echo",
      execute: ({ args }) => ({ echoed: args }),
    })

  protected override parseAttachment(req: Request): Claims {
    return { userId: req.headers.get("x-user") ?? "anon" }
  }
}

interface Env {
  TEST_DO: DurableObjectNamespace
  SYNC_DO: DurableObjectNamespace
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url)
    if (url.pathname.startsWith("/sync/")) {
      const name = url.pathname.slice("/sync/".length) || "default"
      return env.SYNC_DO.get(env.SYNC_DO.idFromName(name)).fetch(req)
    }
    return new Response("test-worker")
  },
}
