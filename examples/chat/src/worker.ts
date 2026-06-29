// Chat example — Worker + SessionDO.
//
// Imports the library straight from source (../../../src) so the example tracks
// the real code with no build step for the lib. A published consumer would
// instead `import { ... } from "tanstack-do-db-collection"`.

import { defineSync, SyncDurableObject } from "../../../src/server/index.ts"

interface Env {
  SESSION_DO: DurableObjectNamespace
  ASSETS: { fetch: (req: Request) => Promise<Response> }
}

interface Claims {
  userId: string
}

interface Message {
  id: string
  author: string
  content: string
  created_at: number
}

// `defineSync` binds identity (Claims) and binding-env (Env) once; the helpers
// it returns flow those types into every handler ctx.
const sync = defineSync<Claims, Env>()

const chatSchema = sync.schema({
  collections: {
    // The collection KEY is the table name (ADR-0007: sole TEXT, client pk).
    messages: sync.collection<Message>({
      pk: "id",
      mutations: {
        insert: {
          // Only let a client write messages authored by itself.
          authorize: ({ user, op }) => {
            if (op.cols.author !== user.userId) {
              throw new Error("author must match the connected user")
            }
          },
          execute: ({ op, sql }) => {
            const c = op.cols
            sql.exec(
              "INSERT INTO messages(id, author, content, created_at) VALUES (?, ?, ?, ?)",
              c.id,
              c.author,
              c.content,
              c.created_at,
            )
          },
        },
      },
    }),
  },
  commands: {
    // A COMMAND (not a mutation): "clear the room" isn't a single typed row
    // write, so it can't be insert/update/delete. A command is the escape hatch
    // — it runs outside the optimistic path, can return a result, and (the part
    // worth seeing) its own SQL writes still flow through the CDC triggers, so
    // the DELETE fans out to every connected tab as ordinary delete deltas. The
    // collection empties live for everyone, and the caller gets the count back
    // on `committed`.
    clearRoom: sync.command()(({ sql }) => {
      const before = Array.from(sql.exec("SELECT count(*) AS c FROM messages"))[0]!.c as number
      sql.exec("DELETE FROM messages")
      return { deleted: before }
    }),
  },
})

// The client type-only imports this to type its transport + collections.
export type ChatApi = typeof chatSchema

export class SessionDO extends SyncDurableObject<Env, Claims> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      // You own your schema (ADR-0007); the framework wires sync after.
      this.sql.exec(`CREATE TABLE IF NOT EXISTS messages (
        id         TEXT PRIMARY KEY,
        author     TEXT NOT NULL,
        content    TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )`)
      this.registerSync(chatSchema)
    })
  }

  // The example trusts a `user` query param for identity. A real app verifies a
  // token at the Worker and forges a claims header (see the README).
  protected override parseAttachment(req: Request): Claims {
    return { userId: new URL(req.url).searchParams.get("user") ?? "anon" }
  }
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url)
    if (url.pathname === "/sync") {
      const room = url.searchParams.get("room") ?? "lobby"
      return env.SESSION_DO.get(env.SESSION_DO.idFromName(room)).fetch(req)
    }
    return env.ASSETS.fetch(req) // index.html + client.js
  },
} satisfies ExportedHandler<Env>
