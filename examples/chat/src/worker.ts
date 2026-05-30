// Chat example — Worker + SessionDO.
//
// Imports the library straight from source (../../../src) so the example tracks
// the real code with no build step for the lib. A published consumer would
// instead `import { ... } from "tanstack-do-db-collection"`.

import { Registry, SyncDurableObject } from "../../../src/server/index.ts"

interface Env {
  SESSION_DO: DurableObjectNamespace
  ASSETS: { fetch: (req: Request) => Promise<Response> }
}

interface Claims {
  userId: string
}

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
      this.registerSync(
        new Registry<Claims>()
          .defineCollection({ table: "messages", pk: "id" })
          .defineMutation({
            collection: "messages",
            type: "insert",
            // Only let a client write messages authored by itself.
            authorize: ({ user, op }) => {
              if ((op.cols as { author: string }).author !== user.userId) {
                throw new Error("author must match the connected user")
              }
            },
            execute: ({ op, sql }) => {
              const c = op.cols as { id: string; author: string; content: string; created_at: number }
              sql.exec(
                "INSERT INTO messages(id, author, content, created_at) VALUES (?, ?, ?, ?)",
                c.id,
                c.author,
                c.content,
                c.created_at,
              )
            },
          }),
      )
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
