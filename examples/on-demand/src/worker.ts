// On-demand example — Worker + ItemsDO. One `items` collection, categorised.
// A GET /seed?room=… endpoint inserts fixed rows so subsets have pre-existing
// data to load (demonstrating loadSubset fetching, not just live inserts).

import { Registry, SyncDurableObject } from "../../../src/server/index.ts"

interface Env {
  ITEMS_DO: DurableObjectNamespace
  ASSETS: { fetch: (req: Request) => Promise<Response> }
}
interface Claims {
  userId: string
}

const SEED: ReadonlyArray<readonly [string, string, string]> = [
  ["a1", "A", "Apple one"],
  ["a2", "A", "Apple two"],
  ["b1", "B", "Banana one"],
  ["c1", "C", "Cherry one"],
]

export class ItemsDO extends SyncDurableObject<Env, Claims> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      this.sql.exec(`CREATE TABLE IF NOT EXISTS items (
        id         TEXT PRIMARY KEY,
        category   TEXT NOT NULL,
        text       TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )`)
      this.registerSync(
        new Registry<Claims>()
          .defineCollection({ table: "items", pk: "id" })
          .defineMutation({
            collection: "items",
            type: "insert",
            execute: ({ op, sql }) => {
              const c = op.cols as { id: string; category: string; text: string; created_at: number }
              sql.exec(
                "INSERT INTO items(id, category, text, created_at) VALUES (?, ?, ?, ?)",
                c.id,
                c.category,
                c.text,
                c.created_at,
              )
            },
          }),
      )
    })
  }

  protected override parseAttachment(req: Request): Claims {
    return { userId: new URL(req.url).searchParams.get("user") ?? "anon" }
  }

  override async fetch(req: Request): Promise<Response> {
    if (new URL(req.url).pathname.endsWith("/seed")) {
      const now = Date.now()
      // runSyncedWrite applies + broadcasts (ADR-0006); tables/triggers already
      // exist from the constructor (ADR-0007).
      this.runSyncedWrite((sql) => {
        for (const [id, category, text] of SEED) {
          sql.exec("INSERT OR IGNORE INTO items(id, category, text, created_at) VALUES (?, ?, ?, ?)", id, category, text, now)
        }
      })
      return new Response("seeded")
    }
    return super.fetch(req)
  }
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url)
    if (url.pathname === "/sync" || url.pathname === "/seed") {
      const room = url.searchParams.get("room") ?? "demo"
      return env.ITEMS_DO.get(env.ITEMS_DO.idFromName(room)).fetch(req)
    }
    return env.ASSETS.fetch(req)
  },
} satisfies ExportedHandler<Env>
