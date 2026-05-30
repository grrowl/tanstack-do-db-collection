// Stress example — a high-volume "task board" on one Durable Object.
//
// The point: one scope (board) holds THOUSANDS of tasks, but a client loads only
// a bounded window (top N by `updated_at`) and pages older ones in on demand.
// Tasks are ordered by a MUTABLE key — voting/editing sets `updated_at = now`,
// which BUMPS a task to the top. That exercises move-in/move-out: a bump to a
// task a client never loaded arrives as an `update` delta for an absent key and
// upserts into the window (ADR-0002 C4's always-emit rule).
//
// Endpoints on the DO (routed by ?room): /sync (WebSocket), /seed, /bump, /count.
// `/bump` is a server-side load generator — it mutates random tasks (likely cold
// ones the caller never loaded) and broadcasts, so the OTHER tab sees move-in.

import { Registry, SyncDurableObject } from "../../../src/server/index.ts"

interface Env {
  BOARD_DO: DurableObjectNamespace
  ASSETS: { fetch: (req: Request) => Promise<Response> }
}
interface Claims {
  userId: string
}

const UPDATABLE = new Set(["title", "status", "votes", "updated_at"])

export class BoardDO extends SyncDurableObject<Env, Claims> {
  protected registry = new Registry<Claims>()
    .defineCollection({
      table: "tasks",
      pk: "id",
      ddl: `CREATE TABLE IF NOT EXISTS tasks (
              id         TEXT PRIMARY KEY,
              title      TEXT NOT NULL,
              status     TEXT NOT NULL,
              votes      INTEGER NOT NULL,
              updated_at INTEGER NOT NULL
            )`,
    })
    .defineMutation({
      collection: "tasks",
      type: "insert",
      execute: ({ op, sql }) => {
        const c = op.cols as { id: string; title: string; status: string; votes: number; updated_at: number }
        sql.exec(
          "INSERT INTO tasks(id, title, status, votes, updated_at) VALUES (?, ?, ?, ?, ?)",
          c.id,
          c.title,
          c.status,
          c.votes,
          c.updated_at,
        )
      },
    })
    .defineMutation({
      collection: "tasks",
      type: "update",
      // A vote/edit sends a getChanges() diff — any of title/status/votes plus
      // the bumped updated_at. Build the SET from the whitelisted keys present.
      execute: ({ op, sql }) => {
        const cols = op.cols as Record<string, unknown>
        const keys = Object.keys(cols).filter((k) => UPDATABLE.has(k))
        if (keys.length === 0) return
        const set = keys.map((k) => `"${k}" = ?`).join(", ")
        sql.exec(`UPDATE tasks SET ${set} WHERE id = ?`, ...keys.map((k) => cols[k]), op.key as string)
      },
    })
    .defineMutation({
      collection: "tasks",
      type: "delete",
      execute: ({ op, sql }) => {
        sql.exec("DELETE FROM tasks WHERE id = ?", op.key as string)
      },
    })

  protected override parseAttachment(req: Request): Claims {
    return { userId: new URL(req.url).searchParams.get("user") ?? "anon" }
  }

  override async fetch(req: Request): Promise<Response> {
    const url = new URL(req.url)
    const path = url.pathname

    if (path.endsWith("/seed")) {
      this.initRegistry()
      const n = Math.max(0, Math.min(50_000, Number(url.searchParams.get("n") ?? "5000")))
      // Seed with TIE-BURSTS: every 25 tasks share an `updated_at`, so scrolling
      // back crosses real tie boundaries (exercising the cursor `whereCurrent`).
      // Seeds use a year-2001 base so any later /bump (Date.now()) sorts above
      // them — a bumped task visibly rockets to the top.
      const base = 1_000_000_000_000
      this.ctx.storage.transactionSync(() => {
        for (let i = 0; i < n; i++) {
          const updated_at = base + Math.floor(i / 25) * 1000
          this.sql.exec(
            "INSERT OR IGNORE INTO tasks(id, title, status, votes, updated_at) VALUES (?, ?, ?, ?, ?)",
            `t${String(i).padStart(5, "0")}`,
            `Task #${i}`,
            ["open", "doing", "done"][i % 3],
            i % 7,
            updated_at,
          )
        }
      })
      // Advance the drain cursor PAST the seed so these inserts aren't later
      // re-broadcast as deltas. Seeding happens before any client connects (see
      // boot()), so there are no subscribers — this just consumes the backlog;
      // the bounded initial snapshot already covers the seeded rows via SELECT.
      this.drainAndBroadcast()
      return new Response(`seeded ${n}`)
    }

    if (path.endsWith("/bump")) {
      this.initRegistry()
      const n = Math.max(1, Math.min(100, Number(url.searchParams.get("n") ?? "5")))
      const now = Date.now()
      this.ctx.storage.transactionSync(() => {
        const ids = Array.from(this.sql.exec("SELECT id FROM tasks ORDER BY RANDOM() LIMIT ?", n)).map(
          (r) => r.id as string,
        )
        for (const id of ids) this.sql.exec("UPDATE tasks SET votes = votes + 1, updated_at = ? WHERE id = ?", now, id)
      })
      // Raw SQL writes fired the CDC triggers but not the broadcast path — flush
      // the buffered deltas to every subscriber so move-in is observed live.
      this.drainAndBroadcast()
      return new Response("bumped")
    }

    if (path.endsWith("/count")) {
      this.initRegistry()
      const c = Array.from(this.sql.exec("SELECT count(*) AS c FROM tasks"))[0]!.c as number
      return Response.json({ count: c })
    }

    return super.fetch(req)
  }
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url)
    if (["/sync", "/seed", "/bump", "/count"].includes(url.pathname)) {
      const room = url.searchParams.get("room") ?? "demo"
      return env.BOARD_DO.get(env.BOARD_DO.idFromName(room)).fetch(req)
    }
    return env.ASSETS.fetch(req)
  },
} satisfies ExportedHandler<Env>
