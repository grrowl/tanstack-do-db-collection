// Test worker entry — declares the DO classes bound by vitest.config's
// miniflare.durableObjects, and routes WebSocket upgrades to the sync DO.

import { DurableObject } from "cloudflare:workers"
import { SyncRegistry } from "../src/server/registry.ts"
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
  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      // The author owns table creation; the framework wires sync after (ADR-0007).
      this.sql.exec(`CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`)
      this.sql.exec(`CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, name TEXT)`)
      this.registerSync(
        new SyncRegistry<Claims>()
          .defineCollection({ table: "messages", pk: "id" })
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
          // A second collection on the same DO — exercises multiplexing over one WS.
          .defineCollection({ table: "files", pk: "id" })
          .defineMutation({
            collection: "files",
            type: "insert",
            execute: ({ op, sql }) => {
              const c = op.cols as unknown as { id: string; name: string }
              sql.exec("INSERT INTO files(id, name) VALUES (?, ?)", c.id, c.name)
            },
          })
          // `files:delete` exercises afterCommit: the synchronous execute is the
          // durable write; afterCommit is fire-and-forget async post-work (here it
          // records a marker proving both `sql` and `env` reached the hook).
          .defineMutation({
            collection: "files",
            type: "delete",
            execute: ({ op, sql }) => {
              sql.exec("DELETE FROM files WHERE id = ?", op.key as string)
            },
            afterCommit: async ({ op, sql, env }) => {
              // A genuine async hop, to prove afterCommit awaits and runs off the
              // request path. `_afterlog` is a plain side table (no CDC triggers).
              await Promise.resolve()
              if ((op.key as string) === "boom") throw new Error("afterCommit boom")
              sql.exec("CREATE TABLE IF NOT EXISTS _afterlog (key TEXT PRIMARY KEY, tag TEXT)")
              const hasEnv = (env as { SYNC_DO?: unknown }).SYNC_DO ? "has-env" : "no-env"
              sql.exec("INSERT OR REPLACE INTO _afterlog(key, tag) VALUES (?, ?)", op.key as string, hasEnv)
            },
          })
          .defineCommand({
            name: "echo",
            execute: ({ args }) => ({ echoed: args }),
          })
          .defineCommand({
            name: "boom",
            execute: () => {
              throw new Error("command boom")
            },
          }),
      )
    })
  }

  protected override parseAttachment(req: Request): Claims {
    const userId = req.headers.get("x-user") ?? "anon"
    // Sentinel for gate tests: parseAttachment is the ONE auth gate for both
    // the WS upgrade and the readSyncSnapshot RPC (ADR-0011 D1).
    if (userId === "forbidden") throw new Response("forbidden", { status: 403 })
    return { userId }
  }
}

/** A sync DO that never calls registerSync — proves accessing sync before
 *  registration fails loud (ADR-0007). */
export class UnregisteredDO extends SyncDurableObject<unknown, Claims> {}

/** Same collections as SyncTestDO, but with a tiny compaction threshold so the
 *  opportunistic `maybeCompact` housekeeping (compaction + retention prune +
 *  dedup sweep) fires after a few writes instead of 200 — lets tests exercise
 *  the real drain → maybeCompact → waitUntil path. Keeps the default 2-day
 *  `changelogRetentionMs` so the retention wiring is tested as shipped. */
export class MaintTestDO extends SyncTestDO {
  protected override readonly compactionEvery = 3
}

/** Same collections as SyncTestDO with an effectively-infinite coalescer tick:
 *  enqueued deltas stay buffered until something calls flushOne explicitly.
 *  Lets the cursor-barrier tests (ADR-0011 C1′) hold "pending egress" still
 *  while a snapshot/catch-up is served on the same socket. */
export class SlowTickDO extends SyncTestDO {
  protected override readonly tickMs = 30_000
}

interface Env {
  TEST_DO: DurableObjectNamespace
  SYNC_DO: DurableObjectNamespace
  MAINT_DO: DurableObjectNamespace
  SLOW_DO: DurableObjectNamespace
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url)
    if (url.pathname.startsWith("/sync/")) {
      const name = url.pathname.slice("/sync/".length) || "default"
      return env.SYNC_DO.get(env.SYNC_DO.idFromName(name)).fetch(req)
    }
    if (url.pathname.startsWith("/maint/")) {
      const name = url.pathname.slice("/maint/".length) || "default"
      return env.MAINT_DO.get(env.MAINT_DO.idFromName(name)).fetch(req)
    }
    if (url.pathname.startsWith("/slow/")) {
      const name = url.pathname.slice("/slow/".length) || "default"
      return env.SLOW_DO.get(env.SLOW_DO.idFromName(name)).fetch(req)
    }
    return new Response("test-worker")
  },
}
