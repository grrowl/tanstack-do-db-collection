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
export class SyncTestDO extends SyncDurableObject<unknown, Claims> {
  protected registry = new Registry().defineCollection({
    table: "messages",
    pk: "id",
    ddl: `CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`,
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
