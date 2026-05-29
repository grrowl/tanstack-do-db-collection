// Test worker entry — declares the DO classes bound by vitest.config's
// miniflare.durableObjects, and routes WebSocket upgrades to the sync DO.

import { DurableObject } from "cloudflare:workers"
import { Registry } from "../src/server/registry.ts"
import { SyncDurableObject } from "../src/server/sync-do.ts"
import type { ClientFrame } from "../src/wire/frames.ts"

/** Bare DO for the M1 CDC tests; they drive storage via runInDurableObject. */
export class TestDO extends DurableObject {}

interface Claims {
  userId: string
}

/** Exercises the M2 WebSocket lifecycle. Echoes a `call` named "echo" back as a
 *  `committed` frame so tests can assert the full wire round-trip through the
 *  real DO. Real sub/mut/call dispatch lands in M3. */
export class SyncTestDO extends SyncDurableObject<unknown, Claims> {
  protected registry = new Registry().defineCollection({
    table: "messages",
    pk: "id",
    ddl: `CREATE TABLE IF NOT EXISTS messages (id TEXT PRIMARY KEY, body TEXT)`,
  })

  protected override parseAttachment(req: Request): Claims {
    return { userId: req.headers.get("x-user") ?? "anon" }
  }

  protected override onFrame(ws: WebSocket, frame: ClientFrame): void {
    if (frame.t === "call" && frame.name === "echo") {
      this.send(ws, { t: "committed", txId: frame.txId, seq: "0", result: frame.args })
    }
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
