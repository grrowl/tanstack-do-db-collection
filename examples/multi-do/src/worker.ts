// Multi-DO example — Worker + TWO Durable Objects (the Cloudflare
// microservices story). RoomDO owns a chat room's messages; InboxDO owns a
// user's notifications. They are entirely separate DOs with separate storage,
// schemas, and sync streams; the client opens ONE transport PER DO.
//
// Imports the library straight from source (../../../src) so the example tracks
// the real code with no build step for the lib. A published consumer would
// instead `import { ... } from "tanstack-do-db-collection"`.

import { SyncDurableObject } from "../../../src/server/index.ts"
import type { Claims, Env } from "./env.ts"
import { inboxSchema } from "./inbox-schema.ts"
import { roomSchema } from "./room-schema.ts"

export class RoomDO extends SyncDurableObject<Env, Claims> {
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
      this.registerSync(roomSchema)
    })
  }

  // The example trusts a `user` query param for identity. A real app verifies a
  // token at the Worker and forges a claims header (see the README).
  protected override parseAttachment(req: Request): Claims {
    return { userId: new URL(req.url).searchParams.get("user") ?? "anon" }
  }
}

export class InboxDO extends SyncDurableObject<Env, Claims> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)
    ctx.blockConcurrencyWhile(async () => {
      this.sql.exec(`CREATE TABLE IF NOT EXISTS notifications (
        id         TEXT PRIMARY KEY,
        user       TEXT NOT NULL,
        kind       TEXT NOT NULL,
        body       TEXT NOT NULL,
        read       INTEGER NOT NULL,
        created_at INTEGER NOT NULL
      )`)
      this.registerSync(inboxSchema)
    })
  }

  // Like RoomDO, the example trusts a `user` query param for identity. NOTE the
  // path (`/inbox/:user`) names the DO while `?user=` names the caller — the
  // example does not bind them, so `/inbox/alice/sync?user=bob` would let bob
  // into alice's inbox. A real app verifies a token at the Worker and either
  // rejects a path/caller mismatch there or authorizes each inbox write against
  // the DO owner; the per-user boundary here is illustrative, not enforced.
  protected override parseAttachment(req: Request): Claims {
    return { userId: new URL(req.url).searchParams.get("user") ?? "anon" }
  }
}

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const url = new URL(req.url)

    // /rooms/:room/sync → the RoomDO instance named by :room. Each room is its
    // own DO (idFromName), so two rooms never share a stream.
    const room = url.pathname.match(/^\/rooms\/([^/]+)\/sync$/)
    if (room) {
      const name = decodeURIComponent(room[1]!) // capture group is mandatory when the match succeeds
      return env.ROOM_DO.get(env.ROOM_DO.idFromName(name)).fetch(req)
    }

    // /inbox/:user/sync → the InboxDO instance named by :user — a per-user DO.
    const inbox = url.pathname.match(/^\/inbox\/([^/]+)\/sync$/)
    if (inbox) {
      const name = decodeURIComponent(inbox[1]!) // capture group is mandatory when the match succeeds
      return env.INBOX_DO.get(env.INBOX_DO.idFromName(name)).fetch(req)
    }

    return env.ASSETS.fetch(req) // index.html + client.js
  },
} satisfies ExportedHandler<Env>
