// InboxDO sync surface — a user's notifications.
//
// A SECOND, independent DO with its OWN schema/Api. Note the command name
// `markAllRead` could perfectly well coexist with RoomDO's `clearRoom` — they
// live on different transports, so even identically-named commands on the two
// DOs never collide (the client keys them by DO; see client.tsx / README).

import { defineSync } from "../../../src/server/index.ts"
import type { Claims, Env } from "./env.ts"

export interface Notification {
  id: string
  user: string
  kind: string
  body: string
  /** SQLite has no boolean: 0 = unread, 1 = read. */
  read: number
  created_at: number
}

const sync = defineSync<Claims, Env>()

export const inboxSchema = sync.schema({
  collections: {
    notifications: sync.collection<Notification>({
      pk: "id",
      mutations: {
        insert: {
          execute: ({ op, sql }) => {
            const c = op.cols
            sql.exec(
              "INSERT INTO notifications(id, user, kind, body, read, created_at) VALUES (?, ?, ?, ?, ?, ?)",
              c.id,
              c.user,
              c.kind,
              c.body,
              c.read,
              c.created_at,
            )
          },
        },
        // Marking one notification read IS a typed single-row write — a partial
        // `update` patch ({ read: 1 }) — so it's a mutation, not a command.
        update: {
          execute: ({ op, sql }) => {
            if (op.cols.read !== undefined) {
              sql.exec("UPDATE notifications SET read = ? WHERE id = ?", op.cols.read, op.key)
            }
          },
        },
      },
    }),
  },
  commands: {
    // Bulk "mark everything read" returns a count — a command (returns a value,
    // touches many rows). Its UPDATE fans out as update deltas to every tab.
    markAllRead: sync.command()(({ sql }) => {
      const unread = Array.from(sql.exec("SELECT count(*) AS c FROM notifications WHERE read = 0"))[0]!.c as number
      sql.exec("UPDATE notifications SET read = 1 WHERE read = 0")
      return { marked: unread }
    }),
  },
})

export type InboxApi = typeof inboxSchema
