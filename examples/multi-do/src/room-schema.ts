// RoomDO sync surface — a chat room's messages.
//
// `defineSync<Claims, Env>()` binds identity + env once; the schema VALUE it
// produces is registered by the DO (`registerSync`) AND imported *as a type
// only* by the client to drive `transport.call.*` and `doCollectionOptions`.
// Export the type as `RoomApi` — that's this DO's whole client contract.

import { defineSync } from "../../../src/server/index.ts"
import type { Claims, Env } from "./env.ts"

export interface Message {
  id: string
  author: string
  content: string
  created_at: number
}

const sync = defineSync<Claims, Env>()

export const roomSchema = sync.schema({
  collections: {
    // KEY "messages" === the DB table name (interpolated into trigger DDL).
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
    // A COMMAND, not a mutation: "clear the room" isn't a typed single-row write.
    // Its own DELETE still flows through the CDC triggers, so it fans out to
    // every connected tab as delete deltas, and the caller gets the count back.
    clearRoom: sync.command()(({ sql }) => {
      const before = Array.from(sql.exec("SELECT count(*) AS c FROM messages"))[0]!.c as number
      sql.exec("DELETE FROM messages")
      return { deleted: before }
    }),
  },
})

export type RoomApi = typeof roomSchema
