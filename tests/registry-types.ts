// Type-level tests for ADR-0014 (the object-schema authoring API + typed
// mutations/commands). NOT a runtime test — the filename is intentionally not
// `*.test.ts`, so vitest ignores it while `tsc -p tsconfig.json` type-checks it.
// `@ts-expect-error` marks lines that MUST NOT compile; an unmarked line that
// fails to compile is a red type-test. This pins the author-facing typing the
// same way a runtime test pins behaviour.

import { defineSync } from "../src/server/registry.ts"

interface Claims {
  userId: string
}
interface Env {
  BUCKET: unknown
}
interface Message {
  id: string
  author: string
  content: string
  created_at: number
}
interface FileRow {
  id: string
  name: string
}

const sync = defineSync<Claims, Env>()

// collection: pk must be an actual column of Row.
sync.collection<Message>({ pk: "id" })
// @ts-expect-error pk must be an actual column of Message
sync.collection<Message>({ pk: "nope" })

// Per-op typing: insert → cols is the full row; update → Partial<Row>; delete →
// no cols, just key. authorize + afterCommit see the same typed op.
sync.collection<Message>({
  pk: "id",
  mutations: {
    insert: {
      authorize: ({ op }) => {
        const a: string = op.cols.author
        void a
      },
      execute: ({ op }) => {
        const a: string = op.cols.author
        void a
      },
      afterCommit: ({ op }) => op.key, // key: string
    },
    update: {
      execute: ({ op }) => {
        const a: string | undefined = op.cols.author
        void a
      },
    },
    delete: {
      // @ts-expect-error delete carries no cols
      execute: ({ op }) => void op.cols.author,
    },
  },
})

// The mutation trio is CLOSED: a fourth key is an excess-property error.
sync.collection<Message>({
  pk: "id",
  mutations: {
    // @ts-expect-error `archive` is not a member of the insert/update/delete trio
    archive: { execute: () => {} },
  },
})

// command: type-only Args is CURRIED (call twice) so Result infers from the
// return; `args` carries the declared Args.
const echo = sync.command<{ n: number }>()(({ args }) => ({ echoed: args.n }))
// @ts-expect-error args.missing is not a declared arg
sync.command<{ n: number }>()(({ args }) => args.missing)

// schema(): the collection KEY is the table name; `commands` is optional but
// carries Result/Args inference into the Api when present.
const schema = sync.schema({
  collections: {
    messages: sync.collection<Message>({ pk: "id" }),
    files: sync.collection<FileRow>({ pk: "id" }),
  },
  commands: { echo },
})
void schema
