// Type-level tests for ADR-0010 (typed mutations via the collection manifest).
// NOT a runtime test — the filename is intentionally not `*.test.ts`, so vitest
// ignores it while `tsc -p tsconfig.json` type-checks it. `@ts-expect-error`
// marks lines that MUST NOT compile; an unmarked line that fails to compile is
// a red type-test. This pins the author-facing typing the same way a runtime
// test pins behaviour.

import { SyncRegistry } from "../src/server/registry.ts"

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

// The manifest types both call sites.
const r = new SyncRegistry<Claims, Env, { messages: Message; files: FileRow }>()

// defineCollection: table ∈ manifest, pk ∈ keyof Row.
r.defineCollection({ table: "messages", pk: "id" })
r.defineCollection({ table: "files", pk: "id" })
// @ts-expect-error pk must be an actual column of Message
r.defineCollection({ table: "messages", pk: "nope" })
// @ts-expect-error table must be a declared collection
r.defineCollection({ table: "ghosts", pk: "id" })

// insert → cols is the full row.
r.defineMutation({
  collection: "messages",
  type: "insert",
  execute: ({ op }) => {
    const a: string = op.cols.author
    void a
  },
})

// update → cols is Partial<Row>.
r.defineMutation({
  collection: "messages",
  type: "update",
  execute: ({ op }) => {
    const a: string | undefined = op.cols.author
    void a
  },
})

// delete → no cols, just key.
r.defineMutation({
  collection: "messages",
  type: "delete",
  // @ts-expect-error delete carries no cols
  execute: ({ op }) => void op.cols.author,
})

// authorize + afterCommit get the same typed op.
r.defineMutation({
  collection: "files",
  type: "insert",
  authorize: ({ op }) => {
    const n: string = op.cols.name
    void n
  },
  execute: ({ op }) => op.cols.name.toUpperCase(),
  afterCommit: ({ op }) => op.key, // key: string
})

// @ts-expect-error unknown collection name is rejected
r.defineMutation({ collection: "ghosts", type: "insert", execute: () => {} })

// Untyped fallback: a 2-arg SyncRegistry still compiles; cols is `unknown` (cast, as before).
const untyped = new SyncRegistry<Claims>()
untyped.defineCollection({ table: "anything", pk: "whatever" })
untyped.defineMutation({
  collection: "anything",
  type: "insert",
  execute: ({ op }) => {
    const m = op.cols as Message
    void m.author
  },
})
