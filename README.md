# tanstack-do-db-collection

> Sync a [TanStack DB](https://tanstack.com/db) collection to a
> [Cloudflare Durable Object](https://developers.cloudflare.com/durable-objects/)
> over WebSockets — optimistic mutations, live queries, and reconnect
> catch-up, with a **single ordered stream** carrying both data and write
> confirmation.

The Durable Object owns the data, the browser runs a TanStack DB collection
against it, and this library moves the diffs — nothing more — over a single
ordered stream.

It's a deliberately plain topology, each part doing what it does best. One
authoritative writer keeps the change log totally ordered and contiguous, so a
single cursor drives live deltas, reconnect catch-up, and write confirmation
alike — no second ack channel, no CRDT to merge, no Postgres to mirror. The
Durable Object holds authoritative state and assigns order; TanStack DB gives
the client its reactive layer (live queries, IVM, optimistic rollback); this
library carries the diffs between. You stop trading one good thing for another
— optimistic CRUD *and* a single source of truth, a simple transport *and* a
fully reactive client, at once.

---

## Why this exists

If you reach for sync on Cloudflare today, the good options each ask you to
give something up. CRDT engines — Cloudflare's own
[PartyKit](https://github.com/cloudflare/partykit) — are superb for
collaborative editing, but they're [Yjs](https://github.com/yjs/yjs)-shaped
(merge semantics, document baggage) with a thin authorization story. We were
reaching for live-CRUD engines like [Zero](https://zero.rocicorp.dev/) and
[LiveStore](https://dev.docs.livestore.dev/) — excellent for traditional web
apps — but the technical requirements get steep for globally-distributed apps:
a separate store to mirror into and operate alongside every DO. So this library
takes the third path — the DO is the source of truth, and the entire
client-side reactive layer (live queries, incremental view maintenance,
optimistic rollback) comes from TanStack DB for free.

| | This library |
|---|---|
| **Source of truth** | The Durable Object's own SQLite. No Postgres, no external sync service. |
| **Transport** | One WebSocket per DO. Hibernation-native. |
| **Writes** | Bidirectional. Optimistic on the client; authoritative in the DO. |
| **Confirmation** | A position in the one stream the client already tails — no second ack channel. |
| **Reads** | Live queries via TanStack DB's client-side IVM. The DO never joins or aggregates. |
| **Consistency** | Server-authoritative. Single writer per DO. No CRDTs, no multi-DO transactions. |

---

## The model in 30 seconds

- **One DO instance = one sync scope** (a session, a workspace, a document —
  whatever you shard by). It owns a SQLite database with one or more
  collections.
- **Change-data-capture via triggers.** Every write to a collection table
  fires a trigger appending to a single per-DO change log. That log is the
  one ordered stream — the source of truth for live deltas, reconnect
  catch-up, *and* write confirmation.
- **One cursor.** The client tracks a single position (`appliedSeq`). A
  write is confirmed when that position passes the sequence the DO assigned
  the write — exactly Electric's `awaitTxId`, reduced to a `>=` comparison
  because a single writer produces a contiguous log.
- **Client-supplied keys.** The client mints the primary key (ULID / UUIDv7),
  so the optimistic row and the confirmed row are *the same row* — the write
  applies locally under the chosen id and the server confirms that same id, with
  no key reconciliation or id swap on commit.
- **Bounded retention.** The change log stays light: compaction keeps only the
  latest op per key, and changes are swept after a retention window (2 days by
  default, configurable). A client reconnecting from beyond that window gets a
  fresh snapshot instead of a delta. Bounded storage, no event-log explosion.

See [ADR-0001](./docs/adr/0001-sync-architecture.md) for the full rationale,
and [the build plan](./docs/adr/0001-sync-architecture.md#build-sequence) for
the milestone sequence.

---

## Quick start

### 1. Define your Durable Object

```ts
import { defineSync, SyncDurableObject } from "tanstack-do-db-collection"

interface Claims { userId: string }
interface Env { /* your bindings */ }
interface Message { id: string; author: string; content: string; created_at: number }

// defineSync binds identity (Claims) and binding-env (Env) once and returns
// three co-located helpers. They flow `user`/`env` into every handler ctx.
const sync = defineSync<Claims, Env>()

// The schema VALUE is both the DO registration and the client contract. The
// collection KEY ("messages") is the DB table name. `pk` must be a real column
// of Row — the sole TEXT, client-supplied key (ADR-0007).
export const chatSchema = sync.schema({
  collections: {
    messages: sync.collection<Message>({
      pk: "id",
      // The closed mutation trio { insert?; update?; delete? } — a 4th key is a
      // type error. op.cols is typed per op: full Row on insert, Partial on
      // update, absent on delete.
      mutations: {
        insert: {
          // authorize runs BEFORE the tx (async ok); throw to deny.
          // op.cols is typed Message here — no cast.
          authorize: ({ user, op }) => {
            if (op.cols.author !== user.userId) {
              throw new Error("author mismatch")
            }
          },
          // execute runs INSIDE transactionSync — synchronous only.
          execute: ({ op, sql }) => {
            const m = op.cols // Message
            sql.exec(
              "INSERT INTO messages(id, author, content, created_at) VALUES (?, ?, ?, ?)",
              m.id, m.author, m.content, m.created_at,
            )
          },
          // afterCommit (optional): fire-and-forget AFTER the commit + receipt —
          // the home for external side effects execute can't do (delete an R2
          // object, enqueue a job). Receives `env`; owns its own idempotency.
          // afterCommit: async ({ op, env }) => { await env.BUCKET.delete(op.key) },
        },
        delete: {
          execute: ({ op, sql }) => {
            // delete carries op.key only — no op.cols.
            sql.exec("DELETE FROM messages WHERE id = ?", op.key)
          },
        },
      },
    }),
  },
  // Commands are the escape hatch for writes that aren't a single typed row op.
  // Their own SQL still flows through the CDC triggers, and they can return a
  // result. Type-only Args is curried (call the factory twice); Result is
  // inferred from the return. If you have commands you MUST declare them inline
  // here so Args/Result inference flows into the Api type.
  commands: {
    clearRoom: sync.command()(({ sql }) => {
      const before = Array.from(sql.exec("SELECT count(*) AS c FROM messages"))[0]!.c as number
      sql.exec("DELETE FROM messages")
      return { deleted: before }
    }),
  },
})

// Export the schema type as the client contract.
export type Api = typeof chatSchema

export class SessionDO extends SyncDurableObject<Env, Claims> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env)

    // You own your schema — migrate with anything (raw DDL, Drizzle, …), then
    // call registerSync to wire CDC. blockConcurrencyWhile runs it before the
    // first request.
    ctx.blockConcurrencyWhile(async () => {
      this.sql.exec(`CREATE TABLE IF NOT EXISTS messages (
        id         TEXT PRIMARY KEY,        -- client-supplied TEXT key (ULID/UUIDv7)
        author     TEXT NOT NULL,
        content    TEXT NOT NULL,
        created_at INTEGER NOT NULL
      )`)

      // registerSync takes the schema VALUE — it compiles it, validates pk
      // affinity, and wires the CDC triggers.
      this.registerSync(chatSchema)
    })
  }

  // Read the Worker-forged claims header into the per-socket attachment.
  protected parseAttachment(req: Request): Claims {
    return JSON.parse(req.headers.get("x-claims") ?? "{}") as Claims
  }
}
```

> [!IMPORTANT]
> **Schema & migrations.** You own the table — create it with anything (raw
> `CREATE TABLE`, Drizzle, a versioned migrator), then call `registerSync` to
> wire CDC. The pk must have **TEXT affinity** (`TEXT`, `VARCHAR`, `CHAR`, …) so
> it stores the client-supplied id verbatim; an `INTEGER` key is rejected — it
> aliases rowid (server-assigned) and breaks optimistic id parity. Evolve
> freely: the CDC triggers capture only the row key, so `ALTER TABLE ADD COLUMN`
> flows to clients with no re-wiring, and re-running `registerSync` on the next
> deploy is idempotent (ADR-0007).

> [!NOTE]
> Server-side writes outside the client flow — an agent inserting a row, a
> webhook, a cron job, a bulk seed — go through `this.runSyncedWrite(sql => …)`:
> it applies your write and broadcasts it to connected clients (ADR-0006).

### 2. Route the upgrade from your Worker (the trust boundary)

This Worker fronts every `/sync/<sessionId>` WebSocket upgrade: match the path,
authenticate, then forge the claims header and hand off to the right DO.

```ts
export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    // Only handle /sync/<sessionId> — the sessionId is the DO shard key.
    const match = new URL(req.url).pathname.match(/^\/sync\/(.+)$/)
    if (!match) return new Response("not found", { status: 404 })
    const sessionId = match[1]

    // The trust boundary: authenticate here, then stamp claims the DO can trust.
    const claims = await verifyToken(req) // your auth
    if (!claims) return new Response("unauthorized", { status: 401 })
    const headers = new Headers(req.headers)
    headers.set("x-claims", JSON.stringify(claims)) // .set() overwrites any client-injected value

    const id = env.SESSION_DO.idFromName(sessionId)
    return env.SESSION_DO.get(id).fetch(new Request(req, { headers }))
  },
} satisfies ExportedHandler<Env>
```

### 3. Use it from the browser

```ts
import { createCollection } from "@tanstack/db"
import { useLiveQuery } from "@tanstack/react-db"
import { doCollectionOptions, WebSocketTransport } from "tanstack-do-db-collection/client"
import { ulid } from "ulid"
import type { Api } from "./session-do" // TYPE-ONLY — nothing server-side is bundled

const transport = new WebSocketTransport<Api>({ url: `wss://${host}/sync/${sessionId}` })

// Api-driven: the row type is inferred from the schema Api + table name, so
// there's no runtime schema value and no explicit Row generic. `m` is Message.
const messages = createCollection(
  doCollectionOptions<Api, "messages">({ transport, table: "messages", getKey: (m) => m.id }),
)

function ChatRoom({ userId }: { userId: string }) {
  const { data } = useLiveQuery((q) => q.from({ m: messages }).orderBy(({ m }) => m.created_at, "asc"))
  const send = (content: string) =>
    // Optimistic; resolves once the server confirms on the single stream.
    messages.insert({ id: ulid(), author: userId, content, created_at: Date.now() })
  // Commands run over the transport (not the collection). `call` is a typed
  // Proxy — the name autocompletes and the result is checked against the Api.
  const clear = () => transport.call.clearRoom() // Promise<{ deleted: number }>
  return <ChatView rows={data} onSend={send} onClear={clear} />
}
```

One `WebSocketTransport` per DO is shared by every collection on that DO
(multiplexed over the single socket). Pass `where` to
`doCollectionOptions` to sync only a matching subset. Commands go through the
same socket: `transport.call.<name>(args)` (typed sugar) or the low-level
`transport.sendCall("clearRoom", undefined)` — both mint the txId for you and
resolve with the command's result on `committed`.

---

## Examples

Each is a runnable Worker + browser client (`npm install && npm run dev`),
browser-verified.

- **[`examples/chat`](./examples/chat)** — eager sync of a room's messages;
  multi-tab live updates. The smallest end-to-end shape.
- **[`examples/on-demand`](./examples/on-demand)** — `syncMode: 'on-demand'`:
  categorised items where each panel loads only its subset (`loadSubset`/
  `unloadSubset`) and unopened categories are never synced.
- **[`examples/board`](./examples/board)** — the at-scale stress test: 5,000
  tasks on one DO with a bounded window, `useLiveInfiniteQuery` cursor
  scroll-back, and a mutable order key so voting bumps a task to the top
  (move-in). Its firehose makes the deferred bounded-window-under-churn
  limitation visible — `loaded` climbs past `window`.
- **[`examples/multi-do`](./examples/multi-do)** — two separate DOs (a room and
  an inbox) behind one Worker: one transport per DO, each typed by its own `Api`
  so `transport.call.*` is scoped to that DO's commands, and a cross-DO feed
  merged client-side (the DO never joins — ADR-0001).

> [!TIP]
> Using on-demand with `orderBy` + `limit`? Add a **range index** on the order
> column (`collection.createIndex((r) => r.field, { indexType: BTreeIndex })`) —
> without it the window can't page lazily and falls back to loading the whole
> subset. See `examples/board`.

---

## Common patterns and recipes

Task-oriented guides in [`recipes/`](./recipes):

- **[Commands vs mutations](./recipes/commands-vs-mutations.md)** — when a write
  is a typed `insert`/`update`/`delete` and when it's a named command.
- **[End-to-end types](./recipes/end-to-end-types.md)** — share one schema type
  between server and client so the transport, commands, and collections are typed.
- **[On-demand and windows](./recipes/on-demand-and-windows.md)** — sync only the
  rows a query asks for, and grow a bounded window as the user scrolls.
- **[Server-originated writes](./recipes/server-originated-writes.md)** — write
  rows from the DO itself (webhooks, jobs, seeds) so clients still see them.

---

## Non-goals

- **Multi-DO transactions.** A transaction touches collections in one DO.
- **Server-assigned primary keys.** Optimism requires id parity.
- **Per-row read authorization.** Reads are gated at the WebSocket upgrade
  (per DO). Shard into more DOs for finer read isolation.
- **Server-side joins / IVM.** The DO stores and emits; the client composes.
- **An event log.** The change log is a *state-convergence* log, compacted to
  latest-op-per-key. It is not an audit trail.

---

## Acknowledgements

The design is indebted to, and learns directly from, the open-source work of
[ElectricSQL](https://github.com/electric-sql/electric) and
[TanStack DB](https://github.com/TanStack/db). This library is offered back to
that community under the MIT license.

## License

[MIT](./LICENSE) © Tom McKenzie
