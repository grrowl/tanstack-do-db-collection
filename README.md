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
import { SyncRegistry, SyncDurableObject } from "tanstack-do-db-collection"

interface Claims { userId: string }
interface Message { id: string; author: string; content: string; created_at: number }

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

      this.registerSync(
        // The third generic is the collection manifest: table → row type. It
        // types `pk` (must be a column) and every handler's `op` — no casts.
        new SyncRegistry<Claims, Env, { messages: Message }>()
          .defineCollection({ table: "messages", pk: "id" })
          .defineMutation({
            collection: "messages",
            type: "insert",
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
          }),
      )
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

const transport = new WebSocketTransport({ url: `wss://${host}/sync/${sessionId}` })
const messages = createCollection(
  doCollectionOptions<Message>({ transport, table: "messages", getKey: (m) => m.id }),
)

function ChatRoom({ userId }: { userId: string }) {
  const { data } = useLiveQuery((q) => q.from({ m: messages }).orderBy(({ m }) => m.created_at, "asc"))
  const send = (content: string) =>
    // Optimistic; resolves once the server confirms on the single stream.
    messages.insert({ id: ulid(), author: userId, content, created_at: Date.now() })
  return <ChatView rows={data} onSend={send} />
}
```

One `WebSocketTransport` per DO is shared by every collection on that DO
(multiplexed over the single socket). Pass `where` to
`doCollectionOptions` to sync only a matching subset.

### 4. SSR (experimental)

Tracks TanStack DB's draft [`DbClient` SSR PR](https://github.com/TanStack/db/pull/1564);
the upstream hooks may change before release. Why/how trade-offs live in
[ADR-0011](./docs/adr/0011-ssr-dehydrate-hydrate.md).

On the worker, render through a **per-request** `DbClient` backed by one
snapshot read per subscription — no WebSocket from the render path:

```ts
// Route loader / server handler (per request!)
import { DbClient, collectionOptions } from "@tanstack/db"
import { doCollectionOptions, SsrSnapshotTransport } from "tanstack-do-db-collection/client"

const stub = env.CHAT_DO.get(env.CHAT_DO.idFromName(sessionId))
const transport = new SsrSnapshotTransport({ read: (req) => stub.readSnapshot(req) })
const db = new DbClient()
const messages = db.collection(
  collectionOptions(doCollectionOptions<Message>({ transport, table: "messages", getKey: (m) => m.id })),
)
await messages.preload()
return { dbState: db.dehydrate() } // rows + our cursor (opaque syncMeta)
```

In the browser, hydrate before going live. The collection is ready
immediately with the dehydrated rows (stale-while-revalidate); the first sub
resumes from the dehydrated cursor, so the catch-up applies exactly what
changed while the HTML was in flight — updates *and* deletes:

```ts
const db = new DbClient()
db.hydrate(dbState)
const messages = db.collection(
  collectionOptions(doCollectionOptions<Message>({ transport: wsTransport, table: "messages", getKey: (m) => m.id })),
)
```

Mutations during SSR throw (`SsrReadOnlyError`); `readSnapshot` is callable by
any worker holding the DO binding — the same trust boundary as the upgrade's
forged-claims header, so end users never reach it.

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

> [!TIP]
> Using on-demand with `orderBy` + `limit`? Add a **range index** on the order
> column (`collection.createIndex((r) => r.field, { indexType: BTreeIndex })`) —
> without it the window can't page lazily and falls back to loading the whole
> subset. See `examples/board`.

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
