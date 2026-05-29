# tanstack-do-db-collection

> Sync a [TanStack DB](https://tanstack.com/db) collection to a
> [Cloudflare Durable Object](https://developers.cloudflare.com/durable-objects/)
> over WebSockets — optimistic mutations, live queries, and reconnect
> catch-up, with a **single ordered stream** carrying both data and write
> confirmation.

> **Status: pre-1.0, under active construction.** The API will change until
> 1.0. Built in the open, milestone by milestone — see
> [`docs/adr/`](./docs/adr/) for the decisions and `git log` for the path.

The Durable Object owns the data. The browser runs a TanStack DB collection
against it. This library moves the diffs — and nothing more than the diffs.

It is, in spirit, [ElectricSQL](https://electric-sql.com)'s sync model ported
to a Durable Object: one ordered log, consumed from a single cursor. The
difference is that a DO is a **single authoritative writer**, so the log is
totally ordered and contiguous — which makes the model *simpler* here than it
is over Postgres, and lets the same library own the **write** path too.

---

## Why this exists

If you reach for sync on Cloudflare today you either (a) adopt a
Postgres-backed engine and give up DO sovereignty, or (b) hand-roll
per-table broadcast plumbing and reinvent the hard client-side reactive
layer. This library takes the third path: the DO is the source of truth, and
the entire client-side reactive layer (live queries, incremental view
maintenance, optimistic rollback) comes from TanStack DB for free.

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
- **Client-supplied keys.** Primary keys are client-chosen (ULID / UUIDv7).
  The optimistic row id must equal the confirmed row id.
- **Compaction-defined retention.** The change log compacts to latest-op-per-
  key beyond a horizon; clients reconnecting from before the horizon get a
  fresh snapshot. Bounded storage, no event-log explosion.

See [ADR-0001](./docs/adr/0001-sync-architecture.md) for the full rationale,
and [the build plan](./docs/adr/0001-sync-architecture.md#build-sequence) for
the milestone sequence.

---

## Quick start

> Lands incrementally as the milestones complete. The shape below is the
> target API.

```ts
// server: your Durable Object
import { Registry, SyncDurableObject } from "tanstack-do-db-collection"

export class SessionDO extends SyncDurableObject<Env> {
  protected registry = new Registry()
    .defineCollection({
      table: "messages",
      pk: "id",
      ddl: `CREATE TABLE IF NOT EXISTS messages (
              id TEXT PRIMARY KEY, author TEXT NOT NULL,
              content TEXT NOT NULL, created_at INTEGER NOT NULL)`,
    })
    .defineMutation({ collection: "messages", type: "insert", /* authorize, execute */ })
}
```

```ts
// client: a TanStack DB collection backed by the DO
import { createCollection } from "@tanstack/db"
import { doCollectionOptions, WebSocketTransport } from "tanstack-do-db-collection/client"

const transport = new WebSocketTransport(`wss://${host}/sync/${sessionId}`)
const messages = createCollection(doCollectionOptions({ table: "messages", transport }))
```

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
