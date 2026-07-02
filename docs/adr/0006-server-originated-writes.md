# 0006 — Server-originated writes: `runSyncedWrite`

**Status:** Accepted. Extends [ADR-0001](./0001-sync-architecture.md)'s write/sync
model with a third write origin. The "caller ensures init" caveat below is
retired by [ADR-0007](./0007-author-owned-schema-register-sync.md) (schema +
triggers now exist at construction via `registerSync`).
[ADR-0015](./0015-syncable-mixin.md) leaves `runSyncedWrite` unchanged and makes
it also the write path for a host tool body on a mixed base
(`this.sync.runSyncedWrite`), so a committed insert drains the CDC log and
broadcasts in the same step regardless of the DO's framework base.

## Context

The model has two **client-originated** write paths: a **mutation** (`mut` —
optimistic, applied in `transactionSync`, confirmed exactly-once by `txId` on the
single ordered stream) and a **command** (`call` — a side-effecting call that
returns a result). Both assume a connected client and an optimistic overlay to
retire.

But writes also originate **server-side**, outside that flow entirely: an agent
inserts a message row, a webhook lands an event, a cron/`alarm` job updates state,
an admin tool edits a record, a bulk seed loads fixtures. For this agent-runtime
spec, the agent case is first-class — agent-generated rows must reach connected
clients.

These have no home. A raw `ctx.storage.sql.exec` fires the CDC triggers (the
change lands in `_sync_changes`) but is **not broadcast** until the next
mutation's `drainAndBroadcast` happens to flush the backlog — so the write is
invisible to clients until something unrelated nudges it, and the accumulated
backlog then floods out at once. The `examples/board` seed/firehose worked around
this by reaching into the `protected drainAndBroadcast()` and knowing the
drain-cursor rule. That footgun is the gap this ADR closes.

## Decision

A single `protected` method on `SyncDurableObject`:

```ts
protected runSyncedWrite<T>(fn: (sql: SqlStorage) => T): T
//  = transactionSync(fn)  →  drainAndBroadcast()  →  return result
```

It runs the caller's **raw write closure** inside `transactionSync`, then drains
and broadcasts the resulting CDC, and returns the closure's value. Called from
the DO's own handlers (`fetch` routes, `alarm`). It is **not** a client write:
no `txId`, no `committed` receipt, no optimistic overlay, no dedup.

### The grilled choices

- **Raw closure, not a registered-mutation dispatch.** The motivating writes —
  5,000-row seed, `UPDATE … ORDER BY RANDOM()`, arbitrary admin SQL — don't map
  to a single per-row mutation `op`. The CDC triggers are on the *tables*, so any
  write the closure makes is captured and broadcast without `runSyncedWrite`
  consulting the registry. (Re-running a registered mutation server-side is a
  narrower convenience that can layer on later; not built speculatively.)

- **Synchronous closure in `transactionSync`.** Atomicity (the seed commits or
  rolls back as one, not 5,000 inserts racing the firehose) and the same workerd
  constraint mutations already live under (ADR-0001 D11/C6). A thenable return is
  rejected, the same guard `handleMut` applies to `execute` — fail loud rather
  than silently lose atomicity. Any async prep is the caller's job, before the
  call.

- **No flush barrier.** `handleMut` flushes the originating socket before its
  `committed` (ADR-0002 C1) because a client mutation has a receipt to order
  against. A server write has **no originating connection and no receipt**, so it
  just `drainAndBroadcast()` and lets the coalescer tick flush every subscriber.

- **No dedup; the caller owns idempotency.** A server write has no `txId`. But
  collections already mandate **client-supplied stable keys** (ULID/UUIDv7, the
  optimistic-id-parity rule), so the idempotency unit already exists: write
  `id=<ulid>` with `INSERT OR IGNORE` and a retry/redelivery is a SQL no-op.
  Building a second dedup mechanism for a caller who already holds a stable key
  was rejected. Same contract as `afterCommit`: make it idempotent.

- **`afterCommit` is out of scope.** `runSyncedWrite` originates from DO handlers
  (which have `this`), not from inside a mutation's hook. Broadcasting a
  *cascading* write from `afterCommit` would need a distinct `AfterCommitCtx`
  carrying the method (it can't go in the shared `MutationCtx` — `execute` already
  runs inside a transaction, `authorize` runs pre-tx), and there's no concrete
  need yet. Deferred.

- **Caller ensures `initRegistry()`.** Kept out of `runSyncedWrite` to keep the
  primitive overhead-free on frequent ops. See the caveat below.

## Consequences

- **Cold / idle DO is fine.** With no subscribers connected, `drainAndBroadcast`
  enqueues nothing but still advances the drain cursor; the write is durable, and
  a later client's bounded snapshot (or a reconnect catch-up within retention)
  delivers it. Consistent with the existing snapshot/catch-up paths.

- **Init caveat, and its sunset.** Because schema + CDC triggers are installed
  lazily by `initRegistry()` (the base constructor can't read the subclass's
  `registry` field — it's `undefined` during `super()`), a server write to a
  never-connected room must `initRegistry()` first, or it writes to a table with
  no triggers and silently produces no CDC. Recorded follow-up: move schema
  bootstrap into the constructor via `ctx.blockConcurrencyWhile` (idiomatic CF
  migrate-before-serve; cf. Cloudflare Actors' migrate-on-start), or move
  `CREATE TABLE` ownership out of the registry to the subclass entirely. Either
  retires this caveat — orthogonal to `runSyncedWrite`'s contract, so deferred.

- **Dogfood.** `examples/board`'s `/seed` and `/bump` move from the
  `drainAndBroadcast()` reach-in to `runSyncedWrite`.
