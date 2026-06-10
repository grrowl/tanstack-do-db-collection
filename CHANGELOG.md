# Changelog

All notable changes to `tanstack-do-db-collection` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While pre-1.0, the public API may change between 0.x releases.

## [Unreleased]

### Fixed

- **Cursor barrier (C1′, ADR-0011).** A snapshot or catch-up served on a socket
  with still-buffered coalesced deltas could advance the client's cursor past
  an undelivered write (multi-collection reconnect; drop before the tick lost
  the write). The server now flushes the socket's pending deltas before any
  synchronous cursor-advancing emission — ADR-0002 C1 generalized from
  `committed` to all cursor boundaries.

## [0.3.0] — 2026-06-09

### Added

- **Typed mutations** (ADR-0010). `SyncRegistry` takes a third generic — a
  collection-row manifest — so handlers are fully typed without casts:
  `new SyncRegistry<TUser, Env, { messages: Message }>()` types `pk` (must be a
  column) and each handler's `op.cols` per op (`insert`→`Message`,
  `update`→`Partial<Message>`, `delete`→none). Purely type-level; the untyped
  two-generic form still works (`op.cols` falls back to `unknown`).
- **Changelog time-based retention** (ADR-0009). A new `changelogRetentionMs`
  knob (default 2 days) prunes `_sync_changes` rows older than the window, so the
  log is bounded by age, not just key-cardinality. A client reconnecting from
  beyond the surviving floor now receives a `reset` + full snapshot instead of an
  incremental delta. Set `changelogRetentionMs: null` to disable retention
  (compaction-only, the prior behavior).

### Changed

- **Renamed `Registry` → `SyncRegistry`.** Breaking, no compat shim. The old
  name was too generic and clashy; the public surface is uniformly `Sync*`
  (`SyncDurableObject`, `runSyncedWrite`, `registerSync`). Update your import and
  `new SyncRegistry(...)`.
- `registerSync` now reconciles CDC triggers to the registry instead of only
  adding them: triggers for a collection you've removed from the registry are
  dropped on the next `registerSync`, so an orphaned table stops firing capture
  triggers into `_sync_changes`. Trigger reap only — existing change rows are
  left untouched. (ADR-0008)

## [0.2.0] — 2026-05-31

### Changed

- **Author-owned schema; `registerSync` wires the sync.** Breaking. Collection
  definitions are now `{ table, pk }` — `ddl` is removed. You create your tables
  yourself (raw DDL, Drizzle, any migrator), then call `this.registerSync(registry)`
  — typically in your constructor's `blockConcurrencyWhile`, after migrating. It
  validates each table (one `PRAGMA`: the pk is a sole `TEXT` client key — D9) and
  installs the CDC triggers. Lazy `initRegistry`-on-`fetch` is gone; schema exists
  before the first event. Forget the call → `this.registry` throws loud. This also
  retires `runSyncedWrite`'s "caller ensures init" caveat. See
  [ADR-0007](docs/adr/0007-author-owned-schema-register-sync.md).

- **The cursor `fetch` frame now mirrors `@tanstack/db`'s `LoadSubsetOptions`.**
  Breaking wire change (client and server ship together). The frame carries a
  base `where` plus a raw `cursor: { whereFrom, whereCurrent }` — TanStack's own
  `CursorExpressions` names and semantics (the cursor expressions exclude the
  base `where`) — replacing the previous private `where`/`ties` fields that
  combined the predicates client-side. The server now composes `base AND
  whereCurrent` (ties, unbounded) and `base AND whereFrom` (next page, bounded by
  `limit`). Behaviour is unchanged (identical compiled SQL); the win is
  traceability to upstream. A malformed cursor (a missing half) is now rejected
  loudly instead of degrading to an unbounded scan. See
  [ADR-0005](docs/adr/0005-fetch-frame-mirrors-loadsubsetoptions.md).

- **Collection pk validation accepts TEXT *affinity*, not the literal `"TEXT"`.**
  `registerSync` now admits any TEXT-affinity pk (`TEXT`, `VARCHAR`, `CHAR`,
  `NVARCHAR`, …) so ORM/migrator-generated DDL composes; `INTEGER` (rowid alias)
  and other non-TEXT affinities are still rejected loudly (they'd break optimistic
  id parity). Part of ADR-0007.

### Added

- **`runSyncedWrite(fn)`** — a `protected` `SyncDurableObject` primitive for
  **server-originated writes** (an agent inserting a row, a webhook, a cron/
  `alarm` job, an admin edit, a bulk seed): apply a raw synchronous SQL closure
  in a transaction, then broadcast the resulting CDC to connected clients.
  Outside the client mutation flow — no `txId`, no receipt, no dedup (idempotency
  rides the collection's mandated stable keys).
  See [ADR-0006](docs/adr/0006-server-originated-writes.md).
- **`examples/board`** — an at-scale stress example: 5,000 tasks on one DO,
  bounded window load, `useLiveInfiniteQuery` cursor scroll-back, and a mutable
  order key (`updated_at`) so voting/starring bumps a task to the top (move-in
  via the always-emit upsert). A server-side firehose makes the deferred
  bounded-window-under-churn limitation visible: `loaded` climbs past `window`.
- A real-tie integration test (unbounded boundary ties + limited next page + base
  predicate composed into both halves) covering the cursor `fetch` against the DO.
- A move-in integration test: a cold row bumped server-side arrives via the
  no-`where` live sub and upserts into the collection (ADR-0002 C4).

## [0.1.0] — 2026-05-30

### Added

- **`afterCommit` post-commit hook + `env` in handler context.** Mutations gain
  an optional `afterCommit(ctx)` that runs fire-and-forget via `waitUntil` after
  the commit and receipt — the sanctioned home for external side effects a
  synchronous `execute` can't do (delete an R2 object, enqueue a job). It's
  isolated (a throw is logged, never affects the committed mutation) and owns its
  own idempotency. Handler contexts (`authorize`/`execute`/`afterCommit` and
  command `execute`) now receive the DO's `env`, typed via a new `Env` generic on
  `Registry` that defaults to `unknown` (existing `Registry<TUser>` unchanged).
  See [ADR-0004](docs/adr/0004-after-commit-hook.md).
- **Bounded initial load for on-demand windows.** A live query's `orderBy` and
  `limit` are forwarded to the server so the initial snapshot is the bounded
  window (e.g. the most recent N rows) rather than the whole `where` subset. The
  live subscription's predicate stays the `where` clause, so entering rows are
  still delivered.
- **Cursor load-more (scroll-back).** Extending a windowed query past its first
  page issues one one-shot paginated `fetch` (new `fetch`/`page` wire frames)
  carrying both halves of the cursor double-read — boundary ties (`ties`,
  unbounded) and the next page (`where`, bounded by `limit`), each combined with
  the base `where`. The server reads both at a single `seq` (atomic), and no new
  live subscription is taken — the window's deltas already flow over the existing
  `where` sub. See [ADR-0003](docs/adr/0003-atomic-cursor-fetch.md).

### Fixed

- **Cursor load-more no longer resurrects a concurrently-deleted row.** The
  earlier two-frame double request read the ties at one `seq` but applied them
  after a deferred merge; a live delete landing in between let the stale tie
  re-insert the deleted row, with no future delta to correct it. The double-read
  is now one atomic `fetch`, so the page applies in stream order before any later
  delta. See [ADR-0003](docs/adr/0003-atomic-cursor-fetch.md).
- **Cursor load-more no longer throws on overlapping boundary rows.** Page rows
  are written insert-if-absent, so a boundary tie already in the window (or a row
  a concurrent live delta already refreshed) is skipped rather than re-inserted —
  which would otherwise throw `DuplicateKeySyncError` and abort the open sync
  transaction. The live `where` subscription stays the source of truth for rows
  currently in the collection.
- **Rejected subscriptions no longer hang `preload()`.** A `reset` with no
  `snap-end` (the server's response to an unsupported predicate or unknown
  collection) now resolves the subset's load promise instead of leaving the live
  query waiting indefinitely.
- **On-demand `orderBy` IR shape.** `orderBy` clauses from real live queries
  (`{ expression, compareOptions }`) were not recognised by the SQL compiler, so
  server-side ordering was silently dropped and the wrong rows were returned for
  a bounded window. The compiler now accepts the live-query clause shape.

## [0.0.1]

Initial release — sync a [TanStack DB](https://tanstack.com/db) collection to a
[Cloudflare Durable Object](https://developers.cloudflare.com/durable-objects/)
over a single WebSocket. Server-authoritative, single-writer, no CRDTs.

### Added

- **Single-ordered-stream sync.** Change-data-capture via SQLite triggers into
  one per-DO change log; one monotonic `seq` cursor drives live deltas,
  reconnect catch-up, and write confirmation. The client tracks a single
  `appliedSeq` — no second acknowledgement channel.
- **Optimistic mutations** with single-stream confirmation (`awaitSeq`):
  `mut` (atomic row transactions) and `call` (named commands), exactly-once via
  `txId` dedup. Mutation `execute` runs inside `transactionSync`; `authorize`
  runs before it.
- **Client-supplied keys** enforced at `defineCollection` (ULID/UUIDv7); the
  optimistic id equals the confirmed id.
- **Filtered subscriptions.** A `where` predicate (a `@tanstack/db`
  `BasicExpression`) is evaluated server-side with `@tanstack/db`'s own
  compiler, so operators match the client exactly; move-in/move-out handled
  without a before-image. Client-side write-outside-filter preflight
  (`WriteOutsideSubError`).
- **Subset shaping** — `where`/`orderBy`/`limit`/`offset` lowered into SQLite
  (operator floor: eq, ne, gt, gte, lt, lte, like, in, and, or, not);
  un-lowerable predicates are rejected, never silently full-scanned.
- **On-demand subsets** (`syncMode: 'on-demand'`) — load only the subsets your
  live-query `where` clauses request; each distinct `where` is one refcounted
  server subscription, released on the last unload. Writes outside every loaded
  subset are confirmed without stranding an optimistic row.
- **Egress coalescer** — per-`(sub, key)` last-write-wins on a tunable tick,
  collapsing high-rate writes (e.g. streaming tokens) to one delta per tick;
  hibernation-native (no timer when idle).
- **Reconnect catch-up** — auto-reconnect resubscribes from the applied cursor;
  the server serves a windowed delta or, past the retention floor, a reset +
  snapshot.
- **Compaction-defined retention** — opportunistic (every N writes, off the
  response path via `waitUntil`; no idle-DO wakeups): the change log collapses
  to latest-op-per-key, with independent time-based dedup GC.
- **Multiplexing** — many collections on one DO share a single WebSocket.
- **Client IVM** — live queries (joins, filters, aggregates) run client-side
  via `@tanstack/db`; the DO stores and emits, never runs IVM.
- **Binary wire** — MessagePack frame codec (JSON debug fallback) + a tagged
  value codec preserving bigint/Date/NaN/±Infinity/-0/undefined/Uint8Array.
- **Hibernating WebSockets** — `acceptWebSocket` + auto-response ping/pong;
  per-socket identity via `serializeAttachment`.
- **Examples** — `examples/chat` (eager, multi-tab live sync) and
  `examples/on-demand` (on-demand subsets), both verified in a real browser.

### Deferred (post-1.0)

- Windowed pagination (`orderBy`/`limit`/cursor double-request with
  server-side window maintenance under churn).
- `isWhereSubset` containment dedup of overlapping subsets.
