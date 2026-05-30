# Changelog

All notable changes to `tanstack-do-db-collection` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While pre-1.0, the public API may change between 0.x releases.

## [Unreleased]

### Changed

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

### Added

- A real-tie integration test (unbounded boundary ties + limited next page + base
  predicate composed into both halves) covering the cursor `fetch` against the DO.

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
