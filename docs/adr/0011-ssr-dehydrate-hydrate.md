# 0011 — SSR: dehydrate on the worker, hydrate to the cursor

**Status:** Accepted (experimental — tracks TanStack DB draft PR
[#1564](https://github.com/TanStack/db/pull/1564); the upstream hook signatures
may change before release). Generalizes ADR-0002 C1's flush-before-`committed`
barrier to *all* cursor-advancing emissions (C1′ below).

## Context

TanStack DB's draft SSR PR adds `DbClient` with row-level
`dehydrate()`/`hydrate()` and three opaque sync-config hooks —
`exportSyncMeta(): unknown`, `importSyncMeta(meta)`,
`mergeSyncMeta(current, incoming)`. Facts about the upstream design that
constrain ours (verified against the PR, not its docs — it has none for
adapter authors):

- Hydration applies rows as **synced upserts** (`committed: true,
  immediate: true`) **before** `importSyncMeta` runs. An adapter cannot veto
  row application; correctness must come from post-connect catch-up.
- Dehydrated state has **no tombstones**. It is a snapshot *at* the exported
  cursor, never a delta. All delete-correctness is the adapter's problem.
- Hydration does **not** mark the collection ready; readiness stays under the
  adapter's `markReady()`.
- The hooks live on the (potentially module-scoped, request-shared) sync
  config. Our options creator takes the transport as an argument, so on the
  server our options are **per-request by construction** — we sidestep the
  upstream cross-request-leak hazard, and document per-request creation as a
  requirement.
- TanStack sync-write semantics: a sync `insert` for an existing key throws
  `DuplicateKeySyncError` unless values deep-equal; a sync `update` for a
  missing key upserts (our move-in path already relies on this).

Our model is one ordered stream per DO and a **single client cursor**
(`appliedSeq`) advanced only at commit boundaries (ADR-0001/0002). The server
already serves `since` on *any* sub — windowed catch-up within the retention
floor, honest `reset` below it (ADR-0009). SSR support is therefore mostly:
get a snapshot + cursor out of the DO without a WebSocket, round-trip the
cursor through the dehydrated state, and make the first sub carry `since`.

## Decision

### D1 — Socketless snapshot read: `readSnapshot` RPC

`SyncDurableObject` gains a public RPC method:

```ts
readSnapshot(req: { collection: string; where?: unknown; orderBy?: unknown; limit?: number })
  : { rows: Array<Record<string, SqlStorageValue>>; cursor: string }
```

Same compile path as the `fetch` frame (`compileSubsetQuery`); synchronous
SQLite, so rows and cursor are at one position. Throws on unknown collection or
unsupported predicate — fail loud; RPC propagates. Trust model: callable by any
worker holding the binding, the same boundary as the Worker-forged-claims model
for WS auth (the SSR worker is first-party).

**The exported cursor is a durable high-water mark** — `max(MAX(_sync_changes
.seq), drain_cursor)` — *not* bare `currentSeq()`, because retention can prune
the changelog empty while the table has rows, and a bogus cursor `0` against
live rows would let a delete that lands between render and hydration strand a
stale row forever (adversarial-review finding). Cursor `"0"` therefore honestly
means "no resume point": the client omits `since` and reconciles (D4).

### D2 — `SsrSnapshotTransport`, and `Transport` as an interface

What `doCollectionOptions` consumes becomes a structural `Transport` interface
(satisfied by `WebSocketTransport` unchanged). `SsrSnapshotTransport` implements
it for server rendering: constructor takes `read: (req) => Promise<{rows,
cursor}>` (the author passes `(req) => stub.readSnapshot(req)`; no Cloudflare
types in the client build). `subscribe` performs one read and synthesizes
`onSnap*`/`onSnapEnd`; `connect()` resolves immediately (so on-demand
`loadSubset` during a server `preload()` works unchanged); its cursor is the
**min** across reads (the safe joint resume point — replay is idempotent);
`sendMut`/`sendCall`/`fetch` throw `SsrReadOnlyError`. SSR is read-only.

### D3 — syncMeta carries the cursor; the first sub carries `since`

`doCollectionOptions` implements the hooks:

- `exportSyncMeta → { v: 1, cursor: transport.appliedCursor }`
- `importSyncMeta` — validate (`v` unknown → throw), stash `hydratedCursor` in
  the per-call closure, `transport.seedCursor(cursor)`.
- `mergeSyncMeta → min(cursor)` — min is self-healing: a late/stale chunk's
  rows are applied upstream before we're consulted, and a min cursor makes the
  next catch-up replay exactly the clobbered window.

`seedCursor(c)` may **regress** `appliedSeq` (claiming a *shorter* applied
prefix is always safe); if already subscribed it triggers `resubscribeAll()`,
whose catch-up replay (latest-op-per-key against current rows) re-freshens
whatever a late hydration chunk clobbered. One mechanism for early and late
hydration; no second cursor, no ack channel.

With a `hydratedCursor` (consumed once at sync start; cleared in the sync
cleanup fn — after a collection GC the rows are wiped, so a retained cursor
would resume over an empty store and silently lose data):

- **Eager**: the first sub carries `since`; `markReady()` immediately (rows are
  present; catch-up arrives as `d`+`uptodate`, which never fires `snap-end`).
  Below the retention floor the server answers `reset` → truncate + fresh
  snapshot: an explicit stale-while-revalidate choice, documented.
- **On-demand**: `markReady()` on connect as today, plus **one transient
  unfiltered catch-up sub** (`since = hydratedCursor`, no `where`) that
  unsubscribes at `uptodate`/`reset`. The dehydrated rows are the union of the
  server-loaded subsets; per-subset `since` is unsound for any subset the
  dehydrated state didn't cover, and subset-tracking still leaves
  overlapping-`where` stale-delete holes. One unfiltered catch-up covers every
  changed key (always-emit ⇒ synthetic deletes included) in the seconds-wide
  render→hydrate window. **Semantic cost, accepted and documented**: changes to
  rows outside any hydrated subset land in the collection during that window,
  weakening on-demand's "only loaded subsets are present" model (bounded by
  change volume in the window). The catch-up sub is registered before any
  `loadSubset` sub, so a below-floor `reset`'s truncate lands before subset
  snapshots repopulate (frame order on one socket).

### D4 — Snapshot reconciliation (and two pre-existing bugs fixed)

Adversarial review (gpt-5.5) rejected the obvious "insert-if-absent" guard for
snapshots — `snap-end` advances the cursor, so *skipping* a fresher snapshot
value and then dropping the socket loses that write forever. Instead:

- **C1′ (server)**: `broadcaster.flushOne(ws)` before any synchronous
  cursor-advancing emission (`handleSub` snapshot, `emitCatchUp`). C1 said
  "deltas flush before `committed`"; C1′ says **a socket's pending coalesced
  deltas always precede any cursor boundary on that socket**. This fixes a
  pre-existing bug independent of SSR: a multi-collection reconnect's catch-up
  `uptodate` could advance the cursor past another collection's still-buffered
  delta (drop before the tick ⇒ lost write).
- **`onSnap` writes update-if-exists** (snapshot value wins). With C1′ a
  snapshot value is never staler than the held row, so this converges; it also
  absorbs `DuplicateKeySyncError` when a subset snapshot lands over hydrated
  rows that changed since dehydration. (`loadMore`'s page path keeps its
  insert-if-absent: `page` frames never advance the cursor, and a page *can*
  be staler than a held row.)
- **Key-reconcile for the hydrated-eager fresh-snapshot path**: when a
  snapshot arrives over hydrated rows (cursor `"0"`, i.e. no resume point),
  collect the snapshot's keys and at `snap-end` delete held keys absent from
  it, scoped by the static `where` predicate. The honest set semantics of a
  snapshot, without a truncate's flash-to-empty (SSR exists for first paint).
- **`onDelta` maps `insert` → `update` when the key exists** — catch-up emits
  the latest CDC op per key, so a delete-then-reinsert since the cursor arrives
  as `insert` against a held key and would throw. Pre-existing on reconnect
  catch-up too; fixed for both.

### D5 — Packaging

Core stays compatible with released `@tanstack/db` (>= 0.6): the hooks are
additive and ignored by older versions; `since`-on-first-sub and `seedCursor`
are version-independent. No self-branding via `Symbol.for` — users wrap
`collectionOptions(doCollectionOptions(...))` for `DbClient`. Round-trip tests
and `examples/ssr` (TanStack Start on Cloudflare) build against packed
PR-branch tarballs vendored as **branch-only** devDependencies, removed when
upstream publishes. Everything lands as **experimental** in the changelog.

## Known limitations

- **No incarnation epoch.** A cursor from a pre-storage-reset DO whose new
  changelog already reaches past it would catch up silently-wrong. The exposure
  window for SSR is seconds and requires a storage reset inside it; fixing it
  properly is a protocol rev (an epoch in `_sync_meta` + a hello/epoch frame),
  deliberately deferred. Pre-existing for in-page reconnects too.
- **Upstream is a draft.** The hook signatures (per-config, no collection
  argument, no veto in `mergeSyncMeta`) are likely to change; our surface is
  one closure and three small hook bodies, kept deliberately thin.

## Consequences

- SSR first paint with no WebSocket from the render path, no idle timers, no
  hibernation impact (the RPC is a plain request).
- The single-cursor inversion survives intact: `since` at first sub is a
  bootstrap parameter, `seedCursor` only ever claims a shorter prefix, and
  confirmation still rides the one stream.
- C1′ and the `onDelta` normalization harden reconnect for all clients, SSR or
  not.
