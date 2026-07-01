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

### D1 — Socketless snapshot read: `readSyncSnapshot` RPC

`SyncDurableObject` gains a public RPC method:

```ts
readSyncSnapshot(
  req: { collection: string; where?: unknown; orderBy?: unknown; limit?: number },
  request: Request, // REQUIRED — runs through parseAttachment, the one auth gate
): Promise<{ rows: Array<Record<string, SqlStorageValue>>; cursor: string }>
```

Same compile path as the `fetch` frame (`compileSubsetQuery`); the gate awaits
*before* the reads, so rows and cursor are still taken at one position
(synchronous SQLite between them). Throws on unknown collection or unsupported
predicate — fail loud; RPC propagates.

Trust model: the binding limits callers to first-party workers, and the
REQUIRED `request` argument runs through **`parseAttachment` — the same gate
as the WS upgrade**. The worker passes the claims-bearing Request it already
forges (or forwards) for the socket path; a rejecting `parseAttachment`
rejects the read. Two paths, one gate: an author's tenant check cannot be
silently bypassed by the snapshot read (grill-session finding — an earlier
draft had no gate here, inverting the WS path's safe-by-default shape). The
minted claims are also the seam where uniform read-scoping would land, on
subs and snapshots alike — note that today *neither* path filters rows by
identity; `parseAttachment` is connection/read-level gating, and the
client-supplied `where` is shaping, not security.

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
cursor}>` (the author passes `(req) => stub.readSyncSnapshot(req, request)`,
closing over the request's claims; no Cloudflare
types in the client build). `subscribe` performs one read and synthesizes
`onSnap*`/`onSnapEnd`; `connect()` resolves immediately (so on-demand
`loadSubset` during a server `preload()` works unchanged); its cursor is the
**min** across reads; `sendMut`/`sendCall`/`fetch` throw `SsrReadOnlyError`.
SSR is read-only.

Min is not merely the *safe* joint resume point (replay is idempotent;
skipping is not) — it is *self-consistent-making*: a render's reads land at
slightly different positions (milliseconds of DO time apart), and the first
catch-up from min replays exactly that skew window, converging every
dehydrated row to one position. Because the changelog `seq` is one stream
across all collections on the DO, the min is also a coherent position for
every collection sharing the transport — no per-collection reset risk.
Per-table cursor tracking (`cursorFor(table)`) was considered and rejected:
permanent interface surface to avoid a transient milliseconds-wide replay.

### D3 — syncMeta carries the cursor; the first sub carries `since`

`doCollectionOptions` implements the hooks:

- `exportSyncMeta → { v: 1, cursor: transport.appliedCursor, where? }` —
  `where` is a fingerprint (the codec envelope) of the eager filter the rows
  were dehydrated under. A cursor is only a sound resume point *for that
  filter*: catch-up emits changed keys only, so an **unchanged** out-of-filter
  hydrated row would never be reconciled away (second-review finding).
- `importSyncMeta` — validate (`v` unknown / malformed cursor → throw); a
  fingerprint mismatch (deploy skew) refuses the cursor and downgrades to the
  always-sound snapshot-reconcile path (`hydratedCursor = "0"`, transport
  unseeded); otherwise stash `hydratedCursor` and `transport.seedCursor(c)`.
- `mergeSyncMeta → min(cursor)` — min is self-healing: a late/stale chunk's
  rows are applied upstream before we're consulted, and a min cursor makes the
  next catch-up replay exactly the clobbered window.

`seedCursor(c)` may **regress** `appliedSeq` (claiming a *shorter* applied
prefix is always safe). A regress while LIVE cannot replay on the same socket:
boundary frames the server already sent (full duplex) would dispatch after the
regress and re-advance the cursor past the repair window (second-review
blocker). It therefore **forces a reconnect** — the old socket's queued
boundaries stop counting (`advance` suppressed; their data still applies,
idempotently) and the fresh socket resubscribes from the seed. One mechanism
for early and late hydration; no second cursor, no ack channel.

With a `hydratedCursor` (consumed once at sync start; cleared in the sync
cleanup fn — after a collection GC the rows are wiped, so a retained cursor
would resume over an empty store and silently lose data):

- **Eager**: the first sub carries `since`; `markReady()` immediately (rows are
  present; catch-up arrives as `d`+`uptodate`, which never fires `snap-end`).
  Be explicit about what this changes: **hydration redefines `ready` as
  "renderable", not "synced"** — `isReady` is true on the server pass (no
  socket will ever exist) and stays true offline with stale rows. That is the
  stale-while-revalidate contract, deliberately. An app that wants a
  "catching up → live" signal (a SyncIndicator) doesn't need new API: the
  transport already exposes it — `awaitSeq(String(BigInt(dehydratedCursor) +
  1n))` resolves at the first post-hydration boundary, i.e. caught up. Not
  README material (sharp-edged); recorded here for when someone asks.
  Below the retention floor the server answers `reset` → truncate + fresh
  snapshot — which DOES flash empty between the truncate's commit and the
  snapshot's (unlike the cursor-`"0"` reconcile path). Accepted, not fixed:
  the dehydrated cursor is seconds old, so falling below the floor requires
  `changelogRetentionMs` (default 2 days) shorter than the HTML's flight time
  — pathological config, not a reachable state. Unifying it would need the
  client to skip the truncate and let snapshot set-semantics reconcile, but a
  `reset` is also the only terminal for a REJECTED sub (no snapshot follows),
  where skipping the truncate keeps stale rows forever — the one outcome
  ranked worst throughout this design. The reset-cause ambiguity is harmless
  today (rejection is dev-loud; below-floor is pathological) and becomes
  worth a wire-level distinction — likely alongside the incarnation epoch —
  when client-side persistence (an LRU'd local db) makes days-old cursors
  routine. Future scope, deliberately not now.
- **On-demand**: **one transient unfiltered catch-up sub**
  (`since = hydratedCursor`, no `where`) that unsubscribes at *its own*
  sub-scoped terminal — never at a broadcast boundary, which can precede its
  frames. The dehydrated rows are the union of the server-loaded subsets;
  per-subset `since` is unsound for any subset the dehydrated state didn't
  cover, and subset-tracking still leaves overlapping-`where` stale-delete
  holes. One unfiltered catch-up covers every changed key (always-emit ⇒
  synthetic deletes included) in the seconds-wide render→hydrate window.
  **Semantic cost, accepted and documented**: changes to rows outside any
  hydrated subset land in the collection during that window (bounded by
  change volume). The leaked rows' staleness is **unobservable**: a live
  query whose predicate matches one has a server sub with that same
  predicate, whose snapshot/deltas converge it at observation time
  (update-if-exists) — stale only while nothing looks, fresh by the time
  anything does. Eager rendering of stale/leaked data is acceptable against
  the snappy client-first UI it buys; the residual cost is memory, bounded
  by seconds of change volume. `markReady()` **gates on the catch-up sub frame being
  sent** (not completed): `loadSubset` subs fire only after ready, so on the
  single ordered socket the catch-up always precedes subset snapshots
  (second-review finding — `connect().then(markReady)` alone races).
  Wire note: the transient's teardown depends on the server scoping the
  catch-up terminal (`uptodate.sub`); against a pre-0011 server the terminal
  arrives unscoped and the transient sub never tears down (an unfiltered
  live sub leaks until the socket drops). Matter-of-fact, not mitigated:
  pre-1.0, client/server version skew is not a supported configuration —
  the worker ships the bundle and the DO from one deploy.
  When the hydrated rows are **unresumable** — cursor `"0"`, or the server
  `reset`s the catch-up below the floor — on-demand **truncates** them
  (the reset path also unsubscribes immediately so the trailing unfiltered
  resnapshot is dropped unhandled). A full-table snapshot was rejected here —
  and the principled line between this and the tolerated catch-up leak above
  (both are "stale while unobserved, fresh when observed") is **on-demand's
  memory contract**: memory proportional to what you observe. A seconds-wide
  window of changed keys respects that contract asymptotically; a full-table
  snapshot breaks it categorically — unbounded in table size, on the mode
  whose purpose is not loading the table. The truncate refuses to convert
  on-demand into accidental-eager. Eager keeps the no-flash reconcile;
  on-demand keeps honesty.

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
- **Key-reconcile is ALWAYS armed for eager subs** (grill-session
  generalization; never for on-demand subset subs, whose snapshot must not
  delete other subsets' rows): an eager snapshot is authoritative set
  semantics over synced rows, period — at `snap-end`, held synced keys
  absent from the snapshot are deleted. For the normal empty-at-first-
  snapshot flow it is a no-op (and boundary-free: `begin` opens only when a
  delete is due); for ANY path where synced rows precede a snapshot —
  hydration with no resume point, a refused foreign-filter cursor, meta that
  failed validation — it is what prevents a server-deleted held row from
  being stale forever. An EMPTY snapshot still reconciles (zero keys is an
  authoritative set — second-review blocker). Honest set semantics without a
  truncate's flash-to-empty (SSR exists for first paint). Presence checks
  steer by `syncedData`, never the combined view — optimistic overlays are
  invisible to sync writes by design.
- **The syncMeta hooks fail loud but SAFE** (grill-session finding): upstream
  applies a chunk's rows BEFORE `mergeSyncMeta`/`importSyncMeta` run — a
  validation throw cannot veto them, so throwing alone would leave applied
  rows with no reconcile intent (and, on-demand, no truncate): stale
  forever. Both hooks set `hydratedCursor = "0"` (the always-sound
  snapshot-reconcile / truncate route) BEFORE throwing — the version skew
  still surfaces to the app, and the state left behind converges. This is
  also the gradual-upgrade path: a future `v: 2` payload degrades old
  clients safely and loudly; no per-version fallback logic.
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
