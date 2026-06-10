# 0002 — Corrections from adversarial review: ordering, shaping, retention

**Status:** Accepted. Amends [ADR-0001](./0001-sync-architecture.md). C5's
changelog-retention floor is refined by
[ADR-0009](./0009-changelog-time-retention.md). C1's flush-before-`committed`
barrier is generalized to ALL cursor-advancing emissions (C1′) by
[ADR-0011](./0011-ssr-dehydrate-hydrate.md).

## Context

Before writing implementation code, ADR-0001's design was put through an
adversarial review (gpt-5.5 via `codex exec`), cross-checked against the actual
TanStack DB and Electric source in `_reference/`. It surfaced one real ordering
hazard (three related blockers with a single root cause) and several
retention/shaping corrections. We adjust the design *before* M1, while nothing
depends on the superseded wording.

Each correction below was verified by reading the cited source, not by trusting
the review. Where we override the review, we say so and why.

## Corrections

### C1 — `committed` must be an ordered barrier, not an out-of-order leap

*Supersedes the "bypass the coalescer" detail of ADR-0001's single-ordered-
stream model and DESIGN-INPUT §2.*

**Flaw.** ADR-0001 had the `committed` frame bypass the egress coalescer and
advance `appliedSeq` immediately, while the matching row delta stayed buffered
~50ms. Verified against TanStack DB:

- `collection/state.ts:336-349` — reads prefer optimistic, then synced state.
- `transactions.ts` `commit()` awaits `mutationFn`, then sets `completed` and
  touches the collection, triggering an optimism recompute.

If `committed(seq=N)` reaches the client before the coalesced delta for the same
write: (a) `appliedSeq` advances past deltas the client never received → a
reconnect from `since=N` silently skips them (data loss); and (b) the optimistic
overlay is dropped before the authoritative row lands → the row flickers to a
stale/absent value.

**Correction.**

1. The DO flushes the **originating connection's** pending matched deltas
   (`flushOne(ws)`) immediately before sending that connection's `committed`
   frame. This is a *per-connection* ordered barrier — **not** the prior spike's
   global flush-before-ack across all connections, and **not** a second cursor.
2. The client binds confirmation to a **sync commit**: inbound deltas are
   buffered via `begin()`/`write()`; a `committed`/`uptodate` frame triggers
   `commit()`, which advances the single `appliedSeq` and resolves `awaitSeq`.
   `appliedSeq` therefore always denotes a *contiguous applied prefix*.

**Net.** The single-cursor win of ADR-0001 stands — the spike's dual
`appliedCursor`/`ackedCursor` remains deleted. What changes is the honesty of
the claim: there **is** a scoped, per-connection flush-before-`committed`
barrier. "No flush-before-ack of any kind" was an overclaim. The win is
"one cursor + a per-connection barrier," not "no barrier."

### C2 — Retiring optimistic state requires a client sync commit (incl. no-match)

*New; corrects ADR-0001's "no synced write needed for the no-subscription-match
case."*

**Flaw.** `collection.insert/update/delete` create **direct** transactions
(`collection/mutations.ts:232-245`). Verified (`collection/state.ts:483-508`,
`:552-558`): a *completed* direct optimistic upsert is **retained and still
shown** even with no pending sync, and is cleared **only** by a later sync
commit (`state.ts:1167-1188`). So a payload-free `committed` that merely
resolves `mutationFn` does **not** drop the overlay — for a no-subscription-
match write the row would linger indefinitely.

**Correction.** The client adapter synthesizes a follow-up **sync-commit
boundary** after a mutation's transaction completes (mirroring Electric, which
relies on the next `up-to-date` sync commit to clear stale direct upserts —
`electric-db-collection/src/electric.ts:1786-1817`). For a matched write the
boundary carries the delta; for a no-subscription-match write it is empty and
exists solely to run the clear path.

> **VERIFIED** (tests/empty-commit-probe.test.ts): an empty `begin()/commit()`
> after the transaction completes DOES run the clear path and drops a confirmed
> no-match direct upsert (`get` returns undefined). For an in-view write the
> same empty commit is a no-op on the visible row — the key is still in synced
> state, so `state.ts:1170`'s `!currentVisibleState.has(key)` guard keeps it.
> So a single post-mutation empty commit is safe for both cases; no targeted
> delete needed. Wired into the adapter + integration-tested when on-demand
> windows (which create the reachable no-match case) land.

> **Reachability (refined during M3):** the stranded-direct-upsert case only
> arises when a client writes a row that lands in **no active subscription**.
> Under full-collection sync (M3) a client that can call `insert` is, by
> construction, subscribed to that collection — so every write is in-view, the
> confirming delta always arrives, and the completed direct upsert is harmless
> (it equals the synced value and clears on the next sync commit). The phantom
> only becomes reachable with **filtered subsets (M5/M6)**: inserting a row
> outside your filter.
>
> **Therefore the must-verify moves to M5**, where it is needed: does an empty
> `begin()/commit()` trigger the `state.ts:1167-1188` clear path against a
> completed direct transaction? If yes, the adapter issues a post-mutation
> empty sync commit to retire phantoms (and keep in-view overlays, which match
> synced state). If an empty transaction is short-circuited, the adapter
> instead emits a targeted idempotent synced `delete` for the out-of-view key.
> Tracked in M5.

### C3 — Server-side shaping requires `syncMode: 'on-demand'`

*Supersedes the default implied by ADR-0001 D5/D8.*

**Flaw.** Verified `collection/sync.ts:482-486`: `loadSubset` returns
immediately when `syncMode === 'eager'`, so a live query's `where` never reaches
the sync adapter under eager sync (it filters client-side after a full-table
sync). Electric's adapter likewise provides no `loadSubset` in eager mode
(`electric.ts:422-425`).

**Correction.** Two distinct mechanisms, split across milestones:
- **M5 — static collection-level `where`.** `doCollectionOptions({ where })`
  sends the predicate on the `sub` frame directly (not via `loadSubset`), so it
  works regardless of `syncMode`. A synchronous write-outside-filter preflight
  (`WriteOutsideSubError`) rejects writes whose row wouldn't match, *preventing*
  the out-of-filter phantom rather than cleaning it up. Because every accepted
  write is in-view, the no-subscription-match path is not exercised here.
- **M6 — dynamic `loadSubset` windows** (`syncMode: 'on-demand'`,
  orderBy/limit/cursor). Here a confirmed write can legitimately fall outside
  all currently-loaded windows (e.g. inserting outside the visible page), which
  the preflight cannot catch — so **the empty-commit must-verify (C2) is
  answered and applied in M6**, not M5. M6 must test a live query whose `where`
  reaches `loadSubset` (eager bypasses it — verified, sync.ts:482-486).

### C4 — Drop the before-image column from the base design

*Supersedes ADR-0001 D10/D12 before-image; **overrides the review**.*

The review argued a before-image (OLD row captured in the UPDATE trigger) is
*necessary* for stateless server-side move-out. It is **not**. The prior spike
performs correct move-in/move-out without it: for each changed key it emits to
each filtered sub either the **current row** (if it matches the predicate) or an
**idempotent synthetic delete** (if not), relying on the client treating a sync
`insert` as an upsert. Before-image only *optimizes* fan-out — it lets the DO
skip sending an idempotent delete to subs that never held the key.

**Correction.** The base design uses the spike's always-emit rule (no
before-image, no per-sub membership state) — simpler and lower-storage, which
also relieves the write-amplification concern the review raised against
before-image. Before-image becomes a deferred optimization, revisited only if
delete fan-out is measured as a real cost.

> **Verified in M5** (tests/tanstack-upsert-probe.test.ts): a sync
> `write({ type: 'update' })` for an absent key UPSERTS in @tanstack/db. So the
> server may emit the actual change op on move-in (no need to distinguish
> insert vs update, no before-image, no per-sub membership). The always-emit
> rule stands: matching live row -> its current state with the actual op;
> otherwise -> idempotent synthetic delete.

### C5 — Compaction and dedup GC must respect liveness

*Supersedes ADR-0001's retention section.*

**Flaw.** A retention floor of `MIN(seq)` after compaction ignores
connected-but-slow clients: one whose delivered cursor is below the floor would
miss tombstones / intermediate state if served only latest-op-per-key deltas.
The adopted prune primitive has no notion of connected cursors
(`db-sqlite-persistence-core/src/sqlite-core-adapter.ts:1777-1823`).

Separately, `_sync_seen_tx` dedup GC was wrongly tied to changelog retention:
the claim "an old retry implies an old read cursor" is false — a fully-current
client can retry an old `txId`.

**Correction.**

1. Track a per-connection delivered cursor. Before sending further deltas to a
   connection whose cursor `< retentionFloor`, force a `reset`. Gate compaction
   and tombstone GC by the minimum active delivered cursor, or explicitly reset
   laggards before advancing the floor past them.

   > **Refined by [ADR-0009](./0009-changelog-time-retention.md).** In the
   > as-built push architecture the changelog's only historical reader is
   > reconnect catch-up (live deltas are push-once and self-contained), so a
   > connected client never re-reads the log. 0009 takes **option (b)** — reset
   > a laggard when it reconnects below the floor — and drops the per-connection
   > delivered-cursor tracking (option a) as unnecessary here.
2. `_sync_seen_tx` dedup retention is **independent** of changelog retention,
   sized to the maximum client retry / outbox window (time-based). Commands with
   side effects store their result by `txId` for idempotent replay.

### C6 — Reinforced constraints (already in ADR-0001, made explicit)

- Mutation `execute` is typed **non-Promise** and **runtime-rejects** a returned
  thenable; async `authorize` runs **before** `transactionSync`
  (`execute` must complete synchronously inside the workerd transaction).
- `rowUpdateMode: 'partial'` is a **shallow top-level** merge
  (`state.ts:993-1000`): the wire carries top-level-column partials only; nested
  JSON columns are replaced whole, never partially patched.

## Consequences

- The "one cursor" architecture survives review; the "no barrier at all" framing
  is corrected to "one cursor + a scoped per-connection barrier."
- M3, M5, M6, M7 task definitions are updated to carry these corrections and
  their must-verify items.
- Two correctness checks are explicitly deferred into the milestone where they
  can be settled empirically (M3 empty-commit clear path; M5 insert-as-upsert)
  rather than assumed now.
