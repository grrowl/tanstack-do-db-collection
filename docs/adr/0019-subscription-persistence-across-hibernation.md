# ADR-0019: Subscriptions persist in SQLite and restore on hibernation wake

**Status**: Accepted
**Date**: 2026-07-27
**Issue**: field report against 0.5.1 (downstream); also resolves [#29](https://github.com/grrowl/tanstack-durable-object-sync/issues/29) (real-eviction wake test)

## Context

ADR-0001 D13 chose hibernation-native sockets (`acceptWebSocket`, identity via
`serializeAttachment`); M7 added reconnect catch-up: after a socket **close**,
the client transport re-sends its `sub` frames with `since` cursors
(`resubscribeAll`, `transport.ts`). The unstated assumption: subscription loss
and socket loss coincide.

Hibernation breaks that assumption by design. An eviction tears down instance
memory — including `SubscriptionRegistry` (`subscriptions.ts`), a
`WeakMap<WebSocket, …>` whose own header said "lost on hibernation and
re-established by the client's `resub` on reconnect" — while hibernatable
sockets **survive**. The client never sees a close, so its only re-subscribe
trigger never fires. After any eviction:

- the wake restore (`mixin.ts` constructor) re-populates the broadcaster's
  socket set from `getWebSockets`, but the subscription registry starts empty;
- mutations still confirm — `committed` flows normally — while
  `drainAndBroadcast` fans out to nobody;
- the client's live queries go **silently dead on a still-open socket**, until
  an unrelated network drop forces a reconnect.

Field-reported against 0.5.1. Pinned by `tests/hibernation.test.ts` under a
**real eviction** (`evictDurableObject`, default `webSockets: "hibernate"` —
production semantics), in both field shapes: eviction after successful
broadcasts, and eviction before the first-ever broadcast (which additionally
crosses the never-drained `_sync_meta` state with the restore). This is the
eviction-based wake test issue #29 asked for; the toolchain blocker recorded
in ADR-0015's Consequences ("verified by code-reading only", pinned
`vitest-pool-workers@0.12.21`) was lifted by the vitest 4 migration (PR #34),
and that note is superseded for this path.

## Decisions

### D0: Subscriptions are durable per-socket state

The resub-on-reconnect design stays — it is correct for actual reconnects.
This ADR adds the missing half: on a wake **without** a reconnect, the server
restores what only the server lost. No protocol change, no client change; an
unmodified 0.5.1 client against a fixed server recovers fully. Any design
that needed a client upgrade to stop losing data was rejected on that ground
alone (see rejected alternatives).

### D1: Storage — `_sync_subs`, written through, codec-encoded predicate

```sql
CREATE TABLE IF NOT EXISTS _sync_subs (
  socket_id TEXT NOT NULL,
  sub_id    TEXT NOT NULL,
  collection TEXT NOT NULL,
  where_ir  TEXT,              -- tagged-value codec; NULL = unfiltered
  PRIMARY KEY (socket_id, sub_id)
)
```

Created in `initSchema` (idempotent), so an already-deployed DO picks it up on
its first wake under the new build — the same in-place pattern as
`_sync_seen_tx.error_code` (ADR-0014 era migration). The predicate IR is
stored with the wire **value codec** (`encode`/`decode`, as
`dedup.ts#encodeResult` already does for command results), not `JSON.stringify`
— predicate literals may carry tagged values (dates, bytes) that JSON would
corrupt; the codec round-trips them exactly.

Write-through happens at the mixin, keyed to the same events that mutate the
in-memory registry: `#handleSub` success upserts (`INSERT OR REPLACE` — a
re-sub on an existing `subId` replaces, matching ADR-0012's cap semantics),
`unsub` deletes the row, `webSocketClose`/`webSocketError` delete the socket's
rows. `SubscriptionRegistry` itself stays a pure in-memory index: its
predicate-floor behavior (ADR-0013) and unit tests are untouched.

### D2: Socket identity across eviction — a second tag, not the attachment

Rows must be re-associated with *surviving sockets* on wake. The socket id is
carried as a second tag at accept: `acceptWebSocket(server, [SYNC_TAG,
SOCKET_ID_TAG_PREFIX + id])`. Tags are the only per-socket datum that both
survives hibernation and stays out of the author's attachment; budget is
comfortable (Cloudflare allows 10 tags × 256 bytes; this uses 2).

**Rejected alternative — the socket attachment** (the pattern a downstream
consumer uses for its own fixed-shape step subscriptions; also Cloudflare's
documented default for per-socket wake state). Steelmanned honestly (this
section was corrected on the simplicity-adversary round):

- Its genuine advantages, conceded: the attachment dies with the socket —
  no id tag, no orphan rows, no sweep; and it is the platform primitive
  built for exactly this.
- **Contract stability is the decisive rejection.** The attachment is the
  **author's** `TUser` claims (ADR-0001 D13; handlers read it raw via
  `deserializeAttachment`, and ADR-0015's second discriminator — the `__pk`
  guard — inspects that exact shape). A library envelope changes a documented
  contract, needs shape-sniffing to coexist with pre-envelope sockets
  surviving an upgrade wake, and every future reader inherits the ambiguity.
- **No honest aggregate bound.** `serializeAttachment` caps at 16,384 bytes
  (current Cloudflare docs; an earlier revision of this ADR said 2 KiB —
  stale). Most small deployments would fit, but `maxSubsPerSocket` is 256
  (ADR-0012) with unbounded predicate IR, so sufficiently complex apps hit a
  cliff where sub #N is rejected *because of its siblings' sizes* — failure
  dependent on unrelated state, for bookkeeping reasons. Reject-don't-
  degrade cuts both ways. A size-triggered SQL fallback would be two
  persistence designs instead of one. Re-serializing the whole set per
  `sub`/`unsub` is also O(total state) write amplification.
- Cloudflare's own guidance for state beyond the cap is "use the Storage API
  and store the corresponding key as an attachment" — i.e. **this ADR's
  architecture is the platform's documented escalation path**; the key rides
  a tag instead of the attachment purely for the contract reason above.

**Rejected alternative — close every sync socket on wake** (the null
hypothesis; ~3 constructor lines would replace this entire ADR, with the
battle-tested reconnect + `resubscribeAll` + `since` path doing all
recovery, and the client staying sole owner of subscription truth):
hibernation follows ~10 seconds of idleness (Cloudflare lifecycle docs), so
this converts *ordinary application quiet* into reconnect waves — every wake
closes every connected client, each reconnect re-upgrades, re-auths, re-subs
and re-snapshots or catches up; a once-a-minute-burst workload would
reconnect each client ~1,440×/day, and the hibernation API's entire purpose
(cheap stable sockets across idle) is nullified by keeping sockets alive
with auto-pong only to slam them shut on wake. Correct but regressive.

### D3: Restore point — inside `registerSync`, before anything can dispatch

Restore runs at the end of `#registerSync`, after `initSchema`: for each
restored socket, read its id tag, load its rows, and `#subs.add` each. The
before-anything-dispatches guarantee is **conditional on the author's
documented obligation** (`registerSync` in the constructor's
`blockConcurrencyWhile`); a host that registers late fails loud at
`#registry` on the first frame rather than racing a half-restored registry.

Two failure paths, corrected on adversarial review (both first-draft versions
sent `reset`, which the client answers by **truncating without
re-subscribing** — a reset here would strand the query dead while its single
cursor kept advancing past unseen changes):

- **Predicate no longer compiles** (`UnsupportedPredicateError` — a version
  change narrowed the evaluator floor): the socket is **closed** with a
  non-terminal code (1011) after its durable rows are dropped. The client's
  reconnect machinery re-subscribes everything recoverable with `since`
  cursors, and the bad predicate flows through `#handleSub`'s pre-existing,
  app-visible rejection. One bad predicate costs the socket's other subs a
  reconnect round trip — acceptable for a should-never-happen skew path;
  fail-loud beats limping.
- **Collection absent from the compiled schema** (an older build persisted
  the row, or a mid-life re-registration removed the collection): a
  post-restore **reconcile pass** removes the sub (memory + durable) and
  sends `reset` — which IS correct here: the collection is gone, truncation
  is the terminal state, there is nothing to re-subscribe to. Without this,
  the restored sub is a zombie no drain will ever service. The reconcile pass
  runs on every `registerSync`, so a mid-life re-registration that drops a
  collection sheds its live subs the same way.

### D4: No new timers; orphan hygiene rides compaction

A socket that dies without `webSocketClose` ever firing (hard termination,
`webSockets: "close"` eviction of a crashed instance) leaves rows behind.
They are swept in `#maybeCompact`'s existing deferred housekeeping: delete
`_sync_subs` rows whose `socket_id` matches no live socket's id tag. Nothing
polls; the sweep rides work the DO was already awake to do. The no-idle-timer
invariant (ADR-0006) is preserved end to end: persistence writes ride frame
handlers, restore rides construction, hygiene rides compaction.

**Rejected alternative — wake-time server-prompted resub** (a new server
frame asking surviving sockets to re-send `sub`s): requires a client upgrade
in lockstep (violates D0); turns every wake into per-socket
snapshot/catch-up round trips, which production eviction cadence would
amplify; and grows the wire protocol to route around what is purely
server-side bookkeeping. Recorded for the future, not planned: a
capability-negotiated protocol resub could in principle retire `_sync_subs`
once pre-protocol clients leave the support window (the simplicity round's
strongest long-term candidate) — but even at that end state, per-wake round
trips lose to a free local restore, so it is a machinery-deletion play, not
a runtime win.

## Consequences

- Live queries survive eviction on still-open sockets. Both field shapes are
  pinned by `tests/hibernation.test.ts` under real evictions — the suite's
  first actual eviction-and-reconstruct coverage (closes the gap ADR-0015
  documented; resolves #29). The test premise-guards the harness (asserts the
  socket did **not** close on eviction) so a future change in pool-workers
  semantics fails loudly instead of greening the pin for the wrong reason.
- Sockets accepted by a **pre-fix** build that survive an in-place upgrade
  wake carry no id tag and no rows: they behave exactly as before the fix
  (dead until reconnect). Durability begins with sockets accepted by the
  fixed build. A one-release sharp edge — documented, not worked around.
- `sub`/`unsub` each cost one extra SQLite write (write-through), bracketed
  by the snapshot they already trigger. Negligible.
- `_sync_subs` rows are socket ephemera, not data: `unsub` and close delete
  them, re-subs replace in place, and compaction sweeps orphans — so the
  table holds exactly the live subscriptions of currently-open sockets, and
  is empty when nobody is connected. **No TTL, deliberately**: the sweep is
  liveness-keyed, not time-keyed — a TTL shorter than a long-idle-but-alive
  socket would delete live subscriptions (re-creating this ADR's bug), and a
  longer one is pointless.
- `subscriptions.ts`'s header comment — the stale assumption itself — is
  rewritten to point here.
- **Restored subs are grandfathered against a lowered `maxSubsPerSocket`**
  (codex review): they passed the cap at accept time; enforcing a newly
  lowered cap at restore would kill arbitrary live queries
  nondeterministically. New subs enforce the current cap as before.
- **Write-through is not failure-atomic, deliberately** (codex review): a
  SQLite failure between the in-memory mutation and the durable write is
  handled by the platform's failure model — a failed Durable Object storage
  write gates output and resets the instance, so a memory/durable divergence
  cannot outlive the instance that created it; the next construction restores
  from the durable truth. No rollback code rides the happy path.
- **Residual risk, out of scope**: deltas enqueued in the egress coalescer die
  with instance memory *after* the drain cursor has advanced. In production
  this window is closed by construction — an armed flush timer prevents
  hibernation (broadcast.ts's documented design), so a DO with pending
  coalesced deltas cannot be evicted. Only a test-harness forced eviction can
  land inside the ~tickMs window; the hibernation tests await delta delivery
  before evicting. Flagged rather than engineered around.
