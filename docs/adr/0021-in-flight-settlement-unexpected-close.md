# 0021 — In-flight mut/call settlement across an unexpected close

**Status:** Accepted. Fixes issue #39. Builds on ADR-0020 (connect contract),
ADR-0016 (reconnect policy), ADR-0011 (stale-socket receipt semantics), and the
dedup table (ADR-0002 C5, issue #21). Amends none.

## Context

`sendMut`/`sendCall` resolve when the server's `committed`/`rejected` for their
`txId` arrives on the same ordered stream (ADR-0002 C1). Each pending waiter is
armed with a single confirmation timeout (`timeoutMs`, default 5 s). Until this
ADR, the UNEXPECTED-close listener never touched `pendingTx`: only the app's own
`close()` cleared it. So a `mut`/`call` in flight when the socket dropped sat,
fully decoupled from the close event, until its own timeout fired.

The sharp edge is server-side ordering. In `#handleMut`/`#handleCall`, `recordTx`
(the durable dedup entry) and the delta broadcast run **before** the `committed`
send, with no try/catch around the send. A drop in that window means the write
is already durably committed while the client's confirmation times out and
**rolls back a write that succeeded**. The reconnect's resubscribe later
converges the row back, but the app saw commit → rollback → reappear, and the
`mut` promise rejected — with a *generic timeout* indistinguishable from a
server that simply went quiet — for a write that landed.

Observed 2026-07-29 against 0.6.0 while wiring terminal-close handling in a
shipping product; verified still present on `main` post-#36. ADR-0011's lift
fixed one adjacent edge (a stale socket's *late* receipt now settles its
waiter), but the drop-to-timeout path remained.

## Decision

Two complementary mechanisms, primary + fallback. Both hang off the existing
unexpected-close handler and the existing reconnect path — no new timers, no
idle work (ADR-0016 hibernation invariant preserved).

### 1. Hold-and-replay (primary) — reconcile through the dedup table

On an unexpected close while subscriptions are active (the same condition under
which ADR-0016 reconnects), the transport does **not** reject the in-flight
`mut`/`call`. It PARKS each one: the generic-timeout timer is swapped for a
bounded `ConnectionLostError` timer, and the encoded frame is retained. When the
reconnect installs a fresh socket, `resendParkedTx()` runs alongside
`resubscribeAll()` and **replays every parked frame**. The server's dedup table
answers each replayed `txId` with its true recorded outcome (`#replayReceipt`):

- committed before the drop → the client receives `committed` and the mutation
  **resolves** — no rollback, the exact case issue #39's second acceptance
  bullet demands;
- rejected before the drop → the client receives `rejected` and the mutation
  rejects with the true `MutationRejectedError` (and its code, issue #21);
- never received by the server (dropped in transit) → no dedup entry, so the
  replay simply EXECUTES now, once. Exactly-once holds either way — this is
  precisely what client-generated txIds + dedup were built for (ADR-0002 C5).

This is "retry-through-dedup": the mutation the app already issued is the retry,
and the server's dedup makes the retry safe.

### 2. Typed fallback — `ConnectionLostError` when no replay can answer

A new exported error, `ConnectionLostError` (`instanceof`-distinct from
`MutationRejectedError`, `TransportClosedError`, and the generic timeout
`Error`), settles the waiter when reconnect cannot resolve it:

- **Terminal close** (ADR-0016 policy returns `null`, e.g. a 4xxx auth
  rejection): no reconnect will ever replay, so the parked waiters are failed
  immediately after `onClosed`.
- **No active subscriptions**: ADR-0016 gates reconnect on live subscriptions
  (to preserve hibernation), so a mutation-only transport that drops can never
  replay. Its in-flight `mut`/`call` settle promptly with the typed error — we
  do **not** bypass ADR-0016 to reconnect for a replay. This includes the
  *transition* case: if the last subscription is `unsubscribe`d while a reconnect
  is pending with parked txs, `unsubscribe` cancels a pending reconnect timer,
  clears the reconnecting flag (so a handshake already in flight installs without
  replaying), and settles the parked txs typed — a reconnect with zero subs must
  never replay (codex review). A dial already parked on `open()` is not
  force-aborted (that would entangle ADR-0020's epoch/revival machinery); at worst
  it installs an idle, demand-less socket the app disposes via `close()`, exactly
  as an initial connect can.
- **Replay too slow**: the parked timer bounds the hold to `timeoutMs` from the
  drop. If the reconnect+replay hasn't answered by then, the app gets the typed
  error rather than an unbounded hang.

The type is the contract. An app catches `ConnectionLostError` to keep its
optimistic overlay pending (the outcome is genuinely unknown) instead of
flashing a rollback; the resubscribe catch-up then converges the row.

### Why this shape (a **and** b, not a-only)

Issue #39 sketched two options: (a) settle immediately with a typed
unknown-outcome error, or (b) reconcile after reconnect via dedup replay. We
chose **(b) as primary with (a) as the bounded fallback** rather than (a) alone:

- **(a)-only cannot report the true outcome.** It settles every drop as
  "unknown", so an app holding its overlay on that signal would be *wrong* for a
  write the server actually *rejected* (validation failing exactly as the socket
  dropped) — the overlay would linger for a write that never landed, and no
  catch-up delta ever arrives to correct it. (b) resolves committed-vs-rejected
  from the authoritative record.
- **(b) matches the ethos** (truthfulness, correctness over speed on load-bearing
  paths): the app gets ground truth whenever a reconnect is possible, and the
  typed error only where truth is genuinely unreachable.
- **(a) is still needed** because (b) is not always possible (terminal close, no
  subs) and must be bounded (the parked timer) — and because the *type* is what
  lets an app distinguish "unknown" from a real rejection or a quiet server.

## Consequences & honesty

- **New export:** `ConnectionLostError` (with optional `txId`). Settles an
  in-flight `mut`/`call` only on an *unexpected* drop; never on an app `close()`
  (that keeps its existing generic `Error("transport closed")` — the app knows
  it closed) and never on a socket that stays open (generic confirmation
  timeout, unchanged).
- **Dedup-window honesty — a deployment invariant, not a hope.** A replayed
  `txId` only resolves the *true* outcome while its dedup entry survives
  (`dedupRetentionMs`, default 1 h); outside that window the server sees it as a
  brand-new frame and re-executes it. This is not new to this ADR — it is the
  standing dedup contract (ADR-0002 C5): *any* client retry/outbox replay after
  retention re-executes, which is exactly why C5 sizes retention to "the maximum
  client retry/outbox window". This ADR's automatic replay is bounded to
  `timeoutMs` from the drop (the parked timer removes the waiter and stops any
  resend after that), so the operative invariant is simply **`timeoutMs` ≤
  `dedupRetentionMs`** — trivially satisfied by the defaults (5 s ≪ 1 h, three
  orders of magnitude). A deployment that raises `timeoutMs` above the server's
  dedup retention breaks the same at-most-once guarantee any retrying client
  already depends on; the client cannot see `dedupRetentionMs` to enforce it, so
  it is documented as a co-configuration constraint (codex review). Fully closing
  it would need a server frame distinguishing "expired/unknown txId" from
  "genuinely new" — a protocol change out of scope here and unnecessary at any
  sane configuration.
- **Flapping is bounded.** A waiter already parked keeps the *first* drop's timer
  across subsequent drops, so a flapping connection cannot extend the hold past
  `timeoutMs` from the first drop.
- **One waiter per txId.** A concurrent reuse of an in-flight `txId` is rejected
  loud (`MutationRejectedError` `DUPLICATE_TXID`) rather than allowed to overwrite
  the first waiter — otherwise a timer's delete-by-key could evict a *different*
  waiter and drop its receipt, and parking would carry the ambiguity across a
  reconnect (codex review). A *sequential* retry after settlement is unaffected
  (the prior entry is gone) — that is the intended retry-through-dedup path.
- **ADR-0011 not regressed.** A stale socket's late receipt still settles its
  waiter (receipt dispatch is `stale`-agnostic; only the cursor is guarded). If a
  buffered `committed` arrives before the `close` event, it removes the waiter
  from `pendingTx` first, so parking only ever catches genuinely-unsettled
  txIds. Double-settle is impossible: the receipt handler and both timers guard
  on the map entry, and whichever fires first deletes it.
- **No new timers when idle.** Parking reuses the per-waiter timer slot; replay
  rides the existing reconnect. Nothing polls while the DO is quiet.

## Alternatives considered

- **(a)-only (reject-with-unknown on every drop).** Simpler, prompt, but discards
  the true outcome and shifts a rollback-vs-hold decision onto every app for a
  case the dedup table can answer authoritatively. Rejected — see above.
- **Reconnect a subscription-less transport just to replay.** Would bypass
  ADR-0016's hibernation gate for a marginal case; the typed fallback covers it
  honestly instead.
- **Re-arm a fresh generic timeout on reconnect** (total wait up to 2×timeoutMs).
  Rejected: the parked `ConnectionLostError` timer already bounds the hold and
  carries the correct, distinguishable meaning ("connection was lost").
