# 0020 — connect() never resolves disconnected; open() is abortable via AbortSignal

**Status:** Accepted. Fixes issues #37 and #38. Amends ADR-0016 (reconnect
policy) and ADR-0011's `Transport` seam; does not supersede either.

## Context

`WebSocketTransport.connect()` and `close()` share one race-prone window: the
in-flight `open()` between "start dialing" and "socket adopted". Two real
defects lived there, both observed against 0.6.0 in a shipping product and
verified on `main` post-#36.

### #37 — connect() could resolve having adopted no socket

The close-epoch guard (ADR-0016's third race guard) discards a socket whose
`open()` was in flight when `close()` landed, then **`return`ed** — resolving
the connect promise with `this.ws === null`. `subscribe`/`sendMut`/`fetch` all
`await connect()` and then send; the send threw `transport not connected`, the
subscription registered with no frame ever on the wire, and the collection's
`preload()` promise floated as an unhandled rejection (measured: 9 per cold
page load). The natural React shape — `createCollection(...)` + `preload()` in
one render pass, StrictMode's mount→unmount→remount — hit it deterministically.

Two residues shared the same surface:

- **`intentionallyClosed` latched with no reset.** Set `true` by `close()` and
  never cleared — not even by a later `connect()`. A transport revived after
  `close()` (connection pools reuse instances) lost auto-reconnect forever
  (measured: a 1006 on a revived transport never re-dialed).
- **The latch also silenced `onClosed`.** The unexpected-close handler returns
  early when `intentionallyClosed` is set, skipping both `scheduleReconnect`
  *and* the terminal `onClosed` — so an app wiring `onClosed` to learn about a
  server's deliberate 4xxx close heard nothing on any revived transport.

### #38 — close() could not abort an in-flight open()

`connect()` assigns `this.ws` only *after* the handshake resolves; `close()`
disposes through that field (`this.ws?.close()`). While `open()` is in flight
`this.ws` is `null`, so `close()` is a no-op on the socket. The only path that
disposed the in-flight socket was the close-epoch discard — which runs **only
when `open()` resolves**. A handshake that is slow or never completes (a dev
proxy that doesn't upgrade, a hung upgrade, a half-open LB connection) leaked a
socket stuck in CONNECTING forever. Deterministic on any dispose-before-open;
benign in prod (handshakes complete in ms), user-visible in dev and a real
leak wherever a handshake can hang.

## Decision

### 1. `connect()` never resolves disconnected

`await connect()` resolves only with a live socket adopted, and never resolves
with `this.ws === null`. At the close-epoch discard it now either:

- **re-dials** — if a later `connect()` revived the transport (see §3), it
  defers to whichever dial now owns the transport (`return this.connect()`),
  so the original awaiter rides the socket that dial installs; or
- **rejects** `TransportClosedError` — a new typed, catchable error — if the
  transport was closed and *stays* closed.

This is the contract issue #37 asked for: fix `connect()`, not add a queue in
`subscribe`. `subscribe`/`sendMut`/`fetch` inherit it for free — a racing
`subscribe` now either lands its frame on the next connection or rejects typed;
`do-collection` already routes that rejection to `markError`, so `preload()`
fails loud (and a retried `preload()` / the policy-driven reconnect recovers)
rather than resolving a silently-empty collection.

Only the dial that still **owns** `connectPromise` drives failure recovery (the
`catch` guards on `this.connectPromise === p`): a superseded dial that re-dialed
via the epoch branch must not null a newer attempt's promise or double-schedule
its reconnect.

### 2. `open()` takes an `AbortSignal`; `close()` aborts the in-flight handshake

The pluggable opener's contract widens (backward-compatibly):

```ts
open?: (signal?: AbortSignal) => WebSocketLike | Promise<WebSocketLike>
```

The transport creates one `AbortController` per dial, holds it in `openAbort`
for exactly as long as a dial is parked on `await open()`, and `close()` aborts
it. The default browser `open()` honours the signal: on abort it closes its
still-CONNECTING socket and rejects `TransportClosedError`. This is the one
path that disposes a handshake that *never resolves* — the acceptance criterion
the epoch discard could not meet.

**Backward compatibility is preserved.** An existing `open()` that takes no
argument (or ignores the signal) still works: `close()` cannot abort it
mid-flight, but the epoch guard still closes the orphan the instant `open()`
resolves — the pre-fix behaviour, minus the never-resolves leak. A custom
opener that wants dispose-before-open to free its socket immediately (including
the never-resolves case) **must** honour the signal. This is the *only*
residual gap and it is opt-in: the default opener closes it, and a custom
opener closes it in the ~15 lines the reporting consumer already wrote at this
seam. `openAbort` tracks the current in-flight dial only; a pathological
close→connect→close chain can leave an earlier signal-ignoring dial's socket to
the epoch discard, same as today.

`openAbort` is released in a `finally` the instant `open()` settles — before
the epoch check and install — so a `close()` arriving during install-processing
never aborts the socket about to be adopted; it bumps `closeEpoch` instead, and
the epoch guard handles it.

### 3. Dialing clears the intentional-close latch

`connect()` sets `intentionallyClosed = false` at its head. Dialing is the
clearest possible statement of intent to be connected, so a revived transport
regains **both** auto-reconnect and `onClosed` delivery — the two residues fall
out of one line. This is orthogonal to the close-epoch guard: the epoch still
discards a socket opened before *this* dial's `close()`, while the latch governs
whether a *future* unexpected drop reconnects.

**Behaviour change (deliberate).** `close()` is no longer permanent across a
later `connect()`. ADR-0016's `intentionallyClosed` still governs auto-reconnect
suppression; only its clearing point moves — from "never" to "the next
`connect()`". The `reconnect-policy` test that pinned the old permanence is
rewritten to pin revival (issue #37's acceptance).

## Alternatives considered

- **A re-subscribe queue in `subscribe`.** Rejected by #37 itself: the defect
  is `connect()`'s contract, and `sendMut`/`fetch` share it. Fixing one caller
  leaves the others broken.
- **`open()` returns `{ socket, whenOpen }`** so the transport owns the handle
  from creation. Strictly more capable, but a *breaking* signature change for
  every existing opener; an `AbortSignal` is additive and the default opener
  implements the whole contract. If a future need forces transport-owned socket
  creation, that is a separate ADR.
- **The full generation-counter unification** ADR-0016 deferred (collapse the
  tracked timer, the `this.ws !== ws` close guard, and the close-epoch check
  into one counter). Not adopted: it does not *fall out* of this fix, the diff
  is already surgical, and the existing race tests (its intended harness) stay
  green. The deferral stands; unify when the fragility actually bites.

## Consequences

- **New export:** `TransportClosedError`. `connect()`/`subscribe`/`sendMut`/
  `fetch` can reject with it around a close race — catchable and distinct from
  `MutationRejectedError`.
- **`open` gains an optional `AbortSignal`.** Additive; existing openers compile
  and run unchanged. Custom openers should honour it to be abortable-before-open.
- **`close()` is revivable.** A later `connect()` restores auto-reconnect and
  `onClosed`. Code that relied on `close()` being permanently terminal must not
  re-`connect()` the same instance (or must track terminality itself).
- No new timers, no idle work: `openAbort` exists only while a dial is parked on
  `open()`; the no-idle-timers invariant (ADR-0016) is preserved.

## Out of scope

Issue #39 (in-flight `pendingTx` settlement on an unexpected close) is a
separate follow-up and is untouched here.
