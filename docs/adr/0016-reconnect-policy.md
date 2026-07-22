# 0016 — Transport reconnect policy: injectable delay function, terminal 4xxx, jittered backoff

**Status:** Accepted. Fixes issues #25 and #26.

## Context

`WebSocketTransport` auto-reconnects from two sites — the socket close handler
and the connect-failure catch — and both were blind to *why* the connection
went away: a fixed `reconnectDelayMs` (default 250 ms) rescheduled forever,
never inspecting `CloseEvent.code`. Two real failures fall out:

- **Terminal rejections retry forever (#25).** The DO's only client-readable
  auth-rejection pattern is accept-then-close with an application code (e.g.
  4403) — a failed HTTP handshake status is invisible to a browser WebSocket.
  The transport turned exactly that deliberate rejection into an infinite
  250 ms retry loop against the DO, with no way for the app to even observe
  the code.
- **Fixed-interval thundering herd (#26).** During a DO outage every client
  retries on the same fixed interval indefinitely; when the DO returns, they
  all arrive in lockstep. The option's own doc comment admitted it:
  "production should layer exponential backoff + jitter."

## Decision

### 1. The policy is a function, not a bag of flags

One option subsumes the whole space:

```ts
reconnectDelay?: number | ((attempt: number, closeCode?: number, closeReason?: string) => number | null)
```

The function form is called once per attempt with a 1-based counter; it
returns the delay in ms before the next attempt, or `null` to stop
reconnecting. The alternative — config knobs (`maxRetries`, `backoffFactor`,
`capMs`, `terminalCodes`, …) — grows a flag per policy nuance and still can't
express app-specific shapes ("retry 4408 rate-limit closes after the reason's
hinted delay"). A function is the minimal complete surface; the knobs become a
*default implementation* of it. The number form is the one sanctioned
shorthand: the default policy's base delay. It **replaces** `reconnectDelayMs`
(removed — pre-1.0, one option per concern; a fixed delay is spelled
`() => ms`).

`closeCode` stays `number` and `closeReason` stays `string` deliberately: the
4xxx range is *private-use* vocabulary that each application defines (this
library emits no close codes of its own), the reason is app-authored prose,
and a closed union at a network boundary is a promise the wire doesn't keep.
The closed set in this signature is the *return* — `number | null`, the
policy's actual decision.

Both reconnect sites route through one `scheduleReconnect(code?, reason?)`.
The connect-failure path has no close frame, so the policy (and `onClosed`)
see `undefined` there — honest, and distinguishable from a server-sent close.

### 2. Default: application closes (4000-4999) are terminal

The 4xxx range is reserved for application meaning: the server closed *on
purpose* and said why. Retrying a deliberate rejection cannot succeed and is
indistinguishable from an attack pattern from the DO's perspective. The
default policy returns `null` for any 4000-4999 code; everything else —
1000/1001 (hibernation, redeploy), 1006 (network drop), or a failed open —
stays transient and retries. Apps that use a 4xxx code as "please reconnect
later" opt out with a custom policy.

A terminal stop must be observable, or an auth rejection looks like a silent
hang. New hook:

```ts
onClosed?: (code: number | undefined, reason: string | undefined) => void
```

Called exactly when the policy returns `null` — never for an intentional
`close()`, never while retries continue. This replaces the previous workaround
(attaching a close listener inside a custom `open()` and calling
`transport.close()` before the transport's own handler ran — a
listener-ordering hack).

### 3. Default delay: capped exponential backoff, full jitter, reset on open

`defaultReconnectDelay(baseMs, capMs = 30_000)` returns a uniform delay in
`[0, min(cap, base·2^(attempt−1))]` — AWS-style *full* jitter, which
desynchronizes a fleet of clients that all lost the same DO at the same
moment. The cap never drops below `baseMs`, so a caller who set
`reconnectDelay: 60_000` is not silently truncated. The attempt counter
resets on a successful *open* (the demand-driven reconnect window makes
"first server frame" needlessly stricter — an open that immediately drops
re-enters backoff on the very next close anyway, one attempt later).

### Invariants preserved

- **`reconnecting` is set at SCHEDULING time**, not in the timer — the
  demand-driven connect window (a mutation racing the timer) must run the
  resubscribe path (pre-existing bug, ADR-0011 grill). `scheduleReconnect`
  sets it before consulting the policy; a *terminal* close also sets it, so an
  app-driven `connect()` after re-auth still resubscribes from the cursor.
- **No idle timers.** At most one reconnect timer exists, and only while a
  retry is pending: the handle is tracked and cancelled on a successful open,
  on a terminal stop, on `close()`, and when a newer drop supersedes it.
  Without this (adversarial-review catch), a stale timer from a transient
  drop could fire after a later terminal 4xxx close and resurrect the
  connection — retrying past "stop" and double-firing `onClosed`.

## Alternatives considered

- **Flag-based config** (`maxRetries`/`backoffFactor`/`terminalCodes`). Rejected
  above — a function is smaller and complete.
- **Rejecting `connect()` for a post-handshake 4xxx close.** The close arrives
  *after* `open()` resolved, so connect() has already succeeded by the time
  the code is known; faking a rejection would need a "wait for first frame"
  handshake phase. `onClosed` reports the same fact without restructuring the
  connect path.
- **Terminal = give up on any close code the app didn't allowlist.** Inverts
  the safe default: transient network drops (1006) are the overwhelmingly
  common case and must retry out of the box.

## Consequences

- **Behavior change:** a 4xxx server close no longer auto-reconnects. Code
  that relied on retry-after-4xxx must pass a custom `reconnectDelay`.
- **Breaking (pre-1.0):** `reconnectDelayMs: n` becomes `reconnectDelay: n`
  (same meaning: the default policy's base).
- Retry delays are now randomized and growing; tests that need a fast
  deterministic retry inject `reconnectDelay: () => smallMs` (the migrated
  `reconnectDelay: 20` fixtures stay valid — jitter only shortens them; a
  fixture that needs the timer to NEVER fire uses a fixed policy, since a
  jittered 60 s base can land near 0).
- The `open()`-listener workaround for observing close codes is obsolete;
  `onClosed` is the supported surface.
