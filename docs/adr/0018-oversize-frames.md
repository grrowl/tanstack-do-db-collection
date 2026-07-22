# ADR-0018: Oversize frames — client pre-send guard is the rejection surface; outbound is warn-only

**Status**: Accepted
**Date**: 2026-07-22
**Issue**: [#28](https://github.com/grrowl/tanstack-durable-object-sync/issues/28) (part b; column projection, part a, remains open)

## Context

ADR-0012's `maxFrameBytes` guard drops an oversize inbound frame with a
server-side `console.error` and **no reply**. For a hostile frame that is the
right stance; for an honest client whose mutation happens to carry a large
value (a multi-MB TEXT column, say) it is the worst failure mode we have: the
`mut` vanishes, the client's `sendMut` waits out its full confirmation
timeout, and the optimistic overlay rolls back only via the generic timeout
error — untyped, uncoded, seconds late.

Outbound had no size awareness at all: `drainAndBroadcast` hydrates full rows
(`SELECT *`), so a small-field UPDATE on a row holding a large column re-sends
the whole row per tick per subscriber, silently.

The load-bearing asymmetry (verified in workerd; documented in Cloudflare's
WebSocket limits):

- **Inbound** (client → DO) WebSocket messages are capped at ~1 MiB **at the
  edge** in production. An oversize client frame may never reach the DO at
  all — workerd may refuse the send or close the socket before
  `webSocketMessage` runs. A server-side rejection therefore *cannot* be the
  primary surface: there may be no server-side anything.
- **Outbound** (DO → client) is not capped the same way; a >1 MiB frame is
  delivered whole (pinned by `tests/frame-limits.test.ts`, snapshot and live
  delta).

## Decisions

### D0: The limits are infrastructure facts, not application preferences

Cloudflare's ~1 MiB inbound edge cap is a **fact about the one infrastructure
this library supports** — and both wire endpoints ship in this package. Facts
don't get knobs. The thresholds below are therefore module-level **constants**
(`MAX_FRAME_BYTES` in the transport, `WARN_OUTBOUND_FRAME_BYTES` in the
mixin), both `1_048_576`. If Cloudflare changes the cap, the constants change
with an ADR note.

**Rejected alternative — configurable limits** (a `maxFrameBytes` transport
option and a `warnOutboundFrameBytes: number | null` protected tunable, in an
earlier revision of this branch): rejected on review. Since we control both
sides and support exactly one infra, a client-side knob could only be set
*below* the cap (pointless — the server and edge still enforce the fact) or
*above* it (a lie — the guard would wave through frames the edge then kills,
reintroducing exactly the silent-timeout failure this ADR removes). The
warn-threshold knob likewise had no honest setting other than the cap itself;
`null`-disabling it only hid the one production breadcrumb.

The overall shape: **inbound = enforced constant; outbound = deliberately
unenforced** (breaking a correct broadcast to save bandwidth would invert the
failure hierarchy), with the D3 warning as the only production breadcrumb.

### D1: Client pre-send guard — the reliable half

Before sending a `mut` or `call`, the transport encodes the frame once,
checks the encoded size against `MAX_FRAME_BYTES`, and on breach rejects
**locally and immediately** with the existing typed surface:
`MutationRejectedError` with code `"FRAME_TOO_LARGE"`. No round trip, no
timeout; the optimistic overlay rolls back promptly through TanStack's normal
rejected-mutation path. The encoded bytes are reused for the actual send, so
the guard costs no extra encode.

Size is measured on the encoded wire value: `byteLength` for the binary
codec, UTF-8 bytes (not UTF-16 code units) for the JSON debug codec's string
output — `.length` would undercount non-ASCII payloads and let them slip
past the guard only to die at the edge (codex review).

Only `mut`/`call` are guarded — they are the frames that carry client data
and have a typed rejection surface. `sub`/`fetch`/`unsub` frames are
structurally small in practice; a pathological `where` predicate could in
principle also exceed the cap (codex raised this), but that exposure predates
this ADR, carries no row data, and has no typed rejection surface to reuse —
a bounded-predicate rule would be its own decision. Explicitly out of scope
here.

### D2: Server drop stays a drop — with the reason on record

The ADR-0012 inbound guard is unchanged in behavior: oversize → drop +
`console.error`, no reply. A typed `rejected` would require the `txId`, and
recovering it means decoding the very payload the guard exists **not** to
decode — the bound is on memory/CPU, and a partial/prefix decode of
MessagePack would be a hand-rolled parser coupled to field order, for a path
production traffic mostly cannot reach (the edge cap drops it first). The
drop is now explicitly framed as defense in depth behind D1, and the code
comment says so.

### D3: Outbound is warn-only — observability, not enforcement

When an encoded outbound frame exceeds `WARN_OUTBOUND_FRAME_BYTES` (fixed
1 MiB, per D0), the DO logs a `console.warn` with the frame type, byte size,
the collection (resolved from the socket's subscription registry only on the
warn path — never a hot-path lookup), and a pointer at the real fix (column
projection, issue #28). The frame is **still sent whole**.

Why not split or drop: outbound has no edge cap, so delivery works; splitting
would need a reassembly protocol for a problem whose real fix is sending less
— column projection / changed-columns-only patches, which is issue #28 part
(a) and deliberately out of scope here. The warning is the operator's signal
that they are paying the full-row-hydration cost.

## Consequences

- An oversize client mutation now fails in milliseconds with
  `MutationRejectedError` / `"FRAME_TOO_LARGE"` instead of a 5 s generic
  timeout; optimistic state rolls back promptly. Pinned by
  `tests/frame-limits.test.ts` (raw transport and collection-level rollback).
- No new public API surface: both thresholds are constants (D0), so there is
  nothing to configure and nothing to misconfigure.
- Large outbound rows keep working exactly as before (whole, single frame) —
  now with a warning naming the collection and pointing at column projection
  (#28) when they cross the threshold.
- The server's silent inbound drop is unchanged and remains pinned by
  `tests/wire-hardening.test.ts`; its comment now explains why it stays
  silent. Its measure for **string** frames is still `message.length`
  (UTF-16 code units) per ADR-0012 D2 — an undercount for non-ASCII text
  frames, flagged here rather than silently changed; the wire default is
  binary, where the measure is exact.
- A socket that refuses a `mut`/`call` send synchronously (workerd does this
  for frames over its own cap) now cleans up the pending-receipt waiter and
  its timeout before rejecting, instead of leaving a stale entry armed for
  `timeoutMs` (codex review).
