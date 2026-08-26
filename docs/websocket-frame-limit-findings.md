# Findings: the inbound WebSocket frame limit ADR-0018 rests on is stale

**Status**: Investigation — **not a decision**. Feeds a future ADR superseding
ADR-0018 D0's factual basis.
**Date**: 2026-07-26
**Working note** — not part of the ADR record until the probe in §6 resolves
the open question.
**Relates to**: [ADR-0018](./adr/0018-oversize-frames.md),
[ADR-0012](./adr/0012-wire-input-hardening.md), issue #28

## 1. Summary

ADR-0018 builds its central decision (D0: "the limits are infrastructure facts,
not application preferences") on this claim:

> **Inbound** (client → DO) WebSocket messages are capped at ~1 MiB **at the
> edge** in production.

That was true until **2025-10-30**. Cloudflare raised the inbound cap from
1 MiB to **32 MiB** nine months before ADR-0018 was written on 2026-07-22.

The *shape* of ADR-0018 survives this. The inbound/outbound asymmetry is real —
inbound is capped, outbound is not — and a client pre-send guard is still the
right rejection surface. What does not survive is the specific number and the
"this is a fact, so it gets no knob" argument built on it.

There is a complication that may rescue the constant for the wrong reason: the
**hibernation** read loop this library uses may still be capped at 1 MiB in
workerd. That is unresolved and **cannot be settled by any test in this repo**
(§5). Until it is, both `MAX_FRAME_BYTES` and `WARN_OUTBOUND_FRAME_BYTES` are
resting on an unverified assumption.

## 2. What actually changed, and when

| Date | Event |
|---|---|
| — | kj-http default `SUGGESTED_MAX_MESSAGE_SIZE = 1u << 20` (1 MiB). This is the origin of the 1 MiB figure. |
| 2024-08-02 | workerd `fc7775879` (PR #2164) adds compat flag `increase_websocket_message_size` → 128 MiB. Marked `$experimental`; capnp comment says "For local development purposes only… not expected ever to be made available in production." |
| 2025-10-23 | workerd `b78d0b69d` "EW-9632 Increase max WebSocket message size limit to 32MiB" adds `WEBSOCKET_MAX_MESSAGE_SIZE = 32u << 20` behind autogate `websocket-max-message-size-32m`. |
| 2025-10-30 | workerd `39ad5e399` (merge `aee8c81a7`) removes the autogate. 32 MiB becomes unconditional. |
| 2025-10-31 | Cloudflare changelog: "Workers WebSocket message size limit increased from 1 MiB to 32 MiB… Workers, **including those using Durable Objects** and Browser Rendering, may now process WebSocket messages up to 32 MiB in size." |
| 2026-07-22 | ADR-0018 written, citing the ~1 MiB cap as current. |

Current source, `src/workerd/api/web-socket.c++` (~line 712):

```c++
// Default max WebSocket message size limit. Note that kj-http's own default is 1MiB
// (`kj::WebSocket::SUGGESTED_MAX_MESSAGE_SIZE`). We've found this to be too small ...
static constexpr size_t WEBSOCKET_MAX_MESSAGE_SIZE = 32u << 20;
```

Two corrections to ADR-0018's Context section beyond the number itself:

- **Oversize inbound is not silently dropped.** Per the Workers WebSockets
  docs, the socket is closed with **`1009` "Message is too large"**. ADR-0018
  reasons about a frame that "may never reach the DO at all" as if it vanishes;
  in production it takes the socket down with a specific code. That is a
  materially different failure mode, and one the transport's `onClosed` hook
  (ADR-0016) could actually surface.
- **"only for received messages"** on the DO limits page means received *by the
  Worker/DO* — i.e. inbound. ADR-0018 reads this direction correctly. Recording
  it because it is the obvious thing to second-guess when revisiting this.

Sources:
- https://developers.cloudflare.com/changelog/2025-10-31-increased-websocket-message-size-limit
- https://developers.cloudflare.com/workers/runtime-apis/websockets/
- https://developers.cloudflare.com/durable-objects/platform/limits/
- workerd commits `b78d0b69d`, `39ad5e399`, `fc7775879`

## 3. The hibernation ambiguity (unresolved)

The October 2025 change touched `src/workerd/api/web-socket.c++`, which serves
sockets accepted with **`ws.accept()`**.

Hibernatable sockets — `ctx.acceptWebSocket()`, which is what
`src/server/mixin.ts:310` calls — are served by a different read loop, in
`src/workerd/io/legacy-hibernation-manager.c++:305`:

```c++
kj::WebSocket::Message message = co_await ws.receive();   // no maxSize argument
```

With no `maxSize`, this defaults to kj's `SUGGESTED_MAX_MESSAGE_SIZE` = **1 MiB**.
It is the only `receive()` call in that file, and nothing plumbs a larger size
in. The file's recent commits (`4999e3007`, 2026-06-26, a rename; the
in-progress `HibernationManagerImpl` refactor in `hibernation-manager.h` is
still a `KJ_UNIMPLEMENTED` skeleton with its autogate off) did not change it.

**So in open-source workerd `main` today, hibernatable sockets appear to still
cap inbound at 1 MiB while `ws.accept()` sockets get 32 MiB.**

Arguments the other way, which is why this is *unresolved* rather than settled:

- The changelog explicitly names Durable Objects in the raise.
- Production Cloudflare may carry internal patches not in the OSS tree.
- There are no public bug reports either confirming or refuting a
  hibernation-specific cap post-October-2025.

Both readings are defensible from public evidence. Only §6 settles it.

## 4. What this costs us today

A **live capability regression**, not a theoretical one:

- A ~1.5 MB TEXT column syncs server → client whole. We pin this in
  `tests/frame-limits.test.ts` ("delivers a ~1.5MB row server->client in the
  initial snapshot, whole").
- The same row **cannot be written** client → server. `transport.ts:427` rejects
  it before it reaches the wire.

If the true cap on our path is 32 MiB, that asymmetry is entirely self-imposed,
and we are rejecting writes at 1/32nd of what the platform accepts with no
way for a user to opt out.

### What it does to D0's argument

D0 rejected a configurable `maxFrameBytes` on this reasoning:

> a client-side knob could only be set *below* the cap (pointless…) or *above*
> it (a lie — the guard would wave through frames the edge then kills)

That argument is sound **only while 1 MiB is the actual cap**. If the real cap
is 32 MiB, then 1 MiB is not an infrastructure fact but an application
preference — and by D0's own logic, a preference is exactly the kind of thing
that should be configurable, or else set at the true cap.

The no-knobs conclusion may still be right. But it currently rests on a premise
that is either false or unverified, and the superseding ADR has to re-derive it
rather than inherit it.

## 5. Why no test in this repo can settle this

`vitest-pool-workers` connects the two ends with an in-process `WebSocketPair`
that never traverses the kj read loop. A throwaway probe run in this repo's test
environment (`SELF.fetch` upgrade + raw `ws.send`) at **900 KB, 2 MB and 40 MB**
delivered *all three* to the DO's `webSocketMessage` handler. The 40 MB frame
reached our own `maxFrameBytes` check at `mixin.ts:346` and was rejected there —
by our code, not by the runtime. No `1009`, no close.

This also explains why the drift never surfaced.
`tests/wire-hardening.test.ts:181-213` hedges:

```ts
} catch {
  // workerd may throw synchronously on an oversized send — treat as closed.
  wsClosedByWorkerd = true
}
if (wsClosedByWorkerd) { … return }
```

In this harness that branch does not fire — the frame goes through and our guard
catches it. The test passes down the other path either way, so it never
contradicted the ADR's premise. It is a test that cannot fail when the fact it
documents changes, which is the failure mode CLAUDE.md's "tests encode *why*"
rule exists to catch.

**Consequence for the build discipline:** this is the same class of gap as issue
#29 (the hibernation wake-restore verified by code-reading only). Anything whose
behaviour lives in the kj read loop or the real edge is invisible to our suite.
Frame limits belong on the short list of things that need an out-of-CI smoke
test against a deployed DO.

## 6. The probe that settles it

Deploy a minimal DO that accepts via **`ctx.acceptWebSocket`** (the path that
matters — a `ws.accept()` probe answers the wrong question), then drive it from
a real browser against the deployed origin, not `wrangler dev`.

```ts
// probe-do.ts — deploy, don't run under vitest or `wrangler dev`
export class ProbeDO {
  constructor(private ctx: DurableObjectState) {}
  async fetch(req: Request) {
    const [client, server] = Object.values(new WebSocketPair())
    this.ctx.acceptWebSocket(server)            // hibernation path — the point
    return new Response(null, { status: 101, webSocket: client })
  }
  webSocketMessage(ws: WebSocket, msg: string | ArrayBuffer) {
    const n = typeof msg === "string" ? msg.length : msg.byteLength
    console.log("DO received", n)               // tail this
    ws.send(String(n))                          // echo size back
  }
  webSocketClose(_ws: WebSocket, code: number, reason: string) {
    console.log("closed", code, reason)         // 1009 => cap hit
  }
}
```

Browser side — send ascending sizes and record which echo back:

```js
const ws = new WebSocket("wss://<deployed-host>/probe")
ws.onmessage = (e) => console.log("echoed", e.data)
ws.onclose = (e) => console.log("CLOSED", e.code, e.reason)  // 1009 => cap
await new Promise(r => ws.onopen = r)
for (const n of [900_000, 1_048_577, 2_000_000, 8_000_000, 33_554_433]) {
  ws.send("x".repeat(n))
  await new Promise(r => setTimeout(r, 500))
}
```

**Read the result as:**

| Observation | Conclusion |
|---|---|
| `1_048_577` closes with `1009` | Hibernation path really is 1 MiB. `MAX_FRAME_BYTES` is correct — for a reason ADR-0018 does not state. |
| `1_048_577` and `8_000_000` echo; `33_554_433` closes `1009` | 32 MiB applies to hibernatable sockets too. Our constant is a self-imposed 1 MiB. |
| Everything echoes | No cap on this path at all. Re-derive the guard's purpose from scratch. |

Run it against `wrangler dev` **as well as** deployed, and record both — a
divergence between them is itself worth documenting, since local dev is where
contributors will form their intuitions.

## 7. Decision matrix for the superseding ADR

Per CLAUDE.md the ADR record is append-mostly: this becomes a **new ADR that
supersedes ADR-0018 D0's factual basis**, not an edit to ADR-0018.

**If the probe shows 1 MiB on the hibernation path:**
- Keep both constants.
- Re-justify them on `legacy-hibernation-manager.c++` defaulting to kj's
  `SUGGESTED_MAX_MESSAGE_SIZE`, explicitly flagged as a **workerd implementation
  detail, not a documented limit** — a weaker and more fragile foundation than
  D0 currently claims. It can change in any workerd release with no changelog
  entry, because the documented figure already says 32 MiB.
- That fragility is a genuine argument for revisiting the no-knobs decision:
  an undocumented implementation detail is exactly the kind of "fact" that
  deserves an escape hatch.
- Add the probe to the out-of-CI smoke checklist alongside the #29 wake-restore
  and the partyserver `__pk` audit.

**If the probe shows 32 MiB:**
- Raise `MAX_FRAME_BYTES` to `33_554_432`, or reintroduce it as a bounded option
  defaulting to the true cap.
- Reconsider `WARN_OUTBOUND_FRAME_BYTES` separately — it is an *observability*
  threshold for full-row hydration cost (#28a), and 1 MiB may well remain the
  right warn point even when the inbound cap is 32×. D0 coupled the two numbers
  because they shared a justification; that coupling does not survive.
- Note the `1009` close behaviour: with a 32 MiB cap the pre-send guard's job
  shifts from "avert a silent drop" to "avert a socket teardown", which
  strengthens rather than weakens the case for keeping it.

**Either way:**
- Correct the stale claim everywhere it is asserted as settled fact:
  - `docs/adr/0018-oversize-frames.md` — Context bullets and D0
  - `tests/frame-limits.test.ts:1-3` — header comment
  - `src/client/transport.ts:429` — the `MutationRejectedError` message string,
    which names "the 1_048_576-byte inbound WebSocket edge cap" to users
  - `tests/wire-hardening.test.ts:181-213` — the dead `wsClosedByWorkerd`
    branch, which documents behaviour this harness cannot produce
  - `CHANGELOG.md` `[Unreleased]` — the ADR-0018 entry repeats the 1 MiB
    justification
- Record in the ADR that the figure is version-dated. The lesson is not "we got
  a number wrong", it is that a platform constant cited without a date silently
  rots — ADR-0018 stated an infrastructure fact with no "as of" and no source
  link, so nothing prompted a recheck.

## 8. Open questions

- Does production Cloudflare match OSS workerd on the hibernation path? (§6)
- Does `wrangler dev` match production? Worth recording either way.
- If the cap is 32 MiB, does a >1 MiB **outbound** frame still arrive whole in
  production? `tests/frame-limits.test.ts` pins this in-process only, and
  ADR-0018 D3's "outbound has no edge cap, so delivery works" has exactly the
  same evidentiary weakness as the inbound claim did — asserted from docs,
  verified only in a harness that cannot observe the real thing.
- Should `sub`/`fetch` frames get a bound? ADR-0018 D1 explicitly deferred this
  ("a pathological `where` predicate could in principle also exceed the cap").
  If the cap turns out to be 32 MiB the exposure is far smaller than it looked,
  which may close the question.
