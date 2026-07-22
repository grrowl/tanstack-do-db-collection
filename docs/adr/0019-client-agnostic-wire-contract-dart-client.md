# 0019 — The wire protocol is a client-agnostic contract; a Dart client mirrors it

**Status:** Accepted
**Date:** 2026-07-22
**Characterization:** `packages/do_sync_client/test/` (unit + conformance),
`tests/dart-conformance.test.ts` (TS-side verification of Dart-encoded frames)

## Context

A Flutter app wants to reuse this sync backend and wire protocol. Until now the
wire protocol was an *internal* detail between `src/wire/` and the two TS
endpoints that ship in one package — both ends could change together in one
commit. A second-language client ends that: whatever the Dart client speaks
becomes load-bearing, so we must decide (a) what exactly the contract is,
(b) what a non-TanStack client substitutes for `@tanstack/db`'s role, and
(c) how two implementations are kept honest against each other.

The TS client splits cleanly (verified by reading imports, not assumption):

- `client/transport.ts` imports **nothing** from `@tanstack/db` — it is pure
  protocol (single cursor, tx waiters, reconnect, pre-send guard).
- `client/do-collection.ts` is the **only** file that touches `@tanstack/db`,
  a ~300-line adapter onto its sync API plus the optimistic-mutation wrapper.

What `@tanstack/db` actually contributes through that adapter: the optimistic
overlay with rollback, live queries via IVM, and the on-demand window machinery.
The IVM exists because a browser has no queryable local store; Flutter has real
SQLite (and Dart has first-class `Stream`s), so porting the IVM would be
reimplementing a workaround for a constraint the platform doesn't have.

## Decisions

### D1: The wire contract is public and client-agnostic — frames, encoding, IR

The contract a client must speak is exactly three things, all now frozen
against casual change (a change requires a new ADR **and** a conformance-
fixture update, see D6):

1. **Frames** — the `ClientFrame`/`ServerFrame` unions in `src/wire/frames.ts`,
   with their documented semantics (ADR-0001/0002): `seq` is an opaque
   stringified bigint; the cursor advances only at `snap-end` / `uptodate` /
   `committed`; deltas flush before `committed` on the originating socket;
   `reset` means truncate-and-resnapshot *and* is the terminal signal for a
   rejected sub; application closes 4000–4999 are terminal (ADR-0016); inbound
   frames are capped at 1 MiB and the client pre-send guard is the rejection
   surface (ADR-0018).
2. **Encoding** — MessagePack as `@msgpack/msgpack` with `useBigInt64` and the
   ext registrations of `src/wire/frame-codec.ts`. The value mapping is pinned
   empirically (probed against the actual library, not its docs):

   | wire (msgpack) | JS | Dart |
   |---|---|---|
   | int formats (≤ 32-bit) | `number` (integral) | `int` |
   | float64 (`0xcb`) | `number` — **incl. all integers beyond 32-bit**, NaN, ±Inf | `double` |
   | uint64/int64 (`0xcf`/`0xd3`) | `bigint` | `BigInt` |
   | timestamp ext −1 | `Date` | `DateTime` (UTC) |
   | bin (`0xc4…`) | `Uint8Array` | `Uint8List` |
   | ext 0 | bare `ArrayBuffer`, normalized (ADR-0017) | `Uint8List` |

   The float64 row matters most: `@msgpack/msgpack` encodes an *integral* JS
   number above the 32-bit formats (e.g. any `Date.now()` value) as float64.
   A Dart encoder must mirror that — a Dart `int` outside int32/uint32 range is
   encoded as float64 (values ≥ 2^53 are rejected loudly rather than silently
   rounded) — otherwise a Dart-written row would decode on JS as `bigint`
   where a JS-written row decodes as `number`, and the two clients would
   silently diverge on the same column.
3. **The predicate IR** — `@tanstack/db`'s `BasicExpression` node shape
   (`ref`/`val`/`func`) restricted to the ADR-0013 floor
   `{eq, gt, gte, lt, lte, like, in, and, or, not}` with its verified
   semantics (case-sensitive `like`, string-literal pattern only). The IR
   stops being a TanStack implementation detail and becomes part of the wire
   contract; a Dart client ships a small builder emitting that node shape, and
   ADR-0013's "one evaluator" discipline extends to it: the Dart evaluator's
   parity cases mirror `tests/predicate-parity.test.ts`.

The JSON debug codec (`wire/codec.ts`) is explicitly **not** part of the
contract — binary MessagePack is the wire.

### D2: The Dart client does not port TanStack DB — mirror + overlay instead

`packages/do_sync_client` substitutes the platform-native equivalents for each
of `@tanstack/db`'s three roles:

- **Reactive reads** — a `SyncCollection` holds a synced **mirror**
  (`Map<String, Row>`) and exposes `Stream`s that emit **only at commit
  boundaries** (`snap-end`/`uptodate`/`reset`/overlay change), never per-frame.
  This is the same batch-atomicity `begin()/commit()` bought in TanStack:
  readers never observe a half-applied batch. Live *queries* (filter/sort/join)
  are the app's business — Dart has `where`/`sorted`/Drift; the collection's
  job ends at "a consistent, reactively-updated set of rows", honoring the
  ADR-0001 inversion (the DO never joins; the client composes).
- **Optimistic writes** — a pending-mutation **overlay** shadowed over the
  mirror by pk at read time. The client-supplied-pk invariant (ADR-0007,
  optimistic id == confirmed id) makes shadowing a map lookup. On `committed`
  the overlay entry is dropped — the matched delta has already landed in the
  mirror (C1 ordering guarantees it). On `rejected`, timeout, or transport
  close the entry is dropped too: that *is* the rollback; no compensation
  journal, no empty-commit dance (ADR-0002 C2 was a workaround for TanStack's
  direct-upsert retention semantics, which the overlay design simply doesn't
  have).
- **Windowing** — none (see D4).

Persistence (SQLite/Drift) stays **out** of this package: the mirror is
in-memory, and the boundary-committed change notifications are sufficient for
a later `drift` adapter to write-through transactionally. Simplest thing that
works; a persistent mirror + persisted `appliedSeq` (offline catch-up within
the ADR-0009 retention window) is deferred until a real app needs it.

### D3: Eager `where` keeps its preflight — the floor evaluator is ported

The TS client rejects a write falling outside an eager collection's static
`where` before any I/O (`WriteOutsideSubError`), because such a write would be
confirmed but never delivered — a phantom. Dropping the preflight would make
the Dart client *silently* accept-then-vanish such rows: a behavioral
divergence on exactly the kind of edge ADR-0013 exists to prevent. So the Dart
package ships the ~100-line floor evaluator (same floor, same semantics:
case-sensitive `like` with string-only operands, `in` on a literal list,
three-valued null handling collapsed via `toBooleanPredicate`'s null→false) and
performs the same preflight. Parity cases are copied from
`tests/predicate-parity.test.ts` row-for-row.

### D4: Scope — eager mode, mutations, commands; on-demand deferred

Sequenced the way this repo itself was built:

- **In:** eager subscribe (optionally with static `where` + preflight),
  snapshot/delta/catch-up application (including the held-key-insert-as-upsert
  rule, ADR-0002 C4 — the scroll-back `DuplicateKeySyncError` lesson),
  `reset` handling, reconnect with `since` resubscribe (ADR-0016 policy:
  jittered capped backoff, terminal 4xxx), `mut` with optimistic overlay,
  `call` commands (untyped `sendCall(name, args)`; Dart has no structural
  typing to project a TS schema Api — codegen is deferred until the shape is
  proven), the 1 MiB pre-send guard (ADR-0018).
- **Out (deferred):** `syncMode: 'on-demand'` / `loadSubset` refcounting, the
  atomic cursor `fetch` (ADR-0003/0005), orderBy/limit on `sub`. With a local
  queryable mirror, plain client-side ordering over an eager subset covers the
  Flutter use cases we have; the window machinery is real work and nothing
  drives it yet.

### D5: The msgpack codec is hand-rolled pure Dart

Not `msgpack_dart` (unmaintained, uncertain ext/timestamp semantics). The wire
needs a specific dialect — D1's table, timestamp ext −1 both fixext8
(nanoseconds) and the 4/12-byte forms, ext 0, and the exact integer/float64
split — and getting that from a third-party package would mean auditing it to
the same depth as writing ~400 lines we fully control. The codec is pinned by
D6's fixtures, which is a stronger guarantee than any package's own tests.

### D6: Conformance is tested cross-language, in both directions

A protocol spoken by two implementations needs an executable referee:

1. **TS → Dart:** `tests/fixtures/wire-fixtures.mjs` (run via the repo's own
   `@msgpack/msgpack`) emits golden frames — every frame tag, every D1 value
   type, edge integers at the 32-bit/2^53 boundaries — as base64 + a JSON
   description. Dart tests decode the bytes and assert the typed values.
2. **Dart → TS:** the same Dart test suite re-encodes each frame; a TS-side
   test (`tests/dart-conformance.test.ts`) decodes the Dart-emitted bytes with
   the production `createFrameCodec()` and asserts deep equality with the
   originals. Byte-identity is deliberately **not** asserted (msgpack allows
   shortest-form variance); *value*-identity through the production decoder is
   the contract.
3. **Live e2e:** a real `SyncDurableObject` served by workerd (`wrangler dev`),
   driven by the Dart client over a real WebSocket: snapshot, live delta
   between two Dart connections, `mut` → `committed` confirmation, `call`
   round-trip, reconnect catch-up via `since`. This is the "speaks the same
   protocol" proof; the fixtures are the fast regression net.

## Consequences

- The wire protocol, D1's value table, and the predicate floor are now a
  **public contract**. A frame or floor change is a breaking change to an
  ecosystem, not a refactor: new ADR + fixture regeneration + both clients.
- `packages/do_sync_client` exists with zero runtime dependencies; Flutter
  integration is a thin consumer (`Stream`s → widgets, later Drift).
- ADR-0002 C2's empty-commit mechanism is TS-client-specific and stays there;
  the Dart overlay achieves retirement by construction. If the Dart client
  later adopts on-demand mode, the C2 analysis must be revisited for it.
- Command typing across languages is unsolved (deferred codegen); Dart callers
  get `Future<Object?>` and their own casts.
- A second client raises the bar on server changes: `tests/` alone no longer
  proves a wire change safe — the conformance suite must pass too.
