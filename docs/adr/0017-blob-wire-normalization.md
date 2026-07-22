# ADR-0017: BLOB wire normalization — bare ArrayBuffer becomes Uint8Array at emission

**Status**: Accepted
**Date**: 2026-07-22
**Issue**: [#27](https://github.com/grrowl/tanstack-durable-object-sync/issues/27)

## Context

workerd's `SqlStorage` returns BLOB column values as **bare `ArrayBuffer`**.
Both row-reading paths (`SELECT *` in the sql-compiler's snapshot/fetch reader
and `hydrateRows` for deltas) hand that value straight to the frame codec.

Neither codec handled it:

- **Binary (msgpack)**: `@msgpack/msgpack`'s `encodeObject` only special-cases
  `ArrayBuffer.isView`, so a bare `ArrayBuffer` fell through to `encodeMap`
  and encoded as an **empty map**. The client received `{}` — silently, for
  snapshots and deltas alike.
- **JSON debug codec**: `classify()` tagged `Uint8Array` (`u8`) but not
  `ArrayBuffer`, and `JSON.stringify(ArrayBuffer)` is `{}` — same corruption.

Silent data loss is the worst failure mode this repo recognises ("fail loud"),
so *something* had to change. Two candidate shapes:

1. **Normalize at emission**: teach both codecs that a bare `ArrayBuffer` is
   bytes, delivered to the decoder as `Uint8Array`.
2. **Reject in `assertSyncCompatible`**: refuse BLOB-affinity columns at
   `registerSync` so authors fail loud instead of losing data.

## Decision

**Normalize at emission, in both codecs.** Rejection is only the right tool
when the data genuinely cannot be made safe; here it can be, losslessly:

- A BLOB is bytes. `Uint8Array` is the codecs' existing byte type (msgpack
  `bin`; tagged-codec `u8`), and the decode side already produces `Uint8Array`
  for it. Normalizing `ArrayBuffer → Uint8Array` at encode time is a pure
  view change over the same bytes — nothing to lose.
- The round-trip is stable: a client writing a `Uint8Array` back through a
  mutation stores the same bytes in the BLOB column; the next read emits the
  same `Uint8Array`. Optimistic and confirmed values agree.

Implementation:

- **Binary codec** (`wire/frame-codec.ts`): an `ExtensionCodec` entry
  (custom ext type `0`) — `@msgpack/msgpack` consults the extension codec
  *before* the `encodeMap` fallthrough, so it intercepts exactly the bare
  `ArrayBuffer` case while `Uint8Array` keeps using the native `bin` format.
  Decode returns the bytes as `Uint8Array`, never reconstructing an
  `ArrayBuffer`.
- **JSON debug codec** (`wire/codec.ts`): `classify()` tags `ArrayBuffer` as
  `u8`; `toPlaceholder` reads its bytes through a `Uint8Array` view. Decode
  was already symmetric (`u8 → Uint8Array`) and is unchanged.

The normalization is one-way by design: **the wire has exactly one byte type,
`Uint8Array`**. Clients never see a bare `ArrayBuffer`, regardless of which
codec or which server runtime produced the row.

### Why `assertSyncCompatible` does not also warn

Once normalized, a BLOB column round-trips as faithfully as TEXT or INTEGER —
there is nothing for an author to act on, so a warning would be noise
attached to a working feature. Rejection/warning remains the tool for
genuinely unsafe shapes (e.g. the rowid and pk-affinity rules of ADR-0015 /
0001 D9), not for this one.

### Out of scope

INTEGER values above 2^53 still lose precision — workerd's `SqlStorage`
returns JS numbers, so the damage happens before the wire is involved. That
is issue [#10](https://github.com/grrowl/tanstack-durable-object-sync/issues/10)
(wontfix), unaffected by this decision; `tests/row-shape.test.ts` pins the
rounding so a behaviour change is noticed.

## Consequences

- BLOB columns are now syncable: clients receive `Uint8Array` with the exact
  bytes, for cold snapshots, fetches, and live deltas alike.
- Binary wire format: bare `ArrayBuffer` values encode as msgpack ext type
  `0` rather than (corrupt) map. Both ends of the wire ship in this package,
  so no cross-version concern exists — but a foreign msgpack decoder would
  need the same extension registered to read BLOB values.
- `tests/row-shape.test.ts` pins the end-to-end guarantee (snapshot + delta);
  `tests/codec.test.ts` / `tests/frame-codec.test.ts` pin it per codec.
