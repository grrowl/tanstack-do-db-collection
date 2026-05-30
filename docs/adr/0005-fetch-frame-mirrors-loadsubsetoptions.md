# 0005 — The cursor `fetch` frame mirrors TanStack's `LoadSubsetOptions`

**Status:** Accepted. Supersedes the wire field *naming* of
[ADR-0003](./0003-atomic-cursor-fetch.md) (its atomic single-frame decision is
unchanged). Lands after 0.1.0.

## Context

ADR-0003 made cursor load-more one atomic `fetch` frame. As first shipped (0.1.0)
that frame carried **pre-combined, privately-named** predicates: the client
computed `and(base, whereFrom)` and `and(base, whereCurrent)` and sent them as
`where` and `ties`.

`ties` is a real term of art (TanStack's own docs call `whereCurrent`
"tie-breaking/duplicates at the boundary"), but it is *our* identifier, not
TanStack's. The wire thus spoke a private dialect for a concept TanStack already
names precisely — `CursorExpressions { whereFrom, whereCurrent }` on
`LoadSubsetOptions` (`@tanstack/db` `types.d.ts`).

## Decision

The `fetch` frame is a **serialized subset of `LoadSubsetOptions`**: a base
`where` plus a raw `cursor: { whereFrom, whereCurrent }` — TanStack's field names,
with TanStack's semantics. Critically the cursor expressions are carried **raw**,
*excluding* the base `where`, exactly as `CursorExpressions` are defined. The
**server** composes `base AND whereCurrent` (ties, unbounded) and
`base AND whereFrom` (next page, bounded by `limit`) via `andPredicates`
(`sql-compiler.ts`).

We carry only `{ whereFrom, whereCurrent }`. `lastKey`, `offset`, and
`subscription` from `LoadSubsetOptions`/`CursorExpressions` are intentionally
omitted — dedup is by key via insert-if-absent, pagination is keyset-by-choice
(not offset), and the subscription handle is a client concern.

## Why

- **Predictable and traceable.** Every field maps to upstream with identical
  meaning. A reader fluent in TanStack's sync API reads the frame cold; the frame
  doubles as documentation.
- **It avoids a semantic trap.** A bare rename (`ties → whereCurrent`) over the
  *combined* predicate would name a field `whereCurrent` while it carried
  `and(base, whereCurrent)` — the inverse of TanStack's contract, which says
  `whereCurrent` excludes the base. Carrying it raw is the only faithful option.
- **Conformance past this repo.** Speaking the ecosystem's words with the
  ecosystem's meanings lets us lean on TanStack's and Electric's reasoning
  directly instead of maintaining a private vocabulary. Standing on shoulders.
- **Simpler client.** `loadMore` forwards TanStack's `options.where` and
  `options.cursor` ~verbatim and drops the `@tanstack/db` `and` import; predicate
  composition lives in the server, which already owns IR→SQL.

## Trade-offs

- It moves `and` composition from client to server. This is **behaviour-
  preserving**: `andPredicates(a, b)` builds the same `{ type: "func", name:
  "and", args: [a, b] }` node `@tanstack/db`'s `and()` produces (`query/ir.js`),
  and only those enumerable fields cross the MessagePack wire — so the compiled
  SQL and bound params are identical across all four (base × cursor) presence
  cases. Hand-computed and confirmed by adversarial review (codex gpt-5.5).
- It is a breaking **wire** change versus 0.1.0. Client and server ship from one
  package and upgrade together, so there is no mixed-version surface in practice.

## Consequences

- The server validates that a present `cursor` carries **both** halves; a
  malformed cursor (a missing `whereCurrent`/`whereFrom`) is rejected loudly
  (logged, empty page) rather than composed away into an unbounded ties scan —
  upholding the operator floor's "never silently full-scan" discipline. Found by
  the adversarial review.
- A real tie boundary is now integration-tested against the DO (unbounded ties +
  limited next + base composed into both halves) — `tests/on-demand.test.ts`,
  "cursor fetch returns ALL boundary ties …".
- ADR-0003's deferred item stands: an unbounded `whereCurrent` over-fetches on a
  low-cardinality lead order column; mitigation is a high-cardinality lead key
  and, if needed, a fail-loud ceiling.
