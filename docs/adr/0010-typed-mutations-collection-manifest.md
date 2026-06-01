# 0010 — Typed mutations via a collection-row manifest on `SyncRegistry`

**Status:** Accepted. Types the mutation-handler surface introduced by
[ADR-0007](./0007-author-owned-schema-register-sync.md). Commands
(`defineCommand`) are explicitly out of scope (a follow-up).

## Context

A mutation handler receives `op.cols` as `Record<string, unknown>` — the wire
type (`MutOp`, `frames.ts`). So every handler casts: `op.cols as Message`. That
is repetitive, and it is *a lie to the compiler* — an unchecked assertion with
no runtime backing. We want `op` typed by the collection's row type **and** the
op kind:

- `insert` → `cols: Row` (the full row, ADR-0001 D19)
- `update` → `cols: Partial<Row>` (top-level patch, ADR-0002 C6)
- `delete` → no `cols` (just `key`)

This is **purely a type-level concern**: runtime dispatch is string-keyed and
untyped (`mutations.get(\`${collection}:${op.type}\`).execute({ op, … })`), and
the row type has no runtime presence. So the change touches signatures only —
zero runtime behaviour change — and is verified by type-level tests.

### The constraint that decided the shape

The natural idea — `.defineCollection<Message>({ table: "messages", pk: "id" })`,
accumulating `{ messages: Message }` so mutations infer the row from the
collection name — **cannot compile**, for two converging reasons (both verified
against `tsc --strict`, see below):

1. **TypeScript has no partial type-argument inference**
   ([microsoft/TypeScript#26242](https://github.com/microsoft/TypeScript/issues/26242)).
   The moment you write an explicit `<Message>`, TS stops inferring the other
   type params, so the table-name literal can't be captured.
2. **The row type has no runtime witness** in `{ table, pk }` — nothing to infer
   it *from* — so it must be an explicit annotation somewhere; which trips (1).

A wall probe confirmed it: with `defineCollection<Row, Table extends string = string>`,
calling `<Message>(…)` widens `Table` to `string`, yielding `Record<string, Message>`
— collections become indistinguishable.

So the only question is **where the one explicit row annotation lives.** Three
homes compile (all verified); a fourth (schema) was rejected.

## Options (all compile-tested)

- **A — per mutation:** `.defineMutation<Message>({ collection, type, execute })`,
  the arg a discriminated union keyed on `type` (so no second inference is
  needed; `cols` is precise per op). Clean single `<>`, co-located with the
  handler. **But:** the row type is repeated on every mutation, and nothing ties
  `collection: "messages"` to `Message` (same trust as `as`, just centralised).
- **B1 — per collection (curry):** `.defineCollection<Message>()({ table, pk })`.
  Declared once, co-located, links collection→row, and enforces *define-before-
  mutate* at compile time. **But:** the `()()` curry on every collection.
- **B2 — constructor manifest:** `new SyncRegistry<TUser, Env, { messages: Message }>()`,
  then plain `.defineCollection`/`.defineMutation` with everything inferred. **The
  only option that types *both* call sites** — `pk` against `keyof Row` and the
  table/collection names against the manifest, *and* `op.cols` per op — with no
  curry. **Cost:** the manifest sits apart from the `.defineCollection` calls
  (TS checks they agree), and you specify `Env` to reach the third slot.
- **B3 — schema-driven (rejected):** `.defineCollection({ table, pk, schema })`
  infers `Row` from a [Standard Schema](https://standardschema.dev/) value *and*
  validates inbound `cols` at runtime. Rejected: it adds no real **safety** — SQL
  injection is already prevented by parameterised binding (values are never
  interpolated; identifiers are validated at registration, ADR-0007), and
  malformed input is already rejectable in `authorize` — while taxing the
  **write hot path** with a per-mutation validator, against the light/streaming
  ethos. Input validation, where wanted, belongs in `authorize`.

## Decision

**B2 — the constructor manifest.**

```ts
class SyncRegistry<TUser = unknown, Env = unknown,
               TCols extends Record<string, unknown> = Record<string, unknown>> {
  defineCollection<Name extends keyof TCols & string>(
    def: { table: Name; pk: PkOf<TCols[Name]> },
  ): this
  defineMutation<Name extends keyof TCols & string, T extends RowOp>(
    def: {
      collection: Name
      type: T
      authorize?:  (ctx: MutationCtx<TUser, Env, OpFor<T, TCols[Name]>>) => void | Promise<void>
      execute:     (ctx: MutationCtx<TUser, Env, OpFor<T, TCols[Name]>>) => void
      afterCommit?: (ctx: MutationCtx<TUser, Env, OpFor<T, TCols[Name]>>) => unknown
    },
  ): this
}
type OpFor<T extends RowOp, Row> =
  T extends "insert" ? { type: "insert"; key: string; cols: Row }
  : T extends "update" ? { type: "update"; key: string; cols: Partial<Row> }
  : { type: "delete"; key: string; cols?: undefined }
// PkOf<Row> = [keyof Row] extends [never] ? string : keyof Row & string
```

- **Non-breaking for the untyped path.** `TCols` defaults to
  `Record<string, unknown>`, so `new SyncRegistry<Claims>()` still compiles; an
  unannotated collection falls back to `cols: unknown` (the author casts, as
  today). `PkOf` degrades to `string` when the row is unknown.
- **Zero runtime change.** The `collections`/`mutations` Maps stay string-keyed;
  the generic builder casts the typed def into the erased stored
  `MutationDef` at the storage boundary (one deliberate, isolated `as`).
  `MutationCtx` gains a third type param `TOp = MutOp` (default preserves
  existing internal call sites). Dispatch is untouched.

**Why B2 over A and B1.** Over **A**: A re-states the row type per mutation and
can't check collection↔row; B2 declares it once and types both sides. Over
**B1**: B1's co-location is nice but forces `()()` on every collection; B2 has no
curry, and the manifest-apart-from-calls gap is itself type-checked
(`table extends keyof TCols`). The deciding vote was the `()()` ergonomics
against B2's single up-front declaration.

## Verification

The four ideas were compiled under `tsc --noEmit --strict` before deciding:
A and B2 type-check with precise per-op `cols` (insert `Row`, update
`Partial<Row>`, delete no `cols`); a generic *on `execute`* fails (the param is
caller-chosen and opaque in the body); the accumulating `defineCollection<Row>`
widens the table to `string`. These become permanent **type-level tests**
(`tsc` expect-pass + `@ts-expect-error` expect-fail) alongside the unchanged
runtime suite.

## Consequences

- Authors declare row types once (the manifest); handlers get precise
  `op.cols`/`op.key`; the `as Message` casts disappear — including the ones in
  the README quick-start (added during the README pass as a stopgap).
- The untyped path is unchanged at runtime and only mildly narrower at compile
  time (`cols: unknown` vs `Record<string, unknown>`); our own call sites cast,
  so nothing breaks.
- Type-level tests join the suite — a new category for this repo.

## Out of scope

- **Typed command `args`** (`defineCommand`). Commands are a different shape
  (free-form args, not a collection row), and their API status warrants its own
  review. Deferred to a follow-up.
