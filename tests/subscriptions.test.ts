import { describe, expect, it } from "vitest"
import { UnsupportedPredicateError } from "../src/server/sql-compiler.ts"
import { SubscriptionRegistry } from "../src/server/subscriptions.ts"

// WHY: a filtered sub's predicate is compiled with @tanstack/db's evaluator (the
// delta/catch-up path). That JS floor MUST agree with the SQL snapshot floor, and
// a predicate it cannot compile MUST fail loud as UnsupportedPredicateError — not
// escape uncaught and hang the subscriber. This is the unit-level pin of the `ne`
// crash fix (ADR-0013): before, compileSingleRowExpression("ne") threw a raw
// QueryCompilationError that escaped handleSub past its UnsupportedPredicateError
// catch, so the client got no `reset` and waited forever. These tests pin the JS
// floor directly — the wire test (predicate-parity) covers the SQL floor, which
// now also rejects `ne` one step earlier.

const ref = (col: string) => ({ type: "ref", path: [col] })
const val = (value: unknown) => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>) => ({ type: "func", name, args })

describe("SubscriptionRegistry predicate floor (ADR-0013)", () => {
  // The registry uses ws only as a WeakMap key; no methods are called on it.
  const ws = {} as unknown as WebSocket

  it("rejects an operator @tanstack/db cannot compile (`ne`) as UnsupportedPredicateError", () => {
    const reg = new SubscriptionRegistry()
    expect(() => reg.add(ws, "s1", "messages", fn("ne", ref("body"), val("x")))).toThrow(UnsupportedPredicateError)
  })

  it("accepts the supported not-equal form not(eq(...)) and mirrors SQL 3-valued NULL", () => {
    const reg = new SubscriptionRegistry()
    const sub = reg.add(ws, "s2", "messages", fn("not", fn("eq", ref("body"), val("x"))))
    expect(sub.predicate({ body: "y" })).toBe(true)
    expect(sub.predicate({ body: "x" })).toBe(false)
    // NOT (NULL = 'x') is NULL in SQL → toBooleanPredicate collapses it to false.
    expect(sub.predicate({ body: null })).toBe(false)
  })

  it("an unfiltered sub matches every row", () => {
    const reg = new SubscriptionRegistry()
    const sub = reg.add(ws, "s3", "messages")
    expect(sub.predicate({ anything: 1 })).toBe(true)
  })
})
