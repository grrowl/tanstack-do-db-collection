import { compileSingleRowExpression, toBooleanPredicate } from "@tanstack/db"
import { describe, expect, it } from "vitest"

// PROBE (M5 foundation): the server evaluates the client's predicate IR at
// runtime via @tanstack/db's compiler, so operator support matches the client
// exactly. This confirms @tanstack/db imports and runs in workerd, and that a
// plain-JSON IR (as it arrives off the wire — not class instances) compiles.

describe("@tanstack/db predicate eval runs in workerd", () => {
  it("compiles and evaluates a single-row eq predicate from plain-JSON IR", () => {
    const ir = {
      type: "func",
      name: "eq",
      args: [
        { type: "ref", path: ["body"] },
        { type: "val", value: "hi" },
      ],
    }
    const evaluate = compileSingleRowExpression(ir as never) as (row: Record<string, unknown>) => boolean | null
    expect(toBooleanPredicate(evaluate({ id: "a", body: "hi" }))).toBe(true)
    expect(toBooleanPredicate(evaluate({ id: "a", body: "no" }))).toBe(false)
  })

  it("evaluates a compound and(gt, eq) predicate", () => {
    const ir = {
      type: "func",
      name: "and",
      args: [
        { type: "func", name: "gt", args: [{ type: "ref", path: ["n"] }, { type: "val", value: 5 }] },
        { type: "func", name: "eq", args: [{ type: "ref", path: ["kind"] }, { type: "val", value: "x" }] },
      ],
    }
    const evaluate = compileSingleRowExpression(ir as never) as (row: Record<string, unknown>) => boolean | null
    expect(toBooleanPredicate(evaluate({ n: 9, kind: "x" }))).toBe(true)
    expect(toBooleanPredicate(evaluate({ n: 1, kind: "x" }))).toBe(false)
    expect(toBooleanPredicate(evaluate({ n: 9, kind: "y" }))).toBe(false)
  })
})
