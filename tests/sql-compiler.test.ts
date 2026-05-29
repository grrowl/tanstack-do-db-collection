import { describe, expect, it } from "vitest"
import { compileSubsetQuery, compileWhere, UnsupportedPredicateError } from "../src/server/sql-compiler.ts"

// WHY: this lowers client predicates into SQLite so subsets are filtered at the
// source, not scanned in memory. It must produce correct parameterised SQL for
// the operator floor, quote identifiers, and REJECT anything outside the floor
// (D6: fail loud, never a silent full scan). Tests assert both the SQL shape
// and the rejections.

const ref = (col: string) => ({ type: "ref", path: [col] })
const val = (value: unknown) => ({ type: "val", value })
const fn = (name: string, ...args: Array<unknown>) => ({ type: "func", name, args })

describe("IR -> SQL compiler (M6)", () => {
  it("compiles comparison operators with bound params", () => {
    expect(compileWhere(fn("eq", ref("body"), val("x")))).toEqual({ sql: `"body" = ?`, params: ["x"] })
    expect(compileWhere(fn("gte", ref("n"), val(5)))).toEqual({ sql: `"n" >= ?`, params: [5] })
    expect(compileWhere(fn("like", ref("body"), val("h%")))).toEqual({ sql: `"body" LIKE ?`, params: ["h%"] })
  })

  it("compiles and/or/not with grouping", () => {
    const e = fn("and", fn("gt", ref("n"), val(5)), fn("not", fn("eq", ref("k"), val("z"))))
    expect(compileWhere(e)).toEqual({ sql: `("n" > ? AND (NOT "k" = ?))`, params: [5, "z"] })
  })

  it("compiles `in` with one placeholder per element", () => {
    expect(compileWhere(fn("in", ref("id"), val(["a", "b", "c"])))).toEqual({
      sql: `"id" IN (?, ?, ?)`,
      params: ["a", "b", "c"],
    })
  })

  it("compiles an empty `in` to a constant-false", () => {
    expect(compileWhere(fn("in", ref("id"), val([])))).toEqual({ sql: `0`, params: [] })
  })

  it("rejects operators outside the floor (ilike, isNull, functions)", () => {
    expect(() => compileWhere(fn("ilike", ref("body"), val("x")))).toThrow(UnsupportedPredicateError)
    expect(() => compileWhere(fn("isNull", ref("body")))).toThrow(UnsupportedPredicateError)
    expect(() => compileWhere(fn("upper", ref("body")))).toThrow(/not supported/)
  })

  it("rejects nested/aliased column paths", () => {
    expect(() => compileWhere(fn("eq", { type: "ref", path: ["a", "b"] }, val(1)))).toThrow(/nested/)
  })

  it("rejects a non-literal comparison RHS and a non-ref LHS", () => {
    expect(() => compileWhere(fn("eq", ref("a"), ref("b")))).toThrow(/literal/)
    expect(() => compileWhere(fn("eq", val(1), val(2)))).toThrow(/column reference/)
  })

  it("builds a full subset SELECT with where/orderBy/limit/offset", () => {
    const q = compileSubsetQuery("messages", {
      where: fn("eq", ref("body"), val("x")),
      orderBy: [{ col: "created_at", dir: "desc" }],
      limit: 10,
      offset: 20,
    })
    expect(q.sql).toBe(`SELECT * FROM "messages" WHERE "body" = ? ORDER BY "created_at" DESC LIMIT ? OFFSET ?`)
    expect(q.params).toEqual(["x", 10, 20])
  })

  it("emits LIMIT -1 when offset is given without a limit (SQLite requirement)", () => {
    const q = compileSubsetQuery("t", { offset: 5 })
    expect(q.sql).toBe(`SELECT * FROM "t" LIMIT -1 OFFSET ?`)
    expect(q.params).toEqual([5])
  })

  it("rejects a negative limit/offset and an invalid orderBy column", () => {
    expect(() => compileSubsetQuery("t", { limit: -1 })).toThrow(/non-negative/)
    expect(() => compileSubsetQuery("t", { orderBy: [{ col: "a; DROP" }] })).toThrow(/orderBy column/)
  })
})
