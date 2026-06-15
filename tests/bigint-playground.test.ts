// CHARACTERIZATION PLAYGROUND (issue #10): how int64 / bigint behaves across the
// workerd DO SqlStorage boundary, and the lossless round-trip path we could build on.
//
// Status: issue #10 is wontfix (it's a workerd platform limitation, tracked upstream
// at cloudflare/workerd#4195). This test is NOT a regression guard for a library
// feature — it is an executable record of the exact runtime behavior the issue
// documents, kept so a future fix (or a workerd change) has solid footing to start
// from. If a future workerd changes any of these facts, THIS test breaks and tells
// us the int64 calculus has shifted — rather than the change surfacing as a silent
// precision bug in user data.
//
// The shape of the problem (all proven below):
//   - SQLite STORES int64 exactly (it's a 64-bit signed integer type).
//   - The JS↔SQLite BIND boundary cannot accept a `bigint` (any magnitude throws).
//   - The JS↔SQLite RETURN boundary coerces INTEGER → JS `number` (lossy > 2^53).
//   - BUT: binding the DECIMAL STRING stores an exact int64 (numeric affinity), and
//     reading via CAST(col AS TEXT) returns it exactly — so a lossless bigint
//     round-trip IS achievable if the library is made column-type aware.

import { env, runInDurableObject } from "cloudflare:test"
import type { SqlStorage } from "@cloudflare/workers-types"
import { describe, expect, it } from "vitest"

// 2^53 + 1 — the smallest positive integer a JS number cannot represent exactly.
// Number(2^53+1) === 2^53, so any number-typed round-trip is detectably lossy here.
const BIG_STR = "9007199254740993"
const BIG_BIGINT = 9007199254740993n

async function inDO<T>(fn: (sql: SqlStorage) => T): Promise<T> {
  const stub = env.TEST_DO.get(env.TEST_DO.idFromName(`bp-${Math.random()}`))
  return runInDurableObject(stub, (_i, state) => fn(state.storage.sql))
}

describe("workerd SqlStorage int64/bigint characterization (issue #10)", () => {
  it("SQLite STORES int64 exactly — the value is intact, only the JS view is lossy", async () => {
    const r = await inDO((sql) => {
      sql.exec(`CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER)`)
      sql.exec(`INSERT INTO t(id,n) VALUES ('lit', ${BIG_STR})`) // literal: no JS coercion
      return {
        exactViaText: Array.from(sql.exec<{ s: string }>(`SELECT CAST(n AS TEXT) s FROM t WHERE id='lit'`))[0]!.s,
        lossyViaNumber: String(Array.from(sql.exec<{ n: number }>(`SELECT n FROM t WHERE id='lit'`))[0]!.n),
        affinity: Array.from(sql.exec<{ ty: string }>(`SELECT typeof(n) ty FROM t WHERE id='lit'`))[0]!.ty,
      }
    })
    expect(r.affinity).toBe("integer") // stored as a real int64, not text
    expect(r.exactViaText).toBe(BIG_STR) // STORAGE is exact
    expect(r.lossyViaNumber).toBe("9007199254740992") // the JS `number` RETURN is lossy
  })

  it("BIND boundary rejects a bigint of ANY magnitude (workerd#4195)", async () => {
    const r = await inDO((sql) => {
      sql.exec(`CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER)`)
      const tryBind = (v: unknown): string => {
        try {
          sql.exec(`INSERT INTO t(id,n) VALUES ('k', ?)`, v as never)
          return "OK"
        } catch (e) {
          return e instanceof Error ? e.message : String(e)
        }
      }
      return { small: tryBind(42n), big: tryBind(BIG_BIGINT) }
    })
    // Not a >2^53 issue — even 42n is refused. SqlStorageValue has no `bigint` member.
    expect(r.small).toMatch(/Cannot convert a BigInt value to a number/)
    expect(r.big).toMatch(/Cannot convert a BigInt value to a number/)
  })

  it("WRITE WORKAROUND: binding the decimal STRING stores an exact int64", async () => {
    const r = await inDO((sql) => {
      sql.exec(`CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER)`)
      // Numeric affinity coerces the bound string to an integer on the way in.
      sql.exec(`INSERT INTO t(id,n) VALUES ('bstr', ?)`, BIG_STR)
      return {
        affinity: Array.from(sql.exec<{ ty: string }>(`SELECT typeof(n) ty FROM t WHERE id='bstr'`))[0]!.ty,
        exact: Array.from(sql.exec<{ s: string }>(`SELECT CAST(n AS TEXT) s FROM t WHERE id='bstr'`))[0]!.s,
      }
    })
    expect(r.affinity).toBe("integer") // affinity converted string -> exact int64
    expect(r.exact).toBe(BIG_STR)
  })

  it("READ WORKAROUND: CAST(col AS TEXT) returns the exact value to reconstruct as bigint", async () => {
    const reconstructed = await inDO((sql) => {
      sql.exec(`CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER)`)
      sql.exec(`INSERT INTO t(id,n) VALUES ('k', ?)`, BIG_STR)
      return Array.from(sql.exec<{ s: string }>(`SELECT CAST(n AS TEXT) s FROM t WHERE id='k'`))[0]!.s
    })
    expect(BigInt(reconstructed)).toBe(BIG_BIGINT) // lossless end-to-end via TEXT
  })
})
